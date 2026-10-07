# What a client is told

What the server says about a stream while a client waits for it or plays it: the `stats.json` fields stremio-core parses and the ones added beside them, the playback panel's numbers, the background-activity light and the DHT's health. Every number here is either measured or absent -- a client is never handed a zero that stands for "unknown".

## Startup phases in `stats.json`

`ServerHandle::engine_stats` / `file_stats` and the core's `/{infoHash}/{fileIdx}/stats.json` keep the server.js-compatible shape stremio-core parses and add these camelCase fields so a client can show honest pre-playback progress. (Of that shape, `connectionTries` is always `0` -- the one zero here that is not a measurement: stremio-core's `Statistics` requires the number, so it cannot be absent or `null`, and librqbit keeps no total of connection attempts.)

| Field | Meaning |
|---|---|
| `phase` | `resolvingMetadata` (no metadata yet), `checking` (hash-checking data already on disk), `buffering` (live or paused, and the stream file's initial window has not all arrived), `ready` (`initialWindowReadyBytes` has reached `initialWindowBytes` -- playback can start), `error` |
| `error` | Present only with `phase: "error"` when anything knows why: the message of a failed magnet add (metadata timeout, backend error; see [Query parameters and metadata resolution](#query-parameters-and-metadata-resolution)), a fixed message for a torrent the backend put in an error state (broken download folder, full disk), or a fixed "stopped for want of disk space" message for a torrent the engine's free-space watch paused before the disk was full (it resumes when there is room; see [What bounds the cache](storage.md#what-bounds-the-cache)) -- the backend's own text names server paths and stays in the log |
| `checkedBytes`, `checkTotalBytes` | Hash-check progress; non-null only while `checking` |
| `initialWindowReadyBytes`, `initialWindowBytes` | Bytes of the window the stream is waiting on that have arrived: whole verified pieces, plus the chunks already landed in a piece still arriving (so the count moves between pieces, and drops back if that piece fails its hash check); non-null only in `buffering`/`ready`. Also present per entry in `files[]`, where they are omitted rather than null. The window is measured from the offset the file's newest open reader was opened at, over the lookahead that reader was opened with, capped at the startup window (`STREAMING_LOOKAHEAD_BYTES`, 32 MiB): a `Range` request, a seek or a re-open is a fresh reader and moves it, and it does not advance as that reader reads on. With no reader open it is measured from the file's head. It is expanded to the whole pieces it touches, because a piece is the unit that becomes readable |
| `isFinished` | Every file the torrent wants is whole on the disk -- measured on the disk, not on the want set: a torrent whose want set retention trimmed to its window has nothing left to fetch and is not finished |
| `pieceLength` | The torrent's piece length; `null` until metadata resolves. On a multi-gigabyte torrent this is 8-16 MiB, so the startup window is a few pieces and the wait is best rendered **in pieces**, with `inFlightPiece` (below) for the progress inside the one the stream is waiting on |
| `inFlightPiece` | Byte progress of the single piece at the offset the file's open reader was opened at: `{ index, downloadedBytes, totalBytes, verified }`, or `null`. This is what lets a client say "waiting for the first piece, 6.2 of 16 MB" and draw a bar that moves. `null` -- **never a zeroed object** -- whenever we do not know: no reader open on that file (before the first, and again once the last closes), no metadata yet, or a torrent with no chunk map (`resolvingMetadata`/`checking`/`error`). Also present per entry in `files[]`, where it is omitted rather than null. See [The in-flight piece](#the-in-flight-piece) |
| `peerDiscovery` | `{ seen, queued, connecting, live, known }` peer counters, `known` being every address the peer table holds right now (`peers`/`unique`/`queued` remain as before) |
| `connectedSeeders` | How many of the peers we are **connected to** hold the complete torrent, i.e. can serve any piece. Not the swarm's seeder count -- it only ever counts our own connections and is always bounded by `peers`; for the swarm read `swarmSeeders`. 0 while `resolvingMetadata` -- a magnet with no metadata yet has no peers. (`swarmSize` is not this either: it is a server.js-compatible alias of `peers`, kept for wire compatibility.) |
| `swarmSeeders`, `swarmLeechers` | Seeders and leechers in the **whole swarm**, as the torrent's trackers report them -- see [Swarm counts](#swarm-counts-from-tracker-scrapes) below. `null` when unknown, **never** `0` |
| `swarmScrapeAgeSecs` | How many seconds ago the freshest scrape behind those two numbers came back. `null` exactly when they are |

The top-level window/phase describe the guessed stream file for `engine_stats` and the requested file for `file_stats` / `/{infoHash}/{fileIdx}/stats.json`.

### The in-flight piece

A piece is the unit that becomes readable -- none of a 16 MiB piece can be served until all 16 MiB of it verifies -- so whole verified pieces, all the have-bitfield can show, are too coarse to show a waiting player: it could only ever be told 0% or 100%. `inFlightPiece` is the finer view of the one piece that matters, the piece at the offset the open reader was opened at:

```json
"pieceLength": 16777216,
"inFlightPiece": {
  "index": 137,
  "downloadedBytes": 6553600,
  "totalBytes": 16777216,
  "verified": false
}
```

- `index` is the piece's index **in the torrent**, not in the file.
- `totalBytes` is that piece's real length. Every piece is `pieceLength` except the torrent's last, which is short -- the server converts librqbit's 16 KiB chunk counts to bytes and clamps them to the piece's own length, so a complete short last piece reports exactly its length rather than a rounded-up one. Render `downloadedBytes` of `totalBytes`; do not multiply anything yourself.
- The piece is the one at the offset the file's newest open reader was opened at -- what a player starting up, or resuming after a seek, waits on. A `Range` request, a seek or a re-open is a fresh reader and moves it, as it moves `initialWindowBytes`; it does not advance as that reader reads on. An offset at or past the end of the file names the file's last piece.
- It is `null` before any reader opens the file and again once the last one closes. With two open (a player's second connection, to the index at the file's tail, say), the newer one names it, and when that one closes the older one does again.

**`downloadedBytes` can go backwards, and `verified` is the only field that means "ready".** A chunk counts as downloaded the moment it is written to disk, *not* when it is checked: the piece's hash is only verified once every chunk is in, and a piece that fails the check is discarded, dropping the count **back to zero**. So:

- Never present a full `downloadedBytes` as playable on its own -- it only means "complete enough to be hashed". `verified: true` (and only that) means the piece is in the have-bitfield and can be served.
- **Hold at nearly-complete until `verified`.** Cap the bar somewhere short of 100% while `verified` is false, and let `verified` be what fills it.
- **Do not animate backwards.** A decrease is a failed hash check, not progress being undone in a way a user can act on. Keep the bar where it was (or reset it without a transition) rather than running it down.

### Swarm counts from tracker scrapes

`stats.json` reports **three different numbers** that are easy to confuse:

| Field | Question it answers |
|---|---|
| `peers` | How many peers we currently have a live connection to (`swarmSize` is a server.js-compatible alias of this -- it is not a swarm-size estimate) |
| `connectedSeeders` | How many of *those* connections hold the complete torrent. Always `<= peers` |
| `swarmSeeders` / `swarmLeechers` | How many seeders and leechers exist **in the whole swarm**, including everyone we never connected to |

The swarm numbers come from this server scraping the torrent's own trackers -- BEP-48 over HTTP(S), BEP-15 action 2 over UDP. A scrape is read-only: it carries no port, peer id or event, so it cannot register us as a peer or interfere with the announces the torrent engine makes. (This is why the engine does not do it for us: a client is expected to scrape for itself.)

Because they are a **tracker snapshot rather than a live measurement**, they come with `swarmScrapeAgeSecs`, the age of the freshest scrape behind them -- show it, or at least do not present a 20-minute-old count as "now". A tracker is scraped at most once every 15 minutes per torrent, with an exponential backoff (60 s up to 30 min) after failures, and only while something is actually polling that torrent's stats. Numbers older than an hour are dropped rather than shown.

`swarmSeeders` and `swarmLeechers` are **`null`, never `0`, when we do not know** -- a swarm with zero seeders is a real state, and a client has to be able to tell "nobody is seeding this" from "we have not been able to ask". Expect `null` for:

- a **DHT-only** torrent (a magnet with no `tr=` trackers -- there is nothing to scrape),
- a **private** torrent (`private` in the info dictionary): those are never scraped at all, since an unsolicited request can breach a private tracker's rules and its announce URL carries a passkey,
- a torrent whose trackers have not answered yet, do not answer, or do not know the info hash,
- a magnet whose metadata has not arrived (we cannot yet tell whether it is private, so we leave it alone).

Multiple trackers are aggregated with **`max`, not `sum`**, computed separately for seeders and leechers. Each tracker only ever sees the peers that registered with *it*, so no tracker's number is a share of a total; and several trackers in the shipped list share a backend and answer with byte-identical counts, so summing would report the same swarm several times over. The largest number a single tracker vouches for is the honest floor. Trackers that failed or do not know the hash contribute nothing at all (they are not folded in as zeroes), and an implausible count (above 100000) is logged and ignored. Per-tracker figures are in `sources[]`, so a client can see the disagreement for itself.

### DHT health: `ServerHandle::dht_status()`

`ServerHandle::dht_status()` answers a `DhtStatus` (as JSON over FFI, these names):

| Field | Meaning |
|---|---|
| `enabled` | Whether a DHT is running at all |
| `nodes` / `nodesV6` | Nodes in the IPv4 / IPv6 routing table right now |
| `everBootstrapped` | Whether either routing table has been non-empty at any point this session. **Sticky** -- this is what tells "idle right now" apart from "never worked on this network" |

**The DHT is a peer *source*, not a requirement.** A torrent with working trackers downloads at full speed without one; only a trackerless magnet depends on it. Some networks -- carrier-grade NAT, a firewalled mobile APN, a captive portal -- drop the UDP the DHT needs, and then bootstrap never completes: a real Android session had every bootstrap host failing for 28 minutes while torrents pulled 30+ MB/s from trackers.

So a client should treat `enabled && !everBootstrapped` as an **informational state, not an error**: something like *"DHT unavailable -- using trackers only"* in a diagnostics panel, and, where a magnet has no `tr=` trackers, a warning that this link may not find peers. Do not poll for it as though it will change quickly -- the server reports the conclusion once, in the log, after a 90-second grace window.

**Bootstrap names are resolved by the server, not by librqbit** (`backend/dht_bootstrap.rs`): the system resolver can fail for every bootstrap host at once, so bootstrap dies at DNS, before a UDP packet is sent, unless something else resolves the names. librqbit accepts address literals, so the server hands it literals: the system resolver first, then **DNS over HTTPS** (`dns.google`, then `cloudflare-dns.com`, short timeouts), then a small address cache persisted as `dht-bootstrap.json` next to the `dht.json` routing table. A name still unresolved is passed on **as a name**, so librqbit's own retries can succeed if DNS comes back. One INFO line reports what resolved and how, one WARN if nothing did; the pass is bounded and cannot fail start-up. **This fixes DNS only**: if the network drops the DHT's UDP outright, correct addresses change nothing.

**IPv6 addresses are dropped on a host with no IPv6 route.** The resolver probes the route once per pass (a UDP `connect()` to a global v6 address, which sends nothing) and, when there is none, hands librqbit the v4 literals only; a dual-stack host keeps both, v4 first. On an IPv4-only device, the AAAA records are retried forever at one warning each while the v4 literals of the same hosts bring the DHT up in under a second. A list that would end up *empty* is kept whole instead: librqbit treats a bootstrap with no successful entry as a failure and stops its DHT worker.

**The bootstrap list is the two hosts measured to answer**: `dht.libtorrent.org:25401` and `dht.transmissionbt.com:6881`. `router.utorrent.com`, `dht.aelitis.com` and `router.bittorrent.com` resolve but answer no mainline `ping` or `find_node` from either of two networks tried, so they are left out -- a name that never replies is retry noise, not resilience. Do not add a host without pinging it first. `dhtBootstrapNodes` ([settings.md](settings.md)) replaces the list.

librqbit's own per-attempt DHT and UPnP warnings (`librqbit_dht::dht` and `librqbit_upnp`) are pinned to `error` in the default log directives, so they do not appear at INFO: both retry forever and warn on every attempt, which would otherwise produce hundreds of identical lines and no conclusion. The single conclusion from `diagnostics::dht_health` stands in their place. UPnP port forwarding is only requested for a **fixed** torrent listen port (`TorrentListenPort::Fixed`, `42000..42010`), since an ephemeral one -- the default, and what xtremio uses -- asks the router for a mapping that never comes back.

### Query parameters and metadata resolution

The stats route and `ServerHandle::engine_stats` take the same parameters as `/{infoHash}/{fileIdx}` (`engine_stats` and `file_stats` take the trackers as an argument) and behave like it when they are the first request for a torrent:

- **`tr=`** (repeatable, `tracker:`-prefixed values accepted, `dht:` ignored) -- trackers merged into the engine when the stats request is the one that creates it. Poll stats before the first stream request freely: the engine is created exactly as the stream route would create it, so the addon's trackers are kept for the session (the engine passes them to librqbit as `tr=` params of the magnet link it adds -- librqbit reads a magnet's trackers from the link alone, so `sources` lists them once metadata arrives). Trackers can only be set by the request that creates the engine -- librqbit has no API to add trackers to a torrent later, so a later request carrying extra trackers does not extend the set.
- **`f=`** (repeatable) -- file filters for resolving `fileIdx=-1`, as on the stream route.
- **`sources`** lists the trackers the torrent was added with. librqbit exposes no per-tracker announce counters, so `numRequests`/`numFound`/`lastStarted` are `0`/empty; a tracker we have successfully scraped also carries `seeders`, `leechers` and `completed` (absent until it answers).

**During metadata resolution** (a magnet whose info dictionary has not arrived yet) the stats route and `engine_stats`/`file_stats` answer immediately with `200` and `phase: "resolvingMetadata"`, `hasMetadata: false`, an empty `files` array, `streamLen: 0` and `sources` listing the trackers in use -- the per-file route included, since there is no file list to index into yet. Requests never block on metadata, and concurrent requests for one magnet share a single resolution -- the stream routes, the stats route, `engine_stats`/`file_stats` and stremio-core's `/{infoHash}/create` all join the same in-flight add. Once metadata is known, a `fileIdx` that does not exist returns `404` as before.

**Metadata resolution is bounded**: an add that has not produced metadata after **90 s** (`enginefs::METADATA_RESOLVE_TIMEOUT`) is given up on. Requests that were waiting for it (`/{infoHash}/{fileIdx}`, `HEAD`, `/{infoHash}/create`) get `504 Gateway Timeout` (`502` if librqbit itself refused the add, `500` otherwise; bodies are fixed strings, details go to the log). The failure is remembered: until something retries it, the stats route and `engine_stats`/`file_stats` answer `200` with `phase: "error"` and an `error` message for that hash, so a poller can stop waiting. Only a request that needs the file list (stream, `HEAD`, `/create`) retries -- a fresh play attempt gets a fresh 90 s -- while stats polls never restart an add. A failure record nobody has asked about for 5 minutes is dropped by the same inactivity sweep that removes idle torrents; the next request then starts over.

## What a panel is told about a stream

`ServerHandle::stream_numbers(url)` answers, for **the URL a client handed its player**, what this server holds of that stream right now. One call, and the shape of the URL is what dispatches it: a torrent stream is `/{infoHash}/{fileIdx}` -- including the auto-select `/{infoHash}/-1?f=…`, resolved to a file with the route's own `resolve_file_idx`, so a client that lets the server pick the file can ask with the URL it handed its player -- or its `/stream/` alias; a proxied one is `/proxy/?d=…` or the Core path format; and both of those shapes are this server's own routes -- so the two stores behind them (the piece store, keyed by info hash; the proxy cache, keyed by entity) answer through one interface (`server/src/stream_numbers.rs`) that keeps no state of its own. Each answers from a live reading of its own directories and remembers nothing.

```json
{
  "window": {
    "behindBytes": 1288490188,
    "aheadBytes": 356515840,
    "behindSeconds": 412.5,
    "aheadSeconds": 114.1
  },
  "sharing": {
    "committedBytes": 859832320,
    "transfer": {
      "downloadedBytes": 4800,
      "unverifiedBytes": 400,
      "uploadedBytes": 2100,
      "ratio": 0.4375
    },
    "refusedReclaims": 3
  }
}
```

- **`window`** is the **unbroken run** of this stream that is on the disk round the byte a player has actually reached, split at that byte: `behindBytes` is how far a scan back is served from the cache, `aheadBytes` is read-ahead that has *arrived*. Only complete pieces (a torrent) or chunks (a proxied stream) count, the first one missing on either side ends that half, and neither half reaches outside the file. It is **not the retention policy's window** and does not depend on the budget: under a budget that splits the file the run is usually what the policy kept, and under one that covers it, none published or a pin it is whatever has been fetched round the playhead. `behindSeconds`/`aheadSeconds` are the same halves in seconds at the reader's own consumption (capped at the film's bitrate where a duration is known), `null` where nothing is consuming them -- see `enginefs::retention::CacheWindow::worst_of` for which reader a half is measured round when several are open.
- **`sharing`** is torrents only. `committedBytes` is the play session's committed set: the pieces of its shared set this server holds, which is what a peer is told it has -- never reclaimed while the torrent is in the swarm. Under a budget that covers the file the shared set is the whole file, so it is every byte of the file held, the read-ahead's included. `transfer` is what the torrent has moved: `downloadedBytes`/`uploadedBytes` are **this session's** -- librqbit's own per-torrent counters, which start at zero when the torrent is added to this process -- and `ratio` is their quotient, or `null` -- never `0` -- when nothing has been downloaded, a ratio against zero being undefined rather than nil. It is deliberately not the conventional across-restarts ratio: persisting counters would mean storing a claim about a past this process never saw, and a client should label it as the session's.
  - `unverifiedBytes` is `downloadedBytes` less what piece hashes have vouched for (librqbit's `downloaded_and_checked_bytes`), and it is **not "wasted": the subtraction cannot tell which**. Three things sit in it. Chunks of a piece still in flight are in it and leave it the moment that piece completes and checks; so is the deliberate second copy of a chunk, asked of a fast peer to get a piece out from under a stalled one; and so is what really was thrown away, a piece deleted before it could complete or one a peer beat us to. So draw it as a **level, not a verdict**: a few per cent of what was played is the ordinary cost of duplication in flight, and a multiple of it is a stream fetching what its own retention pass is deleting, which is the bug this figure exists to show. It saturates at zero rather than going negative, defensively: `downloaded_and_checked_bytes` counts only pieces completed off peers in this session -- a torrent restored from disk seeds its have-bytes and not this counter -- so within a live torrent the subtraction cannot go negative at the pinned rev, and the saturation is there for a rev that seeds the counter from disk.
  - `refusedReclaims` sits beside `transfer` rather than inside it -- it is the engine's number, not the connection's: how many pieces this engine's retention passes asked the backend to forget and were refused, over the life of the engine. **Zero is the only healthy value.** A refusal is librqbit keeping a piece an open stream's lookahead still covers and this policy's window no longer does, so the disk cannot come back under budget while that stream lives and every tick spends a `drop_pieces` to be refused again. It and `unverifiedBytes` are the two numbers that made a phone fetching 1.6 GB to play a hundred megabytes legible. Never `null`: a proxied stream, which has no engine and so runs no passes, has no `sharing` row at all.

**Every absence is a real one, and a client draws no row rather than a zero.** An absent field is JSON `null`, never a zero and never a missing key. The whole answer is `null` for a URL this server is not holding -- a stream fetched directly from an addon, a local file -- and that is a `200`, not a `404`: "nothing" is a complete answer to "what do you hold of this". `window` is absent only for a torrent no reader has been inside in this process, so there is no playhead to measure from. A proxied stream that is not being played, or that no byte has reached a player of yet, has no answer at all: `null`, like a stream this server does not hold. `sharing` is absent for a proxied stream and only for one: it is not seeded, so it has no committed set, no ratio and no retention passes to be refused. A torrent whose store is live always has a `sharing` row (one in error, or one whose store has not been seeded yet, can answer `null` as a whole), because it always has at least `refusedReclaims` -- a count, which is `0` and not absent when nothing has been refused. `committedBytes` alone is absent where the play session has promised nothing: its shared set not drawn yet (it waits for the player to state the film's length), none published, a file only a side read or an archive's translated source opened, or a pin -- which shares the file whole on its own account. `transfer` alone is absent for a torrent whose counters cannot be read -- librqbit keeps them in a torrent's live state, so one that is paused, still checking, stopped for space or in error has none, and a torrent that has moved gigabytes and then paused has not moved nothing: its four numbers go together and go absent together rather than reading as a session that has shared nothing.

Cheap enough to poll while a panel is open, and no cheaper: it creates no engine and starts no magnet add, and it does not count as a poll (so it cannot hold a torrent out of the idle sweep just by being asked), but a proxied stream's window is counted from a listing of its own directories on the blocking pool (a torrent's from a copy of the bits its store keeps). Ask it while the panel is up, not for the life of the process.

## Background activity

`ServerHandle::background_traffic()` answers one question: **is this server using your connection while you are not watching?** Serving a peer and an offline download running with nothing on screen are the same fact to a viewer, so there is deliberately no taxonomy of who is at the other end -- but there are two directions, because a client shows them as one light with three glyphs (up, down, both) and offers a control for each. So the answer is two halves, `downloading` and `uploading`, each honest on its own, and `active` is either.

It is one call rather than two on purpose. Traffic and playback are measured differently -- traffic is a counter compared against an earlier reading, playback is state the engine already keeps -- and a client sampling them separately across an FFI boundary would take them a moment apart and flicker on every disagreement. The conjunction is taken in the server (`routes::system::background_traffic`), where they cannot disagree.

- **What is counted**: the connection, not the disk. Each torrent's own peer counters as librqbit keeps them -- bytes received from peers and bytes sent to them -- summed over the torrents that exist (`bytes_downloaded`, `bytes_uploaded`) in the one librqbit session, offline downloads included. A download of what is not a torrent -- an addon link, a Drive file -- adds what its filler fetched from the origin to `bytes_downloaded` and lights the down half the same way; a player's own reads through `/proxy`, posters and subtitles are not in it, and nothing but a torrent uploads. Counting bytes through the torrent storage instead would light this up on every restart: librqbit's initial check reads every restored torrent back off the local disk to hash it, with no network involved. The counters are the live state's, so a torrent that pauses or is removed takes its bytes out of the sum; less than before is not growth.
- **Over what window**: five seconds (`window_secs`). A counter that has not grown since the last reading is the measurement; a single sample of a total is not a rate. So nothing is reported until one window has closed -- the first call is a baseline -- and a torrent that is connected but stalled reads as idle, because this is a light about traffic. If nobody asks for a long stretch (a backgrounded app, a suspended phone) the reading is used as a fresh baseline instead of a verdict about minutes nobody observed.
- **What "not watching" means**: no open stream, over the window as well as right now -- and no stream *started* since the reading before, which is what catches a short playback that began and ended between two polls and was open at neither (`BackendEngineFS::playback_starts`, a count that only grows). The bytes a player's own stream pulled in stay in the counters after playback ends, so a signal that only asked about *now* would accuse the background of the viewer's own film every time they stopped it. The cost of getting that right is that after playback ends the light can take up to two windows to come on for traffic that really is unattended.
- **The sharing setting and the light cannot disagree.** The session's upload switch (`BackendEngineFS::apply_upload_switch`) is on while a player reads, while a torrent download is on its way, and otherwise only with `seedingEnabled` on and not held (`ServerHandle::set_idle_sharing_held`) -- decided from the same playback reading the light uses, recomputed when a stream opens, a pin lands, the setting or the hold moves and on every reconciler tick. So with the setting off, a lit "up" is always a download still fetching, which the rule lets share, and never an upload the setting has ruled out.
- `downloading` / `uploading`: that direction's counter grew over the last closed window and nothing was playing over it or since. `active` is `downloading || uploading`; `playing` is what the server sees right now, offered so the answer can be explained rather than only shown.

Asking is cheap -- per torrent that exists, one read of librqbit's live stats snapshot (a handful of counters, no file list, no tracker scrape) and the three live playback fields, nothing built -- and it is a peek, not a poll: it touches no torrent's idle clock, so it cannot keep anything out of the idle sweep, and it creates nothing: no engine, no magnet add. Polling once a second or two is fine. The verdict changes when a window closes, or the moment playback is seen, whichever comes first; asking faster than the window is otherwise answered from the standing reading.
