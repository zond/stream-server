# Stream Server

<div align="center">

**🚀 Pure-Rust Torrent Streaming Engine**

*A headless, zero-system-dependency streaming backend, forked from Stremio's `server.js` replacement*

[![Release Build](https://github.com/zond/stream-server/actions/workflows/release.yml/badge.svg)](https://github.com/zond/stream-server/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-MIT%20src%20%2F%20GPL--3.0%20binary-blue?style=flat-square)](#-license)
[![Open Source](https://img.shields.io/badge/Open%20Source-✓-brightgreen?style=flat-square)](https://github.com/zond/stream-server)

</div>

---

## 💡 About

Stream Server is zond's hard fork of [stremio-native/stream-server](https://github.com/stremio-native/stream-server) (formerly `perpetus/stream-server`, itself an open-source alternative to Stremio's closed-source `server.js`), rewritten around a fork of `librqbit`. It has **no ambition to merge back upstream**: the API, the engine and the licensing have all diverged, and it is shaped by one client. That client is [xtremio](https://github.com/zond/xtremio), a Flutter Stremio client that embeds this server in-process as a Rust library (`stream_server::start`, see [Library API](#library-api)); the same crate also builds a standalone `server` binary that runs on its own.

Its goal is narrower than upstream's: a **headless torrent-streaming server with no system-library requirements**. `cargo build` on a machine with a Rust toolchain and a C compiler — no libtorrent, no libclang, no FFmpeg, no GUI toolkits — is enough to produce a working server, whether you build the default binary or the `--no-default-features` one. The server itself is Rust; the C compiler is for the C some dependencies bundle and build from source (`aws-lc-sys` under rustls, `libmimalloc-sys`). The one external program the server runs is `curl`, and only for the `/ftp` route (see [Routes](#routes)).

To get there, this fork **deliberately drops Stremio server.js API compatibility**: there is no HLS transcoding, no FFmpeg/FFprobe integration, and no video-probing endpoints. Those existed to reformat video for Stremio's web-based player. This server instead sits behind a native client (xtremio: Flutter, with `media_kit`/`libmpv` for playback) that does direct play and handles codecs and subtitles itself — so the server's only job is getting torrent and archive bytes onto an HTTP connection efficiently, not transcoding them.

The torrent engine is [`librqbit`](https://github.com/ikatson/rqbit) — the **sole** torrent backend, in Rust, no system libraries — consumed via a fork ([`zond/rqbit`](https://github.com/zond/rqbit), pinned to one git rev in `enginefs/Cargo.toml`) that follows upstream and adds what a bounded streaming cache needs from the engine: a configurable per-stream lookahead window; piece reclaim (the engine forgets a piece so its storage may delete it, and the storage decides the have-set at startup); holding pieces back from what a torrent announces; a runtime per-torrent peer cap; a session-wide upload switch; per-piece chunk progress and a count of the connected peers that are seeders (what `inFlightPiece` and `connectedSeeders` below are read from); Mozilla's compiled-in TLS roots; and the fixes found on the way (a write past 2 GiB on a 32-bit `off_t` among them). There used to be an optional C++ `libtorrent` backend; it has been removed entirely, along with its vcpkg build apparatus, so there is nothing left in this repo that pulls in a C or C++ toolchain for torrenting.

---

## 🌟 Why this fork?

| | Stream Server (this fork) | Upstream `server.js` / stream-server |
|---|---|---|
| **Build deps** | ✅ The Rust toolchain and a C compiler; no system libraries | FFmpeg/FFprobe required at runtime; Node.js for `server.js` |
| **Transcoding** | ❌ Not the server's job — client plays containers/codecs directly | ✅ HLS transcoding via FFmpeg |
| **Torrent backend** | Pure-Rust `librqbit`, the only backend | Native libtorrent (or Node bindings) |
| **Open Source** | ✅ Source is MIT; default binary is GPL-3.0 (see [License](#-license)) | Upstream `server.js` is closed source |
| **Archive Streaming** | ✅ ZIP/7Z/TAR/TGZ/RAR built in (pure Rust) | ✅ |
| **Headless** | ✅ No tray, no desktop GUI in this repo | Varies |

This is not a drop-in replacement for `server.js` — the API surface it exposes is intentionally smaller. It's built to be the backend of one specific client, not a generic Stremio-compatible service.

---

## ✨ Features

### Core Streaming
- **🚀 No system libraries, always**: the entire build — every feature combination — has no system-library or external-binary dependencies, only the pinned Rust toolchain and a C compiler for the C that `aws-lc-sys` and `libmimalloc-sys` bundle
- **🔧 Single backend**: `librqbit` (Rust, via the `zond/rqbit` fork — see [About](#-about)) is the only torrent engine — there is no C++ alternative to opt into
- **📡 HTTP Range Requests**: torrent pieces are streamed straight to HTTP range requests for instant seeking — direct play, no transcoding step in between

### Media & Archives
- **📦 Archive Streaming**: direct playback from ZIP, 7Z, TAR, tgz (`.tar.gz`) and RAR archives out of the box (all pure Rust). RAR is **on by default** via `unrar-rs`, which is GPL-3.0-or-later, so the default binary is GPL-3.0-or-later — see [License](#-license); build `--no-default-features` for an MIT binary without RAR
- Subtitles are the client's job: there is no subtitle conversion, track discovery or OpenSubtitles hashing in the server (see [Removed routes](#removed-routes))

### Control API
- **🔐 Per-launch bearer token** on every non-media route; media routes stay open for players. See [API](#-api)
- **📚 Library API**: an embedder calls `ServerHandle::{settings, update_settings, engine_stats, file_stats, pin_download, unpin_download, downloads, download_path, cache_usage, clean_cache_now}` directly — the same code the HTTP routes run, no HTTP client needed
- **📊 Stats**: `/stats.json`, `/{infoHash}/stats.json`, `/{infoHash}/{fileIdx}/stats.json` for server status and torrent progress
- **⚙️ Settings**: runtime-configurable via `/settings`, with the stremio-core-compatible shape
- **🔒 BitTorrent Privacy Controls**: DHT, PeX, LSD, encryption, interface binding, ports, and proxy settings. All accepted and persisted, but each has a fixed effect against `librqbit` — applied live, applied on the next start, or not honoured (no backend knob) — listed per setting in `bt_settings_support()`; a `POST /settings` response reports which bucket each one you sent fell into. See [BitTorrent Settings](docs/bittorrent-settings.md).
- **📺 LAN media listener**: an optional second listener that serves *media bytes only* to the local network, so a Chromecast can fetch a stream while the control API stays on loopback. Off by default and session-scoped. See [LAN media listener](#lan-media-listener)

---

## 📦 Installation

### Pre-built Binaries

No releases have been published from this fork yet — build from source (see below). The [`release.yml`](.github/workflows/release.yml) workflow is wired up to publish Windows/Linux/Arch binaries from a `v*` tag when that happens, but no tag has been pushed so far.

### Build from Source

**The build needs zero system libraries** — no libtorrent, no libclang, no FFmpeg, no GUI toolkits, for any feature combination this repo has; the one tool beyond Rust is a C compiler, for the C `aws-lc-sys` and `libmimalloc-sys` build from source (and, on macOS, a one-file shim `network-interface` compiles). The pinned toolchain in `rust-toolchain.toml` (Rust 1.98.0) is picked up automatically by rustup, and this is exactly what CI verifies with no `apt install` step at all:

```bash
# Default build: pure-Rust librqbit backend (the only backend) + pure-Rust RAR.
# NOTE: this links unrar-rs (GPL-3.0-or-later), so this binary is
# GPL-3.0-or-later — see the License section below.
cargo build --release
```

To get an MIT-licensed binary instead, drop RAR:

```bash
# MIT binary: same librqbit backend, no RAR (no unrar-rs, no GPL)
cargo build --release --no-default-features
```

| Feature | What it adds | Extra system deps |
|---|---|---|
| *(default)* | `rar` (pure-Rust RAR via `unrar-rs`) on top of the always-on `librqbit` backend | None |
| `rar` | RAR archive streaming via pure-Rust `unrar-rs` (**on by default**) | None |
| `tui` | The binary's `--tui` terminal UI (ratatui/crossterm). **Off by default**, so the library an app embeds carries no terminal stack; the release builds turn it on with `--features server/tui`, and a binary built without it refuses `--tui` | None |

RAR streaming is **on by default** and pure Rust — no libclang or C++ toolchain. ZIP, 7Z, TAR and tgz streaming are always built in too, and are not gated by any feature. Because `unrar-rs` is GPL-3.0-or-later, the default binary is GPL-3.0-or-later; drop the `rar` feature (`--no-default-features`) for an MIT binary, where RAR requests then return a 501 JSON error.

---

## 🚀 Quick Start

```bash
# Run the binary built above (the release packages install it as `stream-server`)
./target/release/server

# Or with cargo
cargo run --release -p server
```

The binary listens on **every interface**, on the standard streaming-server port: `ServerConfig::binary_default()` binds `0.0.0.0:11470` and advertises `http://127.0.0.1:11470` as its base URL, and an HTTPS listener on `0.0.0.0:12470` runs once a certificate is on disk (see `/get-https` below). So on the standalone binary the open media routes — `/proxy` and `/ftp` among them, which fetch whatever URL they are handed — are reachable from the network, and the control routes are guarded by the bearer token alone. An embedder's `ServerConfig::embedded()` binds `127.0.0.1:11470` only. One binary runs per machine: a second one finds `stream-server.lock` in the system temp dir held, and exits. Settings (`settings.json`) and logs (`logs/`) live in `<platform config dir>/stremio-server`, torrent data under `<platform cache dir>/stremio-server` unless `settings.cacheRoot` names another root.

Every control route requires a bearer token for this launch; the binary chooses it from its command line:

| Flag / variable | Effect |
|---|---|
| *(nothing)* | A fresh random token is generated and printed **to stdout** as `control API token: <token>` — it is never written to the log files. (With `--tui` the alternate screen hides that line; use one of the options below instead.) |
| `--token <t>` / `--token=<t>` | Use exactly this token (headless use: the operator already knows it, nothing is printed) |
| `STREAM_SERVER_TOKEN=<t>` | Same as `--token`, from the environment; `--token` wins if both are given, a blank value counts as unset |
| `--no-auth` | Run the control API open (every route answers without a token). Wins over `STREAM_SERVER_TOKEN`; contradicts an explicit `--token` and is rejected together with it |
| `--tui` | A terminal UI in place of the log on stdout. Only in a build with the `tui` feature (`cargo build --release --features server/tui`); any other build refuses the flag |

The `stremio-runtime` stub spawns the server with `--no-auth`: it is the compatibility shim for legacy clients that speak plain HTTP and cannot send the header. With the binary's all-interfaces bind, that leaves every control route open to the local network while the stub runs it. See [API](#-api).

### Startup phases in `stats.json`

`/{infoHash}/stats.json` and `/{infoHash}/{fileIdx}/stats.json` keep the server.js-compatible shape stremio-core parses and add these camelCase fields so a client can show honest pre-playback progress:

| Field | Meaning |
|---|---|
| `phase` | `resolvingMetadata` (no metadata yet), `checking` (hash-checking data already on disk), `buffering` (live, but the stream file's initial priority window is not fully on disk), `ready` (initial window on disk — playback can start), `error` |
| `error` | Present only with `phase: "error"` when anything knows why: the message of a failed magnet add (metadata timeout, backend error), see below, a fixed message for a torrent the backend put in an error state (broken download folder, full disk), or a fixed "stopped for want of disk space" message for a torrent the engine's free-space watch paused before the disk was full (it resumes when there is room; see *What bounds the cache*) — the backend's own text names server paths and stays in the log |
| `checkedBytes`, `checkTotalBytes` | Hash-check progress; non-null only while `checking` |
| `initialWindowReadyBytes`, `initialWindowBytes` | Bytes of the window the stream is waiting on that are already verified on disk; non-null only in `buffering`/`ready`. Also present per entry in `files[]`. The window is measured from the offset the file's newest open reader was opened at, over the lookahead that reader was opened with (never more than the startup window): a `Range` request, a seek or a re-open is a fresh reader and moves it, and it does not advance as that reader reads on. With no reader open it is measured from the file's head. It is expanded to the whole pieces it touches, because a piece is the unit that becomes readable. `ready == window` still means exactly "servable" |
| `pieceLength` | The torrent's piece length; `null` until metadata resolves. On a multi-gigabyte torrent this is 8-16 MiB — bigger than the startup window — so `initialWindowReadyBytes` can only read 0 or all of it and a percentage built from the pair sits at 0% for tens of seconds while the download runs perfectly. Render the wait **in pieces**, and use `inFlightPiece` (below) to show the progress inside the one the stream is waiting on |
| `inFlightPiece` | Byte progress of the single piece at the offset the file's open reader was opened at: `{ index, downloadedBytes, totalBytes, verified }`, or `null`. This is what lets a client say "waiting for the first piece, 6.2 of 16 MB" and draw a bar that moves. `null` — **never a zeroed object** — whenever we do not know: no reader open on that file (before the first, and again once the last closes), no metadata yet, or a torrent with no chunk map (`resolvingMetadata`/`checking`/`error`). Also present per entry in `files[]`, where it is omitted rather than null. See [The in-flight piece](#the-in-flight-piece) |
| `peerDiscovery` | `{ seen, queued, connecting, live }` peer counters (`peers`/`unique`/`queued` remain as before) |
| `connectedSeeders` | How many of the peers we are **connected to** hold the complete torrent, i.e. can serve any piece. Not the swarm's seeder count — it only ever counts our own connections and is always bounded by `peers`; for the swarm read `swarmSeeders`. 0 while `resolvingMetadata` — a magnet with no metadata yet has no peers. (`swarmSize` is not this either: it is a server.js-compatible alias of `peers`, kept for wire compatibility.) |
| `swarmSeeders`, `swarmLeechers` | Seeders and leechers in the **whole swarm**, as the torrent's trackers report them — see [Swarm counts](#swarm-counts-from-tracker-scrapes) below. `null` when unknown, **never** `0` |
| `swarmScrapeAgeSecs` | How many seconds ago the freshest scrape behind those two numbers came back. `null` exactly when they are |

The top-level window/phase describe the guessed stream file for `/{infoHash}/stats.json` and the requested file for `/{infoHash}/{fileIdx}/stats.json`.

#### The in-flight piece

A piece is the unit that becomes readable — none of a 16 MiB piece can be served until all 16 MiB of it verifies — so whole verified pieces, all the have-bitfield can show, are too coarse to show a waiting player: it could only ever be told 0% or 100%. `inFlightPiece` is the finer view of the one piece that matters, the piece at the offset the open reader was opened at:

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
- `totalBytes` is that piece's real length. Every piece is `pieceLength` except the torrent's last, which is short — the server converts librqbit's 16 KiB chunk counts to bytes and clamps them to the piece's own length, so a complete short last piece reports exactly its length rather than a rounded-up one. Render `downloadedBytes` of `totalBytes`; do not multiply anything yourself.
- The piece is the one at the offset the file's newest open reader was opened at — what a player starting up, or resuming after a seek, waits on. A `Range` request, a seek or a re-open is a fresh reader and moves it, as it moves `initialWindowBytes`; it does not advance as that reader reads on. An offset at or past the end of the file names the file's last piece.
- It is `null` before any reader opens the file and again once the last one closes. With two open (a player's second connection, to the index at the file's tail, say), the newer one names it, and when that one closes the older one does again.

**`downloadedBytes` can go backwards, and `verified` is the only field that means "ready".** A chunk counts as downloaded the moment it is written to disk, *not* when it is checked: the piece's hash is only verified once every chunk is in, and a piece that fails the check is discarded, dropping the count **back to zero**. So:

- Never present a full `downloadedBytes` as playable on its own — it only means "complete enough to be hashed". `verified: true` (and only that) means the piece is in the have-bitfield and can be served.
- **Hold at nearly-complete until `verified`.** Cap the bar somewhere short of 100% while `verified` is false, and let `verified` be what fills it.
- **Do not animate backwards.** A decrease is a failed hash check, not progress being undone in a way a user can act on. Keep the bar where it was (or reset it without a transition) rather than running it down.

#### Swarm counts from tracker scrapes

`stats.json` reports **three different numbers** that are easy to confuse:

| Field | Question it answers |
|---|---|
| `peers` | How many peers we currently have a live connection to (`swarmSize` is a server.js-compatible alias of this — it is not a swarm-size estimate) |
| `connectedSeeders` | How many of *those* connections hold the complete torrent. Always `<= peers` |
| `swarmSeeders` / `swarmLeechers` | How many seeders and leechers exist **in the whole swarm**, including everyone we never connected to |

The swarm numbers come from this server scraping the torrent's own trackers — BEP-48 over HTTP(S), BEP-15 action 2 over UDP. A scrape is read-only: it carries no port, peer id or event, so it cannot register us as a peer or interfere with the announces the torrent engine makes. (This is why the engine does not do it for us: a client is expected to scrape for itself.)

Because they are a **tracker snapshot rather than a live measurement**, they come with `swarmScrapeAgeSecs`, the age of the freshest scrape behind them — show it, or at least do not present a 20-minute-old count as "now". A tracker is scraped at most once every 15 minutes per torrent, with an exponential backoff (60 s up to 30 min) after failures, and only while something is actually polling that torrent's stats. Numbers older than an hour are dropped rather than shown.

`swarmSeeders` and `swarmLeechers` are **`null`, never `0`, when we do not know** — a swarm with zero seeders is a real state, and a client has to be able to tell "nobody is seeding this" from "we have not been able to ask". Expect `null` for:

- a **DHT-only** torrent (a magnet with no `tr=` trackers — there is nothing to scrape),
- a **private** torrent (`private` in the info dictionary): those are never scraped at all, since an unsolicited request can breach a private tracker's rules and its announce URL carries a passkey,
- a torrent whose trackers have not answered yet, do not answer, or do not know the info hash,
- a magnet whose metadata has not arrived (we cannot yet tell whether it is private, so we leave it alone).

Multiple trackers are aggregated with **`max`, not `sum`**, computed separately for seeders and leechers. Each tracker only ever sees the peers that registered with *it*, so no tracker's number is a share of a total; and several trackers in the shipped list share a backend and answer with byte-identical counts, so summing would report the same swarm several times over. The largest number a single tracker vouches for is the honest floor. Trackers that failed or do not know the hash contribute nothing at all (they are not folded in as zeroes), and an implausible count (above 100000) is logged and ignored. Per-tracker figures are in `sources[]`, so a client can see the disagreement for itself.

#### DHT health: the `dht` key on `/stats.json`

`GET /stats.json` always carries a `dht` object alongside the per-torrent entries (and `ServerHandle::dht_status()` is the same call):

| Field | Meaning |
|---|---|
| `enabled` | Whether a DHT is running at all |
| `nodes` / `nodesV6` | Nodes in the IPv4 / IPv6 routing table right now |
| `everBootstrapped` | Whether either routing table has been non-empty at any point this session. **Sticky** — this is what tells "idle right now" apart from "never worked on this network" |

**The DHT is a peer *source*, not a requirement.** A torrent with working trackers downloads at full speed without one; only a trackerless magnet actually depends on it. Some networks — carrier-grade NAT, a firewalled mobile APN, a captive portal — simply drop the UDP the DHT needs, and then bootstrap never completes no matter how long it runs. A real Android session showed exactly that: every bootstrap host failing for 28 minutes while torrents pulled 30+ MB/s from trackers.

So a client should treat `enabled && !everBootstrapped` as an **informational state, not an error**: something like *"DHT unavailable — using trackers only"* in a diagnostics or connection panel, and, where a magnet has no `tr=` trackers, a warning that this particular link may not find peers. Do not surface it as a failure while playback is fine, and do not poll for it as though it will change quickly — the server reports the conclusion once, in the log, after a 90-second grace window, and never repeats it.

**Bootstrap names are resolved by the server, not by librqbit.** The same field log also showed the *system resolver* returning `No address associated with hostname` for every bootstrap host — bootstrap failed at DNS, before a single UDP packet was sent. librqbit resolves `bootstrap_addrs` with `tokio::net::lookup_host`, which accepts an address literal, so the server resolves the names first and hands it literals: the system resolver, then **DNS over HTTPS** (`dns.google`, then `cloudflare-dns.com`, JSON API, short timeouts) if the system resolver returns nothing, then a small address cache persisted as `dht-bootstrap.json` next to the `dht.json` routing table so a later launch on the same broken network can reuse what worked. Anything still unresolved is passed to librqbit **as a name** rather than dropped, so its own retries can still succeed if DNS comes back. One INFO line reports what resolved and by which path; one WARN if every path failed for every host. None of it can fail or meaningfully delay start-up — the whole pass is bounded and falls back to the raw names.

**This only fixes the DNS half.** If the network drops the DHT's UDP outright, perfectly correct bootstrap addresses change nothing, because the queries never leave the device. The Android log that motivated this also showed SSDP refused with `EPERM` and UPnP timing out, which is what a network blocking this traffic wholesale looks like — so on *that* device this change may well not bring the DHT up. It removes one specific, observed, fixable cause of bootstrap failure; it is not a cure for "the DHT does not work here".

The bootstrap list itself is deliberately short. Of the five conventional public bootstrap names an earlier revision shipped, only `dht.libtorrent.org:25401` and `dht.transmissionbt.com:6881` actually answered a mainline DHT `ping` (3/3 attempts each); `router.utorrent.com`, `dht.aelitis.com` and `router.bittorrent.com` resolve but answered 0/3 and have been removed — a name that never replies is retry noise, not resilience. `router.bittorrent.com` was kept longest, on the grounds that it is the most widely deployed bootstrap name in the ecosystem and might answer from other networks; a 2026-09 re-probe from two networks, twice each, with both `ping` and `find_node`, found it answering on neither. Do not add a host to that list without pinging it first.

**IPv6 addresses are dropped on a host with no IPv6 route.** The resolver probes the route once per pass (a UDP `connect()` to a global v6 address, which sends nothing and fails immediately when there is no route) and, when there is none, hands librqbit the v4 literals only; a dual-stack host keeps both, v4 first. On the IPv4-only device that motivated it — a v6 link-local and no v6 default route — the AAAA records of the bootstrap names were being retried forever, at one warning per attempt, while the v4 literals of the very same hosts brought the DHT up in under a second. A list that would end up *empty* is kept whole instead: librqbit treats a bootstrap with no successful entry as a failure and stops its DHT worker.

The server no longer logs librqbit's own per-attempt DHT and UPnP warnings (`librqbit_dht::dht` and `librqbit_upnp` are pinned to `error` in the default log directives): both retry forever and warn on every attempt, which produced hundreds of identical lines and no conclusion. The single conclusion from `diagnostics::dht_health` replaces them. UPnP port forwarding is now only requested for a **fixed** torrent listen port (the desktop binary's `42000..42010`), since an ephemeral one — what embedders and the Android build use — asks the router for a mapping that never comes back.

Both stats routes accept the same query parameters as `/{infoHash}/{fileIdx}` and behave like it when they are the first request for a torrent:

- **`tr=`** (repeatable, `tracker:`-prefixed values accepted, `dht:` ignored) — trackers merged into the engine when the stats request is the one that creates it. Poll stats before the first stream request freely: the engine is created exactly as the stream route would create it, so the addon's trackers are kept for the session (the engine passes them to librqbit as `tr=` params of the magnet link it adds — librqbit reads a magnet's trackers from the link alone, so `sources` lists them once metadata arrives). Trackers can only be set by the request that creates the engine — librqbit has no API to add trackers to a torrent later (`add_trackers` is a documented no-op), so a later request carrying extra trackers does not extend the set.
- **`f=`** (per-file route, repeatable) — file filters for resolving `fileIdx=-1`, as on the stream route.
- **`sources`** lists the trackers the torrent was added with. librqbit exposes no per-tracker announce counters, so `numRequests`/`numFound`/`lastStarted` are `0`/empty; a tracker we have successfully scraped also carries `seeders`, `leechers` and `completed` (absent until it answers).

**During metadata resolution** (a magnet whose info dictionary has not arrived yet) both routes answer immediately with `200` and `phase: "resolvingMetadata"`, `hasMetadata: false`, an empty `files` array, `streamLen: 0` and `sources` listing the trackers in use — the per-file route included, since there is no file list to index into yet. Requests never block on metadata, and concurrent requests for one magnet share a single resolution — the stream routes, both stats routes and stremio-core's `/{infoHash}/create` all join the same in-flight add. Once metadata is known, a `fileIdx` that does not exist returns `404` as before.

**Metadata resolution is bounded**: an add that has not produced metadata after **90 s** (`enginefs::METADATA_RESOLVE_TIMEOUT`) is given up on. Requests that were waiting for it (`/{infoHash}/{fileIdx}`, `HEAD`, `/{infoHash}/create`) get `504 Gateway Timeout` (`502` if librqbit itself refused the add, `500` otherwise; bodies are fixed strings, details go to the log). The failure is remembered: until something retries it, both stats routes answer `200` with `phase: "error"` and an `error` message for that hash, so a poller can stop waiting. Only a request that needs the file list (stream, `HEAD`, `/create`) retries — a fresh play attempt gets a fresh 90 s — while stats polls never restart an add. A failure record nobody has asked about for 5 minutes is dropped by the same inactivity sweep that removes idle torrents; the next request then starts over.

---

## 🔌 API

The HTTP surface is deliberately small and split in two by `build_router()` (`server/src/lib.rs`):

- **Media routes are OPEN.** They hand bytes to a player. Players (mpv, a future Chromecast receiver) fetch the URLs stremio-core builds for them (`types/resource/stream.rs`) and cannot attach headers, so these routes take no token.
- **Everything else is control API and requires a bearer token**: `Authorization: Bearer <token>`, header only — a token in the query string is never accepted, so it does not end up in access logs or in URLs handed to third parties. A missing or wrong token gets `401` with the fixed body `unauthorized` and `WWW-Authenticate: Bearer`; the compare is constant-time. Control routes are what stremio-core's `StreamingServer` model calls through `Env::fetch` (so the embedding client attaches the header there), plus the app/test status probes.

### Authentication

`ServerConfig.auth: ServerAuth` decides how the token is chosen:

| Variant | Meaning |
|---|---|
| `Generated` (**default** for both `ServerConfig::embedded()` and `ServerConfig::binary_default()`) | 32 random bytes, hex-encoded, fresh per launch. The standalone binary prints it once to stdout at startup (`control API token: <token>`) and never passes it to `tracing`, so it is in no log file; an embedder reads `ServerHandle::auth_token()` |
| `Token(String)` | Use exactly this token (must not be empty). The binary's `--token <t>` flag and `STREAM_SERVER_TOKEN` variable select this |
| `Disabled` | No authentication; every route is open. The binary's `--no-auth` flag selects this, and the `stremio-runtime` stub always passes it. The Android JNI entry point (`server/src/jni.rs`) also runs this way: it can return only a URL to the Kotlin side, and its listener is loopback-only |

### Routes

| Method | Path | Access | Consumer |
|---|---|---|---|
| GET, HEAD | `/{infoHash}/{fileIdx}` | OPEN | players — the stream URL stremio-core builds (`?tr=…`, `?f=…` as documented under [Startup phases](#startup-phases-in-statsjson), plus `?buffer=` — see [Buffer profiles](#buffer-profiles)) |
| GET, HEAD | `/stream/{infoHash}/{fileIdx}` | OPEN | players (alias of the above) |
| GET, POST | `/{rar\|zip\|7zip\|tar\|tgz}/create`, `/{…}/create/{key}` | OPEN | players — archive session creation via `?lz=` (stremio-core builds these URLs) |
| GET | `/{rar\|zip\|7zip\|tar\|tgz}/stream`, `/{…}/stream/{key}`, `/{…}/stream/{key}/{*file}` | OPEN | players — archive member bytes |
| GET | `/ftp/{filename}?lz=…` | OPEN | players (FTP/FTPS passthrough through a spawned `curl`, which must be on `PATH` — without it the answer is `500`; any other scheme is `400`) |
| GET, HEAD, OPTIONS | `/proxy/{*rest}`, `/proxy`, `/proxy/` | OPEN | players — a remote stream fetched on their behalf, with the headers the addon asked for, and cached in whole chunks so a seek back into it is answered from disk. Any other method is `405` with `Allow`. See [Proxied remote streams](#proxied-remote-streams) |
| GET | `/local-addon/manifest.json` | OPEN | stremio-core default profile — **stub**: a valid manifest (`org.stremio.local`, "Local Files") declaring no types, resources or catalogs |
| GET | `/local-addon/stream/{type}/{id}`, `/local-addon/stream/{type}/{id}.json` | OPEN | stremio-core default profile — **stub**: always `{"streams": []}` |
| GET | `/local-addon/catalog/{type}/{id}`, `/local-addon/catalog/{type}/{id}/{extra}` (with or without `.json`) | OPEN | stremio-core profiles that carry the catalog-declaring descriptor — **stub**: always `{"metas": []}` |
| GET | `/local-addon/meta/{type}/{id}` | OPEN | stremio-core default profile — **stub**: `404`, logged at debug level only |
| any | anything else under `/local-addon/` | OPEN | **stub**: deliberate `404`, logged at debug level only (never the ERROR-level unhandled-request line) |
| GET | `/heartbeat` | TOKEN | app / tests |
| GET | `/stats.json` (always carries `dht`; `?sys=1` adds a `sys` object with `loadavg`/`cpus`) | TOKEN | app |
| GET | `/{infoHash}/stats.json`, `/{infoHash}/{fileIdx}/stats.json` | TOKEN | stremio-core `Statistics`; accept `tr=`/`f=` like the stream route |
| POST | `/create` | TOKEN | stremio-core `CreateTorrent` (torrent blob / URL) |
| POST | `/{infoHash}/create` | TOKEN | stremio-core `CreateTorrent` (magnet) |
| GET, POST | `/settings` | TOKEN | stremio-core `StreamingServer` (`{ baseUrl, options, values }` / `{ success }`) |
| GET | `/network-info`, `/device-info` | TOKEN | stremio-core `StreamingServer` |
| GET | `/casting` | TOKEN | stremio-core playback devices: what SSDP discovery has found. Only the binary runs discovery (`binary_default()`), so an embedded server answers `[]`; nothing can be cast to a listed device either (next row). No trailing slash: `/casting/` is an unknown path (`404`) |
| POST | `/casting/{devID}/player` | TOKEN | stremio-core `play_on_device`; answers `501` because casting is not implemented |
| GET | `/get-https?authKey=…&ipAddress=…` | TOKEN | stremio-core remote-HTTPS certificate fetch: fetches the certificate, writes it to the config dir, starts (or restarts) the HTTPS listener on `ServerConfig::https_addr` with it, and answers with **that listener's** port. `501` when no HTTPS address is configured (`embedded()`, the Android embed) — nothing is written and no network call is made |
| POST | `/{infoHash}/{fileIdx}/download` | TOKEN | offline downloads — pin the file; optional body `{"trackers":[…]}` (`sources`/`announce` accepted too), answer is a `DownloadInfo`. See [Offline downloads](#offline-downloads) |
| DELETE | `/{infoHash}/{fileIdx}/download?deleteFiles=1` | TOKEN | offline downloads — drop the pin, and with `deleteFiles` the data too |
| GET | `/downloads.json` | TOKEN | offline downloads — every pinned file |
| GET | `/cache.json` | TOKEN | cache usage against `settings.cacheSize` — see [Cache usage and cleaning](#cache-usage-and-cleaning) |
| GET | `/stream-numbers.json?url=…` | TOKEN | what this server holds of the stream a player is playing, for a playback panel — see [What a panel is told about a stream](#what-a-panel-is-told-about-a-stream) |
| POST | `/cache/clean` | TOKEN | give back everything nobody is playing and nobody is reading, now, and report what is left — see [Cache usage and cleaning](#cache-usage-and-cleaning) |
| POST | `/proxy-streams/{token}/close` | TOKEN | end every `/proxy` stream carrying the client's own `p=` token, and retire the token; answers `{"closed": n}` — see [Ending a proxied stream](#ending-a-proxied-stream) |

Unknown paths get `404`, a wrong method on a known path `405` (or `401` first, on a control route).

RAR routes return a `501` JSON error in a `--no-default-features` build.

**Archive members.** A member of a ZIP, TAR or tgz (`.tar.gz`/`.tgz`) archive is served with ranges; a tgz member streams as it is decompressed, its length read from its tar header first. An unsatisfiable `Range` on a member is `416` with `Content-Range: bytes */<len>`, and an empty member is an empty body. The `torrent:` form (`/{format}/stream/torrent:<infoHash>%2F<path>/{member}`) reads a ZIP inside a torrent and nothing else: any other format there — a 7z, whose decoder wants a seekable file — is `501` before the torrent is looked at. An archive named by URL is downloaded by `/create` into `<cacheRoot>/.archives`, with a 30 s connect timeout and a 60 s timeout between reads (the whole has no bound: an archive is gigabytes over whatever link the origin has); it is refused with `507` when the volume has no room for it above the 512 MiB free-space floor — before the body when the origin states a length that will not fit, and otherwise when the download reaches the floor.

### Buffer profiles

How far ahead playback reads is a choice, not a constant. A spotty connection — or a receiver whose own buffer is shallower than mpv's — wants more of the file fetched before it is needed; a fast link on a metered phone wants less. The choice is one of three profiles, and it is offered twice:

- **`settings.bufferProfile`** (`GET`/`POST /settings`, `ServerHandle::settings`/`update_settings`) — the default for every stream request that does not say otherwise. `"normal"` unless set.
- **`?buffer=` on the stream route** — `GET`/`HEAD /{infoHash}/{fileIdx}` and its `/stream/…` alias, alongside the existing `tr=`, `f=` and `download=`. It overrides the setting for that request only, so a client can keep a global preference and still change the buffer for one playback.

| Profile | Playback read-ahead | Startup window |
|---|---|---|
| `normal` (default) | 128 MiB (`MAX_SEEK_HOT_WINDOW_BYTES`) | 4 MiB |
| `large` | 256 MiB (×2) | 4 MiB |
| `maximum` | 512 MiB (×4) | 4 MiB |

The read-ahead is librqbit's per-stream lookahead (`FileStreamOptions::lookahead_bytes`, via `priorities::librqbit_stream_lookahead_bytes`) once bytes are flowing — after a seek and while playing sequentially. A reader is opened with the smaller of it and how far the retention window reaches ahead of the reader (`Engine::fetch_bound`), so a stream never asks the swarm for a piece the next retention pass would reclaim; the profile is the whole of the lookahead only for a file nothing bounds (the cache budget covers it). Either way it is a byte budget the engine tries to have on disk ahead of the read head, not a promise: a swarm that cannot fill it simply does not.

**The startup window is the same under every profile, deliberately.** The narrow first-frame want-set (4 MiB, `MAX_STARTUP_WINDOW_BYTES`) is what makes playback start quickly — widening it would spend that latency to buy read-ahead the very next request already asks for. Choosing a bigger profile never slows a play down; it changes what happens after the first frame.

**What it costs.** A larger window downloads further ahead of what is being watched, which means more of the file on disk at once and more **bandwidth** spent on bytes the viewer may seek past or never reach — worth saying out loud on mobile data. `maximum` asks for up to 512 MiB ahead of the read head. Where the cache budget is smaller than the file, the retention window caps the read-ahead, so a bigger profile buys nothing past the window there. If the connection is bad enough that even `maximum` stutters, the honest answer is not a bigger window but an offline download: pin the file (`POST /{infoHash}/{fileIdx}/download`, see [Offline downloads](#offline-downloads)) and watch it once it is there.

**Validation is lenient by design.** The value is matched case-insensitively with surrounding whitespace ignored. Anything else — a profile a future build added, a typo, an empty value — is *not* an error: on `?buffer=` it falls back to `settings.bufferProfile`, and on `POST /settings` it leaves the setting as it was, like every other unrecognised value in that payload. A player must never lose a playback because it guessed a name wrong. The wire is additive throughout: a client that sends neither gets exactly today's behaviour.

Only the torrent stream route reads the profile: archive members and offline downloads are not affected.

### Library API

An embedder holds a `ServerHandle` (from `stream_server::start`) and never needs an HTTP client for control calls. Every method runs on the server's own runtime and blocks the calling thread until done; all returned types are `serde`-serializable, so they can be passed as JSON over FFI:

| Method | Same as |
|---|---|
| `auth_token() -> Option<&str>` | the token control routes require (`None` with `ServerAuth::Disabled`) |
| `base_url() -> &str` | `settings.baseUrl` |
| `settings() -> Result<ServerSettings>` | `GET /settings` → `values` |
| `update_settings(patch: serde_json::Value) -> Result<ServerSettings>` | `POST /settings` (same keys, validation, engine update and persistence); returns the settings afterwards |
| `install_https_certificate(cert_pem: &str, key_pem: &str) -> Result<SocketAddr>` | the serving half of `GET /get-https`: write the PEMs to the config dir and start — or restart, so the new certificate is the one presented — the HTTPS listener on `ServerConfig::https_addr`; returns its bound address. Refused when no HTTPS address is configured |
| `https_addr() -> Option<SocketAddr>` | where the HTTPS listener is bound; `None` until a certificate has been installed (or found on disk at startup), after a boot whose HTTPS start failed (a certificate that will not load, a busy port: logged, and the server serves plain HTTP until the next `/get-https`), and always when `https_addr` is unset |
| `engine_stats(info_hash, trackers: &[String]) -> Result<EngineStats>` | `GET /{infoHash}/stats.json?tr=…` — including creating the engine with `trackers` when it is the first request for the hash and answering `resolvingMetadata` at once. `trackers` are normalised inside the shared function exactly like `tr=` (`tracker:` prefix stripped, `dht:` dropped, trimmed), so a stream's `sources` array can be passed as is |
| `file_stats(info_hash, file_idx: usize, trackers) -> Result<EngineStats>` | `GET /{infoHash}/{fileIdx}/stats.json?tr=…`; the route's `404` is a `FileNotFound` error |
| `pin_download(info_hash, file_idx: usize, trackers) -> Result<DownloadInfo>` | `POST /{infoHash}/{fileIdx}/download` — pin the file as an offline download (see [Offline downloads](#offline-downloads)); `trackers` are normalised as for `engine_stats` |
| `unpin_download(info_hash, file_idx: usize, delete_files: bool) -> Result<UnpinOutcome>` | `DELETE /{infoHash}/{fileIdx}/download?deleteFiles=1`; `unpinned: false` when nothing was pinned, `deletedFiles` what actually left the disk |
| `downloads() -> Result<Vec<DownloadInfo>>` | `GET /downloads.json` |
| `download_path(info_hash, file_idx: usize) -> Result<Option<String>>` | the `path` of that file's `downloads()` entry on its own — where the download is *placed*, not a file that exists (data is stored one file per piece). Never creates an engine |
| `cache_usage() -> Result<CacheUsage>` | `GET /cache.json` — what the cache occupies against its limit right now, without deleting anything. See [Cache usage and cleaning](#cache-usage-and-cleaning) |
| `clean_cache_now() -> Result<EvictionReport>` | `POST /cache/clean` — drop both owners' slack immediately and report what is left; a pin and the window of the stream being played are never touched. See [Cache usage and cleaning](#cache-usage-and-cleaning) |
| `stream_numbers(url: &str) -> Result<Option<StreamNumbers>>` | `GET /stream-numbers.json?url=…` — the cache around the playhead and, for a torrent, the committed set and this session's transfer totals (each absent where there is no such number, never zeroed). `None` is a stream this server does not hold, which is not an error. See [What a panel is told about a stream](#what-a-panel-is-told-about-a-stream) |
| `close_proxy_streams(token: &str) -> usize` | `POST /proxy-streams/{token}/close` — end every proxied stream the client marked with `token`, retire the token, and answer how many streams that was. See [Ending a proxied stream](#ending-a-proxied-stream) |
| `background_traffic() -> Result<BackgroundTraffic>` | no route — the one signal a client's "working in the background" indicator reads: `{active, downloading, uploading, playing, bytes_downloaded, bytes_uploaded, window_secs}`. See [Background activity](#background-activity) |
| `proxy_streams_live() -> usize` | how many proxied streams are being read right now, over all tokens — the number of players attached through `/proxy` |
| `set_lan_media(enabled: bool) -> Result<Option<SocketAddr>>` | start/stop the [LAN media listener](#lan-media-listener); returns its bound address afterwards. Refused while the `lanMediaEnabled` setting is false or `ServerConfig::lan_media_addr` is unset |
| `lan_media_addr() -> Option<SocketAddr>` / `lan_media_running() -> bool` | where that listener is bound right now, and whether it is running at all |
| `lan_media_requests_served() -> u64` | how many requests have reached that listener since the current cast session began — per session, reset by every start (an already-running listener included) and by every stop. Zero after a load is the receiver never having asked for the stream. See [LAN media listener](#lan-media-listener) |
| `lan_media_base_url(for_peer: IpAddr) -> Option<Url>` | the base URL to hand a receiver at `for_peer` — host = the local interface on its subnet, or the best-ranked one when nothing matches. `None` while the listener is off |

The HTTP handlers and these methods call the same functions (`routes::system::{engine_stats, file_stats, update_settings}`, `routes::downloads::{pin_download, unpin_download, downloads, download_path}`, `routes::cache::{cache_usage, clean_cache_now}`, `routes::stream_numbers::stream_numbers`, `proxy_streams::ProxyStreams::close`), so they cannot drift; `server/tests/embed.rs` compares them.

### What a panel is told about a stream

`GET /stream-numbers.json?url=…` (`ServerHandle::stream_numbers`) answers, for **the URL a client handed its player**, what this server holds of that stream right now. One call, and the shape of the URL is what dispatches it: a torrent stream is `/{infoHash}/{fileIdx}` — including the auto-select `/{infoHash}/-1?f=…`, resolved to a file with the route's own `resolve_file_idx`, so a client that lets the server pick the file can ask with the URL it handed its player — or its `/stream/` alias; a proxied one is `/proxy/?d=…` or the Core path format; and both of those shapes are this server's own routes — so the two stores behind them (the piece store, keyed by info hash; the proxy cache, keyed by entity) answer through one interface (`server/src/stream_numbers.rs`) that keeps no state of its own. Each answers from a live reading of its own directories and remembers nothing.

```json
{
  "window": { "behindBytes": 1288490188, "aheadBytes": 356515840 },
  "sharing": {
    "committedBytes": 859832320,
    "transfer": { "downloadedBytes": 4800, "uploadedBytes": 2100, "ratio": 0.4375 }
  }
}
```

- **`window`** is what is **on the disk** for this stream, split at the byte a player has actually reached: `behindBytes` is how far a scan back is served from the cache, `aheadBytes` is read-ahead that has *arrived*. Neither is the extent the policy intends to fill. Divide by the stream's bitrate for a time — the client has that and this server does not.
- **`sharing`** is torrents only. `committedBytes` is the retention policy's committed set: pieces advertised and promised not to be reclaimed while the stream is being played. `transfer` is what the torrent has moved: `downloadedBytes`/`uploadedBytes` are **this session's** — librqbit's own per-torrent counters, which start at zero when the torrent is added to this process — and `ratio` is their quotient. It is deliberately not the conventional across-restarts ratio: persisting counters would mean storing a claim about a past this process never saw, and a client should label it as the session's.

**Every absence is a real one, and a client draws no row rather than a zero.** The whole answer is `null` for a URL this server is not holding — a stream fetched directly from an addon, a local file — and that is a `200`, not a `404`: "nothing" is a complete answer to "what do you hold of this". `window` is absent when no retention policy is bounding the stream (the budget covers it, or the volume's free space could not be read — see [Cache usage and cleaning](#cache-usage-and-cleaning)) or when no reader has been inside it in this process; what is on the disk in that case is not a window but whatever has been fetched and not yet given back, which is a different quantity. `sharing` is absent for a proxied stream, which is not seeded and so has no committed set and no ratio, and for a torrent this server can say neither half about; `committedBytes` alone is absent for a torrent with no policy, which has promised nothing whatever it announces. `transfer` alone is absent for a torrent whose counters cannot be read — librqbit keeps them in a torrent's live state, so one that is paused, still checking, stopped for space or in error has none, and a torrent that has moved gigabytes and then paused has not moved nothing: the three numbers go together and go absent together rather than reading as a session that has shared nothing.

Cheap enough to poll while a panel is open, and no cheaper: it creates no engine and starts no magnet add, and it does not count as a poll (so it cannot hold a torrent out of the idle sweep just by being asked), but a proxied stream's window is counted from a listing of its own directories on the blocking pool (a torrent's from a copy of the bits its store keeps). Ask it while the panel is up, not for the life of the process.

### Background activity

`ServerHandle::background_traffic()` answers one question: **is this server using your connection while you are not watching?** Serving a peer and an offline download running with nothing on screen are the same fact to a viewer, so there is deliberately no taxonomy of who is at the other end — but there are two directions, because a client shows them as one light with three glyphs (up, down, both) and offers a control for each. So the answer is two halves, `downloading` and `uploading`, each honest on its own, and `active` is either.

It is one call rather than two on purpose. Traffic and playback are measured differently — traffic is a counter compared against an earlier reading, playback is state the engine already keeps — and a client sampling them separately across an FFI boundary would take them a moment apart and flicker on every disagreement. The conjunction is taken in the server (`routes::system::background_traffic`), where they cannot disagree.

- **What is counted**: the connection, not the disk. Each torrent's own peer counters as librqbit keeps them — bytes received from peers and bytes sent to them — summed over the torrents that exist (`bytes_downloaded`, `bytes_uploaded`) in the one librqbit session, offline downloads included. The first version counted bytes through the torrent storage instead, and lit up on every restart: librqbit's initial check reads every restored torrent back off the local disk to hash it, with no network involved. The counters are the live state's, so a torrent that pauses or is removed takes its bytes out of the sum; less than before is not growth.
- **Over what window**: five seconds (`window_secs`). A counter that has not grown since the last reading is the measurement; a single sample of a total is not a rate. So nothing is reported until one window has closed — the first call is a baseline — and a torrent that is connected but stalled reads as idle, because this is a light about traffic. If nobody asks for a long stretch (a backgrounded app, a suspended phone) the reading is used as a fresh baseline instead of a verdict about minutes nobody observed.
- **What "not watching" means**: no open stream, over the window as well as right now. The bytes a player's own stream pulled in stay in the counters after playback ends, so a signal that only asked about *now* would accuse the background of the viewer's own film every time they stopped it. The cost of getting that right is that after playback ends the light can take up to two windows to come on for traffic that really is unattended.
- `downloading` / `uploading`: that direction's counter grew over the last closed window and nothing was playing over it or since. `active` is `downloading || uploading`; `playing` is what the server sees right now, offered so the answer can be explained rather than only shown.

Asking is cheap — per torrent that exists, one read of librqbit's live stats snapshot (a handful of counters, no file list, no tracker scrape) and the three live playback fields, nothing built — and it is a peek, not a poll: it touches no torrent's idle clock, so it cannot keep anything out of the idle sweep, and it creates nothing: no engine, no magnet add. Polling once a second or two is fine. The verdict changes when a window closes, or the moment playback is seen, whichever comes first; asking faster than the window is otherwise answered from the standing reading.

### Offline downloads

A **pinned** file stays wanted no matter which file of the torrent is being played, and its torrent is exempt from idle removal and keeps running when nothing is playing, for as long as it has a pinned file — and nothing ever reclaims a byte of it: a pin is kept until it is unpinned, whatever the cache is doing. A pin is per file: the torrent's other files are cached like any unpinned file, windowed while they play and given back after, except for the pieces they share with the pinned one. **Pinning is a retention property, not a location**: a pinned download is not written anywhere else, does not move, and is stored one file per piece under the store's single root exactly like a torrent that is only being streamed. `stats.json` reports `pinnedFiles` and, per file, `pinned` and `complete` (`downloaded == length`).

- **Pin**: `POST /{infoHash}/{fileIdx}/download` (`ServerHandle::pin_download`). The body is optional; `{"trackers":["udp://…"]}` — or the same array under `sources`/`announce`, so a stream's own field can be posted as is — supplies the trackers, which, as everywhere else, only matter when this request is the one that creates the engine. Idempotent. The answer is one `DownloadInfo`: `{infoHash, fileIdx, path, name, length, downloaded, complete, phase, error}`. A file the torrent does not have (or a `{fileIdx}` that is not a number) is a `404`, a disk without room a `507`, a magnet that never resolved a `504` — bodies carry `PinDownloadError::client_message` only, which names no local path.
- **List**: `GET /downloads.json` (`ServerHandle::downloads`) — the same `DownloadInfo` shape, one entry per pinned file, ordered by info hash then file index. A dormant pin (see *Restarts* below) is listed last with `phase: "error"` and an `error` explaining that its torrent is not managed right now; `path`/`length`/`downloaded` are then unknown (`null`/`0`). `ServerHandle::download_path(info_hash, file_idx)` returns just the `path` without creating anything. It is a name and not a file: torrent data is stored one file per piece, so nothing is ever written at that path and a client plays a completed download through the media routes like any other.
- **Unpin**: `DELETE /{infoHash}/{fileIdx}/download` (`ServerHandle::unpin_download`) answers `{infoHash, fileIdx, unpinned, deletedFiles}`; `unpinned: false` means nothing was pinned, and `deletedFiles` reports what actually left the disk rather than echoing the query flag (a failed delete is logged, not raised). Without `?deleteFiles=1` only the pin goes: the bytes stay where they are and the torrent becomes an ordinary, evictable one again (with nothing playing, the next reconciler tick stops the torrent and its bytes are given back like any unpinned file's). With `?deleteFiles=1` the data goes too — the whole torrent (files, session record and its now-empty folder) when this was its last pin, only the one file while other files of it stay pinned, since the torrent must keep running for them. Deleting does not require the file to have been pinned, but it does require the file to exist: with `deleteFiles`, a `{fileIdx}` the torrent does not have is a `404` exactly as it is for a pin, never a request to delete the whole torrent. A per-file delete is two deletions, because a torrent's bytes are piece files: the pieces the backend agrees to give up (`drop_pieces` forgets them and frees nothing — the indices come back so that the caller can delete them), and, if an earlier version of this server left one there, the whole-file copy at the path the backend reports. That file is truncated before it is unlinked, because librqbit holds an open handle on every file of a running torrent and an unlink alone would free no space — and it stops counting as the torrent's active file first, so the want-set re-planned around it cannot select it again and start refilling the unlinked inode (deleting the file you are watching frees the disk; the stream itself simply ends). `deletedFiles` answers for both and means bytes actually left the disk: a delete that found nothing to take says `false` rather than reading "already absent" as "freed". Note what "the whole torrent" costs: deleting the **last** pin drops files that were only ever streamed along with the pinned one, and any stream open on that torrent dies with it — the active streams are not consulted. An unpin that keeps the files leaves the bytes in place, of course. A pin whose torrent the backend does not have (see *Restarts*) still has its directory in the piece store: `deleteFiles` removes that once no file of that torrent is pinned any more, and the entry leaves `downloads.json` with the pin. The launch sweep takes every directory no pin claims, so those bytes were never immortal — but while the pin stands the sweep keeps that directory, so an explicit delete is the only thing that takes it at once. (This used to delete `<downloadsDir>/<infoHash>` instead, and so deleted nothing at all unless a `downloadsDir` was configured; asking the store answers for every pin.) **"The backend does not have it" is checked, not inferred from the engine registry**: the registry can lose an engine while the session keeps the torrent running (the TUI's delete key, the idle sweep mid-step), and a magnet add parks a hash outside both for as long as metadata takes. So a `deleteFiles` unpin that finds no engine deletes the piece directory by hand only for a hash the session has never heard of; a torrent it still holds is deleted through the session instead, which drops the torrent before its storage releases the pieces, and a hash being added right now is left alone and answers `deletedFiles: false`. Without a pin to remove there is nothing to delete either — the bytes belong to no download the server knows of, and the next launch's sweep takes them. A hash nothing ever downloaded has nothing in the store, and the answer then says `deletedFiles: false` rather than reading "already absent" as "freed".
- **Where**: **nowhere in particular, and nothing decides it.** A torrent's data is one file per piece under the store's single root (`<cacheRoot>/rqbit-downloads/.pieces/<infoHash>`), for the streaming cache and offline downloads alike, and a pin changes only what is *kept*. A torrent that was streamed first and is then pinned does not move: it is pinned where it is, the engine every open reader holds stays the engine, and `path` in the answer is the same name the backend gave the file before. There is exactly one torrent-data root and it is `settings.cacheRoot` (`POST /settings`): set to an absolute path it is checked first, then created if missing (a refused setting leaves no directory behind) and stored resolved, since the session is opened on it and everything else is spelled the way the engine reports it. The update fails if it is not a string, is relative, or cannot be created or written. **A running librqbit session cannot be moved onto a new root**, so a change takes effect at the next start; at that start the value is prepared *before* anything opens on it, and one that cannot be used any more falls back to the configured cache directory — persisted too, so `GET /settings`, the settings file and the next boot agree. `settings.downloadsDir` is gone: it was a second location for pinned downloads, there is no second-card use case, and a pin does not move a torrent, so the key named a place that never held anything. A client that still sends it gets what it gets for any unknown settings key — nothing. **What is gone with the placement**: a pin used to write its torrent to a second root of its own (`downloadsDir`) and to *relocate* one that was already managed — dropped from the session, files moved, re-added there, re-checked (`phase: checking`), the hash parked as an in-flight add for the length of a copy that could take minutes, the move detached from the request so a hangup could not strand it, both ends of it held out of the cleaner's reach meanwhile, and the per-hash pin lock handed to the move so an unpin could not delete the tree being written into. None of that exists any more, because none of it moved a byte: the store took one root, so the relocation moved a name.
- **Free space**: a pin is refused (`PinDownloadError::InsufficientSpace`) when the volume its bytes land on has less than the pinned file's missing bytes plus a **500 MiB margin** (`PIN_FREE_SPACE_MARGIN`). That volume is the piece store's root, for every torrent, and there is no longer any other candidate: nothing places a torrent anywhere, so the one volume a pin can write to is the one the store is on. (It used to probe the pin's own placement folder, where no payload byte was ever written — passing a pin onto a full store and refusing one that had all the room it needed.) Only the missing bytes are asked for, because that is all a pin writes. Re-pinning a complete file needs nothing, and a torrent still `checking` data that may already be there (right after a restart, or a stream's add the pin joined) is not measured at all — its `downloaded` reads 0 until the check ends and its want-set is already librqbit's. A whole-file download an earlier version left on disk is *not* data in place: this session neither converts nor reads it, so a fresh pin over it is measured for the whole file. A refused pin **drops the torrent it added and leaves every byte where it is**: the refusal comes before anything is downloaded, so its own add wrote nothing, while what the store holds for that hash was fetched by an earlier stream or an earlier session and stays slack, to be given back at the next pass like any other unpinned byte. A torrent the pin merely joined — a stream request's in-flight add — is not dropped at all, since a reader is about to open on it; nor is one a reader took hold of *while the pin was checking*, which is a real window (the engine is published before the check, and the check reads the file list and the stats before it ever probes free space) and one that the join count cannot see, so the live activity registers are consulted as well.
- **What bounds the cache**: **every byte under the one root has exactly one owner that knows whether anybody wants it**, and nothing walks the disk looking for victims. The root is `settings.cacheRoot`, taken from the running engine rather than from the setting, since a root set for the next start is not where the bytes are now. **The cap is the smaller of `cacheSize` and what the filesystem can give**: `cacheSize` unset is `u64::MAX`, which on a 4 GB television meant nothing bounded the cache and librqbit wrote until the filesystem refused — an ENOSPC that arrives as a fatal torrent error, mid-film. So the server reads the volume's free space (`fs4::available_space` → `statvfs`: the raw `statfs` syscall on Linux, bionic's `statvfs` on Android, both `f_frsize * f_bavail`, i.e. `df`'s "Available") and caps the cache at `occupancy + available − 512 MiB`, keeping that 512 MiB floor free. The floor is the same number `routes::stream::ensure_download_disk_ready` demands before it will stream to disk at all — below it that check asks both owners for their slack and, if the disk is still short, answers the stream `507 Insufficient Storage`. A volume that cannot be probed is not read as "full": the configured `cacheSize` then stands alone. **A cap is a statement about one volume, and there is one root, so there is one cap.** `overLimit` stays as a field of its own — how far over the cap the cache still is — because it is what a client should read before telling a user that cleaning helped. **What keeps a byte, and what takes it**: a **pin** is kept until it is unpinned, whatever else is happening; the **one stream being played** keeps the window round its playhead and the half it has committed for sharing (and any file an open reader is still delivering); **everything else is slack** — the moment a viewer opens something else, what they left is disposable, and it goes at the reconciler's next two-second tick, at the switch itself, when the volume runs low, on `POST /cache/clean`, or at the next boot. So stopping playback changes nothing on disk and resuming inside the window plays from the cache, while starting something else really does throw the previous stream away. At boot the launch sweep deletes every piece directory the embedder's pin set does not name and empties the proxy cache, before the session opens. **A stream that would not fit is refused rather than served by evicting something**: the `507` path drops every disposable byte, re-reads the volume and tries again, and if pins plus the live window still do not fit, the answer is `507` — never a pin or a playing stream taken to make room. What is nobody's is not kept: a plain-file download an earlier version of this server left under the root (there is no migration — the torrent that owns it re-downloads as pieces) is never counted, and the launch sweep (`piece_store::sweep_legacy_downloads`) removes it whole on the first start that is handed a pin set — a start handed none keeps everything on the disk, that included. The session's own records in the cache root (`session.json`, `<infoHash>.torrent`, the `.bitv` bitfields and their temp files) are not cache either, and nothing sweeps them -- nor a `pinned-downloads.json` an older build of this server left there. What `/proxy` cached is (`.proxy` under the cache root, see [Proxied remote streams](#proxied-remote-streams)): counted in the same figure, capped by the same number, and kept by the same question — is somebody inside these bytes right now? **Sizes are what a file occupies, not its apparent length**: librqbit's filesystem storage pre-allocated every file it wanted at full size, so a part-streamed film was a multi-gigabyte `length` over a handful of allocated blocks — counting `length` once reported 17 GB of cache on a phone holding 3.85 GB. The piece store this server runs on pre-allocates nothing, but the roots are still full of what earlier versions wrote and nothing migrates, so the rule stands: Unix uses `st_blocks`; Windows has no cheap equivalent through `std`, so the apparent length still stands in there. **A full disk is a signal to give slack back, not to stop**: librqbit treats a write that hits `ENOSPC` as fatal and leaves the torrent in an error state, which is how a film died ninety minutes in against a swarm of 459 seeds. **The engine stops a torrent before the filesystem has to**: the engine's free-space arm (the reconciler's, every 2 s, one `statvfs` of the volume every torrent's pieces land on) pauses a torrent that is writing (`Session::pause`: peers dropped, writes stopped, files and piece map kept — pinned torrents included, since a pin is a reason to keep the bytes, not a licence to run the disk out) the moment the volume falls under the floor, and rings the running-low bell so both owners drop their slack at once — so the torrent's readers see a buffering pause of a few seconds, not a dead stream. The reconciler restarts a torrent librqbit killed with ENOSPC once the volume is 64 MiB over the floor and somebody is playing or pinning it, at most once per 15 s. **A proxied stream stops writing at the floor too**: before each chunk a `/proxy` fill asks the volume (one `statvfs`, re-read at most every 2 s, less what it has written since), and a chunk that would take the volume under the floor is not written; the player keeps getting the body from the origin, and a later seek back into it fetches again. Its `stats.json` says `phase: "error"` with a fixed "stopped for want of disk space" message while it is stopped, and a `GET` for it is a `507`. **A film larger than the free space plays anyway**, which is what the rolling piece window is for. Torrent data is one file per piece (the piece store is the session's default storage), so reclaiming a single piece is a `remove_file`, and `enginefs/src/retention.rs` drives it from the playhead on the reconciler's two-second tick: half the cache budget is a window around where the player is, half is a set committed for sharing, and everything else of the file being streamed is given back — the backend told to forget it first, then the bytes, under one claim. **Only what is committed is announced to a peer**, so while a file is being played this server advertises no piece it is about to throw away (once the viewer moves on, the file is slack and its committed pieces go too). **The budget is published on its own timer**: `server/src/cache_budget.rs` states it from one `statvfs` and what the owners of the cache say they hold (a sum over the bits each piece store keeps, plus the proxy cache's running count — no walk, no syscall), on a minute timer, at startup before the router serves a request, and whenever a client changes `cacheSize`. `GET /cache.json` and `POST /cache/clean` are how a client reads the cache's state and asks for its slack back on demand. See [Cache usage and cleaning](#cache-usage-and-cleaning).
- **Restarts**: librqbit persists each torrent's place and want-set and, with fastresume, its verified pieces (`<cacheRoot>/rqbit-downloads/<infoHash>.bitv`), so a pinned download resumes where it was without a full re-hash; the pin set itself is the embedder's, handed in at startup (`ServerConfig::pins`) and applied to the torrents the session brought back. A pin whose torrent the session did not bring back (a record librqbit could not restore — an unparseable `.torrent`, an add that errored) is held dormant for the run, until the torrent returns — on a later boot, or with the next pin of the same torrent, which applies the dormant pins alongside its own — or until it is unpinned.

### Cache usage and cleaning

The cache bounds itself (see *What bounds the cache* above) and had no way to be asked about or triggered from outside the process — a client wanting to show cache usage, or offer a "clean now" action, had to restart the server just to make its start-up sweep fire, which stops playback. These two calls replace that:

- **Usage**: `GET /cache.json` (`ServerHandle::cache_usage`) reports a `CacheUsage` without deleting anything: `{totalBytes, limitBytes, protectedBytes, protectedFiles}`. `totalBytes` is occupancy (`st_blocks * 512` on Unix, not apparent length), and it comes from the owners that hold the bytes rather than from a walk: a sum over the bits each live piece store keeps, plus the proxy cache's running count, plus one `read_dir` of the store root for what no live store speaks for (a torrent held in Error, a previous process's leftovers), plus the staged copies of the pieces a live store is still writing, one `stat` each. `limitBytes` is the limit actually enforced in the same accounting — the smaller of `settings.cacheSize` and what the volume holding the cache can give while keeping 512 MiB free, so on a small device it is a number even when `cacheSize` is `null` (`null` only when neither caps anything: `cacheSize` unlimited *and* the volume's free space unreadable). `protectedBytes`/`protectedFiles` are what a pin keeps or the stream being played is inside — the part nothing can take. When `protectedBytes` equals `totalBytes` and the cache is still over `limitBytes`, cleaning cannot help: there is nothing disposable left until playback moves on or something is unpinned, and that is the line to give a user, not "clean failed". It costs no walk, one `read_dir` and a `stat` per piece in flight, so calling it when a "Storage" screen opens or is refreshed is cheap; it is not cached server-side, so avoid polling it on a sub-second timer.
- **Clean now**: `POST /cache/clean` (`ServerHandle::clean_cache_now`) gives back everything nobody is playing and nobody is reading, now — the same passes the reconciler's tick and a viewer opening something else run, so it can never be less careful — and answers an `EvictionReport`: `{total, protected, protectedFiles, freed, deleted, limit, overLimit}`, occupancy throughout. **It is not a choice of victims**: a pin is kept until it is unpinned and the window of the one entity being played is kept until something else is played, so on a device with one film playing and one pinned the honest answer is that nothing was freed. `freed`/`deleted` are what this call really took; `total` is what the cache occupies once it finished, which can still be over `limit`. The cap is restated before the report is built, so `limit` is the number the owners are now sized against and the one `GET /cache.json` answers. A client offering "clean now" should read the response the same way as `cache.json`'s shortfall case: if `total` is still over `limit` afterwards, say that the reachable cache is as small as it gets right now, because a pin or the stream being played is holding the rest — not that the clean failed.

Both share their functions with `ServerHandle::{cache_usage, clean_cache_now}` (`routes::cache`), token-protected control routes, absent from the LAN media listener like every other control route.

`ServerConfig::embedded()` (the `Default`) is tuned for a host process: loopback HTTP on 11470, no logging/TUI/SSDP, a generated token, and `torrent_listen_port: TorrentListenPort::Ephemeral` — librqbit's incoming BitTorrent listener takes an OS-assigned port, so any number of embedded servers (and the tests) coexist with a desktop instance. `ServerConfig::binary_default()` keeps `TorrentListenPort::Fixed(42000..42010)`: the first free port of the range, stable and forwardable. Set the field explicitly if an embedder needs a fixed port.

### Proxied remote streams

`/proxy` fetches a URL the caller names and relays it to the player, adding the headers the addon said it needs. It is how an addon stream that is not a torrent reaches the player at all, and it is open (players cannot send a bearer header). It is deliberately absent from the [LAN media listener](#lan-media-listener), because it will fetch any URL whoever reaches it names; on an embedded server (`127.0.0.1`) only this device can reach it, but the standalone binary's main listener is on every interface (see [Quick Start](#-quick-start)).

Two URL shapes, and both carry the same four parameters of the proxy's own:

| Shape | Example |
|---|---|
| Core path format | `/proxy/d=<encoded origin>&h=…&r=…&p=…/<path on the origin>` — what stremio-core builds |
| query format | `/proxy/?d=<encoded target>&h=…&r=…&p=…` — the whole target in one parameter |

| Parameter | What it does |
|---|---|
| `d=` | the target. In the Core format it is the origin, and the request's path after the segment is appended to it |
| `h=Name:Value` | a request header to send to the origin, **replacing** what the player sent under that name. Repeatable |
| `r=Name:Value` | a response header to send back to the player, **replacing** what the origin said under that name. Repeatable — `Content-Type` is the usual one, and correcting it is what the parameter exists for. `content-length`, `transfer-encoding` and `connection` are refused: they frame the response this hop is writing, which is hyper's business, and an addon saying `r=Content-Length:1` in front of a film panics the connection task in a debug build and hangs the player in a release one |
| `p=<token>` | the client's name for the player reading this stream — see [Ending a proxied stream](#ending-a-proxied-stream) |

None of the four is sent to the origin.

**Certificates are verified, and a host that fails is downgraded by name.** This route was built with verification off from its first commit — inherited from the closed-source proxy it was ported from, and nothing in the history names a host that needed it. But the client fetching a remote stream used to be mpv and is now this, so that flag stopped being about a rarely-used route and became how every remote stream is fetched. So: verify everything, and when a fetch fails *because the certificate would not verify*, retry that one host once with verification off, remember it for the life of the process, and say so at WARN by name — a stream then pays the failed handshake once rather than once per segment. Be plain about what that is worth: an on-path attacker can produce a certificate error as easily as a misconfigured CDN can, so it stops nothing it could not also trigger. What it buys is that the downgrade is per host, visible in the log, and enumerable — the blanket silence it replaced could not tell you which hosts to scope it to. A failure is read from the TLS error's *type*, never from its text (a URL ending `certificate-of-authenticity.mkv` used to be enough), and what is written down is the whole **origin** — `https://host:port` — for the endpoint that actually failed. Both halves were wrong: keyed by host alone, a broken certificate on `:8443` turned verification off for `:443` as well; and reqwest attributes a connect failure to the URL the request *started* at, so a redirect meant the host marked was the one that redirected us rather than the one whose handshake failed — an `https` → `https` chain permanently downgraded the *good* host. This route walks the redirect chain itself now (see below), so the hop that failed is simply the hop it is on, and it can ask whether *that* endpoint is already downgraded rather than paying the failed handshake again on every request.

**Redirects are followed here, not by the HTTP client**, and the reason is `h=`. reqwest's default policy strips `Authorization`, `Cookie` and `Proxy-Authorization` on any cross-host *or cross-port* redirect — which is exactly what a CDN handing off to an edge is, and how authenticated addon streams are usually served: the playlist fetched `200` and everything it named `403`, with no sign in the log that a header had been dropped. Every hop is built the same way, so every hop carries the caller's `h=` — **except that the three names reqwest calls sensitive cross a redirect only where the one credential rule below allows it**, which for a hop means: on a chain every URL of which is `https`, to an `https` target; on a chain that began at the `http://` URL a caller named, back to that one origin; and nowhere else. `redirect_target` compares no schemes, so an `https` origin answering `302 Location: http://…` could otherwise have the caller's `Authorization` or `Cookie` written onto a hop anyone on the path can read: that is the origin publishing the credential rather than the resource moving, and no `403` is avoided by obliging. A `302` sent *over* cleartext named its target in the clear as well, so whoever could read the credential could also have chosen who receives it next — which is why the only target such a hop can trust the answer about is the origin the caller named itself, the one that already holds the credential, and not the redirecting host or anywhere it nominates. Every other hop, and every hop after it that is not home to that origin, is built without those three names — **including the hops the player makes for us**, since a rewritten playlist line is the next request in the same chain (see below); the rest of `h=` still travels, and a chain that began over `https` and stepped down gets nothing back by stepping up again, an `https` target having been named over a wire that was read. `Location` resolves against the URL that sent it, a scheme other than `http`/`https` is not followed (an origin must not be able to redirect this route somewhere no caller could have named), and the chain stops after ten hops — which is also the loop detection.

Only `301`, `302`, `303`, `307` and `308` are followed, which is the set reqwest's own policy follows. Any `3xx` with a `Location` used to be, and that is what the reference does — so a `300`, a `304`, a `305` or a `306` had its `Location` fetched and served under a `200`. **`305 Use Proxy` is why this matters**: it never named a new home for the resource, it named a proxy to send the request *through*, so with `h=` re-applied per hop an origin answering `305` was choosing the host our credentials go to. Giving up reqwest's `remove_sensitive_headers` is a deliberate trade for the CDN-to-edge `403` above; what is left holding the line is that set of statuses and the hop bound. A `3xx` that is **not** followed is relayed as it stands, `Location` included — an unfollowable redirect and a headerless one used to be the same silent dead end. It is not rewritten into a `/proxy/` link (declining to follow it and then offering to is making the request with extra steps), but a *relative* one is resolved against the URL the response came from: relayed byte for byte, `Location: /elsewhere` resolves against this server and points the player back at a path we do not serve. `location` is in the CORS `expose_headers` list so a browser-hosted client can actually read it.

**Playlists are rewritten.** A `GET` whose URL — either the one the caller named or the one the body came from — ends `.m3u8`/`.m3u`, or whose content type says `mpegurl` in any capitalisation, has every URI in it rewritten to come back through this proxy, so the segments of an HLS stream are fetched the same way the playlist was. `#EXT-X-KEY` and `#EXT-X-MEDIA` are rewritten too, through the `URI="…"` attribute they name their resource in.

**The body gets a veto over the name.** A URL that ends `.m3u8` and answers `video/*`, `image/*`, or `audio/*` outside the mpegurl family is that medium, not a playlist, and is relayed untouched: a `.m3u8` URL redirecting to an MP4 was otherwise stripped of its `Content-Length`, `Content-Range`, `ETag` and `Accept-Ranges` and run through the line rewriter, which ffmpeg then failed on. The reference has the same weakness — its extension test reads the caller-named, pre-redirect path with nothing able to overrule it — and this is a deliberate divergence. A content type that says nothing (`application/octet-stream`, which is how an indifferent edge labels the playlist it serves at an extension-less URL) vetoes nothing. **Only the origin's own type vetoes**, because only the origin has seen the bytes. What a caller forces with `r=` may add the playlist verdict and never take it away: `r=Content-Type:application/x-mpegURL` — what stremio-core sends for an HLS stream — still forces the playlist path for an origin that mislabels, while `r=Content-Type:video/mp4` in front of a genuine `application/x-mpegURL` no longer suppresses the rewrite. Merging the two into one effective type let it: measured, the playlist came back verbatim and the player fetched every segment straight from the origin, without the `h=` those segments needed and without the `p=` a close is addressed by. The reference ORs its two arms for the same reason.

Each rewritten line is written in the **path format**, `/proxy/d=<origin>&h=…&p=…/<path on that origin>`, because a rewritten line has to keep a directory of its own: a media playlist named by a master one is fetched through the URL we wrote, and the player then resolves *its* relative lines against that. Under the query format they became `/proxy/seg-0.ts` and 404ed at this server's own router, so nothing multi-variant played at all.

`h=` and `p=` travel into every line — a segment needs the same authorization the playlist needed, and closing a player has to break the read actually in flight, which is a segment. **`r=` does not**: it labels the resource the caller named, and the caller named a playlist, so copying it onto the lines told the player that the MPEG-TS segments and the AES key were playlists too. Lines are resolved against **the URL the playlist came from**, redirects followed, not the one we asked for: an ordinary CDN-to-edge `302` otherwise sent every relative segment back to the redirecting host.

**One rule covers the whole of it: the `Authorization`, `Cookie` and `Proxy-Authorization` in `h=` leave the origin the caller named only over `https`.** A rewritten line *is* a hop — made by the player, written by us — so the rule has to hold on the lines as well as on the redirects, and it holds in both because both ask the same predicate about the same thing (`CredentialChain::may_carry_to`, given the chain and the target): a chain every URL of which is `https` carries the credentials to any `https` target, across hosts, which is the trade the `403` above bought; any other chain carries them to the origin the caller named in `d=`, when the caller named it in the clear, and to nobody else — at any depth, whatever the target's scheme. So a line naming `http://` where the playlist came over `https` is written without them; a playlist fetched over a chain that has stepped off `https` arms none of its lines at all, `https` lines included, having published nothing and so having nothing to spend; and a cleartext caller's playlist arms lines home to its own origin and no others, an `https` line included. That is what makes the drop above stick — without it an `https` origin naming an `http` segment had the caller's credentials written into the segment's URL and delivered to the cleartext host by the player's own next fetch, one line after the redirect guard had withheld them. Implementing the rule in the redirect loop and on the lines *separately* is how it went wrong four rounds running, each round in whichever direction had not just been tested; there is one implementation now, and an asymmetry between the two cannot be written without changing it.

A caller that names an `http://` target has spent the credential on that origin, in the clear, itself — which is why such a playlist's own segments still carry it, and why nothing else in a cleartext chain does. A line pointing back at that origin publishes nothing that is not published already, and an authenticated plain-`http` stream would lose every segment without it; a line naming anywhere else is a fresh disclosure the *origin* chose and the player makes without asking. Three earlier rounds shipped it wrong in three different directions. Keyed on the playlist's **scheme**, every `http` line in a cleartext playlist was armed: measured, a caller naming `http://A/master.m3u8` with `h=Authorization:Bearer …` got back a playlist naming `http://B/seg-0.ts`, and B logged the credential when the line was fetched the way a player fetches it — and it re-armed at every nesting level (A naming B's playlist, B naming `http://C/seg-c.ts`, C getting it), because a rewritten line is not a hop of the request that wrote it but a fresh one, so the ten-hop bound covers none of it. Keyed on the origin the playlist **came from**, a cleartext `302` moved the exception onto the redirect target: measured, `http://A/live/master.m3u8` redirecting to `http://B/edge/master.m3u8` gave B's own lines the credential and wrote the line home to `http://A/back-on-a.ts` with no `h=` at all, so every segment of an authenticated stream 403s while the secret leaks. Keyed on the **scheme again, in the other direction**, an `https` line in a cleartext playlist was armed while the loop refused the identical hop: measured, a caller naming `http://A/live/master.m3u8` with `h=Authorization:Bearer s3cret` and `h=Cookie:session=abc` got back a playlist naming `https://C/live/master.m3u8` with both written in, C logged both when the line was fetched the way a player fetches one, and C's own playlist — an `https` chain of its own by then — armed `https://D/seg.ts`, which logged them too: two hosts the caller never named, from a chain the loop would not have carried one hop of. The origin the *caller* named is every one of those halves at once, whatever a target's scheme, and it is what `d=` is written with: scheme, host **and port** — a subdomain is a different host, and a different port is a different listener that may be a different party, while the default port spelled out is neither.

Unifying the two moved one thing the other way, deliberately: a `302` **home to the origin the caller named** now carries the credentials again, where the loop used to drop them for the rest of a chain that had left that origin at all. The rewriter has armed that line since the exception was keyed on `d=` — a playlist from B may name `http://A/seg.ts` and A is asked with the credential — and a redirect to the same URL is the same request with the same recipient, already holding the same secret, so the two answering differently was the disagreement rather than the guard. An authenticated plain-`http` stream that bounces off a CDN and back home plays instead of `403`ing; every host in between is still asked with nothing.

The rewrite **streams**: lines are rewritten as the body arrives, so nothing measures the result and hyper frames the response from what it actually writes. Line endings survive per line and a body that ended without one still does. A compressed playlist is relayed unrewritten (this client decodes nothing) with a WARN saying so, and so is anything that is not the whole body — a `206` carrying a fragment, whose edge lines are cut in half. A `206` that carries the *whole* entity, which is what an origin answers the `Range: bytes=0-` a player opens with, is rewritten and answered as the `200` it has become. A `HEAD` is not rewritten — there is no body to rewrite — but it answers with the *rewrite's* headers all the same: no length and `Accept-Ranges: none`, because what a `HEAD` describes is the response a `GET` would get. Relaying the origin's framing there instead had a client size the resource at 67 bytes, ask for `Range: bytes=0-66`, and receive a `200` carrying 199.

**Proxied bytes are cached, in whole 256 KiB chunks.** This route relayed and kept nothing, which was measured rather than assumed: the same range fetched twice went to the origin twice. That was tolerable while the player kept a disk cache of its own; it stopped being tolerable once the client began routing every remote stream through here so that it would not have to, and every backward seek past the player's memory became a fresh origin fetch. So `/proxy` now keeps what it fetches, under `<cache dir>/rqbit-downloads/.proxy`, beside the piece store and inside the cache root — so every byte of it is in the one usage figure and under the one published cap, with its own retention owner reclaiming it and nothing of it pinned. A proxied stream nobody is reading is the first thing that should go. A fill never writes the volume under the 512 MiB free-space floor: the chunk that would is not kept, and the player carries on from the origin (see *What bounds the cache*).

**And it is bounded by the same retention policy a torrent is.** A proxied stream has a playhead now — written from bytes that really reached a player, on both the cached and the fetched path, and never from a `Range` header — and the policy keeps a window round it: roughly 90% ahead and 10% behind, sized from the published cap, with everything the window has left behind given back as playback moves on. So a film streamed through `/proxy` never has more than the budget of it on disk, however long it is, and a short seek back is answered from the disk instead of from the origin, which is what the 10% is for. One thing differs from the torrent's half of it and it follows from there being no swarm: nothing is committed for sharing, so the whole budget is window. Two things are the same fact seen from the other side, and both are here because there is no backend to refuse a delete the way librqbit refuses one for a piece a reader is waiting on. **The chunks under a live player's window are nobody's to take** — and where nothing is being reclaimed at all (the budget covers the response, the volume has no cap and no `cacheSize` is set) a live player is inside *all* of them, exactly as a torrent with no policy announces everything it holds and gives up none of it. **And an open read holds the bytes it has already promised**: a response is framed before its first byte goes out, so the chunks between a hit's first and last are owed to that player until they have gone out, and no pass may unlink one — a window is 90% ahead of the playhead, so a body longer than that has its own tail outside it from its first chunk onwards, and this is what stops the cache truncating a read it told the player it held. Two players inside one response are two playheads and get two windows, so neither reclaims the other's read-ahead. Nothing about a playhead is persisted or guessed: before a byte has gone out this process has no idea where anyone is in a cached response, and a response nobody is playing and nobody is reading holds nothing at all — its chunks go the moment a stream opens on something else.

**Ranges are the whole of it.** A player seeks; it does not download. The cache is asked before a socket is opened, and it answers one of three things: the range is here, and it is served off disk with the origin never learning the read happened; part of it is here, and the `Range` that goes on the wire is narrowed to the missing part, from the chunk boundary the cache ended at, with the cached head written in front of the origin's tail where the two prove to be one entity, below (the player is still answered about the whole range it asked for); or none of it is, and the fetch goes out as asked. A miss streams to the player **as it fills** — the writer sits under the same registry the body is read through, so a close or a client that vanished ends the fill at the byte it ends the read.

**A head and a tail are joined only where the origin says they are one entity.** Length and content type cannot say it — a URL whose content is replaced by content of the same size passes both — and a body spliced out of two generations is the one way this can go wrong that does not end in a read visibly breaking: the player was told a coherent `Content-Range`, and unlike the piece store there is no hash to check the bytes against, so what it gets is a file that plays and is wrong. So an entity is filed under the origin's own validator too (`ETag` when there is a strong one, `Last-Modified` otherwise), which is also what keeps two generations out of one entity directory; the narrowed fetch carries it as `If-Range`, so an origin that honours the condition answers with the whole of the new entity instead of a tail belonging to nobody's head; and the join itself happens only when the `206` that comes back names that same validator, since an origin may ignore the condition. The joined response then states that one validator and no other, because it is the only thing the join proved. A `206` about a different entity is relayed as it stands: the player asked for a wider range than it is given, reads the `Content-Range` and re-reads, which is a broken read and is logged as one — the price of narrowing against a store that never revalidates, and a price worth paying where a silent splice is not.

A response the origin will identify by **neither** validator is not kept at all, for the same reason: nothing could tell a second generation of it from the first, on the way in or on the way out.

**A hit and a miss are classified the same way.** The classification that decides whether a body is a rewritten playlist is one function, and *every* hit asks it — whole or partial — before it answers and before the fetch is narrowed against it; a hit whose verdict is "playlist" steps aside whole, and the origin is fetched unnarrowed. That is what keeps `r=` out of the key honestly, and it has had to be got right at both ends. `r=Content-Type:application/x-mpegURL` is what stremio-core sends for an HLS stream: while a full hit answered before the classification, that request was rewritten on a miss and relayed raw on a hit; while a partial hit narrowed before it, the same request came back as a raw unrewritten `206` of the tail alone, under a `Content-Range` naming a range the player never asked for. Either way the segments of the second player's stream went straight to the origin, without the `h=` they needed.

**Presence means complete, and that had to be built.** The piece store can afford a weaker claim because librqbit hash-checks every piece against the swarm's own SHA-1; a URL response has no hashes to check against. So a chunk is held in memory until it is whole, written to a temporary name and renamed into place, and a chunk file whose length disagrees with the entity it is filed under is refused at the read and deleted, never served (the lookup goes by the names in a bucket directory, one listing per thousand chunks, not a stat per chunk). A client that goes away mid-chunk leaves nothing on disk at all. Nothing here survives a restart: the launch sweep (`proxy_cache::sweep`) empties the proxy cache before the router serves anything, because a proxied entity is kept only while something plays it and a process that has served nothing is playing nothing.

**It is deliberately narrow, and this is the whole of what it does not do:** nothing here is revalidated — no `If-None-Match`, no `If-Modified-Since`, no freshness lifetime, and an entry is served until its owner reclaims it. The validator above is kept and compared, but only ever on a fetch the player had asked for anyway; a read the store answers in full asks the origin nothing, so a resource that changed under the URL is served as it was until this key next misses, and it is the fill that misses which discovers the change and drops what was held of the old entity. There is no `Vary`, no coalescing of two players filling the same chunk, and nothing kept of the chunk a fetch starts inside — the bytes before a body's first chunk boundary are dropped, since there is no completing a chunk whose front the response does not carry, while every whole chunk after it is written as usual. It also refuses, each for its own reason:

| Refused | Why |
|---|---|
| a credential in `h=` — `Authorization`, `Cookie`, `Proxy-Authorization` | `/proxy` takes no bearer token of its own, so an entry one caller's secret filled is one another caller could name by naming the same URL and the same secret. Refused outright rather than keyed carefully, which makes it structural |
| playlists | live content, and the rewrite replaces the body anyway. The verdict is the one the rewrite already reached, never a second opinion |
| anything but a `200` or a `206` | an error page, a `304` and an unfollowed redirect are not the resource |
| a content coding other than identity | the bytes are not what the framing headers a hit writes would describe |
| an origin that has not said it answers ranges | a `206` is the proof in itself; a `200` has to carry `Accept-Ranges: bytes`. Anything else and a later hit would be claiming seekability the stream loses the moment the cache misses |
| an entity whose length the origin will not state, or states as zero | there is nothing to file the chunks under, no `Content-Range` a hit could write, and no chunk in an entity of no bytes |
| an entity the origin will not identify — no `ETag`, no `Last-Modified` | nothing could ever tell a second generation of the resource from the one being stored: not on the way in, where chunks of both would land in one entity directory, and not on the way out, where a cached head would be joined to a stranger's tail |
| an entity whose length, type and validator will not make a directory name | the three of them *are* the name, and a name past what a filesystem holds fails the `mkdir` of every chunk of that response. Refused once instead of failing per chunk |
| `Cache-Control: no-store`, `private`, `no-cache`, or `max-age=0` | the first two say so outright — "do not write this down", and "this is one client's copy" in a store any caller can reach; the last two say "revalidate first", and this never revalidates, so for it they say the same thing. **This header was read nowhere in the route before the cache existed** — the rule is introduced, not inherited |
| `HEAD` and `OPTIONS` | no body to keep |
| this server's own listener | `/proxy` fetching `/stream` would put the engine's bytes in a second store under one volume's cap |

`r=` is not part of the key: it never reaches the origin, so it varies the response the player is handed and not one byte of what the origin sends, and both things it does to those bytes — the playlist verdict and the header overrides — are done to a hit exactly as to a fetch, and to a hit holding half the range exactly as to one holding all of it. `p=` is not part of it either — each player mints its own, and two players reading one stream at two offsets are exactly the case the cache exists for.

### Ending a proxied stream

A client that tears a player down has, until now, had to wait for the player's read to time out, and `network-timeout` is deliberately generous — a slow swarm must not be mistaken for a dead connection. The server can end the read instead, but only if it can be told *which* stream: it knows its streams by ids it minted, and the client cannot map those to its player.

So the client names them. It mints a token per player and puts it in the `/proxy` URL that player is given:

| URL shape | Where the token goes |
|---|---|
| query format | `/proxy/?d=<encoded target>&p=<token>` |
| Core path format | `/proxy/d=<encoded origin>&h=…&p=<token>/<path>` — inside the `d=`/`h=` segment, beside the other proxy parameters |

The token is the proxy's own parameter, like `d=`, `h=` and `r=`: it is never sent to the origin. Any string the client can generate works — it is a name, not a credential, and the call that uses it is token-protected. Every playlist this proxy rewrites carries the token into the segment URLs it writes, so an HLS player's segment fetches belong to the same token as its playlist.

Then `POST /proxy-streams/{token}/close` (`ServerHandle::close_proxy_streams`) ends every live stream carrying that token and answers `{"closed": n}`. Zero is an ordinary answer — the player may already have finished — and closing twice is harmless. It is a **control** route: bearer token on the main listener, and absent from the LAN media listener like every other control route, because the ability to cut playback is not something to hand the network.

**What it ends is the read *and the token*.** The closed stream's body yields an error, so the connection drops and the player's demuxer sees its source fail at once rather than at timeout. On its own that would not end the stream: ffmpeg runs with `reconnect=1` and re-fetches the aborted body through the very same URL, token and all — measured, three closes on one live reader gave three `{"closed": 1}` answers and three fresh origin fetches at the offsets they interrupted. So the token is retired at the same time: every later `/proxy` request carrying it is answered **`410 Gone`** — gone, not `404`, because the stream was here and was deliberately ended. The pair is what ends a stream.

That includes a request that is already in flight. `/proxy` checks the token before it opens the origin, which makes a refusal free, but the origin then takes as long as it takes: a close landing during one time-to-first-byte used to answer `{"closed": 0}` — nothing was registered yet — and the stream it did not see went on to be served in full, measured at 3.9 MB. Registering a stream and retiring a token are decided under one lock now, so a fetch in flight is either counted by the close or refused `410` when it comes back.

**Close after the player has been told to quit, not before.** A demuxer that has been cancelled never reaches its reconnect — ffmpeg checks its interrupt callback before the retry delay, before every read and inside the socket poll — so the close arrives to find nothing live and answers `{"closed": 0}`, which is the outcome to want. Closing *first* races the cancel, and losing that race provokes exactly the reconnect the refusal then has to catch: one more origin connection on the way out. A client whose teardown is a quit followed by a close never needs the refusal; it is there for the case where the quit does not arrive, or does not work.

**And what it does not end.** A demuxer wedged on something *other* than the read — a texture handoff, an audio device — is not waiting on this and is unaffected; a wedged player still costs its own teardown deadline. A player that has stopped reading altogether is not polling the body either, so it observes the close when it next reads, or when it goes away.

`ServerHandle::proxy_streams_live()` is the same registry counted: how many players are attached through `/proxy` right now.

### LAN media listener

A cast receiver is not on loopback. `ServerConfig::embedded()` binds
`127.0.0.1` only, so a Chromecast cannot fetch a byte from it — casting is
blocked before the media is even prepared. Widening that bind is not the fix:
it would put `/settings`, `/downloads.json`, the stats routes and `/create` on
the local network behind nothing but a bearer token.

So there is a **second listener** instead, and it serves media routes only.

| | |
|---|---|
| **What it exposes** | An explicit allow-list (`lan_media_routes()` in [`server/src/lib.rs`](server/src/lib.rs)), not `media_router()` itself: exactly what a cast receiver needs, which is the bytes of something this device already has. `/{infoHash}/{fileIdx}` and `/stream/…` over torrents that exist — an unknown hash is a `404` and `tr=` is ignored, where on loopback the same request would create the torrent with the caller's trackers — and the archive `/{fmt}/stream/…` routes over sessions loopback already created. `/proxy`, `/ftp`, every `/create` and the `/local-addon` stub are deliberately absent — see below |
| **What it does not** | The control router is **not mounted on it at all**, not even behind the bearer middleware. A control path there is an unknown path: `404`, never the `401` that would confirm the route exists and only a token is missing. There is no token on that listener to guess, leak or brute-force. `/proxy` and `/ftp` are likewise unmounted and answer `404` |
| **Where it binds** | `ServerConfig::lan_media_addr: Option<SocketAddr>` — `None` by default for **both** `embedded()` and `binary_default()`, so nothing changes unless an embedder asks for it. `Some(0.0.0.0:0)` lets the OS pick the port |
| **When it runs** | `ServerHandle::set_lan_media(true)` starts it, `set_lan_media(false)` stops it — meant to bracket a cast session, so the LAN surface exists only while something is casting. Nothing is bound at startup, whatever the configuration: a port already in use fails the cast that asked for the listener, never the server |
| **How it is switched off entirely** | The `lanMediaEnabled` setting (`POST /settings`, **`false` by default**). While it is false, `set_lan_media(true)` is refused; setting it back to false also stops a listener that is already running |

`ServerHandle::lan_media_base_url(peer_ip)` builds the URL to hand a receiver:
the host is the local interface that shares `peer_ip`'s subnet, taken from the
same interface enumeration `GET /network-info` answers from — on a host with a
VPN or a container bridge the first interface in the list is regularly one the
receiver cannot route back to. A listener bound to one specific address
reports that address as is. It is `None` whenever the listener is not running,
which is also the signal that no cast URL can be built yet.

**When no interface matches**, because the receiver is behind a router or
because the caller has no receiver address to give (not every platform reports
one), the candidates are *ranked* rather than taken in enumeration order: an
ordinary interface before one no receiver on a home network can be behind —
carrier links (`rmnet`, `ccmni`, `pdp_ip`), tunnels (`tun`, `utun`, `tap`,
`wg`), container and VM bridges (`docker`, `br-`, `veth`, `virbr`, `vboxnet`),
the interfaces this device hands out rather than reaches a LAN through (`ap0`,
`p2p`, `rndis`) and `dummy`, all matched by name — and, within each, an RFC1918
address before anything else. The bridges are why the address shape cannot
decide this on its own: `docker0`'s `172.17.0.1` is as private as the Wi-Fi
address beside it, and only the name tells them apart. An interface the kernel
reported no netmask for is matched against no subnet at all, since a `0.0.0.0`
mask matches every peer and would win outright over the interface that really
shares one. A phone is on Wi-Fi and cellular at once and
`getifaddrs` will happily list the cellular interface first; naming that
address to a Chromecast is a cast that hangs forever, because a TCP connect to
an unroutable host does not fail, it waits. The demotion is only ever a
tie-break: a matching subnet still wins outright, and a host whose one
routable address is cellular is still offered it rather than nothing.

**A cast that never starts leaves no other trace**, which is why this
listener reports on itself. Every answer `lan_media_base_url` gives is logged
at INFO — the peer, the interface picked and the URL — as is each of the ways
it can answer `None`, and so is every request that reaches the listener
(method, path, peer). `ServerHandle::lan_media_requests_served()` is the same
arrival count as a number: it starts at zero on every start — whether or not
a listener was already running, since starting a cast to a second receiver
mid-session asks about that cast and not the one before it — counts each
request the listener receives (a `404` from the fallbacks included — the
receiver still got here), never carries over from the previous session, and
is back to zero once the listener has been stopped.
A receiver told an address it cannot route to reports no error at all, because
a TCP connect to an unroutable host hangs rather than failing; from the sofa
that is indistinguishable from buffering. A count still at zero well after a
load is what tells the two apart, and it is worth different words: nothing
reached this device, so the address was wrong — as opposed to a non-zero count,
where the receiver fetched the stream and the problem is the media. The
addresses in those log lines are private ones on the user's own LAN, and the
one thing that is secret, the bearer token, is never on this listener at all.

**Stopping closes the door, not the connections already through it.**
`set_lan_media(false)` aborts the serving task and awaits it, so by the time
the call returns the listener socket is closed and the port is free (it
rebinds immediately): nothing new is accepted, and a connection idling on
keep-alive is closed without serving another request. A response that is
*already* streaming is **not** cut — axum spawns each accepted connection into
its own task, and dropping the serve future asks those to shut down
gracefully, which finishes the response in flight — so a receiver mid-file
keeps being fed until it has the whole thing or hangs up. The call is still
not a drain, which is the point: it returns at once rather than waiting out a
movie-length response, so ending a cast session or revoking `lanMediaEnabled`
never blocks. But it is not a kill switch for bytes already on the wire, and
the server has none; stopping the LAN listener stops new fetches. The loopback
listener owns a different socket and a different serve future; it and every
request in flight on it are untouched.

**The trade-off, stated plainly.** While the listener is up, *anyone* on the
same network can fetch media from this server: the media routes are open by
design (players cannot attach headers), so there is no authentication on that
port at all. Anyone who can guess or observe an info hash can pull that file
out of the piece cache. That is why it is off by default, why it is meant to
be held open only for the length of a cast session, and why `lanMediaEnabled`
exists as an operator veto that no embedder call can override.

**Nothing a stranger could make this device *do* travels with it.** The test
of a route belonging on the LAN is that it serves bytes the loopback side has
already arranged and cannot be made to arrange anything. `/proxy` and `/ftp`
fail it outright: both fetch an arbitrary caller-supplied remote URL rather
than media bytes from this server — `/proxy` over HTTP(S), `/ftp` through a
spawned `curl` for FTP/FTPS only — which makes either an open proxy
for whoever can reach it. So do the archive `/create` routes, which
download an archive from a caller-named URL, and the loopback stream route's first request for
an info hash, which starts a torrent with the caller's trackers on this
device's disk and connection; the LAN's stream route only looks a hash up.
That is acceptable on an embedded server's loopback listener, where only this
device can reach it, but not on a listener the whole LAN can reach, so neither
is on the LAN allow-list. (The standalone binary's main listener is on
`0.0.0.0`, so there they are reachable from the LAN anyway — see
[Quick Start](#-quick-start).) The consequence is deliberate, not an
oversight: a stream stremio-core plays *through* `/proxy` — an addon stream
that needs custom request headers, which a player cannot attach itself —
cannot be cast directly while the LAN media listener is the source, because
the receiver would need to fetch it from the LAN listener and that route
simply is not there. Casting that stream needs another path (e.g. the client
resolving it through the loopback listener itself, or an addon that hands out
a header-free URL); the server does not paper over the gap by widening the
LAN surface.

CORS is set up for what a receiver needs: `Content-Type`, `Accept-Encoding`
and `Range` are named allowed request headers (Google's Web Receiver CORS
requirements ask for exactly those, and even a plain MP4 needs CORS once
tracks are involved), and `Accept-Ranges`, `Content-Range` and
`Content-Length` are exposed to script so a player can seek (`Location` too, for
the `3xx` `/proxy` relays without following). Byte-range
requests and `HEAD` work on this listener exactly as they do on loopback.

### Removed routes

Everything below existed for server.js compatibility and had no consumer in stremio-core, the Flutter client or the tests; it was removed to shrink the attack surface to what is actually used:

- `/` (redirect to web.stremio.com), `/favicon.ico`, `/thumb.jpg`, `/samples/{filename}` — desktop/web-UI leftovers.
- `/list`, `/removeAll`, `/{infoHash}/remove`, `/{infoHash}/peers`, the `GET` variants of `/create` and `/{infoHash}/create` — engine management nothing called (stremio-core POSTs).
- `/diagnostics/*` — local debugging endpoints; the memory sampler still logs its snapshot.
- All subtitles routes (`/subtitles.vtt`, `/subtitles.{ext}`, `/{infoHash}/{fileIdx}/subtitles.vtt`, `/opensubHash`, `/opensubHash/{infoHash}/{fileIdx}`, `/subtitlesTracks`) and the engine code behind them — the client fetches addon subtitles and selects tracks itself.
- `/update/*` and the self-update manager plus the `stream-server-updater` helper binary — desktop baggage.
- `/{ipc_key}/downloader/*` — stubs for an HTTP downloader that was never implemented.
- `/local-addon/*` — the local-files Stremio addon (scanned `localFiles/` directory, catalogs, `bt:`/`local:` metas). **A stub remains** (see the table above), because stremio-core's `OFFICIAL_ADDONS` carries a *protected* descriptor for `http://127.0.0.1:11470/local-addon/manifest.json` with a `stream` resource for `tt` movies/series: a stock profile requests `/local-addon/stream/{type}/{id}.json` on every details page, and a `404` there shows up as an error group in the client and an ERROR-level unhandled-request log line each time. A profile synced from a Stremio account carries an older descriptor for the same addon that *also* declares an `other`/`local` catalog, so core requests `/local-addon/catalog/other/local.json` (and `/local-addon/catalog/other/local/{extra}.json` once the board pages or a filter is applied — the two shapes `AddonHTTPTransport::resource` builds); a `404` there broke the catalog row and logged an ERROR on every refresh. The stub answers an empty manifest, `{"streams": []}` and `{"metas": []}` instead and serves no local files; `meta` (only ever asked for `local:`/`bt:` ids) is a quiet `404`, and so is every other path under the prefix — the stub has its own fallback so a 404 it *intends* is logged at debug, not through the ERROR-level unhandled-request path. The served manifest still declares no catalogs, so a profile that does not already carry one gains no empty row.
- `/casting/transcode`, `/casting/convert`, `GET /casting/{devID}` and the `501` stubs for `/ftp/create*` and `/ftp/stream*`.

**Usenet (NZB)**: `/nzb/create*` and `/nzb/stream*` are gone, with the NNTP client and yEnc decoder behind them. stremio-core still builds `/nzb/create` for an addon's `nzbUrl` streams; no route here serves it, and as a two-segment path it lands in the `/{infoHash}/{fileIdx}` stream route, which tries to add `nzb` as an info hash and answers with an error, never media. The feature never worked end to end: article bodies were read as UTF-8 text, so binary data did not survive, and the connection pool was never refilled. Its routes were also open, fetching a caller-named URL and opening connections to caller-named news servers.

**YouTube**: stremio-core builds `/yt/{id}` URLs for `StreamSource::YouTube` when a streaming server is configured; this server has no `/yt` route, so YouTube-via-server is unsupported and a client has to open YouTube streams itself. `/yt/{id}` is two segments as well, so it too lands in the stream route and gets an error, not media.

---

## 🔧 Build Instructions

All you need on any platform is Rust via [rustup](https://rustup.rs) — `rust-toolchain.toml` pins the exact toolchain (1.98.0) and rustup installs it automatically on first build. No platform has any extra system packages to install; the steps below are the same everywhere:

```bash
cargo build --release
```

<details>
<summary><b>🐧 Arch Linux</b></summary>

```bash
sudo pacman -S rustup
rustup default stable
cargo build --release
```

</details>

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🐧 Fedora / RHEL</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🍎 macOS</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🪟 Windows</b></summary>

```powershell
# Install Rust from https://rustup.rs — that's it, no other tooling needed.
cargo build --release
```

</details>

---

## 📁 Project Structure

```
stream-server/
├── server/           # HTTP server (media + token-protected control routers), embeddable library
│   ├── src/auth.rs   # ServerAuth + the bearer middleware
│   └── src/archives/ # ZIP/7Z/TAR (always on) + RAR (default-on "rar" feature), all pure Rust
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       └── librqbit.rs   # The sole torrent backend (pure Rust)
└── stremio-runtime-stub/ # Legacy-compatible launcher shim
```

There is no `bindings/` directory and no vcpkg apparatus: the optional C++ `libtorrent` backend and everything it needed to build (the `libtorrent-sys` FFI crate, `triplets/`, `vcpkg-overlays/`, `vcpkg.json`) have been removed. RAR is handled by the pure-Rust `unrar-rs` crate — a direct `server` dependency behind the default-on `rar` feature — so there is no separate RAR binding crate either.

---

## Upgrade notes

- Offline downloads add one `<infoHash>.bitv` per torrent next to the session state in `<cache dir>/rqbit-downloads` (fastresume bitfields — the first start after upgrading still hash-checks each torrent once, after which restarts skip it). The pin set itself is not written here at all: the embedder keeps it and hands it in at startup. Nothing reclaims either: the launch sweep works inside `.pieces` and never touches the session's records beside it, and a `pinned-downloads.json` an older build left is kept where it lies rather than swept as a previous release's data. `settings.downloadsDir` is gone; the one torrent-data root is `settings.cacheRoot`, which now decides where the session opens (see [Offline downloads](#offline-downloads)). A client that still sends the old key gets what any unknown settings key gets — nothing — and the key is dropped from `settings.json` on the next save.
- **Torrent data is stored one file per piece**, under `<cache dir>/rqbit-downloads/.pieces/<infoHash>/`, for the streaming cache and offline downloads alike: whole `.mkv` files are not produced any more. **There is no migration, and the old bytes are deleted, not converted.** The torrent that owns a download written by an earlier version comes up with nothing on its first start and downloads again as pieces. The old file sits beside the piece store, not under it, so no owner speaks for it and no usage figure ever includes it — and **the launch sweep removes it whole** (`piece_store::sweep_legacy_downloads`) on the first start that is handed a pin set, because a byte nobody counts and nothing deletes is how a disk fills once and never empties. A start handed no pin set sweeps nothing, that included. Delete that directory by hand if you want the space back, or unpin the download with `deleteFiles=1`, which is the one call that still knows the path. Nothing breaks, no path a client already holds stops resolving, and `DownloadInfo::path` keeps its shape — but it names a file that will not appear, so a client that opened it directly must go through the media routes instead.
- The DHT bootstrap address cache is a new file, `<cache dir>/rqbit-downloads/dht-bootstrap.json`, written next to `dht.json`. It holds only the IP addresses the bootstrap host names last resolved to, is rewritten whenever they resolve, and is safe to delete — it is a fallback for a network whose DNS is broken, not state anything depends on.
- Existing desktop installs: the librqbit DHT routing table is now stored at `<cache dir>/dht.json` (it previously lived under the XDG/`directories` project dir, which does not exist on Android). The old file is simply ignored and the DHT re-bootstraps once on the first start after upgrading — a one-time, self-healing cost.

---

## 📄 License

**The source in this repository is MIT** — see [LICENSE](LICENSE). It contains no GPL code; the `LICENSE` file is unchanged and stays MIT.

**The stream-server binaries this project distributes are under the GNU GPL, version 3 or later** — see [LICENSE-GPL-3.0](LICENSE-GPL-3.0). RAR streaming is on by default and is powered by the [`unrar-rs`](https://crates.io/crates/unrar-rs) crate, which is licensed **GPL-3.0-or-later**. That crate is fetched and linked only at build time, but linking it means a **default-built binary of stream-server, with RAR support, is distributed under GPL-3.0-or-later** — every release download, the `.deb`, the `.msi`, the AppImage and the Arch package included. This is a deliberate choice: RAR support is wanted on by default, and the project is released openly.

Each package carries both texts: the GPL, which the binary is distributed under, and the MIT notice, which the source it is built from carries (`/usr/share/doc/server/` in the `.deb`, `/usr/share/licenses/stream-server/` in the Arch package, `/usr/share/doc/stream-server/` in the AppImage, the install folder for the `.msi`). The release page lists both as `LICENSE-GPL-3.0.txt` and `LICENSE-MIT.txt`, for the portable binaries. The `stremio-runtime` stub links no GPL code and is MIT.

To produce an **MIT-licensed binary with no GPL code**, build without the `rar` feature (RAR requests then return a 501 JSON error):

```bash
cargo build --release --no-default-features
```

MIT is GPL-compatible, so shipping the MIT source alongside GPL default binaries is fine; the GPL obligation attaches to the compiled/distributed default binary, not to this repository's source.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>

## Keywords

`pure rust torrent streaming` `headless torrent server` `librqbit` `rust torrent` `video streaming server` `http range streaming` `torrent to http` `archive streaming` `enginefs` `no ffmpeg`
