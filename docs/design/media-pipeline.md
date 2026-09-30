# Media pipeline: fetchers, their caches, translation, and three ways out

Design, 2026-09-30. **Proposed; nothing here is built yet.** Written
against stream-server `95bd0c0`, xtremio `22814b1`, the stremio-core fork
`f2cc08bf9`, and the xtremio spike branch `spike/mpv-stream-cb` (`13206ba`,
`0d2c187`). Where this and the code disagree once steps land, the code is the
answer, as for every note in this directory.

Most of this pipeline exists. Bytes come from a **fetcher** -- a torrent
through librqbit, an HTTP origin (a debrid link, an addon's CDN), a Google
Drive file, and, once step B lands, a local file. A fetcher that should
keep what it fetched has its **own disk cache** inside it -- the piece store
under a torrent, the proxy chunk store under HTTP and Drive -- and that
cache is where **pins** (offline downloads) live. Between a fetched file
and whatever reads it sits **translation** (`docs/design/translated-sources.md`):
a member of a ZIP, RAR, 7z, TAR or disc image is ranges of its container, a
`MemberView`, itself a `ByteSource`, so containers nest. What reads the result
is one of three **outputs**: mpv in this process, a cast receiver on the
LAN, and -- step F -- a transcoded rendition for a receiver that cannot
decode the original.

What does not exist is a name for "a thing the app plays" that is not a URL.
Every output today reaches the bytes through an HTTP route whose shape
encodes what the bytes are, and the app rewrites those URLs (`p=`,
`buffer=`, `/proxy/d=`) to steer the server. This note replaces the URL
with an opaque id handed out by one registration call, puts mpv on a
custom libmpv protocol that reads through that id in-process, puts the
cast receiver on one route keyed by a published token, and says what is
deleted as each lands.

## 1. What each case does today

| Case | Played on this device | Cast |
|---|---|---|
| Torrent file | mpv fetches `{base}/{hash}/{idx}?tr=..&p=..&buffer=..` over loopback HTTP; `routes::stream::stream_video_with` (`routes/stream.rs:979-1338`) registers the play and opens the file shared. | The same URL rebuilt on the LAN base with `p=` kept; served by `lan_stream_routes()` (`EngineAccess::ExistingOnly`). Works. |
| HTTP origin that ranges | The app wraps it: `proxiedThroughServer` (`lib/core/stream_proxy.dart:93`) writes `{base}/proxy/d=<origin>&p=<token><path>`; `/proxy` relays through the proxy cache. | The `/proxy` URL rebuilt on the LAN base: **`404`**, because `/proxy` is not on the LAN (it fetches a caller-named URL). |
| HTTP origin that will not range (HLS playlist, live TV) | Same `/proxy` wrapping; the route answers what the origin answers. | Same `404`. |
| Google Drive file | `ServerHandle::open_drive_file` (the grant per call) answers `{key, url, name, content_type, length}`; mpv fetches `/drive/stream/{key}`. | `/drive/stream` is not on the LAN: **`404`**. |
| Pinned torrent download | `{base}/{hash}/{idx}`, served off the piece store. | As a torrent file. Works. |
| Pinned URL or Drive download | `/downloads/{key}/stream` (`routes/downloads.rs:614`), off `Entry::look_up` when complete, through a `ProxySource` when not (a Drive download mid-fill answers `409`). | Not on the LAN: **`404`**. |
| Addon-declared archive (`rarUrls`, `zipUrls`, ...) | stremio-core writes `{base}/rar/create?lz=..`; a `GET` indexes and redirects to `/rar/stream/{key}/{member}`. | The `/create` URL is what the player holds; rebuilt on the LAN base it matches `/{infoHash}/{fileIdx}` and the LAN stream handler answers **`404`**. |
| Archive the app sniffed (a debrid `.rar`, a torrent whose file is a `.zip` or `.iso`) | mpv fails, `archive_sniff.dart` reads the first `0x8006` bytes, `archive_route.dart` posts `/{fmt}/create` (or the `torrent:` key form) and replays on the member URL. | The member URL is on `archive_stream_routes()`, which the LAN mounts. Works, sharing nothing. |
| Local file (SAF, Downloads folder) | Not offered by the server. | -- |

So four casts `404` today (URL, Drive, URL/Drive download, addon-declared
archive), and the app's watchdog cannot tell: `LanMedia::record_request`
counts every request that reaches the listener, **including the ones answered
`404`** -- on purpose, because what the count exists to say is whether the
receiver could reach this device at all, and `embed.rs`
`the_lan_listener_counts_the_requests_that_reach_it` pins it. The app's
`castFetchTimeout` (20 s, `player_screen.dart:204`) reads a non-zero count
as "the network is fine, the problem is the media", and says nothing to the
viewer.

Two more facts the rest of this note leans on:

* **There is no remote streaming server any more.** `core::pin_to_embedded`
  (xtremio `rust/src/core.rs:326`) rewrites whatever the profile holds to
  the embedded server's URL at every launch and after every settings reset.
  Leftovers of that case in the app (`isEmbeddedServer` in `_castUrl`, the
  "configured a streaming server elsewhere" paragraph of `proxiedThroughServer`)
  are being removed in a separate PR; nothing below keeps a path for it.
* **Archive playback shares nothing, by rule, and the rule is enforced twice.**
  The stream route decides by name (`played_through_a_translator`,
  `routes/stream.rs:404`, which makes the play session
  `Played::Torrent { shares: false }`), and the retention owner decides again
  by content (`Backing::content_shares`, over
  `enginefs::retention::sniff::is_archive`) before any draw; a test pins the
  second (`an_archive_told_by_its_content_shares_nothing`). On top of that
  the member's reads go through `TorrentFileSource`, which opens every reader
  with `try_get_file_unshared` and carries no token. Section 2.8 is what
  changes.

## 2. The abstraction

### 2.1 The layers, named

```
 fetchers            caches (inside)          translation            outputs
 ─────────           ───────────────          ───────────            ───────
 torrent  ────────── piece store (.pieces) ─┐
 HTTP     ────────── proxy cache (.proxy)  ─┼── MemberView ──┬── mpv, in process   (xtremio://<id>)
 Drive    ────────── proxy cache (.proxy)  ─┤   (nests)      ├── cast receiver     (/cast/<token>)
 local    ────────── none                  ─┘                └── rendition (F)     (/cast/<token>/hls/...)
                     └── pins live here
```

**The caches stay inside their fetchers** (zond). The coupling is tight --
the piece store *is* librqbit's storage, the proxy cache is keyed by what
the origin was asked -- and a local file needs none. Nothing is gained by a
cache layer that all four would have to be adapted to, and a pin is a
property of the cache that holds the bytes, so pins are exposed by the
cache layer (2.6), one call dispatching to the two pin paths there are.

Everything from the fetchers to `MemberView` exists and does not change:
`ByteSource`, `SeekableReader` and `ReadHint` (`sources/mod.rs:66-178`),
`MemberView` (`sources/view.rs`), `Translator` and `Index`
(`translators/mod.rs`), `Sessions<T>` and `Lease` (`translators/session.rs`),
`ProxySource` and `DriveSource`. What is new is one torrent source (2.2),
one local source, a registry of ids over all of them (2.3), a reader task
that lets a blocking thread read any of them (2.4), and the two outputs'
front doors (2.5, 2.7).

### 2.2 One torrent source, with an optional player token

`TorrentFileSource` (`sources/torrent.rs`) is replaced by one source whose
open is the stream route's open, factored out:

```rust
/// One file of one torrent. With a token its reads are the viewer's
/// playback; without one they are an aside (a subtitle, an index read).
pub struct TorrentSource {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    len: u64,
    name: String,
    play: Option<Play>,
}

pub struct Play {
    token: PlayerToken,          // `<viewer>.<screen>`, as `p=` carries it today
    buffer: BufferProfile,       // what `buffer=` carries today; settable live (2.5)
    shares: bool,                // false for a container file, until 2.8
}
```

With a play, an open does exactly what `stream_video_with` does between
resolving the file and building the body, in the same order and for the
same reasons (each is commented there): `note_player` moves the play
session; `StreamLifecycleGuard::start` registers the stream and the
liveness cell (`on_stream_start_unreconciled`, which reaches `switch_to`)
with no await between the registration and the guard; the disk gate
(`ensure_disk_ready_or_refuse`); `focus_torrent`; and the shared reader
(`try_get_file_with_intent`) for the current screen of a file that shares,
`try_get_file_unshared` for everything else. The guard's drop is
`on_stream_end`. Without a play it is today's aside: registration through
`TorrentMemberStream`, `try_get_file_unshared`, no session touched.

The route keeps its HTTP framing and calls the factored open; the reader
task (2.4) calls the same function. One open, two callers, and a test that
the route's existing suite passes unchanged across the refactor, as the
proxy route's did when `ProxySource` was cut out of it.

**Registration is per reader, the file handle is per position.** A seek
drops the `FileHandle` and opens another at the new offset rather than
seeking the old one: the engine takes the open's offset as where the read
is about to be and prioritises the swarm around it, and the lookahead is
computed at open (the comment on `TorrentFileSource::open` says the same
thing about the offset being told twice). That is what an HTTP player's
seek already is -- a new request, a new open -- so nothing about the
engine's view of a seek changes. The stream registration and the play
session are held by the reader, not the handle, so a seek does not move
the session or end the stream.

`MemberView` reads its container through this source, so a member opened
for playback carries the viewer's play into the container's file. What
that does to sharing is 2.8.

### 2.3 Ids: registration and resolution

**The app names a playable thing by an opaque id the server issued.** Not
a path, not a URL, not anything a string from outside could forge:

* **Opaque and random**: 128 bits from `getrandom` (as the auth token is,
  `auth.rs:48`), hex. Nothing in an id says what it names; a `local:` path
  can only be registered over FFI, and no HTTP route resolves an id, so a
  playlist an addon serves cannot name one, and a file on this device is
  never reachable by guessing its path.
* **Per process**: the registry is memory. A restart invalidates every id,
  which costs nothing, because the app registers at open.
* **Cleared** when evicted: the registry is a `Sessions<T>`-shaped map with a
  cap and least-recently-used eviction of entries nobody holds, like the
  Drive opens (`state.drive_files`) and archive sessions already are. An open
  reader or a publish is a lease; an id with neither is evictable. There is
  no clock.

Two calls, because they cost differently:

```rust
/// What the app hands the server to play. Parsed, never fetched.
pub enum MediaSpec {
    /// A URL stremio-core built (`streaming_url`): a torrent, `/proxy`,
    /// an archive `/create`, `/ftp`. Parsed by the routes' own parsers.
    StreamingUrl(Url),
    /// A Google Drive file, and where its grant comes from (the server
    /// keeps no refresh token; xtremio's `ServerState::drive_grant` does).
    Drive { file_id: String, name: Option<String>, grant: GrantSupplier },
    /// A file on this device (step B).
    Local(LocalFile),
}

pub enum LocalFile { Path(PathBuf), Fd(OwnedFd) }
pub type GrantSupplier = Arc<dyn Fn() -> Option<String> + Send + Sync>;

pub struct MediaId(String);

/// What resolution found: enough to show, open, cast and pin.
pub struct Resolved {
    pub name: String,
    pub content_type: String,
    pub len: u64,
    /// The member a container resolved to, when it was one.
    pub member: Option<MemberInfo>,
    /// `false` only for an HTTP origin that will not range (2.5).
    pub in_process: bool,
}
```

`register(spec) -> MediaId` does no I/O: it parses and records. `resolve(id)
-> Result<Resolved, Refusal>` does the work -- adds or finds the torrent,
probes the origin (`ProxySource::open`'s `bytes=0-0`), refreshes the Drive
grant, reads the head, and **sniffs for a container** (2.9) -- and caches
its answer on the entry. The brief asked registration to return `{id, name,
content_type}`; that is `resolve`'s answer instead, because a magnet's name
is not known until its metadata is, which can be ninety seconds, and the
app wants the id before it knows whether to wait. `ByteSource` still carries
no name or MIME; `Resolved` is where they live.

**The core's URL is parsed in Rust, by the parsers the routes already use**
(`PlaybackQuery::parse`, `compat::resolve_file_idx` with `-1` and `f=`, the
proxy's `d=`/`h=`/`r=` reading, the archive `lz` decode, the FTP target).
`stream_numbers.rs` already dispatches on a URL's shape through those same
parsers. So stremio-core stays the one place the *building* rules live --
the largest-file `-1`, `proxyHeaders`, FTP, archive payloads -- and this
server keeps the one place they are *read*. The core never sees what mpv
opens: its player model keys on the stream JSON, not on the URL.

### 2.4 The reader task: a blocking reader over async sources

mpv's `stream_cb` callbacks (and later JNI) are blocking calls on foreign
threads. They never poll a future themselves. **One runtime task per open
reader owns the reader**, and the foreign thread talks to it over a channel.
Three reasons, each from the code:

1. Dropping a reader spawns: `TorrentMemberStream::drop`
   (`sources/torrent.rs`) and `StreamLifecycleGuard::notify_end`
   (`routes/stream.rs:216`) both call `tokio::spawn`, which panics off a
   runtime. A reader dropped on mpv's thread would do exactly that.
2. A read parked on a missing piece has no cancellation today. HTTP relies
   on the socket closing, which drops the body future. mpv's `cancel_fn` runs
   on another thread *while* a read blocks, "should not block", and cancels
   "any current or future read and seek operations" (`stream_cb.h`), so the
   cancel has to overtake the read it interrupts.
3. The runtime can go away under a reader (the app stops the server); that
   has to be an error on the foreign side, not a hang.

The protocol, all of it:

```rust
enum Command {
    /// Up to `max` bytes from the current position; the position advances
    /// by what is returned. `Ok(empty)` is end of file.
    Read { max: usize, reply: oneshot::Sender<io::Result<Bytes>> },
    /// Move to `offset`; answers the offset it is at.
    Seek { offset: u64, reply: oneshot::Sender<io::Result<u64>> },
}

/// What the foreign side holds (mpv's cookie).
pub struct MediaReader {
    commands: mpsc::Sender<Command>,     // bounded(1): one call in flight
    cancel: CancellationToken,           // tokio_util, already a dependency
    len: u64,
    runtime: tokio::runtime::Handle,
}
```

* **Read** is sequential: mpv's `read_fn(cookie, buf, n)` sends `Read { max:
  n }` and blocks on the reply (`blocking_recv`), then copies into mpv's
  buffer. One copy per read; the spike measured the overhead as nothing
  worth counting against a local file. The task answers with one `read()`
  of its reader -- whatever has arrived, at least one byte, not a filled
  buffer -- so a slow torrent delivers as it arrives, as the HTTP body does.
* **Seek** is `seek_fn`. The task reopens its source at the offset (2.2) --
  for every source, not only torrents, so there is one rule -- except that a
  seek to the position it is already at is answered without touching the
  reader, since mpv seeks to 0 right after every open to learn whether the
  stream is seekable.
* **Cancel** is not a queued message, because a queued message waits
  behind the read it is meant to interrupt. `cancel_fn` calls
  `cancel.cancel()`, which does not block; the task `select!`s every read,
  seek and reopen against `cancel.cancelled()` and answers the one in
  flight with `ErrorKind::Interrupted`. It is sticky, as mpv's contract
  says: every later command is answered with the same error at once.
  **This is what wakes a read parked on a missing piece**: the task stops
  awaiting the piece, and dropping that future is exactly what a closed
  socket does to an HTTP body today.
* **Close** is `close_fn`, which drops `commands`. The task sees the channel
  end, drops its reader *on the runtime* -- so the drops that spawn, spawn
  where they can -- and exits. `close_fn` does not wait for that; nothing
  after it needs the stream's end to have been recorded, and the HTTP path
  does not wait for it either.

**Who owns what.** The task owns the reader and everything a reader holds:
the `FileHandle`, the stream registration and play guard, a session lease,
a proxy-streams registration. The foreign side owns a sender, a token and a
number. Neither holds `Arc<ServerHandle>`: xtremio's `stop_in` joins the
server by spinning on `Arc::try_unwrap` (`sole`, `rust/src/server.rs:373`),
so a reader holding the handle would hold up the stop for as long as mpv
held the stream. The task holds the pieces of `AppState` it needs and the
runtime `Handle`, as `translators::IndexReader` (the precedent for a blocking
reader over a `ByteSource`) and `ServerHandle::block_on_server`
(`lib.rs:1097`) do.

**When the runtime shuts down**, its tasks are dropped: the reader goes
with its task, the reply sender with it, and the foreign `blocking_recv`
returns an error, which `read_fn` answers as `-1` (`MPV_ERROR_GENERIC` for a
seek). A command sent after the shutdown finds the channel closed and
answers the same. No foreign thread waits on a runtime that is not there.

`open_reader(id, play: Option<PlayToken>) -> Result<MediaReader, Refusal>` is
the call that makes one. It resolves first if the id has not been.

### 2.5 mpv on `xtremio://<id>`

xtremio registers a custom protocol on media_kit's `mpv_handle` with
libmpv's `mpv_stream_cb_add_ro`, and mpv opens `xtremio://<id>`; the
callbacks are 2.4's. The struct layout is `stream_cb.h`'s
(`mpv_stream_cb_info`: `cookie`, `read_fn`, `seek_fn`, `size_fn`,
`close_fn`, `cancel_fn`, the last since API 1.106). The spike
(`spike/mpv-stream-cb`, `rust/src/api/mpv_stream.rs`) played a file this
way, and seeks mapped one to one.

**media_kit needs one small vendored patch.** `Player.open` loads through a
playlist, and mpv refuses a `stream_cb` protocol from a playlist as an
unsafe origin. media_kit already has the escape for `fd://` on Android: when
any item is `fd://` it sends each item as its own `loadfile`
(`media_kit-1.2.6` `lib/src/player/native/player/real.dart:181`). The patch
widens that condition to our scheme. The spike's second commit shows the
bare `loadfile` plays; the patch keeps media_kit's playlist bookkeeping
around it. **Not** `load-unsafe-playlists=yes`: it is global, and it would let
an addon's m3u name `fd://` or `lavf://`.

What moves from the URL to the id, in the app:

* `p=` becomes the `play` argument of `open_reader`, `buffer=` becomes a
  call on the open reader (`set_buffer(id, profile)`), which the task
  applies to the next open -- so **a buffer change no longer reopens mpv**
  (`_reopenForBuffer`, `player_screen_open.dart`, goes).
* `force-seekable` is decided per id: every in-process source answers any
  offset, so it is forced for every `xtremio://` stream and for none of the
  `/proxy` leftovers below (`MediaKitEngine.forcesSeekable` today decides
  by the URL's host and path).
* `stream_numbers(url)`, `note_duration(info_hash, file_idx, filters, ..)`,
  `note_player_opened(info_hash)`, `note_player_stalled(info_hash)` and
  `close_proxy_streams(token)` take an id. The server knows what the id is;
  the app stops reconstructing it from a URL.

**The one HTTP path left in process is an origin that will not range.**
`ProxySource::open` probes `bytes=0-0`, and a `200` is
`ProxySourceError::WillNotRange` (`sources/proxy.rs:296`). Such a stream --
an HLS playlist, a live channel -- is not a `ByteSource` and never will be;
`resolve` answers `in_process: false` with the `/proxy` URL, and mpv reads
that as today. That is the only reason `proxiedThroughServer` survives step
A', and it survives only as "the URL `resolve` handed back".

**A risk this changes.** A read over HTTP is bounded by mpv's
`network-timeout` and, past that, by the app's false-end re-open. A
`stream_cb` read is not a network read to mpv and has no timeout: it blocks
until the bytes come or the stream is cancelled. A torrent that stalls
therefore parks the demuxer until the viewer quits or seeks (both cancel),
which is what the stall overlay already shows; but any app logic that
counts on `network-timeout` firing must be moved onto the server's own
stall signal before A' ships.

### 2.6 Pins

`pin(id)` and `unpin(id, delete_files)` dispatch to the two pin paths there
are: `pin_download(info_hash, file_idx, trackers)` for a torrent, and
`pin_proxy_download(ProxyDownloadRequest)` for a URL or a Drive file (whose
grant comes from the id's supplier). A member of a container pins its
container's files (all volumes of a set). A local id refuses: there is
nothing to download.

**One pin set, not two config fields.** Today the embedder hands in
`ServerConfig::pins` (torrents) and `ServerConfig::proxy_pins` (URL and
Drive keys) separately, and keeps two kinds of row. The boot contract
becomes one `pins: Option<Vec<PinKey>>`, `PinKey` an enum of the two keys,
with the same meaning of `None` (unknown: sweep nothing, treat everything as
pinned). The two stores still apply their halves; what merges is the
contract.

**The cache directory is renamed**, from `<cacheRoot>/rqbit-downloads` to
`<cacheRoot>/media-cache` (pick: it holds the torrent pieces, the proxy
chunks and the session's records, and is not librqbit's in any sense a
reader needs). The only production literal is
`enginefs/src/lib.rs:5487`; about ten tests spell the old name (the fixture
in `server/tests/support/torrent_fixtures.rs`, `create_wants.rs`,
`proxy_downloads.rs`, `embed.rs`, `cache_cleaner.rs`'s tests, and
enginefs's own), which is the reason to make it a public constant
(`enginefs::CACHE_DIR_NAME`) rather than another literal.

**No migration** (zond: the data is cheap). A `rqbit-downloads` found at boot
is deleted -- the torrent pieces, the proxy cache, librqbit's
`session.json` and DHT state, and every pinned download's bytes with them.
The deletion is a rename to `.rqbit-downloads.deleting` before the session
boots (cheap, and at the cache root, outside anything
`sweep_legacy_downloads` walks) and a `remove_dir_all` on the blocking pool
after, retried by the next boot if the process dies first; a synchronous
delete of a store of one-file-per-piece on an SD card is seconds of boot.
`LEGACY_ARCHIVE_SCRATCH_DIR` (`lib.rs:100`, the `.archives` delete at
`lib.rs:1441-1460`) is the precedent, and its comment is the argument:
nothing else would ever take those bytes. `NOT_OURS` in
`piece_store/sweep.rs` lists `.pieces` and `.proxy`; any new directory under
`media-cache` must join it or the next launch deletes it.

**What the app then shows is not what the brief assumed** (see §8). After
the delete, a torrent pin the app hands in names a torrent the session
never restored: `downloads()` reports it dormant (`DORMANT_DOWNLOAD_ERROR`),
xtremio's `holds_whole` reads that as `Held::Unknown` -- "the server cannot
answer yet" -- and a finished row is **never** marked gone; Play answers
`Unavailable`. Pick: a dormant pin whose `.pieces/<hash>` directory does not
exist is reported as held-nothing (`complete: false`, no dormant error),
which the server knows for certain, so the app's existing `NotWhole` path
marks the row `gone` ("No longer on this device -- download again") with no
app change. An unfinished row is re-pinned at launch as today, which starts
it again from nothing.

### 2.7 Cast: publish a token, one LAN route

```rust
pub struct CastToken(String);            // 128 random bits, hex; never an id
fn publish(&self, id: &MediaId) -> anyhow::Result<CastToken>;
fn unpublish(&self, token: &CastToken);
```

The LAN listener mounts **one** route, `GET/HEAD /cast/{token}`, which
serves the id the token was published for with the shared range framing
(`util::MediaRange`, `archive::media_body`, `routes/archive.rs:53`) over a
reader opened from the resolved source. It replaces `lan_media_routes()`
(`lib.rs:2042`: `lan_stream_routes()` and `archive_stream_routes()`) whole.
`lan_media.rs` is route-agnostic and keeps its start, stop and base URL.

What this fixes: every case in §1 casts, because every case is an id. The
four `404`s go. **Drive stops being a privacy exception**: the receiver sees
one published file under a random token, not a Drive route that could be
walked. A torrent cast carries the viewer's play (the app passes its token
to `publish`, as it adds `p=` today); a member does too, from 2.8.

The token's rules:

* **Random, not derived**: a receiver that saw one token learns nothing
  about another, or about the id.
* **Cleared** by `unpublish`, by `set_lan_media(false)` (every token), and by
  the process ending. An unknown token is `404`.
* **`unpublish` cuts a body in flight.** Today `LanMedia::stop` closes the
  door and lets a streaming response run to its end, and its comment names
  the only fix: "every media body wrapped in a cancellation token". With one
  body type on the LAN that is one wrapper, `ClosableStream`'s shape from
  `proxy_streams.rs` (`register`/`close` by token). `set_lan_media(false)`
  unpublishes everything, so it now stops the bytes too; its documentation
  changes with it.
* **`log_path` elides the token**: `/cast/<token>` logs as `/cast`, beside
  `/proxy` and `/ftp` (`routes/util.rs:171`). A token in a log file is a
  URL into this device for as long as it is published.

**The watchdog.** The count `lan_media_requests_served` stays what it is --
reachability, 404s included, as its test pins -- and a second count joins
it: **bodies served**, counted when a `/cast` response begins a body. The
app's 20 s check then has three answers where it had two: no requests (the
address is unreachable: end the session, as now), requests but no body (the
receiver asked for something this device would not serve: end the session
and say so), bodies (leave it to the media). The brief's "count only served
bodies" would have turned the first diagnosis into the second.

### 2.8 What sharing does for an archive member

**After step A, a member the viewer plays shares like a film, with one
arithmetic caveat and one exception.** This reverses a stated rule, so it
is spelled out.

Today the rule is not an accident of member readers lacking a token; it is
enforced by name and by content (§1). For a member played through an id
with a play, three things change together:

1. The member path calls `note_player` with `shares: true` for the
   container file it is reading.
2. The retention owner's content check (`content_shares`, which answers
   "archive, share nothing" for exactly these files) is skipped for a play
   session the member path opened, which knows what the file is. It stays
   for a container file played directly, which after A' only a stale URL
   does.
3. `played_through_a_translator` stays on the HTTP route for the same
   reason.

**The caveat: the draw is sized from the container's length against the
member's duration.** The draw's size is `bytes_per_second x window_seconds`,
capped by the budget, and `bytes_per_second` is the entity's length over the
duration the app reported (`note_duration`) -- "bytes of file over seconds of
film is the bitrate by definition" (`retention/owner.rs`). The entity is
the container file; the duration is the member's film. For the ordinary
case -- a stored RAR or ZIP of one film and an `.nfo` -- the container is the
member plus a few KiB and the error is nothing. For a container holding
several films (a season in one ZIP, a multi-title ISO) the rate is
overstated by container over member, and so is the draw, up to the budget
cap; and a draw is published once and never withdrawn. Pick (zond's, over
the doc's first draft): **the play is told the member's byte extent inside
the container file**, which the member path knows from its `Extent`s. The
session's rate is then the member's length over the real duration, and the
draw is made inside that extent, so a season in one ZIP shares the episode
being watched and not the ZIP. No reported number is scaled to make an
arithmetic come out; a duration stays a duration. If carrying an extent into
`retention::sessions` turns out too invasive for step A, the fallback is a
`note_rate` beside `note_duration` -- never a scaled duration.

**The exception: a multi-volume set keeps sharing nothing.** A play session
names one file (`Played::Torrent { info_hash, file_idx }`), and a set's
member crosses files. Moving the session at each volume boundary would be
"a player moving", which ends what the previous file shared -- stop, rebuild
the advertised set, start -- at every volume, every few hundred MiB. The
liveness cell already has sets (`Live::hold_set`); play sessions do not.
Until they do, a member whose extents span more than one source opens its
volumes with `shares: false`. **The follow-up is named, not open**: play
sessions gain a set the way the cell did -- `Played::Torrent` names a set of
files, moving within the set is not "a player moving", and the draw is made
over the set's files within the member's extent. That is step A2 below,
after A has landed, and is what makes a RAR set share like a film.

### 2.9 The container sniff moves into `resolve`

`resolve` reads the first `0x8006` bytes (`enginefs::retention::sniff::HEAD_BYTES`,
the same figure as `archiveSniffBytes` in the app) and asks
`sniff::is_archive`: ISO 9660's `CD001` and UDF's `BEA01` are at `0x8001`, so
32 KiB misses both. A UDF image's `NSR02`/`NSR03` (which is what actually
says UDF) is in the next sectors, about 36 KiB in; `BEA01` is enough to try.
On a hit it tries each translator's `index()`, which already verifies its
own format -- 7z's signature header, ZIP's end record at the tail, a TAR
header's checksum, RAR through `unrar`, ISO/UDF descriptors -- and the first
that indexes wins. The member is picked by the rule `/create` uses with no
`fileIdx` (`compat::resolve_file_idx`). A refusal (`415`, `422`, `501` today)
is `resolve`'s `Refusal`, with the same sentence.

Multi-volume RAR: from a torrent, the volumes are the named file's siblings
by the two naming rules (`Translator::volumes`), and that works for a sniff
as it does for the `torrent:` form. From a URL or Drive there are no
siblings to look at: the addon-declared case carries its list (`rarUrls`),
and a sniffed URL is one volume -- a set behind links needs an explicit
volume list, which is a `MediaSpec` field when something supplies one.

The app's `archive_sniff.dart` fail-then-sniff path stays as the fallback
until the server's sniff has shipped, and is then deleted.

### 2.10 Renditions (step F): transcode for a receiver, on demand, no disk

For a receiver that cannot decode the original (the per-model table; on
zond's TV, AC3/E-AC3 over Bluetooth audio is **silent**), the cast is a VOD
HLS rendition:

* **No cache, nothing on disk.** The playlist is written up front (the
  duration is known); segment N is produced when asked, from N's timestamp;
  a small in-memory ring keeps the last few; a seek restarts the producer;
  a pause idles it. A segment is produced whole before it is answered, so
  `MediaRange` and `Content-Length` hold.
* **The producer is Kotlin `MediaCodec`**, reading the source through an
  Android `MediaDataSource` backed by JNI exports from xtremio's Rust crate
  over 2.4's reader. Not mpv's ffmpeg (the vendored libmpv is built for
  playback; see §8 for what was not verified), not Media3 Transformer (a new
  dependency). The server crate stays a plain `rlib` (AGENTS.md): the route,
  the playlist and the ring are the server's, and the producer is an
  embedder-installed trait object the route asks for segment N.
* **Speed is measured**; a producer below real time for a few seconds ends
  the cast with a sentence rather than a spinner.
* **Which rendition** (as-is, repackage, audio to stereo AAC, full
  transcode) is the app's decision, from mpv's codec report and the
  receiver table; surround audio is re-encoded to stereo by default. The
  stats poll must gain the audio channel count (only per track today). The
  cast wrapper (flutter_chrome_cast 1.4.8) sends HLS; the
  `hlsSegmentFormat`/`hlsVideoSegmentFormat` hints must be exposed for fMP4
  on the default receiver.

Routes: `GET /cast/{token}/hls/index.m3u8` and `GET /cast/{token}/hls/{n}.m4s`
(plus the init segment), under the same token and the same cut-on-unpublish.
F gets its own design note before it is built; this section fixes only
what it must not break.

## 3. Routes and the contract with the client

**stremio-core sees nothing change.** Every core-protocol route and every
URL the core builds stays exactly as it is; the core's `streaming_url` is
now read by `register` rather than by mpv.

**The app's contract is `ServerHandle` methods** (the house rule: a new
capability is a method, never a control route):

| Method | Does |
|---|---|
| `register(MediaSpec) -> MediaId` | Parses and records. No I/O. |
| `resolve(&MediaId) -> Result<Resolved, Refusal>` | Adds/finds, probes, sniffs; cached on the entry. |
| `open_reader(&MediaId, Option<PlayToken>) -> Result<MediaReader, Refusal>` | 2.4. Blocking calls on the reader; never an async API across FFI. |
| `set_buffer(&MediaId, BufferProfile)` | Applies to the reader's next open. |
| `publish(&MediaId) -> CastToken` / `unpublish(&CastToken)` | 2.7. |
| `pin(&MediaId) -> Result<DownloadInfo, PinError>` / `unpin(&MediaId, bool) -> UnpinOutcome` | 2.6. |
| `stream_numbers`, `note_duration`, `note_player_opened`, `note_player_stalled`, `close_streams` | As today, keyed by id. |

`MediaReader`'s blocking methods (`read`, `seek`, `cancel`, `len`, drop as
close) are what xtremio's `stream_cb` shim and later its JNI exports call;
FRB never sees a reader.

**Media routes**: the LAN listener serves `/cast/{token}` and nothing else. The loopback media routes stay for the core and
for the in-process `/proxy` leftovers until their callers are gone (§4).

Refusals keep translated-sources' mapping (`415`/`422`/`501` with
`{refused, message}`), carried as a typed `Refusal` over FFI instead of a
status.

## 4. What is deleted

As each step lands, not beside it:

* **`TorrentFileSource` and `TorrentMemberStream`** (`sources/torrent.rs`, 297
  lines), by step A. `TorrentSource` (2.2) replaces both, and the archive
  route's `torrent:` form and `file_names` move onto it. *(Done, step A's
  first slice: `routes::stream::open_torrent_stream` is the factored open.)*
* **`lan_media_routes()` as an allow-list of route groups** -- the function,
  `lan_stream_routes()`, `archive_stream_routes()`, the LAN arms of the
  stream route (`lan_stream_video`, `lan_head_stream_video`,
  `EngineAccess::ExistingOnly`, whose `404` is what today answers the
  `/rar/create` and `/proxy/..` paths that collide with `/{infoHash}/{fileIdx}`)
  -- by step C. The LAN router is
  `/cast/{token}`. The allow-list's argument (a stranger must not be able to
  make this device *do* anything) is what one token route satisfies by
  construction. About ten tests (`embed.rs`'s LAN group,
  `log_redaction.rs`) and the LAN passages of README, AGENTS.md,
  `docs/api.md`, `docs/lan-media.md`, `docs/proxy.md`, `docs/settings.md` and
  translated-sources' §3 change with it.
* **The remote-streaming-server case.** Dead already (`pin_to_embedded`);
  the app's leftovers go in their own PR, and nothing here re-grows one.
* **The app's URL rewriting for mpv**, once A' lands: `proxiedThroughServer`
  (except as 2.5's non-ranging leftover), `withBufferAhead`,
  `withPlayerToken`, `isProxiedByServer`-based decisions, `_reopenForBuffer`,
  and `_castUrl`'s rebuild-on-the-LAN-base. The brief counted ~97 URL
  assertions across 18 player test files that move to ids.
* **The name `rqbit-downloads`**, by step D, and the data under it at first
  boot (2.6).
* **`archive_sniff.dart`'s fail-then-sniff**, after step E has shipped.
* **Not deleted**: `/proxy`, `/ftp`, `/drive/stream`, `/downloads/{key}/stream`
  and the archive routes on loopback. stremio-core builds some of these
  URLs and the non-ranging case needs `/proxy`; the rest go when nothing
  calls them, which is a later, separate step with its own grep.

**Untouched**: the retention owner and its policies, the piece store, the
proxy cache's internals, librqbit and its wiring, the reconciler, the
liveness cell and the RAR set hold (`Live::hold_set`), and stremio-core.

## 5. Steps, in order, each shippable

Each lands green with its own tests, revert-proven hunk by hunk, the old
path removed as its replacement lands. Sizes are the brief's: S, M, L.

A. **Server: one torrent source, ids, the reader task.** (L.) Factor the
   stream route's open into 2.2's function; `TorrentSource` with and without
   a play; the registry with `register`/`resolve`; `open_reader` and the
   task of 2.4; the `ServerHandle` methods. Tests: the stream route's suite
   unchanged (the proof the factoring is one); a reader over each source
   kind reads, seeks and reaches end of file; a read parked on a piece
   nobody has is woken by `cancel` with `Interrupted` and every later
   command answers the same; a reader dropped on a non-runtime thread
   spawns nothing there; a server shut down under a blocked read answers it
   with an error; a torrent reader with a play moves the session and draws,
   without one it does neither (the lib fake's `violations` guard holds);
   a single-container member with a play draws, a multi-volume one does
   not (2.8). *(Second slice done: `server/src/media/` -- the registry,
   the reader task and the handle methods, over torrent, `/proxy` and
   Drive ids; an archive `/create` and `/ftp` register and resolve
   `notYet`. A played reader's stream ends with its own log line,
   `reader_stream_end`, not the route's. Member sharing is the third.)*

A2. **Play sessions understand sets.** (M.) `Played::Torrent` over a set of
   files, mirroring `Live::hold_set`; a move inside the set ends nothing;
   the draw spans the set within the member's extent. Removes 2.8's
   exception. Tests: a two-volume member with a play draws once and the
   torrent is not stopped at the boundary.

A'. **App: mpv on `xtremio://<id>`.** (L.) The vendored media_kit patch
   (2.5); the `stream_cb` shim in xtremio's crate over `MediaReader`; torrent,
   URL, Drive, download and member streams through ids; `p=`, `buffer=`,
   `force-seekable`, stream numbers and stream close by id; the
   `network-timeout` dependence of 2.5 moved first. The URL assertions in
   the player tests move.

B. **Local source.** (S.) A path, or on Android an fd from
   `ParcelFileDescriptor.detachFd()`. A pipe fd from a cloud SAF provider is
   not seekable; `resolve` refuses it (`lseek` fails) with a sentence rather
   than streaming it forward-only.

C. **Publish and the cast route.** (M.) 2.7 whole: tokens, `/cast/{token}`,
   the body wrapper, `set_lan_media(false)` unpublishing, `log_path`, the
   bodies count and the app's three-way check. Deletes the allow-list.
   Tests: each of §1's cases casts; an unknown token is `404`; `unpublish`
   ends a body mid-stream; no token reaches a log line.

D. **Pins by id; one pin set; the directory.** (S-M.) `pin(id)`,
   `PinKey`, the rename, the boot delete, `NOT_OURS`, and the held-nothing
   answer for a dormant pin without a directory (2.6).

E. **The sniff in `resolve`.** (S; M more for explicit volume lists on URL
   and Drive sets.) 2.9, then the app's fallback deleted.

F. **Renditions.** (Server route M; Kotlin producer L.) Its own design note
   first (2.10).

## 6. Decisions taken here, for zond to overrule

* **The cache directory is `media-cache`.** It holds more than librqbit's
  data and the name says what a reader needs.
* **The old directory is renamed then deleted in the background**, not
  deleted on the boot path.
* **A dormant pin with no piece directory reports held-nothing**, so the app
  marks it gone without a change of its own.
* **`register` does no I/O and `resolve` answers name and type**, rather
  than registration returning them, because of magnets.
* **Ids are evicted by count, like sessions**, with a lease for an open
  reader or a publish; no clock.
* **Seek reopens for every source**, with the no-op for a seek to where the
  reader already is.
* **Cancel is sticky**, as `stream_cb.h` says, and rides a
  `CancellationToken` beside the command channel, not in it.
* **The reachability count stays; a bodies count joins it.**
* **Single-container members share from A; multi-volume sets from A2** (2.8).
* **A member's play carries its byte extent** (zond): the rate is member
  length over real duration and the draw is inside the extent; `note_rate`
  is the fallback, a scaled duration is not an option.
* **The URL of a `streaming_url` is parsed in the server** with the routes'
  own parsers, not in xtremio's crate.
* **UDF is sniffed by `BEA01`** within `0x8006` bytes; the translator's index
  is the proof, not the sniff.

## 7. What this does not do

It does not add a cache. The proxy cache and the piece store remain the
only places bytes are kept, a local file is read where it is, a rendition
is produced in memory and dropped, and a translator still writes nothing.

It does not change what the core does. stremio-core builds the same URLs
for the same streams; the difference is only who reads them.

It does not make a non-ranging origin a source. An HLS playlist or a live
channel goes to mpv through `/proxy` as it does now; step C does not cast
one (it is not an id with a reader), which stays as today.

It does not make play sessions understand sets; that is the named gap of
2.8. It does not make a compressed member playable, and it does not
transcode for this device's own player: renditions are for receivers.

## 8. Brief vs code

Where the brief (2026-10-01, from three scouts) and the tree disagree, and
what this note does about it:

* **"Archive playback does not share only because member readers never
  carried a token" -- no.** It is a stated rule enforced by name
  (`played_through_a_translator`, `Played::Torrent { shares: false }`) and by
  content (`content_shares` over `retention::sniff::is_archive`), with a test
  and an AGENTS.md entry, and the app's `_castUrl` comment repeats it. A
  token alone would share nothing: the content check would still find an
  archive. 2.8 lists what has to change, and the multi-volume exception the
  brief did not raise (a play session names one file; moving it per volume
  would stop and restart the torrent at every boundary).
* **"Rows survive and show 'no longer on this device'" -- not as the code
  stands.** A torrent pin whose session state was deleted is dormant,
  `downloads()` reports `DORMANT_DOWNLOAD_ERROR`, and xtremio reads that as
  `Held::Unknown`, which never marks a finished row gone. 2.6's pick closes
  it. The delete also takes librqbit's `session.json` and the proxy cache,
  which live under the same directory (`.proxy` is under the download dir,
  `proxy_cache.rs:433`).
* **"Count only served bodies" contradicts a pinned decision.** The count
  includes `404`s on purpose (reachability), and a test pins a `404`
  counting. 2.7 adds a second count instead.
* **`CachedRange::body`** is `Cached::body` (`proxy_cache.rs:1247`, `1325`);
  `Entry::look_up` returns `Option<Cached>`.
* **`stream_video_with`** runs `routes/stream.rs:979-1338` at `95bd0c0`, not
  `979-1210`.
* **"UDF-only images want ~38 KiB"**: `BEA01` is at `0x8001` and inside
  `0x8006`; `NSR02`/`NSR03` is about 36 KiB in; `TEA01` about 38 KiB. The
  server's sniff already accepts `BEA01`.
* **Confirmed as stated**: `sources/mod.rs:66-178`, `block_on_server` at
  `lib.rs:1097`, `lan_media_routes()` at `lib.rs:2042`,
  `routes/archive.rs:53` (`media_body`), the sole production
  `"rqbit-downloads"` at `enginefs/src/lib.rs:5487`, `NOT_OURS` and
  `sweep_legacy_downloads`, `ProxySource`'s `bytes=0-0` probe and
  `WillNotRange`, the two `tokio::spawn`s in drops, `Arc::try_unwrap` in
  xtremio's stop, `drive_grant`/`set_drive_grant`, `pin_to_embedded`,
  media_kit 1.2.6 `real.dart:181`, `jni` 0.22.4 in xtremio's lock,
  flutter_chrome_cast 1.4.8, no Media3/MediaCodec code in the app (comments
  only), no JNI symbols in xtremio's crate, the `stream_cb.h` layout, the
  spike branch and its two commits, and the four `404` casts (URL, Drive
  and download routes are not on the LAN; the core's archive URL is a
  `/create`).
* **Not verified**: that the vendored libmpv's ffmpeg has encoders and
  muxers disabled (the `full` flavour's build scripts are upstream, not in
  the tree); that release `strip = true` keeps JNI exports (it strips the
  symbol table, not `.dynsym`, so it should -- untested); that
  `System.loadLibrary("xtremio_core")` shares the already-loaded library
  (standard linker behaviour, untested here); the exact ~97/18 count of URL
  assertions (a rough grep finds about 60 lines in 13 test files naming
  `p=`, `buffer=` or `/proxy/`, and 20 files touching opened URLs); what
  the app does with proxy downloads after `.proxy` is deleted; and the
  spike's "~zero overhead", measured against a local file, not a torrent.
