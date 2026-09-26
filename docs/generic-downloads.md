# Downloads for every source: a pin on the byte owner, and a filler

Design, 2026-09-26. Written against stream-server `9334c93`, xtremio
`be034e6`, rqbit `d02b73a2`.

**Status (2026-09-26, later the same day):** §3.1–3.4 are built on the
server -- the proxy pin set through `ServerConfig::proxy_pins`, the
keeping sweep with the occupancy seeded from what it kept,
`ProxyBacking::keeps_everything`, quiet readers, the filler, and the
embed calls (`ServerHandle::{pin_proxy_download, unpin_proxy_download,
proxy_download_key}`, rows in `downloads()` with `source` and `playUrl`;
the HTTP routes §3.4 sketched were built and then removed the same day
with every other app-facing control route -- the app speaks FFI, see the
README's "API"). One
thing the design did not foresee: a finished download does **not** play
from the stream's `/proxy` URL, because that route keys the cache on the
player's own negotiation headers, so a player's request lands on a key the
filler never filled. It plays from the download's own media route,
`GET /downloads/{key}/stream`, which serves the pinned entry by range off
the disk (`server/tests/proxy_downloads.rs` measures it: filled from a
loopback origin, played back with the origin asked for nothing, kept
across a restart that names it, swept by one that does not, deleted on
request). Not built: §3.5 (a complete Drive download played offline:
the media route serves any complete pinned entry, Drive included, but the
app still opens Drive files through `open_drive_file`, whose probe goes to
the origin), §4 (read-ahead), and §5 (the app) -- the app side is next.

**The ask (zond):** every source should download, not only torrents -- and
"maybe a new set of functions on the byte owner that handles download
caching". Also: does Drive streaming sit in the caching/lookahead system?

**The answer in one paragraph.** Yes to both, and they are the same work.
The principle already in force (`translated-sources.md`) is *only the thing
that downloads a file may store it*: the piece store for torrents, the
proxy cache for everything else. Every non-torrent source there is -- an
addon URL, a Drive file, and whatever comes next -- already reads through
`ProxySource` into the proxy cache, under the proxy retention owner, with
the same budget, window and reclaim as a torrent file. A download of one of
them is therefore two things the torrent side already has and the proxy
side does not: a **pin** (a claim that outlives the process and exempts the
entity from reclaim) and a **filler** (something that fetches the bytes when
no player is asking for them). Both belong to the proxy's retention owner,
which is the byte owner zond named. And once a filler exists, pointing it at
the owner's *want set* instead of "everything" is read-ahead for Drive and
HTTP streams, which they lack today.

## 1. Where the code is, and what is missing

| | Torrent (enginefs + librqbit) | Proxy / Drive (`proxy_cache`, `proxy_retention`, `sources::*`) |
|---|---|---|
| Store | one file per piece under `.pieces/<hash>/` | one file per 256 KiB chunk under `<key-sha256>/<validator>/` |
| Owner | `enginefs::retention::owner` via `TorrentBacking` | the same owner via `ProxyBacking` -- same budget, same 90/10 window, same pass |
| Holes | any set of pieces | any set of chunks (`Entry::look_up` walks from `first` to the first missing) |
| Fetcher when nobody reads | librqbit, driven by the want set | **none** -- the owner computes `want`, nothing fills it |
| Pin | `Engine::pinned_files`; `keeps_everything` answers from it | `keeps_everything` is hard-wired `false` ("the proxy has no pins") |
| Claim that outlives the run | `PinSet` handed in by the embedder at boot; the launch sweep keeps what it names | **none** -- the launch sweep removes the whole cache ("no claim that outlives the run") |
| Occupancy at boot | seeded from the held set the sweep left | "what this process wrote", true because the sweep emptied the disk |
| Progress | `downloads()`, `download progress` line | -- |
| Playback of a finished download | a `url` stream naming the server's media route, served off the pieces | would be the `/proxy` or `/drive/stream` URL itself: a complete entry answers without opening the origin (`Cached::complete`) |

Drive specifically: `DriveSource` **is** a `ProxySource` (`sources/drive.rs`)
with a header supplier that renews the token in Rust, and its entry is a
vouched key (`ProxyCache::entry_for_vouched_url`: the file's URL with no
token, in a namespace no `/proxy` key can reach). So a Drive stream today is
cached, windowed and reclaimed exactly as an addon URL is. What it does
not get is read-ahead, for the reason in the table: the proxy owner's
`want` has no filler.

## 2. The identity of a non-torrent download

A torrent download is `(info_hash, file_idx)`. A proxy download is **the
entity's cache key**, which is already the right thing: the final target
URL with its query, the `h=` request headers, and the player's
content-negotiation headers (`ProxyCache::entry`'s key doc) -- or, for
Drive, the vouched URL. Two consequences:

* The key is what `/proxy` and `ProxySource` agree on for one URL (a test
  already asserts it), so a download and a later play of the same stream
  share chunks. That is the whole point of storing in the cache rather
  than beside it.
* Credentials are not in it, by construction (`CREDENTIAL_REQUEST_HEADERS`
  are refused from the key; a Drive token never enters the URL). A pin
  record therefore never persists a secret.

For the app's registry (`xtremio rust/src/downloads.rs`, keyed by
`(metaId, videoId)`), the row's coordinates become an enum rather than two
fields:

```rust
#[serde(tag = "kind", rename_all = "camelCase")]
enum Source {
    Torrent { info_hash: String, file_idx: usize },
    Url     { target: String, headers: Vec<(String, String)> },   // what stremio-core puts in d=/h=
    Drive   { file_id: String },
}
```

Rows written before this carry `infoHash`/`fileIdx` at the top level and
deserialise as `Torrent` (a `#[serde(untagged)]` fallback, or a migration
at load -- the registry already has a version field). `_streamKey`,
`names`, `pin_is_shared` and the replace logic compare `Source`s; none of
them cares what is inside.

## 3. The server side: five pieces, in dependency order

### 3.1 A proxy pin set that outlives the run

Mirror `ServerConfig::pins` exactly, because its semantics were argued
over and are right (`piece_store/pin_record.rs`): the embedder is the
authority, it hands the set in at boot, and **`None` is "nobody told me",
not "nothing is pinned"**.

```rust
pub struct ServerConfig {
    pub pins: Option<enginefs::piece_store::PinSet>,     // torrents, as today
    pub proxy_pins: Option<BTreeSet<ProxyPinKey>>,        // new
}
/// What the embedder can name: the identity, not the hash. The server
/// derives the key directory from it the way `ProxyCache::entry` does,
/// so a change to the hashing never orphans a pin.
pub enum ProxyPinKey { Url { target, headers }, Drive { file_id } }
```

`proxy_cache::sweep` takes the resolved set of key directories and keeps
them -- every entity generation under a pinned key, since the validator
that names the generation is not known until the origin is asked. With
`None` it keeps everything (the piece store's rule; a boot that reads no
downloads list must not delete the user's downloads). `ProxyRetention::
occupancy` is then seeded from what the sweep kept, the way the piece
store seeds its held set: "what this process wrote" becomes "what this
process wrote or was told to keep", and the figure stays true from the
first byte.

### 3.2 `keeps_everything` on `ProxyBacking`

```rust
fn keeps_everything(&self, key: &PathBuf) -> bool {
    self.pins.read().contains(key_dir_of(key))     // copy-out read, no owner lock; same shape as the torrent's
}
```

The owner already does the rest: a kept entity's chunks are committed,
not slack; the pass never plans them; the out-of-space rule refuses a new
stream rather than deleting a pin (owner design #5). Nothing new is
decided here -- this is the line that makes the owner treat a proxy pin as
it treats a torrent pin.

### 3.3 The filler -- the one piece of real machinery

```rust
/// Fetches the holes of one pinned entity until it is whole. It is not a
/// reader: it opens no `Live` entity, has no playhead and no window, and
/// its bytes are not a promise -- they are a claim (the pin).
pub struct Filler {
    entity: proxy_cache::Entry,
    source: Arc<dyn ByteSource>,      // ProxySource or DriveSource, as `/proxy` and `/drive` build them
    stride: u64,                      // bytes per ranged request, e.g. 32 MiB; small enough that a stall shows within one progress line
    abort: AbortHandle,
}
```

The loop is the proxy route's own miss path, turned around:

1. `entity.look_up("bytes=0-")` -- complete? log `download complete`, stop.
2. Otherwise `Cached::remaining_range()` names the next hole (its start is
   a chunk boundary by construction). Ask for `min(hole, stride)` through
   `cache_assisted_range` -- the same call `/proxy` and `ProxySource` make:
   narrowed against the disk, `If-Range` on the validator, wrapped in
   `Filling` so whole chunks land at their final names. Read the body and
   drop it; the disk is what we wanted.
3. A validator change (the origin's `If-Range` answered `200`): the fill
   drops the old generation (`remove_other_entities`, as today) and the
   walk starts again from byte 0 -- reported in the progress line as a
   restart, once, rather than hidden.
4. A `WillNotRange` origin (a `200` to a ranged request) is **refused at
   pin time**, with the message `ProxySource::open` already has: a file
   that cannot be resumed cannot be a download, for the same reason it
   cannot be a translated source.

Ownership: one filler per key, claimed and released like the progress
loggers (`routes/downloads.rs::claim_progress_logger`). It runs while the
pin stands and stops when the pin is dropped -- **an unpin aborts the
filler first**, the rule `9334c93` just established for a torrent's
in-flight add, so Cancel is immediate here too. On restart the filler
resumes from the holes: the disk is the truth, nothing about progress is
persisted, and a chunk a kill was writing was never renamed into place.

Rate: unlimited by default; a later setting can cap it. The filler counts
as background traffic for the sharing light (it *is* "using your
connection while you are not watching"), through the same
`BackgroundTraffic` reading, by direction.

### 3.4 Routes and the embed API

*(As designed. What was kept is the embed API only; the routes went with
the rest of the app-facing control routes on 2026-09-26.)*

```
POST   /downloads            {"url": ..., "headers": [[name, value], ...]}   -> DownloadInfo
POST   /downloads            {"driveFileId": ..., "pairing": {...}}          -> DownloadInfo
DELETE /downloads/{key}?deleteFiles=1                                        -> UnpinOutcome
GET    /downloads.json                                                       (torrent and proxy rows, `source` on each)
```

`DownloadInfo` gains `source: Source` and keeps `length`, `downloaded`,
`complete`, `error`. `downloaded` for a proxy entity is *held chunks x
CHUNK_BYTES*, off the held set -- exact, and free. `ServerHandle` gets the
same three calls the torrent has (`pin_url_download`, `pin_drive_download`,
`unpin_proxy_download`) so xtremio's FFI layer changes in one place.

The `download progress` line (`9334c93`) gets its proxy twin: `moved`,
`still_secs`, bytes held / total, the hole being fetched, and `origin` as
the source describes itself (`ByteSource::describe`: never a URL, never a
credential).

### 3.5 Playing a finished download

Today's contract (`offline_play.dart`): a finished download is not a file;
the player is handed a `url` stream naming the server's own route, served
off what is on the device. For a URL download that route is the `/proxy`
URL the stream already had -- a complete entry answers from disk and the
origin is never opened. For Drive it is `/drive/stream/{key}`, with one
change: `DriveSource::open` probes the origin with `bytes=0-0` and treats
an answer from the cache as an error ("the probe was answered from the
cache"). For a pinned, complete entity the probe must accept the cache's
answer, or an offline device cannot play the download it holds. Small,
and it is the only piece of §3 that touches the sources.

## 4. Drive streaming, and read-ahead

What zond asked: does Drive fit the caching/lookahead system? **Caching
and retention, yes, today, unchanged.** Lookahead, no -- and not because
Drive is special: *no* proxied stream reads ahead of its player, because
nothing fills the owner's `want`. The player's own `Range` is the only
thing that ever fetches.

The filler above is the missing half, and driving it from `want` instead
of "everything" is the same object:

```rust
enum Fill { Everything /* a pin */, Wanted /* the owner's want set, refreshed each pass */ }
```

Under `Wanted` the filler fetches the chunks the pass published as wanted
for the entity's live readers -- the film's bitrate times the buffer
profile's seconds, the same arithmetic the torrent's want set is made of
-- and nothing else. That gives Drive and addon streams what a torrent
stream has: a buffer ahead of the playhead, and a seek that lands inside
the window served locally. Bounded by the same budget, reclaimed by the
same pass, exempt while promised.

Two cautions, both stated rather than hidden. Read-ahead is origin
traffic: on Drive it counts against the file's daily download quota, on a
debrid link against whatever the addon meters; the window is seconds, not
the film, so the cost is bounded, and it should be a setting with the
buffer profile. And a filler must never be mistaken for a viewer: it opens
no `Live` entity, so a torrent nobody is playing is still stopped by the
reconciler, and a proxied entity nobody is playing has no window to fill.

## 5. The app

* `torrent_source(stream)` becomes `source_of(stream)`: `infoHash` ->
  `Torrent`; `StreamKind.url` with an `http(s)` URL -> `Url { target,
  headers }` from what stremio-core puts in `d=`/`h=`
  (`types/resource/stream.rs`); a linked Drive file -> `Drive { file_id }`.
  Anything else (`youtube`, `external`) is refused with a sentence.
* `_StreamDownloads.starter` admits `url`; `_downloadsFor` hands Drive
  groups a `_StreamDownloads` after all -- the reason it did not (no
  torrent the server could keep) is gone.
* `pins()` publishes both sets to `ServerConfig` at boot; `pins_in`
  splits rows by `Source`.
* The replace and cancel logic, the notification and the library's
  Downloaded pill are source-agnostic already: they read `Entry::state`
  and `wants_pin`, never `info_hash`.

## 6. Tests that prove it, in the order it is built

1. **Sweep keeps a pinned key and only it** (`proxy_cache` tests): two
   entities on disk, one named by the set -> one survives; `None` ->
   both; an empty set -> neither. Occupancy equals what survived.
2. **`keeps_everything` exempts** (`proxy_retention` tests, scaffolded
   scenarios as the retention work was): a pinned entity over budget is
   never planned; a stream opening beside it is refused (507) rather than
   the pin reclaimed.
3. **The filler completes and resumes** (a loopback origin, as
   `tests/proxy.rs` has): pin -> `complete()`; kill mid-fill (drop the
   filler) -> restart with the pin set -> resumes from the holes and no
   chunk is fetched twice (count the origin's requests); validator change
   -> one restart, reported.
4. **Unpin aborts** (the `9334c93` shape): the origin never answers the
   second range; the unpin returns before it would have; the directory is
   gone with `deleteFiles`.
5. **A stream shares the download's chunks**: fill half, open a `/proxy`
   read inside it -> served from disk, origin not opened.
6. **Drive offline**: a complete pinned Drive entity plays with the
   pairing service unreachable.
7. **Read-ahead** (`Wanted`): with a reader at offset X and a stated
   duration, the filler fetches exactly the wanted chunks and stops at the
   window's edge; the sharing light stays dark while the player reads.

## 7. Staging

1. §3.1 + §3.2 + occupancy seed. Pins exist; nothing fills them yet, but a
   *streamed* film that was pinned survives a restart, which is already a
   feature.
2. §3.3 + §3.4. Downloads of URLs and Drive files, end to end, on the
   server.
3. §5. The app admits them.
4. §3.5. Offline Drive playback.
5. §4. Read-ahead for streams, behind the buffer profile.

Related: `translated-sources.md` (the principle and `ByteSource`),
`read-pattern-retention.md` (the owner both backings run).
