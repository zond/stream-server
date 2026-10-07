# Downloads for every source: a pin on the byte owner, and a filler

Design, 2026-09-26. Written against stream-server `9334c93`, xtremio
`be034e6`, rqbit `d02b73a2`.

**Status: built, all of it** (2026-09-26): the proxy half of the pin set
(since 2026-10-01 one `ServerConfig::pins: Option<Vec<PinKey>>`, whose
`Url`/`Drive` keys `PinKey::proxy_pins` hands to `ProxyDownloads::install`;
built first as a separate `proxy_pins`), `ProxyBacking::keeps_everything`,
the filler (`proxy_downloads::Filler`), the embed calls
(`ServerHandle::{pin_proxy_download, unpin_proxy_download,
proxy_download_key}`, and `pin`/`unpin` by media id over them, rows in
`downloads()` with `source` and `playUrl`),
offline Drive playback, and read-ahead (§4). Where the build departed from
the design, the section says so. `server/tests/proxy_downloads.rs` and
`server/tests/drive.rs` measure it.

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
HTTP streams, which they lacked.

## 1. What the two sides share

The proxy cache (`proxy_cache`, `proxy_retention`, `sources::*`) keeps one
file per 256 KiB chunk under `<key>/<length>_<content type>_<validator>/<bucket>/`
(`proxy_cache`'s module doc; since 2026-10-01
the entity's *identity*, never absent: a Drive file's checksum, else the
origin's validator, else its length -- Drive names no validator, and under
the old rule a Drive download kept nothing; see `docs/proxy.md`), and the same
retention owner that runs torrents runs it through `ProxyBacking`: same
budget, same window, same pass. What the torrent side had and the proxy
side lacked was a fetcher when nobody reads, a pin, and a claim that
outlives the run -- which is what this design adds. A Drive file **is** a
`ProxySource` (`sources/drive.rs`) with a header supplier that renews the
token in Rust, under a vouched key (`ProxyCache::entry_for_vouched_url`: the
file's URL with no token, in a namespace no `/proxy` key can reach), so
everything here applies to it unchanged.

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

*Departed:* no `Source` enum was built. A row keeps `infoHash`/`fileIdx`;
for a link or Drive file `infoHash` is the server's key name
(`proxy_download_key`, 64 hex), rows are told apart by `is_proxy_key`, and
the `ProxyPinKey` is derived again from the stored stream
(`proxy_pin_of_stream`).

## 3. The server side: five pieces, in dependency order

### 3.1 A proxy pin set that outlives the run

Mirror `ServerConfig::pins` exactly, because its semantics were argued
over and are right (`piece_store/pin_record.rs`): the embedder is the
authority, it hands the set in at boot, and **`None` is "nobody told me",
not "nothing is pinned"**.

```rust
pub struct ServerConfig {
    pub pins: Option<enginefs::piece_store::PinSet>,     // torrents, as today
    pub proxy_pins: Option<Vec<ProxyPinKey>>,             // new
}
/// What the embedder can name: the identity, not the hash. The server
/// derives the key directory from it the way `ProxyCache::entry` does,
/// so a change to the hashing never orphans a pin.
pub enum ProxyPinKey { Url { target, headers }, Drive { file_id } }
```

*Built as one field since 2026-10-01:* `pins: Option<Vec<PinKey>>` covers
torrents, links and Drive files (first built as a separate `proxy_pins`).
`ProxyPinKey` is in `proxy_downloads.rs`, its `headers` a `BTreeMap`.

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

*(As built: the pin set holds `key.parent()`, the entity's key directory,
and a `None` set exempts nothing.)*

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

*Built differently* (`proxy_downloads::Filler`): the walk is offset 0 to
the end in `FILL_STRIDE` (32 MiB) steps through `proxy_retention::read_through`
over the pin's quiet `ProxySource`, so a span the disk holds costs no
request. It checks the pin before every stride. A failed read is logged
(`download filler: a read failed; asking again shortly`) and asked again
after `FILL_RETRY` (15 s); the end is logged as `download filler: reached
the end`, with `whole`. A read the origin answers with something that is
not the span (a `200` or a `416`: its `If-Range` found the file changed, or
the file is shorter now) opens the source again, which is the
revalidation: a new identity or length is a new generation -- the open
retires the old one, the walk starts again from byte 0 of the new file,
logged once as `download filler: the origin's file changed; downloading it
again from the start`; an origin that no longer ranges is refused as at
pin time, the filler stops and the row's `error` says why (its `phase` is
then `checking`); the same entity, or no answer, is asked again after
`FILL_RETRY`. *(Until 2026-10-07 a validator change was asked again every
`FILL_RETRY` for as long as the pin stood.)*
One filler per key directory lives in `ProxyDownloads`, not in a
progress-logger slot.

Rate: unlimited by default; a later setting can cap it. The filler counts
as background traffic for the sharing light (it *is* "using your
connection while you are not watching"), through the same
`BackgroundTraffic` reading, by direction.

### 3.4 The embed API

The HTTP routes sketched here were built and removed the same day with every
other app-facing control route; the app speaks FFI. `DownloadInfo` gains
`source` and `playUrl` and keeps `length`, `downloaded`, `complete`,
`error`. *(As built: `downloaded` for a proxy entity is the bytes of the
chunks the disk holds (`Entry::held_facts`, a listing of the entity's
buckets per row -- exact, one disk walk per listing); `phase` is `ready`
when whole, `buffering` while a filler runs, `checking` for a pin with no
filler. There is no proxy twin of the `download progress` line: the filler
logs a failed read and its end.)*

### 3.5 Playing a finished download

A finished download is not a file: the player is handed a `url` stream
naming the server's own route, served off what is on the device. **It is
not the stream's `/proxy` URL**, as designed: that route keys the cache on
the player's own negotiation headers, so a player's request lands on a key
the filler never filled. It is the download's own media route,
`GET /downloads/{key}/stream`, which serves the pinned entry by range. For
Drive, `ServerHandle::open_drive_file` answers a complete pinned download
from the disk before it probes anything -- that route, the length and type
the disk holds, no token spent -- and a pin over an entry that is already
whole opens no source at all, so the app's launch-time re-pin works offline
too (`a_drive_download_plays_from_the_disk_with_no_network`).

## 4. Drive streaming, and read-ahead

**Built 2026-09-26** (`server/src/proxy_retention.rs`, `Prefetcher`), with
two departures from the design, which had one filler driven either by
"everything" (a pin) or by the owner's want set. There is no such `Fill` enum: the
download filler and the read-ahead are separate tasks over the same quiet
`ProxySource`, because the download is a pin's and the read-ahead is a
player's. And the want set alone was not enough: where the budget covers
the response whole the owner installs no policy and runs no pass -- an
unbounded torrent is simply left to its picker -- so that case is driven
from the player's delivered bytes and fetches the rest of the file, which
is what the picker would do. Read-ahead is on for a player's stream only:
a `/proxy` request carrying the app's player token, or a Drive session.
Measured in `tests/proxy.rs`
(`a_reader_with_a_rate_is_read_ahead_of_and_a_lone_range_is_not`) and
`tests/drive.rs` (`a_playing_drive_file_is_read_ahead_of`).

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
  splits rows by `Source`. *(As built: `pins()` hands `ServerConfig::pins`
  one `Vec<PinKey>` -- `pin_keys_in`, `pins_in`'s torrent rows and
  `proxy_pins_in`'s link and Drive rows, `None` if either half is unknown;
  `source_of` answers a `TorrentSource`, and there is no `Source` enum, §2.)*
* The replace and cancel logic, the notification and the library's
  Downloaded pill are source-agnostic already: they read `Entry::state`
  and `wants_pin`, never `info_hash`.

Related: `translated-sources.md` (the principle and `ByteSource`),
`read-pattern-retention.md` (the owner both backings run).
