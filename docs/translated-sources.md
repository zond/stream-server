# Translated sources: one byte source, translators in front of it, nothing on disk but the fetcher's own cache

Design, 2026-09-20. Agreed principle (zond): **only the thing that downloads
a file may store it** -- the torrent piece store, the proxy cache. Everything
that sits between a downloaded file and the player -- an archive, a disc
image, later a cloud drive or a network share -- is a *translation* of byte
ranges: the player asks for bytes `a..b` of the film, the translator works
out which bytes of the underlying file those are, and asks the fetcher for
exactly those. **A format that cannot answer that question is refused**, with
a message the player can show. It is never acceptable to download a whole
file to seek to its end.

This replaces the archive layer as it stands. It is written against the
code at stream-server `e93f3c4`, rqbit `29f4804e`, xtremio `473789c`.

## 1. Where the code is today

What the archive routes do now, per case:

| Case | Today |
|---|---|
| Stored ZIP or TAR member, in a torrent or behind a web link | Served by byte range from wherever the container is -- the piece store, or the proxy cache through one ranged request -- as a `MemberView` over a `ByteSource`. Nothing downloaded, nothing extracted, nothing written. (Step 2, 2026-09-20.) |
| Compressed or encrypted ZIP or TAR member | Refused: `415` with `{"refused", "message"}`, one sentence for the player. (Step 2.) |
| `tar.gz` | Refused whole: `415 noRandomAccess`. `archives/tgz.rs` is gone. (Step 2.) |
| Stored RAR member, in a torrent or behind a link, one volume or a set | Served by byte range from wherever the volumes are -- one extent per part, per volume. Nothing downloaded, nothing extracted, nothing written. `archives/rar.rs` is gone. (Step 3, 2026-09-20.) |
| Compressed, encrypted or solid RAR member | Refused: `415` with `{"refused", "message"}`. An encrypted *stored* member too, whose bytes the crate could map. (Step 3.) |
| A RAR set with a volume missing, or a chain still open after the last | Refused: `422`, naming the volume it wanted. (Step 3.) |
| Stored (COPY) 7z member, in a torrent or behind a link | Served by byte range, like a stored ZIP member: `translators::sevenz::SevenZ` over `sevenz-rust2`'s `Archive::read`. Nothing downloaded, nothing extracted, nothing written. (Step 5, 2026-09-20.) |
| Compressed, solid or encrypted 7z member | Refused: `415` with `{"refused", "message"}`. A block that is not COPY and holds several files is `Solid` rather than `Compressed`: a decoder could not enter at it either. (Step 5.) |
| A multi-part 7z (`.7z.001`, `.7z.002`, ...) | Refused: `422`, saying it is one file cut up. (Step 5.) |
| An origin that will not serve ranges | Refused, `501`, with a sentence: serving it would mean downloading the archive. (Step 2.) |
| Multi-volume RAR (`rarUrls` with several entries) | The list **is** the volume list, in order; from a torrent the volumes are the named file's siblings by the two naming rules. (Step 3, 2026-09-20.) |
| ISO 9660 or UDF image, in a torrent or behind a web link | Served by byte range like a stored ZIP member: `/iso/create` and the `torrent:` form, `translators::iso::Iso` over `server/src/images/`. A UDF metadata partition map (the first thing a real Blu-ray image hits) is refused `415 unsupported`, naming the map. (Step 6, 2026-09-20.) |

The cost of that shape is not only disk: an extraction has to reach the
byte the player wants before it can be served, so a seek to the end of a
compressed member is a download of the whole member, and a request that
lands before the extraction has got there waits on it.

What exists that the new shape keeps:

* `archives::window::MemberWindow<R>`: a member as a reader over the
  archive, in the member's own coordinates. It is the single-extent case of
  the `MemberView` below.
* The ZIP central-directory and local-header reading in `archives::zip`
  (data offset from the member's *local* header, because its extra-field
  length may differ from the central directory's).
* `archives::sessions::Sessions`: a leased, idle-swept map. It survives, but
  a session no longer owns files. (Now `translators::session::Sessions`,
  merged with the session it leases -- step 4.)
* `routes::compat::resolve_file_idx` with `fileIdx` / `fileMustInclude`: the
  member selection stremio-core's `rarUrls`/`zipUrls` contract needs.
* `TorrentMemberStream`: the stream registration a torrent-backed body
  rides in, so the torrent is kept running while the member is read.
* The proxy cache's building blocks: `ProxyCache::entry(...)` (one entity per
  URL and player headers), `Entry::look_up(range) -> Cached` (what the disk
  holds from `first`), `Cached::body()`, `Cached::remaining_range()`, the
  narrowed fetch with `If-Range` on the `EntityValidator`, and
  `Entry::fill(total, content_type, validator, body_start) -> Filler`
  wrapped as `proxy_cache::Filling` around the origin's body.
* librqbit's per-stream reader: `Engine::try_get_file_with_intent(file,
  offset, priority, Fetching::Streaming, profile)` -> `FileHandle`, which is
  `AsyncRead + AsyncSeek`, registers the position with the retention pass
  and follows a seek.
* In the format crates: `async_zip` 0.0.18 (central directory, entry
  `compression()`, `header_offset`) -- **not used in the end**: the ZIP
  index is parsed by hand, because what is wanted is a stated number of
  bytes at stated offsets, counted, and a `BufReader`'s fills are its own
  business. The crate stays a dependency, for writing the fixtures; `unrar-rs` 0.10.5's
  `RarArchive::parse_volume_facts(Read + Seek)` and
  `stored_layout::{StoredLayoutBuilder, StoredMember, StoredMemberPart,
  MemberEligibility, MappedSlice}`, built for exactly this (stored members
  mapped to physical ranges across a multi-volume set, eligibility per
  member, encryption reported rather than hidden); `sevenz-rust2` 0.22.2's
  `Archive::read(Read + Seek)` with `pack_pos()`, `pack_sizes()`,
  `StreamMap::{pack_stream_offsets, block_first_pack_stream_index,
  file_block_index}` and `Block::coders` (method id `[0x00]` is COPY),
  documented by the crate as "used for calculating byte offsets when
  streaming uncompressed (COPY) content"; the `tar` crate for headers.

## 2. The abstraction

Three things, and the routes compose them. Everything below is async,
`Send`, and holds no file of its own.

### 2.1 `ByteSource`: a file somebody else fetches

```rust
/// A file whose bytes are fetched and kept by something else. Random
/// access by construction: a source that cannot answer `read_at` for an
/// arbitrary offset is not a ByteSource, it is a refusal.
#[async_trait]
pub trait ByteSource: Send + Sync {
    /// The file's length. Known up front for every source there is (a
    /// torrent file, an HTTP entity with Content-Length, a member view).
    fn len(&self) -> u64;
    /// A short name for logs and errors: never a URL (see the proxy's
    /// logging rule), never a credential.
    fn describe(&self) -> String;
    /// `buf.len()` bytes at `offset`, or fewer at the end of the file.
    /// For indexes: small, scattered reads.
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// A reader positioned at `offset` for a long read: the body of a
    /// response, or a parser that seeks about. Seekable, like every other
    /// handle on a fetched file: a seek is a seek of the piece store, or
    /// one new ranged request through the proxy cache. `hint` is advice
    /// about how much is coming -- an HTTP source spends it on the Range's
    /// last byte; a torrent spends it on nothing, since the engine works
    /// the lookahead out from the intent -- and never a cap.
    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn AsyncSeekableReader>>;
}
```

Two methods rather than one because the two uses differ: an index is a
handful of reads at offsets the format dictates (the end of a ZIP, the start
of a RAR volume, sector 16 of an ISO), and a body is one long read the
fetcher should treat as a stream -- with a lookahead on a torrent and one
ranged request on HTTP -- not thousands of `read_at`s.

Implementations, in order of need:

* **`TorrentFileSource { engine, file_idx }`.** `len` from the file list.
  `read_at` seeks a short-lived `Fetching::Streaming` handle -- or better,
  keeps one handle per source and seeks it under a mutex, since the piece
  store serves a seek cheaply. `open` is `try_get_file_with_intent(file,
  offset, priority, Streaming, profile)` -- **Streaming, not `Download`**:
  the `torrent:` form today opens its reader with the 256 MiB download
  lookahead (`routes/archive.rs`), which is wrong for a player and is what
  this design fixes for free. A source registers the torrent stream
  (`TorrentMemberStream`) for as long as it is held, as the route does now.
* **`ProxySource { entry: proxy_cache::Entry, client, total, validator, content_type }`.**
  Built by one `HEAD` (or a `GET` of `bytes=0-0`) through the proxy's own
  request builder, which is how it learns `total`, the validator and
  whether the origin honours `Range` -- **an origin that answers `200` to a
  ranged request is refused here**, since serving it would mean downloading
  the file. `read_at` and `open` are the proxy route's own miss path
  turned into a reader: `entry.look_up(range)` -> serve `Cached::body()` for
  what the disk holds, then the narrowed fetch with `If-Range` for the rest,
  wrapped in `Filling` with `entry.fill(...)` so the bytes land in the cache.
  This is the one piece of new plumbing of any size, and it is a
  refactor of code the proxy route already has, into a function both can
  call. The cache stays the owner of the bytes; the retention owner keeps
  reclaiming them; `/stream-numbers.json` keeps answering for them.
  Credentials travel as the proxy already carries them (`h=` request
  headers, kept out of the cache key's credential set and out of logs).
* **`MemberView`** (below) is a `ByteSource` too, so a translator can sit on
  another translator's member: an ISO inside a RAR, a ZIP inside a torrent
  inside nothing. Nothing is built for that case; it falls out.
* **Test sources:** `MemorySource(Bytes)` and `FileSource(File)`, plus a
  counting wrapper that records every `read_at`/`open` for the tests that
  prove "no byte was read that the range did not need".

Later, and outside this plan: `DriveSource` (Google Drive; ranged `GET`
with the addon's OAuth header, which is a `ProxySource` with a header
supplier that refreshes), `SmbSource`, `NfsSource`. That they are the same
seam is the point of having it.

### 2.2 `Translator`: what a container says about the bytes inside it

```rust
/// One member of a container, as ranges of the container's own bytes.
pub struct Member {
    pub name: String,        // as the container states it
    pub len: u64,            // unpacked length, which for a direct member equals the sum of its extents
    pub body: Body,
}

pub enum Body {
    /// The member's bytes are these ranges of these sources, in order.
    Direct(Vec<Extent>),
    /// The member cannot be served by range, and this is why.
    Opaque(Refusal),
}

pub struct Extent {
    pub source: usize,       // index into the translator's sources (volume number for a set)
    pub offset: u64,         // where in that source
    pub len: u64,
}

pub enum Refusal {
    Compressed { format: &'static str, method: String },  // deflate, LZMA, RAR "Normal", gzip
    Encrypted,                                            // any encryption, until there is a password UX
    Solid,                                                // a solid RAR/7z block holding several files
    Malformed(String),
    NoRandomAccess { format: &'static str },              // tar.gz as a whole
}

#[async_trait]
pub trait Translator: Send + Sync {
    /// What this reads, for the sentences a refusal is made of.
    fn format(&self) -> &'static str;
    /// Read the container's index from its sources. Reads what the format
    /// needs and nothing else: the reads are bounded, small and counted.
    ///
    /// Written as an associated function here; it takes `&self` as built,
    /// because that is what makes the trait object-safe, and the route
    /// has a format prefix in a URL and needs *a* translator for it at
    /// run time. Every implementation is a unit struct.
    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal>;
}

pub struct Index { pub members: Vec<Member> }
```

Rules every translator obeys:

1. **Index reads are small and bounded.** A translator may read headers,
   directories and trailers. It may not read a member's data to index it.
   The test suite counts bytes read per index against a per-format bound
   -- and asserts **by range** that no read overlapped the member's own
   data, which a byte count alone would not catch -- through a
   `Budget` the translator reads everything through (`crate::translators`,
   after `crate::images`). This, and not the shape of the handle, is what
   keeps an index an index: a reader is seekable and uncapped, like every
   other handle on a fetched file (§2.1).
   (ZIP: the end record -- the last 22 bytes, widening to the 64 KiB
   comment window only when they are not it -- plus the directory plus one
   30-byte local header per *stored* member, since a compressed one is
   refused without reading its header at all; RAR: the headers of each volume;
   7z: the signature header and the packed header at the end; ISO: the
   descriptors and the directory tree; TAR: one 512-byte header per member,
   the data skipped by size).
2. **A member is `Direct` or it is refused. There is no third state.** No
   "extract it then", no partial decode, no "serve sequentially but refuse
   seeks". A compressed film is a refusal with a reason the player shows.
3. **A translator never writes.** Not to disk, not to a temp file, not to
   the cache root. `.archives` ceases to exist -- since step 4 nothing in
   the workspace writes it, and the suite asserts the directory does not
   appear after a test that plays a member of every format.
4. **Verification is the fetcher's.** A torrent's bytes are piece-verified
   by librqbit; a proxy entity is what the origin served. A translator does
   not checksum a member on the way through: a CRC over a member is a read
   of the whole member, which is the thing this design forbids. The one
   exception is a *header* checksum a format carries for its own index,
   which is small and is checked.

The formats, and what each one's translator is:

* **ZIP.** Read the end-of-central-directory record (search the last 64 KiB
  plus 22 bytes for the signature; zip64 locator if present), then the
  central directory, then for the selected member its local header for the
  data offset. `Stored` -> `Direct(one extent)`; any other method ->
  `Compressed`; general-purpose bit 0 -> `Encrypted`. Multi-part ZIP
  (`.z01`) -> `Malformed("split zip")` for now. `async_zip` can read the
  directory over `AsyncBufRead + AsyncSeek`; if its buffering reads more
  than the bound allows, the directory is parsed by hand -- it is a fixed
  layout and the code for the local header already is.
* **TAR.** Walk 512-byte headers from offset 0, skipping each entry's data
  by its size (rounded to 512): one small read per member. Every regular
  file is `Direct(one extent)`. GNU long names and PAX headers are read as
  the entries they are. **`tar.gz` is `NoRandomAccess`**: `archives/tgz.rs`
  goes.
* **RAR.** Per volume: `RarArchive::parse_volume_facts(reader, None)` over a
  blocking `Read + Seek` shim of the source (the parser is synchronous;
  the shim runs on the blocking pool and reads through the runtime handle;
  the facts walk seeks past member data by `compressed_size`, so it reads
  headers only). Then `StoredLayoutBuilder::add_volume(volume, &facts)` for
  each volume in order, and `members()`: a `StoredMember` whose eligibility
  `routes_direct()` and whose parts all have `logical_offset: Some` is
  `Direct(parts as extents: (volume, data_offset, data_size))`;
  `EncryptedStore` -> `Encrypted`; `Ineligible(reason)` -> `Compressed` or
  `Malformed` by the reason; `is_solid` volumes -> `Solid`;
  header-encrypted volumes (`is_encrypted` at the volume level) ->
  `Encrypted` before any member is looked at. **Multi-volume sets are the
  ordinary case** (`.part1.rar`/`.part2.rar`, or `.rar`/`.r00`/`.r01`): from
  an addon, the `rarUrls` list *is* the volume list, in order; from a
  torrent, the volumes are the sibling files of the named one, by the two
  naming rules, in the order the rules give (a set that is missing a
  volume is `Malformed`, named). The layout builder reports a chain that is
  still open (`ProvisionallyDirect`) -- that is a member whose later volume
  has not been added yet, and since every volume is added before the index
  is answered, it is `Malformed` if it is still open then.
* **7z.** `Archive::read(reader, &Password::empty())` over the same blocking
  shim (7z's header lives at the end: two small reads). A file whose block
  (`stream_map.file_block_index`) has exactly one coder with method id
  `[0x00]` (COPY) and no encryption is `Direct(one extent)` at
  `32 + pack_pos + pack_stream_offsets[block_first_pack_stream_index[block]]
  + (sum of the sizes of the files before it in the same block)`. Anything
  else is `Compressed` (LZMA2 etc.), `Encrypted` (AES coder) or `Solid`. In
  practice a 7z of a film is LZMA2 and will be refused; the translator is
  cheap and honest, so it is kept.
* **ISO.** No crate in the registry does this; it is a small parser. ISO
  9660: the primary volume descriptor at byte 32768 (sector 16), the root
  directory record in it, directory records walked breadth-first (each
  record: extent LBA, data length, flags, name; Joliet's supplementary
  descriptor for real names; Rock Ridge `NM` where present). A file is
  `Direct(extents)` -- an ISO 9660 file is one contiguous extent, and a file
  over 4 GiB is several directory records for one name, which is several
  extents: the model above carries that as written. **UDF** is the second
  half and matters more than it looks: DVD-Video discs are ISO 9660/UDF
  bridge (the 9660 tree is enough), but **Blu-ray images are UDF 2.50
  only**, with no 9660 tree at all. UDF: anchor at sector 256, the volume
  descriptor sequence, the file set descriptor, file entries whose
  allocation descriptors are extents -- again `Direct(extents)`. **Both are
  written** (`server/src/images/`, 2026-09-20: two independently written
  parsers, which index a real bridge image to identical extents). What UDF
  refuses by name is a **metadata partition map** -- UDF 2.50 remaps logical
  blocks through a metadata file, so an extent computed without it is the
  wrong range of the image -- and that is the gap a real Blu-ray image hits
  first, and the next UDF increment.

### 2.3 `MemberView`: a member as a file

`MemberView { member: Member, sources: Vec<Arc<dyn ByteSource>> }` implements
`AsyncRead + AsyncSeek` -- and `ByteSource`, for nesting. A seek to `p`
finds the extent holding `p` (binary search on the running offsets) and,
when that is the extent a reader is already open on, **seeks that reader**
to `extent.offset + (p - extent.start)`; a seek into another extent, or
crossing one's end, opens the next source's reader. What keeps a read
inside the member is the run of the extent it is in, not the source
stopping: a source's reader is a handle on the whole container.
`MemberWindow` is this with one extent, and is replaced by it.

For the response body, the stream route's own range framing is reused:
`Content-Length`, `Content-Range`, `206`/`416`, `HEAD`, the same functions
`routes::stream` uses, over a `MemberView` instead of a `FileHandle`. Nothing
about a member's HTTP behaviour is allowed to differ from a plain file's.

### 2.4 Sessions: an index, in memory, leased

`translators::session::Sessions<TranslatedSession>` (was
`archives::sessions`) stays as the map; the
session becomes `{ origin, sources, index, selected: Option<usize> }` and
owns **no file**. A *torrent-backed* session holds the hash and path
rather than the source: a `TorrentFileSource` registers a stream for as
long as it lives, and holding one for the session's ten idle minutes
would keep a torrent the viewer left ten minutes ago running, so each
body opens its own -- a file lookup and a reconcile, not a fetch. The
index, which is what was expensive to read, is what the session is for. It is created by `/{fmt}/create` (from URLs) or on first use by
the `torrent:` form (from a torrent file and its sibling volumes), leased
by every response body, and swept `SESSION_IDLE_TIMEOUT` after the last
lease as now. What sweeping frees is memory and, for a torrent, the stream
registration; for a proxy source, nothing at all -- the cache's bytes are
the cache's, under its own retention, exactly as if the player had fetched
them through `/proxy`.

## 3. Routes and the contract with the client

Nothing the client sends changes.

* `POST/GET /{rar|zip|7zip|tar|iso}/create` with the `lz`-encoded
  `{ urls, fileIdx, fileMustInclude }` (what stremio-core builds from
  `rarUrls`/`zipUrls` -- `types/resource/stream.rs`): every URL becomes a
  `ProxySource` (probe, refuse a non-ranging origin with `501` and a
  message), the translator for `{fmt}` indexes them, `resolve_file_idx`
  picks the member, the session is inserted under the key, and a `GET`
  redirects to `./stream/{key}/{member}` as today. A selected member that
  is `Opaque` answers **`415 Unsupported Media Type`** with a JSON body
  `{ "refused": "<Refusal>", "message": "<one sentence>" }`; the player
  shows the message (xtremio's `archive_sniff` already has the place for it).
* `GET/HEAD /{fmt}/stream/{key}/{member}`: `MemberView` + the shared range
  framing. Same on the LAN listener (`archive_stream_routes`): a Cast
  receiver reads a member of a session loopback created, and since the
  session holds no file this is unchanged.
* The `torrent:` key form (`torrent:<hash>/<path in torrent>`): the source
  is `TorrentFileSource`, the format is the path's suffix, sibling volumes
  are found in the torrent's file list; otherwise identical. `iso` joins
  the prefixes.
* `/{fmt}/create/{key}` (client-chosen key) keeps its `409` rule.

Error mapping, once, in one function: `Refusal::Compressed|Encrypted|Solid`
-> `415`; `NoRandomAccess` -> `415`; `Malformed` -> `422`; an origin that
will not range -> `501` with `refused: "noRanges"` (it is *this server* that
declines to do the work), and a build with no reader for the format -> `501`
with `refused: "noReader"`, whose message names a cargo feature and is for
whoever built the app, never for a viewer -- two different things that shared
one status and one untyped `error`, so a client had to match English to tell
them apart;
a source error mid-body is the body's error, as for a plain stream.

## 4. What is deleted

**All of it is gone**, as of 2026-09-20. What actually went, by commit:

| Gone | Lines | Why |
|---|---|---|
| ~~`server/src/archives/torrent.rs`~~ (`TorrentArchives`) | 420 | **Step 2.** Sessions hold indexes, not extractions; the `torrent:` form joins the ordinary session map. |
| ~~`server/src/archives/tgz.rs`~~ | 289 | **Step 2.** gzip has no random access. |
| ~~`server/src/archives/window.rs`~~ | 184 | **Step 2.** Generalised into `MemberView`. |
| ~~`server/src/archives/{zip,tar}.rs`~~, `streams_from_a_reader`, `get_archive_reader_from_stream` | 573 | **Step 2.** Both formats are translated. |
| ~~`rar::RarHandler`~~ (file-based extraction) and the `.rar` arm of the reader dispatch | 431 | **Step 3.** `archives::RAR_DISABLED_ERROR` moved to `routes::archive`, its only caller. |
| ~~`server/src/archives/cache.rs`~~ (`ProgressiveCache`, `VolumeRoom`, `ABANDONED_AFTER`, the reader/writer notify dance the AGENTS gotcha described) | 1245 | **Step 4.** No extraction, so no extraction cache. |
| ~~`server/src/archives/source.rs`~~ (`ArchiveSource`, `ArchiveSession`, `MemberCaches`, the `NamedTempFile` ownership) | 363 | **Step 4.** A source is a `ByteSource`; the origin string survives on `TranslatedSession`. |
| ~~`server/src/archives/sevenz.rs`~~ (`SevenZHandler`, the block decoder into a cache) | 470 | **Step 4**, once `translators/sevenz.rs` landed in step 5. |
| ~~`server/src/archives/mod.rs`~~ (the `ArchiveReader` trait, `OpenedMember`, `AsyncSeekableReader`, `CacheConfig`, `scratch_file`, `archive_suffix{,_from_magic}`, `SCRATCH_DIR_NAME`, `sweep_scratch`) | 281 | **Step 4.** Nothing dispatches by suffix and nothing writes. |
| `routes/archive.rs`: `download_archive`, `DownloadRoom`, `SNIFF_BYTES`, the download timeouts, `resolve_source`, `archive_cache_config`, `select_archive_file`, `url_file_name`, `create_downloaded`, the old `stream_file`, `is_storage_full`, and the `urls.len() > 1 -> 501` | 716 (of 45 added back) | **Step 4.** Nothing is downloaded by this layer. |
| `AppState::archive_cache` | ~12 | **Step 4.** One session map, holding indexes. |
| The `.archives` entry in `piece_store::sweep::NOT_OURS`, and the `piece_store/mod.rs` comment naming it | ~10 | **Step 4.** Nothing writes the name. See below. |

**Kept, and moved**: `archives/sessions.rs` (418 lines) is the leased,
idle-swept map both layers used. It is now `translators/session.rs`,
merged with `TranslatedSession` -- one module for "an indexed container,
leased, swept when idle" -- and `SESSION_IDLE_TIMEOUT` came with it from
`archives/mod.rs`.

**Kept, deliberately**: one constant and about fifteen lines in
`server/src/lib.rs`, `LEGACY_ARCHIVE_SCRATCH_DIR`, which deletes
`<cacheRoot>/.archives` at launch. The design says the directory ceases to
exist, and it does -- for a fresh install. For an **upgraded** one it is
already there, holding about twice the size of every archive played since
that build's last clean exit, and nothing else would ever take it: no
retention owner speaks for those bytes, `GET /cache.json` does not count
them, and the piece store's legacy sweep walks
`<cacheRoot>/rqbit-downloads`, one level *below* where `.archives` sits.
(Which is also why removing it from `NOT_OURS` frees nothing by itself:
that list is about names directly under the download dir, and `.archives`
was never one of them. The exemption was never load-bearing; it is removed
because the name means nothing now.)

About 4,700 lines out across the four steps; the new layer is roughly
1,900 in (sources ~900, translators: zip ~250, tar ~200, rar ~450 incl.
the volume naming and the blocking shim, 7z ~200, iso ~200 over
`images/`; view ~350; route glue net negative).

## 5. Steps, in order, each shippable

Each step lands green on master with its own tests, revert-proven hunk by
hunk, behind no flag: the old path is removed as its replacement lands,
never kept beside it.

1. **`ByteSource`, `MemberView`, `TorrentFileSource`, `ProxySource`.** *(Landed 2026-09-20.)*
   New module `server/src/sources/`. Tests: `MemberView` over several
   extents across two `MemorySource`s (reads, seeks, boundaries, `SeekFrom::End`);
   `ProxySource` against the test origin: the second read of a range is
   served from the cache and the origin is asked once, the cache entry is
   the same one `/proxy` would use for that URL and headers (assert on the
   entity directory), a non-ranging origin is refused; `TorrentFileSource`:
   a read registers a stream and a seek reaches the piece store (the
   existing `an_archive_body_keeps_its_torrent_running_while_it_is_open`
   is the model). The proxy route's miss path is refactored to call the
   same function -- with its existing tests unchanged, which is the proof
   the refactor is one.
2. **ZIP and TAR translators; the routes switch to `MemberView`; compressed
   members refused; `tgz.rs`, `window.rs`, the extraction paths for zip/tar
   deleted.** *(Landed 2026-09-20.)* Tests: index read bounds (counting source); stored member end
   to end from a torrent and from a URL (existing fixtures); deflate member
   -> `415` with the message; `tar.gz` -> `415`; the existing zip/tar
   fixtures under `server/tests/archive.rs` and `embed.rs` keep passing
   where they test stored members and are rewritten where they tested
   extraction. After this step **`.archives` is created by nothing for
   ZIP/TAR**; assert the directory does not exist after the suite.
3. **RAR translator: single volume, then multi-volume.** *(Landed 2026-09-20.)* The blocking shim;
   `parse_volume_facts` + `StoredLayoutBuilder`; the two naming rules for
   sibling volumes in a torrent; `rarUrls` order for URLs. Fixtures:
   `archives/rar.rs` already builds store-method RAR5 archives by hand for
   its tests; extend the builder to emit a split set (the RAR5 split flags
   and the per-part sizes are documented in the crate's `StoredMemberPart`).
   Tests: a stored film across three volumes in a torrent is served whole
   and by range with reads landing in the right volumes; a compressed RAR
   -> `415 Compressed`; a header-encrypted one -> `415 Encrypted`; a set
   missing its middle volume -> `422`. Delete `RarHandler`'s extraction
   and the `rar` feature's file path; the feature flag stays (licence).
   **This is the step that changes what a user sees tonight.**
   Done as written, with three things worth recording. The volume list is
   what names a session (`routes::archive::set_origin` joins every URL):
   two sets can share a `.part1.rar` and differ after it, and a session
   found by the first URL alone would answer one set's index over
   another's bytes. `Translator::volumes(named, siblings)` is the hook the
   `torrent:` form finds a set through -- a default of "just this file"
   for every other format. And a **member whose only checksum is
   BLAKE2sp, or none, is served**: `unrar-rs` marks such a member
   ineligible because *it* cannot verify one out of order, which is its
   business and not this server's (§2.2.4). What the layout API refuses
   that real releases do use is written up in the step's report: nothing
   found so far beyond the refusals above.
4. **Delete the rest.** *(Landed 2026-09-20, after step 5, which is what
   left it with no callers.)* `cache.rs`, `source.rs`, `sevenz.rs` and
   `mod.rs` -- the whole `archives/` module -- the download path,
   `sweep_scratch`, `CacheConfig`, `AppState::archive_cache`, and the
   `.archives` entry in `piece_store::sweep::NOT_OURS`. §4 above is the
   inventory of what actually went. Two things differ from the sketch.
   `archives/sessions.rs` is **kept**, merged into
   `translators/session.rs`: the translated sessions are leased out of it.
   And the launch removal of `<cacheRoot>/.archives` is **kept** as fifteen
   lines in `lib.rs` rather than deleted with `sweep_scratch`, because
   removing it from `NOT_OURS` frees nothing -- that list names directories
   directly under `<cacheRoot>/rqbit-downloads`, and `.archives` sat a
   level above it -- so deleting both would leave an upgraded install's
   extraction cache on the disk for ever, counted by nobody. Docs: AGENTS.md
   (workspace map, the progressive-cache gotcha is gone whole, the "every
   byte has an owner" list loses `.archives` and gains "a translated
   container's member owns nothing"), README (the archive section and the
   project tree), known-issues (the `.archives` mentions and the "Dead
   modules" entry).
5. **7z translator** (COPY blocks direct, all else refused) and delete
   `sevenz.rs`'s extraction. *(Landed 2026-09-20: `translators/sevenz.rs`,
   `Format::SevenZ` translated. The offset is the crate's own -- `32 +
   pack_pos + pack_stream_offsets[block_first_pack_stream_index[block]]`,
   where `ArchiveReader::build_decode_stack` seeks before decoding -- plus
   the sum of the sizes of the files before it in the same block, which is
   sound because a COPY block's output **is** its packed bytes; proven by
   reading the fixtures' members back, including three files sharing one
   COPY block. Refusals: an AES coder anywhere in the block, or an index
   the crate wants a password for, is `Encrypted`; a block that is not COPY
   and holds several files is `Solid` (chosen over `Compressed` because it
   says the stronger thing -- a decoder could not enter there either);
   one that holds a single file is `Compressed`, naming the coder chain; a
   multi-part set is `Malformed`, whether it arrives as several URLs or as
   a first part whose stated index is past its own end. The signature
   header is read by hand before the crate sees the file, because a 7z
   whose start-header fields are all zero sends `Archive::read` scanning
   backwards over the last mebibyte **one byte at a time**, which over a
   piece store is a million round trips. A 256 KiB stored fixture indexes
   for 191 bytes, none of them the member's.)*
6. **The images module behind `ByteSource`** -- it is written, with its own
   small `ImageReader` trait (`len` + `read_at`), so what is left is an
   adapter, the refusal mapping of §3, and the `iso` route prefix, sniff signature already in
   xtremio's `archive_sniff` (`CD001` at 32769). Fixture: a tiny image
   built in the test (PVD + root record + one file, 2048-byte sectors) and,
   if `genisoimage`/`xorriso` is on the CI runner, a real one. Then **UDF**
   as its own step with a Blu-ray-shaped fixture. *(Landed 2026-09-20:
   `translators/iso.rs`, `Format::Iso`, `/iso` in the prefixes. The
   mapping needed one variant the sketch above lacks, `Refusal::Unsupported
   { format, what }` (`415`, kind `unsupported`), for a well-formed image
   using a structure the parser names and does not read -- the UDF
   metadata partition map first of all, so a Blu-ray image today answers
   `415` with "this UDF image uses a type 2 partition map (...), which
   remaps logical blocks; that is not supported yet". `images::Refusal` maps
   as `Unsupported`/`Encrypted` -> `415`, `Malformed`/`NotAnImage` ->
   `422`, `Unreadable` -> `Malformed` carrying the read error, as
   `translators::Budget` words one. The images module's own 8 MiB
   `Budget` is the bound; the translator adds no second counter.
   `images::fixtures` is no longer `#[cfg(test)]`, so `server/tests/iso.rs`
   can put the same images in a torrent and behind a URL. The real-tool
   image test skips when no writer is installed. The UDF increment that
   is still open is the metadata partition map itself.)*
7. **xtremio**: a URL stream whose first bytes say RAR/ZIP/7z/ISO is sent to
   `/{fmt}/create` with `urls: [url]` instead of shown the message; the
   message is shown when the server answers `415`/`422`/`501`, with the
   server's own sentence. A torrent whose selected file is an archive or
   an image goes the same way through the `torrent:` form. `StreamKind.archive`
   already exists for addon-declared archives; this is the sniffed case.

Steps 1-4 are the refactor zond asked about; 5-7 are what the seam then
buys. Sizes, in the units this project has been working in: steps 1-2 one
agent run, step 3 one to two, step 4 half, steps 5-6 one each, UDF one,
step 7 half.

## 6. Decisions taken here, for zond to overrule

* **An origin that does not honour `Range` is refused.** The alternative is
  downloading the archive, which is the thing this design exists to stop.
  Debrid hosts and CDNs range; a few addon-hosted files may not.
* **No checksum on a served member.** Torrent bytes are piece-verified;
  proxy bytes are what the origin sent, as with any `/proxy` stream. A
  member CRC is a read of the whole member.
* **Encryption is refused everywhere**, including RAR's `EncryptedStore`
  whose mapping the crate can do: there is no password UX, and a wrong
  guess is ciphertext to the player.
* **Solid blocks are refused** even when every file in them is stored:
  a stored solid block is byte-identical and *could* be mapped, but the
  layout crate marks the member ineligible and the case is rare enough that
  honesty beats cleverness.
* **Index caching is memory only**, in the session, for the session's
  lease plus idle time. A re-index after a sweep is a few small ranged
  reads that the proxy cache answers from disk.
* **The `torrent:` form finds volumes by the two naming rules and nothing
  else** (`name.partN.rar` ascending N; `name.rar`, `name.r00`, `name.r01`
  ...). A set named any other way is one volume, and if that volume's chain
  is open it is `Malformed`, saying which volume it wanted.
* **UDF comes after ISO 9660.** Blu-ray images are UDF-only; until that
  step a BD image is refused with a message that says so.

## 7. What this does not do

**Block-compressed formats with a seek table are declined too, deliberately**
(zond, 2026-09-20). Some codecs do offer random access -- an xz stream
written in multi-block mode carries an index, zstd has a seekable format
with a table in a skippable frame, and a 7z *block* is independently
decodable -- and a third body kind (a block list, decoded one block at a
time in memory) would fit this design without storing anything. It was
priced and declined on coverage, not on taste: block compression buys entry
points, and a seek costs decoding from the nearest one, so the value is
entirely about block size. RAR's and deflate's compressed modes have no
entry points at all; a film in a 7z is normally *one* block, so its only
entry is byte zero; and the formats with real indexes (tar.xz, tar.zst) are
how software is shipped, not films -- which arrive stored in RAR or ZIP,
which this design already serves. The cost would have been ~1200-1600 lines
plus C-linked decoders in four build targets. **What replaces it is
evidence**: every refusal names the container and the exact method, so a
field log says which codec actually cost a playback, and one that keeps
appearing is the one to build for.

It does not decode. A compressed film stays unplayable through this
server, by decision. It does not transcode, does not remux, and does not
turn a DVD's `VIDEO_TS` folder into one stream: an ISO's members are its
files, and a player that can open a `VIDEO_TS/VTS_01_1.VOB` by URL gets
exactly that file. Playing a DVD *as a DVD* (menus, titles across VOBs) is
mpv's `dvd://` over a local path, which is a different feature over the
same `MemberView`.
