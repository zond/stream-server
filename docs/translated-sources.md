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
| Stored ZIP or TAR member inside a torrent | Served by byte range from the torrent through `archives::window::MemberWindow`. No copy. (Landed 2026-09-20, review #99.) |
| Any RAR inside a torrent | Refused, `501`: the `torrent:` form is ZIP-only (`archives::streams_from_a_reader`), and `rar::RarHandler` reads a `std::fs::File`. |
| Compressed member, any container, any source | Extracted whole into `archives::cache::ProgressiveCache` under `<cache root>/.archives`: a second copy of the film, bounded by the volume floor, swept when idle. |
| Archive behind a web link (`/{fmt}/create` with `urls`) | The **whole archive is downloaded** to `.archives` first (`routes::archive::download_archive`, `DownloadRoom`), then read as a file. A compressed member is then extracted beside it: two copies. |
| `tar.gz` | Always extracted; gzip has no random access. |
| Multi-volume RAR (`rarUrls` with several entries) | Refused, `501`. |
| ISO | Nothing. |

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
  a session no longer owns files.
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
  `compression()`, `header_offset`); `unrar-rs` 0.10.5's
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
    /// A reader positioned at `offset` for a long sequential read:
    /// the body of a response. `hint` is how far the caller expects to read,
    /// which is what a torrent turns into a lookahead and an HTTP fetch
    /// into a Range end.
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
    /// Read the container's index from its sources. Reads what the format
    /// needs and nothing else: the reads are bounded, small and counted.
    async fn index(sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal>;
}

pub struct Index { pub members: Vec<Member> }
```

Rules every translator obeys:

1. **Index reads are small and bounded.** A translator may read headers,
   directories and trailers. It may not read a member's data to index it.
   The test suite counts bytes read per index against a per-format bound
   (ZIP: the end-of-central-directory search window plus the directory plus
   one local header per selected member; RAR: the headers of each volume;
   7z: the signature header and the packed header at the end; ISO: the
   descriptors and the directory tree; TAR: one 512-byte header per member,
   the data skipped by size).
2. **A member is `Direct` or it is refused. There is no third state.** No
   "extract it then", no partial decode, no "serve sequentially but refuse
   seeks". A compressed film is a refusal with a reason the player shows.
3. **A translator never writes.** Not to disk, not to a temp file, not to
   the cache root. `.archives` ceases to exist.
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
  allocation descriptors are extents -- again `Direct(extents)`. UDF is a
  separate step after 9660, and until it lands a UDF-only image is
  `Malformed("UDF only; not yet")`, named as such.

### 2.3 `MemberView`: a member as a file

`MemberView { member: Member, sources: Vec<Arc<dyn ByteSource>> }` implements
`AsyncRead + AsyncSeek` -- and `ByteSource`, for nesting. A seek to `p`
finds the extent holding `p` (binary search on the running offsets),
`open`s that extent's source at `extent.offset + (p - extent.start)` with a
hint of `extent.len - ...`, and reads; crossing an extent's end closes
that reader and opens the next. `MemberWindow` is this with one extent, and
is replaced by it.

For the response body, the stream route's own range framing is reused:
`Content-Length`, `Content-Range`, `206`/`416`, `HEAD`, the same functions
`routes::stream` uses, over a `MemberView` instead of a `FileHandle`. Nothing
about a member's HTTP behaviour is allowed to differ from a plain file's.

### 2.4 Sessions: an index, in memory, leased

`archives::sessions::Sessions<TranslatedSession>` stays as the map; the
session becomes `{ sources, index, selected: Option<usize> }` and owns
**no file**. It is created by `/{fmt}/create` (from URLs) or on first use by
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
will not range -> `501` (it is *this server* that declines to do the work);
a source error mid-body is the body's error, as for a plain stream.

## 4. What is deleted

| Gone | Lines | Why |
|---|---|---|
| `server/src/archives/cache.rs` (`ProgressiveCache`, `VolumeRoom`, `ABANDONED_AFTER`, the reader/writer notify dance the AGENTS gotcha describes) | 1245 | No extraction, so no extraction cache. |
| `server/src/archives/torrent.rs` (`TorrentArchives`, `MemberCaches` use) | 420 | Sessions hold indexes, not extractions; the `torrent:` form joins the ordinary session map. |
| `server/src/archives/source.rs` (`ArchiveSource`, `MemberCaches`, the `NamedTempFile` ownership) | ~300 of 363 | A source is a `ByteSource` now; the origin string survives as the session's identity. |
| `server/src/archives/tgz.rs` | 289 | gzip has no random access. |
| `routes/archive.rs`: `download_archive`, `DownloadRoom`, `SNIFF_BYTES`, the download timeouts, `archive_cache_config`, `CacheConfig`, the `507` extraction arm | ~450 | Nothing is downloaded by this layer. |
| `archives::sweep_scratch`, the `.archives` entry in `piece_store::sweep::NOT_OURS`, `.archives` in `server/src/lib.rs` startup, the AGENTS/README paragraphs about it | ~80 | The directory ceases to exist. |
| `rar::RarHandler` (file-based extraction), `sevenz.rs`'s extraction, `zip.rs`'s inflate thread, the `ArchiveReader` trait and `OpenedMember` | ~900 | Replaced by translators. |
| `archives/window.rs` | 184 | Generalised into `MemberView`. |

About 3900 lines out; the new layer is roughly 1500 in (sources ~450,
translators: zip ~250, tar ~150, rar ~400 incl. the volume naming and the
blocking shim, 7z ~150, iso9660 ~350; view ~200; route glue net negative).
UDF is another ~400 on its own step.

## 5. Steps, in order, each shippable

Each step lands green on master with its own tests, revert-proven hunk by
hunk, behind no flag: the old path is removed as its replacement lands,
never kept beside it.

1. **`ByteSource`, `MemberView`, `TorrentFileSource`, `ProxySource`.**
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
   deleted.** Tests: index read bounds (counting source); stored member end
   to end from a torrent and from a URL (existing fixtures); deflate member
   -> `415` with the message; `tar.gz` -> `415`; the existing zip/tar
   fixtures under `server/tests/archive.rs` and `embed.rs` keep passing
   where they test stored members and are rewritten where they tested
   extraction. After this step **`.archives` is created by nothing for
   ZIP/TAR**; assert the directory does not exist after the suite.
3. **RAR translator: single volume, then multi-volume.** The blocking shim;
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
4. **Delete the rest**: `cache.rs`, `torrent.rs`, the download path,
   `sweep_scratch`, the sweep entries, `CacheConfig`; docs: AGENTS.md
   (workspace map, the gotcha about the progressive cache, the "every byte
   has an owner" list loses `.archives` and gains "a translated member owns
   nothing"), README, known-issues (#18/#99/#102 close; the CRC note from
   #99 becomes the rule in §2.2.4).
5. **7z translator** (COPY blocks direct, all else refused) and delete
   `sevenz.rs`'s extraction.
6. **ISO 9660 translator**, `iso` route prefix, sniff signature already in
   xtremio's `archive_sniff` (`CD001` at 32769). Fixture: a tiny image
   built in the test (PVD + root record + one file, 2048-byte sectors) and,
   if `genisoimage`/`xorriso` is on the CI runner, a real one. Then **UDF**
   as its own step with a Blu-ray-shaped fixture.
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

It does not decode. A compressed film stays unplayable through this
server, by decision. It does not transcode, does not remux, and does not
turn a DVD's `VIDEO_TS` folder into one stream: an ISO's members are its
files, and a player that can open a `VIDEO_TS/VTS_01_1.VOB` by URL gets
exactly that file. Playing a DVD *as a DVD* (menus, titles across VOBs) is
mpv's `dvd://` over a local path, which is a different feature over the
same `MemberView`.
