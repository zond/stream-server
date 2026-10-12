# Translated sources: one byte source, translators in front of it, nothing on disk but the fetcher's own cache

Design, 2026-09-20. Agreed principle (zond): **only the thing that downloads
a file may store it** -- the torrent piece store, the proxy cache.
Everything that sits between a downloaded file and the player -- an archive,
a disc image, later a cloud drive or a network share -- is a *translation*
of byte ranges. The player asks for bytes `a..b` of the film. The translator
works out which bytes of the underlying file those are, and asks the fetcher
for exactly those. **A format that cannot answer that question is refused**,
with a message the player can show. It is never acceptable to download a
whole file to seek to its end.

It replaced an archive layer that extracted members to disk. It was written
against stream-server `e93f3c4`, rqbit `29f4804e` and xtremio `473789c`.
Steps 1-6 of §5 landed on 2026-09-20 and `DriveSource` on 2026-09-25, so the
code is the answer where this and the code disagree.

## 1. What each case does

What the archive routes do, per case:

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

*(As built, `sources/mod.rs`: `open` answers a `Box<dyn SeekableReader>`.
The trait has two more methods with defaults: `is_empty`, and
`is_live(&Reading)`. That one asks whether the entity being played is this
source's own file, which is what §2.4's session rule asks.)*

There are two methods rather than one because the two uses differ. An index
is a handful of reads at offsets the format dictates (the end of a ZIP, the
start of a RAR volume, sector 16 of an ISO). A body is one long read the
fetcher should treat as a stream -- with a lookahead on a torrent and one
ranged request on HTTP -- not thousands of `read_at`s.

Implementations, in order of need:

* **`TorrentFileSource { engine, file_idx }`.** `len` comes from the file
  list. `read_at` seeks a short-lived `Fetching::Streaming` handle. Better,
  it keeps one handle per source and seeks it under a mutex, since the piece
  store serves a seek cheaply. `open` is
  `try_get_file_with_intent(file, offset, priority, Streaming, profile)` --
  **Streaming, not `Download`**. A player wants a player's lookahead, not
  the 256 MiB a download reads ahead. A source registers the torrent stream
  (`TorrentMemberStream`) for as long as it is held, as the route does now.
  *(Both since replaced by `TorrentSource`, whose open is the stream
  route's. See [media-pipeline.md](media-pipeline.md) §2.2.)*
* **`ProxySource { entry: proxy_cache::Entry, client, total, validator, content_type }`.**
  *(As built: an `Arc` of the cache, URL, credentials, total, content type
  and validator. The cache entry is taken per read, and the probe is always
  a `GET` of `bytes=0-0`.)* It is built by one `HEAD` (or a `GET` of
  `bytes=0-0`) through the proxy's own request builder. That is how it
  learns `total`, the validator and whether the origin honours `Range`. **An
  origin that answers `200` to a ranged request is refused here**, since
  serving it would mean downloading the file. `read_at` and `open` are the
  proxy route's own miss path turned into a reader: `entry.look_up(range)`
  -> serve `Cached::body()` for what the disk holds, then the narrowed fetch
  with `If-Range` for the rest, wrapped in `Filling` with `entry.fill(...)`
  so the bytes land in the cache. This is the one piece of new plumbing of
  any size. It is a refactor of code the proxy route already has, into a
  function both can call. The cache stays the owner of the bytes. The
  retention owner keeps reclaiming them, and `ServerHandle::stream_numbers`
  keeps answering for them. Credentials travel as the proxy already carries
  them (`h=` request headers, kept out of the cache key's credential set and
  out of logs).
* **`MemberView`** (below) is a `ByteSource` too, so a translator can sit on
  another translator's member: an ISO inside a RAR, a ZIP inside a torrent
  inside nothing. Nothing is built for that case. It falls out.
* **Test sources:** `MemorySource(Bytes)` and `FileSource(File)`, plus a
  counting wrapper that records every `read_at`/`open` for the tests that
  prove "no byte was read that the range did not need". *(As built,
  `sources::testing`: `MemorySource` and `CountingSource`. There is no
  `FileSource`. A file on this device is `sources::local::LocalSource`,
  which is not a test source.)*

Later, and outside this plan: `DriveSource` (Google Drive; ranged `GET`
with the addon's OAuth header, which is a `ProxySource` with a header
supplier that refreshes), `SmbSource`, `NfsSource`. That they are the same
seam is the point of having it.

**`DriveSource` is built** (2026-09-25, `server/src/sources/drive.rs`), and
it is that shape exactly: a `ProxySource` whose headers come from a
`Credentials::Own` supplier rather than a fixed map. It settled two things
the sentence above did not anticipate, and the next cloud source inherits
both:

* **The refresh is in Rust and it is serialised.** An access token lasts
  about an hour, and a film does not. So the header is minted per request.
  It is renewed a minute before it expires rather than after a `401`, and
  renewed once however many reads meet the expiry together. A dead pairing
  (`invalid_grant` -> `pairAgain`) is a typed terminal error and is never
  retried, so the app shows a QR rather than a spinner. Nothing crosses FFI
  to do any of it.
* **A credentialed read can be cached, but only on a vouch.** The proxy
  cache still refuses every credentialed request (`ProxyCache::entry` is
  unchanged). It refuses because the question it can ask ("does this carry a
  credential?") is about the request, when a key needs to know about the
  bytes. The question a *source's constructor* can ask is the right one
  ("does this URL identify the content?"). `sources::proxy::Vouch` is where
  it answers. A Drive file id names one file for everyone entitled to it, so
  the credential authorises the fetch without determining the result. The
  key is the URL with no token in it and nothing that churns hourly. The
  consequence is that an entry made with authorisation can be read without
  it, by anything on this device that names the same URL. That is bounded by
  the server being loopback-only, and was accepted knowingly. It is written
  out at `ProxyCache::entry_for_vouched_url`, which is where a later source
  has to read it before vouching for anything.

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
   The test suite counts bytes read per index against a per-format bound,
   through a `Budget` the translator reads everything through
   (`crate::translators`, after `crate::images`). It also asserts **by
   range** that no read overlapped the member's own data, which a byte count
   alone would not catch. This, and not the shape of the handle, is what
   keeps an index an index: a reader is seekable and uncapped, like every
   other handle on a fetched file (§2.1). (ZIP: the end record -- the last
   22 bytes, widening to the 64 KiB comment window only when they are not it
   -- plus the directory plus one 30-byte local header per *stored* member,
   since a compressed one is refused without reading its header at all; RAR:
   the headers of each volume; 7z: the signature header and the packed
   header at the end; ISO: the descriptors and the directory tree; TAR: one
   512-byte header per member, the data skipped by size).
2. **A member is `Direct` or it is refused. There is no third state.** No
   "extract it then", no partial decode, no "serve sequentially but refuse
   seeks". A compressed film is a refusal with a reason the player shows.
3. **A translator never writes.** Not to disk, not to a temp file, not to
   the cache root. `.archives` ceases to exist. Since step 4 nothing in the
   workspace writes it, and the suite asserts the directory does not appear
   after a test that plays a member of every format.
4. **Verification is the fetcher's.** A torrent's bytes are piece-verified
   by librqbit. A proxy entity is what the origin served. A translator does
   not checksum a member on the way through: a CRC over a member is a read
   of the whole member, which is the thing this design forbids. The one
   exception is a *header* checksum a format carries for its own index,
   which is small and is checked.

The formats, and what each one's translator is:

* **ZIP.** Read the end-of-central-directory record (search the last 64 KiB
  plus 22 bytes for the signature; zip64 locator if present), then the
  central directory, then the selected member's local header for the data
  offset. `Stored` -> `Direct(one extent)`; any other method ->
  `Compressed`; general-purpose bit 0 -> `Encrypted`. Multi-part ZIP
  (`.z01`) -> `Malformed("split zip")` for now. `async_zip` can read the
  directory over `AsyncBufRead + AsyncSeek`. The directory is parsed by hand
  if its buffering reads more than the bound allows. It is a fixed layout,
  and the code for the local header already is. *(It is parsed by hand:
  `translators/zip.rs`.)*
* **TAR.** Walk 512-byte headers from offset 0, skipping each entry's data
  by its size (rounded to 512): one small read per member. Every regular
  file is `Direct(one extent)`. GNU long names and PAX headers are read as
  the entries they are. **`tar.gz` is `NoRandomAccess`**: `archives/tgz.rs`
  goes.
* **RAR.** Per volume, `RarArchive::parse_volume_facts(reader, None)` runs
  over a blocking `Read + Seek` shim of the source. The parser is
  synchronous. The shim runs on the blocking pool and reads through the
  runtime handle. The facts walk seeks past member data by
  `compressed_size`, so it reads headers only. Then
  `StoredLayoutBuilder::add_volume(volume, &facts)` runs for each volume in
  order, and `members()` maps the result: a `StoredMember` whose eligibility
  `routes_direct()` and whose parts all have `logical_offset: Some` is
  `Direct(parts as extents: (volume, data_offset, data_size))`;
  `EncryptedStore` -> `Encrypted`; `Ineligible(reason)` -> `Compressed` or
  `Malformed` by the reason; `is_solid` volumes -> `Solid`; header-encrypted
  volumes (`is_encrypted` at the volume level) -> `Encrypted` before any
  member is looked at. *(As built: a solid volume refuses the whole archive,
  `Solid`; header encryption arrives as the parse's `EncryptedArchive` and
  is `Encrypted`.)* **Multi-volume sets are the ordinary case**
  (`.part1.rar`/`.part2.rar`, or `.rar`/`.r00`/`.r01`). From an addon, the
  `rarUrls` list *is* the volume list, in order. From a torrent, the volumes
  are the sibling files of the named one, by the two naming rules, in the
  order the rules give. A set that is missing a volume is `Malformed`,
  named. The layout builder reports a chain that is still open
  (`ProvisionallyDirect`). That is a member whose later volume has not been
  added yet. Every volume is added before the index is answered, so a chain
  still open then is `Malformed`.
* **7z.** `Archive::read(reader, &Password::empty())` runs over the same
  blocking shim. 7z's header lives at the end: two small reads. A file whose
  block (`stream_map.file_block_index`) has exactly one coder with method id
  `[0x00]` (COPY) and no encryption is `Direct(one extent)` at
  `32 + pack_pos + pack_stream_offsets[block_first_pack_stream_index[block]]`,
  plus the sum of the sizes of the files before it in the same block.
  Anything else is `Compressed` (LZMA2 etc.), `Encrypted` (AES coder) or
  `Solid`. A 7z of a film is LZMA2 in practice, and will be refused. The
  translator is cheap and honest, so it is kept.
* **ISO.** No crate in the registry does this. It is a small parser. ISO
  9660: the primary volume descriptor at byte 32768 (sector 16), the root
  directory record in it, directory records walked breadth-first (each
  record: extent LBA, data length, flags, name; Joliet's supplementary
  descriptor for real names; Rock Ridge `NM` where present). A file is
  `Direct(extents)`. An ISO 9660 file is one contiguous extent, and a file
  over 4 GiB is several directory records for one name, which is several
  extents. The model above carries that as written. **UDF** is the second
  half, and it matters more than it looks. DVD-Video discs are ISO 9660/UDF
  bridge (the 9660 tree is enough), but **Blu-ray images are UDF 2.50
  only**, with no 9660 tree at all. UDF: anchor at sector 256, the volume
  descriptor sequence, the file set descriptor, file entries whose
  allocation descriptors are extents -- again `Direct(extents)`. **Both are
  written** (`server/src/images/`, 2026-09-20: two independently written
  parsers, which index a real bridge image to identical extents). What UDF
  refuses by name is a **metadata partition map**. UDF 2.50 remaps logical
  blocks through a metadata file, so an extent computed without it is the
  wrong range of the image. That is the gap a real Blu-ray image hits first,
  and the next UDF increment.

### 2.3 `MemberView`: a member as a file

`MemberView { member: Member, sources: Vec<Arc<dyn ByteSource>> }`
implements `AsyncRead + AsyncSeek` -- and `ByteSource`, for nesting. A seek
to `p` finds the extent holding `p` (binary search on the running offsets).
It **seeks that reader** to `extent.offset + (p - extent.start)` when that
is the extent a reader is already open on. A seek into another extent, or
crossing one's end, opens the next source's reader. What keeps a read inside
the member is the run of the extent it is in, not the source stopping. A
source's reader is a handle on the whole container. `MemberWindow` is this
with one extent, and was replaced by it. *(As built, `sources/view.rs`:
`MemberView` holds an `Arc` of the member -- its name, sources, extents and
starts -- and is a `ByteSource`. `MemberView::reader` hands out the
`MemberReader` that implements `AsyncRead + AsyncSeek`.)*

For the response body, the stream route's own range framing is reused:
`Content-Length`, `Content-Range`, `206`/`416`, `HEAD`, the same
`routes::util::MediaRange` the stream route uses, over a `MemberView`
instead of a `FileHandle`. Nothing about a member's HTTP behaviour is
allowed to differ from a plain file's.

### 2.4 Sessions: an index, in memory, leased

`translators::session::Sessions<TranslatedSession>` (was
`archives::sessions`) stays as the map. The session becomes
`{ origin, sources, index, selected: Option<usize> }` and owns **no file**.
A *torrent-backed* session holds the hash and path rather than the source
*(as built: `SessionSources::Torrent { info_hash, paths, hold }`, the hold a
`SetHold` over a set's volumes; and a `Kept` variant for bytes already on
this device)*. A `TorrentSource` registers a stream for as long as it lives.
Holding one for the session's life would keep a torrent the viewer left
running, so each body opens its own: a file lookup and a reconcile, not a
fetch. The index, which is what was expensive to read, is what the session
is for. It is created by `/{fmt}/create` (from URLs) or on first use by the
`torrent:` form (from a torrent file and its sibling volumes), and leased by
every response body. Dropping one frees memory and, for a torrent, the
stream registration. For a proxy source it frees nothing at all. The cache's
bytes are the cache's, under its own retention, exactly as if the player had
fetched them through `/proxy`.

**A session's life is the live entity's, not a clock's** (2026-09-20,
replacing ten idle minutes, `SESSION_IDLE_TIMEOUT`). The bytes a session
indexes -- a torrent's pieces, a proxied entity's ranges -- are kept while
nothing else has become live, and go the moment something has, bounded by
the cache budget. `enginefs::retention::live` states why. The index of a
container is not a different question from the bytes in it. So:

* a lease out keeps a session, as before -- a body streaming to a player
  or to a cast receiver is never dropped under its reader;
* a session whose container **is** the live entity is kept
  (`TranslatedSession::is_live`: any file of its torrent, or any of its
  `ProxySource`s' keys, because a body crosses volumes while it reads);
* every other session goes when the live entity moves. `Sessions<T>` holds
  no rule of its own and runs no janitor: `Sessions::retain` is handed the
  rule by the switch task in `stream_server::run`, the same signal the two
  retention owners drop their slack on;
* the backstop is a **cap on entries** (`SESSION_CAP`, 32) evicting the
  least recently used unleased session, not a longer timeout. It is a count,
  not a clock. Nothing in the module calls `Instant::now()`, and "least
  recently used" is kept as an ordering (a monotonic counter over the map's
  own uses) so it cannot be compared against a duration.

What this closes: a cast paused longer than the old timeout lost a
link-borne container's session. `/create` is not on the LAN listener, so the
receiver got a `404` with no way to make another (xtremio
`docs/CASTING.md`). A paused cast now keeps its session for as long as
nothing else is opened.

## 3. Routes and the contract with the client

Nothing the client sends changes.

* `POST/GET /{rar|zip|7zip|tar|tgz|iso}/create` (`tgz` answers `415` for
  every member) takes the `lz`-encoded `{ urls, fileIdx, fileMustInclude }`.
  That is what stremio-core builds from `rarUrls`/`zipUrls`
  (`types/resource/stream.rs`). Every URL becomes a `ProxySource` (probe,
  refuse a non-ranging origin with `501` and a message). The translator for
  `{fmt}` indexes them, `resolve_file_idx` picks the member, the session is
  inserted under the key, and a `GET` redirects to `./stream/{key}/{member}`
  as today. A selected member that is `Opaque` answers
  **`415 Unsupported Media Type`** with a JSON body
  `{ "refused": "<Refusal>", "message": "<one sentence>" }`. The player
  shows the message. xtremio's `archive_sniff` had the place for it. Since
  step E the sniff is the server's ([media-pipeline.md](media-pipeline.md)
  §2.9).
* `GET/HEAD /{fmt}/stream/{key}/{member}`: `MemberView` + the shared range
  framing, on loopback. A Cast receiver no longer reads this route. The LAN
  listener serves published cast tokens and nothing else
  (`docs/design/media-pipeline.md` §2.7). A member is cast by publishing its
  media id, whose body is the same view and framing.
* The `torrent:` key form (`torrent:<hash>/<path in torrent>`): the source
  is `TorrentSource`, opened per body. The format is the path's suffix, and
  sibling volumes are found in the torrent's file list. It is otherwise
  identical. `iso` joins the prefixes.
* `/{fmt}/create/{key}` (client-chosen key) keeps its `409` rule.

Error mapping: `Refusal::Compressed|Encrypted|Solid|Unsupported` -> `415`;
`NoRandomAccess` -> `415`; `Malformed` -> `422`; an origin that will not
range -> `501` with `refused: "noRanges"` (it is *this server* that declines
to do the work); a build with no reader for the format -> `501` with
`refused: "noReader"`; a source error mid-body is the body's error, as for a
plain stream. The `noReader` message names a cargo feature and is for
whoever built the app, never for a viewer. The two `501`s are different
things that used to share one status and one untyped `error`, so a client
had to match English to tell them apart. *(As built, the mapping is three
functions in `routes/archive.rs`: `refusal_response` for a `Refusal`,
`source_error_response` for a source that failed, and `no_reader_response`.
`source_error_response` answers `noRanges` with `501`, an origin's `404` as
`404`, and any other origin or fetch failure with `502`. It is shared with
`routes::drive`.)*

## 4. What was deleted

The whole extracting `archives/` module went: about 4,700 lines across steps 2-4, including the progressive extraction cache, the whole-archive download and the reader dispatch. Roughly 1,900 lines of sources, translators and the member view replaced it. `archives/sessions.rs` survived as `translators/session.rs`, merged with the session it leases.

**Kept, deliberately**: one constant and about fifteen lines in
`server/src/lib.rs`, `LEGACY_ARCHIVE_SCRATCH_DIR`, which deletes
`<cacheRoot>/.archives` at launch. The design says the directory ceases to
exist, and it does for a fresh install. An **upgraded** install already has
it, holding about twice the size of every archive played since that build's
last clean exit. Nothing else would ever take it. No retention owner speaks
for those bytes, `ServerHandle::cache_usage` does not count them, and the
piece store's legacy sweep walks `<cacheRoot>/rqbit-downloads` (now
`media-cache`), one level *below* where `.archives` sits. (That is also why
removing it from `NOT_OURS` frees nothing by itself. That list is about
names directly under the download dir, and `.archives` was never one of
them. The exemption was never load-bearing. It is removed because the name
means nothing now.)

## 5. Steps, in order, each shippable

Each step lands green on master with its own tests, revert-proven hunk by
hunk, behind no flag: the old path is removed as its replacement lands,
never kept beside it.

1. **`ByteSource`, `MemberView`, `TorrentFileSource`, `ProxySource`.**
   *(Landed 2026-09-20.)* New module `server/src/sources/`. Tests:
   `MemberView` over several extents across two `MemorySource`s (reads,
   seeks, boundaries, `SeekFrom::End`); `ProxySource` against the test
   origin: the second read of a range is served from the cache and the
   origin is asked once, the cache entry is the same one `/proxy` would use
   for that URL and headers (assert on the entity directory), a non-ranging
   origin is refused; `TorrentFileSource`: a read registers a stream and a
   seek reaches the piece store (the existing
   `an_archive_body_keeps_its_torrent_running_while_it_is_open` is the
   model). The proxy route's miss path is refactored to call the same
   function. Its existing tests stay unchanged, which is the proof the
   refactor is one.
2. **ZIP and TAR translators; the routes switch to `MemberView`; compressed
   members refused; `tgz.rs`, `window.rs`, the extraction paths for zip/tar
   deleted.** *(Landed 2026-09-20.)* Tests: index read bounds (counting
   source); stored member end to end from a torrent and from a URL (existing
   fixtures); deflate member -> `415` with the message; `tar.gz` -> `415`;
   the existing zip/tar fixtures under `server/tests/archive.rs` and
   `embed.rs` keep passing where they test stored members and are rewritten
   where they tested extraction. After this step **`.archives` is created by
   nothing for ZIP/TAR**. Assert the directory does not exist after the
   suite.
3. **RAR translator: single volume, then multi-volume.** *(Landed
   2026-09-20.)* The blocking shim; `parse_volume_facts` +
   `StoredLayoutBuilder`; the two naming rules for sibling volumes in a
   torrent; `rarUrls` order for URLs. Fixtures: `archives/rar.rs` already
   builds store-method RAR5 archives by hand for its tests. Extend the
   builder to emit a split set (the RAR5 split flags and the per-part sizes
   are documented in the crate's `StoredMemberPart`). Tests: a stored film
   across three volumes in a torrent is served whole and by range with reads
   landing in the right volumes; a compressed RAR -> `415 Compressed`; a
   header-encrypted one -> `415 Encrypted`; a set missing its middle volume
   -> `422`. Delete `RarHandler`'s extraction and the `rar` feature's file
   path; the feature flag stays (licence). Done as written, with three
   things worth recording. The volume list is what names a session
   (`routes::archive::set_origin` joins every URL). Two sets can share a
   `.part1.rar` and differ after it, and a session found by the first URL
   alone would answer one set's index over another's bytes.
   `Translator::volumes(named, siblings)` is the hook the `torrent:` form
   finds a set through. Its default, for every other format, is "just this
   file". And a **member whose only checksum is BLAKE2sp, or none, is
   served**. `unrar-rs` marks such a member ineligible because *it* cannot
   verify one out of order. That is its business and not this server's
   (§2.2.4). What the layout API refuses that real releases do use is
   written up in the step's report: nothing found so far beyond the refusals
   above.
4. **Delete the rest.** *(Landed 2026-09-20, after step 5, which is what
   left it with no callers.)* The whole `archives/` module, the download
   path, `AppState::archive_cache` and the `.archives` entry in
   `piece_store::sweep::NOT_OURS`. §4 says what was kept and why.
5. **7z translator** (COPY blocks direct, all else refused) and delete
   `sevenz.rs`'s extraction. *(Landed 2026-09-20: `translators/sevenz.rs`,
   `Format::SevenZ` translated. The offset is the crate's own plus the sum
   of the sizes of the files before it in the same block. The crate's part
   is
   `32 + pack_pos + pack_stream_offsets[block_first_pack_stream_index[block]]`,
   where `ArchiveReader::build_decode_stack` seeks before decoding. The sum
   is sound because a COPY block's output **is** its packed bytes. That was
   proven by reading the fixtures' members back, including three files
   sharing one COPY block. Refusals: an AES coder anywhere in the block, or
   an index the crate wants a password for, is `Encrypted`; a block that is
   not COPY and holds several files is `Solid` (chosen over `Compressed`
   because it says the stronger thing -- a decoder could not enter there
   either); one that holds a single file is `Compressed`, naming the coder
   chain; a multi-part set is `Malformed`, whether it arrives as several
   URLs or as a first part whose stated index is past its own end. The
   signature header is read by hand before the crate sees the file. A 7z
   whose start-header fields are all zero sends `Archive::read` scanning
   backwards over the last mebibyte **one byte at a time**, and over a piece
   store that is a million round trips. A 256 KiB stored fixture indexes for
   191 bytes, none of them the member's.)*
6. **The images module behind `ByteSource`.** It is written, with its own
   small `ImageReader` trait (`len` + `read_at`). What is left is an
   adapter, the refusal mapping of §3, and the `iso` route prefix. The sniff
   signature is already in xtremio's `archive_sniff` (`CD001` at 32769).
   Fixture: a tiny image built in the test (PVD + root record + one file,
   2048-byte sectors), and a real one if `genisoimage`/`xorriso` is on the
   CI runner. Then **UDF** as its own step with a Blu-ray-shaped fixture.
   *(Landed 2026-09-20: `translators/iso.rs`, `Format::Iso`, `/iso` in the
   prefixes. The mapping needed one variant the sketch above lacks,
   `Refusal::Unsupported { format, what }` (`415`, kind `unsupported`). It
   is for a well-formed image using a structure the parser names and does
   not read. The UDF metadata partition map is the first of those, so a
   Blu-ray image today answers `415` with "this UDF image uses a type 2
   partition map (...), which remaps logical blocks; that is not supported
   yet". `images::Refusal` maps as `Unsupported` -> `415`,
   `Malformed`/`NotAnImage` -> `422`, `Unreadable` -> `Malformed` carrying
   the read error, as `translators::Budget` words one. The images module's
   own 8 MiB `Budget` is the bound, and the translator adds no second
   counter. `images::fixtures` is no longer `#[cfg(test)]`, so
   `server/tests/iso.rs` can put the same images in a torrent and behind a
   URL. The real-tool image test skips when no writer is installed. The UDF
   increment that is still open is the metadata partition map itself.)*
7. **xtremio**: a URL stream whose first bytes say RAR/ZIP/7z/ISO is sent to
   `/{fmt}/create` with `urls: [url]` instead of shown the message. The
   message is shown when the server answers `415`/`422`/`501`, with the
   server's own sentence. A torrent whose selected file is an archive or an
   image goes the same way through the `torrent:` form. `StreamKind.archive`
   already exists for addon-declared archives. This is the sniffed case.
   *(Moving into the server: a media id's `resolve` now does this sniff
   itself and answers the member. See `docs/design/media-pipeline.md`
   §2.9.)*

## 6. Decisions taken here, for zond to overrule

* **An origin that does not honour `Range` is refused.** The alternative is
  downloading the archive, which is the thing this design exists to stop.
  Debrid hosts and CDNs range; a few addon-hosted files may not.
* **No checksum on a served member.** Torrent bytes are piece-verified.
  Proxy bytes are what the origin sent, as with any `/proxy` stream. A
  member CRC is a read of the whole member.
* **Encryption is refused everywhere**, including RAR's `EncryptedStore`
  whose mapping the crate can do. There is no password UX, and a wrong guess
  is ciphertext to the player.
* **Solid blocks are refused** even when every file in them is stored. A
  stored solid block is byte-identical and *could* be mapped. But the layout
  crate marks the member ineligible, and the case is rare enough that
  honesty beats cleverness.
* **Index caching is memory only**, in the session, for as long as the
  session lives (§2.4). A re-index after a sweep is a few small ranged
  reads that the proxy cache answers from disk.
* **The `torrent:` form finds volumes by the two naming rules and nothing
  else** (`name.partN.rar` ascending N; `name.rar`, `name.r00`, `name.r01`
  ...). A set named any other way is one volume, and if that volume's chain
  is open it is `Malformed`, saying which volume it wanted.
* **UDF came after ISO 9660**, and is built except the metadata partition
  map: a Blu-ray image is refused `415 unsupported`, naming the map (step 6).

## 7. What this does not do

**Block-compressed formats with a seek table are declined too,
deliberately** (zond, 2026-09-20). Some codecs do offer random access. An xz
stream written in multi-block mode carries an index, zstd has a seekable
format with a table in a skippable frame, and a 7z *block* is independently
decodable. A third body kind (a block list, decoded one block at a time in
memory) would fit this design without storing anything. It was priced and
declined on coverage, not on taste. Block compression buys entry points, and
a seek costs decoding from the nearest one, so the value is entirely about
block size. RAR's and deflate's compressed modes have no entry points at
all. A film in a 7z is normally *one* block, so its only entry is byte zero.
The formats with real indexes (tar.xz, tar.zst) are how software is shipped,
not films. Films arrive stored in RAR or ZIP, which this design already
serves. The cost would have been ~1200-1600 lines plus C-linked decoders in
four build targets. **What replaces it is evidence**: every refusal names
the container and the exact method, so a field log says which codec actually
cost a playback. One that keeps appearing is the one to build for.

It does not decode. A compressed film stays unplayable through this server,
by decision. It does not transcode, does not remux, and does not turn a
DVD's `VIDEO_TS` folder into one stream. An ISO's members are its files, and
a player that can open a `VIDEO_TS/VTS_01_1.VOB` by URL gets exactly that
file. Playing a DVD *as a DVD* (menus, titles across VOBs) is mpv's `dvd://`
over a local path, which is a different feature over the same `MemberView`.
