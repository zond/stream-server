//! What `/proxy` fetched, kept on disk so the next read of the same bytes
//! does not go back to the origin.
//!
//! A player seeks; it does not download. So the unit here is a byte range and
//! never a response: the cache answers the part of a range it holds, the
//! origin is asked only for the rest, and a miss streams to the player *as it
//! fills* rather than being fetched and then served. A design that only cached
//! whole responses would be no use to a demuxer.
//!
//! # Why this is not the piece store
//!
//! `enginefs::piece_store` stores fixed-size chunks as files, buckets them a
//! thousand to a directory, reclaims with `remove_file`, tells "not there"
//! from "the disk is failing" as a typed error, counts occupancy in
//! `st_blocks`, and lives inside the cache root so the cleaner can see it.
//! **All of that is reused here -- as design.** None of the code is, and the
//! reason is worth writing down rather than discovering twice:
//!
//! * It is a [`librqbit::storage::TorrentStorage`] implementation. Its entry
//!   points are `init`, `pread_exact`, `pwrite_all`, `remove_file` and
//!   `take`, it is built from a `TorrentMetadata` by a `StorageFactory` keyed
//!   on an info hash, and its arithmetic (`PieceLayout`) maps a *multi-file
//!   torrent's* global byte space through a file table with BEP-47 padding in
//!   it. A URL response is one file. Its arithmetic is `offset / CHUNK`.
//! * **Presence there means "some of this piece is on disk"; here it has to
//!   mean "all of it".** The piece store can afford the weaker claim because
//!   librqbit hash-checks every piece against the swarm's own SHA-1 before it
//!   counts as had, so a short file is caught by machinery outside the store.
//!   A URL response has no hashes to check against. So completeness is built
//!   rather than inherited: a chunk is buffered whole in memory, written to a
//!   temporary name and renamed into place, and a chunk file whose length is
//!   not the length its entity says it should be is read as absent.
//! * It is also not wired into a session yet, for two reasons that are about
//!   torrent have-sets. This must not wait on that, and must not be a second
//!   `TorrentStorage`.
//!
//! # On disk
//!
//! ```text
//! <download dir>/.proxy/<key>/<length>_<content type>_<validator>/<bucket>/<chunk>
//! ```
//!
//! * `<key>` is [`ProxyCache::entry`]'s hash of everything that varies what
//!   the origin sends back -- see there.
//! * `<length>_<content type>_<validator>` is the **entity**: its byte
//!   length, the type the origin labelled it with, and the validator the
//!   origin identified it by, the last two percent-encoded. All three are in
//!   the directory name rather than in a metadata file beside the chunks, and
//!   that is deliberate. The cache cleaner evicts file by file, oldest mtime
//!   first; a metadata file is written once at the start of a fill and never
//!   touched again, so it is the *first* thing in an entry the cleaner would
//!   take -- and losing it would leave a directory of chunks nothing could
//!   state the length, type or identity of. A directory name cannot be
//!   evicted out from under the files it describes. An entity that differs
//!   from the one before it in any of the three gets a new directory, and the
//!   fill that discovers it removes the old one.
//! * **The validator is the only one of the three that can tell two
//!   generations of one resource apart**, and it is why chunks from two of
//!   them can never end up in one directory or one body. Length and type say
//!   nothing about a URL whose content was replaced by content of the same
//!   size -- which is why a response the origin will identify by neither
//!   `ETag` nor `Last-Modified` is not kept at all
//!   (`routes::proxy::cacheable_entity`).
//! * `<bucket>` is `<chunk> / 1000`, for the same reason the piece store
//!   buckets: exFAT and FAT32 scan a directory linearly, and that is exactly
//!   where a phone's cache lives.
//!
//! The root is `.proxy` inside the engine's `download_dir`, beside
//! `.pieces` -- inside the cache root on purpose, so the cleaner walks,
//! counts and evicts every byte of it with no change to `cache_roots`. It is
//! **not** named `.cache` or `.metadata`: `cache_cleaner::is_session_artifact`
//! exempts any path with either component anywhere in it, and a cache that
//! exempts itself from the cleaner is unbounded disk nobody counts. Nothing
//! here is in `EngineFS::protected_paths` either -- a proxied stream nobody is
//! reading is the first thing that should go.
//!
//! # What is not here, and no comment may imply otherwise
//!
//! * **No revalidation of any kind.** No `ETag`, no `If-None-Match`, no
//!   `Last-Modified`, no `If-Modified-Since`, no freshness lifetime, no
//!   `Age`. An entry is served until the cleaner evicts it. If the origin
//!   changes the entity under the same URL and the change is visible in
//!   neither its length nor its content type, this serves the old one. That
//!   is the single largest thing it does not do.
//! * **No `Vary`.** The key is fixed (below); the header is not read.
//! * **No credentialed responses at all.** Not "keyed carefully" -- refused,
//!   in [`ProxyCache::entry`].
//! * **No coalescing.** Two players filling the same missing chunk both fetch
//!   it.
//! * **Nothing is stored for a fetch that starts mid-chunk.** Only whole
//!   chunks are written, and the bytes before the first chunk boundary in a
//!   body are dropped rather than provoking a wider fetch than the player
//!   asked for.

use bytes::Bytes;
use futures_util::Stream;
use reqwest::Method;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use url::Url;

/// The store's directory inside a torrent cache root, beside the piece
/// store's `.pieces`. Dot-prefixed so a target host called `proxy` could
/// never land on top of it, and deliberately neither `.cache` nor
/// `.metadata` -- see the module docs.
pub const PROXY_CACHE_DIR: &str = ".proxy";

/// How many bytes one chunk file holds, except the last of an entity.
///
/// 256 KiB. Small enough that the cleaner's single-file rule -- a file
/// bigger than the whole cap is kept rather than evicted -- can never see one
/// (`cacheSize` is megabytes at the very least), that eviction is
/// fine-grained, and that the two places a chunk boundary costs something
/// bound it to this much: a fetch that starts mid-chunk drops what it carries
/// of that chunk, and a chunk buffered but never completed is this much
/// memory per stream. Large enough that a 2 GB film is eight thousand files
/// rather than a hundred thousand.
pub const CHUNK_BYTES: u64 = 256 * 1024;

/// How many chunk files share one directory. A thousand, decimal, for the
/// reason `piece_store::PIECES_PER_DIRECTORY` is a thousand: a filesystem
/// that scans directory entries linearly is exactly where this cache lives.
pub const CHUNKS_PER_DIRECTORY: u64 = 1000;

/// The forwarded request headers that say *which bytes* of one answer are
/// wanted rather than what the answer is.
///
/// The keyed set is `routes::proxy::FORWARDED_REQUEST_HEADERS` minus these,
/// computed rather than written out, so there is one list of what a player
/// sends on and not two that have to agree about it. A header added to the
/// forwarded list is keyed on from the moment it is added, which is the safe
/// direction: the unsafe one is a header that changes the origin's answer
/// and is not in the key.
const RANGE_REQUEST_HEADERS: [&str; 2] = ["range", "if-range"];

/// Names each written chunk's temporary file apart from every other one in
/// the process. The process id goes with it, for a kill that leaves one
/// behind while another process is writing the same chunk.
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// One cache, rooted where the cleaner can find it.
pub struct ProxyCache {
    root: PathBuf,
}

impl ProxyCache {
    /// The cache under an engine's `download_dir` -- the directory
    /// `cache_cleaner::cache_roots` already walks.
    pub fn new(download_dir: &Path) -> Self {
        Self {
            root: download_dir.join(PROXY_CACHE_DIR),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The entry this request reads and writes, or `None` for a request the
    /// cache will not touch at all.
    ///
    /// # What is in the key
    ///
    /// Everything that varies **what the origin sends back**:
    ///
    /// * the final target URL, query included -- a debrid link signs its
    ///   query, and two signatures are two resources;
    /// * the `h=` overrides, which replace what the player sent and are the
    ///   only way an addon changes the request at all;
    /// * the player's own headers that reach the origin and are not about
    ///   which bytes it wants: `accept`, `accept-language`, `user-agent`.
    ///   They are content negotiation, and an origin may answer two of them
    ///   with two entities.
    ///
    /// `accept-encoding` is not in it because it is pinned to `identity` for
    /// every proxied request, so it is a constant.
    ///
    /// # What is deliberately *not* in it
    ///
    /// * **`r=`.** It never travels to the origin. It overrides headers on
    ///   the way back and it can flip the playlist verdict -- so it varies
    ///   *the response the player is handed*, and not one byte of what the
    ///   origin sends. What is stored here is origin bytes, replayed through
    ///   the same classification and header code, so `r=` stays out. (The
    ///   note is here because the design this was built from asserted the
    ///   opposite. A cache of assembled *responses* would have to key on it.)
    /// * **`p=`.** The client's name for its player, never sent to the
    ///   origin. Keyed on, two players reading the same stream would share
    ///   nothing, since each mints its own token -- which is the case this
    ///   cache exists for.
    ///
    /// # What it refuses outright
    ///
    /// * anything but `GET`: a `HEAD` or an `OPTIONS` has no body to keep;
    /// * a request carrying `if-range`: it is a conditional, and answering a
    ///   conditional from a store that never revalidates would be inventing
    ///   the condition's answer;
    /// * an `h=` naming any of [`crate::routes::proxy::CREDENTIAL_REQUEST_HEADERS`].
    ///   `/proxy` is an open route with no bearer token of its own, so a key
    ///   that merely *distinguished* credentials would still be one caller's
    ///   authenticated response sitting in a store another caller can reach
    ///   by naming the same URL and the same secret. Refusing is structural
    ///   where keying is careful, and the constant is the credential rule's
    ///   own rather than a second list of names;
    /// * a target on this server's own listener. `/proxy` fetching from
    ///   `/stream` is the engine's bytes going through a second store, both
    ///   of them under one volume's cap, which is the shape of every disk bug
    ///   this project has had. `self_addr` is what makes that a statement
    ///   about *this* server rather than about loopback in general -- a test
    ///   origin, and an addon a viewer runs on the same machine, are on
    ///   loopback too and are nothing of ours.
    pub fn entry(
        &self,
        method: &Method,
        url: &Url,
        request_header_overrides: &BTreeMap<String, String>,
        player_headers: &axum::http::HeaderMap,
        self_addr: std::net::SocketAddr,
    ) -> Option<Entry> {
        if method != Method::GET {
            return None;
        }
        if player_headers.contains_key("if-range") {
            return None;
        }
        if request_header_overrides.keys().any(|name| {
            crate::routes::proxy::CREDENTIAL_REQUEST_HEADERS
                .contains(&name.to_ascii_lowercase().as_str())
        }) {
            return None;
        }
        if names_this_server(url, self_addr) {
            return None;
        }

        let mut hash = Sha256::new();
        // Length-prefixed rather than delimited: a header value cannot hold a
        // newline, but a URL can hold anything a delimiter could be, and a
        // key that two different requests can collide on is one caller's
        // bytes served to another.
        let mut field = |bytes: &[u8]| {
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        };
        field(url.as_str().as_bytes());
        for (name, value) in request_header_overrides {
            field(name.to_ascii_lowercase().as_bytes());
            field(value.as_bytes());
        }
        for name in crate::routes::proxy::FORWARDED_REQUEST_HEADERS {
            if RANGE_REQUEST_HEADERS.contains(&name) {
                continue;
            }
            if let Some(value) = player_headers.get(name) {
                field(name.as_bytes());
                field(value.as_bytes());
            }
        }
        Some(Entry {
            dir: self.root.join(hex::encode(hash.finalize())),
        })
    }
}

/// Whether a target names this server's own HTTP listener, in any of the
/// spellings that reach it: `127.0.0.1`, `::1`, `localhost`, or the address
/// it actually bound. Not a security check -- `/proxy` is loopback-only
/// anyway -- but a budget one: bytes the engine is already serving out of the
/// cache root must not be stored a second time against the same volume's cap.
///
/// The port has to match too. Loopback alone is not this server: an addon or
/// a debrid helper a viewer runs on the same machine is a perfectly ordinary
/// origin, and refusing to cache it would refuse the case the cache is for.
fn names_this_server(url: &Url, self_addr: std::net::SocketAddr) -> bool {
    if url.port_or_known_default() != Some(self_addr.port()) {
        return false;
    }
    match url.host() {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => {
            address.is_loopback() || self_addr.ip() == std::net::IpAddr::V4(address)
        }
        Some(url::Host::Ipv6(address)) => {
            address.is_loopback() || self_addr.ip() == std::net::IpAddr::V6(address)
        }
        None => false,
    }
}

/// One cache key's directory: every entity ever stored for one request
/// shape. Normally there is exactly one entity in it.
pub struct Entry {
    dir: PathBuf,
}

impl Entry {
    /// What is on disk for the range this request asked for, or `None` when
    /// there is nothing here the request can be answered from.
    ///
    /// Blocking: it stats one file per chunk of the range, which for a fully
    /// cached film is thousands. Call it on the blocking pool.
    ///
    /// `range` is the request's `Range` header as it arrived. **A request
    /// with no `Range` is answered only from a complete entry**: it asks for
    /// the whole entity, and a partial answer to it would have to be re-framed
    /// as a `200` carrying a body stitched out of a cached head and the
    /// origin's `206` tail. The shape a player actually sends is
    /// `Range: bytes=0-`, which is the ranged path.
    pub fn look_up(&self, range: Option<&str>) -> Option<Cached> {
        let (dir, total, content_type, validator) = self.sole_entity()?;
        let (first, last) = match range {
            Some(header) => crate::routes::util::parse_range(header, total)?,
            None => (0, total.checked_sub(1)?),
        };

        let mut held_to: Option<u64> = None;
        let mut index = first / CHUNK_BYTES;
        loop {
            let want = chunk_len(index, total);
            match std::fs::metadata(chunk_path(&dir, index)) {
                // Presence is not enough: a chunk is written under a
                // temporary name and renamed, so a file at the final name is
                // complete -- but a length that disagrees with the entity is
                // a file some other accident left, and reading it as a chunk
                // would serve a hole as content.
                Ok(metadata) if metadata.is_file() && metadata.len() == want => {}
                _ => break,
            }
            let end = index * CHUNK_BYTES + want - 1;
            held_to = Some(end);
            if end >= last {
                break;
            }
            index += 1;
        }
        let held_to = held_to?.min(last);
        if range.is_none() && held_to < last {
            return None;
        }
        Some(Cached {
            dir,
            total,
            content_type,
            validator,
            first,
            last,
            held_to,
        })
    }

    /// Start filling from a response whose bytes the rules allow us to keep.
    ///
    /// `body_start` is the absolute offset of the response body's first byte:
    /// zero for a `200`, the `Content-Range`'s first byte for a `206`.
    pub fn fill(&self, total: u64, content_type: &str, validator: &str, body_start: u64) -> Filler {
        let dir = self
            .dir
            .join(entity_dir_name(total, content_type, validator));
        // The entity directory and the removal of any *other* entity under
        // this key happen once, off the reactor, and neither has to finish
        // before the first chunk is written: a chunk write creates its own
        // bucket directory, and what is being removed is by definition not
        // the directory being written into.
        let stale = self.dir.clone();
        let fresh = dir.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) = std::fs::create_dir_all(&fresh) {
                tracing::debug!(path = %fresh.display(), %error, "could not open a proxy cache entry");
                return;
            }
            remove_other_entities(&stale, &fresh);
        });
        Filler {
            dir,
            total,
            offset: body_start,
            collecting: None,
            buffer: Vec::new(),
        }
    }

    /// The entity under this key, when there is exactly one.
    ///
    /// Two is what a kill between writing a new entity and removing the old
    /// one leaves, and there is nothing here that can say which of them the
    /// origin would send now -- so the request goes to the origin, and the
    /// fill it comes back with removes the loser.
    fn sole_entity(&self) -> Option<(PathBuf, u64, String, String)> {
        let mut only: Option<(PathBuf, u64, String, String)> = None;
        for entry in std::fs::read_dir(&self.dir).ok()?.flatten() {
            let name = entry.file_name();
            let Some((total, content_type, validator)) =
                name.to_str().and_then(parse_entity_dir_name)
            else {
                continue;
            };
            if only.is_some() {
                return None;
            }
            only = Some((entry.path(), total, content_type, validator));
        }
        only
    }
}

/// `<length>_<content type>_<validator>`, the last two percent-encoded.
///
/// `_` is escaped along with everything else a percent-encoding escapes, so
/// the name splits into exactly three fields however an origin spells a type
/// or a tag. It is in the unreserved set that `urlencoding::encode` leaves
/// alone, and a content type may hold one (`application/x-foo_bar`), so
/// leaving it would make the separator ambiguous the first time an origin
/// used it.
fn entity_dir_name(total: u64, content_type: &str, validator: &str) -> String {
    format!(
        "{total}_{}_{}",
        encode_field(content_type),
        encode_field(validator)
    )
}

fn encode_field(value: &str) -> String {
    urlencoding::encode(value).replace('_', "%5F")
}

/// The three fields back, and `None` for a name that is not three fields or
/// whose validator is empty. An entity with no validator is one nothing could
/// tell a later generation of the resource from, and nothing writes one -- so
/// a directory claiming to be one is not read as an entity at all.
fn parse_entity_dir_name(name: &str) -> Option<(u64, String, String)> {
    let mut fields = name.split('_');
    let total: u64 = fields.next()?.parse().ok()?;
    let content_type = urlencoding::decode(fields.next()?).ok()?.into_owned();
    let validator = urlencoding::decode(fields.next()?).ok()?.into_owned();
    if fields.next().is_some() || validator.is_empty() {
        return None;
    }
    Some((total, content_type, validator))
}

/// How long an entity's directory name may be: 255 bytes, which is what one
/// name may hold on every filesystem this runs on, and the name is ASCII once
/// its two text fields are percent-encoded so bytes and characters are the
/// same count.
///
/// An origin is free to send a content type or an `ETag` longer than the
/// remainder; what it is not free to do is make every chunk write of that
/// response fail its `mkdir` and say so in the log. Beyond this the response
/// is simply not one the cache keeps, like every other thing it declines.
const MAX_ENTITY_DIR_NAME: usize = 255;

/// Whether an entity of this length, type and validator can be filed at all.
/// Asked by `routes::proxy::cacheable_entity` before a response is kept,
/// because the answer is a refusal and every refusal lives there.
pub fn can_be_filed(total: u64, content_type: &str, validator: &str) -> bool {
    entity_dir_name(total, content_type, validator).len() <= MAX_ENTITY_DIR_NAME
}

/// Remove every entity under `key_dir` but `keep`: the origin has just said
/// what this resource is, and an entity of a different length, a different
/// type or a different validator is not it any more.
fn remove_other_entities(key_dir: &Path, keep: &Path) {
    let Ok(entries) = std::fs::read_dir(key_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == keep || parse_entity_dir_name(&entry.file_name().to_string_lossy()).is_none() {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => tracing::debug!(
                path = %path.display(),
                "the origin's entity changed; dropping what was cached of the old one"
            ),
            Err(error) => {
                tracing::debug!(path = %path.display(), %error, "could not drop a stale cache entity")
            }
        }
    }
}

/// `<entity dir>/<bucket>/<chunk>`.
fn chunk_path(dir: &Path, index: u64) -> PathBuf {
    let mut path = dir.join((index / CHUNKS_PER_DIRECTORY).to_string());
    path.push(index.to_string());
    path
}

/// How long chunk `index` of a `total`-byte entity is: a whole chunk, or
/// whatever is left at the end of the entity.
fn chunk_len(index: u64, total: u64) -> u64 {
    let start = index.saturating_mul(CHUNK_BYTES);
    (total.saturating_sub(start)).min(CHUNK_BYTES)
}

/// What one entity holds for one request's range.
pub struct Cached {
    dir: PathBuf,
    /// The entity's length, which is what a `Content-Range` has to state.
    pub total: u64,
    /// What the origin labelled the entity, empty when it said nothing.
    pub content_type: String,
    /// How the origin identified the entity, as
    /// `routes::proxy::EntityValidator` files it -- the header it came in and
    /// its value. Never empty: a response the origin would identify by
    /// neither `ETag` nor `Last-Modified` is not kept.
    ///
    /// It is what says that this and a fresh `206` are parts of one entity,
    /// and it is the `If-Range` the narrowed fetch asks under.
    pub validator: String,
    /// The first byte the request asked for, clamped into the entity.
    pub first: u64,
    /// The last byte it asked for, likewise.
    pub last: u64,
    /// The last byte held contiguously from [`Cached::first`] -- never past
    /// [`Cached::last`], and never before [`Cached::first`], since a lookup
    /// with nothing at `first` is not a [`Cached`] at all.
    pub held_to: u64,
}

impl Cached {
    /// Whether the whole of what was asked for is here, so the origin need
    /// not be opened at all.
    pub fn complete(&self) -> bool {
        self.held_to >= self.last
    }

    /// The `Range` the origin still has to be asked for. `held_to + 1` is a
    /// chunk boundary by construction, so what comes back fills whole chunks.
    pub fn remaining_range(&self) -> String {
        format!("bytes={}-{}", self.held_to + 1, self.last)
    }

    /// The cached bytes themselves, `first..=held_to`, a chunk at a time.
    ///
    /// A read that fails -- the cleaner took the chunk between the lookup and
    /// here, which is an ordinary race and not a fault -- ends the stream with
    /// an error rather than a short body, so the player sees a broken source
    /// instead of a file that ended early.
    pub fn body(&self) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        let dir = self.dir.clone();
        let total = self.total;
        let last = self.held_to;
        futures_util::stream::unfold(self.first, move |offset| {
            let dir = dir.clone();
            async move {
                if offset > last {
                    return None;
                }
                let index = offset / CHUNK_BYTES;
                let start = index * CHUNK_BYTES;
                let want = chunk_len(index, total);
                let bytes = match tokio::fs::read(chunk_path(&dir, index)).await {
                    Ok(bytes) if bytes.len() as u64 == want => bytes,
                    Ok(_) => {
                        return Some((
                            Err(io::Error::other("a cached chunk is not the length it was")),
                            last + 1,
                        ));
                    }
                    Err(error) => return Some((Err(error), last + 1)),
                };
                let to = last.min(start + want - 1);
                let served = Bytes::copy_from_slice(
                    &bytes[(offset - start) as usize..=(to - start) as usize],
                );
                Some((Ok(served), to + 1))
            }
        })
    }
}

/// Writes whole chunks of a response to disk as its bytes go past.
///
/// A chunk is held in memory until it is complete and only then written, in
/// one blocking task, to a temporary name that is renamed into place. Two
/// properties come out of that and both are load-bearing: a client that
/// vanishes mid-chunk leaves *nothing* on disk to be mistaken for a complete
/// chunk later, and the reactor never blocks on the write.
pub struct Filler {
    dir: PathBuf,
    total: u64,
    /// Absolute offset of the next byte to arrive.
    offset: u64,
    /// The chunk the buffer is collecting, if any. `None` while the body is
    /// running through bytes that cannot complete a chunk.
    collecting: Option<u64>,
    buffer: Vec<u8>,
}

impl Filler {
    fn take(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() && self.offset < self.total {
            let index = self.offset / CHUNK_BYTES;
            let want = chunk_len(index, self.total);
            let within = self.offset - index * CHUNK_BYTES;
            let take = (want - within).min(bytes.len() as u64) as usize;
            if self.collecting != Some(index) && within == 0 {
                // A chunk already on disk is not written again: the fill is
                // only ever asked for what the lookup did not hold, but a
                // second reader of the same stream can overlap it.
                if chunk_path(&self.dir, index).is_file() {
                    self.offset += take as u64;
                    bytes = &bytes[take..];
                    continue;
                }
                self.collecting = Some(index);
                self.buffer.clear();
            }
            if self.collecting == Some(index) {
                self.buffer.extend_from_slice(&bytes[..take]);
                if self.buffer.len() as u64 == want {
                    let chunk = std::mem::take(&mut self.buffer);
                    self.collecting = None;
                    let dir = self.dir.clone();
                    tokio::task::spawn_blocking(move || write_chunk(&dir, index, &chunk));
                }
            }
            // Bytes before the first chunk boundary of a body belong to a
            // chunk whose front this response does not carry. There is
            // nothing to be done with them but drop them.
            self.offset += take as u64;
            bytes = &bytes[take..];
        }
    }
}

/// Write one complete chunk: temporary name, then rename. Nothing fails
/// loudly -- a cache that cannot write is a slower stream and never a broken
/// one.
fn write_chunk(dir: &Path, index: u64, chunk: &[u8]) {
    let path = chunk_path(dir, index);
    let Some(bucket) = path.parent() else {
        return;
    };
    if let Err(error) = std::fs::create_dir_all(bucket) {
        tracing::debug!(path = %bucket.display(), %error, "could not create a proxy cache bucket");
        return;
    }
    let temp = bucket.join(format!(
        "{index}.{}-{}.part",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    if let Err(error) = std::fs::write(&temp, chunk) {
        tracing::debug!(path = %temp.display(), %error, "could not write a proxy cache chunk");
        let _ = std::fs::remove_file(&temp);
        return;
    }
    if let Err(error) = std::fs::rename(&temp, &path) {
        tracing::debug!(path = %path.display(), %error, "could not commit a proxy cache chunk");
        let _ = std::fs::remove_file(&temp);
    }
}

/// The origin's body, with every whole chunk of it written to the cache on
/// the way past.
pub struct Filling<S> {
    inner: S,
    filler: Option<Filler>,
}

impl<S> Filling<S> {
    pub fn new(inner: S, filler: Filler) -> Self {
        Self {
            inner,
            filler: Some(filler),
        }
    }
}

impl<S> Stream for Filling<S>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Unpin,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_next(cx);
        match &polled {
            Poll::Ready(Some(Ok(bytes))) => {
                if let Some(filler) = this.filler.as_mut() {
                    filler.take(bytes);
                }
            }
            // A body that ended, broke or was closed carries no more chunks;
            // the half-collected one in memory is dropped with the filler and
            // was never on disk to begin with.
            Poll::Ready(_) => this.filler = None,
            Poll::Pending => {}
        }
        polled
    }
}

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Temporary chunk files removed.
    pub removed: usize,
    /// What they occupied, in bytes as the volume counts them.
    pub freed_bytes: u64,
    /// Entries that could not be read or removed. Logged, never fatal.
    pub errors: usize,
}

/// Delete the temporary files a kill left behind, at launch.
///
/// **This is not the piece store's sweep, and the difference is the claim
/// set.** A torrent's pieces are claimed by the session, so anything
/// unclaimed there is data nothing will ever reclaim. Nothing claims a
/// cached URL: every chunk here is cache, the cleaner counts and evicts all
/// of it, and surviving a restart is the whole point. So the only thing a
/// kill can leave that is not cache is a chunk that was being written when
/// the process died -- a `.part` file, which no read will ever look at and
/// no fill will ever finish.
pub fn sweep(root: &Path) -> SweepReport {
    let mut report = SweepReport::default();
    for entry in walkdir::WalkDir::new(root) {
        let entry = match entry {
            Ok(entry) => entry,
            // No cache root yet is the ordinary first-launch state.
            Err(error) if error.io_error().map(|e| e.kind()) == Some(io::ErrorKind::NotFound) => {
                continue;
            }
            Err(error) => {
                tracing::warn!(root = %root.display(), %error, "could not read the proxy cache");
                report.errors += 1;
                continue;
            }
        };
        if !entry.file_type().is_file() || !entry.file_name().to_string_lossy().ends_with(".part") {
            continue;
        }
        let freed = entry.metadata().map(|m| occupied_bytes(&m)).unwrap_or(0);
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                report.removed += 1;
                report.freed_bytes += freed;
            }
            Err(error) => {
                tracing::warn!(path = %entry.path().display(), %error, "could not sweep a proxy cache temporary");
                report.errors += 1;
            }
        }
    }
    if report.removed > 0 {
        tracing::info!(
            removed = report.removed,
            freed = report.freed_bytes,
            "swept proxy cache chunks that were being written when the process died"
        );
    }
    report
}

/// Occupancy, never apparent length -- the same rule as
/// `cache_cleaner::occupied_bytes` and the piece sweep's, so a partly
/// written temporary is reported at what deleting it actually frees.
#[cfg(unix)]
fn occupied_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() * 512
}

#[cfg(not(unix))]
fn occupied_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn cache() -> (tempfile::TempDir, ProxyCache) {
        let dir = tempfile::tempdir().expect("a scratch root");
        let cache = ProxyCache::new(dir.path());
        (dir, cache)
    }

    fn url(spelling: &str) -> Url {
        Url::parse(spelling).expect("a literal URL")
    }

    /// Where the server under test is listening. Every target in these
    /// tests is somewhere else, which is what an origin is.
    const SELF_ADDR: std::net::SocketAddr = std::net::SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        11470,
    );

    /// How an origin identified the entity these tests store, as
    /// `routes::proxy::EntityValidator` files it. Every entity has one --
    /// a response the origin will identify by neither `ETag` nor
    /// `Last-Modified` is not kept at all.
    const VALIDATOR: &str = "etag:\"v1\"";

    fn entry_of(cache: &ProxyCache, target: &str) -> Entry {
        cache
            .entry(
                &Method::GET,
                &url(target),
                &BTreeMap::new(),
                &HeaderMap::new(),
                SELF_ADDR,
            )
            .expect("a plain GET is cacheable")
    }

    fn key_of(cache: &ProxyCache, entry: &Entry) -> String {
        entry
            .dir
            .strip_prefix(cache.root())
            .expect("the entry is under the root")
            .to_string_lossy()
            .into_owned()
    }

    fn header(name: &str, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).expect("a literal header name"),
            axum::http::HeaderValue::from_str(value).expect("a literal header value"),
        );
        headers
    }

    /// The whole of what the key promises: two requests the origin would
    /// answer differently must not share an entry, and two it would answer
    /// the same must.
    #[test]
    fn the_key_follows_what_the_origin_was_asked() {
        let (_root, cache) = cache();
        let plain = key_of(&cache, &entry_of(&cache, "https://host/film.mkv"));

        assert_eq!(
            plain,
            key_of(&cache, &entry_of(&cache, "https://host/film.mkv")),
            "the same request twice is the same entry, or nothing is ever a hit"
        );
        assert_ne!(
            plain,
            key_of(&cache, &entry_of(&cache, "https://host/film.mkv?sig=abc")),
            "a debrid link signs its query; two signatures are two resources"
        );
        assert_ne!(
            plain,
            key_of(&cache, &entry_of(&cache, "https://other/film.mkv")),
        );

        let with_header = cache
            .entry(
                &Method::GET,
                &url("https://host/film.mkv"),
                &BTreeMap::from([("Referer".to_string(), "https://addon".to_string())]),
                &HeaderMap::new(),
                SELF_ADDR,
            )
            .expect("a Referer is not a credential");
        assert_ne!(
            plain,
            key_of(&cache, &with_header),
            "h= replaces what the player sent, so it changes the request"
        );

        let with_agent = cache
            .entry(
                &Method::GET,
                &url("https://host/film.mkv"),
                &BTreeMap::new(),
                &header("user-agent", "mpv"),
                SELF_ADDR,
            )
            .expect("a user agent is not a credential");
        assert_ne!(
            plain,
            key_of(&cache, &with_agent),
            "content negotiation can hand two players two entities"
        );

        let ranged = cache
            .entry(
                &Method::GET,
                &url("https://host/film.mkv"),
                &BTreeMap::new(),
                &header("range", "bytes=1000-1999"),
                SELF_ADDR,
            )
            .expect("a range is the ordinary shape of a proxied read");
        assert_eq!(
            plain,
            key_of(&cache, &ranged),
            "a range selects bytes of one entity; it does not make another"
        );
    }

    /// Same URL, different `h=` credentials: refused, not keyed. `/proxy`
    /// takes no bearer token, so an entry one caller's `Authorization`
    /// filled is one another caller can name.
    #[test]
    fn a_credential_in_h_is_not_cached_at_all() {
        let (_root, cache) = cache();
        for name in crate::routes::proxy::CREDENTIAL_REQUEST_HEADERS {
            for spelling in [name.to_string(), name.to_ascii_uppercase()] {
                assert!(
                    cache
                        .entry(
                            &Method::GET,
                            &url("https://host/film.mkv"),
                            &BTreeMap::from([(spelling.clone(), "s3cret".to_string())]),
                            &HeaderMap::new(),
                            SELF_ADDR,
                        )
                        .is_none(),
                    "h={spelling} names a credential"
                );
            }
        }
    }

    #[test]
    fn what_else_the_cache_will_not_touch() {
        let (_root, cache) = cache();
        let target = url("https://host/film.mkv");
        for method in [Method::HEAD, Method::OPTIONS, Method::POST] {
            assert!(
                cache
                    .entry(
                        &method,
                        &target,
                        &BTreeMap::new(),
                        &HeaderMap::new(),
                        SELF_ADDR
                    )
                    .is_none(),
                "{method} has no body to keep"
            );
        }
        assert!(
            cache
                .entry(
                    &Method::GET,
                    &target,
                    &BTreeMap::new(),
                    &header("if-range", "\"tag\""),
                    SELF_ADDR,
                )
                .is_none(),
            "a conditional cannot be answered by a store that never revalidates"
        );
        for ourselves in [
            "http://127.0.0.1:11470/abcd/0",
            "http://localhost:11470/abcd/0",
            "http://[::1]:11470/abcd/0",
        ] {
            assert!(
                cache
                    .entry(
                        &Method::GET,
                        &url(ourselves),
                        &BTreeMap::new(),
                        &HeaderMap::new(),
                        SELF_ADDR,
                    )
                    .is_none(),
                "{ourselves} is this server, and its bytes are already under this budget"
            );
        }
        // Loopback is not by itself this server: an addon or a debrid helper
        // running on the viewer's own machine is an ordinary origin, and
        // refusing it would refuse the case the cache exists for.
        assert!(
            cache
                .entry(
                    &Method::GET,
                    &url("http://127.0.0.1:7000/film.mkv"),
                    &BTreeMap::new(),
                    &HeaderMap::new(),
                    SELF_ADDR,
                )
                .is_some()
        );
    }

    /// A chunk written whole and renamed into place is readable; a chunk
    /// whose length disagrees with the entity is read as absent, because
    /// serving it would serve a hole as content.
    #[tokio::test]
    async fn a_chunk_is_read_back_only_when_it_is_the_length_it_should_be() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let total = CHUNK_BYTES + 10;
        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        std::fs::create_dir_all(chunk_path(&dir, 0).parent().unwrap()).unwrap();

        write_chunk(&dir, 0, &vec![7u8; CHUNK_BYTES as usize]);
        let cached = entry.look_up(Some("bytes=0-99")).expect("chunk 0 is here");
        assert_eq!((cached.first, cached.last, cached.held_to), (0, 99, 99));
        assert!(cached.complete());
        assert_eq!(cached.total, total);
        assert_eq!(cached.content_type, "video/mp4");

        // The final chunk of this entity is ten bytes; a full-length one at
        // its name is not that chunk.
        write_chunk(&dir, 1, &vec![7u8; CHUNK_BYTES as usize]);
        let cached = entry.look_up(Some("bytes=0-")).expect("chunk 0 is here");
        assert!(!cached.complete(), "the second chunk is the wrong length");
        assert_eq!(cached.held_to, CHUNK_BYTES - 1);
        assert_eq!(
            cached.remaining_range(),
            format!("bytes={CHUNK_BYTES}-{}", total - 1)
        );

        write_chunk(&dir, 1, &[7u8; 10]);
        let cached = entry.look_up(Some("bytes=0-")).expect("chunk 0 is here");
        assert!(
            cached.complete(),
            "and the right length is the whole entity"
        );
        assert_eq!(cached.held_to, total - 1);
    }

    /// The lookup answers a *range*, not a response: a gap in the middle of
    /// an entity bounds what can be served and names what the origin still
    /// has to be asked for.
    #[tokio::test]
    async fn a_gap_bounds_the_hit_and_names_the_rest() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let total = CHUNK_BYTES * 4;
        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        for index in [0u64, 1, 3] {
            write_chunk(&dir, index, &vec![index as u8; CHUNK_BYTES as usize]);
        }

        let cached = entry
            .look_up(Some("bytes=0-"))
            .expect("the entity starts here");
        assert!(!cached.complete());
        assert_eq!(cached.held_to, CHUNK_BYTES * 2 - 1);
        assert_eq!(
            cached.remaining_range(),
            format!("bytes={}-{}", CHUNK_BYTES * 2, total - 1),
            "the origin is asked for the gap and what follows it, from a chunk boundary"
        );

        // Mid-chunk, which is what a seek looks like: what is held runs to
        // the end of the run the seek landed in, not to the end of its chunk.
        let cached = entry
            .look_up(Some(&format!("bytes={}-{}", CHUNK_BYTES + 5, total - 1)))
            .expect("chunk 1 is here");
        assert_eq!(cached.first, CHUNK_BYTES + 5);
        assert_eq!(cached.held_to, CHUNK_BYTES * 2 - 1);

        // A seek into the hole is not a hit at all.
        assert!(
            entry
                .look_up(Some(&format!("bytes={}-", CHUNK_BYTES * 2)))
                .is_none()
        );

        // And a request with no Range is answered only from a complete entry.
        assert!(entry.look_up(None).is_none());
        write_chunk(&dir, 2, &vec![2u8; CHUNK_BYTES as usize]);
        let whole = entry.look_up(None).expect("every chunk is here now");
        assert!(whole.complete());
        assert_eq!((whole.first, whole.last), (0, total - 1));
    }

    /// What the entity directory is for: a resource whose length or type
    /// changed is a different entity, and the fill that finds out drops what
    /// was cached of the old one rather than serving halves of both.
    #[tokio::test]
    async fn a_changed_entity_replaces_the_one_before_it() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let old = entry
            .dir
            .join(entity_dir_name(CHUNK_BYTES * 2, "video/mp4", VALIDATOR));
        write_chunk(&old, 0, &vec![1u8; CHUNK_BYTES as usize]);
        assert!(entry.look_up(Some("bytes=0-0")).is_some());

        let filler = entry.fill(CHUNK_BYTES * 3, "video/mp4", VALIDATOR, 0);
        // The sibling removal is a blocking task; wait for it the way a test
        // waits for anything it did not await.
        for _ in 0..200 {
            if !old.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            !old.exists(),
            "the old entity is not this resource any more"
        );
        assert!(filler.dir.is_dir());
    }

    /// Only whole chunks are written, and a body that stops mid-chunk leaves
    /// nothing on disk that a later read could mistake for a complete one.
    #[tokio::test]
    async fn a_fill_that_stops_mid_chunk_writes_nothing_of_it() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let total = CHUNK_BYTES * 2 + 7;
        let mut filler = entry.fill(total, "video/mp4", VALIDATOR, 0);
        filler.take(&vec![9u8; CHUNK_BYTES as usize + 12]);
        drop(filler);

        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        for _ in 0..200 {
            if chunk_path(&dir, 0).is_file() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(chunk_path(&dir, 0).is_file(), "the whole chunk is here");
        assert!(
            !chunk_path(&dir, 1).exists(),
            "and the twelve bytes of the next one are not"
        );
        assert_eq!(sweep(cache.root()), SweepReport::default());
    }

    /// A body that does not start on a chunk boundary contributes nothing to
    /// the chunk it starts inside -- there is no way to complete a chunk
    /// whose front the response does not carry.
    #[tokio::test]
    async fn a_fill_that_starts_mid_chunk_skips_to_the_next_boundary() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let total = CHUNK_BYTES * 3;
        let mut filler = entry.fill(total, "video/mp4", VALIDATOR, 100);
        filler.take(&vec![9u8; (CHUNK_BYTES * 2) as usize]);
        drop(filler);

        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        for _ in 0..200 {
            if chunk_path(&dir, 1).is_file() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!chunk_path(&dir, 0).exists(), "its first bytes never came");
        assert!(chunk_path(&dir, 1).is_file());
        assert!(!chunk_path(&dir, 2).exists(), "the body ran out first");
    }

    /// The sweep's whole job: a chunk that was being written when the
    /// process died. Everything else under the root is cache, which is what
    /// surviving a restart means.
    #[test]
    fn the_sweep_takes_the_temporaries_and_nothing_else() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let dir = entry
            .dir
            .join(entity_dir_name(CHUNK_BYTES, "video/mp4", VALIDATOR));
        write_chunk(&dir, 0, &vec![3u8; CHUNK_BYTES as usize]);
        let killed = chunk_path(&dir, 0).parent().unwrap().join("0.999-0.part");
        std::fs::write(&killed, [3u8; 64]).unwrap();

        let report = sweep(cache.root());
        assert_eq!(report.removed, 1);
        assert_eq!(report.errors, 0);
        assert!(!killed.exists());
        assert!(chunk_path(&dir, 0).is_file(), "a committed chunk is cache");
        assert_eq!(
            sweep(cache.root()),
            SweepReport::default(),
            "idempotent: a second pass finds nothing to do"
        );
    }

    /// The subtraction the key is built by, from both sides: every name
    /// taken out is one that was actually being forwarded, and what is left
    /// is the description of the request rather than the selection of bytes
    /// within one answer.
    #[test]
    fn the_keyed_headers_are_the_forwarded_ones_that_are_not_about_a_range() {
        let keyed: Vec<&str> = crate::routes::proxy::FORWARDED_REQUEST_HEADERS
            .into_iter()
            .filter(|name| !RANGE_REQUEST_HEADERS.contains(name))
            .collect();
        assert_eq!(keyed, ["accept", "accept-language", "user-agent"]);
        for name in RANGE_REQUEST_HEADERS {
            assert!(
                crate::routes::proxy::FORWARDED_REQUEST_HEADERS.contains(&name),
                "{name} is subtracted from a list it is not on"
            );
        }
        for name in crate::routes::proxy::CREDENTIAL_REQUEST_HEADERS {
            assert!(
                !crate::routes::proxy::FORWARDED_REQUEST_HEADERS.contains(&name),
                "{name} would reach the origin without an addon ever naming it"
            );
        }
    }

    /// The three fields go into one directory name, and the rule about that
    /// name is the only reason a response the origin describes perfectly well
    /// might still not be kept.
    ///
    /// This is where it is pinned rather than in the route's tests, because
    /// from outside the two answers are the same: a name past what a
    /// filesystem takes fails its `mkdir` for every chunk of the response, so
    /// nothing is cached either way. What the rule buys is that the refusal
    /// is a decision made once instead of a line in the log per chunk -- and
    /// the boundary is asserted against the filesystem itself, since a
    /// constant that were one byte out would refuse names that fit or accept
    /// names that do not.
    #[test]
    fn an_entity_is_kept_only_under_a_name_a_directory_can_hold() {
        let (root, cache) = cache();
        let validator = "etag:\"v1\"";
        let fits = "video/".to_string()
            + &"x"
                .repeat(MAX_ENTITY_DIR_NAME - entity_dir_name(u64::MAX, "video/", validator).len());
        assert!(can_be_filed(u64::MAX, &fits, validator));
        assert!(!can_be_filed(u64::MAX, &(fits.clone() + "x"), validator));

        // And the longest name it accepts is one the filesystem takes.
        let name = entity_dir_name(u64::MAX, &fits, validator);
        assert_eq!(name.len(), MAX_ENTITY_DIR_NAME);
        std::fs::create_dir_all(root.path().join(&name)).expect("a name a directory can hold");

        // A validator is as able to be the long field as a type is: the rule
        // is about the name, not about which of them grew.
        assert!(!can_be_filed(1, "video/mp4", &"etag:x".repeat(64)));
        let _ = cache;
    }

    /// The name splits into exactly the three fields it was written from,
    /// whatever an origin spells them with -- `_` included, which is in the
    /// set a percent-encoding would otherwise leave alone.
    #[test]
    fn an_entity_name_round_trips_through_the_characters_that_could_break_it() {
        for (content_type, validator) in [
            ("video/mp4", "etag:\"v1\""),
            ("application/x-foo_bar", "etag:\"a_b\""),
            (
                "video/mp4; codecs=\"avc1\"",
                "last-modified:Wed, 21 Oct 2015 07:28:00 GMT",
            ),
            ("", "etag:%5F"),
        ] {
            let name = entity_dir_name(4096, content_type, validator);
            assert_eq!(
                parse_entity_dir_name(&name),
                Some((4096, content_type.to_string(), validator.to_string())),
                "{name}"
            );
        }
        // An entity with no validator is one nothing could tell a later
        // generation of the resource from, and nothing writes one -- so a
        // directory claiming to be one is not read as an entity at all.
        assert_eq!(
            parse_entity_dir_name(&entity_dir_name(4096, "video/mp4", "")),
            None
        );
        assert_eq!(parse_entity_dir_name("4096_video%2Fmp4"), None);
    }

    #[test]
    fn a_cache_that_has_never_been_written_is_not_a_problem() {
        let (root, cache) = cache();
        assert_eq!(sweep(cache.root()), SweepReport::default());
        assert_eq!(
            sweep(&root.path().join("never")),
            SweepReport::default(),
            "and neither is one whose root does not exist"
        );
    }
}
