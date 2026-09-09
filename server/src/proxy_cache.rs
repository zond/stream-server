//! What `/proxy` fetched, kept on disk so the next read of the same bytes
//! does not go back to the origin.
//!
//! A player seeks; it does not download. So the unit here is a byte range and
//! never a response: the cache answers the part of a range it holds, the
//! origin is asked only for the rest, and a miss streams to the player *as it
//! fills* rather than being fetched and then served. A design that only cached
//! whole responses would be no use to a demuxer.
//!
//! # One chunk store, two adapters
//!
//! The bytes themselves are [`enginefs::chunk_store::ChunkDir`]'s -- the
//! same store the torrent pieces are in, and none of its code is written
//! twice. It owns the directory shape, the bucketing, the two staging
//! spellings, the rename that makes presence mean complete, the typed read,
//! the listings and the occupancy. What is here is the *adapter*: the key,
//! the entity, the refusals, and the arithmetic, which for a URL response is
//! `offset / CHUNK` where a torrent's is a file table with BEP-47 padding in
//! it.
//!
//! Only two things about the store differ between the two adapters, and they
//! are parameters of it rather than a reason for a second one:
//!
//! * **Staging identity.** A `/proxy` chunk is buffered whole and written
//!   once, and two readers of one stream may fill the same chunk at the same
//!   time, so it is staged *anonymously*
//!   ([`enginefs::chunk_store::ChunkDir::write_whole`]) and a kill leaves
//!   nothing resumable. librqbit writes a piece 16 KiB at a time and resumes
//!   it, so its staged copy is addressable.
//! * **The commit trigger.** Completeness for a URL response has to be
//!   established internally: there is no hash to check the bytes against. So
//!   a chunk is buffered until it is `chunk_len` bytes and that count is
//!   passed to the store as the commit criterion, where a torrent piece
//!   passes `None` -- librqbit never writes padding, so a piece whose tail is
//!   padding is legally short. A chunk file whose length disagrees with its
//!   entity is refused at the read and deleted, never served (see
//!   [`Cached::body`]).
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
//!   generations of one resource apart**, and it is what keeps chunks of two
//!   of them out of one directory and out of one body. Length and type say
//!   nothing about a URL whose content was replaced by content of the same
//!   size -- which is why a response the origin will identify by neither
//!   `ETag` nor `Last-Modified` is not kept at all
//!   (`routes::proxy::cacheable_entity`). What the origin says is the whole
//!   of the evidence, and it is the whole of what is claimed here: an origin
//!   that serves new bytes under an old validator is being untruthful, and
//!   nothing in this module can catch that.
//! * `<bucket>` is `<chunk> / 1000`, the chunk store's own bucketing
//!   ([`enginefs::chunk_store::CHUNKS_PER_DIRECTORY`]): exFAT and FAT32 scan
//!   a directory linearly, and that is exactly where a phone's cache lives.
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
//! # What bounds it, and where the playhead comes from
//!
//! The cleaner is the outer bound and it is not a fast one: it walks the
//! volume a minute after the last write at best, and a proxied stream at
//! 20 MB/s writes a gigabyte in that minute. So the same retention policy
//! the piece store is under bounds this too --
//! `enginefs::piece_store::policy`, one budget, a window roughly 90% ahead
//! of the playhead and 10% behind it -- driven by
//! [`crate::proxy_retention`]. The only input it was missing is the
//! playhead: a proxied stream serves ranges, so the reads were always here,
//! but nothing recorded where playback had got to. It is recorded now, in
//! the two places a byte of a proxied response reaches a player --
//! [`Cached::body`] for what came off the disk and [`Filler::take`] for what
//! came off the origin -- and nowhere else. A `Range` header is what a
//! player *asks* for and is not one of them.
//!
//! Two things follow for what may be unlinked, and they are the same fact
//! from two sides. A chunk inside a live stream's window is not offered to
//! the cleaner's size rule
//! (`enginefs::retention::ReclaimGate::releases_file`), and a chunk an open
//! body has been framed to deliver and has not delivered yet is offered to
//! neither the cleaner nor the retention pass -- a response says its length
//! before its first byte goes out, and a window 90% ahead of the playhead
//! does not cover a body longer than that, so without it the pass a read's
//! own playhead drives would delete that read's tail. `Cached` opens a
//! [`crate::proxy_retention::Reader`] and tells it what the response is
//! framed around; the promise shrinks as the bytes go out and is released
//! when the body ends. Everything else here, including every byte of every
//! stream nobody is reading, is ordinary cache exactly as before.
//!
//! # What is not here, and no comment may imply otherwise
//!
//! * **Nothing here is revalidated.** No `If-None-Match`, no
//!   `If-Modified-Since`, no freshness lifetime, no `Age`. An entry is served
//!   until the cleaner evicts it. The origin's validator *is* kept -- it is a
//!   third of the entity's directory name -- but it is only ever compared
//!   when the origin is being asked for something anyway, and a read this
//!   store answers in full asks the origin nothing. So a resource that
//!   changed under the URL is served as it was until this key next misses,
//!   and the fill that misses is what discovers the change and drops what was
//!   held of the old entity. That is the single largest thing this does not
//!   do.
//!
//!   The one conditional the cache itself adds to a request is the
//!   `If-Range` that goes with a narrowed range, and that is not revalidation
//!   either: it asks the origin to answer the tail *only* while the head
//!   being narrowed against is still part of the entity, and it rides on a
//!   fetch the player had asked for regardless.
//! * **No `Vary`.** The key is fixed (below); the header is not read.
//! * **No credentialed responses at all.** Not "keyed carefully" -- refused,
//!   in [`ProxyCache::entry`].
//! * **No coalescing.** Two players filling the same missing chunk both fetch
//!   it.
//! * **Nothing is kept of the chunk a fetch starts inside.** Only whole
//!   chunks are written and there is no completing a chunk whose front the
//!   response does not carry, so the bytes before a body's first chunk
//!   boundary are dropped rather than provoking a wider fetch than the player
//!   asked for. Every whole chunk after that boundary is written as usual.

use crate::proxy_retention::ProxyRetention;
use bytes::Bytes;
use enginefs::chunk_store::ChunkDir;
use futures_util::Stream;
use reqwest::Method;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
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

/// One cache, rooted where the cleaner can find it.
pub struct ProxyCache {
    root: PathBuf,
    /// Where playback has got to in each entity being read, and the window
    /// that follows it (see [`crate::proxy_retention`]). It lives here
    /// rather than beside the cache because the two things that can observe
    /// a proxied playhead are both this module's -- the body served from
    /// disk and the body filled on its way past -- and a playhead nothing
    /// observes is the gap this whole policy exists to close.
    retention: Arc<ProxyRetention>,
}

impl ProxyCache {
    /// The cache under an engine's `download_dir` -- the directory
    /// `cache_cleaner::cache_roots` already walks -- bounded by the cache
    /// cleaner's cap, which is the *same* cell the torrent half reads
    /// (`EngineFS::cache_budget`) and not a second copy of the number.
    pub fn new(download_dir: &Path, budget: Arc<enginefs::retention::RetentionBudget>) -> Self {
        Self {
            root: download_dir.join(PROXY_CACHE_DIR),
            retention: Arc::new(ProxyRetention::new(budget)),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The playheads and windows of what is being read right now, for the
    /// cache cleaner's gate.
    pub fn retention(&self) -> &Arc<ProxyRetention> {
        &self.retention
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
    ///   origin sends. What is stored here is origin bytes, and both things
    ///   `r=` does to them are done to a hit exactly as to a fetch: the
    ///   playlist verdict is `routes::proxy::is_a_playlist`, asked of
    ///   **every** hit -- whole or partial -- before it answers and before a
    ///   fetch is narrowed against it, and the overrides are
    ///   `apply_custom_response_headers`, applied by both. That is what the
    ///   omission rests on, and it is load-bearing at both ends: while a
    ///   full hit answered before the classification, an `r=` that turned the
    ///   verdict over got the rewrite on a miss and the origin's own body on
    ///   a hit; while a partial hit narrowed before it, the same `r=` got a
    ///   raw unrewritten tail of a range the player never asked for. Both are
    ///   this key promising something it did not do. (The note is here
    ///   because the design this was built from asserted the opposite. A
    ///   cache of assembled *responses* would have to key on it.)
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
            retention: self.retention.clone(),
            target: url.as_str().into(),
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
    retention: Arc<ProxyRetention>,
    /// The origin URL this entry is of, carried down to every reader it
    /// opens so that a client holding a `/proxy` URL can ask what is held
    /// for the stream it is playing. See [`crate::proxy_retention`].
    target: Arc<str>,
}

impl Entry {
    /// What is on disk for the range this request asked for, or `None` when
    /// there is nothing here the request can be answered from.
    ///
    /// Blocking: it reads one directory per thousand chunks of the range,
    /// from the chunk the range starts in to the first one missing. Call it
    /// on the blocking pool -- it is a handful of `getdents` for a cached
    /// film, but a handful on a phone's flash is still not nothing.
    ///
    /// It used to stat every chunk file instead, from the range's first
    /// chunk to its last: `Range: bytes=0-` on a fully cached 2 GB film was
    /// eight thousand `statx`, on every rewatch, all of it before the first
    /// byte. The names in a bucket directory say which chunks are there; what
    /// the stat added was each file's length, and that check has moved to
    /// the one place the file is opened anyway ([`Cached::body`]). **What
    /// this reads as held is therefore a chunk at its final name**, which is
    /// the claim the store is built to make good: a chunk gets that name by
    /// being renamed into it whole. A file of some other length under that
    /// name is an accident's, and the read refuses it -- see `body` for why
    /// that is now the better place to.
    ///
    /// `range` is the request's `Range` header as it arrived. **A request
    /// with no `Range` is answered only from a complete entry**: it asks for
    /// the whole entity, and a partial answer to it would have to be re-framed
    /// as a `200` carrying a body stitched out of a cached head and the
    /// origin's `206` tail. The shape a player actually sends is
    /// `Range: bytes=0-`, which is the ranged path.
    pub fn look_up(&self, range: Option<&str>) -> Option<Cached> {
        let (dir, total, content_type, validator) = self.sole_entity()?;
        let dir = ChunkDir::new(dir);
        let (first, last) = match range {
            Some(header) => crate::routes::util::parse_range(header, total)?,
            None => (0, total.checked_sub(1)?),
        };

        let mut held_to: Option<u64> = None;
        let mut index = first / CHUNK_BYTES;
        // The bucket directory being read from, listed once and consulted
        // for every chunk in it; the walk moves to the next listing when the
        // run of held chunks crosses into the next bucket.
        let mut bucket: Option<(u64, std::collections::HashSet<u64>)> = None;
        loop {
            let in_bucket = index / enginefs::chunk_store::CHUNKS_PER_DIRECTORY;
            if bucket
                .as_ref()
                .is_none_or(|(listed, _)| *listed != in_bucket)
            {
                bucket = Some((in_bucket, dir.held_in_bucket(in_bucket)));
            }
            if !bucket
                .as_ref()
                .is_some_and(|(_, present)| present.contains(&index))
            {
                break;
            }
            let end = index * CHUNK_BYTES + chunk_len(index, total) - 1;
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
        // What a response framed around this will have promised: every
        // chunk from the one the range starts in to the one it ends in.
        let reader = self.retention.reader(&dir, total, self.target.clone());
        reader.promises(first / CHUNK_BYTES..held_to / CHUNK_BYTES + 1);
        Some(Cached {
            dir,
            reader: Arc::new(reader),
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
        let dir = ChunkDir::new(dir);
        Filler {
            reader: self.retention.reader(&dir, total, self.target.clone()),
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
/// `_` is escaped too, which a percent-encoding on its own would not do: it
/// is in the unreserved set `urlencoding::encode` leaves alone, and a content
/// type may hold one (`application/x-foo_bar`), so leaving it would make the
/// separator ambiguous the first time an origin used one. Escaped, the name
/// splits into exactly three fields however an origin spells a type or a
/// tag.
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

/// How long chunk `index` of a `total`-byte entity is: a whole chunk, or
/// whatever is left at the end of the entity.
fn chunk_len(index: u64, total: u64) -> u64 {
    let start = index.saturating_mul(CHUNK_BYTES);
    (total.saturating_sub(start)).min(CHUNK_BYTES)
}

/// What one entity holds for one request's range.
pub struct Cached {
    dir: ChunkDir,
    /// This read, for as long as it lasts: the chunks between
    /// [`Cached::first`] and [`Cached::held_to`] are promised to whatever
    /// response is framed around them, and nothing may unlink one until it
    /// has gone out or this read has ended (see [`crate::proxy_retention`]).
    /// It is also where the bytes that do go out are noted as a playhead.
    ///
    /// An `Arc` because [`Cached::body`] hands the promise to a stream that
    /// outlives this struct -- the route drops the `Cached` once the
    /// response is built, and the body it built is what is still reading.
    reader: Arc<crate::proxy_retention::Reader>,
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
    ///
    /// **This is where a chunk's length is checked, and the only place.** A
    /// chunk file is never written at any length but its entity's, so one
    /// found at another length is an accident's -- a power loss that
    /// committed the rename and not the data is the realistic one -- and it
    /// is not served: the read ends in an error, as above. It is also
    /// *deleted*, which is what makes checking here rather than in the
    /// lookup the better arrangement and not merely the cheaper one. The
    /// lookup used to measure every chunk and read a wrong one as absent,
    /// and the fill skips a chunk whose name is taken, so such a file was
    /// skipped by every lookup and every fill for as long as the cleaner
    /// left it, and the origin was asked for those bytes at every play.
    /// Taken here, it costs the player one broken read, the next lookup
    /// finds the gap, and the next fill writes the chunk again.
    pub fn body(&self) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        let dir = self.dir.clone();
        let reader = self.reader.clone();
        let total = self.total;
        let last = self.held_to;
        futures_util::stream::unfold(self.first, move |offset| {
            let dir = dir.clone();
            let reader = reader.clone();
            async move {
                if offset > last {
                    return None;
                }
                let index = offset / CHUNK_BYTES;
                let start = index * CHUNK_BYTES;
                let want = chunk_len(index, total);
                let path = dir.chunk_path(index);
                let bytes = match tokio::fs::read(&path).await {
                    Ok(bytes) if bytes.len() as u64 == want => bytes,
                    Ok(_) => {
                        let _ = tokio::fs::remove_file(&path).await;
                        return Some((
                            Err(io::Error::other("a cached chunk is not the length it was")),
                            last + 1,
                        ));
                    }
                    Err(error) => return Some((Err(error), last + 1)),
                };
                let to = last.min(start + want - 1);
                // The read's own allocation is the body's: `slice` shares
                // it, where copying the wanted span out was a second 256 KiB
                // allocation and memcpy per chunk served, and twice the
                // chunk in flight at once.
                let served =
                    Bytes::from(bytes).slice((offset - start) as usize..=(to - start) as usize);
                // A byte that really went out, which is the only thing this
                // server will call a playhead. Here and in [`Filler::take`]
                // are the two places one exists for a proxied stream, and
                // between them they cover a hit, a miss and the two halves
                // of a partial hit.
                reader.note(to);
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
    dir: ChunkDir,
    /// This fill, as a read of the entity: the origin's body is on its way
    /// to the player as it goes past here, so every byte of it is a
    /// playhead. It promises nothing -- what a fill delivers comes off the
    /// origin and not off the disk, so there is no chunk of ours it is
    /// waiting to read.
    reader: crate::proxy_retention::Reader,
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
                if self.dir.has_chunk(index) {
                    self.offset += take as u64;
                    bytes = &bytes[take..];
                    continue;
                }
                self.collecting = Some(index);
                self.buffer.clear();
                // The whole chunk's room, once. The finished buffer is handed
                // to the write task below rather than copied, which leaves
                // this one with no capacity at all, and a body arrives in
                // pieces of 8 or 16 KiB -- so without this the buffer grew
                // from the first piece's size by doubling, several
                // reallocations and about one extra copy of the chunk per
                // chunk written.
                self.buffer.reserve_exact(want as usize);
            }
            if self.collecting == Some(index) {
                self.buffer.extend_from_slice(&bytes[..take]);
                if self.buffer.len() as u64 == want {
                    let chunk = std::mem::take(&mut self.buffer);
                    self.collecting = None;
                    let dir = self.dir.clone();
                    tokio::task::spawn_blocking(move || write_chunk(&dir, index, &chunk, want));
                }
            }
            // Bytes before the first chunk boundary of a body belong to a
            // chunk whose front this response does not carry. There is
            // nothing to be done with them but drop them.
            self.offset += take as u64;
            bytes = &bytes[take..];
        }
        // The origin's body is on its way to the player as it goes past
        // here, so this is a playhead exactly as a cached read is -- and it
        // is the one that matters most, since a miss is when the cache is
        // growing and the window is what bounds that growth.
        if self.offset > 0 {
            self.reader.note(self.offset - 1);
        }
    }
}

/// Write one complete chunk through the store's anonymous staging: a
/// temporary no other filler can be using, then the rename into place.
///
/// `want` is the commit criterion -- what this entity says the chunk's length
/// is. There is no hash to check a URL's bytes against, so this count is the
/// whole of what establishes completeness, and it is checked by the store
/// before anything is renamed.
///
/// Nothing fails loudly -- a cache that cannot write is a slower stream and
/// never a broken one.
fn write_chunk(dir: &ChunkDir, index: u64, chunk: &[u8], want: u64) {
    if let Err(error) = dir.write_whole(index, chunk, Some(want)) {
        tracing::debug!(path = %dir.chunk_path(index).display(), %error, "could not write a proxy cache chunk");
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

/// Delete the staged files a kill left behind, at launch.
///
/// **This is not the piece store's sweep, and the difference is the claim
/// set and the staging identity.** A torrent's pieces are claimed by the
/// session, so anything unclaimed there is data nothing will ever reclaim,
/// and its staged copy is addressable and resumable -- so its sweep discards
/// a staged copy only when it *shadows* a complete one. Nothing claims a
/// cached URL: every chunk here is cache, the cleaner counts and evicts all
/// of it, and surviving a restart is the whole point. And a staged chunk
/// here is anonymous: nothing can address it, so nothing can resume it. So
/// the only thing a kill can leave that is not cache is a chunk that was
/// being written when the process died, and all of those go.
///
/// What a staged file is *called* is asked of the store
/// ([`enginefs::chunk_store::is_staged_name`]) rather than spelled a second
/// time here.
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
        if !entry.file_type().is_file()
            || !enginefs::chunk_store::is_staged_name(&entry.file_name().to_string_lossy())
        {
            continue;
        }
        let freed = entry
            .metadata()
            .map(|m| enginefs::chunk_store::occupied_bytes(&m))
            .unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use enginefs::chunk_store::CHUNKS_PER_DIRECTORY;
    use std::collections::HashSet;

    /// The entity directory as the store sees it.
    fn chunks(dir: &Path) -> ChunkDir {
        ChunkDir::new(dir.to_path_buf())
    }

    /// Put a file at a chunk's committed name, whatever its length -- a test
    /// standing in for a fill, and for the accidents a fill cannot produce.
    fn write_chunk(dir: &Path, index: u64, bytes: &[u8]) {
        chunks(dir).write_whole(index, bytes, None).expect("write");
    }

    fn chunk_path(dir: &Path, index: u64) -> PathBuf {
        chunks(dir).chunk_path(index)
    }

    fn committed_chunks(dir: &Path, bucket: u64) -> HashSet<u64> {
        chunks(dir).held_in_bucket(bucket)
    }

    fn cache() -> (tempfile::TempDir, ProxyCache) {
        let dir = tempfile::tempdir().expect("a scratch root");
        // No budget: nothing has published one, which is what these tests
        // are about anyway -- they are about the key, the entity and the
        // arithmetic, and no chunk here is ever reclaimed by the window.
        let cache = ProxyCache::new(dir.path(), Arc::default());
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

    /// A chunk written whole and renamed into place is readable. A file at a
    /// chunk's name whose length disagrees with the entity is never served
    /// -- that would serve a hole as content -- but the place it is caught is
    /// the read, not the lookup: the lookup goes by names, the read refuses
    /// the file and removes it, and from then on the lookup sees the gap it
    /// leaves and a fill can write the chunk again. (The lookup used to
    /// measure every chunk and read a wrong one as absent, which left it in
    /// place for ever: skipped by every lookup, skipped by every fill.)
    #[tokio::test]
    async fn a_chunk_of_the_wrong_length_is_refused_at_the_read_and_removed() {
        use futures_util::StreamExt as _;

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

        // The final chunk of this entity is ten bytes; a full-length file at
        // its name is not that chunk. The lookup takes the name at its word
        // -- nothing but a completed fill puts a file there --
        write_chunk(&dir, 1, &vec![7u8; CHUNK_BYTES as usize]);
        let cached = entry.look_up(Some("bytes=0-")).expect("chunk 0 is here");
        assert!(
            cached.complete(),
            "the lookup goes by the names in the bucket"
        );
        // ...and the read is what finds it out: chunk 0 is served, the
        // impostor ends the body in an error and is gone.
        let served: Vec<Result<Bytes, io::Error>> = cached.body().collect().await;
        assert_eq!(served.len(), 2, "one chunk, then the refusal");
        assert_eq!(
            served[0].as_ref().map(|bytes| bytes.len()).ok(),
            Some(CHUNK_BYTES as usize)
        );
        assert!(served[1].is_err(), "a hole is never served as content");
        assert!(
            !chunk_path(&dir, 1).exists(),
            "and the file is removed rather than left for the next read to trip on"
        );

        // So the next lookup sees the gap and names what a fill must write.
        let cached = entry.look_up(Some("bytes=0-")).expect("chunk 0 is here");
        assert!(!cached.complete());
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
        let served: Vec<Result<Bytes, io::Error>> = cached.body().collect().await;
        assert_eq!(
            served
                .iter()
                .map(|chunk| chunk.as_ref().unwrap().len())
                .sum::<usize>(),
            total as usize
        );
    }

    /// **Reading bytes off the disk is a playhead too.**
    ///
    /// The two places a proxied byte reaches a player are this body and the
    /// origin's on its way past, and only one of them is on the path that
    /// matters here: a rewatch off a warm cache asks the origin nothing at
    /// all, so if a cached read moved no playhead there would be no window
    /// over the very stream a player is inside, and the cache cleaner would
    /// be free to take the chunk under its head.
    ///
    /// Sixteen chunks on disk, a budget of four, and a read of the first
    /// three: what is left afterwards is a window round where the read got
    /// to, and the tail of the entity -- written last, so the *newest* thing
    /// in the directory and the last thing any age or size rule would take
    /// -- is gone. Nothing but the window can produce that shape.
    #[tokio::test]
    async fn a_read_off_the_disk_moves_the_playhead_and_the_window_follows_it() {
        use futures_util::StreamExt as _;

        let dir = tempfile::tempdir().expect("a scratch root");
        let budget = Arc::new(enginefs::retention::RetentionBudget::default());
        budget.set(Some(4 * CHUNK_BYTES));
        let cache = ProxyCache::new(dir.path(), budget);
        let entry = entry_of(&cache, "https://host/film.mkv");

        let total = 16 * CHUNK_BYTES;
        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        let whole = vec![7u8; CHUNK_BYTES as usize];
        for index in 0..16 {
            write_chunk(&dir, index, &whole);
        }

        let cached = entry
            .look_up(Some(&format!("bytes=0-{}", 3 * CHUNK_BYTES - 1)))
            .expect("the whole range is on disk");
        assert!(cached.complete(), "and nothing here asks an origin");
        let served: Vec<Result<Bytes, io::Error>> = cached.body().collect().await;
        assert_eq!(
            served
                .iter()
                .map(|chunk| chunk.as_ref().expect("a chunk").len())
                .sum::<usize>(),
            3 * CHUNK_BYTES as usize
        );

        // The pass runs on the blocking pool once the last byte has gone
        // past. Bounded so a regression fails rather than hangs.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if !chunk_path(&dir, 15).exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            !chunk_path(&dir, 15).exists(),
            "the end of the film is nowhere near the playhead, and it went"
        );
        assert!(
            chunk_path(&dir, 2).is_file(),
            "the chunk the read ended in is still here"
        );
        assert!(
            chunks(&dir).held().len() <= 5,
            "and what is left is a window, not sixteen chunks: {:?}",
            chunks(&dir).held()
        );
    }

    /// **A read is handed every byte its response was framed around, while
    /// the window its own reading moves passes over them.**
    ///
    /// The `Content-Length` and `Content-Range` of a hit are a promise about
    /// chunks that are still on the disk when the player gets to them, and
    /// the thing most likely to take one is not the cleaner: it is the
    /// retention pass this very body's playhead is driving. A window is 90%
    /// ahead of the playhead, so a body longer than that has its own tail
    /// outside the window from its first chunk onwards.
    ///
    /// Twenty chunks on disk, four chunks of budget, a read of the first
    /// sixteen. The pass really runs -- the four chunks nothing promised go
    /// while the body is open, which is what says this is not a test of a
    /// pass that never happened -- and the read is served whole regardless.
    #[tokio::test]
    async fn a_read_is_served_every_byte_it_was_promised() {
        use futures_util::StreamExt as _;

        let dir = tempfile::tempdir().expect("a scratch root");
        let budget = Arc::new(enginefs::retention::RetentionBudget::default());
        budget.set(Some(4 * CHUNK_BYTES));
        let cache = ProxyCache::new(dir.path(), budget);
        let entry = entry_of(&cache, "https://host/film.mkv");

        let total = 20 * CHUNK_BYTES;
        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        let whole = vec![7u8; CHUNK_BYTES as usize];
        for index in 0..20 {
            write_chunk(&dir, index, &whole);
        }

        let want = 16 * CHUNK_BYTES;
        let cached = entry
            .look_up(Some(&format!("bytes=0-{}", want - 1)))
            .expect("all of it is here");
        assert!(cached.complete());

        let mut served = 0usize;
        let mut body = Box::pin(cached.body());
        let mut waited = false;
        while let Some(next) = body.next().await {
            match next {
                Ok(bytes) => served += bytes.len(),
                Err(error) => panic!("the body broke after {served} of {want}: {error}"),
            }
            if waited {
                continue;
            }
            // A player reads as it plays, so the pass its own playhead
            // started runs while the body is still open. Wait for it once,
            // on something it is free to take: the four chunks past the end
            // of what this read promised.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if !chunk_path(&dir, 19).exists() {
                    waited = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(waited, "no pass ever ran, so this proves nothing");
        }
        assert_eq!(served as u64, want, "every byte the response promised");
    }

    /// The lookup reads bucket directories, not chunk files, so what is in a
    /// bucket that is not a chunk must not be mistaken for one, and a run of
    /// held chunks must be followed from one bucket's listing into the next.
    #[test]
    fn the_lookup_reads_names_and_follows_a_run_into_the_next_bucket() {
        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        // The entity's last chunk is the first of bucket 1, so two files are
        // enough to hold the boundary: 999 whole, 1000 the ten-byte tail.
        let total = CHUNKS_PER_DIRECTORY * CHUNK_BYTES + 10;
        let dir = entry
            .dir
            .join(entity_dir_name(total, "video/mp4", VALIDATOR));
        write_chunk(&dir, 0, &vec![0u8; CHUNK_BYTES as usize]);
        write_chunk(&dir, 999, &vec![9u8; CHUNK_BYTES as usize]);
        write_chunk(&dir, 1000, &[1u8; 10]);
        // What else a bucket can hold: a chunk still being written, a
        // directory with a chunk's name, and names that parse as a chunk
        // index without being one.
        let bucket = chunk_path(&dir, 0).parent().unwrap().to_path_buf();
        std::fs::write(bucket.join("1.4242-7.part"), [1u8; 64]).unwrap();
        std::fs::create_dir(bucket.join("2")).unwrap();
        std::fs::write(bucket.join("003"), vec![3u8; CHUNK_BYTES as usize]).unwrap();
        std::fs::write(bucket.join("+4"), vec![4u8; CHUNK_BYTES as usize]).unwrap();
        std::fs::write(bucket.join("1005"), vec![5u8; CHUNK_BYTES as usize]).unwrap();

        assert_eq!(
            committed_chunks(&dir, 0),
            HashSet::from([0, 999]),
            "chunks are the files spelled the way a fill spells them"
        );
        assert_eq!(committed_chunks(&dir, 1), HashSet::from([1000]));
        assert!(
            committed_chunks(&dir, 2).is_empty(),
            "a bucket that was never written"
        );

        let head = entry.look_up(Some("bytes=0-")).expect("chunk 0 is here");
        assert_eq!(
            head.held_to,
            CHUNK_BYTES - 1,
            "the run ends at the first gap"
        );

        let tail = entry
            .look_up(Some(&format!("bytes={}-", 999 * CHUNK_BYTES)))
            .expect("chunk 999 is here");
        assert!(
            tail.complete(),
            "the run crosses from bucket 0 into bucket 1"
        );
        assert_eq!(tail.held_to, total - 1);

        assert!(
            entry
                .look_up(Some(&format!("bytes={}-", 998 * CHUNK_BYTES)))
                .is_none(),
            "a seek into a hole is not a hit"
        );
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
        assert!(filler.dir.path().is_dir());
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

    /// A chunk is collected into one allocation of its own size, however
    /// small the pieces it arrives in: the buffer is handed to the write
    /// task whole, so the next chunk starts from nothing, and left to grow by
    /// doubling from an 8 KiB piece it reallocated -- and copied -- its way
    /// to 256 KiB on every chunk of every stream.
    #[tokio::test]
    async fn a_chunk_is_collected_into_one_allocation_of_its_own_size() {
        const PIECE: usize = 8 * 1024;

        let (_root, cache) = cache();
        let entry = entry_of(&cache, "https://host/film.mkv");
        let total = CHUNK_BYTES * 2;
        let mut filler = entry.fill(total, "video/mp4", VALIDATOR, 0);
        let piece = vec![5u8; PIECE];

        for chunk in 0..2u64 {
            filler.take(&piece);
            let capacity = filler.buffer.capacity();
            let allocation = filler.buffer.as_ptr();
            assert_eq!(
                capacity as u64, CHUNK_BYTES,
                "chunk {chunk}: the first piece reserves the whole chunk"
            );
            for _ in 1..(CHUNK_BYTES as usize / PIECE) - 1 {
                filler.take(&piece);
                assert_eq!(
                    filler.buffer.capacity(),
                    capacity,
                    "chunk {chunk}: no regrowth"
                );
                assert_eq!(
                    filler.buffer.as_ptr(),
                    allocation,
                    "chunk {chunk}: and so no move of what was collected"
                );
            }
            // The last piece completes the chunk, which goes to the writer;
            // the next chunk starts with an empty buffer and reserves again.
            filler.take(&piece);
            assert_eq!(filler.collecting, None);
            assert_eq!(
                filler.buffer.capacity(),
                0,
                "the allocation went with the chunk"
            );
        }
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

    /// The three fields go into one directory name, and a name past what a
    /// filesystem holds is the one refusal that is about none of the things
    /// the origin said -- only about how long they are.
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
