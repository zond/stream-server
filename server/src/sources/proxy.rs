//! An HTTP entity as a [`ByteSource`], read through `/proxy`'s own cache.
//!
//! This is the source an archive behind a web link is read from: the
//! `urls` an addon puts in `rarUrls`/`zipUrls`, a debrid link, a CDN. A
//! read of it is the route's own read -- [`cache_assisted_range`], which
//! serves what the cache holds and fetches exactly the rest under
//! `If-Range`, filling the cache on the way past. The cache stays the
//! owner of those bytes: its retention pass keeps reclaiming them and
//! `/stream-numbers.json` keeps answering for them, exactly as if the
//! player had fetched the file through `/proxy` itself.
//!
//! **An origin that will not range is refused here**, at construction,
//! rather than read from. Serving one would mean downloading the whole
//! archive to reach a member's bytes, which is the thing this design
//! exists to prevent; the refusal has a sentence the player can show
//! ([`ProxySourceError`]).
//!
//! Credentials travel as they already do: `h=` request headers, which the
//! proxy cache refuses to key on at all (so a credentialed target is read
//! without a cache rather than into a store another caller could name),
//! and which reach no log -- [`ByteSource::describe`] is the target's
//! origin and nothing else.

use super::{ByteSource, ReadHint, SeekableReader, read_filling};
use crate::routes::proxy::{
    FetchFailure, OriginAnswer, ProxiedBody, RangeAnswer, cache_assisted_range, cache_filling,
    origin_body, with_cached_head,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use reqwest::Method;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use url::Url;

/// Why a URL could not be opened as a source. Each one is a sentence a
/// player can show: the caller named something this server will not read a
/// range of, and the reason is not the caller's fault to guess at.
#[derive(Debug)]
pub enum ProxySourceError {
    /// The origin answered a ranged request with the whole entity. Reading
    /// a member out of it would mean downloading all of it.
    WillNotRange,
    /// The origin answered something that is not the resource.
    Origin(StatusCode),
    /// It could not be reached at all.
    Fetch(String),
}

impl std::fmt::Display for ProxySourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WillNotRange => write!(
                f,
                "this link's host does not serve byte ranges, so a file inside it could only be \
                 played by downloading the whole thing"
            ),
            Self::Origin(status) => write!(f, "this link's host answered {status}"),
            Self::Fetch(error) => write!(f, "this link's host could not be reached: {error}"),
        }
    }
}

impl std::error::Error for ProxySourceError {}

impl From<FetchFailure> for ProxySourceError {
    fn from(failure: FetchFailure) -> Self {
        Self::Fetch(failure.into_io_error().to_string())
    }
}

/// One HTTP entity, read by range through the proxy cache.
pub struct ProxySource {
    entity: Arc<Entity>,
}

/// What issuing a ranged read of this entity takes. Behind an `Arc`
/// because a reader outlives the call that opened it and has to be able to
/// ask again -- which is what a seek on one is.
struct Entity {
    /// The cache the reads go through. The entry is taken per read, as the
    /// route takes one per request: an `Entry` is a key's directory and
    /// the doors onto it, and asking again is what makes a source built
    /// before a sweep read correctly after one.
    cache: Arc<crate::proxy_cache::ProxyCache>,
    /// This server's own listener, which is what tells a target on it from
    /// an ordinary loopback origin -- an addon a viewer runs on the same
    /// machine is not us. See `ProxyCache::entry`.
    self_addr: SocketAddr,
    url: Url,
    /// The `h=` overrides, as the proxy carries them. Never logged, and
    /// never keyed on: a credential among them is what makes
    /// [`Self::entry`] `None`, so a credentialed read is an uncached one.
    request_headers: BTreeMap<String, String>,
    total: u64,
    content_type: String,
    /// How the origin identified the entity, as the store files it.
    /// `None` for an origin that identified it by neither `ETag` nor
    /// `Last-Modified` -- readable, but never kept, since nothing could
    /// tell a later generation of it from this one.
    validator: Option<String>,
    /// What a log may say about this source: the origin and nothing else.
    describe: String,
}

impl ProxySource {
    /// Probe `url` and open it as a source, or say why it cannot be one.
    ///
    /// The probe is one `GET` of `bytes=0-0` through the proxy's own
    /// request builder -- its redirect chain, its credential rule, its
    /// `h=` overrides -- which is how this learns the entity's length, the
    /// type the origin labels it with, the validator it identifies it by,
    /// and the one thing that decides whether there is a source here at
    /// all: whether a `Range` gets a `206`.
    ///
    /// It deliberately goes to the origin rather than to the cache. What
    /// is being established is what the origin will do *now* with a
    /// ranged request, and a store that never revalidates cannot answer
    /// that; a byte off the disk would say only what it did once.
    ///
    /// `pub(crate)` rather than `pub` because a `ProxyCache` is not part
    /// of the embeddable API; `routes::archive` builds one of these per
    /// URL a `/{fmt}/create` names.
    pub(crate) async fn open(
        cache: Arc<crate::proxy_cache::ProxyCache>,
        self_addr: SocketAddr,
        url: Url,
        request_headers: BTreeMap<String, String>,
    ) -> Result<Self, ProxySourceError> {
        let mut probe = HeaderMap::new();
        probe.insert(header::RANGE, HeaderValue::from_static("bytes=0-0"));
        let answer =
            cache_assisted_range(None, &Method::GET, &url, &probe, &request_headers, None).await?;
        let RangeAnswer::Origin(origin) = answer else {
            // Unreachable with no entry to look up in, and stated rather
            // than assumed: a hit here would be an answer about the store
            // where the question was about the origin.
            return Err(ProxySourceError::Fetch(
                "the probe was answered from the cache".to_string(),
            ));
        };
        let OriginAnswer {
            status,
            res_headers,
            ..
        } = *origin;
        let Some(entity) = crate::routes::proxy::probed_entity(status, &res_headers) else {
            return Err(if status.is_success() {
                // A `200` to `Range: bytes=0-0` is the origin saying it
                // will send the file and nothing less.
                ProxySourceError::WillNotRange
            } else {
                ProxySourceError::Origin(status)
            });
        };
        Ok(Self {
            entity: Arc::new(Entity {
                cache,
                self_addr,
                describe: crate::routes::util::log_origin(url.as_str()),
                url,
                request_headers,
                total: entity.total,
                content_type: entity.content_type,
                validator: entity.validator,
            }),
        })
    }

    /// The type the origin labelled the entity with, for a caller that has
    /// to say what a member of it is.
    pub fn content_type(&self) -> &str {
        &self.entity.content_type
    }

    /// How the origin identifies the entity, as the store files it, or
    /// `None` for one that identifies it by nothing.
    pub fn validator(&self) -> Option<&str> {
        self.entity.validator.as_deref()
    }
}

impl Entity {
    /// The cache entry these reads go through: **the very same one
    /// `/proxy` would use for this URL and these `h=` headers**, because it
    /// is the same call. `None` for a target the cache will not touch --
    /// a credentialed one, or this server's own listener.
    ///
    /// The player headers are empty, and that is the whole of the
    /// difference from a request a player makes: `accept`,
    /// `accept-language` and `user-agent` are content negotiation and are
    /// keyed on, so a source reading with none of them reads the entity a
    /// request with none of them would. `Range` is not keyed on either
    /// way.
    fn entry(&self) -> Option<crate::proxy_cache::Entry> {
        self.cache.entry(
            &Method::GET,
            &self.url,
            &self.request_headers,
            &HeaderMap::new(),
            self.self_addr,
        )
    }

    /// The bytes `first..=last` of the entity: the cache's where it holds
    /// them, the origin's where it does not, filled back into the cache as
    /// they go past.
    async fn ranged(&self, first: u64, last: u64) -> io::Result<ProxiedBody> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::RANGE,
            HeaderValue::from_str(&format!("bytes={first}-{last}"))
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
        );
        let answer = cache_assisted_range(
            self.entry(),
            &Method::GET,
            &self.url,
            &headers,
            &self.request_headers,
            None,
        )
        .await
        .map_err(FetchFailure::into_io_error)?;
        match answer {
            RangeAnswer::Hit(cached) => Ok(Box::pin(cached.body())),
            RangeAnswer::Origin(origin) => {
                let OriginAnswer {
                    response,
                    status,
                    cacheable,
                    entry,
                    head,
                    ..
                } = *origin;
                if status != StatusCode::PARTIAL_CONTENT {
                    // The origin ranged when it was probed and does not
                    // now -- or the `If-Range` found the head stale and it
                    // sent the whole of a new entity. Either way what
                    // arrived is not the span that was asked for, and the
                    // one thing this must not do is read from the top of
                    // the file until it reaches the bytes it wanted.
                    return Err(io::Error::other(format!(
                        "{} answered {status} to a ranged read",
                        self.describe
                    )));
                }
                Ok(with_cached_head(
                    cache_filling(origin_body(response), cacheable, entry),
                    head,
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl ByteSource for ProxySource {
    fn len(&self) -> u64 {
        self.entity.total
    }

    /// The entity being played is this source's own when it is a proxied
    /// body filed under this source's key -- the key `/proxy` would mint
    /// for the same URL and the same `h=` headers, which is the same call
    /// ([`Self::entry`]).
    ///
    /// Asked of the *key* and not of the entity directory under it: the
    /// entity is one generation of the resource, and a container indexed
    /// from a link is still the same container after the origin has
    /// revalidated it (`Reading::proxy_under`). A target the cache will
    /// not touch -- a credentialed one, this server's own listener -- has
    /// no key, and nothing the cell can name; it reads every time as
    /// "not playing", which is correct, since nothing here retains a byte
    /// of it either.
    fn is_live(&self, reading: &enginefs::retention::live::Reading) -> bool {
        self.entity
            .entry()
            .is_some_and(|entry| reading.proxy_under(entry.dir()))
    }

    fn describe(&self) -> String {
        self.entity.describe.clone()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let Some(last) = ReadHint::of(buf.len() as u64).last_byte(offset, self.entity.total) else {
            return Ok(0);
        };
        let stream = self.entity.ranged(offset, last).await?;
        let mut reader = tokio_util::io::StreamReader::new(stream);
        read_filling(&mut reader, buf).await
    }

    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        Ok(Box::new(ProxyReader {
            entity: self.entity.clone(),
            pos: offset,
            // One ranged request for the whole span the caller says it
            // wants, which is what the hint is for: a body read that asked
            // per chunk would be a request per chunk at the origin. A
            // reader that runs past it simply asks for the next span.
            until: hint.last_byte(offset, self.entity.total),
            state: ReaderState::Idle,
            seeking: None,
        }))
    }
}

/// A ranged fetch in flight, as the reader's state machine holds it.
type Fetching =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<ProxiedBody>> + Send>>;

/// A handle on the entity: reads through the cache, and **seeks by ending
/// the response it is reading and asking for one at the new offset**.
///
/// That is what a seek on a proxied file already is -- a player dragging
/// the scrubber through `/proxy` makes exactly this request -- and it is
/// the one thing an HTTP source can do about a seek. What it costs is one
/// request, and what it saves is every consumer of a fetched file being
/// able to treat it as a file (see [`super::SeekableReader`]).
struct ProxyReader {
    entity: Arc<Entity>,
    /// Where the next byte comes from.
    pos: u64,
    /// The last byte of the span the current -- or next -- fetch covers,
    /// or `None` for "to the end of the entity".
    until: Option<u64>,
    state: ReaderState,
    /// Where `start_seek` said to go, until `poll_complete` takes it.
    seeking: Option<u64>,
}

enum ReaderState {
    /// Nothing open: the next read asks for the span at `pos`.
    Idle,
    /// A ranged read is being set up.
    Fetching(Fetching),
    /// A response is being read.
    Reading(tokio_util::io::StreamReader<ProxiedBody, bytes::Bytes>),
}

impl tokio::io::AsyncRead for ProxyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        use std::task::Poll;
        let this = self.get_mut();
        loop {
            match &mut this.state {
                ReaderState::Idle => {
                    if this.pos >= this.entity.total {
                        return Poll::Ready(Ok(()));
                    }
                    let last = this.until.unwrap_or(this.entity.total - 1).max(this.pos);
                    let entity = this.entity.clone();
                    let first = this.pos;
                    this.state =
                        ReaderState::Fetching(Box::pin(
                            async move { entity.ranged(first, last).await },
                        ));
                }
                ReaderState::Fetching(fetch) => match fetch.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        this.state = ReaderState::Idle;
                        return Poll::Ready(Err(error));
                    }
                    Poll::Ready(Ok(body)) => {
                        this.state = ReaderState::Reading(tokio_util::io::StreamReader::new(body));
                    }
                },
                ReaderState::Reading(reader) => {
                    let before = buf.filled().len();
                    match std::pin::Pin::new(reader).poll_read(cx, buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            this.state = ReaderState::Idle;
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(Ok(())) => {
                            let read = (buf.filled().len() - before) as u64;
                            if read == 0 {
                                // The response this was reading is over.
                                // A reader is a handle on the whole
                                // entity and not a window onto the span
                                // it was hinted, so a caller still
                                // reading gets the next span -- one more
                                // request, to the end. A response that
                                // ends while *that* is what is being read
                                // is the entity's own end.
                                this.state = ReaderState::Idle;
                                match this.until.take() {
                                    Some(_) if this.pos < this.entity.total => continue,
                                    _ => return Poll::Ready(Ok(())),
                                }
                            }
                            this.pos += read;
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
            }
        }
    }
}

impl tokio::io::AsyncSeek for ProxyReader {
    fn start_seek(self: std::pin::Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let target = match position {
            io::SeekFrom::Start(from_start) => from_start,
            io::SeekFrom::End(from_end) => {
                if from_end < 0 {
                    this.entity.total.saturating_sub(from_end.unsigned_abs())
                } else {
                    this.entity.total.saturating_add(from_end as u64)
                }
            }
            io::SeekFrom::Current(from_here) => {
                let there = this.pos as i128 + i128::from(from_here);
                if there < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Negative seek"));
                }
                there as u64
            }
        };
        this.seeking = Some(target);
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<u64>> {
        let this = self.get_mut();
        if let Some(target) = this.seeking.take()
            && target != this.pos
        {
            this.pos = target;
            // The response being read covers the old offset and nothing
            // else; the next read asks for one that covers this one.
            this.state = ReaderState::Idle;
            this.until = None;
        }
        std::task::Poll::Ready(Ok(this.pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    /// One byte of the origin's body at `offset`: a pattern, so a range
    /// can be checked to have come from the offset it claims rather than
    /// merely to be the right length.
    fn byte_at(offset: usize) -> u8 {
        (offset % 251) as u8
    }

    const ORIGIN_LENGTH: usize = 1024 * 1024;
    const ORIGIN_ETAG: &str = "\"the-archive\"";

    /// A test origin on loopback that answers ranges, and counts what it
    /// was asked for -- which is the whole of what the cache tests assert:
    /// a read the store answered is a request the origin never saw.
    struct Origin {
        addr: SocketAddr,
        requests: Arc<AtomicUsize>,
    }

    impl Origin {
        /// An origin that answers every ranged request with a `206`.
        fn start(ranges: bool) -> Origin {
            Self::ranging(move |_| ranges)
        }

        /// One that decides per request, by how many it has answered
        /// (1-based): `|_| false` is an origin that ignores `Range` and
        /// answers `200` with the whole file, and `|n| n == 1` is one that
        /// ranges while it is being probed and stops afterwards.
        fn ranging(ranges: impl Fn(usize) -> bool + Send + 'static) -> Origin {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a loopback port");
            let addr = listener.local_addr().expect("the bound address");
            let requests = Arc::new(AtomicUsize::new(0));
            let counted = requests.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { break };
                    let nth = counted.fetch_add(1, Ordering::SeqCst) + 1;
                    answer(&mut stream, ranges(nth));
                }
            });
            Origin { addr, requests }
        }

        fn url(&self) -> Url {
            Url::parse(&format!("http://{}/film.zip", self.addr)).expect("a literal URL")
        }

        fn asked(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    fn answer(stream: &mut TcpStream, ranges: bool) {
        let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
        let mut range = None;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("a request line") == 0 {
                return;
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("range: ") {
                range = Some(value.trim().to_string());
            }
        }
        let (first, last) = match range.as_deref().filter(|_| ranges) {
            Some(header) => {
                let (first, last) = header
                    .trim_start_matches("bytes=")
                    .split_once('-')
                    .expect("a range");
                let first: usize = first.parse().expect("a first byte");
                let last: usize = if last.is_empty() {
                    ORIGIN_LENGTH - 1
                } else {
                    last.parse().expect("a last byte")
                };
                (first, last)
            }
            None => (0, ORIGIN_LENGTH - 1),
        };
        let body: Vec<u8> = (first..=last).map(byte_at).collect();
        let head = if ranges {
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: application/zip\r\nETag: \
                 {ORIGIN_ETAG}\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {first}-{last}/\
                 {ORIGIN_LENGTH}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nETag: {ORIGIN_ETAG}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
        };
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.flush();
    }

    /// Where the server under test is listening. The origins above are
    /// somewhere else, which is what an origin is.
    const SELF_ADDR: SocketAddr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        11470,
    );

    fn cache() -> (tempfile::TempDir, Arc<crate::proxy_cache::ProxyCache>) {
        let dir = tempfile::tempdir().expect("a scratch root");
        let cache = crate::proxy_cache::ProxyCache::new(dir.path(), Arc::default(), Arc::default());
        (dir, Arc::new(cache))
    }

    /// **A second read of a range is the disk's**, and the origin is asked
    /// once -- which is the whole of why a member is read through the
    /// proxy cache rather than beside it.
    #[tokio::test]
    async fn a_range_read_twice_asks_the_origin_once() {
        let origin = Origin::start(true);
        let (_root, cache) = cache();
        let source = ProxySource::open(cache.clone(), SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        assert_eq!(super::ByteSource::len(&source), ORIGIN_LENGTH as u64);
        assert_eq!(source.content_type(), "application/zip");
        assert_eq!(source.validator(), Some("etag:\"the-archive\""));
        // The probe, and nothing else yet.
        let probed = origin.asked();
        assert_eq!(probed, 1);

        // A whole chunk, so what is read fills one and is committed --
        // a fill that never completes a chunk leaves nothing on disk, by
        // design (`proxy_cache::Filler`).
        let want = crate::proxy_cache::CHUNK_BYTES as usize;
        let mut first = vec![0u8; want];
        assert_eq!(source.read_at(0, &mut first).await.unwrap(), want);
        assert_eq!(first, (0..want).map(byte_at).collect::<Vec<_>>());
        assert_eq!(origin.asked(), probed + 1);
        cache.settled().await;

        let mut again = vec![0u8; want];
        assert_eq!(source.read_at(0, &mut again).await.unwrap(), want);
        assert_eq!(again, first);
        assert_eq!(
            origin.asked(),
            probed + 1,
            "the second read of the same range went to the origin"
        );
    }

    /// And the bytes land in **the entry `/proxy` would have used for this
    /// URL and these headers**: the source reads the same store a player
    /// reading the same link reads, not one of its own.
    #[tokio::test]
    async fn the_bytes_land_in_the_entry_the_route_would_use() {
        let origin = Origin::start(true);
        let (root, cache) = cache();
        let source = ProxySource::open(cache.clone(), SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        let want = crate::proxy_cache::CHUNK_BYTES as usize;
        let mut buf = vec![0u8; want];
        source.read_at(0, &mut buf).await.unwrap();
        cache.settled().await;

        // The key a `/proxy` request for this target mints, built the way
        // the route builds it: a player's `Range` is not part of it, which
        // is what lets a source's ranged read and a player's find each
        // other's bytes.
        let mut player = HeaderMap::new();
        player.insert(header::RANGE, HeaderValue::from_static("bytes=0-1023"));
        let route_entry = cache
            .entry(
                &Method::GET,
                &origin.url(),
                &BTreeMap::new(),
                &player,
                SELF_ADDR,
            )
            .expect("a cacheable target");
        assert_eq!(
            route_entry.dir(),
            source.entity.entry().expect("an entry").dir()
        );

        // And there really are chunks under it.
        let entity = std::fs::read_dir(route_entry.dir())
            .expect("the key directory")
            .next()
            .expect("one entity")
            .expect("a readable entry")
            .path();
        assert!(
            walkdir::WalkDir::new(&entity)
                .into_iter()
                .filter_map(Result::ok)
                .any(|found| found.file_type().is_file()),
            "the fill wrote no chunk under {}",
            entity.display()
        );
        drop(root);
    }

    /// **A source that has read is the live entity, and a probe is not a
    /// read.** This is the whole rule a link-borne container's session
    /// hangs on (`crate::translators::session`): the session stays while
    /// its own container is what is playing, and this is how the container
    /// says so.
    ///
    /// The probe goes to the origin with no cache entry at all, so it
    /// opens no reader and moves nothing -- a `/{fmt}/create` that only
    /// asked whether an origin ranges has not made the viewer's stream --
    /// and the first real read does.
    #[tokio::test]
    async fn a_source_is_the_live_entity_once_it_has_read_and_not_before() {
        use enginefs::retention::live::{Live, LiveEntity};

        let origin = Origin::start(true);
        let root = tempfile::tempdir().expect("a scratch root");
        let live = Arc::new(Live::new());
        let cache = Arc::new(crate::proxy_cache::ProxyCache::new(
            root.path(),
            Arc::default(),
            live.clone(),
        ));
        let source = ProxySource::open(cache.clone(), SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        assert!(
            !ByteSource::is_live(&source, &live.reading()),
            "the probe opened a reader"
        );

        let mut buf = vec![0u8; 1024];
        source.read_at(0, &mut buf).await.unwrap();
        assert!(
            ByteSource::is_live(&source, &live.reading()),
            "a read through the source did not make it the live entity"
        );

        // And the viewer opens something else.
        live.open(
            LiveEntity::Torrent {
                info_hash: "aa".into(),
                file_idx: 0,
            },
            false,
        );
        assert!(!ByteSource::is_live(&source, &live.reading()));
        drop(root);
    }

    /// **A range the cache holds the head of fetches only its tail**, and
    /// what the read returns is the two joined: the disk's bytes in front
    /// of the origin's, under the validator the head is filed by. The same
    /// narrowing `/proxy` does for a player, because it is the same call.
    #[tokio::test]
    async fn a_partly_held_range_fetches_only_what_is_missing() {
        let origin = Origin::start(true);
        let (_root, cache) = cache();
        let source = ProxySource::open(cache.clone(), SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        let chunk = crate::proxy_cache::CHUNK_BYTES as usize;

        // The head, which is one whole chunk and so is committed.
        let mut head = vec![0u8; chunk];
        source.read_at(0, &mut head).await.unwrap();
        cache.settled().await;
        let fetched = origin.asked();

        // A range starting inside it and running past its end.
        let mut both = vec![0u8; chunk + 1000];
        assert_eq!(source.read_at(0, &mut both).await.unwrap(), both.len());
        assert_eq!(
            both,
            (0..chunk + 1000).map(byte_at).collect::<Vec<_>>(),
            "the two halves were joined in the wrong order, or at the wrong byte"
        );
        assert_eq!(
            origin.asked(),
            fetched + 1,
            "the tail cost more than one request"
        );
    }

    /// **An origin that answers `200` to a ranged request is refused**, and
    /// refused at construction: reading a member out of it would mean
    /// downloading the whole archive, which is what this design exists to
    /// stop.
    #[tokio::test]
    async fn an_origin_that_will_not_range_is_refused() {
        let origin = Origin::start(false);
        let (_root, cache) = cache();
        let refused = ProxySource::open(cache, SELF_ADDR, origin.url(), BTreeMap::new()).await;
        assert!(
            matches!(refused, Err(ProxySourceError::WillNotRange)),
            "an origin that ignores Range was opened as a source"
        );
        assert!(
            ProxySourceError::WillNotRange
                .to_string()
                .contains("byte ranges"),
            "the refusal has no sentence a player could show"
        );
    }

    /// **An origin that stops ranging breaks the read**, rather than
    /// quietly reading the file from its first byte to reach the span that
    /// was asked for. It is the same refusal the construction probe makes,
    /// at the only other moment it can be found out: a `200` to a narrowed
    /// range is also what an origin answers when the `If-Range` finds the
    /// cached head stale, and neither is a tail.
    #[tokio::test]
    async fn an_origin_that_stops_ranging_fails_the_read() {
        // Ranging for the probe, and a `200` for everything after it.
        let origin = Origin::ranging(|nth| nth == 1);
        let (_root, cache) = cache();
        let source = ProxySource::open(cache, SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranged when it was asked");
        let mut buf = [0u8; 64];
        let error = source
            .read_at(1000, &mut buf)
            .await
            .expect_err("a 200 was read as a tail");
        assert!(
            error.to_string().contains("ranged read"),
            "the read failed for some other reason: {error}"
        );
    }

    /// A body read is one ranged request for the whole span the hint
    /// names, and it stops at it.
    #[tokio::test]
    async fn a_body_read_is_one_request_for_the_span_it_was_hinted() {
        let origin = Origin::start(true);
        let (_root, cache) = cache();
        let source = ProxySource::open(cache, SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        let probed = origin.asked();
        let mut reader = source.open(1000, ReadHint::of(64)).await.unwrap();
        let mut read = [0u8; 64];
        reader.read_exact(&mut read).await.unwrap();
        assert_eq!(read.to_vec(), (1000..1064).map(byte_at).collect::<Vec<_>>());
        assert_eq!(origin.asked(), probed + 1, "one request for the span");

        // **The hint is advice, not a cap.** A caller that reads on past
        // it gets the bytes after it -- one more request, for the rest --
        // rather than a short read it would have to work out for itself.
        let mut past = [0u8; 8];
        reader.read_exact(&mut past).await.unwrap();
        assert_eq!(past.to_vec(), (1064..1072).map(byte_at).collect::<Vec<_>>());
    }

    /// **A seek on a reader is one new ranged request at the new offset**,
    /// which is exactly what a player dragging the scrubber through
    /// `/proxy` already makes. The response being read is ended; nothing
    /// is read through from the old offset to the new one.
    #[tokio::test]
    async fn a_seek_ends_the_response_and_asks_for_the_new_offset() {
        let origin = Origin::start(true);
        let (_root, cache) = cache();
        let source = ProxySource::open(cache, SELF_ADDR, origin.url(), BTreeMap::new())
            .await
            .expect("an origin that ranges");
        let mut reader = source.open(0, ReadHint::of(64)).await.unwrap();
        let mut read = [0u8; 64];
        reader.read_exact(&mut read).await.unwrap();
        let after_first = origin.asked();

        // Forwards, over a span nothing has fetched: one request, and it
        // starts where the seek said.
        let far = (ORIGIN_LENGTH - 1024) as u64;
        assert_eq!(reader.seek(io::SeekFrom::Start(far)).await.unwrap(), far);
        reader.read_exact(&mut read).await.unwrap();
        assert_eq!(
            read.to_vec(),
            (far as usize..far as usize + 64)
                .map(byte_at)
                .collect::<Vec<_>>()
        );
        assert_eq!(origin.asked(), after_first + 1, "one request for the seek");

        // And backwards, which is the seek an HTTP reader could not make
        // at all before this: one request at the new offset, not a read
        // through everything between it and where the reader was.
        let back = origin.asked();
        assert_eq!(reader.seek(io::SeekFrom::Start(8)).await.unwrap(), 8);
        let mut early = [0u8; 8];
        reader.read_exact(&mut early).await.unwrap();
        assert_eq!(early.to_vec(), (8..16).map(byte_at).collect::<Vec<_>>());
        assert!(
            origin.asked() <= back + 1,
            "a seek backwards cost {} requests",
            origin.asked() - back
        );
    }
}
