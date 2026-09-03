//! Tracker *scrape* clients: what the whole swarm looks like, according to
//! the trackers, as opposed to the peers this server happens to be connected
//! to.
//!
//! A scrape is a read-only question ("how many seeders/leechers/completions
//! does this info hash have?"). Unlike an announce it carries no port, peer id
//! or event, so it cannot register us as a peer and cannot disturb the
//! announces librqbit does on its own. That is the whole reason this lives
//! here rather than in the librqbit fork: the client is expected to scrape for
//! itself if it wants swarm numbers.
//!
//! Two wire protocols:
//! * **HTTP/HTTPS** -- BEP-48, `GET <scrape url>?info_hash=<20 raw bytes>`,
//!   answered with a bencoded `files` dictionary.
//! * **UDP** -- BEP-15 action 2 on the same connect handshake the announce
//!   protocol uses. A fresh connection id is obtained per scrape (they expire
//!   after about a minute) on a socket of our own, never one librqbit holds.
//!
//! These functions are one-shot: they ask once and answer, doing no caching
//! and no rate limiting of their own.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::net::UdpSocket;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::{debug, warn};
use url::Url;

/// BEP-15 magic protocol id for the connect handshake.
const UDP_PROTOCOL_ID: u64 = 0x0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
/// BEP-15 scrape. The constant is commented out in librqbit's
/// `tracker_comms_udp.rs`; this module implements the request itself rather
/// than depending on the fork gaining it.
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

/// Whole UDP round trip (connect + scrape) budget.
pub const UDP_SCRAPE_TIMEOUT: Duration = Duration::from_secs(5);
/// Whole HTTP request budget.
pub const HTTP_SCRAPE_TIMEOUT: Duration = Duration::from_secs(8);
/// Cap on a bencoded scrape body we are willing to buffer.
const HTTP_SCRAPE_MAX_BODY: usize = 64 * 1024;

/// Swarm counters for one info hash as one tracker reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SwarmCounts {
    /// Peers with the complete torrent ("complete" on the wire).
    pub seeders: u64,
    /// Peers still downloading ("incomplete" on the wire).
    pub leechers: u64,
    /// Completed downloads this tracker has recorded ("downloaded").
    pub completed: u64,
}

/// What one scrape of one tracker produced.
///
/// The three cases must stay distinguishable all the way to the JSON:
/// "nobody is seeding" and "the tracker has never heard of this hash" and
/// "we could not ask" are three different things, and only the first is a
/// number a client may show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrapeOutcome {
    /// The tracker knows the hash and reported counters.
    Counts(SwarmCounts),
    /// The tracker answered, but does not track this hash: an empty `files`
    /// dictionary (or one without our key) over HTTP, an all-zero entry over
    /// UDP -- BEP-15 has no way to say "unknown", so a tracker that does not
    /// know the hash answers with zeroes.
    UnknownHash,
    /// No usable answer: timeout, DNS/connect error, tracker error (action 3
    /// or a `failure reason`), truncated or malformed packet.
    Failed,
}

/// Derive a tracker's scrape URL from its announce URL, per BEP-48.
///
/// The **last path segment** must begin with `announce`; that prefix is
/// replaced with `scrape` and everything else -- the leading path, any suffix
/// on the segment, and the whole query string (passkeys live there) -- is kept
/// verbatim. A tracker whose last segment does not start with `announce`
/// declares no scrape support, so this returns `None` rather than guessing.
///
/// ```text
/// /announce            -> /scrape
/// /announce.php        -> /scrape.php
/// /x/announce?pk=abc   -> /x/scrape?pk=abc
/// /ann                 -> None
/// /                    -> None
/// ```
pub fn scrape_url(announce: &Url) -> Option<Url> {
    let path = announce.path();
    let split = path.rfind('/')?;
    let (prefix, last) = path.split_at(split + 1);
    let suffix = last.strip_prefix("announce")?;
    let mut url = announce.clone();
    url.set_path(&format!("{prefix}scrape{suffix}"));
    Some(url)
}

/// Scrape one tracker, picking the protocol from its scheme. `announce` is
/// the tracker URL as the torrent carries it.
pub async fn scrape(announce: &str, info_hash: &[u8; 20]) -> ScrapeOutcome {
    let Ok(url) = Url::parse(announce) else {
        return ScrapeOutcome::Failed;
    };
    match url.scheme() {
        "udp" => scrape_udp(&url, info_hash).await,
        "http" | "https" => scrape_http(&url, info_hash).await,
        _ => ScrapeOutcome::Failed,
    }
}

/// BEP-15 UDP scrape: connect handshake, then action 2 for a single hash.
///
/// Takes the tracker's **announce** URL: the UDP protocol selects scrape with
/// an action code, so the path is not part of the request and a tracker
/// published as a bare `udp://host:port` is scrapable all the same. The socket
/// is ours alone -- librqbit's announce socket is never touched -- and the
/// connection id is fetched fresh, because trackers expire them in ~60 s.
pub async fn scrape_udp(announce: &Url, info_hash: &[u8; 20]) -> ScrapeOutcome {
    match tokio::time::timeout(UDP_SCRAPE_TIMEOUT, scrape_udp_inner(announce, info_hash)).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            debug!(tracker = %announce, %error, "UDP scrape failed");
            ScrapeOutcome::Failed
        }
        Err(_) => {
            debug!(tracker = %announce, "UDP scrape timed out");
            ScrapeOutcome::Failed
        }
    }
}

async fn scrape_udp_inner(announce: &Url, info_hash: &[u8; 20]) -> Result<ScrapeOutcome> {
    let host = announce.host_str().context("tracker URL has no host")?;
    let port = announce.port().context("UDP tracker URL has no port")?;
    let addr = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {host}:{port}"))?
        .next()
        .with_context(|| format!("{host}:{port} resolved to nothing"))?;
    let socket = connected_socket(addr).await?;

    let transaction_id = next_transaction_id();
    let mut connect = [0u8; 16];
    connect[0..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    connect[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    connect[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    socket.send(&connect).await.context("sending connect")?;

    let mut buf = [0u8; 1024];
    let n = socket.recv(&mut buf).await.context("connect response")?;
    let connection_id = parse_udp_connect_response(&buf[..n], transaction_id)?;

    let transaction_id = next_transaction_id();
    let mut request = [0u8; 36];
    request[0..8].copy_from_slice(&connection_id.to_be_bytes());
    request[8..12].copy_from_slice(&ACTION_SCRAPE.to_be_bytes());
    request[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    request[16..36].copy_from_slice(info_hash);
    socket.send(&request).await.context("sending scrape")?;

    let n = socket.recv(&mut buf).await.context("scrape response")?;
    parse_udp_scrape_response(&buf[..n], transaction_id)
}

/// Bind a socket of the target's address family and connect it, so an IPv6-only
/// tracker is reachable too (a v4 socket cannot send to a v6 address).
async fn connected_socket(addr: SocketAddr) -> Result<UdpSocket> {
    let bind: SocketAddr = if addr.is_ipv6() {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let socket = UdpSocket::bind(bind).await.context("binding UDP socket")?;
    socket
        .connect(addr)
        .await
        .context("connecting UDP socket")?;
    Ok(socket)
}

fn be_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]])
}

fn parse_udp_connect_response(buf: &[u8], transaction_id: u32) -> Result<u64> {
    if buf.len() < 8 {
        bail!("connect response truncated ({} bytes)", buf.len());
    }
    let action = be_u32(buf, 0);
    if be_u32(buf, 4) != transaction_id {
        bail!("connect response has the wrong transaction id");
    }
    match action {
        ACTION_CONNECT => {
            if buf.len() < 16 {
                bail!("connect response truncated ({} bytes)", buf.len());
            }
            Ok(u64::from_be_bytes(buf[8..16].try_into().unwrap()))
        }
        ACTION_ERROR => bail!("tracker error: {}", error_message(buf)),
        other => bail!("unexpected action {other} in connect response"),
    }
}

/// Parse a BEP-15 scrape response for a single-hash request.
///
/// Layout: `action` (2), `transaction_id`, then one 12-byte entry per hash --
/// `complete`, `downloaded`, `incomplete`. An all-zero entry is how a tracker
/// says it does not know the hash, which is [`ScrapeOutcome::UnknownHash`] and
/// not a swarm of size zero.
fn parse_udp_scrape_response(buf: &[u8], transaction_id: u32) -> Result<ScrapeOutcome> {
    if buf.len() < 8 {
        bail!("scrape response truncated ({} bytes)", buf.len());
    }
    let action = be_u32(buf, 0);
    if be_u32(buf, 4) != transaction_id {
        bail!("scrape response has the wrong transaction id");
    }
    match action {
        ACTION_SCRAPE => {
            if buf.len() < 20 {
                bail!("scrape response truncated ({} bytes)", buf.len());
            }
            let counts = SwarmCounts {
                seeders: be_u32(buf, 8) as u64,
                completed: be_u32(buf, 12) as u64,
                leechers: be_u32(buf, 16) as u64,
            };
            if counts == SwarmCounts::default() {
                Ok(ScrapeOutcome::UnknownHash)
            } else {
                Ok(ScrapeOutcome::Counts(counts))
            }
        }
        ACTION_ERROR => bail!("tracker error: {}", error_message(buf)),
        other => bail!("unexpected action {other} in scrape response"),
    }
}

fn error_message(buf: &[u8]) -> String {
    String::from_utf8_lossy(buf.get(8..).unwrap_or_default()).into_owned()
}

/// Transaction ids only have to be unpredictable enough that a stray datagram
/// from a previous request is not mistaken for this one's answer; a counter
/// seeded from the clock does that without pulling in an RNG crate.
fn next_transaction_id() -> u32 {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    static SEED: OnceLock<u32> = OnceLock::new();
    let seed = *SEED.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() ^ (d.as_secs() as u32))
            .unwrap_or(0x5eed_1234)
    });
    seed.wrapping_add(
        COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(2_654_435_761),
    )
}

fn http_client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<Option<reqwest::Client>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(HTTP_SCRAPE_TIMEOUT)
                .build()
                .ok()
        })
        .as_ref()
}

/// BEP-48 HTTP(S) scrape of a single info hash.
///
/// Takes the **announce** URL and derives the scrape URL from it; a tracker
/// whose announce path does not support the derivation is
/// [`ScrapeOutcome::Failed`] without a request being made.
pub async fn scrape_http(announce: &Url, info_hash: &[u8; 20]) -> ScrapeOutcome {
    let Some(url) = scrape_request_url(announce, info_hash) else {
        debug!(tracker = %announce, "tracker announce URL has no BEP-48 scrape URL");
        return ScrapeOutcome::Failed;
    };
    match scrape_http_inner(&url, info_hash).await {
        Ok(outcome) => outcome,
        Err(error) => {
            debug!(tracker = %announce, %error, "HTTP scrape failed");
            ScrapeOutcome::Failed
        }
    }
}

/// The full scrape request URL: BEP-48 path with `info_hash=` appended to
/// whatever query the announce URL already carried (passkeys survive).
fn scrape_request_url(announce: &Url, info_hash: &[u8; 20]) -> Option<Url> {
    let mut url = scrape_url(announce)?;
    let encoded = urlencoding::encode_binary(info_hash);
    let query = match url.query() {
        Some(existing) if !existing.is_empty() => format!("{existing}&info_hash={encoded}"),
        _ => format!("info_hash={encoded}"),
    };
    url.set_query(Some(&query));
    Some(url)
}

async fn scrape_http_inner(url: &Url, info_hash: &[u8; 20]) -> Result<ScrapeOutcome> {
    let client = http_client().context("no HTTP client")?;
    let response = client
        .get(url.clone())
        .send()
        .await
        .context("sending scrape request")?;
    let status = response.status();
    if !status.is_success() {
        bail!("tracker answered {status}");
    }
    let body = response.bytes().await.context("reading scrape response")?;
    if body.len() > HTTP_SCRAPE_MAX_BODY {
        bail!("scrape response too large ({} bytes)", body.len());
    }
    parse_http_scrape_response(&body, info_hash)
}

/// Parse a bencoded BEP-48 scrape body for one info hash.
///
/// `{"files": {}}` -- or a `files` dictionary without our key -- is
/// [`ScrapeOutcome::UnknownHash`]: the tracker answered and does not track the
/// hash. A `failure reason` is an error, not a count.
fn parse_http_scrape_response(body: &[u8], info_hash: &[u8; 20]) -> Result<ScrapeOutcome> {
    use serde_bencode::value::Value;

    let value: Value = serde_bencode::from_bytes(body).context("decoding bencode")?;
    let Value::Dict(root) = value else {
        bail!("scrape response is not a bencoded dictionary");
    };
    if let Some(Value::Bytes(reason)) = root.get(b"failure reason".as_slice()) {
        bail!("tracker failure: {}", String::from_utf8_lossy(reason));
    }
    let Some(Value::Dict(files)) = root.get(b"files".as_slice()) else {
        bail!("scrape response has no `files` dictionary");
    };
    let Some(Value::Dict(entry)) = files.get(info_hash.as_slice()) else {
        return Ok(ScrapeOutcome::UnknownHash);
    };
    let counter = |key: &[u8]| match entry.get(key) {
        Some(Value::Int(n)) => (*n).max(0) as u64,
        _ => 0,
    };
    Ok(ScrapeOutcome::Counts(SwarmCounts {
        seeders: counter(b"complete"),
        leechers: counter(b"incomplete"),
        completed: counter(b"downloaded"),
    }))
}

// ---------------------------------------------------------------------------
// Caching, rate limiting and aggregation
// ---------------------------------------------------------------------------

/// Minimum wait between two scrapes of the same (tracker, info hash) once the
/// tracker has answered. Scrape is cheap for a tracker but it is still an
/// unsolicited request; a quarter of an hour is far more often than swarm
/// numbers meaningfully move.
pub const MIN_SCRAPE_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// First retry delay after a failed scrape; doubles per consecutive failure.
const BACKOFF_BASE: Duration = Duration::from_secs(60);
/// Ceiling on the failure backoff.
const BACKOFF_CAP: Duration = Duration::from_secs(30 * 60);
/// How long a successful scrape stays presentable. After this the numbers are
/// dropped rather than shown as if current: a client would rather see "we do
/// not know" than an hour-old count labelled as now.
pub const SCRAPE_STALE_AFTER: Duration = Duration::from_secs(60 * 60);
/// Tracker scrapes in flight at once, across every torrent.
const MAX_CONCURRENT_SCRAPES: usize = 4;
/// A swarm this large is a tracker bug or a misparse, not a swarm. The value
/// is logged and left out of the aggregate rather than shown.
const IMPLAUSIBLE_COUNT: u64 = 100_000;
/// Cached torrents untouched for this long are forgotten (a torrent nobody
/// polls has no stats to fill in).
const CACHE_RETENTION: Duration = SCRAPE_STALE_AFTER;

/// Swarm figures for one torrent, as of the last scrape round.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwarmSnapshot {
    /// Seeders across the swarm, `None` when no tracker gave us a usable
    /// number. Never a stand-in zero.
    pub seeders: Option<u64>,
    /// Leechers across the swarm, `None` when unknown.
    pub leechers: Option<u64>,
    /// Age of the freshest scrape the figures rest on, in seconds.
    pub age_secs: Option<u64>,
    /// Per-tracker counters, keyed by the tracker's announce URL. Only
    /// trackers that answered with counts appear.
    pub per_tracker: HashMap<String, SwarmCounts>,
}

/// How [`SwarmScraper`] reaches trackers. The network implementation is the
/// only one that ships; tests substitute their own so the suite never opens a
/// socket and can run on a paused clock.
#[async_trait::async_trait]
pub trait ScrapeTransport: Send + Sync + 'static {
    async fn scrape(&self, tracker: &str, info_hash: [u8; 20]) -> ScrapeOutcome;
}

struct NetworkTransport;

#[async_trait::async_trait]
impl ScrapeTransport for NetworkTransport {
    async fn scrape(&self, tracker: &str, info_hash: [u8; 20]) -> ScrapeOutcome {
        scrape(tracker, &info_hash).await
    }
}

/// What we know about one (tracker, info hash) pair.
#[derive(Debug, Clone, Default)]
struct TrackerScrape {
    /// The last counts this tracker gave, and when.
    counts: Option<(Instant, SwarmCounts)>,
    /// Consecutive failures, driving the backoff.
    failures: u32,
    /// Earliest next attempt; `None` means "never asked, ask now".
    next_attempt: Option<Instant>,
}

#[derive(Debug, Default)]
struct TorrentScrape {
    trackers: HashMap<String, TrackerScrape>,
    /// A refresh round is already running for this torrent.
    refreshing: bool,
    /// Last time stats were asked for; drives cache pruning.
    last_seen: Option<Instant>,
}

/// Swarm-wide seeder/leecher counts from tracker scrapes, cached per info hash.
///
/// [`SwarmScraper::snapshot`] is what the stats path calls, and it **never
/// awaits the network**: players poll `stats.json` about once a second, so it
/// reads the cached snapshot and at most schedules a refresh in the
/// background.
pub struct SwarmScraper {
    /// `None` disables scraping entirely (hermetic test sessions).
    transport: Option<Arc<dyn ScrapeTransport>>,
    cache: Mutex<HashMap<String, TorrentScrape>>,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for SwarmScraper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwarmScraper")
            .field("enabled", &self.transport.is_some())
            .field("torrents", &self.cache.lock().len())
            .finish()
    }
}

impl SwarmScraper {
    /// A scraper that talks to real trackers.
    pub fn network() -> Arc<Self> {
        Self::with_transport(Arc::new(NetworkTransport))
    }

    /// A scraper that never scrapes and always reports "unknown". Used by
    /// hermetic sessions, which must not touch the network at all.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            transport: None,
            cache: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SCRAPES)),
        })
    }

    pub fn with_transport(transport: Arc<dyn ScrapeTransport>) -> Arc<Self> {
        Arc::new(Self {
            transport: Some(transport),
            cache: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SCRAPES)),
        })
    }

    /// The cached swarm figures for `info_hash`, scheduling a refresh of any
    /// tracker that is due. Returns immediately -- no network, no lock held
    /// across an await.
    ///
    /// `private` torrents are skipped outright: an unsolicited scrape is
    /// exactly the kind of request private trackers ban accounts over, and
    /// their announce URLs carry the passkey that would identify us.
    pub fn snapshot(
        self: &Arc<Self>,
        info_hash_hex: &str,
        info_hash: [u8; 20],
        trackers: &[String],
        private: bool,
    ) -> SwarmSnapshot {
        if self.transport.is_none() || private || trackers.is_empty() {
            return SwarmSnapshot::default();
        }
        let now = Instant::now();
        let mut due = Vec::new();
        let snapshot = {
            let mut cache = self.cache.lock();
            cache.retain(|_, torrent| {
                torrent
                    .last_seen
                    .is_none_or(|seen| now.saturating_duration_since(seen) < CACHE_RETENTION)
            });
            let torrent = cache.entry(info_hash_hex.to_string()).or_default();
            torrent.last_seen = Some(now);
            // Trackers can only be set when the engine is created, but a
            // restart or a different creating request can change the list;
            // never keep counters for a tracker this torrent no longer uses.
            torrent.trackers.retain(|url, _| trackers.contains(url));
            for tracker in trackers {
                let entry = torrent.trackers.entry(tracker.clone()).or_default();
                if entry.next_attempt.is_none_or(|at| at <= now) {
                    due.push(tracker.clone());
                }
            }
            let snapshot = aggregate(&torrent.trackers, now);
            if !due.is_empty() && !torrent.refreshing {
                torrent.refreshing = true;
            } else {
                due.clear();
            }
            snapshot
        };
        if !due.is_empty() {
            self.spawn_round(info_hash_hex.to_string(), info_hash, due);
        }
        snapshot
    }

    fn spawn_round(self: &Arc<Self>, info_hash_hex: String, info_hash: [u8; 20], due: Vec<String>) {
        // `snapshot` is reachable from a library caller with no reactor; a
        // scrape needs one, so without a runtime we simply do not refresh.
        if tokio::runtime::Handle::try_current().is_err() {
            self.finish_round(&info_hash_hex);
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let Some(transport) = this.transport.clone() else {
                this.finish_round(&info_hash_hex);
                return;
            };
            let permits = this.permits.clone();
            futures::stream::iter(due)
                .for_each_concurrent(None, |tracker| {
                    let this = this.clone();
                    let transport = transport.clone();
                    let permits = permits.clone();
                    let info_hash_hex = info_hash_hex.clone();
                    async move {
                        let _permit = match permits.acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => return,
                        };
                        let outcome = transport.scrape(&tracker, info_hash).await;
                        this.record(&info_hash_hex, &tracker, outcome);
                    }
                })
                .await;
            this.finish_round(&info_hash_hex);
        });
    }

    fn finish_round(&self, info_hash_hex: &str) {
        if let Some(torrent) = self.cache.lock().get_mut(info_hash_hex) {
            torrent.refreshing = false;
        }
    }

    fn record(&self, info_hash_hex: &str, tracker: &str, outcome: ScrapeOutcome) {
        let now = Instant::now();
        let mut cache = self.cache.lock();
        let Some(torrent) = cache.get_mut(info_hash_hex) else {
            return;
        };
        let entry = torrent.trackers.entry(tracker.to_string()).or_default();
        match outcome {
            ScrapeOutcome::Counts(counts) => {
                entry.counts = Some((now, counts));
                entry.failures = 0;
                entry.next_attempt = Some(now + MIN_SCRAPE_INTERVAL);
            }
            ScrapeOutcome::UnknownHash => {
                // The tracker answered and does not track this hash. Drop any
                // counts it gave earlier -- it has dropped the torrent -- but
                // treat the answer as a success for rate-limiting purposes.
                entry.counts = None;
                entry.failures = 0;
                entry.next_attempt = Some(now + MIN_SCRAPE_INTERVAL);
            }
            ScrapeOutcome::Failed => {
                entry.failures = entry.failures.saturating_add(1);
                entry.next_attempt = Some(now + backoff(entry.failures));
            }
        }
    }
}

/// Retry delay after `failures` consecutive failed scrapes: 60 s doubling to a
/// 30 min ceiling.
fn backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    BACKOFF_CAP.min(BACKOFF_BASE.saturating_mul(1u32 << shift))
}

/// Fold the per-tracker counters into one figure per counter.
///
/// **`max`, never `sum`.** Each tracker only ever sees the peers that
/// registered with *it*, so no tracker's number is a share of a total; and
/// several trackers in the shipped list share a backend and answer with
/// byte-identical counts, so summing over-counts the same swarm several times
/// over. The largest number any single tracker can vouch for is the honest
/// floor, and seeders and leechers are maxed independently because a tracker
/// can be authoritative about one and stale about the other.
///
/// Only [`ScrapeOutcome::Counts`] entries that are fresher than
/// [`SCRAPE_STALE_AFTER`] take part: an unknown hash or a failed scrape
/// contributes nothing at all, rather than folding in as a zero that would
/// drag nothing down but would make "no tracker answered" look like an empty
/// swarm.
fn aggregate(trackers: &HashMap<String, TrackerScrape>, now: Instant) -> SwarmSnapshot {
    let mut snapshot = SwarmSnapshot::default();
    let mut freshest: Option<Duration> = None;
    for (url, entry) in trackers {
        let Some((at, counts)) = entry.counts else {
            continue;
        };
        let age = now.saturating_duration_since(at);
        if age >= SCRAPE_STALE_AFTER {
            continue;
        }
        snapshot.per_tracker.insert(url.clone(), counts);
        merge_max(&mut snapshot.seeders, counts.seeders, url, "seeders");
        merge_max(&mut snapshot.leechers, counts.leechers, url, "leechers");
        freshest = Some(freshest.map_or(age, |best| best.min(age)));
    }
    if snapshot.seeders.is_some() || snapshot.leechers.is_some() {
        snapshot.age_secs = freshest.map(|age| age.as_secs());
    }
    snapshot
}

fn merge_max(slot: &mut Option<u64>, value: u64, tracker: &str, what: &str) {
    if value > IMPLAUSIBLE_COUNT {
        warn!(%tracker, %what, value, "ignoring implausible tracker scrape count");
        return;
    }
    *slot = Some(slot.map_or(value, |current: u64| current.max(value)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(url: &str) -> Option<String> {
        scrape_url(&Url::parse(url).unwrap()).map(|u| u.to_string())
    }

    /// BEP-48 rewriting: only the last path segment is touched, only when it
    /// starts with `announce`, and the query (where passkeys live) survives.
    #[test]
    fn scrape_url_follows_bep48() {
        for (announce, expected) in [
            ("http://t.example/announce", Some("http://t.example/scrape")),
            (
                "https://t.example/announce.php",
                Some("https://t.example/scrape.php"),
            ),
            (
                "https://t.example/x/announce?passkey=abc",
                Some("https://t.example/x/scrape?passkey=abc"),
            ),
            (
                "http://t.example/announce-ext?a=1&b=2",
                Some("http://t.example/scrape-ext?a=1&b=2"),
            ),
            (
                "udp://t.example:1337/announce",
                Some("udp://t.example:1337/scrape"),
            ),
            // Not announce-prefixed: the tracker declares no scrape support.
            ("http://t.example/ann", None),
            ("http://t.example/", None),
            ("http://t.example/x/announce/deep", None),
            ("http://t.example/Announce", None),
        ] {
            assert_eq!(
                parse(announce).as_deref(),
                expected,
                "scrape_url({announce})"
            );
        }
    }

    /// A passkey in the path (not the query) must survive too, and the raw
    /// info hash is percent-encoded byte for byte onto the query.
    #[test]
    fn scrape_request_url_appends_the_info_hash() {
        let hash = [0x12u8; 20];
        let url = scrape_request_url(
            &Url::parse("https://t.example/abc123/announce").unwrap(),
            &hash,
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://t.example/abc123/scrape?info_hash=%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12%12"
        );

        let url = scrape_request_url(
            &Url::parse("https://t.example/announce?passkey=abc").unwrap(),
            &hash,
        )
        .unwrap();
        assert!(
            url.as_str()
                .starts_with("https://t.example/scrape?passkey=abc&info_hash=%12"),
            "passkey must survive: {url}"
        );
    }

    /// Golden bytes captured off the wire shape BEP-15 specifies:
    /// action 2, transaction id, then complete/downloaded/incomplete.
    #[test]
    fn udp_scrape_response_parses_against_the_golden_fixture() {
        let bytes = include_bytes!("../resources/test/udp-tracker-scrape-response.bin");
        assert_eq!(
            parse_udp_scrape_response(bytes, 0x1122_3344).unwrap(),
            ScrapeOutcome::Counts(SwarmCounts {
                seeders: 1234,
                leechers: 42,
                completed: 56789,
            })
        );
        // A response for somebody else's request is not ours to believe.
        assert!(parse_udp_scrape_response(bytes, 0xdead_beef).is_err());
    }

    /// The three UDP non-answers: an all-zero entry (BEP-15's only way of
    /// saying "unknown hash"), an action-3 error, and a short packet.
    #[test]
    fn udp_scrape_response_rejects_errors_and_truncation() {
        let mut zeroed = [0u8; 20];
        zeroed[0..4].copy_from_slice(&ACTION_SCRAPE.to_be_bytes());
        zeroed[4..8].copy_from_slice(&7u32.to_be_bytes());
        assert_eq!(
            parse_udp_scrape_response(&zeroed, 7).unwrap(),
            ScrapeOutcome::UnknownHash
        );

        let mut error = Vec::new();
        error.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        error.extend_from_slice(&7u32.to_be_bytes());
        error.extend_from_slice(b"nope");
        let err = parse_udp_scrape_response(&error, 7)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "error text must reach the log: {err}");

        // Header only, no entry.
        assert!(parse_udp_scrape_response(&zeroed[..8], 7).is_err());
        // Not even a header.
        assert!(parse_udp_scrape_response(&zeroed[..4], 7).is_err());
        assert!(parse_udp_scrape_response(&[], 7).is_err());
    }

    #[test]
    fn udp_connect_response_parses_and_rejects() {
        let mut ok = [0u8; 16];
        ok[4..8].copy_from_slice(&9u32.to_be_bytes());
        ok[8..16].copy_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(
            parse_udp_connect_response(&ok, 9).unwrap(),
            0x0102_0304_0506_0708
        );
        assert!(parse_udp_connect_response(&ok, 10).is_err());
        assert!(parse_udp_connect_response(&ok[..12], 9).is_err());
    }

    fn hash_of(byte: u8) -> [u8; 20] {
        [byte; 20]
    }

    /// Bencode bodies as trackers actually send them.
    #[test]
    fn http_scrape_response_parses_counts() {
        let hash = hash_of(0xab);
        let mut body = Vec::new();
        body.extend_from_slice(b"d5:filesd20:");
        body.extend_from_slice(&hash);
        body.extend_from_slice(b"d8:completei17e10:downloadedi250e10:incompletei4eeee");
        assert_eq!(
            parse_http_scrape_response(&body, &hash).unwrap(),
            ScrapeOutcome::Counts(SwarmCounts {
                seeders: 17,
                leechers: 4,
                completed: 250,
            })
        );
    }

    /// An empty `files` dict is the tracker saying "never heard of it" -- it
    /// must never turn into a swarm of zero seeders.
    #[test]
    fn http_scrape_response_empty_files_is_unknown_hash() {
        let hash = hash_of(0xcd);
        assert_eq!(
            parse_http_scrape_response(b"d5:filesdeee", &hash).unwrap(),
            ScrapeOutcome::UnknownHash
        );
        // A dict that answers about some *other* hash is equally unknown.
        let mut other = Vec::new();
        other.extend_from_slice(b"d5:filesd20:");
        other.extend_from_slice(&hash_of(0x01));
        other.extend_from_slice(b"d8:completei9eeee");
        assert_eq!(
            parse_http_scrape_response(&other, &hash).unwrap(),
            ScrapeOutcome::UnknownHash
        );
    }

    #[test]
    fn http_scrape_response_rejects_failures_and_garbage() {
        let hash = hash_of(0xef);
        let err = parse_http_scrape_response(b"d14:failure reason9:go awayeee", &hash);
        assert!(err.is_err(), "a failure reason is not a count");
        assert!(parse_http_scrape_response(b"d3:fooi1ee", &hash).is_err());
        assert!(parse_http_scrape_response(b"not bencode", &hash).is_err());
    }

    /// End to end over loopback with no network: a stub tracker that answers
    /// the connect handshake and then the scrape.
    #[tokio::test]
    async fn udp_scrape_round_trips_against_a_loopback_tracker() {
        let counts = SwarmCounts {
            seeders: 11,
            leechers: 5,
            completed: 900,
        };
        let (url, _task) = stub_tracker(StubBehaviour::Counts(counts)).await;
        assert_eq!(
            scrape_udp(&url, &hash_of(0x42)).await,
            ScrapeOutcome::Counts(counts)
        );
    }

    #[tokio::test]
    async fn udp_scrape_maps_tracker_errors_and_truncation_to_failed() {
        let (url, _task) = stub_tracker(StubBehaviour::Error).await;
        assert_eq!(scrape_udp(&url, &hash_of(1)).await, ScrapeOutcome::Failed);

        let (url, _task) = stub_tracker(StubBehaviour::Truncated).await;
        assert_eq!(scrape_udp(&url, &hash_of(1)).await, ScrapeOutcome::Failed);

        let (url, _task) = stub_tracker(StubBehaviour::Unknown).await;
        assert_eq!(
            scrape_udp(&url, &hash_of(1)).await,
            ScrapeOutcome::UnknownHash
        );
    }

    enum StubBehaviour {
        Counts(SwarmCounts),
        Unknown,
        /// Answer the scrape with action 3 and a message.
        Error,
        /// Answer the scrape with a header and no entry.
        Truncated,
    }

    /// A UDP tracker on 127.0.0.1:0 that speaks just enough BEP-15. Returns
    /// its announce URL and the task handle (dropping it stops the stub).
    async fn stub_tracker(behaviour: StubBehaviour) -> (Url, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind stub tracker");
        let addr = socket.local_addr().expect("stub addr");
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 128];
            loop {
                let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                    return;
                };
                if n < 16 {
                    continue;
                }
                let action = be_u32(&buf, 8);
                let transaction_id = be_u32(&buf, 12);
                let reply: Vec<u8> = if action == ACTION_CONNECT {
                    let mut reply = Vec::new();
                    reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                    reply.extend_from_slice(&transaction_id.to_be_bytes());
                    reply.extend_from_slice(&0x0bad_c0de_dead_beefu64.to_be_bytes());
                    reply
                } else {
                    let mut reply = Vec::new();
                    match behaviour {
                        StubBehaviour::Error => {
                            reply.extend_from_slice(&ACTION_ERROR.to_be_bytes());
                            reply.extend_from_slice(&transaction_id.to_be_bytes());
                            reply.extend_from_slice(b"stub says no");
                        }
                        StubBehaviour::Truncated => {
                            reply.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
                            reply.extend_from_slice(&transaction_id.to_be_bytes());
                        }
                        StubBehaviour::Unknown => {
                            reply.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
                            reply.extend_from_slice(&transaction_id.to_be_bytes());
                            reply.extend_from_slice(&[0u8; 12]);
                        }
                        StubBehaviour::Counts(counts) => {
                            reply.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
                            reply.extend_from_slice(&transaction_id.to_be_bytes());
                            reply.extend_from_slice(&(counts.seeders as u32).to_be_bytes());
                            reply.extend_from_slice(&(counts.completed as u32).to_be_bytes());
                            reply.extend_from_slice(&(counts.leechers as u32).to_be_bytes());
                        }
                    }
                    reply
                };
                if socket.send_to(&reply, from).await.is_err() {
                    return;
                }
            }
        });
        let url = Url::parse(&format!("udp://127.0.0.1:{}/announce", addr.port())).unwrap();
        (url, task)
    }

    // -- caching, rate limiting and aggregation ----------------------------

    const HASH_HEX: &str = "481b6e3617be4c88f96cb25e47c9d8272130071e";

    fn counts(seeders: u64, leechers: u64) -> SwarmCounts {
        SwarmCounts {
            seeders,
            leechers,
            completed: 0,
        }
    }

    /// Build a tracker map whose entries are `(url, age, counts)`; an entry
    /// with no counts stands for a tracker that failed or answered
    /// `UnknownHash`, which are recorded the same way here -- no numbers.
    fn tracker_map(
        now: Instant,
        entries: &[(&str, Duration, Option<SwarmCounts>)],
    ) -> HashMap<String, TrackerScrape> {
        entries
            .iter()
            .map(|(url, age, counts)| {
                (
                    (*url).to_string(),
                    TrackerScrape {
                        counts: counts.map(|c| (now - *age, c)),
                        failures: 0,
                        next_attempt: None,
                    },
                )
            })
            .collect()
    }

    /// Trackers disagree, and the aggregate takes the biggest number any one
    /// of them vouches for -- per counter, and never the sum.
    #[tokio::test(start_paused = true)]
    async fn aggregate_takes_the_max_per_counter() {
        let now = Instant::now();
        let map = tracker_map(
            now,
            &[
                (
                    "udp://a/announce",
                    Duration::from_secs(30),
                    Some(counts(12, 3)),
                ),
                (
                    "udp://b/announce",
                    Duration::from_secs(10),
                    Some(counts(9, 40)),
                ),
            ],
        );
        let snapshot = aggregate(&map, now);
        assert_eq!(snapshot.seeders, Some(12));
        assert_eq!(snapshot.leechers, Some(40));
        // Freshest contributing scrape, not the oldest.
        assert_eq!(snapshot.age_secs, Some(10));
        assert_eq!(snapshot.per_tracker.len(), 2);
    }

    /// Several trackers in the shipped list share a backend and answer with
    /// identical numbers; summing would report the same swarm three times.
    #[tokio::test(start_paused = true)]
    async fn aggregate_does_not_sum_shared_backend_duplicates() {
        let now = Instant::now();
        let map = tracker_map(
            now,
            &[
                ("udp://a/announce", Duration::ZERO, Some(counts(50, 7))),
                ("udp://b/announce", Duration::ZERO, Some(counts(50, 7))),
                ("udp://c/announce", Duration::ZERO, Some(counts(50, 7))),
            ],
        );
        let snapshot = aggregate(&map, now);
        assert_eq!(snapshot.seeders, Some(50), "three copies of one swarm");
        assert_eq!(snapshot.leechers, Some(7));
    }

    /// Trackers with nothing to say are left out, not folded in as zero; when
    /// every tracker is like that the answer is "unknown", never an empty
    /// swarm.
    #[tokio::test(start_paused = true)]
    async fn aggregate_excludes_unknown_and_failed_trackers() {
        let now = Instant::now();
        let mixed = tracker_map(
            now,
            &[
                ("udp://a/announce", Duration::ZERO, None),
                ("udp://b/announce", Duration::ZERO, Some(counts(4, 6))),
                ("udp://c/announce", Duration::ZERO, None),
            ],
        );
        let snapshot = aggregate(&mixed, now);
        assert_eq!(snapshot.seeders, Some(4), "only the tracker that answered");
        assert_eq!(snapshot.leechers, Some(6));
        assert_eq!(snapshot.per_tracker.len(), 1);

        let all_failed = tracker_map(
            now,
            &[
                ("udp://a/announce", Duration::ZERO, None),
                ("udp://b/announce", Duration::ZERO, None),
            ],
        );
        let snapshot = aggregate(&all_failed, now);
        assert_eq!(snapshot.seeders, None);
        assert_eq!(snapshot.leechers, None);
        assert_eq!(snapshot.age_secs, None);

        // A real zero-seeder swarm still reports 0 -- that is a fact, not an
        // absence.
        let seedless = tracker_map(
            now,
            &[("udp://a/announce", Duration::ZERO, Some(counts(0, 3)))],
        );
        assert_eq!(aggregate(&seedless, now).seeders, Some(0));
    }

    /// An absurd count is a tracker bug; it is logged and left out instead of
    /// being shown.
    #[tokio::test(start_paused = true)]
    async fn aggregate_drops_implausible_counts() {
        let now = Instant::now();
        let map = tracker_map(
            now,
            &[
                (
                    "udp://a/announce",
                    Duration::ZERO,
                    Some(counts(u64::MAX, 2)),
                ),
                ("udp://b/announce", Duration::ZERO, Some(counts(6, 1))),
            ],
        );
        let snapshot = aggregate(&map, now);
        assert_eq!(snapshot.seeders, Some(6));
        assert_eq!(snapshot.leechers, Some(2));

        let only_absurd = tracker_map(
            now,
            &[(
                "udp://a/announce",
                Duration::ZERO,
                Some(counts(IMPLAUSIBLE_COUNT + 1, 0)),
            )],
        );
        assert_eq!(aggregate(&only_absurd, now).seeders, None);
    }

    #[derive(Default)]
    struct FakeTracker {
        calls: std::sync::atomic::AtomicUsize,
        outcome: Mutex<Option<ScrapeOutcome>>,
    }

    impl FakeTracker {
        fn answering(outcome: ScrapeOutcome) -> Arc<Self> {
            Arc::new(Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                outcome: Mutex::new(Some(outcome)),
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
        fn answer_with(&self, outcome: ScrapeOutcome) {
            *self.outcome.lock() = Some(outcome);
        }
    }

    #[async_trait::async_trait]
    impl ScrapeTransport for FakeTracker {
        async fn scrape(&self, _tracker: &str, _info_hash: [u8; 20]) -> ScrapeOutcome {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.outcome.lock().unwrap_or(ScrapeOutcome::Failed)
        }
    }

    /// Let the scheduled refresh round run. Under a paused clock the sleep
    /// only advances once every task is idle, so this is a synchronisation
    /// point, not a timing assumption.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    fn poll(scraper: &Arc<SwarmScraper>, trackers: &[String]) -> SwarmSnapshot {
        scraper.snapshot(HASH_HEX, [0x11; 20], trackers, false)
    }

    fn one_tracker() -> Vec<String> {
        vec!["udp://a.example:1337/announce".to_string()]
    }

    /// A poll every second must not become a scrape every second: one round,
    /// then nothing until the interval has passed.
    #[tokio::test(start_paused = true)]
    async fn scrapes_are_rate_limited_to_the_minimum_interval() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();

        // The first poll can only schedule: it never awaits the network.
        assert_eq!(poll(&scraper, &trackers), SwarmSnapshot::default());
        settle().await;
        assert_eq!(transport.calls(), 1);
        let snapshot = poll(&scraper, &trackers);
        assert_eq!(snapshot.seeders, Some(10));
        assert_eq!(snapshot.leechers, Some(2));

        // Polling hard inside the interval scrapes nothing more.
        for _ in 0..5 {
            poll(&scraper, &trackers);
            settle().await;
        }
        tokio::time::advance(MIN_SCRAPE_INTERVAL - Duration::from_secs(60)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 1, "still inside the interval");

        tokio::time::advance(Duration::from_secs(120)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 2, "interval elapsed");
    }

    /// A tracker that cannot be reached is retried on an exponential backoff,
    /// not on every poll.
    #[tokio::test(start_paused = true)]
    async fn failed_scrapes_back_off_exponentially() {
        let transport = FakeTracker::answering(ScrapeOutcome::Failed);
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();

        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 1);

        tokio::time::advance(Duration::from_secs(30)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 1, "inside the 60 s first backoff");

        tokio::time::advance(Duration::from_secs(40)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 2, "first backoff elapsed");

        tokio::time::advance(Duration::from_secs(90)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 2, "second backoff is 120 s, not 60");

        tokio::time::advance(Duration::from_secs(60)).await;
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(transport.calls(), 3);

        assert_eq!(backoff(1), Duration::from_secs(60));
        assert_eq!(backoff(2), Duration::from_secs(120));
        assert_eq!(backoff(20), BACKOFF_CAP, "capped at 30 min");
    }

    /// Numbers age out: an hour after the last successful scrape they are
    /// dropped rather than presented as current.
    #[tokio::test(start_paused = true)]
    async fn counts_go_unknown_after_an_hour_without_a_successful_scrape() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();

        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(poll(&scraper, &trackers).seeders, Some(10));

        // Every scrape from here on fails, and the torrent stays polled the
        // whole time, so nothing but the age ceiling can drop the numbers.
        transport.answer_with(ScrapeOutcome::Failed);
        let step = Duration::from_secs(10 * 60);
        for _ in 0..3 {
            tokio::time::advance(step).await;
            poll(&scraper, &trackers);
            settle().await;
        }
        let snapshot = poll(&scraper, &trackers);
        assert_eq!(snapshot.seeders, Some(10), "half an hour old, still shown");
        assert!(
            snapshot
                .age_secs
                .is_some_and(|age| (1800..=1810).contains(&age)),
            "age must say how old: {:?}",
            snapshot.age_secs
        );

        for _ in 0..4 {
            tokio::time::advance(step).await;
            poll(&scraper, &trackers);
            settle().await;
        }
        let snapshot = poll(&scraper, &trackers);
        assert_eq!(snapshot.seeders, None, "stale numbers are not current ones");
        assert_eq!(snapshot.age_secs, None);
    }

    /// `UnknownHash` is an answer, not a failure: it clears any counts the
    /// tracker gave before and is rate limited like a success.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_hash_clears_the_counts_it_replaces() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();
        poll(&scraper, &trackers);
        settle().await;
        assert_eq!(poll(&scraper, &trackers).seeders, Some(10));

        transport.answer_with(ScrapeOutcome::UnknownHash);
        tokio::time::advance(MIN_SCRAPE_INTERVAL + Duration::from_secs(1)).await;
        poll(&scraper, &trackers);
        settle().await;
        let snapshot = poll(&scraper, &trackers);
        assert_eq!(snapshot.seeders, None);
        assert!(snapshot.per_tracker.is_empty());
        assert_eq!(transport.calls(), 2);
    }

    /// Private torrents are never scraped: an unsolicited request can breach
    /// a private tracker's rules, and its announce URL carries our passkey.
    #[tokio::test(start_paused = true)]
    async fn private_torrents_are_never_scraped() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();

        for _ in 0..3 {
            assert_eq!(
                scraper.snapshot(HASH_HEX, [0x11; 20], &trackers, true),
                SwarmSnapshot::default()
            );
            settle().await;
        }
        assert_eq!(transport.calls(), 0);
    }

    /// A DHT-only torrent has nothing to scrape, and a disabled scraper never
    /// touches the network at all.
    #[tokio::test(start_paused = true)]
    async fn nothing_to_scrape_without_trackers_or_a_transport() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        assert_eq!(poll(&scraper, &[]), SwarmSnapshot::default());
        settle().await;
        assert_eq!(transport.calls(), 0);

        let disabled = SwarmScraper::disabled();
        assert_eq!(poll(&disabled, &one_tracker()), SwarmSnapshot::default());
        settle().await;
    }

    /// Polls arriving while a round is in flight join it instead of starting
    /// another one.
    #[tokio::test(start_paused = true)]
    async fn a_round_in_flight_is_not_started_twice() {
        let transport = FakeTracker::answering(ScrapeOutcome::Counts(counts(10, 2)));
        let scraper = SwarmScraper::with_transport(transport.clone());
        let trackers = one_tracker();
        for _ in 0..10 {
            poll(&scraper, &trackers);
        }
        settle().await;
        assert_eq!(transport.calls(), 1);
    }

    /// Live scrape of the Debian netinst ISO -- a legitimate, well-seeded
    /// torrent. Ignored by default: it talks to the real internet, so it must
    /// never run in CI. `cargo test -p enginefs -- --ignored live_scrape`.
    #[tokio::test]
    #[ignore = "hits real trackers"]
    async fn live_scrape_of_the_debian_iso_reports_a_plausible_swarm() {
        let info_hash: [u8; 20] = hex::decode("481b6e3617be4c88f96cb25e47c9d8272130071e")
            .unwrap()
            .try_into()
            .unwrap();
        let outcome = scrape("http://bttracker.debian.org:6969/announce", &info_hash).await;
        match outcome {
            ScrapeOutcome::Counts(counts) => {
                assert!(
                    counts.seeders > 0 && counts.seeders < 100_000,
                    "implausible seeder count: {counts:?}"
                );
            }
            other => panic!("expected counts from the Debian tracker, got {other:?}"),
        }
    }
}
