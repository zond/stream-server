//! Resolving DHT bootstrap entries to address literals before librqbit sees
//! them.
//!
//! # Why
//!
//! librqbit takes `bootstrap_addrs: Vec<String>` and resolves each one with
//! `tokio::net::lookup_host` in `bootstrap_hostname`
//! (`crates/dht/src/dht.rs`), retrying forever on failure. A real Android
//! field log had that step failing for a whole 28-minute session with the
//! system resolver returning `No address associated with hostname` for every
//! bootstrap name -- the DHT never came up because **DNS** was broken, not
//! because the bootstrap hosts were down.
//!
//! `lookup_host` also accepts an address *literal* (`"67.215.246.10:6881"`
//! parses straight to a `SocketAddr` without touching a resolver), so if we
//! resolve the names ourselves and hand librqbit literals, a broken system
//! resolver stops being fatal and no fork change is needed.
//!
//! # What this does
//!
//! For each `host:port` entry, in order, stopping at the first that works:
//!
//! 1. **Literal** -- the host part already parses as an IP address. Passed
//!    through untouched; no DNS of any kind is attempted.
//! 2. **System resolver** -- `tokio::net::lookup_host`, under
//!    [`SYSTEM_LOOKUP_TIMEOUT`]. The normal path, and the only one that runs
//!    on a healthy network.
//! 3. **DNS over HTTPS** -- only when the system resolver yielded *no*
//!    address. `https://dns.google/resolve` then
//!    `https://cloudflare-dns.com/dns-query`, both with the
//!    `accept: application/dns-json` JSON API, each under
//!    [`DOH_REQUEST_TIMEOUT`].
//! 4. **Cache** -- the addresses this host last resolved to, persisted next
//!    to the routing table (see [`BootstrapCache`]). Reused only when live
//!    resolution found nothing, so a later launch on a network whose DNS is
//!    broken can still bootstrap.
//! 5. **Give up, keep the name** -- the original `host:port` string is
//!    passed to librqbit unchanged so it can try the lookup itself. A name
//!    we could not resolve is never *dropped*: librqbit retrying forever is
//!    better than a bootstrap list that lost an entry.
//!
//! Entries are resolved concurrently and the whole pass is bounded by
//! [`RESOLUTION_BUDGET`]; if that elapses, the raw entries are handed to
//! librqbit and start-up continues. **Nothing here may fail start-up or
//! stall it for long.** The DHT bootstrapping late (or never) is a degraded
//! peer source; a server that will not start is an outage.
//!
//! # The limit, stated plainly
//!
//! **This fixes the DNS half only.** If the network drops the DHT's UDP
//! outright -- carrier-grade NAT, a firewalled mobile APN, a captive portal
//! -- then perfectly correct bootstrap addresses change nothing, because the
//! queries never leave. The same Android log that showed DNS failing also
//! showed SSDP refused with `EPERM` and UPnP timing out, which is what a
//! network that blocks this traffic wholesale looks like. Resolving names
//! ourselves removes one specific, observed cause of bootstrap failure; it
//! is not a fix for "the DHT does not work on this network", and
//! `diagnostics::dht_health` still exists to say so once.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// How long the system resolver gets for one host. Short: on a healthy
/// network it answers in milliseconds, and the point of the budget is that a
/// resolver which is going to fail fails quickly enough to leave time for
/// the DoH attempt.
pub const SYSTEM_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// How long one DoH provider gets for one host.
pub const DOH_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// Ceiling on the whole resolution pass, however many entries there are.
/// Entries resolve concurrently, so this is roughly one host's worst case,
/// not the sum. On the normal path the pass costs a few milliseconds; this
/// bound only exists so a pathological resolver cannot hold up start-up.
pub const RESOLUTION_BUDGET: Duration = Duration::from_secs(8);

/// Most addresses kept per host. A bootstrap node needs one working address,
/// not every address its name has; the cap keeps a round-robin name from
/// turning three bootstrap entries into thirty.
const MAX_ADDRS_PER_HOST: usize = 4;

/// DoH endpoints tried in order, both speaking the `application/dns-json`
/// API (`{"Answer":[{"type":1,"data":"1.2.3.4"}]}`).
pub const DOH_ENDPOINTS: &[&str] = &[
    "https://dns.google/resolve",
    "https://cloudflare-dns.com/dns-query",
];

/// The system resolver, behind a trait so tests can inject a failure without
/// touching the network.
#[async_trait::async_trait]
pub trait SystemDnsResolver: Send + Sync {
    /// Addresses for `host:port`, or an empty vec for "no address" (which is
    /// exactly what the broken Android resolver returned). Never errors:
    /// every failure mode here is just "no address", and the caller's next
    /// step is the same either way.
    async fn lookup(&self, host: &str, port: u16) -> Vec<SocketAddr>;
}

/// A DNS-over-HTTPS resolver, behind a trait so tests can inject answers
/// without an HTTP client or a network.
#[async_trait::async_trait]
pub trait DohDnsResolver: Send + Sync {
    /// A-record addresses for `host`, or empty. Never errors, same reasoning
    /// as [`SystemDnsResolver::lookup`].
    async fn lookup(&self, host: &str) -> Vec<IpAddr>;
}

/// [`SystemDnsResolver`] over `tokio::net::lookup_host`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioSystemResolver;

#[async_trait::async_trait]
impl SystemDnsResolver for TokioSystemResolver {
    async fn lookup(&self, host: &str, port: u16) -> Vec<SocketAddr> {
        let target = format!("{host}:{port}");
        match tokio::time::timeout(SYSTEM_LOOKUP_TIMEOUT, tokio::net::lookup_host(target)).await {
            Ok(Ok(addrs)) => addrs.collect(),
            Ok(Err(e)) => {
                debug!(host, error = %e, "system resolver failed for a DHT bootstrap host");
                Vec::new()
            }
            Err(_) => {
                debug!(
                    host,
                    timeout_secs = SYSTEM_LOOKUP_TIMEOUT.as_secs(),
                    "system resolver timed out for a DHT bootstrap host"
                );
                Vec::new()
            }
        }
    }
}

/// [`DohDnsResolver`] over the `application/dns-json` API of [`DOH_ENDPOINTS`].
#[derive(Debug, Clone)]
pub struct HttpsDohResolver {
    client: reqwest::Client,
    endpoints: Vec<String>,
}

impl HttpsDohResolver {
    /// The production resolver: a client of its own (so a proxy or timeout
    /// configured elsewhere cannot surprise us here) against
    /// [`DOH_ENDPOINTS`]. `None` if the TLS stack will not build, in which
    /// case there simply is no DoH fallback -- not a start-up failure.
    pub fn new() -> Option<Self> {
        let client = reqwest::Client::builder()
            .timeout(DOH_REQUEST_TIMEOUT)
            .build()
            .ok()?;
        Some(Self {
            client,
            endpoints: DOH_ENDPOINTS.iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Injection point for the HTTP client and the endpoint list.
    pub fn with_client(client: reqwest::Client, endpoints: Vec<String>) -> Self {
        Self { client, endpoints }
    }
}

#[async_trait::async_trait]
impl DohDnsResolver for HttpsDohResolver {
    async fn lookup(&self, host: &str) -> Vec<IpAddr> {
        // The query string is built by hand rather than with
        // `RequestBuilder::query`, which reqwest gates behind a feature this
        // crate does not enable.
        let name = urlencoding::encode(host);
        for endpoint in &self.endpoints {
            let url = format!("{endpoint}?name={name}&type=A");
            let request = self
                .client
                .get(&url)
                .header("accept", "application/dns-json")
                .timeout(DOH_REQUEST_TIMEOUT)
                .send();
            let body = match tokio::time::timeout(DOH_REQUEST_TIMEOUT, request).await {
                Ok(Ok(response)) => match response.error_for_status() {
                    Ok(response) => response.text().await.ok(),
                    Err(e) => {
                        debug!(host, endpoint, error = %e, "DoH provider returned an error status");
                        None
                    }
                },
                Ok(Err(e)) => {
                    debug!(host, endpoint, error = %e, "DoH request failed");
                    None
                }
                Err(_) => {
                    debug!(host, endpoint, "DoH request timed out");
                    None
                }
            };
            let Some(body) = body else {
                continue;
            };
            let addrs = parse_doh_answer(&body);
            if !addrs.is_empty() {
                return addrs;
            }
            debug!(host, endpoint, "DoH provider answered with no address");
        }
        Vec::new()
    }
}

/// Pull the A/AAAA addresses out of an `application/dns-json` body.
///
/// The `Answer` array also carries the CNAME hops (`"type": 5`) a name went
/// through, so entries are filtered by record type -- 1 (`A`) and 28
/// (`AAAA`) -- and then by whether `data` actually parses as an IP. A body
/// that is not JSON, has no `Answer`, or answers `"Status": 3` (NXDOMAIN,
/// which carries no `Answer` at all) yields nothing.
fn parse_doh_answer(body: &str) -> Vec<IpAddr> {
    #[derive(Deserialize)]
    struct DnsJson {
        #[serde(default, rename = "Answer")]
        answer: Vec<DnsAnswer>,
    }
    #[derive(Deserialize)]
    struct DnsAnswer {
        #[serde(default)]
        r#type: u16,
        #[serde(default)]
        data: String,
    }

    let Ok(parsed) = serde_json::from_str::<DnsJson>(body) else {
        return Vec::new();
    };
    parsed
        .answer
        .iter()
        .filter(|a| a.r#type == 1 || a.r#type == 28)
        .filter_map(|a| a.data.trim().parse::<IpAddr>().ok())
        .collect()
}

/// Which path produced an entry's addresses. Reported at INFO once per
/// start-up so a field log says *how* bootstrap addresses were obtained,
/// which is the thing that was invisible before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedVia {
    /// The entry was already an address literal; no DNS was attempted.
    Literal,
    /// `tokio::net::lookup_host`.
    System,
    /// A DoH provider, after the system resolver returned nothing.
    Doh,
    /// The on-disk cache, after both live paths returned nothing.
    Cache,
    /// Nothing resolved it; the name goes to librqbit as a name.
    Unresolved,
}

impl ResolvedVia {
    fn label(self) -> &'static str {
        match self {
            Self::Literal => "literal",
            Self::System => "system",
            Self::Doh => "doh",
            Self::Cache => "cache",
            Self::Unresolved => "unresolved",
        }
    }
}

/// What one bootstrap entry turned into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEntry {
    /// The `host:port` entry as configured.
    pub entry: String,
    /// What to hand librqbit: address literals, or the original entry when
    /// [`ResolvedVia::Unresolved`].
    pub addrs: Vec<String>,
    pub via: ResolvedVia,
}

/// Host -> last known addresses, persisted next to the DHT routing table.
///
/// Deliberately not expiring: it is consulted *only* when both live paths
/// found nothing, and on that network a two-month-old address for a
/// bootstrap node is strictly better than no address. A stale entry costs
/// one unanswered UDP packet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapCache {
    /// Keyed by bare host, not `host:port`, so changing a port keeps the
    /// cached address.
    #[serde(default)]
    pub hosts: BTreeMap<String, CachedHost>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CachedHost {
    /// IP addresses without a port.
    pub addrs: Vec<String>,
    /// Unix seconds when these addresses last resolved live. Informational:
    /// nothing expires on it (see [`BootstrapCache`]).
    #[serde(default)]
    pub updated_at: i64,
}

impl BootstrapCache {
    /// Read the cache, treating every failure (missing, unreadable, corrupt
    /// JSON) as an empty cache. A cache is an optimisation; a start-up
    /// failure over one would be absurd.
    pub async fn load(path: &Path) -> Self {
        match tokio::fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<Self>(&bytes) {
                Ok(cache) => cache,
                Err(e) => {
                    debug!(path = ?path, error = %e, "DHT bootstrap address cache is unreadable; ignoring it");
                    Self::default()
                }
            },
            Err(e) => {
                debug!(path = ?path, error = %e, "no DHT bootstrap address cache yet");
                Self::default()
            }
        }
    }

    /// Write the cache, logging and swallowing any failure.
    pub async fn store(&self, path: &Path) {
        let Ok(bytes) = serde_json::to_vec_pretty(self) else {
            return;
        };
        if let Some(parent) = path.parent()
            && let Err(e) = tokio::fs::create_dir_all(parent).await
        {
            debug!(path = ?path, error = %e, "could not create the DHT bootstrap cache directory");
            return;
        }
        if let Err(e) = tokio::fs::write(path, bytes).await {
            debug!(path = ?path, error = %e, "could not persist the DHT bootstrap address cache");
        }
    }

    fn addrs_for(&self, host: &str) -> Vec<IpAddr> {
        self.hosts
            .get(host)
            .map(|h| h.addrs.iter().filter_map(|a| a.parse().ok()).collect())
            .unwrap_or_default()
    }

    fn remember(&mut self, host: &str, addrs: &[IpAddr]) {
        if addrs.is_empty() {
            return;
        }
        self.hosts.insert(
            host.to_string(),
            CachedHost {
                addrs: addrs.iter().map(|a| a.to_string()).collect(),
                updated_at: unix_now(),
            },
        );
    }
}

fn unix_now() -> i64 {
    // A dead RTC (Android/embedded before its first time sync) reports a
    // pre-1970 clock; `unwrap_or_default` records 0 rather than panicking,
    // and nothing reads this field for a decision anyway.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// The resolvers used for one pass. Both are injected so tests never touch
/// the network.
#[derive(Clone)]
pub struct BootstrapResolvers {
    pub system: Arc<dyn SystemDnsResolver>,
    /// `None` means "no DoH fallback available", which is the same as a DoH
    /// provider that answers nothing.
    pub doh: Option<Arc<dyn DohDnsResolver>>,
    /// Where to read and write the address cache; `None` disables caching.
    pub cache_path: Option<PathBuf>,
}

/// Whether a caller wants bootstrap names resolved at all.
///
/// `Off` is not a performance knob: it exists so hermetic callers (tests,
/// and any embed that must not make outbound requests at start-up) do no DNS
/// and no HTTP. With it, names reach librqbit as names -- exactly the
/// behaviour before this module existed -- and address literals reach it as
/// literals either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DhtBootstrapDns {
    /// The full ladder: system resolver, DoH, cache.
    #[default]
    Resolve,
    /// No DNS, no HTTP, no cache.
    Off,
}

impl DhtBootstrapDns {
    /// The resolvers this choice implies, caching in `dht_dir` (the
    /// directory holding librqbit's `dht.json`) when it resolves at all.
    pub fn resolvers_in(self, dht_dir: &Path) -> BootstrapResolvers {
        match self {
            Self::Resolve => BootstrapResolvers::production_in(dht_dir),
            Self::Off => BootstrapResolvers::offline(),
        }
    }
}

/// Cache file name, written in the same directory librqbit keeps `dht.json`
/// in so the two halves of "what this DHT knew last time" live together and
/// are wiped together.
pub const CACHE_FILE_NAME: &str = "dht-bootstrap.json";

impl BootstrapResolvers {
    /// The production set: the system resolver, DoH over
    /// [`DOH_ENDPOINTS`], and a [`CACHE_FILE_NAME`] cache in `dht_dir` --
    /// the directory holding librqbit's `dht.json`.
    pub fn production_in(dht_dir: &Path) -> Self {
        Self {
            system: Arc::new(TokioSystemResolver),
            doh: HttpsDohResolver::new().map(|r| Arc::new(r) as Arc<dyn DohDnsResolver>),
            cache_path: Some(dht_dir.join(CACHE_FILE_NAME)),
        }
    }

    /// Resolvers that do nothing at all: no DNS, no HTTP, no cache. Literal
    /// entries still pass through and names are handed to librqbit as
    /// names, so a caller that wants no DNS on the start-up path (tests,
    /// hermetic embeds) gets exactly the input back.
    pub fn offline() -> Self {
        Self {
            system: Arc::new(NoDnsResolver),
            doh: None,
            cache_path: None,
        }
    }
}

/// A [`SystemDnsResolver`] that never resolves anything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoDnsResolver;

#[async_trait::async_trait]
impl SystemDnsResolver for NoDnsResolver {
    async fn lookup(&self, _host: &str, _port: u16) -> Vec<SocketAddr> {
        Vec::new()
    }
}

/// Split a `host:port` entry. Handles the bracketed IPv6 literal form
/// (`[::1]:6881`) as well as `name:port`, and returns `None` for anything
/// that is not a `host:port` at all -- such an entry is passed through
/// untouched rather than mangled.
fn split_host_port(entry: &str) -> Option<(&str, u16)> {
    if let Some(rest) = entry.strip_prefix('[') {
        let (host, rest) = rest.split_once(']')?;
        let port = rest.strip_prefix(':')?.parse().ok()?;
        return Some((host, port));
    }
    let (host, port) = entry.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    Some((host, port.parse().ok()?))
}

/// Format an address as the `host:port` literal `tokio::net::lookup_host`
/// accepts (IPv6 gets its brackets from `SocketAddr`'s `Display`).
fn literal(addr: IpAddr, port: u16) -> String {
    SocketAddr::new(addr, port).to_string()
}

/// Resolve one entry. See the module docs for the ladder.
async fn resolve_entry(
    entry: &str,
    resolvers: &BootstrapResolvers,
    cache: &BootstrapCache,
) -> (ResolvedEntry, Option<(String, Vec<IpAddr>)>) {
    let unresolved = |via| {
        (
            ResolvedEntry {
                entry: entry.to_string(),
                addrs: vec![entry.to_string()],
                via,
            },
            None,
        )
    };

    let Some((host, port)) = split_host_port(entry) else {
        // Not a `host:port` at all. The settings route validates this, but
        // a library caller can hand us anything; pass it through and let
        // librqbit reject it.
        return unresolved(ResolvedVia::Unresolved);
    };

    // 1. Already a literal: no resolver, no HTTP, no cache.
    if host.parse::<IpAddr>().is_ok() {
        return (
            ResolvedEntry {
                entry: entry.to_string(),
                addrs: vec![entry.to_string()],
                via: ResolvedVia::Literal,
            },
            None,
        );
    }

    // 2. System resolver.
    let system = resolvers.system.lookup(host, port).await;
    if !system.is_empty() {
        let ips = dedup_capped(system.into_iter().map(|a| a.ip()));
        return (
            ResolvedEntry {
                entry: entry.to_string(),
                addrs: ips.iter().map(|ip| literal(*ip, port)).collect(),
                via: ResolvedVia::System,
            },
            Some((host.to_string(), ips)),
        );
    }

    // 3. DoH, only because the system resolver found nothing.
    if let Some(doh) = &resolvers.doh {
        let answers = doh.lookup(host).await;
        if !answers.is_empty() {
            let ips = dedup_capped(answers.into_iter());
            return (
                ResolvedEntry {
                    entry: entry.to_string(),
                    addrs: ips.iter().map(|ip| literal(*ip, port)).collect(),
                    via: ResolvedVia::Doh,
                },
                Some((host.to_string(), ips)),
            );
        }
    }

    // 4. Whatever this host resolved to last time.
    let cached = dedup_capped(cache.addrs_for(host).into_iter());
    if !cached.is_empty() {
        return (
            ResolvedEntry {
                entry: entry.to_string(),
                addrs: cached.iter().map(|ip| literal(*ip, port)).collect(),
                via: ResolvedVia::Cache,
            },
            // Not re-stamped: the cache records when an address last
            // resolved *live*, and this was not a live resolution.
            None,
        );
    }

    // 5. Keep the name. librqbit retries its own lookup forever, which may
    // yet succeed if DNS comes back; dropping the entry could not.
    unresolved(ResolvedVia::Unresolved)
}

fn dedup_capped(addrs: impl Iterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    for addr in addrs {
        if !out.contains(&addr) {
            out.push(addr);
        }
        if out.len() == MAX_ADDRS_PER_HOST {
            break;
        }
    }
    out
}

/// Resolve every bootstrap entry to address literals where possible, and
/// report what happened once.
///
/// Returns the flat list to hand `SessionOptions.dht.bootstrap_addrs`: the
/// same entries in the same order, each replaced by its addresses or kept as
/// a name. Never empty when `entries` is non-empty, and never fails.
pub async fn resolve_bootstrap_addrs(
    entries: &[String],
    resolvers: &BootstrapResolvers,
) -> Vec<String> {
    let resolved =
        match tokio::time::timeout(RESOLUTION_BUDGET, resolve_all(entries, resolvers)).await {
            Ok(resolved) => resolved,
            Err(_) => {
                warn!(
                    budget_secs = RESOLUTION_BUDGET.as_secs(),
                    entries = entries.len(),
                    "resolving DHT bootstrap addresses took too long; handing librqbit the names \
                 unresolved so start-up is not held up"
                );
                return entries.to_vec();
            }
        };

    report(&resolved);
    resolved.into_iter().flat_map(|r| r.addrs).collect()
}

/// The resolution pass proper, without the budget or the reporting.
async fn resolve_all(entries: &[String], resolvers: &BootstrapResolvers) -> Vec<ResolvedEntry> {
    let cache = match &resolvers.cache_path {
        Some(path) => BootstrapCache::load(path).await,
        None => BootstrapCache::default(),
    };

    let outcomes =
        futures::future::join_all(entries.iter().map(|e| resolve_entry(e, resolvers, &cache)))
            .await;

    let mut resolved = Vec::with_capacity(outcomes.len());
    let mut updated = cache.clone();
    for (entry, learned) in outcomes {
        if let Some((host, ips)) = learned {
            updated.remember(&host, &ips);
        }
        resolved.push(entry);
    }

    if let Some(path) = &resolvers.cache_path
        && updated != cache
    {
        updated.store(path).await;
    }

    resolved
}

/// One INFO line saying what resolved and by which path, and one WARN when
/// every path failed for every host -- the state the Android field log was
/// in, which previously showed up only as librqbit's own retry spam.
fn report(resolved: &[ResolvedEntry]) {
    if resolved.is_empty() {
        return;
    }
    let summary = resolved
        .iter()
        .map(|r| {
            if r.via == ResolvedVia::Unresolved {
                format!("{}=unresolved", r.entry)
            } else {
                format!("{}={} via {}", r.entry, r.addrs.join(","), r.via.label())
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    if resolved.iter().all(|r| r.via == ResolvedVia::Unresolved) {
        warn!(
            entries = %summary,
            "no DHT bootstrap host could be resolved by the system resolver, DNS over HTTPS or \
             the address cache; librqbit will keep retrying the names itself. Peer discovery \
             falls back to trackers, which is enough for any torrent that has them."
        );
    } else {
        info!(entries = %summary, "resolved DHT bootstrap addresses");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records every host it was asked about, so a test can assert that a
    /// path was *not* taken (an address literal must reach librqbit without
    /// any DNS at all).
    #[derive(Default)]
    struct FakeSystem {
        answers: BTreeMap<String, Vec<SocketAddr>>,
        asked: Mutex<Vec<String>>,
    }

    impl FakeSystem {
        fn with(host: &str, addrs: &[&str]) -> Self {
            let mut answers = BTreeMap::new();
            answers.insert(
                host.to_string(),
                addrs.iter().map(|a| a.parse().unwrap()).collect(),
            );
            Self {
                answers,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl SystemDnsResolver for FakeSystem {
        async fn lookup(&self, host: &str, _port: u16) -> Vec<SocketAddr> {
            self.asked.lock().unwrap().push(host.to_string());
            self.answers.get(host).cloned().unwrap_or_default()
        }
    }

    #[derive(Default)]
    struct FakeDoh {
        answers: BTreeMap<String, Vec<IpAddr>>,
        asked: Mutex<Vec<String>>,
    }

    impl FakeDoh {
        fn with(host: &str, addrs: &[&str]) -> Self {
            let mut answers = BTreeMap::new();
            answers.insert(
                host.to_string(),
                addrs.iter().map(|a| a.parse().unwrap()).collect(),
            );
            Self {
                answers,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl DohDnsResolver for FakeDoh {
        async fn lookup(&self, host: &str) -> Vec<IpAddr> {
            self.asked.lock().unwrap().push(host.to_string());
            self.answers.get(host).cloned().unwrap_or_default()
        }
    }

    fn resolvers(
        system: Arc<dyn SystemDnsResolver>,
        doh: Option<Arc<dyn DohDnsResolver>>,
        cache_path: Option<PathBuf>,
    ) -> BootstrapResolvers {
        BootstrapResolvers {
            system,
            doh,
            cache_path,
        }
    }

    /// An address literal is what librqbit wants already; resolving it would
    /// be a pointless round trip and, on the broken network this whole
    /// module is for, a failing one.
    #[tokio::test]
    async fn an_address_literal_passes_through_without_any_dns() {
        let system = Arc::new(FakeSystem::default());
        let doh = Arc::new(FakeDoh::default());
        let out = resolve_bootstrap_addrs(
            &[
                "67.215.246.10:6881".to_string(),
                "[2001:db8::1]:25401".to_string(),
            ],
            &resolvers(system.clone(), Some(doh.clone()), None),
        )
        .await;

        assert_eq!(out, ["67.215.246.10:6881", "[2001:db8::1]:25401"]);
        assert!(
            system.asked.lock().unwrap().is_empty(),
            "a literal must not reach the system resolver"
        );
        assert!(
            doh.asked.lock().unwrap().is_empty(),
            "a literal must not reach DoH"
        );
    }

    /// The healthy path: the system resolver answers, so DoH is never even
    /// asked.
    #[tokio::test]
    async fn the_system_resolver_answers_and_doh_is_not_consulted() {
        let system = Arc::new(FakeSystem::with("dht.libtorrent.org", &["1.2.3.4:25401"]));
        let doh = Arc::new(FakeDoh::with("dht.libtorrent.org", &["9.9.9.9"]));
        let out = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(system, Some(doh.clone()), None),
        )
        .await;

        assert_eq!(out, ["1.2.3.4:25401"]);
        assert!(
            doh.asked.lock().unwrap().is_empty(),
            "DoH is a fallback, not a second opinion"
        );
    }

    /// The Android field-log case: the system resolver returns "no address
    /// associated with hostname", DoH answers, and librqbit gets a literal.
    #[tokio::test]
    async fn a_name_the_system_resolver_fails_is_retried_over_doh() {
        let system = Arc::new(FakeSystem::default());
        let doh = Arc::new(FakeDoh::with("dht.libtorrent.org", &["185.157.221.247"]));
        let out = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(system.clone(), Some(doh.clone()), None),
        )
        .await;

        assert_eq!(out, ["185.157.221.247:25401"]);
        assert_eq!(
            system.asked.lock().unwrap().as_slice(),
            ["dht.libtorrent.org"]
        );
        assert_eq!(doh.asked.lock().unwrap().as_slice(), ["dht.libtorrent.org"]);
    }

    /// Every path failing must leave the name in the list. librqbit retrying
    /// its own lookup forever can still succeed if DNS comes back; an entry
    /// we dropped cannot.
    #[tokio::test]
    async fn a_name_nothing_can_resolve_is_kept_for_librqbit_to_try() {
        let out = resolve_bootstrap_addrs(
            &[
                "dht.libtorrent.org:25401".to_string(),
                "router.bittorrent.com:6881".to_string(),
            ],
            &resolvers(
                Arc::new(FakeSystem::default()),
                Some(Arc::new(FakeDoh::default())),
                None,
            ),
        )
        .await;

        assert_eq!(
            out,
            ["dht.libtorrent.org:25401", "router.bittorrent.com:6881"]
        );
    }

    /// With no DoH resolver at all (client construction failed) the ladder
    /// still works, it just has one rung fewer.
    #[tokio::test]
    async fn a_missing_doh_resolver_is_not_an_error() {
        let out = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(Arc::new(FakeSystem::default()), None, None),
        )
        .await;
        assert_eq!(out, ["dht.libtorrent.org:25401"]);
    }

    /// What resolves is written next to the routing table, and a later pass
    /// on a network where nothing resolves reuses it.
    #[tokio::test]
    async fn the_cache_is_written_and_reused_when_dns_stops_working() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-bootstrap.json");

        let first = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(
                Arc::new(FakeSystem::with("dht.libtorrent.org", &["1.2.3.4:25401"])),
                None,
                Some(path.clone()),
            ),
        )
        .await;
        assert_eq!(first, ["1.2.3.4:25401"]);

        let stored = BootstrapCache::load(&path).await;
        assert_eq!(
            stored.hosts.get("dht.libtorrent.org").map(|h| &h.addrs),
            Some(&vec!["1.2.3.4".to_string()]),
            "the address that resolved should have been persisted"
        );

        // Same host, same cache, but now nothing resolves it live.
        let second = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(
                Arc::new(FakeSystem::default()),
                Some(Arc::new(FakeDoh::default())),
                Some(path.clone()),
            ),
        )
        .await;
        assert_eq!(
            second,
            ["1.2.3.4:25401"],
            "a later launch on a broken network should reuse the cached address"
        );
    }

    /// The cache keeps the *port* out of its key, so a host whose port
    /// changes still hits.
    #[tokio::test]
    async fn a_cached_host_is_reused_under_a_different_port() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-bootstrap.json");
        let cache = BootstrapCache {
            hosts: BTreeMap::from([(
                "dht.libtorrent.org".to_string(),
                CachedHost {
                    addrs: vec!["1.2.3.4".to_string()],
                    updated_at: 1,
                },
            )]),
        };
        cache.store(&path).await;

        let out = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:6881".to_string()],
            &resolvers(Arc::new(FakeSystem::default()), None, Some(path)),
        )
        .await;
        assert_eq!(out, ["1.2.3.4:6881"]);
    }

    /// A corrupt or truncated cache file is an empty cache, never a failure.
    #[tokio::test]
    async fn a_corrupt_cache_file_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-bootstrap.json");
        tokio::fs::write(&path, b"{not json").await.unwrap();

        assert_eq!(BootstrapCache::load(&path).await, BootstrapCache::default());
        let out = resolve_bootstrap_addrs(
            &["dht.libtorrent.org:25401".to_string()],
            &resolvers(Arc::new(FakeSystem::default()), None, Some(path)),
        )
        .await;
        assert_eq!(out, ["dht.libtorrent.org:25401"]);
    }

    /// A host with several addresses contributes several bootstrap entries,
    /// deduplicated and capped.
    #[tokio::test]
    async fn several_addresses_become_several_entries_capped_and_deduplicated() {
        let system = Arc::new(FakeSystem::with(
            "many.test",
            &[
                "1.1.1.1:6881",
                "1.1.1.1:6881",
                "2.2.2.2:6881",
                "3.3.3.3:6881",
                "4.4.4.4:6881",
                "5.5.5.5:6881",
            ],
        ));
        let out = resolve_bootstrap_addrs(
            &["many.test:6881".to_string()],
            &resolvers(system, None, None),
        )
        .await;
        assert_eq!(out.len(), MAX_ADDRS_PER_HOST);
        assert_eq!(out[0], "1.1.1.1:6881");
        assert!(!out[1..].contains(&"1.1.1.1:6881".to_string()));
    }

    /// `offline()` is the "do no DNS" set embeds and tests use: input out,
    /// input in.
    #[tokio::test]
    async fn the_offline_resolver_set_hands_back_exactly_what_it_was_given() {
        let entries = vec![
            "dht.libtorrent.org:25401".to_string(),
            "67.215.246.10:6881".to_string(),
        ];
        assert_eq!(
            resolve_bootstrap_addrs(&entries, &BootstrapResolvers::offline()).await,
            entries
        );
    }

    /// Live check of the real production ladder against the real DoH
    /// providers. Ignored: it does actual DNS and actual HTTPS. Run it when
    /// touching this module, or to re-verify a provider still answers:
    ///
    /// ```sh
    /// cargo test -p enginefs the_real_resolvers -- --ignored --nocapture
    /// ```
    #[ignore = "requires network; see doc comment"]
    #[tokio::test]
    async fn the_real_resolvers_resolve_the_default_list_and_cache_it() {
        let dir = tempfile::tempdir().unwrap();
        let entries: Vec<String> = crate::backend::librqbit::DEFAULT_DHT_BOOTSTRAP_NODES
            .iter()
            .map(|s| s.to_string())
            .collect();

        let system =
            resolve_bootstrap_addrs(&entries, &BootstrapResolvers::production_in(dir.path())).await;
        println!("system: {system:?}");
        for addr in &system {
            addr.parse::<SocketAddr>()
                .expect("every default bootstrap name should resolve on a working network");
        }
        let cached = BootstrapCache::load(&dir.path().join(CACHE_FILE_NAME)).await;
        assert_eq!(cached.hosts.len(), entries.len(), "{cached:?}");

        // Now with the system resolver removed, so the answers can only have
        // come from a real DoH provider.
        let doh = resolve_bootstrap_addrs(
            &entries,
            &BootstrapResolvers {
                system: Arc::new(NoDnsResolver),
                doh: HttpsDohResolver::new().map(|r| Arc::new(r) as Arc<dyn DohDnsResolver>),
                cache_path: None,
            },
        )
        .await;
        println!("doh: {doh:?}");
        for addr in &doh {
            addr.parse::<SocketAddr>()
                .expect("every default bootstrap name should resolve over DoH");
        }
    }

    #[test]
    fn doh_json_answers_are_filtered_by_record_type() {
        // The shape both providers actually return, CNAME hop included.
        let body = r#"{"Status":0,"Answer":[
            {"name":"dht.libtorrent.org","type":5,"data":"cname.example.org."},
            {"name":"cname.example.org","type":1,"data":"185.157.221.247"},
            {"name":"cname.example.org","type":28,"data":"2001:db8::1"},
            {"name":"cname.example.org","type":1,"data":"not-an-ip"}
        ]}"#;
        assert_eq!(
            parse_doh_answer(body),
            vec![
                "185.157.221.247".parse::<IpAddr>().unwrap(),
                "2001:db8::1".parse::<IpAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn a_doh_body_with_no_usable_answer_yields_nothing() {
        // NXDOMAIN carries no Answer array at all.
        assert!(parse_doh_answer(r#"{"Status":3}"#).is_empty());
        assert!(parse_doh_answer(r#"{"Status":0,"Answer":[]}"#).is_empty());
        assert!(parse_doh_answer("<html>captive portal</html>").is_empty());
    }

    #[test]
    fn host_port_splitting_handles_names_and_both_literal_forms() {
        assert_eq!(
            split_host_port("dht.libtorrent.org:25401"),
            Some(("dht.libtorrent.org", 25401))
        );
        assert_eq!(
            split_host_port("67.215.246.10:6881"),
            Some(("67.215.246.10", 6881))
        );
        assert_eq!(
            split_host_port("[2001:db8::1]:6881"),
            Some(("2001:db8::1", 6881))
        );
        assert_eq!(split_host_port("no-port"), None);
        assert_eq!(split_host_port(":6881"), None);
        assert_eq!(split_host_port("host:not-a-port"), None);
    }

    /// An entry that is not a `host:port` at all is passed through rather
    /// than dropped or mangled -- the settings route rejects these, but a
    /// library caller is not bound by it.
    #[tokio::test]
    async fn a_malformed_entry_is_passed_through_untouched() {
        let out = resolve_bootstrap_addrs(
            &["not-a-host-port".to_string()],
            &resolvers(Arc::new(FakeSystem::default()), None, None),
        )
        .await;
        assert_eq!(out, ["not-a-host-port"]);
    }
}
