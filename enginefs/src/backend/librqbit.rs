use crate::backend::dht_bootstrap::{self, BootstrapResolvers};
use crate::backend::{
    BackendFileInfo, BackendMemoryDiagnostics, BtSettingEffect, BtSettingSupport, BtSettingsReport,
    DhtStatus, DroppedFilePieces, EngineStats, FileStreamTrait, Footprint, Growler,
    LEAN_PEER_LIMIT, PeerDiscovery, PeerSearch, PieceReadiness, RunState, Source, StartupPhase,
    StatsFile, StatsOptions, SwarmCap, TorrentBackend, TorrentFilePriorityPlan, TorrentHandle,
    TorrentListenPort, TorrentPlacement, TorrentPrivacyConfig, TorrentProxyType, TorrentSource,
    TorrentSpeedProfile, TransferTotals,
};
use crate::scrape::SwarmScraper;
use anyhow::{Context, Result};
use librqbit::{ManagedTorrent, ManagedTorrentState, Session};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Upper bound on how long a stream request blocks waiting for librqbit to
/// leave its `Initializing` state (opening/hash-checking files; for a magnet
/// the metadata has already been resolved by `Session::add_torrent`). A fresh
/// or cached torrent initializes in well under a second; a large partially
/// downloaded torrent on slow storage can take tens of seconds. Requests
/// blocked on this gate mirror Stremio's server.js, which holds the HTTP
/// request until data exists -- but they never hang forever.
pub const TORRENT_INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Why a torrent could not be waited into a streamable state. Surfaced through
/// `anyhow` so `routes/stream.rs` can downcast it into a proper non-2xx.
#[derive(Debug, thiserror::Error)]
pub enum TorrentInitError {
    #[error("torrent {info_hash} is still initializing after {timeout_secs}s")]
    TimedOut {
        info_hash: String,
        timeout_secs: u64,
    },
    #[error("torrent {info_hash} failed to initialize: {reason}")]
    Failed { info_hash: String, reason: String },
}

/// Initialization gate shared by the real backend and the test fakes: await
/// `wait` (librqbit's `ManagedTorrent::wait_until_initialized`, or a fake
/// standing in for it) bounded by `timeout`. Blocks -- it never returns early
/// with an empty result -- and maps the two failure modes to `TorrentInitError`.
pub(crate) async fn await_initialized<F>(
    info_hash: &str,
    timeout: Duration,
    wait: F,
) -> std::result::Result<(), TorrentInitError>
where
    F: Future<Output = anyhow::Result<()>>,
{
    let start = Instant::now();
    let result = match tokio::time::timeout(timeout, wait).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(TorrentInitError::Failed {
            info_hash: info_hash.to_string(),
            reason: format!("{e:#}"),
        }),
        Err(_elapsed) => Err(TorrentInitError::TimedOut {
            info_hash: info_hash.to_string(),
            timeout_secs: timeout.as_secs(),
        }),
    };
    match &result {
        Ok(()) => debug!(
            info_hash,
            waited_ms = start.elapsed().as_millis() as u64,
            "torrent left the initializing state"
        ),
        Err(e) => warn!(
            info_hash,
            waited_ms = start.elapsed().as_millis() as u64,
            error = %e,
            "torrent did not become ready"
        ),
    }
    result
}

/// A file-selection update that could not be applied because the torrent was
/// still initializing, parked until initialization completes. Latest-wins:
/// re-deferring replaces the queued op, and a direct apply once the torrent is
/// ready supersedes anything still queued (`supersede`). One waiter task per
/// slot drains the queue after the gate opens.
///
/// The "never clobber a newer one" guarantee only covers ops parked *before*
/// the gate opens: `Reconcile`'s planner (`plan_only_files`) ignores the live
/// selection and just applies the active/hot pair captured when it was
/// queued, so a parked op is not re-derived from anything that changed while
/// it waited -- it is latest-wins among what was queued, not a re-plan
/// against fresh state. There is also a sub-microsecond ordering window
/// between the waiter draining a parked op and a direct op landing at the
/// exact moment initialization completes; whichever runs last wins. This is
/// benign: both derive from the same activation (the same file just started
/// streaming) and both selections always include the streamed file, so
/// neither ordering can starve playback.
pub(crate) struct DeferredSelection<Op> {
    pending: Mutex<Option<Op>>,
    waiter_running: AtomicBool,
}

impl<Op: Send + 'static> DeferredSelection<Op> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(None),
            waiter_running: AtomicBool::new(false),
        })
    }

    /// Whether an op is queued waiting for initialization.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.lock().is_some()
    }

    /// Drop whatever is queued: the caller is about to apply a newer op
    /// directly, which supersedes it.
    pub(crate) fn supersede(&self) -> Option<Op> {
        self.pending.lock().take()
    }

    /// Queue `op` and make sure a waiter is running. `wait` is only awaited by
    /// a newly spawned waiter; `apply` runs for each queued op once the gate
    /// opens. If the gate fails (timeout / init error) the queued op is dropped
    /// with a warning -- the next direct call retries the whole cycle.
    pub(crate) fn defer<W, A, AF>(self: &Arc<Self>, op: Op, wait: W, apply: A)
    where
        W: Future<Output = std::result::Result<(), TorrentInitError>> + Send + 'static,
        A: Fn(Op) -> AF + Send + 'static,
        AF: Future<Output = ()> + Send,
    {
        *self.pending.lock() = Some(op);
        if self.waiter_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            match wait.await {
                Ok(()) => loop {
                    let next = this.pending.lock().take();
                    match next {
                        Some(op) => apply(op).await,
                        None => {
                            this.waiter_running.store(false, Ordering::Release);
                            // Close the race with a `defer` that queued between our
                            // `take` and the store above: it saw the waiter running
                            // and returned, so we must drain it. Safe to keep looping
                            // here because the gate is Ok(()) for every iteration --
                            // a late arrival re-planned against a successful gate is
                            // never stale.
                            if this.has_pending()
                                && !this.waiter_running.swap(true, Ordering::AcqRel)
                            {
                                continue;
                            }
                            break;
                        }
                    }
                },
                Err(e) => this.handle_gate_error(&e),
            }
        });
    }

    /// Handle a gate that resolved to `Err`: drop at most the one op parked
    /// right now, then release the slot unconditionally -- no
    /// drain-and-recheck loop against this (now stale) verdict. The gate
    /// result is a one-shot value, not something ops that show up afterwards
    /// should be judged against.
    ///
    /// A `defer` that races in between our `take` and the `waiter_running`
    /// release below sees `waiter_running` still true and returns without
    /// spawning a new waiter -- but because we unconditionally release the
    /// slot afterwards, the *next* direct call (which is how first-play and
    /// reconcile keep re-triggering) spawns a fresh waiter with a fresh gate
    /// instead of that op being silently judged against our stale failure,
    /// or worse, kept alive indefinitely by a loop that never lets go.
    ///
    /// Split out from `defer`'s spawned task so this exact edge -- a `defer`
    /// landing between the `take` and the release -- has direct unit
    /// coverage without needing to reproduce a genuine cross-thread race.
    fn handle_gate_error(&self, e: &TorrentInitError) {
        if self.pending.lock().take().is_some() {
            warn!(
                error = %e,
                "dropping deferred file-selection update: torrent never became ready"
            );
        }
        self.waiter_running.store(false, Ordering::Release);
    }
}

/// Per-torrent deferred selection slots, keyed by info hash and shared by
/// every `LibrqbitHandle` clone (handles are re-created by `get_torrent`).
type DeferredSelections = Arc<Mutex<HashMap<String, Arc<DeferredSelection<DeferredOp>>>>>;

/// Per-torrent pinned file sets (`TorrentHandle::pin_file`), keyed by info
/// hash and shared by every handle clone for the same reason as
/// `DeferredSelections`. Consulted by every want-set update so a pinned file
/// survives playback switching. The map itself is not persisted: librqbit
/// persists the resulting `only_files` (so the file keeps downloading
/// across a restart) and `BackendEngineFS::restore_pinned_downloads`
/// rebuilds the map at startup from its `pinned-downloads.json` by calling
/// `pin_file` again for every restored torrent.
type PinnedFiles = Arc<Mutex<HashMap<String, BTreeSet<usize>>>>;

/// The last error text reported to the log for a torrent, keyed by info
/// hash and shared by every handle clone like [`PinnedFiles`]. Statistics
/// are polled while a broken download is on screen, so the full librqbit
/// error chain goes to the log once per distinct error rather than once
/// per poll (see `LibrqbitHandle::client_torrent_error`).
type ReportedErrors = Arc<Mutex<HashMap<String, String>>>;

/// Where each open reader is positioned, keyed by `(info hash, file index)`
/// and shared by every handle clone like [`PinnedFiles`].
///
/// The startup-window progress a client renders has to describe the bytes
/// somebody is actually waiting for. Computed from the file head it
/// described, after a seek, a region nobody was fetching -- so it sat at 0%
/// while the seek region streamed perfectly. `get_file_reader` records the
/// offset it was opened at (a `Range` request, a seek and a re-open all
/// arrive as a fresh reader at the new offset), and `stats` reads it back.
type StreamPositions = Arc<Mutex<HashMap<(String, usize), u64>>>;

/// What a client is told about a torrent librqbit put in an error state.
/// librqbit's `TorrentStats.error` is the `{e:?}` of an anyhow chain
/// naming absolute cache and download paths, so only this fixed message
/// crosses the boundary -- like `MagnetAddError::client_message` and
/// `PinDownloadError::client_message`, the chain itself is for the log.
pub const TORRENT_ERROR_MESSAGE: &str = "the torrent is in an error state (its download folder may be unwritable, full or gone); see server logs";

/// Whether a torrent's error is the volume running out of space.
///
/// The needles are not guesses. librqbit e314d8b writes payload through
/// `nix::sys::uio::pwritev` (`storage/filesystem/opened_file.rs`), so on Linux
/// and Android the cause in the chain is a `nix::errno::Errno`, **not** a
/// `std::io::Error` -- downcasting to the latter would silently never match.
/// `Errno`'s `Display` is nix's own static table (`ENOSPC => "No space left on
/// device"`), rendered `"ENOSPC: No space left on device"`, which is verbatim
/// what the field log that prompted this shows. Reproduced here by driving
/// that exact call chain at `/dev/full` and printing the `{e:?}` librqbit
/// stores in `TorrentStats.error`:
///
/// ```text
/// error writing to file 0 ("movie.mkv")
///
/// Caused by:
///     0: error calling pwritev
///     1: ENOSPC: No space left on device
/// ```
///
/// Windows takes the `std::fs` path instead (`seek_write`), where the message
/// is "There is not enough space on the disk." and shares no words with the
/// unix one -- so that half is matched by kind, `ErrorKind::StorageFull`,
/// which is what `std` decodes both `ENOSPC` and `ERROR_DISK_FULL` to.
///
/// Anything else is a torrent problem, not a device problem, and must stay
/// fatal: reclaiming space and restarting would be a loop.
fn is_out_of_space(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::StorageFull)
            || cause.to_string().contains("No space left on device")
    })
}

/// A selection op parked until the torrent initializes.
#[derive(Debug, Clone, Copy)]
struct DeferredOp {
    op: SelectionOp,
    context: &'static str,
}

/// What `apply_selection` does when the torrent is still initializing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitPolicy {
    /// Block (bounded by `TORRENT_INIT_TIMEOUT`) and then apply. Used on the
    /// request path right before a reader is opened, which waits anyway.
    Wait,
    /// Return immediately and apply once initialized. Used for reconcile,
    /// which is also driven from background cleanup loops that must not stall.
    Defer,
    /// Skip: a deselect while nothing has started downloading is moot.
    Skip,
}

/// Public BitTorrent mainline DHT bootstrap nodes used to seed librqbit's
/// routing table when it starts cold (no persisted `dht.json`, or a
/// deserialize failure).
///
/// **This list is not "wider is safer".** An earlier revision padded it out
/// with every conventional public bootstrap name mainstream clients ship, on
/// the theory that more hosts hedge against an outage. Measurement said
/// otherwise: of the five, only two answer a mainline DHT `ping` query at
/// all. Three attempts each, from a working residential connection:
///
/// | host | resolves | answers a DHT ping |
/// |---|---|---|
/// | `dht.libtorrent.org:25401` | yes | 3/3, ~11 ms |
/// | `dht.transmissionbt.com:6881` | yes | 3/3, ~31 ms |
/// | `router.bittorrent.com:6881` | yes (`67.215.246.10`) | 0/3 |
/// | `router.utorrent.com:6881` | yes (`82.221.103.244`) | 0/3 |
/// | `dht.aelitis.com:6881` | yes (`34.203.221.232`) | 0/3 |
///
/// `router.utorrent.com` and `dht.aelitis.com` went first: a name that
/// resolves but never replies is not resilience, it is one more address for
/// `DhtWorker::bootstrap` to time out on and one more retry line in the log.
///
/// `router.bittorrent.com` survived one revision longer, on the argument
/// that it is the most widely deployed bootstrap name in the ecosystem and
/// that its address is anycast, so it might answer from some network that
/// was not the one measured. It was re-probed in 2026-09 from two
/// networks, twice each, with both `ping` and `find_node`: it still
/// resolves to `67.215.246.10` and still answers nothing, on either
/// network, while `dht.libtorrent.org` replied in 11 ms and
/// `dht.transmissionbt.com` in 32 ms on those same runs and both returned
/// nodes. Reputation is not a measurement, and the entry cost one
/// forever-retrying backoff loop per launch, so it is gone too. Nothing
/// else belongs here unless someone has actually pinged it.
///
/// What is left is exactly librqbit's own built-in fallback
/// (`DHT_BOOTSTRAP` in the `zond/rqbit` fork's `crates/dht/src/lib.rs`),
/// ordered fastest-first rather than in librqbit's order. Overriding that
/// default therefore no longer changes *which* hosts are used at all. What
/// the override still buys is that these entries pass through
/// [`dht_bootstrap::resolve_bootstrap_addrs`] first, which turns the names
/// into address literals and drops the v6 ones on a host with no v6 route
/// -- see that module for why (a field log had the system resolver itself
/// returning nothing, and an IPv4-only device retrying AAAA records
/// forever).
///
/// Overridable via the `dhtBootstrapNodes` server setting
/// (`server/src/routes/system.rs`) -- see [`resolve_dht_bootstrap_nodes`].
///
/// **Bootstrapping only matters while the routing table is cold, but
/// "cold" is not automatic.** `PersistentDht::create`
/// (`crates/dht/src/persistence.rs`) loads any existing `dht.json` table
/// before the DHT worker starts, but `DhtState::with_config`'s worker
/// (`crates/dht/src/dht.rs`) unconditionally races `self.bootstrap` against
/// the persisted table on *every* session start -- there is no "skip
/// bootstrap, the table is already warm" branch, and total bootstrap
/// failure kills the DHT worker even with a warm table. So a persisted
/// table (the normal case after the first run) makes bootstrap-host
/// reachability *less consequential* in practice, since the DHT already
/// has real peers to query while the bootstrap requests race in the
/// background, but it does not make bootstrap host reachability
/// irrelevant. The cases where it is fully relevant are the first run, a
/// wiped or corrupted `dht.json`, and a cold start on a fresh install or
/// container.
pub const DEFAULT_DHT_BOOTSTRAP_NODES: &[&str] =
    &["dht.libtorrent.org:25401", "dht.transmissionbt.com:6881"];

/// Resolve the effective DHT bootstrap address list: `configured` (already
/// validated by `server/src/routes/system.rs`'s `dhtBootstrapNodes` setting
/// -- non-empty `host:port` strings only) if non-empty, REPLACING
/// [`DEFAULT_DHT_BOOTSTRAP_NODES`] entirely; otherwise the default set.
/// Mirrors `SessionOptions.dht.bootstrap_addrs`'s own `None` = built-in
/// convention -- and since [`DEFAULT_DHT_BOOTSTRAP_NODES`] is now exactly
/// librqbit's own two-host list, leaving it unconfigured and configuring
/// that same list are the same thing. Configuring anything *else* is what
/// changes which hosts are used. Both branches then go through
/// [`dht_bootstrap::resolve_bootstrap_addrs`], so a configured list gets the
/// same name resolution the default does -- and a configured list of
/// address literals still passes through untouched.
pub fn resolve_dht_bootstrap_nodes(configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        DEFAULT_DHT_BOOTSTRAP_NODES
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        configured.to_vec()
    }
}

/// The full bootstrap pipeline: pick the effective list
/// ([`resolve_dht_bootstrap_nodes`]) and then turn its names into address
/// literals ([`dht_bootstrap::resolve_bootstrap_addrs`]).
///
/// The two steps are one function because they must not drift apart: a
/// configured `dhtBootstrapNodes` list gets exactly the same DNS treatment
/// the built-in default does, and neither list can be silently replaced by
/// the other.
pub async fn effective_dht_bootstrap_addrs(
    configured: &[String],
    resolvers: &BootstrapResolvers,
) -> Vec<String> {
    dht_bootstrap::resolve_bootstrap_addrs(&resolve_dht_bootstrap_nodes(configured), resolvers)
        .await
}

/// What of the `bt*` settings librqbit can actually be handed, in
/// librqbit's own terms. Built from the settings by
/// [`SessionTuning::from_settings`]; every field but `download_bps` is read
/// once, at `Session::new`, and a later change waits for the next start.
///
/// The settings' names and semantics are libtorrent's, and most have no
/// counterpart here -- see [`bt_settings_support`] for the row-by-row
/// account. This struct is the part that does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTuning {
    /// `btEnableDht`: off means the session has no DHT at all.
    pub dht: bool,
    /// `btEnableLsd`: local service discovery multicast.
    pub lsd: bool,
    /// The `btProxy*` settings as the one URL librqbit takes,
    /// `socks5://[user:pass@]host:port` -- see [`socks5_proxy_url`] for
    /// what qualifies. Peer connections and HTTP(S) tracker requests go
    /// through it; UDP trackers and the DHT cannot.
    pub proxy_url: Option<String>,
    /// `btDownloadSpeedHardLimit`, bytes per second; the one knob librqbit
    /// changes on a running session.
    pub download_bps: Option<std::num::NonZeroU32>,
    /// `btMaxConnections`, as the per-torrent peer limit
    /// [`TorrentSpeedProfile::effective_connection_limits`] derives from it.
    pub peer_limit: Option<usize>,
    /// `btOutgoingInterfaces`, when it names one network interface (an
    /// `SO_BINDTODEVICE` name, not an address) -- see [`bind_device_name`].
    pub bind_device: Option<String>,
}

impl Default for SessionTuning {
    /// librqbit's own defaults, which are also the settings' defaults.
    fn default() -> Self {
        Self {
            dht: true,
            lsd: true,
            proxy_url: None,
            download_bps: None,
            peer_limit: None,
            bind_device: None,
        }
    }
}

impl SessionTuning {
    /// The settings, reduced to what the session can take. Anything a
    /// setting asks for that does not fit (a SOCKS4 proxy, an interface
    /// given as an address) is left at librqbit's default and logged: it
    /// is a value the session would otherwise refuse to start with.
    pub fn from_settings(profile: &TorrentSpeedProfile, privacy: &TorrentPrivacyConfig) -> Self {
        Self {
            dht: privacy.bt_enable_dht,
            lsd: privacy.bt_enable_lsd,
            proxy_url: socks5_proxy_url(privacy),
            download_bps: download_bps(profile),
            peer_limit: Some(profile.effective_connection_limits().1 as usize),
            bind_device: bind_device_name(&privacy.bt_outgoing_interfaces),
        }
    }

    /// The settings' JSON keys whose session-start value differs between
    /// `self` (what the session opened with) and `wanted` -- what a change
    /// has to wait for the next start for.
    ///
    /// `peer_limit` is deliberately not among them even though it is part
    /// of this struct: it is what the session *opened* with, and
    /// `LibrqbitBackend::set_configured_peer_limit` changes it on the
    /// running session, so a difference here means "already applied", not
    /// "waiting".
    fn pending_restart(&self, wanted: &Self) -> Vec<&'static str> {
        let mut pending = Vec::new();
        if self.dht != wanted.dht {
            pending.push(BT_ENABLE_DHT);
        }
        if self.lsd != wanted.lsd {
            pending.push(BT_ENABLE_LSD);
        }
        if self.proxy_url != wanted.proxy_url {
            pending.extend(BT_PROXY_SETTINGS);
        }
        if self.bind_device != wanted.bind_device {
            pending.push(BT_OUTGOING_INTERFACES);
        }
        pending
    }
}

const BT_ENABLE_DHT: &str = "btEnableDht";
const BT_ENABLE_LSD: &str = "btEnableLsd";
const BT_MAX_CONNECTIONS: &str = "btMaxConnections";
const BT_OUTGOING_INTERFACES: &str = "btOutgoingInterfaces";
const BT_DOWNLOAD_SPEED_HARD_LIMIT: &str = "btDownloadSpeedHardLimit";
/// The settings that together make the one proxy URL.
const BT_PROXY_SETTINGS: [&str; 5] = [
    "btProxyType",
    "btProxyHost",
    "btProxyPort",
    "btProxyUsername",
    "btProxyPassword",
];

/// The `btProxy*` settings as librqbit's `socks5://[user:pass@]host:port`,
/// or nothing.
///
/// librqbit speaks SOCKS5 and nothing else (`SocksProxyConfig::parse`
/// rejects any other scheme, and a URL it rejects fails `Session::new`,
/// i.e. the server does not start), so `socks4`, `http` and `httpPassword`
/// yield nothing here and are reported as not honoured. Credentials go in
/// for `socks5Password` only, percent-encoded so a `@` or `:` in them
/// cannot re-shape the URL; an IPv6 host literal is bracketed. The result
/// is parsed back the way librqbit will parse it before it is trusted --
/// a host that does not survive `Url::parse` is dropped with a warning
/// rather than handed to a session that would refuse to open.
pub fn socks5_proxy_url(privacy: &TorrentPrivacyConfig) -> Option<String> {
    let with_credentials = match privacy.bt_proxy_type {
        TorrentProxyType::Socks5 => false,
        TorrentProxyType::Socks5Password => true,
        TorrentProxyType::None
        | TorrentProxyType::Socks4
        | TorrentProxyType::Http
        | TorrentProxyType::HttpPassword => return None,
    };
    let host = privacy.bt_proxy_host.trim();
    if host.is_empty() || privacy.bt_proxy_port == 0 {
        return None;
    }
    let host = if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let credentials = if with_credentials {
        format!(
            "{}:{}@",
            urlencoding::encode(&privacy.bt_proxy_username),
            urlencoding::encode(&privacy.bt_proxy_password)
        )
    } else {
        String::new()
    };
    let url = format!("socks5://{credentials}{host}:{}", privacy.bt_proxy_port);
    match url::Url::parse(&url) {
        Ok(parsed) if parsed.host_str().is_some() && parsed.port().is_some() => Some(url),
        _ => {
            warn!(
                host = privacy.bt_proxy_host,
                port = privacy.bt_proxy_port,
                "btProxyHost does not make a proxy URL librqbit can parse; the proxy is not applied"
            );
            None
        }
    }
}

/// `btOutgoingInterfaces` as librqbit's `bind_device_name`, or nothing.
///
/// librqbit binds with `SO_BINDTODEVICE` (`IP_BOUND_IF` on macOS), which
/// takes one interface *name*; the setting allows a comma-separated list of
/// names or addresses, so exactly one entry that is not an address is
/// honoured and anything else is logged and left unbound. A name the OS
/// does not know fails `Session::new` (and any name does on Windows), which
/// `LibrqbitBackend::new_with_settings` answers by starting without it.
pub fn bind_device_name(outgoing_interfaces: &str) -> Option<String> {
    let value = outgoing_interfaces.trim();
    if value.is_empty() {
        return None;
    }
    if value.contains(',') {
        warn!(
            value,
            "btOutgoingInterfaces lists several interfaces; librqbit binds to one, so none is applied"
        );
        return None;
    }
    if value.parse::<std::net::IpAddr>().is_ok() {
        warn!(
            value,
            "btOutgoingInterfaces is an address; librqbit binds by interface name, so it is not applied"
        );
        return None;
    }
    Some(value.to_string())
}

/// `btDownloadSpeedHardLimit` (bytes per second, `0` = unlimited) as the
/// rate limiter's `NonZeroU32`.
fn download_bps(profile: &TorrentSpeedProfile) -> Option<std::num::NonZeroU32> {
    let bps = profile.bt_download_speed_hard_limit;
    // A NaN or a negative number is "no limit" too: `as u32` would make
    // either a 0, and NonZeroU32 turns that into None, but say it here.
    if bps.is_nan() || bps <= 0.0 {
        return None;
    }
    std::num::NonZeroU32::new(bps.min(u32::MAX as f64) as u32)
}

/// The truth table for the `bt*` settings against the librqbit this crate
/// pins: what each one does, and why. Every setting is accepted, echoed
/// back and persisted whether or not anything reads it; this is what lets
/// the settings response, the schema and the docs say which is which.
/// A row changes when the mapping in [`SessionTuning::from_settings`] does,
/// and the test `every_bt_setting_has_a_row_in_the_truth_table` keeps the
/// two lists the same length.
pub fn bt_settings_support() -> &'static [BtSettingSupport] {
    use BtSettingEffect::{Live, NextStart, NotHonoured};
    const TABLE: &[BtSettingSupport] = &[
        BtSettingSupport {
            setting: BT_ENABLE_DHT,
            effect: NextStart,
            note: "the DHT is created with the session: off means no DHT at all",
        },
        BtSettingSupport {
            setting: "btEnablePex",
            effect: NotHonoured,
            note: "librqbit has no PeX switch; ut_pex is always on for public torrents",
        },
        BtSettingSupport {
            setting: BT_ENABLE_LSD,
            effect: NextStart,
            note: "local service discovery multicast is on or off for the whole session",
        },
        BtSettingSupport {
            setting: "btEncryptionMode",
            effect: NotHonoured,
            note: "librqbit speaks plain BitTorrent only (no MSE/PE): 'require' cannot be met \
                   and 'disable' is what always stands",
        },
        BtSettingSupport {
            setting: "btAnonymousMode",
            effect: NotHonoured,
            note: "no equivalent; the client name and peer id are librqbit's own",
        },
        BtSettingSupport {
            setting: "btAllowMultipleConnectionsPerIp",
            effect: NotHonoured,
            note: "librqbit has no per-IP connection rule",
        },
        BtSettingSupport {
            setting: "btListenInterfaces",
            effect: NotHonoured,
            note: "the incoming listener is the launch configuration's TorrentListenPort \
                   (42000-42010 for the binary, ephemeral when embedded), never this setting",
        },
        BtSettingSupport {
            setting: BT_OUTGOING_INTERFACES,
            effect: NextStart,
            note: "one interface name, bound with SO_BINDTODEVICE; an address or a list is not \
                   applied, and a name the OS rejects starts the session unbound",
        },
        BtSettingSupport {
            setting: "btOutgoingPort",
            effect: NotHonoured,
            note: "librqbit lets the OS pick outgoing ports",
        },
        BtSettingSupport {
            setting: "btNumOutgoingPorts",
            effect: NotHonoured,
            note: "librqbit lets the OS pick outgoing ports",
        },
        BtSettingSupport {
            setting: "btProxyType",
            effect: NextStart,
            note: "socks5 and socks5Password only; socks4, http and httpPassword are not \
                   applied",
        },
        BtSettingSupport {
            setting: "btProxyHost",
            effect: NextStart,
            note: "with btProxyType and btProxyPort, the one SOCKS5 proxy peer connections \
                   and HTTP(S) tracker requests go through",
        },
        BtSettingSupport {
            setting: "btProxyPort",
            effect: NextStart,
            note: "see btProxyHost",
        },
        BtSettingSupport {
            setting: "btProxyUsername",
            effect: NextStart,
            note: "sent for socks5Password only",
        },
        BtSettingSupport {
            setting: "btProxyPassword",
            effect: NextStart,
            note: "sent for socks5Password only",
        },
        BtSettingSupport {
            setting: "btProxyHostnames",
            effect: NotHonoured,
            note: "peers are addresses, and tracker hostnames are resolved locally (socks5, \
                   not socks5h)",
        },
        BtSettingSupport {
            setting: "btProxyPeerConnections",
            effect: NotHonoured,
            note: "peer connections always go through a configured proxy",
        },
        BtSettingSupport {
            setting: "btProxyTrackerConnections",
            effect: NotHonoured,
            note: "HTTP(S) tracker requests always go through a configured proxy; UDP trackers \
                   and the DHT never can",
        },
        BtSettingSupport {
            setting: "btProxySendHostInConnect",
            effect: NotHonoured,
            note: "an HTTP CONNECT proxy option, and librqbit has no HTTP proxy",
        },
        BtSettingSupport {
            setting: "btValidateHttpsTrackers",
            effect: NotHonoured,
            note: "HTTPS tracker certificates are always validated; that cannot be turned off",
        },
        BtSettingSupport {
            setting: "btSsrfMitigation",
            effect: NotHonoured,
            note: "no equivalent",
        },
        BtSettingSupport {
            setting: BT_DOWNLOAD_SPEED_HARD_LIMIT,
            effect: Live,
            note: "the session's download rate limiter, bytes per second, 0 for none",
        },
        BtSettingSupport {
            setting: "btDownloadSpeedSoftLimit",
            effect: NotHonoured,
            note: "librqbit has one download limit, not a soft and a hard one",
        },
        BtSettingSupport {
            setting: "btHandshakeTimeout",
            effect: NotHonoured,
            note: "librqbit has a connect timeout (10 s here) and a read/write timeout (30 s); \
                   neither is a handshake timeout, and the second would drop idle peers",
        },
        BtSettingSupport {
            setting: "btRequestTimeout",
            effect: NotHonoured,
            note: "librqbit's request timing is its own",
        },
        BtSettingSupport {
            setting: BT_MAX_CONNECTIONS,
            effect: Live,
            note: "the per-torrent peer limit TorrentSpeedProfile::effective_connection_limits \
                   derives from it (40 to 200 peers; 40 for the default 160), applied to \
                   every torrent at once; while the app is in the background the lean cap \
                   stands and this is what a return to the foreground restores",
        },
        BtSettingSupport {
            setting: "btMinPeersForStable",
            effect: NotHonoured,
            note: "nothing reads it; stats echo librqbit's fixed peer-search figures",
        },
    ];
    TABLE
}

pub struct LibrqbitBackend {
    pub session: Arc<Session>,
    /// What the session was opened with, to say which of a later settings
    /// change is waiting for the next start ([`Self::apply_settings`]).
    started_with: SessionTuning,
    /// Sticky "the DHT routing table has been non-empty at least once this
    /// session", latched by [`LibrqbitBackend::dht_status`]. librqbit keeps
    /// no such flag -- `DhtStats` is instantaneous sizes only -- and the
    /// difference between "idle right now" and "never worked on this
    /// network" is exactly what a client needs to say "DHT unavailable,
    /// using trackers only".
    dht_ever_bootstrapped: AtomicBool,
    download_dir: PathBuf,
    deferred_selections: DeferredSelections,
    pinned_files: PinnedFiles,
    reported_errors: ReportedErrors,
    /// Backend-wide reader positions (see [`StreamPositions`]).
    stream_positions: StreamPositions,
    /// Backend-wide tracker-scrape cache, shared by every handle so one
    /// torrent is scraped once however many handles report on it.
    swarm_scraper: Arc<SwarmScraper>,
    /// Whether every add here sets `AddTorrentOptions::piece_reclaim` -- the
    /// option that makes `ManagedTorrent::drop_pieces`, and so
    /// [`LibrqbitHandle::drop_file_pieces`], available on the torrent.
    /// Decided once, when the session opens, by asking the storage factory
    /// the session hands its torrents ([`session_can_release_pieces`]):
    /// librqbit refuses the option on a storage that cannot release a
    /// single piece, because with one drop frees nothing, and the session as
    /// it runs today uses librqbit's filesystem storage, which writes whole
    /// files and answers no. So today this is `false`, `drop_file_pieces`
    /// is refused by name, and the delete path degrades as its doc says; it
    /// becomes `true` the day the piece store is the session's default
    /// factory, with no other change here.
    piece_reclaim: bool,
    /// The two things that together decide a torrent's live-peer cap (see
    /// [`PeerCaps`]).
    ///
    /// A mutex rather than atomics because a cap change and an add must not
    /// interleave: the add reads the cap and applies it to its new torrent
    /// under this lock, and a change applies to every torrent under it, so
    /// a torrent added during a change ends up with the cap that won, never
    /// with the loser's.
    caps: Mutex<PeerCaps>,
}

/// Whether the storage the session gives a torrent that names none can
/// release a single piece -- and so whether the adds here may set
/// `AddTorrentOptions::piece_reclaim`, which `Session::add_torrent` refuses
/// otherwise, naming the factory. Asked of the factory the session really
/// uses: the one installed as `default_storage_factory`, or librqbit's own
/// filesystem storage when that is `None` -- the same fallback
/// `Session::add_torrent` makes. Never assumed from the type: the promise is
/// the factory's to make ([`librqbit::storage::StorageFactory::ensure_can_release_pieces`]).
fn session_can_release_pieces(
    default_storage: Option<&librqbit::storage::BoxStorageFactory>,
) -> bool {
    use librqbit::storage::StorageFactory;
    let answer = match default_storage {
        Some(factory) => factory.ensure_can_release_pieces(),
        None => librqbit::storage::filesystem::FilesystemStorageFactory::default()
            .ensure_can_release_pieces(),
    };
    match answer {
        Ok(()) => true,
        Err(reason) => {
            debug!(
                reason = %format!("{reason:#}"),
                "the session's storage cannot release single pieces; torrents are added without piece_reclaim"
            );
            false
        }
    }
}

impl LibrqbitBackend {
    /// Open a session storing downloads under `download_dir`, listening for
    /// incoming peers on `listen_port` (see [`TorrentListenPort`]) and
    /// seeding its DHT routing table from `dht_bootstrap_nodes` when cold
    /// (empty uses [`DEFAULT_DHT_BOOTSTRAP_NODES`]; see
    /// [`resolve_dht_bootstrap_nodes`]), with librqbit's own defaults for
    /// everything the `bt*` settings could tune --
    /// [`Self::new_with_settings`] takes those.
    ///
    /// Whatever that list ends up being, `bootstrap_resolvers` turns its
    /// names into address literals before librqbit sees them -- see
    /// [`dht_bootstrap`] for why and for the ladder it walks.
    /// [`BootstrapResolvers::production_in`] is the real one;
    /// [`BootstrapResolvers::offline`] does no DNS at all and is what
    /// hermetic callers want.
    pub async fn new(
        download_dir: PathBuf,
        listen_port: TorrentListenPort,
        dht_bootstrap_nodes: Vec<String>,
        bootstrap_resolvers: BootstrapResolvers,
    ) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        Self::new_with_settings(
            download_dir,
            listen_port,
            dht_bootstrap_nodes,
            bootstrap_resolvers,
            SessionTuning::default(),
        )
        .await
    }

    /// [`Self::new`] with the `bt*` settings the session can take
    /// ([`SessionTuning`]). Read once, here: librqbit configures a session
    /// when it opens it, and only the download rate limit can be changed
    /// afterwards ([`Self::apply_settings`]).
    ///
    /// A bind device the OS rejects fails `Session::new` outright, and so
    /// does every bind device on Windows. That is a setting the user typed,
    /// and a server that does not start over it helps nobody, so the
    /// session is opened again without it and the fact is logged at warn --
    /// the client sees it in the report the next settings update returns,
    /// as `btOutgoingInterfaces` pending a restart that will not help
    /// either.
    pub async fn new_with_settings(
        download_dir: PathBuf,
        listen_port: TorrentListenPort,
        dht_bootstrap_nodes: Vec<String>,
        bootstrap_resolvers: BootstrapResolvers,
        tuning: SessionTuning,
    ) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        debug!(path = ?download_dir, "Storing downloads");

        let bootstrap_addrs =
            effective_dht_bootstrap_addrs(&dht_bootstrap_nodes, &bootstrap_resolvers).await;
        let mut tuning = tuning;
        let session = loop {
            match Self::open_session(&download_dir, &listen_port, &bootstrap_addrs, &tuning).await {
                Ok(session) => break session,
                Err(error) if tuning.bind_device.is_some() => {
                    warn!(
                        interface = ?tuning.bind_device,
                        error = %format!("{error:#}"),
                        "librqbit could not open a session bound to btOutgoingInterfaces; \
                         starting unbound"
                    );
                    tuning.bind_device = None;
                }
                Err(error) => return Err(error),
            }
        };
        let started_with = tuning;
        // `open_session` installs no `default_storage_factory` (see there),
        // so the storage asked here is librqbit's own filesystem storage.
        let piece_reclaim = session_can_release_pieces(None);
        let deferred_selections: DeferredSelections = Default::default();
        let pinned_files: PinnedFiles = Default::default();
        let reported_errors: ReportedErrors = Default::default();
        let stream_positions: StreamPositions = Default::default();
        let swarm_scraper = SwarmScraper::network();
        let caps = PeerCaps {
            footprint: Footprint::Full,
            configured: session.peer_limit.unwrap_or(librqbit::DEFAULT_PEER_LIMIT),
        };
        // Restore from session
        let mut restored_handles = session.with_torrents(|iter| {
            let mut map = HashMap::new();
            for (_id, handle) in iter {
                let info_hash = handle.info_hash().as_string();
                map.insert(
                    info_hash.clone(),
                    LibrqbitHandle {
                        handle: handle.clone(),
                        info_hash,
                        session: session.clone(),
                        deferred_selections: deferred_selections.clone(),
                        pinned_files: pinned_files.clone(),
                        reported_errors: reported_errors.clone(),
                        stream_positions: stream_positions.clone(),
                        swarm_scraper: swarm_scraper.clone(),
                    },
                );
            }
            map
        });

        // Restore from .cache directory
        let cache_dir = download_dir.join(".cache");
        if let Ok(mut entries) = tokio::fs::read_dir(&cache_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "torrent")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    let info_hash = stem.to_string();
                    if !restored_handles.contains_key(&info_hash)
                        && let Ok(bytes) = tokio::fs::read(&path).await
                    {
                        let bytes = bytes::Bytes::from(bytes);
                        let add_torrent = librqbit::AddTorrent::from_bytes(bytes);
                        match session.add_torrent(add_torrent, None).await {
                            Ok(response) => {
                                if let librqbit::AddTorrentResponse::Added(_, handle)
                                | librqbit::AddTorrentResponse::AlreadyManaged(_, handle) =
                                    response
                                {
                                    restored_handles.insert(
                                        info_hash.clone(),
                                        LibrqbitHandle {
                                            handle,
                                            info_hash,
                                            session: session.clone(),
                                            deferred_selections: deferred_selections.clone(),
                                            pinned_files: pinned_files.clone(),
                                            reported_errors: reported_errors.clone(),
                                            stream_positions: stream_positions.clone(),
                                            swarm_scraper: swarm_scraper.clone(),
                                        },
                                    );
                                }
                            }
                            Err(e) => warn!(error = %e, "Failed to add torrent from cache"),
                        }
                    }
                }
            }
        }

        Ok((
            Self {
                session,
                started_with,
                dht_ever_bootstrapped: AtomicBool::new(false),
                download_dir,
                deferred_selections,
                pinned_files,
                reported_errors,
                stream_positions,
                swarm_scraper,
                piece_reclaim,
                caps: Mutex::new(caps),
            },
            restored_handles,
        ))
    }

    /// One attempt at `Session::new_with_opts` over the ports `listen_port`
    /// allows, with `tuning` applied.
    ///
    /// librqbit 9.0.1's ListenerOptions binds a single address instead of
    /// the old `listen_port_range: 42000..42010`, so a `Fixed` range's
    /// port-fallback is done here: try each port in order and keep the
    /// first that binds. `Ephemeral` is the single candidate 0, which
    /// librqbit itself defaults to and resolves to the bound port.
    async fn open_session(
        download_dir: &std::path::Path,
        listen_port: &TorrentListenPort,
        bootstrap_addrs: &[String],
        tuning: &SessionTuning,
    ) -> Result<Arc<Session>> {
        let upnp_forwarding = listen_port.wants_upnp_forwarding();
        let mut last_err = None;
        for port in listen_port.candidates() {
            let session_opts = librqbit::SessionOptions {
                listen: Some(librqbit::ListenerOptions {
                    listen_addr: (std::net::Ipv6Addr::UNSPECIFIED, port).into(),
                    // Only for a fixed, repeatable port -- see
                    // `TorrentListenPort::wants_upnp_forwarding`. An
                    // ephemeral listener would ask the router for a
                    // mapping it can never reuse, and on the Android
                    // embed the request cannot succeed at all while
                    // librqbit's forwarder retries (and WARNs) forever.
                    enable_upnp_port_forwarding: upnp_forwarding,
                    ..Default::default()
                }),
                persistence: Some(librqbit::SessionPersistenceConfig::Json {
                    folder: Some(download_dir.to_path_buf()),
                }),
                // Persist each torrent's verified-piece bitfield
                // (`<info hash>.bitv` in the persistence folder) so a
                // restart validates a sample of pieces instead of
                // re-hashing every file; a corrupted sample falls back
                // to the full check. Matters most for pinned offline
                // downloads, which are large and restart-resident.
                fastresume: true,
                // Pin the DHT routing-table dump next to the session
                // state. librqbit's default resolves through
                // `directories::ProjectDirs` (HOME/XDG), which has no
                // answer on Android and would fail `Session::new`.
                // `bootstrap_addrs` is `DEFAULT_DHT_BOOTSTRAP_NODES`
                // (or the operator's `dhtBootstrapNodes` override),
                // already turned into address literals wherever DNS
                // managed it -- see `dht_bootstrap`. `btEnableDht` off
                // is `None`: librqbit has no DHT to switch off later.
                dht: tuning.dht.then(|| librqbit::DhtSessionConfig {
                    bootstrap_addrs: Some(bootstrap_addrs.to_vec()),
                    persistence: Some(librqbit::dht::DhtPersistenceConfig {
                        config_filename: Some(download_dir.join("dht.json")),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                disable_local_service_discovery: !tuning.lsd,
                connect: Some(librqbit::ConnectionOptions {
                    // See `socks5_proxy_url`: peer connections and HTTP(S)
                    // tracker requests go through it.
                    proxy_url: tuning.proxy_url.clone(),
                    // The backend's own constants, not `btHandshakeTimeout`
                    // / `btRequestTimeout`: neither of these is what those
                    // settings mean (see `bt_settings_support`), and a
                    // read/write timeout of the request timeout's 10 s
                    // would disconnect every idle peer between keep-alives.
                    peer_opts: Some(librqbit::PeerConnectionOptions {
                        connect_timeout: Some(Duration::from_secs(10)),
                        read_write_timeout: Some(Duration::from_secs(30)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ratelimits: librqbit::limits::LimitsConfig {
                    download_bps: tuning.download_bps,
                    upload_bps: None,
                },
                // Per torrent -- librqbit has no session-wide count -- and
                // applied to each torrent as it is added, which is why a
                // change waits for the next start.
                peer_limit: tuning.peer_limit,
                bind_device_name: tuning.bind_device.clone(),
                // No `default_storage_factory`: librqbit's own filesystem
                // storage. The default factory is the one a restored
                // torrent comes back on -- the persisted record names no
                // storage -- so when the piece store is wired in it goes
                // here, as the default, and nowhere else.
                ..Default::default()
            };
            match Session::new_with_opts(download_dir.to_path_buf(), session_opts).await {
                Ok(session) => return Ok(session),
                Err(e) => {
                    debug!(port, error = %e, "librqbit listen port unavailable; trying next");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!("no librqbit listen port available in {listen_port:?}")
        }))
    }

    /// Apply a settings change to the running session, and say what
    /// happened to every `bt*` setting: the download limit and the peer
    /// limit change now, the session-start ones are compared with what
    /// this session opened with and reported as waiting for the next start
    /// when they differ, and the ones librqbit has no knob for are listed
    /// as such every time -- see [`bt_settings_support`] for why each is
    /// where it is.
    ///
    /// `btMaxConnections` used to wait for a restart, because the cap was
    /// `SessionOptions::peer_limit` and a session's options are fixed once
    /// it is open. It does not have to any more: the fork's
    /// `ManagedTorrent::set_peer_limit` is the same runtime lever
    /// [`Footprint`] uses, so lowering the setting hangs up on the surplus
    /// now and raising it re-queues the parked peers, with no restart and
    /// no torrent losing its progress. The new value is stored either way,
    /// so it is also what a return from [`Footprint::Lean`] restores.
    pub fn apply_settings(
        &self,
        profile: &TorrentSpeedProfile,
        privacy: &TorrentPrivacyConfig,
    ) -> BtSettingsReport {
        let wanted = SessionTuning::from_settings(profile, privacy);
        self.session
            .ratelimits
            .set_download_bps(wanted.download_bps);
        self.set_configured_peer_limit(wanted.peer_limit.unwrap_or(librqbit::DEFAULT_PEER_LIMIT));
        BtSettingsReport {
            applied_live: vec![BT_DOWNLOAD_SPEED_HARD_LIMIT, BT_MAX_CONNECTIONS],
            pending_restart: self.started_with.pending_restart(&wanted),
            not_honoured: bt_settings_support()
                .iter()
                .filter(|row| row.effect == BtSettingEffect::NotHonoured)
                .map(|row| row.setting)
                .collect(),
        }
    }

    /// Put a new per-torrent live-peer limit on every torrent the session
    /// holds, and on every one added afterwards.
    ///
    /// While the app is in the background the new value is only stored:
    /// [`Footprint::Lean`]'s cap is the one in force, and this is what a
    /// return to [`Footprint::Full`] restores. Note what is *not* done
    /// here -- a settings change is not a footprint change, so it never
    /// prunes the peer table (`apply_footprint` prunes only for `Lean`,
    /// and the footprint does not move here).
    fn set_configured_peer_limit(&self, limit: usize) {
        let mut caps = self.caps.lock();
        if caps.configured == limit {
            return;
        }
        caps.configured = limit;
        let in_force = caps.limit();
        let torrents = self.apply_caps_to_every_torrent(*caps);
        info!(
            peer_limit = limit,
            in_force,
            footprint = ?caps.footprint,
            torrents,
            "btMaxConnections applied to the running session"
        );
    }

    /// Hermetic constructor for tests: no listen port, no DHT, no persistence,
    /// no UPnP — never binds the production 42000-42010 range or touches the
    /// network. Not compiled into release builds.
    #[cfg(test)]
    pub async fn new_for_tests(download_dir: PathBuf) -> Result<Self> {
        let (backend, _restored) =
            Self::new_for_tests_with(download_dir, TestSessionOptions::default()).await?;
        Ok(backend)
    }

    /// [`Self::new_for_tests`] with what a test may turn on: a storage
    /// factory of its own as the session default, persistence, a loopback
    /// listener. Still no DHT and no UPnP, and still a session that never
    /// touches anything outside `download_dir` and the loopback interface.
    /// The torrents it restores from a persisted session are returned like
    /// [`Self::new_with_settings`] returns them.
    #[cfg(test)]
    pub async fn new_for_tests_with(
        download_dir: PathBuf,
        opts: TestSessionOptions,
    ) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        let session_opts = librqbit::SessionOptions {
            // dht: None disables DHT and its persistence together.
            dht: None,
            listen: opts.listen_loopback.then(|| librqbit::ListenerOptions {
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            persistence: opts
                .persist
                .then(|| librqbit::SessionPersistenceConfig::Json {
                    folder: Some(download_dir.clone()),
                }),
            fastresume: opts.persist,
            default_storage_factory: opts
                .default_storage
                .as_ref()
                .map(librqbit::storage::StorageFactory::clone_box),
            ..Default::default()
        };
        // Same question, same factory, as `new_with_settings` asks.
        let piece_reclaim = session_can_release_pieces(opts.default_storage.as_ref());
        let session = Session::new_with_opts(download_dir.clone(), session_opts).await?;
        let deferred_selections: DeferredSelections = Default::default();
        let pinned_files: PinnedFiles = Default::default();
        let reported_errors: ReportedErrors = Default::default();
        let stream_positions: StreamPositions = Default::default();
        let swarm_scraper = SwarmScraper::disabled();
        let caps = PeerCaps {
            footprint: Footprint::Full,
            configured: session.peer_limit.unwrap_or(librqbit::DEFAULT_PEER_LIMIT),
        };
        let restored_handles = session.with_torrents(|iter| {
            iter.map(|(_id, handle)| {
                let info_hash = handle.info_hash().as_string();
                (
                    info_hash.clone(),
                    LibrqbitHandle {
                        handle: handle.clone(),
                        info_hash,
                        session: session.clone(),
                        deferred_selections: deferred_selections.clone(),
                        pinned_files: pinned_files.clone(),
                        reported_errors: reported_errors.clone(),
                        stream_positions: stream_positions.clone(),
                        swarm_scraper: swarm_scraper.clone(),
                    },
                )
            })
            .collect()
        });
        Ok((
            Self {
                session,
                started_with: SessionTuning::default(),
                dht_ever_bootstrapped: AtomicBool::new(false),
                download_dir,
                deferred_selections,
                pinned_files,
                stream_positions,
                reported_errors,
                swarm_scraper,
                piece_reclaim,
                caps: Mutex::new(caps),
            },
            restored_handles,
        ))
    }
}

/// What [`LibrqbitBackend::new_for_tests_with`] lets a test switch on.
#[cfg(test)]
#[derive(Default)]
pub struct TestSessionOptions {
    /// The session's default storage factory. `None` is librqbit's
    /// filesystem storage, as in production.
    pub default_storage: Option<librqbit::storage::BoxStorageFactory>,
    /// Persist the session (`session.json`, the `.bitv` bitfields) under
    /// the download dir, so a second backend opened over the same dir
    /// restores its torrents -- a restart.
    pub persist: bool,
    /// Accept incoming peers on an ephemeral loopback port, so a seeder a
    /// test runs can come to the torrent.
    pub listen_loopback: bool,
}

/// A gate a test closes to hold librqbit's initial check exactly where it
/// is, and opens to let it run on.
///
/// Every check -- the fastresume sample and the full hash walk alike --
/// reads its pieces through the storage, so a storage that blocks in
/// `pread_exact` blocks the check. That is the only way to be *inside* the
/// window this file's pause/unpause questions are about: the divergences
/// between librqbit's `paused` flag and its state machine all open and close
/// during a check, and a test that waits the check out cannot see one at
/// all.
///
/// A `std` mutex and condvar, not tokio's: what waits on it is librqbit's
/// blocking check thread, not a task.
#[cfg(test)]
#[derive(Default)]
struct InitCheckGateState {
    open: bool,
    /// How many reads have reached the gate. A check is held at its *first*
    /// read and makes no further call while it is there, so before the gate
    /// is opened this counts the checks waiting in it; afterwards it counts
    /// pieces read, which is how a test tells a check that bailed from one
    /// that ran on.
    reads: usize,
}

#[cfg(test)]
#[derive(Default)]
struct InitCheckGate {
    state: std::sync::Mutex<InitCheckGateState>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl InitCheckGate {
    /// Called from the storage, on the check's own thread: record that the
    /// check got here, and hold it until the test lets go.
    fn hold(&self) {
        let mut state = self.state.lock().unwrap();
        state.reads += 1;
        self.changed.notify_all();
        while !state.open {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn open(&self) {
        let mut state = self.state.lock().unwrap();
        state.open = true;
        self.changed.notify_all();
    }

    fn reads(&self) -> usize {
        self.state.lock().unwrap().reads
    }
}

/// librqbit's filesystem storage with every read held at an
/// [`InitCheckGate`].
///
/// Deliberately *not* reclaim-capable -- it forwards `ensure_persistable`,
/// so a persistent session accepts it, and leaves `ensure_can_release_pieces`
/// at the trait's refusal -- because that is the shipped shape: with reclaim
/// on, librqbit forces every restored torrent paused, and the restored-and-
/// unpaused sequence these tests are about would never arise.
#[cfg(test)]
#[derive(Clone, Default)]
struct GatedFilesystemFactory {
    inner: librqbit::storage::filesystem::FilesystemStorageFactory,
    gate: Arc<InitCheckGate>,
}

#[cfg(test)]
impl librqbit::storage::StorageFactory for GatedFilesystemFactory {
    type Storage = GatedFilesystemStorage;

    fn create(
        &self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        Ok(GatedFilesystemStorage {
            inner: self.inner.create(shared, metadata)?,
            gate: self.gate.clone(),
        })
    }

    fn ensure_persistable(&self) -> anyhow::Result<()> {
        self.inner.ensure_persistable()
    }

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        use librqbit::storage::StorageFactoryExt;
        self.clone().boxed()
    }
}

#[cfg(test)]
struct GatedFilesystemStorage {
    inner: librqbit::storage::filesystem::FilesystemStorage,
    gate: Arc<InitCheckGate>,
}

#[cfg(test)]
impl librqbit::storage::TorrentStorage for GatedFilesystemStorage {
    fn init(
        &mut self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.gate.hold();
        self.inner.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn librqbit::storage::TorrentStorage>> {
        self.inner.take()
    }
}

/// A test's end of an [`InitCheckGate`]: it opens the gate however the test
/// ends, its own panics included.
///
/// Not tidiness. A check blocked in `pread_exact` is blocked on a runtime
/// worker, and dropping the runtime waits for it, so an assertion that fires
/// while the gate is shut would hang the test binary instead of failing it
/// -- and every assertion here is *about* what is true inside that window.
#[cfg(test)]
struct HeldInitCheck(Arc<InitCheckGate>);

#[cfg(test)]
impl HeldInitCheck {
    /// Let the check run to its end. Idempotent, so a test says it where it
    /// means it and the drop is only a backstop.
    fn open(&self) {
        self.0.open();
    }

    /// Block until `checks` initial checks are actually inside the gate, so
    /// whatever follows is known to happen in the window and not before it.
    async fn wait_until_held(&self, checks: usize) {
        // Generous on purpose: the bound is here so a regression fails
        // instead of hanging, not as a timing assertion.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.0.reads() >= checks {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "only {} of {checks} initial checks reached the storage",
                self.0.reads()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
impl Drop for HeldInitCheck {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// A storage whose reads are held at the returned gate, and the test's end
/// of that gate.
#[cfg(test)]
fn gated_storage() -> (librqbit::storage::BoxStorageFactory, HeldInitCheck) {
    use librqbit::storage::StorageFactoryExt;
    let factory = GatedFilesystemFactory::default();
    let gate = HeldInitCheck(factory.gate.clone());
    (factory.boxed(), gate)
}

/// A filesystem storage that also *claims* it can release a single piece, so
/// a hermetic test can seed a torrent from real files on disk (which the
/// piece store cannot do without a download) *and* have the session set
/// `piece_reclaim` and so exercise the reclaim/drop API.
///
/// It is a shim, honest for what it is used for: `drop_pieces` is librqbit's
/// own have-set bookkeeping and needs no storage cooperation to forget a
/// piece, which is what these tests check. Reclaiming the *bytes* one piece
/// at a time is the real piece store's job and is tested against it in
/// `piece_store`; over a whole-file filesystem storage a drop frees nothing,
/// which is exactly why the shipped session runs with reclaim off. It also
/// forwards `ensure_persistable` (the filesystem storage keeps that
/// promise), so a persistent session accepts it -- what the restart test
/// needs.
#[cfg(test)]
#[derive(Clone, Default)]
struct ReclaimableFilesystemFactory(librqbit::storage::filesystem::FilesystemStorageFactory);

#[cfg(test)]
impl librqbit::storage::StorageFactory for ReclaimableFilesystemFactory {
    type Storage = librqbit::storage::filesystem::FilesystemStorage;

    fn create(
        &self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        self.0.create(shared, metadata)
    }

    fn ensure_persistable(&self) -> anyhow::Result<()> {
        self.0.ensure_persistable()
    }

    fn ensure_can_release_pieces(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        use librqbit::storage::StorageFactoryExt;
        self.clone().boxed()
    }
}

/// A boxed [`ReclaimableFilesystemFactory`], the reclaim-capable storage the
/// tests that need `piece_reclaim` over real files hand to a test session.
#[cfg(test)]
pub fn reclaimable_storage() -> librqbit::storage::BoxStorageFactory {
    use librqbit::storage::StorageFactoryExt;
    ReclaimableFilesystemFactory::default().boxed()
}

#[cfg(test)]
impl LibrqbitBackend {
    /// A hermetic backend whose session can release pieces (so every add
    /// sets `piece_reclaim` and `drop_file_pieces` works), for the tests that
    /// need the reclaim path over a filesystem-seeded torrent.
    pub async fn new_for_tests_reclaiming(download_dir: PathBuf) -> Result<Self> {
        let (backend, _restored) = Self::new_for_tests_with(
            download_dir,
            TestSessionOptions {
                default_storage: Some(reclaimable_storage()),
                ..Default::default()
            },
        )
        .await?;
        Ok(backend)
    }
}

/// Pure helper mapping a file's length and chunk-tracker have-bytes to the
/// (downloaded, progress) pair reported in /stats.json. Clamps have > len
/// (last-piece rounding in the chunk tracker) and treats zero-length files as
/// fully downloaded with progress 1.0.
fn file_progress_fields(len: u64, have: u64) -> (u64, f64) {
    let downloaded = have.min(len);
    let progress = if len == 0 {
        1.0
    } else {
        downloaded as f64 / len as f64
    };
    (downloaded, progress)
}

/// A file-selection operation on a multi-file torrent, mapped by
/// `plan_only_files` onto librqbit's `only_files` want-set.
#[derive(Debug, Clone, Copy)]
enum SelectionOp {
    /// Exclusive selection of one file for streaming (plus the pinned set).
    Prepare(usize),
    /// Deselect one file, keeping the rest of the selection (a pinned file
    /// is never deselected).
    Clear(usize),
    /// Want exactly the union of the active and hot files (plus the pinned
    /// set).
    Reconcile {
        active: Option<usize>,
        hot: Option<usize>,
    },
    /// Add one file to whatever is currently selected (plus the pinned set)
    /// without disturbing the playback selection.
    Pin(usize),
}

/// Pure planner mapping the current `only_files` selection, the pinned set
/// and an operation to the new selection to apply. `None` means "apply
/// nothing".
///
/// Invariants enforced here (unit-tested):
/// - Single-file torrents are always fully wanted: never touch selection.
/// - The result is never an empty set (that would make nothing wanted and
///   starve playback).
/// - `Clear` of a file that is not currently selected (a newer `Prepare`
///   already switched away) is a no-op, so late delayed-cleanup and HLS-lease
///   expiry cannot clobber the active selection.
/// - Every in-range pinned index is in every result, and `Clear` of a pinned
///   index is a no-op: playback switching never deselects an offline
///   download.
/// - Out-of-range indices are dropped; a plan left empty by that is a no-op.
fn plan_only_files(
    current: Option<&[usize]>,
    file_count: usize,
    pinned: &BTreeSet<usize>,
    op: SelectionOp,
) -> Option<HashSet<usize>> {
    if file_count <= 1 {
        return None;
    }
    let in_range = |i: &usize| *i < file_count;
    let with_pinned = |set: HashSet<usize>| -> Option<HashSet<usize>> {
        let set: HashSet<usize> = set
            .into_iter()
            .chain(pinned.iter().copied())
            .filter(in_range)
            .collect();
        if set.is_empty() { None } else { Some(set) }
    };
    match op {
        SelectionOp::Prepare(idx) => {
            if idx >= file_count {
                return None;
            }
            with_pinned(std::iter::once(idx).collect())
        }
        SelectionOp::Clear(idx) => {
            let current = current?;
            if !current.contains(&idx) || pinned.contains(&idx) {
                return None;
            }
            let remainder: HashSet<usize> = current.iter().copied().filter(|i| *i != idx).collect();
            if remainder.is_empty() {
                None
            } else {
                with_pinned(remainder)
            }
        }
        SelectionOp::Reconcile { active, hot } => {
            with_pinned(active.into_iter().chain(hot).collect())
        }
        SelectionOp::Pin(idx) => {
            if idx >= file_count {
                return None;
            }
            // `current == None` is librqbit for "everything wanted": pinning
            // narrows that to the pinned set, which is the point of an
            // offline download (fetch this file, not the whole torrent).
            let mut set: HashSet<usize> = current.unwrap_or_default().iter().copied().collect();
            set.insert(idx);
            with_pinned(set)
        }
    }
}

/// The claim [`LibrqbitHandle::drop_file_pieces`] hands out: librqbit's own
/// [`librqbit::DroppedPieces`], plus the re-selection its release does not
/// do by itself.
///
/// A dropped piece stays dropped until something re-selects it -- a seek
/// into it, or its file going from unselected to selected -- and releasing
/// the claim does not: `finish_release` only lets a piece that was
/// re-selected *meanwhile* be downloaded. So the boundary piece the deleted
/// file shared with a still-selected neighbour would stay out of the
/// neighbour's want-set for the rest of the session, and an offline device
/// would find the neighbour's first or last piece missing with no way to
/// fetch it. Dropping this re-selects the range after the release: librqbit
/// queues the pieces of files that are still selected (the shared one) and
/// leaves the deleted file's own pieces as plain missing pieces, which is
/// what a later re-pin expects to find.
struct ReleaseThenReselect {
    dropped: Option<librqbit::DroppedPieces>,
    torrent: Arc<ManagedTorrent>,
    range: std::ops::Range<u32>,
}

impl Drop for ReleaseThenReselect {
    fn drop(&mut self) {
        // Release first, then re-select: a piece re-selected while still
        // under release would be one the deletion could race.
        drop(self.dropped.take());
        match self.torrent.reselect_pieces(self.range.clone()) {
            Ok(reselected) => debug!(
                info_hash = %self.torrent.info_hash().as_string(),
                reselected,
                "re-selected what a still-selected file shares with the deleted one"
            ),
            // Not live any more (paused or gone): the have-set will be
            // rebuilt from disk when it next comes up, so there is nothing
            // to correct here.
            Err(error) => debug!(
                info_hash = %self.torrent.info_hash().as_string(),
                error = %format!("{error:#}"),
                "could not re-select around the deleted file's pieces"
            ),
        }
    }
}

pub struct LibrqbitHandle {
    pub handle: Arc<ManagedTorrent>,
    pub info_hash: String,
    /// Kept so the handle can apply per-file selection via
    /// `Session::update_only_files` (librqbit persists file selection on the
    /// session, not the torrent handle).
    session: Arc<Session>,
    /// Backend-wide deferred-selection slots (see `DeferredSelection`).
    deferred_selections: DeferredSelections,
    /// Backend-wide pinned file sets (see `PinnedFiles`).
    pinned_files: PinnedFiles,
    /// Backend-wide last-reported error texts (see [`ReportedErrors`]).
    reported_errors: ReportedErrors,
    /// Backend-wide reader positions (see [`StreamPositions`]).
    stream_positions: StreamPositions,
    /// Backend-wide swarm-scrape cache (see [`SwarmScraper`]).
    swarm_scraper: Arc<SwarmScraper>,
}

/// Put `trackers` into a magnet link as `tr=` params.
///
/// librqbit's `Session::add_torrent` takes a magnet's trackers from the magnet
/// URL's own `tr=` params only; `AddTorrentOptions::trackers` is merged in the
/// torrent-file branch alone (session.rs: the `AddTorrent::Url` magnet arm
/// builds `InternalAddResult.trackers` from `Magnet::trackers`, the `other`
/// arm extends the metainfo's announce list with `opts.trackers`). So the
/// merged tracker list has to travel inside the URL, or a magnet add reaches
/// librqbit tracker-less (DHT-only) and `stats().sources` comes back empty.
///
/// Appends one percent-encoded `tr=` per tracker not already in the URL
/// (`Magnet::parse` collects every `tr` via `Url::query_pairs`, which
/// decodes them again). A bare 40-hex info hash, which librqbit also accepts,
/// becomes a full magnet link first. Anything else is returned unchanged.
pub fn magnet_with_trackers(url: &str, trackers: &[String]) -> String {
    let magnet = if url.len() == 40 && url.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!("magnet:?xt=urn:btih:{url}")
    } else {
        url.to_string()
    };
    if !magnet.starts_with("magnet:") {
        return magnet;
    }
    let Ok(mut parsed) = url::Url::parse(&magnet) else {
        return magnet;
    };
    let existing: Vec<String> = parsed
        .query_pairs()
        .filter(|(key, _)| key == "tr")
        .map(|(_, value)| value.into_owned())
        .collect();
    let mut pairs = parsed.query_pairs_mut();
    for tracker in trackers {
        if !existing.contains(tracker) {
            pairs.append_pair("tr", tracker);
        }
    }
    drop(pairs);
    parsed.to_string()
}

/// Move a file, falling back to copy + remove when `rename` cannot cross
/// the device boundary (`EXDEV`; `ERROR_NOT_SAME_DEVICE` on Windows -- both
/// `ErrorKind::CrossesDevices`). A failed copy removes its partial target.
pub(crate) async fn move_file(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::rename(src, dst).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
            copy_then_remove(src, dst).await
        }
        Err(e) => Err(e),
    }
}

/// Whether a file of a torrent being relocated is worth moving: it has
/// verified bytes per the chunk tracker (`have`, known once the torrent
/// has initialized), or -- `have` unknown, the torrent still Initializing
/// -- it has blocks allocated on disk, which a pre-sized sparse placeholder
/// has not (on platforms without block counts every existing file moves).
fn has_data_to_move(have: Option<u64>, metadata: &std::fs::Metadata) -> bool {
    match have {
        Some(have) => have > 0,
        None => has_allocated_blocks(metadata),
    }
}

#[cfg(unix)]
fn has_allocated_blocks(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() > 0
}

#[cfg(not(unix))]
fn has_allocated_blocks(_metadata: &std::fs::Metadata) -> bool {
    true
}

async fn copy_then_remove(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    if let Err(e) = tokio::fs::copy(src, dst).await {
        let _ = tokio::fs::remove_file(dst).await;
        return Err(e);
    }
    tokio::fs::remove_file(src).await
}

/// Put `footprint` on one torrent: prune the peer table first when going
/// lean (see `LibrqbitBackend::set_footprint` for why the order matters),
/// then the cap. A torrent that is not live has nothing to prune and takes
/// the cap for when it is.
fn apply_footprint(handle: &ManagedTorrent, footprint: Footprint, peer_limit: usize) {
    if footprint == Footprint::Lean
        && let Some(live) = handle.live()
    {
        let forgotten = live.forget_disconnected_peers();
        debug!(
            info_hash = %handle.info_hash().as_string(),
            forgotten,
            "going lean: pruned the peer table"
        );
    }
    handle.set_peer_limit(peer_limit);
}

/// What the per-torrent live-peer cap is right now: the configured limit
/// `btMaxConnections` derives, and the [`Footprint`] that may be overriding
/// it. They live in one lock because they are one decision -- `limit()` --
/// and either can change under the other.
#[derive(Debug, Clone, Copy)]
struct PeerCaps {
    footprint: Footprint,
    /// What `btMaxConnections` currently asks for, per torrent
    /// ([`TorrentSpeedProfile::effective_connection_limits`]). Starts as
    /// whatever the session was opened with, and is changed live by
    /// [`LibrqbitBackend::apply_settings`].
    configured: usize,
}

impl PeerCaps {
    /// The cap in force: the configured limit, or [`LEAN_PEER_LIMIT`] while
    /// the app is in the background.
    fn limit(&self) -> usize {
        match self.footprint {
            Footprint::Full => self.configured,
            Footprint::Lean => LEAN_PEER_LIMIT,
        }
    }
}

impl LibrqbitBackend {
    /// Apply `limit` to every torrent the session holds, going lean first
    /// if `footprint` says to. Handles are collected before any work:
    /// `with_torrents` holds the session's torrent table and the lever
    /// takes torrent-level locks. Returns how many torrents it touched.
    fn apply_caps_to_every_torrent(&self, caps: PeerCaps) -> usize {
        let torrents: Vec<Arc<ManagedTorrent>> = self
            .session
            .with_torrents(|iter| iter.map(|(_, handle)| handle.clone()).collect());
        for handle in &torrents {
            apply_footprint(handle, caps.footprint, caps.limit());
        }
        torrents.len()
    }

    fn wrap(&self, handle: Arc<ManagedTorrent>) -> LibrqbitHandle {
        let info_hash = handle.info_hash().as_string();
        LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        }
    }

    /// Shared body of `remove_torrent` (`delete_files: false`, keeping the
    /// downloaded data on disk like the libtorrent backend's
    /// `remove_torrent(handle, false)` did) and `remove_torrent_and_files`.
    async fn delete_torrent(&self, info_hash: &str, delete_files: bool) -> Result<()> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        let output_folder = self
            .session
            .get(id)
            .map(|handle| handle.output_folder().to_path_buf());
        self.session
            .delete(id, delete_files)
            .await
            .with_context(|| format!("failed to remove torrent {info_hash}"))?;
        self.deferred_selections.lock().remove(info_hash);
        self.pinned_files.lock().remove(info_hash);
        self.reported_errors.lock().remove(info_hash);
        // `Session::delete(_, false)` only removes empty directories on the
        // delete_files=true branch, so a torrent that never wrote anything
        // (or whose files were cleaned out) would leave its output folder
        // behind on every idle sweep. `remove_dir` fails on a non-empty
        // directory, so this can only ever drop an empty folder -- and never
        // the session root, which single-file torrents write straight into.
        if let Some(folder) = output_folder
            && folder != self.download_dir
            && let Err(e) = tokio::fs::remove_dir(&folder).await
            && !matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            )
        {
            debug!(error = %e, path = ?folder, "Left the torrent's output folder in place");
        }
        // Best-effort: drop the cached .torrent file so the restore path in
        // `new()` does not resurrect the torrent on the next startup.
        let cached = self
            .download_dir
            .join(".cache")
            .join(format!("{info_hash}.torrent"));
        if let Err(e) = tokio::fs::remove_file(&cached).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(error = %e, path = ?cached, "Failed to remove cached torrent file");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TorrentBackend for LibrqbitBackend {
    type Handle = LibrqbitHandle;

    fn sets_piece_reclaim(&self) -> bool {
        self.piece_reclaim
    }

    /// The hash a source names, without adding it -- for the
    /// evicted-for-space refusal, which has to happen before any file is
    /// created.
    ///
    /// A `.torrent` blob is parsed with librqbit's own
    /// `torrent_from_bytes`, so what this reports is exactly what the add
    /// would manage; a magnet link goes through `Magnet::parse`, and a bare
    /// 40-hex hash through `magnet_with_trackers`'s upgrade of one, so this
    /// accepts everything `add_torrent_placed` does. `None` for a blob that
    /// will not parse (the add is left to produce the real error, in its
    /// own words) and for a `.torrent` behind an http URL, whose hash is
    /// not knowable without fetching it -- librqbit fetches it inside the
    /// add. Lowercased hex, like every hash the engine layer keys on.
    fn source_info_hash(&self, source: &TorrentSource) -> Option<String> {
        match source {
            TorrentSource::Bytes(bytes) => Some(
                librqbit::torrent_from_bytes(bytes)
                    .ok()?
                    .info_hash
                    .as_string()
                    .to_lowercase(),
            ),
            TorrentSource::Url(url) => {
                let magnet = magnet_with_trackers(url, &[]);
                let magnet = librqbit::Magnet::parse(&magnet).ok()?;
                Some(magnet.as_id20()?.as_string().to_lowercase())
            }
        }
    }

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        self.add_torrent_placed(source, trackers, TorrentPlacement::default())
            .await
    }

    /// `placement.output_folder` becomes librqbit's per-torrent
    /// `output_folder` (persisted with the torrent, so a restart restores
    /// the place) and `placement.only_files` its initial want-set; librqbit
    /// rejects an out-of-range index at add time. `overwrite: true` always:
    /// resuming on top of existing files is the normal case here (a restart,
    /// a relocated torrent).
    async fn add_torrent_placed(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
        placement: TorrentPlacement,
    ) -> Result<Self::Handle> {
        let add_torrent = match source {
            // See `magnet_with_trackers`: for a magnet only the URL's own
            // `tr=` params count; `opts.trackers` below covers .torrent adds.
            TorrentSource::Url(url) => {
                librqbit::AddTorrent::Url(magnet_with_trackers(&url, &trackers).into())
            }
            TorrentSource::Bytes(bytes) => {
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(bytes))
            }
        };
        let response = self
            .session
            .add_torrent(
                add_torrent,
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    trackers: Some(trackers),
                    output_folder: placement
                        .output_folder
                        .map(|folder| folder.to_string_lossy().into_owned()),
                    only_files: placement.only_files,
                    // What makes `ManagedTorrent::drop_pieces` -- and so
                    // `LibrqbitHandle::drop_file_pieces` -- available on
                    // this torrent. Off, librqbit refuses to forget a piece
                    // it has, and a per-file delete of a pinned download
                    // leaves it advertising pieces whose bytes are gone.
                    // Nothing drops anything on its own with it on; it only
                    // opens the API, at the price of a read lock per Have
                    // the torrent announces. Set only when the session's
                    // storage can release a single piece (see the field):
                    // librqbit refuses the option otherwise, and would
                    // refuse the whole add with it. Persisted with the
                    // torrent, and a restored torrent that has it comes
                    // back paused whatever it was doing at shutdown --
                    // see `reconcile::Conditions::settled`, which is what
                    // keeps the engine layer from starting one before it
                    // has put the want-set back.
                    piece_reclaim: self.piece_reclaim,
                    ..Default::default()
                }),
            )
            .await
            .context("Failed to add torrent to librqbit")?;

        let (_id, handle) = match response {
            librqbit::AddTorrentResponse::Added(id, handle)
            | librqbit::AddTorrentResponse::AlreadyManaged(id, handle) => (id, handle),
            _ => return Err(anyhow::anyhow!("Unexpected response from librqbit")),
        };
        // Under the footprint lock, so a change racing this add cannot
        // leave the new torrent with the old cap (see the field).
        {
            let caps = self.caps.lock();
            apply_footprint(&handle, caps.footprint, caps.limit());
        }

        let info_hash = handle.info_hash().as_string();
        Ok(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        })
    }

    /// librqbit has no relocate call, so: drop the torrent from the session
    /// keeping its files (`Session::delete(_, false)` also drops its
    /// persisted record and `.bitv` bitfield), move every file that holds
    /// verified data from the old output folder into the new one (rename,
    /// copy + remove across devices), then re-add it from its own metainfo
    /// bytes with the placement and `overwrite: true` -- librqbit
    /// hash-checks the moved data (`checking` phase), so nothing verified is
    /// lost. Files without any verified bytes are not moved but deleted
    /// with the old folder: librqbit pre-sizes every wanted file when a
    /// torrent goes live (all of them for a plain magnet add), so a streamed
    /// season pack has a full-length sparse placeholder per episode, and a
    /// cross-device copy would write every one of them out as zeros. The
    /// re-added torrent pre-sizes what it wants in the new folder itself.
    /// If the move or the re-add fails the torrent is re-added where it was
    /// (best effort) and the error returned. The deferred-selection slot
    /// belonged to the old torrent and is dropped; the pin set stays.
    async fn relocate_torrent(
        &self,
        info_hash: &str,
        placement: TorrentPlacement,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        let handle = self
            .session
            .get(id)
            .with_context(|| format!("torrent {info_hash} is not managed"))?;
        let metadata = handle
            .metadata
            .load_full()
            .with_context(|| format!("torrent {info_hash} has no metadata yet"))?;
        let target = placement
            .output_folder
            .clone()
            .context("relocation needs an output folder")?;
        let old_folder = handle.output_folder().to_path_buf();
        let old_only_files = handle.only_files();
        if old_folder == target {
            return Ok(self.wrap(handle));
        }
        // Per-file verified bytes, snapshotted while the torrent still has a
        // chunk tracker (empty while it is Initializing -- then the file's
        // own allocation decides, see `has_data_to_move`).
        let file_progress = handle.stats().file_progress;
        self.session
            .delete(id, false)
            .await
            .with_context(|| format!("failed to drop torrent {info_hash} before relocating"))?;
        self.deferred_selections.lock().remove(info_hash);

        let relocated = async {
            tokio::fs::create_dir_all(&target)
                .await
                .with_context(|| format!("creating {}", target.display()))?;
            for (idx, file) in metadata.file_infos.iter().enumerate() {
                let src = old_folder.join(&file.relative_filename);
                let Ok(src_metadata) = tokio::fs::metadata(&src).await else {
                    continue;
                };
                if !has_data_to_move(file_progress.get(idx).copied(), &src_metadata) {
                    debug!(
                        src = %src.display(),
                        "no verified data; dropping the placeholder instead of moving it"
                    );
                    tokio::fs::remove_file(&src)
                        .await
                        .with_context(|| format!("removing {}", src.display()))?;
                    continue;
                }
                let dst = target.join(&file.relative_filename);
                if tokio::fs::try_exists(&dst).await.unwrap_or(false) {
                    // Data already in the destination (downloaded there
                    // before) wins over the source: librqbit pre-sizes
                    // every wanted file in the old folder, so the source
                    // is often a sparse placeholder that would wipe
                    // verified bytes. The re-check sorts out what the
                    // destination actually has.
                    debug!(
                        src = %src.display(),
                        dst = %dst.display(),
                        "destination file exists; keeping it and dropping the source"
                    );
                    tokio::fs::remove_file(&src)
                        .await
                        .with_context(|| format!("removing {}", src.display()))?;
                    continue;
                }
                if let Some(parent) = dst.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                move_file(&src, &dst)
                    .await
                    .with_context(|| format!("moving {} to {}", src.display(), dst.display()))?;
            }
            // The old folder is a per-torrent one only for multi-file
            // torrents (single-file ones write into the session root, which
            // stays); drop it once empty, like `remove_torrent` does.
            if old_folder != self.download_dir
                && let Err(e) = tokio::fs::remove_dir(&old_folder).await
                && !matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                )
            {
                debug!(error = %e, path = ?old_folder, "Left the old output folder in place");
            }
            self.add_torrent_placed(
                TorrentSource::Bytes(metadata.torrent_bytes.to_vec()),
                trackers.clone(),
                placement,
            )
            .await
        }
        .await;
        match relocated {
            Ok(handle) => Ok(handle),
            Err(error) => {
                warn!(
                    info_hash,
                    error = %format!("{error:#}"),
                    "relocation failed; re-adding the torrent where it was"
                );
                if let Err(e) = self
                    .add_torrent_placed(
                        TorrentSource::Bytes(metadata.torrent_bytes.to_vec()),
                        trackers,
                        TorrentPlacement {
                            output_folder: Some(old_folder),
                            only_files: old_only_files,
                        },
                    )
                    .await
                {
                    warn!(info_hash, error = %format!("{e:#}"), "re-adding the torrent failed too");
                }
                Err(error)
            }
        }
    }

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash).ok()?;
        let handle = self.session.get(id)?;
        let info_hash = handle.info_hash().as_string();
        Some(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        })
    }

    async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        self.delete_torrent(info_hash, false).await
    }

    /// `Session::delete(_, true)`: librqbit removes the files and, for a
    /// torrent with its own output folder, that folder once empty.
    async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
        self.delete_torrent(info_hash, true).await
    }

    async fn list_torrents(&self) -> Vec<String> {
        self.session.with_torrents(|iter| {
            iter.map(|(_id, handle)| handle.info_hash().as_string())
                .collect()
        })
    }

    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
        BackendMemoryDiagnostics::default()
    }

    /// The per-torrent lever is librqbit's `ManagedTorrent::set_peer_limit`
    /// (the fork's runtime-adjustable live-peer cap: it hangs up on the
    /// surplus, least useful first, and re-queues the parked peers when the
    /// cap goes back up) plus, going lean, its
    /// `TorrentStateLive::forget_disconnected_peers` -- called *before* the
    /// cap is lowered, so the addresses it drops are the dead and
    /// not-needed ones the table had accumulated, not the peers the lower
    /// cap is about to park, which a return to `Full` wants to re-dial. A
    /// torrent that is not live gets the cap stored for when it is.
    ///
    /// Synchronous and cheap: atomics, one read lock per torrent, and a
    /// `Disconnect` message per surplus peer; the peers hang up on their
    /// own tasks afterwards. Nothing is awaited.
    fn set_footprint(&self, footprint: Footprint) {
        let mut caps = self.caps.lock();
        if caps.footprint == footprint {
            // Already there. Not only a shortcut: a second `Lean` would
            // prune the peers the first one parked, and the return to
            // `Full` would then have nobody to re-dial.
            return;
        }
        caps.footprint = footprint;
        let torrents = self.apply_caps_to_every_torrent(*caps);
        info!(
            ?footprint,
            peer_limit = caps.limit(),
            torrents,
            "torrent session footprint changed"
        );
    }

    fn footprint(&self) -> Footprint {
        self.caps.lock().footprint
    }

    /// librqbit's `DhtStats` is instantaneous (`routing_table_size`,
    /// `routing_table_size_v6`) and has no "did bootstrap ever succeed"
    /// flag, so the sticky bit is latched here: any observation of a
    /// non-empty routing table sets it for the rest of the session. Cheap --
    /// two routing-table length reads -- so a poller may call it freely.
    fn dht_status(&self) -> DhtStatus {
        let Some(dht) = self.session.get_dht() else {
            return DhtStatus::default();
        };
        let stats = dht.stats();
        let nodes = stats.routing_table_size as u64;
        let nodes_v6 = stats.routing_table_size_v6 as u64;
        if nodes + nodes_v6 > 0 {
            self.dht_ever_bootstrapped.store(true, Ordering::Relaxed);
        }
        DhtStatus {
            enabled: true,
            nodes,
            nodes_v6,
            ever_bootstrapped: self.dht_ever_bootstrapped.load(Ordering::Relaxed),
        }
    }
}

#[async_trait::async_trait]
impl TorrentHandle for LibrqbitHandle {
    fn info_hash(&self) -> String {
        self.handle.info_hash().as_string()
    }

    fn name(&self) -> Option<String> {
        self.handle
            .metadata
            .load_full()
            .and_then(|m| m.info.name().map(|n| n.to_string()))
    }

    /// Two counters off the live state, read through librqbit's
    /// `stats_snapshot()` -- which loads its handful of torrent-level
    /// atomics and the aggregate peer counters, because
    /// `TorrentStateLive::stats` is private at the pinned rev; cheap and
    /// side-effect free, but a snapshot, not two loads. Zero in every other
    /// state, because the live state is where librqbit keeps them. Not
    /// `progress_bytes`: while a torrent initializes that mirrors the hash
    /// check's `checked_bytes`, which is disk read back, not a peer.
    fn transfer_totals(&self) -> TransferTotals {
        self.handle.with_state(|state| match state {
            ManagedTorrentState::Live(live) => {
                let snapshot = live.stats_snapshot();
                TransferTotals {
                    fetched: snapshot.fetched_bytes,
                    uploaded: snapshot.uploaded_bytes,
                }
            }
            _ => TransferTotals::default(),
        })
    }

    async fn stats(&self) -> EngineStats {
        let stats = self.handle.stats();
        // CAUTION: librqbit's Speed.mbps is MiB/s, NOT megabits/s
        // (librqbit-core speed_estimator.rs: mbps() = bps()/1024/1024), so the
        // conversion to bytes/s is a plain * 1 MiB with no /8.
        let (download_speed, upload_speed) = if let Some(ref live) = stats.live {
            (
                live.download_speed.mbps * 1_048_576.0,
                live.upload_speed.mbps * 1_048_576.0,
            )
        } else {
            (0.0, 0.0)
        };

        // progress_bytes is persisted have-bytes (survives restarts), matching
        // libtorrent's total_done semantics; fetched_bytes resets each session.
        let downloaded = stats.progress_bytes;
        let uploaded = stats.uploaded_bytes;

        let peer_discovery = stats
            .live
            .as_ref()
            .map(|l| {
                let p = &l.snapshot.peer_stats;
                PeerDiscovery {
                    seen: p.seen as u64,
                    queued: p.queued as u64,
                    connecting: p.connecting as u64,
                    live: p.live as u64,
                    // Every state a table entry can be in; librqbit keeps
                    // a transition counter per state, so this is the
                    // table's length without walking it.
                    known: (p.queued + p.connecting + p.live + p.dead + p.not_needed) as u64,
                }
            })
            .unwrap_or_default();
        let (peers, queued, unique) = (
            peer_discovery.live,
            peer_discovery.queued,
            peer_discovery.seen,
        );
        // Connected peers whose bitfield covers the whole torrent. librqbit
        // maintains this as a transition counter next to the live/live_tcp/
        // live_utp/live_socks aggregates, so reading it is O(1) and needs no
        // walk over per-peer bitfields. It is bounded by `peers`: only live
        // peers are counted, never ones we merely know an address for, and
        // never a tracker's seeder count (we do not scrape).
        let connected_seeders = stats
            .live
            .as_ref()
            .map(|l| l.snapshot.peer_stats.live_seeders as u64)
            .unwrap_or(0);

        let has_metadata = self.handle.metadata.load().is_some();
        let phase = startup_phase(has_metadata, &stats.state, stats.finished);
        // Hash-check progress straight from the Initializing state (the same
        // counter librqbit mirrors into `progress_bytes` while initializing).
        let checked_bytes = match phase {
            StartupPhase::Checking => self.handle.with_state(|state| match state {
                ManagedTorrentState::Initializing(init) => Some(init.get_checked_bytes()),
                _ => None,
            }),
            _ => None,
        };
        let check_total_bytes = checked_bytes.map(|_| stats.total_bytes);
        // The verified-piece bitfield only exists once the chunk tracker does
        // (Paused/Live); `api_dump_haves` is the one public accessor for it.
        let haves = match phase {
            StartupPhase::Buffering | StartupPhase::Ready => {
                librqbit::Api::new(self.session.clone(), None)
                    .api_dump_haves(librqbit::api::TorrentIdOrHash::Hash(
                        self.handle.info_hash(),
                    ))
                    .ok()
                    .map(|(bitfield, _pieces)| bitfield)
            }
            _ => None,
        };
        // The startup window is the same for every buffer profile (see
        // `BufferProfile`), so the reported readiness is too.
        let startup_window = crate::backend::priorities::librqbit_stream_lookahead_bytes(
            crate::backend::priorities::PlaybackIntent::DirectInitial,
            crate::backend::priorities::BufferProfile::Normal,
        );

        let pinned = self.pinned_set();
        let positions = self.stream_positions.lock().clone();
        let mut torrent_piece_length = None;
        let mut files = Vec::new();
        let mut total_size = 0u64;
        let mut offset = 0u64;
        if let Some(m) = self.handle.metadata.load_full() {
            let lengths = *m.lengths();
            let piece_length = lengths.default_piece_length() as u64;
            torrent_piece_length = Some(piece_length);
            // Chunk size straight from librqbit's own arithmetic rather than
            // a copy of its 16 KiB constant: a full piece is split into
            // `default_chunks_per_piece` equal chunks. Only the *last* chunk
            // of the torrent's last piece is short, which
            // `InFlightPiece::from_chunks` handles by clamping.
            let chunk_size = match lengths.default_chunks_per_piece() as u64 {
                0 => None,
                chunks => Some(piece_length / chunks),
            };
            for (i, f) in m.info.iter_file_details().enumerate() {
                let filename = f.filename.to_string();
                // file_progress is empty while the torrent is Initializing.
                let have = stats.file_progress.get(i).copied().unwrap_or(0);
                let (file_downloaded, file_progress) = file_progress_fields(f.len, have);
                // Complete = every byte verified; `have` is clamped to the
                // length above, so equality is the whole test. A torrent
                // without a piece map yet (empty file_progress) reports 0
                // and so never claims completion of a non-empty file.
                let complete = file_downloaded == f.len;
                let read_from = positions.get(&(self.info_hash.clone(), i)).copied();
                let window = haves.as_ref().map(|bf| {
                    crate::backend::priorities::initial_window_progress(
                        offset,
                        f.len,
                        piece_length,
                        startup_window,
                        read_from.unwrap_or(0),
                        |piece| bf.get(piece as usize).is_some_and(|bit| *bit),
                    )
                });
                // Sub-piece progress for the piece this file's reader is
                // waiting on. Only for a file somebody has actually opened
                // (`read_from`): otherwise there is no reader, nothing is in
                // flight for it, and absence is the honest answer -- and it
                // keeps the per-poll cost at one cheap bit-count per open
                // stream instead of one per file of the torrent.
                let in_flight_piece = read_from.zip(chunk_size).and_then(|(from, chunk_size)| {
                    let index = crate::backend::priorities::reader_piece_index(
                        offset,
                        f.len,
                        piece_length,
                        from,
                    )?;
                    let valid = lengths.validate_piece_index(u32::try_from(index).ok()?)?;
                    // Errors when the torrent has no chunk tracker (neither
                    // live nor paused), which is exactly when we must report
                    // absence rather than a zeroed piece.
                    let progress = self.handle.piece_chunk_progress(valid.get()).ok()?;
                    Some(crate::backend::InFlightPiece::from_chunks(
                        index,
                        progress.downloaded_chunks,
                        chunk_size,
                        lengths.piece_length(valid) as u64,
                        progress.verified,
                    ))
                });
                files.push(StatsFile {
                    name: filename.clone(),
                    path: filename,
                    length: f.len,
                    offset,
                    downloaded: file_downloaded,
                    progress: file_progress,
                    initial_window_ready_bytes: window.map(|(ready, _)| ready),
                    initial_window_bytes: window.map(|(_, total)| total),
                    in_flight_piece,
                    pinned: pinned.contains(&i),
                    complete,
                });
                total_size += f.len;
                offset += f.len;
            }
        }

        // server.js lists the torrent's peer sources here; we report the
        // tracker set the torrent was added with (fixed for its lifetime, see
        // `add_trackers`) so clients can verify which trackers reached the
        // engine. librqbit exposes no per-tracker announce bookkeeping, so the
        // counters stay 0 and `lastStarted` empty.
        let mut sources: Vec<Source> = self
            .handle
            .shared()
            .trackers
            .iter()
            .map(|url| Source {
                url: url.to_string(),
                ..Source::default()
            })
            .collect();
        sources.sort_by(|a, b| a.url.cmp(&b.url));

        // Swarm-wide counts, which is a different question from
        // `connected_seeders`: what the trackers say about everybody, not
        // what our own connections show. Cached and rate-limited by the
        // scraper -- this call never waits on the network, because players
        // poll stats.json about once a second.
        //
        // A torrent whose metadata has not arrived cannot be shown to be
        // public, so it counts as private and is left alone: an unsolicited
        // scrape is exactly what private trackers ban accounts over.
        let private = self
            .handle
            .with_metadata(|m| m.info.info().private)
            .unwrap_or(true);
        let tracker_urls: Vec<String> = sources.iter().map(|s| s.url.clone()).collect();
        let swarm = self.swarm_scraper.snapshot(
            &self.info_hash,
            self.handle.info_hash().0,
            &tracker_urls,
            private,
        );
        for source in &mut sources {
            if let Some(counts) = swarm.per_tracker.get(&source.url) {
                source.seeders = Some(counts.seeders);
                source.leechers = Some(counts.leechers);
                source.completed = Some(counts.completed);
            }
        }

        EngineStats {
            name: self.name().unwrap_or_else(|| "Unknown".to_string()),
            info_hash: self.info_hash(),
            piece_length: torrent_piece_length,
            // Torrent-level stats describe no particular file; a client
            // asking about one gets it through `focus_stream_file`.
            in_flight_piece: None,
            files,
            sources,
            opts: StatsOptions {
                dht: true,
                tracker: true,
                path: "".to_string(),
                growler: Growler {
                    flood: 0,
                    pulse: None,
                },
                peer_search: PeerSearch {
                    max: 100,
                    min: 10,
                    sources: vec![],
                },
                swarm_cap: SwarmCap {
                    max_speed: None,
                    min_peers: None,
                },
                connections: None,
                handshake_timeout: None,
                timeout: None,
                r#virtual: false,
            },
            download_speed,
            upload_speed,
            downloaded,
            uploaded,
            peers,
            unchoked: peers,
            queued,
            unique,
            connection_tries: 0,
            peer_search_running: true,
            stream_len: total_size,
            stream_name: "".to_string(),
            stream_progress: if stats.total_bytes > 0 {
                stats.progress_bytes as f64 / stats.total_bytes as f64
            } else {
                0.0
            },
            swarm_connections: peers,
            // The state machine's answer, not a constant. This is the one
            // field of the server.js-compatible shape that says whether the
            // torrent is running, and it read `false` for every torrent
            // there has ever been -- including a torrent stopped for want
            // of disk space, and including one this process restored
            // stopped and has not started yet. A client polling stats had
            // no way to tell a torrent that is fetching from one that is
            // not. Read from `run_state` like every other question about
            // what a torrent is doing, never from `is_paused()`.
            swarm_paused: self.run_state() == RunState::Paused,
            swarm_size: peers,
            connected_seeders,
            swarm_seeders: swarm.seeders,
            swarm_leechers: swarm.leechers,
            swarm_scrape_age_secs: swarm.age_secs,
            is_finished: stats.finished,
            has_metadata,
            phase,
            checked_bytes,
            check_total_bytes,
            initial_window_ready_bytes: None,
            initial_window_bytes: None,
            peer_discovery,
            error: self.client_torrent_error(stats.error.as_deref()),
            pinned_files: pinned.into_iter().collect(),
        }
    }

    /// Cheap: librqbit's TorrentStats.finished is precomputed from the chunk
    /// tracker's have/needed counters (no per-piece walk here).
    async fn is_finished(&self) -> bool {
        self.handle.stats().finished
    }

    /// One `ArcSwap` load of the slot librqbit fills when a torrent's info
    /// dictionary arrives (`ManagedTorrent::metadata`), which is the same
    /// thing `stats()` reports as `has_metadata` -- and, unlike `stats()`,
    /// it neither walks the files nor copies the piece bitfield.
    async fn has_metadata(&self) -> bool {
        self.handle.metadata.load().is_some()
    }

    /// Per-file completion from chunk-tracker have-bytes. `file_progress` is
    /// empty while the torrent is still Initializing, in which case the file
    /// is reported incomplete.
    async fn is_file_complete(&self, file_idx: usize) -> bool {
        let stats = self.handle.stats();
        let Some(have) = stats.file_progress.get(file_idx).copied() else {
            return false;
        };
        let Some(len) = self
            .handle
            .metadata
            .load_full()
            .and_then(|m| m.file_infos.get(file_idx).map(|fi| fi.len))
        else {
            return false;
        };
        have >= len
    }

    /// Whether the backend stopped this torrent because the volume ran out
    /// of space, which the cache cleaner treats as a signal to evict rather
    /// than as a dead torrent. The free `is_out_of_space` above is what
    /// tells that error apart from every other fatal one, and says how the
    /// needles were measured.
    ///
    /// Reads the state librqbit already holds behind one lock: no stats
    /// rebuild, no syscall, cheap enough for the cleaner to ask on a timer.
    async fn is_out_of_space(&self) -> bool {
        self.handle.with_state(|state| match state {
            ManagedTorrentState::Error(error) => is_out_of_space(error),
            _ => false,
        })
    }

    /// librqbit's `Error` state, whatever put it there: an ENOSPC write, a
    /// storage that failed its check, a write past what the platform's
    /// `off_t` can address. The one lock read `is_out_of_space` does, minus
    /// the classification.
    async fn is_in_error_state(&self) -> bool {
        self.handle
            .with_state(|state| matches!(state, ManagedTorrentState::Error(_)))
    }

    /// librqbit's `ManagedTorrentState` (one `parking_lot` read through
    /// `ManagedTorrent::with_state`, `torrent_state/mod.rs:317`), plus the
    /// `paused` flag (`ManagedTorrent::is_paused`, `:662`) for the one
    /// variant that carries a pause intent. No stats rebuild, no per-file
    /// walk, no syscall.
    ///
    /// `ManagedTorrentState::None` becomes [`RunState::Gone`]: librqbit calls
    /// it a bug state the outside world should never see, and normally it is
    /// invisible because every swap through it happens under one write guard
    /// -- except in `_start`'s `Paused` arm, which takes the state out and
    /// then does `TorrentStateLive::new(..)?` (`mod.rs:610-612`), so a live
    /// state that fails to build leaves the torrent empty for good. A torrent
    /// like that is neither running nor restartable, which is what `Gone`
    /// says; reporting it as `Live` would have a caller believe it is
    /// downloading.
    fn run_state(&self) -> RunState {
        // Two separate reads, never nested: both go to the same
        // `parking_lot::RwLock`, which is write-preferring, so taking the
        // second inside the first would deadlock the moment a writer queued
        // between them. The cost is that the pair can be torn -- a pause
        // landing between them reports `pause_requested: false` once -- which
        // is why nothing may conclude anything lasting from an
        // `Initializing` reading: it is not a settled state either way, and
        // the next poll sees the pause.
        let initializing = self.handle.with_state(|state| match state {
            ManagedTorrentState::Live(_) => Some(RunState::Live),
            ManagedTorrentState::Paused(_) => Some(RunState::Paused),
            ManagedTorrentState::Error(_) => Some(RunState::Error),
            ManagedTorrentState::None => Some(RunState::Gone),
            ManagedTorrentState::Initializing(_) => None,
        });
        match initializing {
            Some(settled) => settled,
            None => RunState::Initializing {
                pause_requested: self.handle.is_paused(),
            },
        }
    }

    /// `Session::unpause` -> `ManagedTorrent::start`, whose `Error(_)` arm
    /// rebuilds the storage, re-checks what is on disk and goes live again
    /// (librqbit e314d8b, `torrent_state/mod.rs`). The re-check is what makes
    /// this safe after an eviction took files out from under the torrent: it
    /// discovers what is actually there rather than trusting the piece map it
    /// died with.
    async fn restart_from_error(&self) -> Result<()> {
        self.session.unpause(&self.handle).await
    }

    /// `Session::pause` -> `ManagedTorrent::pause`: a live torrent's state
    /// becomes `Paused`, which drops its peers and its pending writes and
    /// keeps its files and piece map -- and which `Session::unpause` (our
    /// [`Self::start_torrent`]) takes straight back to live, no re-check,
    /// so nothing may touch those files while it is paused. Errs on a
    /// torrent already paused or in the error state, in librqbit's words.
    ///
    /// Nothing is recorded here: this is the reconciler's stop and the
    /// reconciler keeps no note of what it stopped.
    async fn stop_torrent(&self) -> Result<()> {
        self.session.pause(&self.handle).await
    }

    /// `Session::unpause`, unconditionally: the `Paused(_)` arm builds the
    /// live state and clears the persisted flag, with no re-check and no
    /// progress lost. Errs on a torrent that is not paused.
    ///
    /// Nothing is recorded here either, and there is nowhere left to
    /// record it: this backend keeps no note of which pauses were whose,
    /// because the caller recomputes that from live conditions on every
    /// pass (`crate::reconcile::desired`).
    async fn start_torrent(&self) -> Result<()> {
        self.session.unpause(&self.handle).await
    }

    /// Deliberate no-op: librqbit (zond/rqbit `feat/configurable-stream-lookahead`)
    /// has no API to add trackers to a torrent that is already managed. The
    /// tracker set lives in `ManagedTorrentShared::trackers`, a plain
    /// `HashSet<Url>` with no interior mutability, and `Session::make_peer_rx`
    /// hands `TrackerComms::start` a one-shot snapshot of it when the torrent
    /// goes live; `TrackerComms::add_tracker` is private startup plumbing. The
    /// only way to change a torrent's trackers is to remove and re-add it,
    /// which would drop its peers and piece state mid-stream. So trackers must
    /// be supplied to `add_torrent` by whichever request creates the engine
    /// (see `routes::compat::get_or_create_engine` in the server crate), and
    /// `stats().sources` reports the set that was actually used.
    async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
        Ok(())
    }

    async fn get_file_reader(
        &self,
        file_idx: usize,
        start_offset: u64,
        _priority: u8,
        _bitrate: Option<u64>,
        intent: crate::backend::priorities::PlaybackIntent,
        buffer: crate::backend::priorities::BufferProfile,
    ) -> Result<Box<dyn FileStreamTrait>> {
        // Where the startup window is measured from now on. A `Range`
        // request, a seek and a re-open all reach the backend as a fresh
        // reader at the new offset, so this is the whole of "follow the
        // reader" (see [`StreamPositions`]).
        self.stream_positions
            .lock()
            .insert((self.info_hash.clone(), file_idx), start_offset);
        // librqbit's FileStream requires the Paused or Live state; opening it
        // while the torrent is still Initializing fails immediately, which the
        // HTTP route would turn into a failed first play. Block here instead.
        self.await_initialized().await?;
        // Size the per-stream lookahead window by playback intent instead of
        // librqbit's fixed 32 MiB default: a narrow startup window verifies the
        // head pieces faster, while seeks/sequential get generous read-ahead.
        let opts = librqbit::FileStreamOptions {
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(
                intent, buffer,
            ),
        };
        let stream = self
            .handle
            .clone()
            .stream_with_options(file_idx, opts)
            .await
            .context("Failed to stream from librqbit")?;
        Ok(Box::new(stream))
    }

    async fn get_files(&self) -> Vec<BackendFileInfo> {
        let mut files = Vec::new();
        if let Some(m) = self.handle.metadata.load_full() {
            for f in m.info.iter_file_details() {
                files.push(BackendFileInfo {
                    name: f.filename.to_string(),
                    length: f.len,
                });
            }
        }
        files
    }

    /// The torrent's resolved output folder (`ManagedTorrent::output_folder`,
    /// public since librqbit 9) joined with the file's relative name from
    /// the metadata's `file_infos` -- exactly where librqbit's storage
    /// writes it. `None` while a magnet is still resolving or for a bad
    /// index. This is for handing a *complete* file to a local player;
    /// reads of an in-progress file keep going through the FileStream,
    /// which blocks on missing pieces where a sparse file would not.
    async fn file_path(&self, file_idx: usize) -> Option<PathBuf> {
        let metadata = self.handle.metadata.load_full()?;
        let file = metadata.file_infos.get(file_idx)?;
        Some(self.handle.output_folder().join(&file.relative_filename))
    }

    /// librqbit's resolved `output_folder` for the torrent: the placement's
    /// folder when one was given, else the session root (single-file
    /// torrents) or `<root>/<torrent name>` (multi-file).
    fn output_folder(&self) -> Option<PathBuf> {
        Some(self.handle.output_folder().to_path_buf())
    }

    fn piece_length(&self) -> Option<u64> {
        self.handle
            .metadata
            .load_full()
            .map(|m| m.lengths().default_piece_length() as u64)
    }

    /// Select `file_idx` as the only wanted file (exclusive downloading, per
    /// the trait contract) on multi-file torrents. Blocks (bounded) while the
    /// torrent is still Initializing -- librqbit refuses selection updates in
    /// that state, and this runs right before the reader is opened, which has
    /// to wait anyway. Other selection failures are best-effort: logged and
    /// swallowed, since playback still works with the selection unchanged.
    /// Err is returned for a provably-bad file index or when the torrent never
    /// becomes ready (`TorrentInitError`).
    ///
    /// librqbit persists only_files across restarts; the next prepare or
    /// reconcile simply rewrites it.
    async fn prepare_file_for_streaming(&self, file_idx: usize) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            warn!(
                info_hash = %self.info_hash,
                file_idx,
                "prepare_file_for_streaming: metadata not resolved; skipping file gating"
            );
            return Ok(());
        };
        if file_idx >= file_count {
            anyhow::bail!("File index {file_idx} out of range ({file_count} files)");
        }
        self.apply_selection(
            SelectionOp::Prepare(file_idx),
            file_count,
            "prepare_file_for_streaming",
            InitPolicy::Wait,
        )
        .await
    }

    // keep_file_downloading stays the default-equivalent no-op: its only call
    // site (lib.rs activate_file) is guarded by !is_multifile, where the whole
    // torrent is a single file and therefore always wanted.
    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    /// Deselect `file_idx`, keeping the rest of the current selection. Called
    /// from delayed cleanup and HLS-lease expiry, possibly AFTER a newer file
    /// was prepared -- the planner refuses to clear a file that is no longer
    /// selected and refuses to produce an empty want-set, so stale cleanups
    /// can never clobber the active selection.
    async fn clear_file_streaming(&self, file_idx: usize) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Clear(file_idx),
            file_count,
            "clear_file_streaming",
            InitPolicy::Skip,
        )
        .await
    }

    /// Pin `file_idx` (see the trait doc): record it in the backend-wide pin
    /// set, which every later planner run unions in, and add it to the
    /// current selection right away. Deferred like `reconcile_file_priorities`
    /// while the torrent is Initializing -- the pin is already recorded, so a
    /// prepare/reconcile that lands first carries it anyway. Err only for a
    /// provably-bad index; other selection failures are best-effort.
    async fn pin_file(&self, file_idx: usize) -> Result<()> {
        let file_count = self.file_count_from_metadata();
        if let Some(file_count) = file_count
            && file_idx >= file_count
        {
            anyhow::bail!("File index {file_idx} out of range ({file_count} files)");
        }
        self.pinned_files
            .lock()
            .entry(self.info_hash.clone())
            .or_default()
            .insert(file_idx);
        let Some(file_count) = file_count else {
            // Metadata still resolving: the pin is recorded and the first
            // selection update after resolution applies it.
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Pin(file_idx),
            file_count,
            "pin_file",
            InitPolicy::Defer,
        )
        .await
    }

    /// Forget the pin (see the trait doc). The selection is left alone: the
    /// file may be the one currently streaming, and only the engine layer
    /// knows what should stay wanted -- it reconciles right after.
    async fn unpin_file(&self, file_idx: usize) -> Result<()> {
        let mut pinned = self.pinned_files.lock();
        if let Some(set) = pinned.get_mut(&self.info_hash) {
            set.remove(&file_idx);
            if set.is_empty() {
                pinned.remove(&self.info_hash);
            }
        }
        Ok(())
    }

    /// `ManagedTorrent::drop_pieces` over the file's piece range (see the
    /// trait doc). librqbit clears the have-bits, stops advertising the
    /// pieces and stops wanting them, and hands back the claim that keeps
    /// them unwanted until the caller has deleted the bytes.
    ///
    /// librqbit skips two kinds of piece on its own: ones it does not have
    /// (nothing to forget) and ones a live stream's lookahead is about to
    /// read (dropping those would only re-request them at once; a reader
    /// still on the file being deleted gets the read error it was going to
    /// get anyway). A boundary piece the neighbouring file shares is
    /// dropped too -- half of its bytes are about to go, so its have-bit
    /// would be a lie -- and comes back through the neighbour's selection
    /// once the claim is released.
    ///
    /// Two states refuse. A torrent that is still hash-checking (or stopped
    /// with an error) has no have-set to edit. And a torrent added without
    /// `piece_reclaim` -- which is every torrent on the shipped session,
    /// whose filesystem storage cannot release a single piece, so
    /// `add_torrent_placed` never sets the option (see
    /// [`LibrqbitBackend`]'s `piece_reclaim` field) -- has librqbit answer
    /// `PieceReclaimDisabled` however long it has been running. Both are
    /// reported, not hidden: the caller deletes the bytes regardless, and
    /// until the next restart librqbit believes it has them. The restart
    /// heals it -- the fastresume validation hash-checks at least one
    /// claimed piece of every file, the deleted file reads back empty, and
    /// the whole torrent is re-checked from disk -- and the pieces stay out
    /// of the want-set because `only_files` is persisted without the file.
    async fn drop_file_pieces(&self, file_idx: usize) -> Result<Option<DroppedFilePieces>> {
        let range = self
            .handle
            .with_metadata(|m| m.file_infos.get(file_idx).map(|f| f.piece_range.clone()))
            .context("torrent has no metadata to name the file's pieces")?
            .with_context(|| format!("file index {file_idx} out of range"))?;
        let dropped = match self.handle.drop_pieces(range.clone()) {
            Ok(dropped) => dropped,
            Err(e)
                if e.downcast_ref::<librqbit::Error>()
                    .is_some_and(|e| matches!(e, librqbit::Error::PieceReclaimDisabled)) =>
            {
                return Err(e.context(
                    "this torrent was added without piece reclaim -- the session's storage \
                     cannot release a single piece, so AddTorrentOptions::piece_reclaim is \
                     off -- and librqbit will not forget a piece it has before the next restart",
                ));
            }
            Err(e) => return Err(e.context("librqbit could not forget the file's pieces")),
        };
        let pieces = dropped.pieces().to_vec();
        debug!(
            info_hash = %self.info_hash,
            file_idx,
            pieces = pieces.len(),
            "dropped the file's pieces from the have-set"
        );
        Ok(Some(DroppedFilePieces::new(
            pieces,
            ReleaseThenReselect {
                dropped: Some(dropped),
                torrent: self.handle.clone(),
                range,
            },
        )))
    }

    /// The engine's primary multi-file switching hook
    /// (reconcile_multifile_engine in lib.rs): want exactly the union of the
    /// active and hot files. While the torrent is Initializing the update is
    /// deferred (latest wins) and applied as soon as librqbit accepts it, so
    /// first-play gating takes effect instead of being silently dropped; the
    /// call itself returns immediately because it is also driven from
    /// background cleanup loops.
    async fn reconcile_file_priorities(&self, plan: TorrentFilePriorityPlan) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Reconcile {
                active: plan.active_file,
                hot: plan.hot_file.map(|h| h.file_idx),
            },
            file_count,
            "reconcile_file_priorities",
            InitPolicy::Defer,
        )
        .await
    }

    /// Block until the piece covering `offset` of `file_idx` is readable, or
    /// the timeout elapses. Mirrors the libtorrent behavioral contract:
    /// timeouts and soft conditions return Ok(ready: false, reason), Err is
    /// reserved for structural failures (bad file index).
    ///
    /// Mechanism: open a short-lived librqbit FileStream and seek to `offset`.
    /// Registering the stream moves librqbit's per-stream lookahead window
    /// (sized by `intent` and `buffer` via `librqbit_stream_lookahead_bytes`,
    /// matching the
    /// window the real read will request) to that offset and reconnects
    /// not-needed peers --
    /// the deadline-equivalent priority yank -- and the subsequent 1-byte read
    /// parks on the piece waker until the piece covering `offset` verifies.
    /// The temporary stream drops at function exit, deregistering its window.
    async fn wait_for_piece_ready(
        &self,
        file_idx: usize,
        offset: u64,
        timeout: Duration,
        intent: crate::backend::priorities::PlaybackIntent,
        buffer: crate::backend::priorities::BufferProfile,
    ) -> Result<PieceReadiness> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let start = std::time::Instant::now();
        debug!(
            info_hash = %self.info_hash,
            file_idx,
            offset,
            timeout_ms = timeout.as_millis() as u64,
            ?intent,
            "wait_for_piece_ready: begin"
        );

        // Phase 1: wait for the torrent to leave Initializing (FileStream
        // requires Paused or Live). Bounded by the caller's timeout and the
        // global gate; a soft failure keeps the libtorrent-style contract.
        if let Err(e) = await_initialized(
            &self.info_hash,
            timeout.min(TORRENT_INIT_TIMEOUT),
            self.handle.wait_until_initialized(),
        )
        .await
        {
            let reason = match e {
                TorrentInitError::TimedOut { .. } => "initializing-timeout".to_string(),
                TorrentInitError::Failed { reason, .. } => format!("init-failed: {reason}"),
            };
            return Ok(self.readiness(start, false, -1, 0, 1, reason));
        }
        // Metadata is resolved before librqbit creates the ManagedTorrent, so
        // this is purely defensive.
        let Some(metadata) = self.handle.metadata.load_full() else {
            return Ok(self.readiness(start, false, -1, 0, 1, "no-metadata".to_string()));
        };

        let fi = metadata
            .file_infos
            .get(file_idx)
            .with_context(|| format!("File index {file_idx} out of range"))?;
        let piece_length = metadata.lengths().default_piece_length() as u64;
        let piece = ((fi.offset_in_torrent + offset) / piece_length) as i32;
        if fi.len > 0 && offset >= fi.len {
            return Ok(self.readiness(
                start,
                false,
                piece,
                0,
                1,
                "piece-out-of-file-range".to_string(),
            ));
        }

        // Phase 2: open the stream. Use the same intent-sized window as the
        // real read so the priority yank moves the lookahead exactly where
        // playback will request it.
        let lookahead = librqbit::FileStreamOptions {
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(
                intent, buffer,
            ),
        };
        let mut stream = match self
            .handle
            .clone()
            .stream_with_options(file_idx, lookahead)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Ok(self.readiness(
                    start,
                    false,
                    piece,
                    0,
                    1,
                    format!("stream-unavailable: {e:#}"),
                ));
            }
        };

        // Seek completes synchronously for FileStream (poll_complete is
        // always Ready) and also moves the shared per-stream window.
        if let Err(e) = stream.seek(std::io::SeekFrom::Start(offset)).await {
            return Ok(self.readiness(start, false, piece, 0, 1, format!("seek-error: {e}")));
        }

        // Phase 3: 1-byte read bounded by the remaining timeout. A successful
        // read of 0 bytes is EOF at the exact file end, which still means the
        // requested position is servable.
        let remaining = timeout.saturating_sub(start.elapsed());
        let mut buf = [0u8; 1];
        let result = match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(_n)) => self.readiness(start, true, piece, 1, 1, "stream-read".to_string()),
            Ok(Err(e)) => self.readiness(start, false, piece, 0, 1, format!("read-error: {e}")),
            Err(_) => self.readiness(start, false, piece, 0, 1, "timeout".to_string()),
        };
        debug!(
            info_hash = %self.info_hash,
            file_idx,
            offset,
            ready = result.ready,
            reason = %result.reason,
            elapsed_ms = result.elapsed_ms,
            "wait_for_piece_ready: end"
        );
        Ok(result)
    }
}

/// Map librqbit's torrent state onto the client-facing startup phase.
/// `Initializing` is the on-disk hash check; `Live`/`Paused` both have a piece
/// map and are `Ready` only when the whole torrent is finished -- otherwise
/// `Buffering` until [`EngineStats::focus_stream_file`] judges the stream
/// file's initial window. Missing metadata wins over everything (a resolving
/// magnet has no pieces to check or buffer).
fn startup_phase(
    has_metadata: bool,
    state: &librqbit::TorrentStatsState,
    finished: bool,
) -> StartupPhase {
    use librqbit::TorrentStatsState as S;
    if !has_metadata {
        return StartupPhase::ResolvingMetadata;
    }
    match state {
        S::Initializing { .. } => StartupPhase::Checking,
        S::Live | S::Paused if finished => StartupPhase::Ready,
        S::Live | S::Paused => StartupPhase::Buffering,
        S::Error => StartupPhase::Error,
    }
}

impl LibrqbitHandle {
    /// File count from resolved metadata; None while a magnet is resolving.
    fn file_count_from_metadata(&self) -> Option<usize> {
        self.handle.metadata.load_full().map(|m| m.file_infos.len())
    }

    /// What a client may be told about librqbit's `TorrentStats.error`:
    /// the fixed [`TORRENT_ERROR_MESSAGE`], never the error itself, which
    /// is the `{e:?}` of an anyhow chain naming absolute cache and
    /// download paths (`PinDownloadError::client_message` draws the same
    /// line). The chain goes to the log instead -- once per distinct
    /// error, since statistics are polled for as long as a broken download
    /// is on screen. `None` in, `None` out, and the record is dropped so a
    /// torrent that recovers reports its next error again.
    fn client_torrent_error(&self, error: Option<&str>) -> Option<String> {
        let mut reported = self.reported_errors.lock();
        let Some(error) = error else {
            reported.remove(&self.info_hash);
            return None;
        };
        if reported.get(&self.info_hash).map(String::as_str) != Some(error) {
            warn!(info_hash = %self.info_hash, error, "torrent_error_state");
            reported.insert(self.info_hash.clone(), error.to_string());
        }
        Some(TORRENT_ERROR_MESSAGE.to_string())
    }

    /// Snapshot of this torrent's pinned file indices.
    fn pinned_set(&self) -> BTreeSet<usize> {
        self.pinned_files
            .lock()
            .get(&self.info_hash)
            .cloned()
            .unwrap_or_default()
    }

    /// True for any torrent state that must go through the init gate rather
    /// than take the fast path: literally `Initializing` (opening/hash-
    /// checking files, where `update_only_files` bails and `FileStream`
    /// cannot be opened), `Error` (already failed -- routing it through
    /// `wait_until_initialized` turns librqbit's raw error into
    /// `TorrentInitError::Failed` instead of letting a `FileStream` open
    /// attempt fail downstream with a bare 500), and the transient `None`
    /// state (only visible mid state-swap; librqbit itself treats seeing it
    /// as a bug, so gating it here is purely defensive, not load-bearing).
    fn is_initializing(&self) -> bool {
        self.handle.with_state(|s| {
            matches!(
                s,
                ManagedTorrentState::Initializing(_)
                    | ManagedTorrentState::Error(_)
                    | ManagedTorrentState::None
            )
        })
    }

    /// Block until the torrent leaves the Initializing state, bounded by
    /// `TORRENT_INIT_TIMEOUT`. Cheap fast path when already initialized.
    async fn await_initialized(&self) -> std::result::Result<(), TorrentInitError> {
        if !self.is_initializing() {
            return Ok(());
        }
        debug!(
            info_hash = %self.info_hash,
            "torrent is initializing; holding the request until it is ready"
        );
        await_initialized(
            &self.info_hash,
            TORRENT_INIT_TIMEOUT,
            self.handle.wait_until_initialized(),
        )
        .await
    }

    /// Owned `'static` gate future for a spawned deferred-selection waiter.
    fn init_gate_future(
        &self,
    ) -> impl Future<Output = std::result::Result<(), TorrentInitError>> + Send + 'static {
        let handle = self.handle.clone();
        let info_hash = self.info_hash.clone();
        async move {
            await_initialized(
                &info_hash,
                TORRENT_INIT_TIMEOUT,
                handle.wait_until_initialized(),
            )
            .await
        }
    }

    fn deferred_selection(&self) -> Arc<DeferredSelection<DeferredOp>> {
        self.deferred_selections
            .lock()
            .entry(self.info_hash.clone())
            .or_insert_with(DeferredSelection::new)
            .clone()
    }

    /// Look up this torrent's deferred-selection slot without creating one --
    /// a torrent that never deferred anything should not get a map entry just
    /// because a direct apply checked whether there was something to
    /// supersede.
    fn deferred_selection_if_present(&self) -> Option<Arc<DeferredSelection<DeferredOp>>> {
        self.deferred_selections
            .lock()
            .get(&self.info_hash)
            .cloned()
    }

    /// Park `op` until the torrent initializes (see `DeferredSelection`).
    fn defer_selection(&self, op: SelectionOp, context: &'static str) {
        debug!(
            info_hash = %self.info_hash,
            ?op,
            context,
            "torrent is initializing; deferring file selection until it is ready"
        );
        let applier = self.clone();
        self.deferred_selection().defer(
            DeferredOp { op, context },
            self.init_gate_future(),
            move |deferred: DeferredOp| {
                let handle = applier.clone();
                async move {
                    let Some(file_count) = handle.file_count_from_metadata() else {
                        return;
                    };
                    handle
                        .apply_selection_now(deferred.op, file_count, deferred.context)
                        .await;
                }
            },
        );
    }

    /// Apply a selection op, handling the Initializing state per `policy`
    /// (see `InitPolicy`). Err only for a failed/timed-out `Wait`; everything
    /// else is best-effort and logged.
    async fn apply_selection(
        &self,
        op: SelectionOp,
        file_count: usize,
        context: &'static str,
        policy: InitPolicy,
    ) -> Result<()> {
        if self.is_initializing() {
            match policy {
                InitPolicy::Wait => self.await_initialized().await?,
                InitPolicy::Defer => {
                    self.defer_selection(op, context);
                    return Ok(());
                }
                InitPolicy::Skip => {
                    debug!(
                        info_hash = %self.info_hash,
                        ?op,
                        context,
                        "torrent is initializing; skipping file selection update"
                    );
                    return Ok(());
                }
            }
        }
        // A direct Prepare/Reconcile sets the whole selection and so supersedes
        // anything still parked from before the torrent became ready. A Clear
        // only removes one file and must not discard a parked reconcile. Use
        // the non-inserting lookup: a torrent that never deferred anything
        // has no slot, and checking should not create one.
        if !matches!(op, SelectionOp::Clear(_))
            && let Some(slot) = self.deferred_selection_if_present()
        {
            slot.supersede();
        }
        if !self.apply_selection_now(op, file_count, context).await
            && policy == InitPolicy::Defer
            && self.is_initializing()
        {
            // Lost the race with a (re-)initialization: park it after all.
            self.defer_selection(op, context);
        }
        Ok(())
    }

    /// Run the pure planner against the live `only_files` selection and apply
    /// the result via `Session::update_only_files`. Returns whether librqbit
    /// accepted the update (a no-op plan counts as accepted). Failures are
    /// logged, not propagated: with the selection unchanged librqbit still
    /// downloads and playback works through the blocking reader.
    async fn apply_selection_now(
        &self,
        op: SelectionOp,
        file_count: usize,
        context: &'static str,
    ) -> bool {
        let current = self.handle.only_files();
        let pinned = self.pinned_set();
        let Some(set) = plan_only_files(current.as_deref(), file_count, &pinned, op) else {
            return true;
        };
        match self.session.update_only_files(&self.handle, &set).await {
            Ok(()) => {
                debug!(
                    info_hash = %self.info_hash,
                    ?op,
                    context,
                    selection = ?set,
                    "Updated librqbit file selection"
                );
                true
            }
            Err(e) => {
                warn!(
                    info_hash = %self.info_hash,
                    ?op,
                    context,
                    error = %e,
                    "Failed to update librqbit file selection (best-effort; selection unchanged)"
                );
                false
            }
        }
    }

    /// Build a PieceReadiness with live peer count and download rate filled
    /// from one stats snapshot.
    fn readiness(
        &self,
        start: std::time::Instant,
        ready: bool,
        piece: i32,
        ready_pieces: u32,
        target_pieces: u32,
        reason: String,
    ) -> PieceReadiness {
        let stats = self.handle.stats();
        let (peers, download_rate) = stats
            .live
            .as_ref()
            .map(|l| {
                (
                    l.snapshot.peer_stats.live as u64,
                    // Speed.mbps is MiB/s; convert to bytes/s.
                    (l.download_speed.mbps * 1_048_576.0) as u64,
                )
            })
            .unwrap_or((0, 0));
        PieceReadiness {
            ready,
            piece,
            ready_pieces,
            target_pieces,
            elapsed_ms: start.elapsed().as_millis() as u64,
            peers,
            download_rate,
            reason,
        }
    }
}

impl Clone for LibrqbitHandle {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            info_hash: self.info_hash.clone(),
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        }
    }
}

/// The file names of a serialized torrent, in the torrent's own order.
///
/// `librqbit::create_torrent` walks a directory with `walkdir` and never
/// sorts, so a torrent built from a fixture folder lists its files in the
/// filesystem's readdir order. On ext4 that order is a hash of the name,
/// seeded per filesystem: the same fixture yields one order here and the
/// reverse on a CI runner. A test must therefore look its file up by name
/// and never assume the order it wrote the files in.
#[cfg(test)]
pub(crate) fn torrent_file_names(torrent_bytes: &[u8]) -> Vec<String> {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse torrent");
    let info = &meta.info.data;
    fn decode(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }
    match info.files.as_ref() {
        // Multi-file: each entry is a path split into components.
        Some(files) => files
            .iter()
            .map(|f| {
                f.path
                    .iter()
                    .map(|c| decode(c.as_ref()))
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect(),
        // Single-file: the torrent name is the one file.
        None => info
            .name
            .as_ref()
            .map(|n| decode(n.as_ref()))
            .into_iter()
            .collect(),
    }
}

/// The index `name` has in the torrent's file list. See
/// [`torrent_file_names`]: a file index is never the fixture's creation
/// order, so tests look it up instead of hardcoding it.
#[cfg(test)]
pub(crate) fn torrent_file_index(torrent_bytes: &[u8], name: &str) -> usize {
    let names = torrent_file_names(torrent_bytes);
    names
        .iter()
        .position(|n| n == name)
        .unwrap_or_else(|| panic!("{name} is not among the torrent's files: {names:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{DEFAULT_LISTEN_PORT_RANGE, TorrentBackend};

    /// Purely a string check -- no DNS resolution, no network. Every entry
    /// must be a nonempty host and a nonzero port, same shape a
    /// `dhtBootstrapNodes` entry is validated against
    /// (`server/src/routes/system.rs`'s `is_valid_dht_bootstrap_node`).
    fn assert_valid_host_port(entry: &str) {
        let (host, port) = entry
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("{entry:?} has no host:port split"));
        assert!(!host.is_empty(), "{entry:?} has an empty host");
        let port: u16 = port
            .parse()
            .unwrap_or_else(|e| panic!("{entry:?} has an unparseable port: {e}"));
        assert_ne!(port, 0, "{entry:?} has the zero port");
    }

    /// UPnP is asked for only when the mapping it installs is worth having
    /// next launch -- see `TorrentListenPort::wants_upnp_forwarding`. The
    /// embedded/JNI/test default is `Ephemeral`, where librqbit's forwarder
    /// would otherwise retry (and WARN) on a loop for the life of the
    /// process for a port number that never comes back.
    #[test]
    fn only_a_fixed_listen_port_asks_the_router_to_forward() {
        assert!(TorrentListenPort::default().wants_upnp_forwarding());
        assert!(TorrentListenPort::Fixed(DEFAULT_LISTEN_PORT_RANGE).wants_upnp_forwarding());
        assert!(!TorrentListenPort::Ephemeral.wants_upnp_forwarding());
    }

    /// A backend built without a DHT says so rather than looking like one
    /// that failed to bootstrap: `enabled: false` is a configuration, and
    /// `diagnostics::dht_health` must not warn about it.
    #[tokio::test]
    async fn a_session_without_a_dht_reports_it_as_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(tmp.path().to_path_buf())
            .await
            .expect("hermetic backend");
        let status = backend.dht_status();
        assert_eq!(status, crate::backend::DhtStatus::default());
        assert!(!status.enabled);
        assert!(!status.is_usable());
        assert!(!status.ever_bootstrapped);
    }

    /// The exact list, not just "non-empty and well-formed": every entry
    /// here is a host somebody has actually sent a DHT `ping` to (see the
    /// `DEFAULT_DHT_BOOTSTRAP_NODES` doc comment for the measurements).
    /// Adding one without that evidence should fail this test and make the
    /// author go and measure.
    #[test]
    fn default_dht_bootstrap_nodes_is_the_measured_list() {
        assert_eq!(
            DEFAULT_DHT_BOOTSTRAP_NODES,
            ["dht.libtorrent.org:25401", "dht.transmissionbt.com:6881"]
        );
        for entry in DEFAULT_DHT_BOOTSTRAP_NODES {
            assert_valid_host_port(entry);
        }
    }

    /// Every name measured to answer nothing stays out, `router.bittorrent`
    /// included -- it was re-probed from two networks and answered on
    /// neither. The fastest of the two survivors leads, so a bootstrap
    /// round reaches a live node as early as possible.
    #[test]
    fn default_dht_bootstrap_nodes_hold_no_host_that_answers_nothing() {
        assert_eq!(DEFAULT_DHT_BOOTSTRAP_NODES[0], "dht.libtorrent.org:25401");
        assert_eq!(
            DEFAULT_DHT_BOOTSTRAP_NODES[1],
            "dht.transmissionbt.com:6881"
        );
        for dead in [
            "router.bittorrent.com",
            "router.utorrent.com",
            "dht.aelitis.com",
        ] {
            assert!(
                !DEFAULT_DHT_BOOTSTRAP_NODES
                    .iter()
                    .any(|e| e.starts_with(dead)),
                "{dead} resolves but never answers a ping; it is retry noise, not resilience"
            );
        }
    }

    #[test]
    fn resolve_dht_bootstrap_nodes_falls_back_to_the_default_when_unconfigured() {
        assert_eq!(
            resolve_dht_bootstrap_nodes(&[]),
            DEFAULT_DHT_BOOTSTRAP_NODES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
    }

    /// A configured list survives the DNS step intact: its names are the
    /// ones resolved, and none of the defaults sneak back in.
    #[tokio::test]
    async fn a_configured_list_is_what_gets_resolved_not_the_default() {
        let configured = vec!["mydht.example.test:6881".to_string()];
        let addrs = effective_dht_bootstrap_addrs(
            &configured,
            &crate::backend::dht_bootstrap::BootstrapResolvers::offline(),
        )
        .await;
        assert_eq!(addrs, configured);
        for default_entry in DEFAULT_DHT_BOOTSTRAP_NODES {
            let host = default_entry.rsplit_once(':').unwrap().0;
            assert!(
                !addrs.iter().any(|a| a.contains(host)),
                "a configured list must not be topped up with the defaults"
            );
        }
    }

    /// A configured list of address literals reaches librqbit byte for
    /// byte -- an operator who already knows the addresses (because DNS on
    /// their network does not work) must not have them re-resolved.
    #[tokio::test]
    async fn a_configured_list_of_literals_reaches_librqbit_untouched() {
        let configured = vec![
            "67.215.246.10:6881".to_string(),
            "[2001:db8::1]:25401".to_string(),
        ];
        assert_eq!(
            effective_dht_bootstrap_addrs(
                &configured,
                &crate::backend::dht_bootstrap::BootstrapResolvers::offline(),
            )
            .await,
            configured
        );
    }

    /// Unconfigured still means the built-in default, resolved the same way.
    #[tokio::test]
    async fn an_unconfigured_list_resolves_the_built_in_default() {
        assert_eq!(
            effective_dht_bootstrap_addrs(
                &[],
                &crate::backend::dht_bootstrap::BootstrapResolvers::offline(),
            )
            .await,
            DEFAULT_DHT_BOOTSTRAP_NODES
        );
    }

    #[test]
    fn resolve_dht_bootstrap_nodes_lets_a_configured_list_replace_the_default() {
        let configured = vec!["example.test:6881".to_string()];
        let resolved = resolve_dht_bootstrap_nodes(&configured);
        // Replaces, not appends: none of the built-in defaults survive
        // alongside the operator's override.
        assert_eq!(resolved, configured);
        for default_entry in DEFAULT_DHT_BOOTSTRAP_NODES {
            assert!(
                !resolved.contains(&default_entry.to_string()),
                "the configured list should have replaced the default entirely"
            );
        }
    }

    /// How long a bounded state-wait in these tests may take before it
    /// gives up. These waits poll for something a background task or the
    /// blocking hash-check pool has to do, so the bound is not a timing
    /// assertion: it is only there so a regression fails instead of
    /// hanging. Generous on purpose -- a CI runner under load (or
    /// `--test-threads=16` on two cores) can be an order of magnitude
    /// slower than an idle laptop, and a tight bound turns that into a
    /// spurious failure.
    const TEST_WAIT_BOUND: Duration = Duration::from_secs(60);

    /// `Ephemeral` sessions never collide: the OS hands each its own port.
    /// (Whether a second bind of an already-taken *fixed* port fails is
    /// platform-dependent -- Windows lets it through -- so that is not
    /// asserted here.)
    #[tokio::test(flavor = "multi_thread")]
    async fn ephemeral_sessions_coexist() {
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

        // `offline()`: this test is about port binding, and a real
        // resolver here would put a DNS lookup on a hermetic test's path.
        let (a, _) = LibrqbitBackend::new(
            dirs[0].path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
        )
        .await
        .expect("first ephemeral session");
        let (b, _) = LibrqbitBackend::new(
            dirs[1].path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
        )
        .await
        .expect("second ephemeral session alongside the first");

        let pa = a.session.listen_addr().expect("listening").port();
        let pb = b.session.listen_addr().expect("listening").port();
        assert_ne!(pa, 0);
        assert_ne!(pb, 0);
        assert_ne!(pa, pb, "each ephemeral session gets its own port");
    }

    fn privacy_with_proxy(kind: TorrentProxyType, host: &str, port: u16) -> TorrentPrivacyConfig {
        TorrentPrivacyConfig {
            bt_proxy_type: kind,
            bt_proxy_host: host.to_string(),
            bt_proxy_port: port,
            bt_proxy_username: "us:er".to_string(),
            bt_proxy_password: "p@ss".to_string(),
            ..TorrentPrivacyConfig::default()
        }
    }

    /// The settings librqbit can take, reduced to what it takes them as --
    /// and the ones it cannot, left at its defaults instead of handed to a
    /// session that would refuse to open over them.
    #[test]
    fn session_tuning_takes_what_librqbit_has_a_knob_for() {
        use TorrentProxyType::*;
        let defaults = SessionTuning::from_settings(
            &TorrentSpeedProfile::default(),
            &TorrentPrivacyConfig::default(),
        );
        assert_eq!(
            defaults,
            SessionTuning {
                // The shipped default of 160 connections derives 40
                // peers per torrent -- the floor, deliberately; see
                // `DEFAULT_BT_MAX_CONNECTIONS`.
                peer_limit: Some(40),
                ..SessionTuning::default()
            }
        );

        // SOCKS5 goes through, with credentials only for the password kind,
        // percent-encoded so a ':' or '@' in them cannot re-shape the URL.
        let proxied = |kind| socks5_proxy_url(&privacy_with_proxy(kind, "proxy.example", 1080));
        assert_eq!(
            proxied(Socks5).as_deref(),
            Some("socks5://proxy.example:1080")
        );
        assert_eq!(
            proxied(Socks5Password).as_deref(),
            Some("socks5://us%3Aer:p%40ss@proxy.example:1080")
        );
        for other in [None, Socks4, Http, HttpPassword] {
            assert_eq!(
                proxied(other),
                Option::None,
                "{other:?} has no librqbit equivalent"
            );
        }
        assert_eq!(
            socks5_proxy_url(&privacy_with_proxy(Socks5, "::1", 1080)).as_deref(),
            Some("socks5://[::1]:1080"),
            "an IPv6 literal is bracketed"
        );
        assert_eq!(
            socks5_proxy_url(&privacy_with_proxy(Socks5, "", 1080)),
            Option::None
        );
        assert_eq!(
            socks5_proxy_url(&privacy_with_proxy(Socks5, "proxy", 0)),
            Option::None
        );
        assert_eq!(
            socks5_proxy_url(&privacy_with_proxy(Socks5, "not a host", 1080)),
            Option::None,
            "a host librqbit's parser would reject is not handed to it"
        );

        // One interface name; an address or a list is not a bind device.
        assert_eq!(bind_device_name(" tun0 ").as_deref(), Some("tun0"));
        assert_eq!(bind_device_name(""), Option::None);
        assert_eq!(bind_device_name("192.168.1.25"), Option::None);
        assert_eq!(bind_device_name("fe80::1"), Option::None);
        assert_eq!(bind_device_name("tun0,wg0"), Option::None);

        // The hard limit is bytes per second; 0 is none.
        let limited = TorrentSpeedProfile {
            bt_download_speed_hard_limit: 1_500_000.0,
            // 100 clamps up to MIN_EFFECTIVE_BT_CONNECTIONS and derives
            // 20, which the floor lifts to 40 -- the same per-torrent cap
            // the default derives, so `peer_limit` is not what makes this
            // tuning differ from `defaults`.
            bt_max_connections: 100,
            ..TorrentSpeedProfile::default()
        };
        let tuning = SessionTuning::from_settings(
            &limited,
            &TorrentPrivacyConfig {
                bt_enable_dht: false,
                bt_enable_lsd: false,
                bt_outgoing_interfaces: "tun0".to_string(),
                ..privacy_with_proxy(Socks5, "127.0.0.1", 1080)
            },
        );
        assert_eq!(
            tuning,
            SessionTuning {
                dht: false,
                lsd: false,
                proxy_url: Some("socks5://127.0.0.1:1080".to_string()),
                download_bps: std::num::NonZeroU32::new(1_500_000),
                peer_limit: Some(40),
                bind_device: Some("tun0".to_string()),
            }
        );
        assert_eq!(
            defaults.pending_restart(&tuning),
            vec![
                "btEnableDht",
                "btEnableLsd",
                "btProxyType",
                "btProxyHost",
                "btProxyPort",
                "btProxyUsername",
                "btProxyPassword",
                "btOutgoingInterfaces",
            ],
            "everything but the two live settings waits for the next start"
        );
        assert!(defaults.pending_restart(&defaults).is_empty());
    }

    /// The truth table names every `bt*` setting the settings structs
    /// carry, once, and nothing else -- so a setting added to either struct
    /// without a row here fails this instead of joining the silently
    /// accepted.
    #[test]
    fn every_bt_setting_has_a_row_in_the_truth_table() {
        let profile = serde_json::to_value(TorrentSpeedProfile::default()).unwrap();
        let privacy = serde_json::to_value(TorrentPrivacyConfig::default()).unwrap();
        let mut settings: Vec<String> = profile
            .as_object()
            .unwrap()
            .keys()
            .chain(privacy.as_object().unwrap().keys())
            .cloned()
            .collect();
        settings.sort();
        let mut rows: Vec<String> = bt_settings_support()
            .iter()
            .map(|row| row.setting.to_string())
            .collect();
        rows.sort();
        assert_eq!(rows, settings);
        for row in bt_settings_support() {
            assert!(!row.note.is_empty(), "{} says why", row.setting);
            assert!(!row.note.contains("  "), "{}: {:?}", row.setting, row.note);
        }
    }

    /// The session opens the way the settings say: no DHT, the peer limit
    /// the connection count derives, the download limit -- and the limit is
    /// the one thing a later update changes on the running session, which
    /// the report says, along with what waits for a restart.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_session_opens_with_the_settings_and_changes_the_limit_live() {
        let tmp = tempfile::tempdir().unwrap();
        let profile = TorrentSpeedProfile {
            bt_download_speed_hard_limit: 2_000_000.0,
            bt_max_connections: 400,
            ..TorrentSpeedProfile::default()
        };
        let privacy = TorrentPrivacyConfig {
            bt_enable_dht: false,
            ..TorrentPrivacyConfig::default()
        };
        let (backend, _) = LibrqbitBackend::new_with_settings(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
            SessionTuning::from_settings(&profile, &privacy),
        )
        .await
        .expect("session");
        assert!(
            backend.session.get_dht().is_none(),
            "btEnableDht off is no DHT"
        );
        assert_eq!(backend.session.peer_limit, Some(100));
        assert_eq!(
            backend.session.ratelimits.get_download_bps(),
            std::num::NonZeroU32::new(2_000_000)
        );

        // The limit moves at once; the DHT coming back waits.
        let report = backend.apply_settings(
            &TorrentSpeedProfile {
                bt_download_speed_hard_limit: 0.0,
                ..profile.clone()
            },
            &TorrentPrivacyConfig::default(),
        );
        assert_eq!(backend.session.ratelimits.get_download_bps(), Option::None);
        assert_eq!(
            report.applied_live,
            vec!["btDownloadSpeedHardLimit", "btMaxConnections"]
        );
        assert_eq!(report.pending_restart, vec!["btEnableDht"]);
        assert!(report.not_honoured.contains(&"btEncryptionMode"));
        assert!(report.not_honoured.contains(&"btProxyPeerConnections"));
        assert!(!report.not_honoured.contains(&"btEnableDht"));

        // Nothing changed: nothing pending, the limit re-applied as it was.
        let report = backend.apply_settings(&profile, &privacy);
        assert!(report.pending_restart.is_empty(), "{report:?}");
        assert_eq!(
            backend.session.ratelimits.get_download_bps(),
            std::num::NonZeroU32::new(2_000_000)
        );

        // And `btMaxConnections` reaches the torrents that already exist,
        // rather than waiting for a restart the user has no way to ask for.
        // 400 derived the 100 asserted above; 800 derives the ceiling.
        let payload = tmp.path().join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes), Vec::new())
            .await
            .expect("add torrent");
        assert_eq!(handle.handle.shared.peer_limit(), 100);
        backend.apply_settings(
            &TorrentSpeedProfile {
                bt_max_connections: 800,
                ..profile.clone()
            },
            &privacy,
        );
        assert_eq!(
            handle.handle.shared.peer_limit(),
            200,
            "the running torrent took the new limit"
        );
        // The session's own option is fixed once it is open, so the report
        // must not be read off it: the live cap is on the torrents.
        assert_eq!(backend.session.peer_limit, Some(100));

        // Under `Lean` the lean cap stands and the new value is only
        // stored -- and it is what the return to `Full` restores.
        backend.set_footprint(Footprint::Lean);
        assert_eq!(handle.handle.shared.peer_limit(), LEAN_PEER_LIMIT);
        backend.apply_settings(&profile, &privacy);
        assert_eq!(
            handle.handle.shared.peer_limit(),
            LEAN_PEER_LIMIT,
            "a settings change must not lift the background footprint"
        );
        backend.set_footprint(Footprint::Full);
        assert_eq!(
            handle.handle.shared.peer_limit(),
            100,
            "the foreground came back to the limit set while lean, not the one before it"
        );
    }

    /// A bind device the OS does not know fails `Session::new`; the backend
    /// starts without it rather than not at all, and says so in the report.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_outgoing_interface_starts_the_session_unbound() {
        let tmp = tempfile::tempdir().unwrap();
        let privacy = TorrentPrivacyConfig {
            bt_enable_dht: false,
            bt_outgoing_interfaces: "no-such-interface-enginefs".to_string(),
            ..TorrentPrivacyConfig::default()
        };
        let tuning = SessionTuning::from_settings(&TorrentSpeedProfile::default(), &privacy);
        assert_eq!(
            tuning.bind_device.as_deref(),
            Some("no-such-interface-enginefs")
        );
        let (backend, _) = LibrqbitBackend::new_with_settings(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
            tuning,
        )
        .await
        .expect("the session opens unbound");
        assert_eq!(backend.started_with.bind_device, Option::None);
        let report = backend.apply_settings(&TorrentSpeedProfile::default(), &privacy);
        assert_eq!(
            report.pending_restart,
            vec!["btOutgoingInterfaces"],
            "the setting reads as not applied to this session"
        );
    }

    /// The proxy is not a field that was set; it is where the packets go.
    /// A listener stands in for the SOCKS5 proxy, a torrent is given a peer
    /// to connect to, and what arrives at the listener is a SOCKS5 greeting
    /// -- librqbit connecting to the peer through the proxy rather than to
    /// the peer.
    #[tokio::test(flavor = "multi_thread")]
    async fn peer_connections_go_through_the_configured_socks5_proxy() {
        use tokio::io::AsyncReadExt;
        let tmp = tempfile::tempdir().unwrap();
        let proxy = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let proxy_port = proxy.local_addr().unwrap().port();
        let privacy = TorrentPrivacyConfig {
            bt_enable_dht: false,
            bt_enable_lsd: false,
            ..privacy_with_proxy(TorrentProxyType::Socks5, "127.0.0.1", proxy_port)
        };
        let (backend, _) = LibrqbitBackend::new_with_settings(
            tmp.path().join("dl"),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
            SessionTuning::from_settings(&TorrentSpeedProfile::default(), &privacy),
        )
        .await
        .expect("session");

        // An unseeded torrent with one peer to try. Loopback port 9 is the
        // discard port: with no proxy the connection would go there and be
        // refused; with one it goes to the listener above.
        write_payload(&tmp.path().join("payload.bin"), 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&tmp.path().join("payload.bin")).await;
        backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(vec![(std::net::Ipv4Addr::LOCALHOST, 9).into()]),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");

        let (mut conn, _) = tokio::time::timeout(TEST_WAIT_BOUND, proxy.accept())
            .await
            .expect("librqbit reaches the proxy for its peer")
            .expect("accept");
        let mut greeting = [0u8; 2];
        tokio::time::timeout(TEST_WAIT_BOUND, conn.read_exact(&mut greeting))
            .await
            .expect("a greeting arrives")
            .expect("read");
        assert_eq!(greeting[0], 0x05, "SOCKS version 5: {greeting:?}");
        assert!(
            greeting[1] >= 1,
            "at least one auth method offered: {greeting:?}"
        );
    }

    /// Write `len` patterned bytes to `path` (deterministic, non-trivial data
    /// so piece hashes are meaningful).
    pub(super) async fn write_payload(path: &std::path::Path, len: usize) {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(path, &data).await.expect("write payload");
    }

    /// Create a .torrent for `path` with small pieces so tests stay fast.
    /// Returns (serialized torrent bytes, info hash as hex). We extract what we
    /// need immediately rather than holding the borrowing result.
    pub(super) async fn make_torrent(path: &std::path::Path) -> (Vec<u8>, String) {
        make_torrent_with_piece_length(path, 16384).await
    }

    /// [`make_torrent`] with the piece length spelled out, for tests that
    /// care how a piece divides into librqbit's 16 KiB chunks -- a 16 KiB
    /// piece is exactly one chunk, which hides every chunk-to-byte question.
    pub(super) async fn make_torrent_with_piece_length(
        path: &std::path::Path,
        piece_length: u32,
    ) -> (Vec<u8>, String) {
        let t = librqbit::create_torrent(
            path,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(piece_length),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");
        (
            t.as_bytes().expect("serialize torrent").to_vec(),
            t.info_hash().as_string(),
        )
    }

    /// Backend + torrent added from bytes; payload seeded iff the caller wrote
    /// the payload file into the download dir beforehand.
    pub(super) async fn backend_with_torrent(
        download_dir: &std::path::Path,
        torrent_bytes: &[u8],
    ) -> (LibrqbitBackend, LibrqbitHandle) {
        let backend = LibrqbitBackend::new_for_tests(download_dir.to_path_buf())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes.to_vec()), vec![])
            .await
            .expect("add torrent");
        (backend, handle)
    }

    /// [`backend_with_torrent`] whose session can release pieces, so the
    /// torrent is added with `piece_reclaim` and `drop_file_pieces` works.
    pub(super) async fn reclaiming_backend_with_torrent(
        download_dir: &std::path::Path,
        torrent_bytes: &[u8],
    ) -> (LibrqbitBackend, LibrqbitHandle) {
        let (backend, _restored) = LibrqbitBackend::new_for_tests_with(
            download_dir.to_path_buf(),
            TestSessionOptions {
                default_storage: Some(reclaimable_storage()),
                ..Default::default()
            },
        )
        .await
        .expect("hermetic session");
        assert!(
            backend.sets_piece_reclaim(),
            "this storage promises it can release pieces"
        );
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes.to_vec()), vec![])
            .await
            .expect("add torrent");
        (backend, handle)
    }

    /// Against the shipped librqbit, a torrent that is fine is not reported
    /// out of space -- the check reads the real `ManagedTorrentState`, so a
    /// classifier that matched everything (or a state read that never fired)
    /// would show up here rather than in production as a restart loop.
    #[tokio::test]
    async fn a_healthy_torrent_is_not_reported_out_of_space() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        assert!(!handle.is_out_of_space().await);
        assert!(!handle.is_in_error_state().await);
    }

    /// Against the shipped librqbit: `stop_torrent` is `Session::pause`,
    /// which takes a live torrent to `Paused` (no peers, no writes, files and
    /// piece map kept) and refuses a torrent already paused, and
    /// `start_torrent` is `Session::unpause`, which takes it straight back to
    /// live. Read off the state machine, never off the persisted flag.
    #[tokio::test]
    async fn stop_torrent_pauses_a_live_torrent_and_start_takes_it_back() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.await_initialized().await.expect("the check ends");

        handle.stop_torrent().await.expect("a live torrent pauses");
        assert_eq!(wait_until_settled(&handle).await, RunState::Paused);
        assert!(
            handle.stop_torrent().await.is_err(),
            "a paused torrent is not paused twice"
        );
        assert!(
            !handle.is_out_of_space().await && !handle.is_in_error_state().await,
            "paused is not the error state"
        );

        handle
            .start_torrent()
            .await
            .expect("unpause takes it back to live");
        assert_eq!(handle.run_state(), RunState::Live, "live again");
        assert!(
            handle.start_torrent().await.is_err(),
            "a live torrent is not started twice"
        );
    }

    /// A reconcile that lands *during* an initial check never pauses the
    /// torrent -- and the reader that follows it does not hang.
    ///
    /// This is the third librqbit shape, and the reason the ladder's first
    /// arm is where it is. `ManagedTorrent::pause` on an `Initializing`
    /// torrent sets the persisted flag and calls `request_pause()`;
    /// `FileOps::initial_check` bails on that (`file_ops.rs:113`) and the
    /// `Err` arm returns `Ok` without changing the state
    /// (`torrent_state/mod.rs:590-593`), leaving the torrent
    /// `Initializing` with no check running -- which
    /// `wait_until_initialized` (`mod.rs:759`) polls for ever. Every
    /// `/stream` request for that torrent goes through
    /// `LibrqbitHandle::await_initialized`, so the visible symptom is a
    /// player that never gets a first byte and a request that never
    /// returns.
    ///
    /// The volume here is full, so the free-space arm wants this torrent
    /// stopped and would make the call on any settled reading. It is the
    /// unsettled reading, not the arm, that holds the call back.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reconcile_during_an_initial_check_does_not_wedge_it() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        // The payload lives outside the download dir, so the torrent this
        // session adds is one that still wants every byte it has -- which
        // is what the free-space arm is about. A torrent checked over its
        // own complete data would be `finished`, and that arm would never
        // apply to it however full the volume was.
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        let payload = src.join("payload.bin");
        write_payload(&payload, 4 * 16 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;
        let dir = tmp.path().join("dl");
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let (storage, gate) = gated_storage();
        let (backend, _restored) = LibrqbitBackend::new_for_tests_with(
            dir.clone(),
            TestSessionOptions {
                default_storage: Some(storage),
                ..Default::default()
            },
        )
        .await
        .expect("open the session");
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes), vec![])
            .await
            .expect("add the torrent");
        gate.wait_until_held(1).await;

        let available = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let probe = available.clone();
        let mut efs = crate::BackendEngineFS::new_with_backend(
            backend,
            HashMap::from([(hash.clone(), handle.clone())]),
            dir.join("cache"),
            dir.clone(),
        );
        efs.set_free_space_probe(move |_| Ok(probe.load(Ordering::SeqCst)));

        // Inside the window, on a volume with nothing free.
        assert_eq!(
            efs.reconcile_tick().await,
            vec![(hash.clone(), crate::reconcile::Decision::Stop)],
            "the ladder says a torrent whose check is running should not be running"
        );
        assert_eq!(
            handle.run_state(),
            RunState::Initializing {
                pause_requested: false
            },
            "and it made no pause call: a pause here is what bails the check"
        );

        // The check runs to the end and the torrent settles, which a bailed
        // check would never do.
        gate.open();
        let waited = Instant::now();
        handle
            .await_initialized()
            .await
            .expect("the check finished, so the reader has something to open");
        let waited = waited.elapsed();
        assert!(
            waited < Duration::from_secs(30),
            "the reader waited {waited:?} on a check that was bailed"
        );
        assert_eq!(handle.run_state(), RunState::Live);

        // Now that the reading is settled the same full volume does stop
        // it, which is the arm the reading was holding back.
        efs.reconcile_tick().await;
        assert_eq!(handle.run_state(), RunState::Paused);

        // And room brings it back.
        available.store(u64::MAX, Ordering::SeqCst);
        efs.reconcile_tick().await;
        assert_eq!(handle.run_state(), RunState::Live);
    }

    /// A restart, over a real persisted librqbit session, with one torrent
    /// that the previous process had stopped: the engine that comes out is
    /// what a fresh boot really holds.
    ///
    /// This is the situation every deleted record was wrong about. The
    /// pause is in `session.json` and survived; nothing in this process
    /// knows it exists, let alone why, and on master the three call sites
    /// that could have lifted it all read `if idle_paused.swap(false) &&
    /// resume()` -- `false && ...` here, so none of them ever ran.
    ///
    /// The free-space probe is declared, so the decisions the tests below
    /// make are about their own inputs rather than about however much room
    /// the machine running them happens to have.
    #[cfg(test)]
    async fn restarted_over_a_stopped_torrent(
        dir: &std::path::Path,
    ) -> (crate::BackendEngineFS<LibrqbitBackend>, String) {
        use crate::backend::TorrentHandle;
        tokio::fs::create_dir_all(dir).await.unwrap();
        let payload = dir.join("movie.bin");
        write_payload(&payload, 4 * 16 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;

        // The process before this one: it added the torrent and stopped it.
        {
            let (backend, _restored) = LibrqbitBackend::new_for_tests_with(
                dir.to_path_buf(),
                TestSessionOptions {
                    persist: true,
                    ..Default::default()
                },
            )
            .await
            .expect("open the first session");
            let handle = backend
                .add_torrent(TorrentSource::Bytes(torrent_bytes), vec![])
                .await
                .expect("add the torrent");
            handle.await_initialized().await.expect("the check ends");
            handle.stop_torrent().await.expect("and it is stopped");
            let deadline = Instant::now() + TEST_WAIT_BOUND;
            let session_json = dir.join("session.json");
            while !session_json.exists() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(session_json.exists(), "the session was persisted");
        }

        // This one.
        let (backend, restored) = LibrqbitBackend::new_for_tests_with(
            dir.to_path_buf(),
            TestSessionOptions {
                persist: true,
                ..Default::default()
            },
        )
        .await
        .expect("open the second session");
        assert_eq!(restored.len(), 1, "the torrent came back");
        assert_eq!(
            wait_until_settled(&restored[&hash]).await,
            RunState::Paused,
            "and it came back stopped, exactly as it was left"
        );

        let mut efs = crate::BackendEngineFS::new_with_backend(
            backend,
            restored,
            dir.join("cache"),
            dir.to_path_buf(),
        );
        efs.set_free_space_probe(|_| Ok(u64::MAX));
        efs.restore_pinned_downloads().await;
        (efs, hash)
    }

    /// What the torrent is doing, from the engine registry -- never
    /// `is_paused()`, which across an initial check is wrong in both
    /// directions.
    #[cfg(test)]
    async fn restored_run_state(
        efs: &crate::BackendEngineFS<LibrqbitBackend>,
        hash: &str,
    ) -> RunState {
        use crate::backend::TorrentHandle;
        efs.get_engine(hash)
            .await
            .expect("the restored engine")
            .handle
            .run_state()
    }

    /// A stream starting on a torrent the last process left stopped starts
    /// it -- over a real persisted session, which is the only place the bug
    /// lived.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_starting_after_a_restart_starts_the_stopped_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let (efs, hash) = restarted_over_a_stopped_torrent(&tmp.path().join("dl")).await;

        efs.on_stream_start(&hash, 0).await;

        assert_eq!(restored_run_state(&efs, &hash).await, RunState::Live);
    }

    /// The same of `pin_download`: an offline download that is not running
    /// is not a download. On master this site read
    /// `if idle_paused.swap(false) && resume()`, so after a restart the pin
    /// was recorded and nothing ever fetched a byte for it.
    #[tokio::test(flavor = "multi_thread")]
    async fn pinning_after_a_restart_starts_the_stopped_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let (efs, hash) = restarted_over_a_stopped_torrent(&tmp.path().join("dl")).await;

        efs.pin_download(&hash, 0, None).await.expect("the pin");

        assert_eq!(restored_run_state(&efs, &hash).await, RunState::Live);
    }

    /// The same of `focus_torrent`, the third of the three dead sites.
    #[tokio::test(flavor = "multi_thread")]
    async fn focusing_after_a_restart_starts_the_stopped_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let (efs, hash) = restarted_over_a_stopped_torrent(&tmp.path().join("dl")).await;

        efs.focus_torrent(&hash).await;

        assert_eq!(restored_run_state(&efs, &hash).await, RunState::Live);
    }

    /// And of the seeding switch. The user turns seeding back on after a
    /// restart; every torrent the last process had stopped for want of it
    /// must seed again, and on master none of them did.
    #[tokio::test(flavor = "multi_thread")]
    async fn re_enabling_seeding_after_a_restart_starts_the_stopped_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let (efs, hash) = restarted_over_a_stopped_torrent(&tmp.path().join("dl")).await;
        efs.seeding_enabled.store(false, Ordering::Relaxed);

        efs.set_seeding_enabled(true).await;

        assert_eq!(restored_run_state(&efs, &hash).await, RunState::Live);
    }

    /// How far a torrent's initial check has got, or `None` once it is past
    /// initializing. `get_checked_bytes` is incremented by a whole piece as
    /// each one is taken up, before it is read
    /// (`crates/librqbit/src/file_ops.rs:120`), so over the whole-piece
    /// fixtures here it counts pieces exactly -- which is how these tests
    /// tell a check that stopped from one that ran on, with no sleeping.
    #[cfg(test)]
    fn checked_pieces(handle: &LibrqbitHandle) -> Option<u64> {
        handle.handle.with_state(|state| match state {
            ManagedTorrentState::Initializing(init) => Some(init.get_checked_bytes() / (16 * 1024)),
            _ => None,
        })
    }

    /// Poll `run_state` until it reports something settled, i.e. not
    /// `Initializing`. The bound is only there so a regression fails instead
    /// of hanging.
    #[cfg(test)]
    async fn wait_until_settled(handle: &LibrqbitHandle) -> RunState {
        let deadline = Instant::now() + TEST_WAIT_BOUND;
        loop {
            let state = handle.run_state();
            if !matches!(state, RunState::Initializing { .. }) {
                return state;
            }
            assert!(
                Instant::now() < deadline,
                "the initial check never finished: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// `has_metadata` reads librqbit's metadata slot, so this pins that it
    /// is reading the right slot the right way round: a torrent added from
    /// a `.torrent` has its info dictionary from the moment it is added,
    /// and says so. (The reconciler asks this of every torrent every two
    /// seconds, and answers `Run` when it is false, so an inverted reading
    /// would keep every torrent on this server running for ever.)
    #[tokio::test]
    async fn a_torrent_added_from_a_file_knows_its_metadata() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dl");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;

        assert!(handle.has_metadata().await);
        assert_eq!(
            handle.has_metadata().await,
            handle.stats().await.has_metadata,
            "the cheap answer and the statistics' answer are the same answer"
        );
    }

    /// The swallowed unpause, against the shipped librqbit: after it, the
    /// `paused` flag says the torrent is running and the torrent is stopped.
    ///
    /// `Session::unpause` writes `g.paused = false` before `_start` has
    /// looked at anything (`torrent_state/mod.rs:649`), then finds the
    /// initial check already running and returns success having started
    /// nothing (`:548-551`). The check that *is* running was spawned by the
    /// add, with `start_paused = true` captured, so when it finishes its own
    /// continuation parks the torrent in `Paused` (`:587`, returning at
    /// `:607`).
    ///
    /// The last assertion is librqbit's own opinion rather than either of
    /// the two readings: it refuses to pause a torrent it considers paused,
    /// so a pause that errs is the state machine agreeing with `run_state`
    /// and contradicting the flag.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_state_sees_the_pause_a_swallowed_unpause_left_behind() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dl");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 4 * 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (storage, gate) = gated_storage();
        let (backend, _restored) = LibrqbitBackend::new_for_tests_with(
            dir.clone(),
            TestSessionOptions {
                default_storage: Some(storage),
                ..Default::default()
            },
        )
        .await
        .expect("open the session");

        // Added paused, because that is what decides the outcome: the check
        // the add spawns captures `start_paused = true`, and it is that
        // capture -- not anything the unpause does -- that the continuation
        // acts on.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    paused: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("add the torrent paused");
        let handle = match response {
            librqbit::AddTorrentResponse::Added(_, handle) => backend.wrap(handle),
            _ => panic!("the torrent was not added"),
        };
        gate.wait_until_held(1).await;

        // Inside the window: the check is running, so the unpause below has
        // something to be swallowed by.
        assert_eq!(
            handle.run_state(),
            RunState::Initializing {
                pause_requested: true
            },
            "the check is running and a pause is pending on it"
        );

        backend
            .session
            .unpause(&handle.handle)
            .await
            .expect("librqbit reports the unpause a success");
        assert!(
            !handle.handle.is_paused(),
            "the flag was cleared before _start looked at the state"
        );

        gate.open();
        let settled = wait_until_settled(&handle).await;

        assert_eq!(
            settled,
            RunState::Paused,
            "the unpause started nothing; the in-flight check parked the torrent"
        );
        assert!(
            !handle.handle.is_paused(),
            "...while the flag it cleared still says the torrent is running"
        );
        assert!(
            backend.session.pause(&handle.handle).await.is_err(),
            "librqbit refuses to pause it again, which is it agreeing with run_state"
        );
    }

    /// The other direction: a pause landing during a *fastresume* check is
    /// dropped on the floor, and the torrent goes live with the flag set.
    ///
    /// `TorrentStateInitializing::check` hands `pause_requested` to
    /// `FileOps::initial_check` and to nothing else
    /// (`torrent_state/initializing.rs:279`); `validate_fastresume` never
    /// reads it. So the check returns `Ok`, its continuation applies the
    /// add-time `start_paused` -- `false`, this torrent having been restored
    /// unpaused -- and takes it `Live` (`torrent_state/mod.rs:606-620`).
    ///
    /// This is the shape behind the measured 3 MiB -> 12 MiB overshoot: the
    /// free-space watch stopped the torrent, was told it was paused, and the
    /// torrent went on writing to the full volume.
    ///
    /// A restart is not decoration here. Fastresume needs a have-bitfield
    /// from a previous run, so the first session is what makes the second
    /// one take the fastresume path at all -- and a restart is exactly when
    /// this happens in the field.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_state_sees_the_torrent_a_swallowed_pause_took_live() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dl");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 4 * 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        // First run: a persistent session over data that is already on disk,
        // so its check finds every piece and stores the bitfield the second
        // run will resume from.
        {
            let (backend, restored) = LibrqbitBackend::new_for_tests_with(
                dir.clone(),
                TestSessionOptions {
                    persist: true,
                    ..Default::default()
                },
            )
            .await
            .expect("open the first session");
            assert!(restored.is_empty(), "nothing to restore yet");
            let handle = backend
                .add_torrent(TorrentSource::Bytes(torrent_bytes.clone()), vec![])
                .await
                .expect("add the torrent");
            handle
                .handle
                .wait_until_initialized()
                .await
                .expect("the first check ends");
            assert!(
                handle.handle.stats().finished,
                "the payload was already on disk, so the check has every piece"
            );
            let deadline = Instant::now() + TEST_WAIT_BOUND;
            while !bitfield_written(&dir) && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                bitfield_written(&dir),
                "the have-bitfield was persisted; without it the restart does a full check"
            );
        }

        // The restart, over a storage that holds every read.
        let (storage, gate) = gated_storage();
        let (backend, restored) = LibrqbitBackend::new_for_tests_with(
            dir.clone(),
            TestSessionOptions {
                default_storage: Some(storage),
                persist: true,
                ..Default::default()
            },
        )
        .await
        .expect("open the second session");
        assert_eq!(restored.len(), 1, "the torrent came back");
        let handle = restored
            .values()
            .next()
            .expect("the restored handle")
            .clone();
        gate.wait_until_held(1).await;
        assert_eq!(
            handle.run_state(),
            RunState::Initializing {
                pause_requested: false
            },
            "restored unpaused, and its fastresume check is inside the storage"
        );

        // The reconciler's stop, landing in that window.
        handle
            .stop_torrent()
            .await
            .expect("librqbit accepts a pause on an initializing torrent");
        assert!(handle.handle.is_paused(), "the flag says paused");

        gate.open();
        let settled = wait_until_settled(&handle).await;

        assert_eq!(
            settled,
            RunState::Live,
            "the fastresume check never read the pause request, so the torrent went live"
        );
        assert!(
            handle.handle.is_paused(),
            "...while the flag still says it is paused"
        );
        backend
            .session
            .pause(&handle.handle)
            .await
            .expect("librqbit pauses it, which it would refuse for a paused torrent");
    }

    /// Whether the session has written a have-bitfield yet
    /// (`<info hash>.bitv` beside `session.json`, see
    /// `crates/librqbit/src/session_persistence/json.rs:121`). Matched by
    /// extension rather than by name so nothing here depends on how librqbit
    /// formats an info hash.
    #[cfg(test)]
    fn bitfield_written(folder: &std::path::Path) -> bool {
        std::fs::read_dir(folder)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("bitv"))
            })
            .unwrap_or(false)
    }

    /// The third shape: a pause during a *full* check does stop it, and the
    /// torrent is left `Initializing` rather than `Paused` -- a state
    /// `wait_until_initialized` (`torrent_state/mod.rs:759`) polls forever.
    /// `is_paused()` says "paused" for it, which is the one word that
    /// suggests the very thing it is not: something a caller can start again
    /// in one transition.
    ///
    /// `FileOps::initial_check` bails on the request (`file_ops.rs:113`) and
    /// the `Err` arm returns `Ok` without touching the state
    /// (`torrent_state/mod.rs:590-593`).
    ///
    /// A second torrent shares the gate and is *not* paused. It is the
    /// clock: nothing here waits on a duration, and "the paused torrent
    /// never moved" would otherwise be a claim about a machine that had not
    /// got round to it yet. The control's check runs the whole way through
    /// the same storage after the same `open()`; when it is live, the paused
    /// torrent has had every chance, and its checked-piece count says
    /// exactly where it stopped.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_state_keeps_a_wedged_initial_check_apart_from_a_pause() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dl");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let paused_payload = dir.join("paused.bin");
        let control_payload = dir.join("control.bin");
        write_payload(&paused_payload, 4 * 16 * 1024).await;
        write_payload(&control_payload, 4 * 16 * 1024).await;
        let (paused_bytes, _) = make_torrent(&paused_payload).await;
        let (control_bytes, _) = make_torrent(&control_payload).await;

        let (storage, gate) = gated_storage();
        let (backend, _restored) = LibrqbitBackend::new_for_tests_with(
            dir.clone(),
            TestSessionOptions {
                default_storage: Some(storage),
                ..Default::default()
            },
        )
        .await
        .expect("open the session");

        let wedged = backend
            .add_torrent(TorrentSource::Bytes(paused_bytes), vec![])
            .await
            .expect("add the torrent to be paused");
        let control = backend
            .add_torrent(TorrentSource::Bytes(control_bytes), vec![])
            .await
            .expect("add the control torrent");
        // Both checks are inside the gate, each held at its first read.
        gate.wait_until_held(2).await;

        wedged
            .stop_torrent()
            .await
            .expect("librqbit accepts a pause on an initializing torrent");
        assert!(wedged.handle.is_paused(), "the flag says paused");
        assert_eq!(
            wedged.run_state(),
            RunState::Initializing {
                pause_requested: true
            },
            "run_state says what the flag cannot: this is a check, not a pause"
        );

        gate.open();
        assert_eq!(
            wait_until_settled(&control).await,
            RunState::Live,
            "the control's whole check ran through the open gate"
        );

        assert_eq!(
            wedged.run_state(),
            RunState::Initializing {
                pause_requested: true
            },
            "the paused torrent's check bailed and left it initializing for good"
        );
        assert_eq!(
            checked_pieces(&wedged),
            Some(1),
            "it stopped at the piece it was already reading, and never took up another"
        );
        assert!(wedged.handle.is_paused(), "and the flag calls that a pause");
    }

    /// The reconciler's stop really stops the fetching, and its start gets
    /// the pieces back.
    ///
    /// This is the half no fake backend can show. The idle pause was the
    /// trait's no-op here, so with `seedingEnabled=false` the engine marked
    /// itself paused and the torrent went on downloading at full rate --
    /// measured at 3 MiB -> 12 MiB in three seconds -- while the free-space
    /// watch, which skipped an engine that claimed to be paused, left it
    /// alone all the way to `ENOSPC`.
    ///
    /// A seeder with a fixed upload rate is what makes "did it stop?"
    /// answerable in a bounded time: the window is first shown to be long
    /// enough by watching progress move across it, and only then used to
    /// assert that it does not.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_reconcilers_stop_stops_fetching_and_its_start_keeps_the_pieces() {
        use crate::backend::TorrentHandle;
        /// The floor on the measurement window. Not a synchronisation
        /// sleep: the window is the longer of this and however long the
        /// unpaused torrent took to fetch its first bytes, so under load
        /// it grows with what it is measuring against.
        const WINDOW: Duration = Duration::from_secs(2);

        let src = tempfile::tempdir().unwrap();
        let payload = src.path().join("payload.bin");
        // Far more than the seeder can push across the whole test, so the
        // torrent is still fetching at every point an assertion is made; a
        // torrent that finished would stop fetching for its own reasons.
        write_payload(&payload, 8 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let seeder = slow_seeder(src.path(), &torrent_bytes, 128 * 1024).await;
        let seeder_addr = seeder.listen_addr().expect("the seeder listens");

        let dl = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.path().to_path_buf())
            .await
            .expect("hermetic session");
        // Straight to the session, for `initial_peers`: with no DHT, no
        // trackers and no LSD, that address is the only peer this session
        // will ever know -- which is also what makes the re-dial after the
        // resume unambiguous.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes.clone())),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(vec![seeder_addr]),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = backend.wrap(inner);

        // The precondition, on its own bound rather than on a fixed
        // window: bytes arrive. How long that takes is also how long the
        // frozen window below has to be, so a loaded runner stretches both
        // together instead of failing the assertion that nothing moved.
        let window = wait_for_a_fetched_byte(&handle).await.max(WINDOW);

        handle
            .stop_torrent()
            .await
            .expect("the reconciler stops it");
        wait_until_paused(&handle).await;
        let at_pause = handle.handle.stats().progress_bytes;
        tokio::time::sleep(window).await;
        let after = handle.handle.stats().progress_bytes;
        assert_eq!(
            after,
            at_pause,
            "a paused torrent fetched {} more bytes across the same {window:?}",
            after.saturating_sub(at_pause)
        );
        assert!(
            !handle.handle.stats().finished,
            "the torrent finished on its own; the payload is too small for this test to mean anything"
        );

        handle
            .start_torrent()
            .await
            .expect("and the reconciler starts it again");
        assert_eq!(handle.run_state(), RunState::Live);
        assert!(
            handle.handle.stats().progress_bytes >= at_pause,
            "the unpause re-checked or re-hashed and lost progress"
        );
        // Back to fetching from the same seeder, on its own bound.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        loop {
            let now = handle.handle.stats().progress_bytes;
            if now > at_pause {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the resumed torrent never fetched another byte (still {now} of {})",
                handle.handle.stats().total_bytes
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait until this torrent has received bytes from a peer, and return
    /// how long that took.
    ///
    /// `fetched` is the cumulative peer-received counter and nothing else
    /// feeds it, where `progress_bytes` is have-bytes over the *selected*
    /// files, which a want-set reconcile moves about. The elapsed time is
    /// the caller's measurement window: a runner too loaded to move bytes
    /// in two seconds is also too loaded for two seconds to say anything
    /// about a torrent that has stopped.
    async fn wait_for_a_fetched_byte(handle: &LibrqbitHandle) -> Duration {
        use crate::backend::TorrentHandle;
        let start = std::time::Instant::now();
        let deadline = start + TEST_WAIT_BOUND;
        loop {
            if TorrentHandle::transfer_totals(handle).fetched > 0 {
                return start.elapsed();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the seeder never fed this torrent, so there is nothing here to stop"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Poll until librqbit has actually parked the torrent. `Session::pause`
    /// asks a torrent still in its initial check to pause when the check
    /// ends, so the state can lag the call.
    async fn wait_until_paused(handle: &LibrqbitHandle) {
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while !handle.handle.is_paused() {
            assert!(
                std::time::Instant::now() < deadline,
                "the torrent never reached the paused state"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The medium finding's own scenario, end to end over a real session:
    /// seeding is off, the stream stops, the idle grace passes -- and the
    /// torrent must not be downloading any more.
    ///
    /// The policy half of this (when the idle arm decides `Stop`) is
    /// pinned by the fake-backend tests in `lib.rs`; what those could not
    /// see is that the call did nothing. Measured before the fix, with the
    /// engine marked
    /// `idle_paused` and the free-space probe pinned at zero bytes free: the
    /// torrent went 3 MiB -> 12 MiB over the next three seconds, because the
    /// free-space watch skips an engine that claims to be paused and nothing
    /// else was stopping it. So this test asserts about the bytes, not about
    /// a counter of trait calls.
    #[tokio::test(flavor = "multi_thread")]
    async fn seeding_off_and_a_stream_that_ended_stops_the_torrent_fetching() {
        /// The floor on the measurement window, as in
        /// `the_idle_pause_stops_fetching_and_the_resume_keeps_the_pieces`.
        const WINDOW: Duration = Duration::from_secs(2);
        let src = tempfile::tempdir().unwrap();
        let payload = src.path().join("payload.bin");
        write_payload(&payload, 8 * 1024 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;
        let seeder = slow_seeder(src.path(), &torrent_bytes, 128 * 1024).await;
        let seeder_addr = seeder.listen_addr().expect("the seeder listens");

        let dl = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.path().to_path_buf())
            .await
            .expect("hermetic session");
        // Straight to the session, for `initial_peers` -- the only peer
        // this session will ever know.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes.clone())),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(vec![seeder_addr]),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = backend.wrap(inner);

        let efs = crate::BackendEngineFS::new_with_backend(
            backend,
            HashMap::from([(hash.clone(), handle.clone())]),
            dl.path().join("cache"),
            dl.path().to_path_buf(),
        );
        // What the user turned off, and what the viewer just did: a stream
        // that starts and then ends.
        efs.seeding_enabled.store(false, Ordering::Relaxed);
        efs.on_stream_start(&hash, 0).await;
        efs.on_stream_end(&hash, 0).await;

        // The precondition: with the stream over and the want-set it left
        // behind, this torrent is still pulling bytes off the seeder --
        // the state the finding describes, and without it there would be
        // nothing here for the pause to stop. On a bound, not a fixed
        // window, and the time it takes is also the width of the frozen
        // window below, so a loaded runner stretches both together.
        let window = wait_for_a_fetched_byte(&handle).await.max(WINDOW);

        // Now the grace, as a clock reading rather than as wall time: this
        // session has real sockets and real check threads, so it cannot run
        // under a paused clock, and sitting out the shipped
        // `INACTIVE_TORRENT_PAUSE_GRACE` would add its whole length to the
        // suite and say nothing more.
        efs.reconcile_tick_at(crate::INACTIVE_TORRENT_PAUSE_GRACE.as_secs() + 1)
            .await;
        assert_eq!(
            handle.run_state(),
            RunState::Paused,
            "seeding is off, the stream is over and the grace has passed, so the \
             reconciler must have stopped the torrent"
        );

        // And the bytes agree with the state. `progress_bytes` is the have
        // count, which a pause keeps and a fetch grows.
        let at_pause = handle.handle.stats().progress_bytes;
        tokio::time::sleep(WINDOW).await;
        let after = handle.handle.stats().progress_bytes;
        assert_eq!(
            after,
            at_pause,
            "an idle-paused torrent fetched {} more bytes across {window:?}",
            after.saturating_sub(at_pause)
        );
        assert!(
            !handle.handle.stats().finished,
            "the torrent finished on its own; the payload is too small for this test \
             to mean anything"
        );
    }

    /// `source_info_hash` reads the same hash the add would manage, from a
    /// real `.torrent` blob and from a magnet, without adding either.
    ///
    /// The evicted-for-space refusal is only as good as this: a hash it
    /// cannot read is a source it waves through, and a wrong one would
    /// refuse the wrong torrent. Checked against the hash `make_torrent`
    /// reports and against a torrent this session really added, so a
    /// spelling difference (case, a `urn:btih:` prefix) shows up here.
    #[tokio::test]
    async fn source_info_hash_reads_a_hash_without_adding_anything() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        assert_eq!(
            backend.source_info_hash(&TorrentSource::Bytes(torrent_bytes.clone())),
            Some(hash.to_lowercase()),
            "a .torrent blob"
        );
        assert_eq!(
            backend.source_info_hash(&TorrentSource::Url(format!("magnet:?xt=urn:btih:{hash}"))),
            Some(hash.to_lowercase()),
            "a magnet link"
        );
        assert_eq!(
            backend.source_info_hash(&TorrentSource::Url(hash.to_uppercase())),
            Some(hash.to_lowercase()),
            "a bare hash, which add_torrent also accepts"
        );
        assert_eq!(
            backend.source_info_hash(&TorrentSource::Bytes(b"not a torrent".to_vec())),
            None,
            "an unparseable blob leaves the real error to the add"
        );
        assert_eq!(
            backend.source_info_hash(&TorrentSource::Url(
                "https://example.invalid/x.torrent".into()
            )),
            None,
            "a .torrent behind a URL has no hash until it is fetched"
        );
        assert!(
            backend.list_torrents().await.is_empty(),
            "and none of that added a torrent"
        );

        // The same hash the add really manages.
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes), vec![])
            .await
            .expect("add torrent");
        assert_eq!(
            crate::backend::TorrentHandle::info_hash(&handle),
            hash.to_lowercase()
        );
    }

    /// `stats().sources` must list the trackers the torrent was added with:
    /// it is the only place a client can confirm its `tr=` trackers reached
    /// the engine, since librqbit cannot add trackers after the fact.
    #[tokio::test]
    async fn stats_sources_list_the_trackers_the_torrent_was_added_with() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(
                TorrentSource::Bytes(torrent_bytes),
                vec![
                    "udp://two.invalid:6969/announce".to_string(),
                    "https://one.invalid/announce".to_string(),
                ],
            )
            .await
            .expect("add torrent");

        let stats = TorrentHandle::stats(&handle).await;
        let urls: Vec<&str> = stats.sources.iter().map(|s| s.url.as_str()).collect();
        assert_eq!(
            urls,
            [
                "https://one.invalid/announce",
                "udp://two.invalid:6969/announce"
            ],
            "sorted tracker URLs, got {urls:?}"
        );
        assert!(stats.sources.iter().all(|s| s.num_requests == 0));
    }

    /// End to end through the stats path: a tracker scrape's counters land on
    /// the matching `sources` entry, and the swarm totals are the max across
    /// the trackers that answered -- a tracker that did not answer reports
    /// nothing at all rather than zero.
    ///
    /// The hermetic backend disables scraping outright, so the handle is
    /// rebuilt around a scraper wired to a stub transport: no socket is
    /// opened here either.
    #[tokio::test]
    async fn stats_report_swarm_counts_from_the_tracker_scrape() {
        use crate::scrape::{ScrapeOutcome, ScrapeTransport, SwarmCounts};

        struct StubTrackers;

        #[async_trait::async_trait]
        impl ScrapeTransport for StubTrackers {
            async fn scrape(&self, tracker: &str, _info_hash: [u8; 20]) -> ScrapeOutcome {
                if tracker.starts_with("https://one") {
                    ScrapeOutcome::Counts(SwarmCounts {
                        seeders: 12,
                        leechers: 4,
                        completed: 900,
                    })
                } else {
                    ScrapeOutcome::Failed
                }
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(
                TorrentSource::Bytes(torrent_bytes),
                vec![
                    "udp://two.invalid:6969/announce".to_string(),
                    "https://one.invalid/announce".to_string(),
                ],
            )
            .await
            .expect("add torrent");
        let handle = LibrqbitHandle {
            swarm_scraper: SwarmScraper::with_transport(Arc::new(StubTrackers)),
            ..handle.clone()
        };

        // The first poll can only schedule the round -- stats never wait on
        // the network -- so poll until it has landed. The bound is only there
        // so a regression fails instead of hanging.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        let stats = loop {
            let stats = TorrentHandle::stats(&handle).await;
            if stats.swarm_seeders.is_some() || std::time::Instant::now() >= deadline {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        assert_eq!(
            stats.swarm_seeders,
            Some(12),
            "max over the answering trackers"
        );
        assert_eq!(stats.swarm_leechers, Some(4));
        assert!(
            stats.swarm_scrape_age_secs.is_some(),
            "figures carry an age: {:?}",
            stats.swarm_scrape_age_secs
        );

        let scraped = stats
            .sources
            .iter()
            .find(|s| s.url.starts_with("https://one"))
            .expect("the scraped tracker is listed");
        assert_eq!(scraped.seeders, Some(12));
        assert_eq!(scraped.leechers, Some(4));
        assert_eq!(scraped.completed, Some(900));

        let unanswered = stats
            .sources
            .iter()
            .find(|s| s.url.starts_with("udp://two"))
            .expect("the failing tracker is still listed");
        assert_eq!(unanswered.seeders, None, "a failed scrape is not a zero");
        assert_eq!(unanswered.leechers, None);
        assert_eq!(unanswered.completed, None);
    }

    /// `connected_seeders` is librqbit's `live_seeders` aggregate -- connected
    /// peers whose bitfield covers the whole torrent -- and not one of the
    /// discovery counters that sit beside it in the same struct.
    ///
    /// No peer can ever go live in a hermetic session (`new_for_tests` binds
    /// no port and runs no DHT), so the count itself stays 0 here; the live
    /// case is `wait_for_piece_ready_live_swarm`, which needs a real swarm and
    /// is `#[ignore]`d. What is pinned down is the wiring: `initial_peers`
    /// hands the torrent one address that will never answer, so `seen` and
    /// `unique` climb off zero while `live_seeders` does not -- a mapping to
    /// the wrong counter reports that address as a seeder.
    #[tokio::test(flavor = "multi_thread")]
    async fn stats_connected_seeders_mirrors_librqbits_live_seeder_count() {
        let src = tempfile::tempdir().unwrap();
        let payload = src.path().join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        // A download dir of its own, so the torrent is a leecher with nothing
        // on disk rather than an instantly-finished seed.
        let dl = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.path().to_path_buf())
            .await
            .expect("hermetic session");
        // Straight to the session: `add_torrent` has no `initial_peers`
        // parameter, and with no DHT and no trackers that is the only way a
        // peer address can reach the torrent at all. Loopback port 9 is the
        // discard port -- nothing listens there, so the peer is seen and then
        // dies without ever going live.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(vec![(std::net::Ipv4Addr::LOCALHOST, 9).into()]),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = LibrqbitHandle {
            info_hash: inner.info_hash().as_string(),
            handle: inner,
            session: backend.session.clone(),
            deferred_selections: Default::default(),
            pinned_files: Default::default(),
            stream_positions: Default::default(),
            reported_errors: Default::default(),
            swarm_scraper: SwarmScraper::disabled(),
        };

        // Bounded poll: the initial peer reaches the peer list once the
        // torrent has finished initializing and gone live. The bound only
        // keeps a regression from hanging.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        let live_seeders = loop {
            let peer_stats = handle.handle.stats().live.map(|l| l.snapshot.peer_stats);
            match peer_stats {
                Some(p) if p.seen > 0 => break p.live_seeders,
                _ => assert!(
                    std::time::Instant::now() < deadline,
                    "the torrent never saw its initial peer"
                ),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(live_seeders, 0, "an address nobody answers is not a seeder");

        let stats = TorrentHandle::stats(&handle).await;
        assert_eq!(
            stats.connected_seeders, live_seeders as u64,
            "connected_seeders must be librqbit's live_seeders"
        );
        assert!(
            stats.unique > 0,
            "the initial peer must have been seen, or the counters are all \
             trivially equal and this proves nothing"
        );
        assert_ne!(
            stats.connected_seeders, stats.unique,
            "connected_seeders is the seeder count, not the seen-peer count"
        );
    }

    /// The magnet branch of librqbit's `Session::add_torrent` ignores
    /// `AddTorrentOptions::trackers`, so the trackers have to be `tr=` params
    /// of the URL it is given -- percent-encoded, one per tracker, exactly as
    /// its `Magnet::parse` reads them back.
    #[test]
    fn magnet_with_trackers_encodes_each_tracker_as_a_tr_param() {
        let trackers = vec![
            "udp://one.invalid:6969/announce".to_string(),
            "https://two.invalid/announce?x=100%25".to_string(),
        ];
        let magnet = magnet_with_trackers(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            &trackers,
        );
        assert_eq!(
            magnet,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
             &tr=udp%3A%2F%2Fone.invalid%3A6969%2Fannounce\
             &tr=https%3A%2F%2Ftwo.invalid%2Fannounce%3Fx%3D100%2525"
        );
        // What librqbit will actually see: `Magnet::parse` collects `tr`.
        let parsed = librqbit::Magnet::parse(&magnet).expect("valid magnet");
        assert_eq!(parsed.trackers, trackers);
        assert_eq!(
            parsed.as_id20().unwrap().as_string(),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn magnet_with_trackers_keeps_existing_trs_and_skips_duplicates() {
        let magnet = magnet_with_trackers(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=x&tr=udp%3A%2F%2Fone.invalid%2Fannounce",
            &[
                "udp://one.invalid/announce".to_string(),
                "udp://two.invalid/announce".to_string(),
            ],
        );
        let parsed = librqbit::Magnet::parse(&magnet).expect("valid magnet");
        assert_eq!(
            parsed.trackers,
            ["udp://one.invalid/announce", "udp://two.invalid/announce"]
        );
        assert_eq!(parsed.name.as_deref(), Some("x"));
    }

    #[test]
    fn magnet_with_trackers_upgrades_bare_hashes_and_leaves_other_urls_alone() {
        let trackers = vec!["udp://one.invalid/announce".to_string()];
        let magnet = magnet_with_trackers("0123456789abcdef0123456789abcdef01234567", &trackers);
        assert_eq!(
            magnet,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=udp%3A%2F%2Fone.invalid%2Fannounce"
        );
        assert_eq!(
            magnet_with_trackers("https://example.invalid/a.torrent", &trackers),
            "https://example.invalid/a.torrent"
        );
        assert_eq!(
            magnet_with_trackers(
                "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                &[]
            ),
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        );
    }

    /// End-to-end check of the same thing against a real session: a magnet
    /// added with a custom tracker must list it in `stats().sources` once
    /// metadata has resolved. Needs peers, so it runs manually:
    ///
    /// ```sh
    /// STREAM_SERVER_TEST_MAGNET='magnet:?xt=urn:btih:...' \
    ///     cargo test -p enginefs --release magnet_add_keeps_custom_trackers_live_swarm -- --ignored --nocapture
    /// ```
    #[ignore = "requires network and STREAM_SERVER_TEST_MAGNET; see doc comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn magnet_add_keeps_custom_trackers_live_swarm() {
        let magnet = std::env::var("STREAM_SERVER_TEST_MAGNET")
            .expect("set STREAM_SERVER_TEST_MAGNET to a magnet link");
        let tmp = tempfile::tempdir().unwrap();
        let (backend, _restored) = LibrqbitBackend::new(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::production_in(tmp.path()),
        )
        .await
        .expect("network session");
        let custom = "udp://custom-tracker.invalid:6969/announce".to_string();
        let handle = backend
            .add_torrent(TorrentSource::Url(magnet), vec![custom.clone()])
            .await
            .expect("add magnet");
        let stats = TorrentHandle::stats(&handle).await;
        let urls: Vec<&str> = stats.sources.iter().map(|s| s.url.as_str()).collect();
        assert!(urls.contains(&custom.as_str()), "sources: {urls:?}");
    }

    #[tokio::test]
    async fn add_get_list_remove_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, expected_hash) = make_torrent(&payload).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;

        // add_torrent must report the real info hash, not a placeholder.
        assert_eq!(handle.info_hash, expected_hash);
        handle.handle.wait_until_initialized().await.unwrap();

        let listed = backend.list_torrents().await;
        assert_eq!(listed, vec![expected_hash.clone()]);

        let got = backend.get_torrent(&expected_hash).await;
        assert!(got.is_some());
        assert_eq!(got.unwrap().info_hash, expected_hash);

        // Unknown but well-formed hash -> None; garbage -> None.
        let missing_hash = "0".repeat(40);
        assert!(backend.get_torrent(&missing_hash).await.is_none());
        assert!(backend.get_torrent("not-a-hash").await.is_none());

        backend.remove_torrent(&expected_hash).await.unwrap();
        assert!(backend.list_torrents().await.is_empty());
        assert!(backend.get_torrent(&expected_hash).await.is_none());
        // Removing again is an error (matches libtorrent's not-found Err).
        assert!(backend.remove_torrent(&expected_hash).await.is_err());
    }

    #[test]
    fn file_progress_fields_maps_have_bytes() {
        // Normal partial progress.
        assert_eq!(file_progress_fields(100, 25), (25, 0.25));
        // Complete.
        assert_eq!(file_progress_fields(100, 100), (100, 1.0));
        // have > len is clamped (last-piece rounding in the chunk tracker).
        assert_eq!(file_progress_fields(100, 120), (100, 1.0));
        // Zero-length files are trivially complete.
        assert_eq!(file_progress_fields(0, 0), (0, 1.0));
        // Initializing torrents report an empty file_progress vec -> have = 0.
        assert_eq!(file_progress_fields(100, 0), (0, 0.0));
    }

    /// The classifier is fed the exact chain librqbit e314d8b builds, in the
    /// exact shape `TorrentStats.error` renders it. The unix arm is text --
    /// there is no `std::io::Error` in that chain at all, because the write
    /// goes through `nix::sys::uio::pwritev` and the cause is a
    /// `nix::errno::Errno` -- and the string below is what that `Errno`'s
    /// `Display` produces, reproduced by driving the same call chain at
    /// `/dev/full` and matching the field log verbatim. The Windows arm is
    /// the kind, since `std`'s message there shares no words with the unix
    /// one.
    #[test]
    fn out_of_space_is_told_apart_from_every_other_torrent_error() {
        let field_log = anyhow::anyhow!("ENOSPC: No space left on device")
            .context("error calling pwritev")
            .context("error writing to file 0 (\"movie.mkv\")");
        assert!(is_out_of_space(&field_log));

        // The same errno arriving as a `std::io::Error` -- the path Windows
        // takes, and what any other caller in the chain would produce.
        let by_kind = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::StorageFull))
            .context("error writing to file 0");
        assert!(is_out_of_space(&by_kind));

        // A bare cause, no context wrapped around it.
        assert!(is_out_of_space(&anyhow::anyhow!(
            "ENOSPC: No space left on device"
        )));

        // Everything else stays fatal: reclaiming space would not help, and
        // restarting would loop.
        for other in [
            "error writing to file 0: Permission denied",
            "ENOENT: No such file or directory",
            "checksum mismatch for piece 12",
            "error opening /data/cache/movie.mkv",
        ] {
            assert!(
                !is_out_of_space(&anyhow::anyhow!("{other}").context("error writing to file 0")),
                "{other}"
            );
        }
    }

    #[test]
    fn startup_phase_maps_librqbit_states() {
        use librqbit::TorrentStatsState as S;
        // Missing metadata wins regardless of state.
        assert_eq!(
            startup_phase(false, &S::Live, false),
            StartupPhase::ResolvingMetadata
        );
        assert_eq!(
            startup_phase(false, &S::Initializing { paused: false }, false),
            StartupPhase::ResolvingMetadata
        );
        // Hash check, paused or not.
        assert_eq!(
            startup_phase(true, &S::Initializing { paused: false }, false),
            StartupPhase::Checking
        );
        assert_eq!(
            startup_phase(true, &S::Initializing { paused: true }, false),
            StartupPhase::Checking
        );
        // Piece map exists: ready only when the whole torrent is finished
        // (per-file refinement happens in EngineStats::focus_stream_file).
        assert_eq!(
            startup_phase(true, &S::Live, false),
            StartupPhase::Buffering
        );
        assert_eq!(startup_phase(true, &S::Live, true), StartupPhase::Ready);
        assert_eq!(
            startup_phase(true, &S::Paused, false),
            StartupPhase::Buffering
        );
        assert_eq!(startup_phase(true, &S::Paused, true), StartupPhase::Ready);
        assert_eq!(startup_phase(true, &S::Error, false), StartupPhase::Error);
    }

    /// A seeded torrent (payload already in the download dir) is `ready`
    /// with its whole initial window on disk, straight from librqbit's have
    /// bitfield via `api_dump_haves`.
    #[tokio::test]
    async fn stats_phase_ready_with_full_initial_window_for_seeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let mut stats = TorrentHandle::stats(&handle).await;
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.checked_bytes, None);
        // Window is clamped to the (small) file length.
        assert_eq!(stats.files[0].initial_window_bytes, Some(payload_len));
        assert_eq!(stats.files[0].initial_window_ready_bytes, Some(payload_len));
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.initial_window_ready_bytes, Some(payload_len));
        assert_eq!(stats.initial_window_bytes, Some(payload_len));
    }

    /// The offline restart, as the activity light sees it. A torrent whose
    /// data is already on disk is added to a session with no network -- the
    /// shape of every restored torrent at startup -- and librqbit's initial
    /// check reads all of it back to hash it. That read is what lit the
    /// light when the storage was the counter; through the connection's own
    /// counters nothing moves, and the light has to stay dark on both sides
    /// while the check runs and once it is over. The counter that *would*
    /// have lit it is asserted alongside, so the trap stays named: the
    /// stats' `downloaded` ends at the payload's length without a peer ever
    /// having been asked for a byte.
    #[tokio::test]
    async fn the_initial_check_moves_nothing_over_the_connection() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        // Sampled while the check may still be running: the first reading
        // the light takes after a restart is exactly this one.
        assert_eq!(handle.transfer_totals(), TransferTotals::default());
        handle.handle.wait_until_initialized().await.unwrap();
        assert_eq!(
            handle.transfer_totals(),
            TransferTotals::default(),
            "the initial check read the whole payload back from disk; none of it crossed \
             the connection"
        );

        let stats = TorrentHandle::stats(&handle).await;
        assert_eq!(
            stats.phase,
            StartupPhase::Ready,
            "the check did run to the end"
        );
        assert_eq!(
            stats.downloaded, payload_len,
            "have-bytes grew to the whole payload with no peer involved -- which is why \
             `downloaded` could not be the light's counter"
        );
    }

    /// The startup window follows the reader, and the piece length is
    /// reported alongside it. Anchored at the file head, the window
    /// described bytes nobody was fetching after a seek -- it sat at 0%
    /// while the seek region streamed perfectly -- and without the piece
    /// length a client cannot tell a slow download from a window that is
    /// simply smaller than one piece and can only read 0% or 100%.
    #[tokio::test]
    async fn stats_window_follows_the_reader_and_reports_the_piece_length() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        let piece_length = stats.piece_length.expect("metadata is resolved");
        assert!(piece_length > 0);
        assert_eq!(
            stats.files[0].initial_window_bytes,
            Some(payload_len),
            "a fresh torrent is measured from the head"
        );

        // Open a reader part-way in, as a `Range` request or a seek does.
        // Start on a piece boundary so the expected window is exactly the
        // tail, whatever the fixture's piece length turns out to be.
        let seek_to = payload_len - piece_length;
        let _reader = handle
            .get_file_reader(
                0,
                seek_to,
                0,
                None,
                crate::backend::priorities::PlaybackIntent::DirectSeek,
                crate::backend::priorities::BufferProfile::Normal,
            )
            .await
            .unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert_eq!(
            stats.files[0].initial_window_bytes,
            Some(piece_length),
            "the window is what the reader is waiting for, not the head"
        );
        assert_eq!(stats.piece_length, Some(piece_length));
    }

    /// Sub-piece progress for the piece the reader is sitting on: without it
    /// a player waiting on a single 16 MiB piece can only ever be shown 0%
    /// or 100%, because whole verified pieces are all the have-bitfield can
    /// say. Also the honesty cases -- absence, not zero, when no reader is
    /// open -- and the short last piece, whose bytes are not
    /// `chunks * 16 KiB`.
    #[tokio::test]
    async fn stats_report_the_progress_of_the_piece_the_reader_waits_for() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        // Two whole 64 KiB pieces (four 16 KiB chunks each) plus a short
        // last piece of 5000 bytes -- less than one chunk, so multiplying
        // its single chunk by the chunk size would overstate it by 11384.
        let piece_length = 64 * 1024u64;
        let last_piece_len = 5000u64;
        let payload_len = 2 * piece_length + last_piece_len;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) =
            make_torrent_with_piece_length(&payload, piece_length as u32).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();
        assert_eq!(
            TorrentHandle::stats(&handle).await.piece_length,
            Some(piece_length)
        );

        // Nothing has opened the file, so nothing is in flight for it: a
        // client must be able to tell that from "0 bytes of the piece".
        let mut stats = TorrentHandle::stats(&handle).await;
        assert_eq!(stats.files[0].in_flight_piece, None);
        stats.focus_stream_file(0);
        assert_eq!(stats.in_flight_piece, None);

        // A reader at the head waits on piece 0, which this fixture has
        // fully on disk: complete *and* verified, the only state in which a
        // client may treat it as ready.
        let _reader = handle
            .get_file_reader(
                0,
                0,
                0,
                None,
                crate::backend::priorities::PlaybackIntent::DirectInitial,
                crate::backend::priorities::BufferProfile::Normal,
            )
            .await
            .unwrap();
        let mut stats = TorrentHandle::stats(&handle).await;
        let head = stats.files[0].in_flight_piece.expect("a reader is open");
        assert_eq!(head.index, 0);
        assert_eq!(head.total_bytes, piece_length);
        assert_eq!(head.downloaded_bytes, piece_length);
        assert!(head.verified);
        // The same piece reaches the top level for the focused file, which
        // is what both stats.json routes serve.
        stats.focus_stream_file(0);
        assert_eq!(stats.in_flight_piece, Some(head));

        // Seek into the short last piece. Its bytes are the piece's real
        // length, not its one chunk rounded up to 16 KiB.
        let _reader = handle
            .get_file_reader(
                0,
                payload_len - 1,
                0,
                None,
                crate::backend::priorities::PlaybackIntent::DirectSeek,
                crate::backend::priorities::BufferProfile::Normal,
            )
            .await
            .unwrap();
        let stats = TorrentHandle::stats(&handle).await;
        let tail = stats.files[0].in_flight_piece.expect("a reader is open");
        assert_eq!(tail.index, 2);
        assert_eq!(tail.total_bytes, last_piece_len);
        assert_eq!(tail.downloaded_bytes, last_piece_len);
        assert!(tail.verified);
    }

    /// A piece nobody has sent us yet is reported as a real 0-of-N, with
    /// `verified` false -- never as absence (which means "we do not know")
    /// and never as ready.
    #[tokio::test]
    async fn stats_never_report_an_unverified_piece_as_ready() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        let piece_length = 64 * 1024u64;
        write_payload(&payload, (2 * piece_length) as usize).await;
        let (torrent_bytes, _hash) =
            make_torrent_with_piece_length(&payload, piece_length as u32).await;

        // Empty download dir: the torrent has metadata and a chunk map, but
        // not one byte of data.
        let download_dir = tmp.path().join("dl");
        let (_backend, handle) = backend_with_torrent(&download_dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();
        let _reader = handle
            .get_file_reader(
                0,
                0,
                0,
                None,
                crate::backend::priorities::PlaybackIntent::DirectInitial,
                crate::backend::priorities::BufferProfile::Normal,
            )
            .await
            .unwrap();

        let mut stats = TorrentHandle::stats(&handle).await;
        stats.focus_stream_file(0);
        let piece = stats.in_flight_piece.expect("a reader is open");
        assert_eq!(piece.index, 0);
        assert_eq!(piece.downloaded_bytes, 0);
        assert_eq!(piece.total_bytes, piece_length);
        assert!(!piece.verified);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_ready_bytes, Some(0));
    }

    /// An unseeded torrent with no peers sits in `buffering` with an empty
    /// initial window, and its peer-discovery counters are all zero.
    #[tokio::test]
    async fn stats_phase_buffering_with_empty_initial_window_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        let payload_len = 64 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let download_dir = tmp.path().join("dl");
        let (_backend, handle) = backend_with_torrent(&download_dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let mut stats = TorrentHandle::stats(&handle).await;
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.files[0].initial_window_bytes, Some(payload_len));
        assert_eq!(stats.files[0].initial_window_ready_bytes, Some(0));
        assert_eq!(stats.peer_discovery, PeerDiscovery::default());
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_ready_bytes, Some(0));
        assert_eq!(stats.initial_window_bytes, Some(payload_len));
    }

    #[tokio::test]
    async fn stats_report_full_progress_for_seeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(stats.has_metadata);
        assert!(stats.is_finished);
        assert_eq!(stats.downloaded, payload_len);
        assert!((stats.stream_progress - 1.0).abs() < f64::EPSILON);
        assert_eq!(stats.files.len(), 1);
        assert_eq!(stats.files[0].length, payload_len);
        assert_eq!(stats.files[0].downloaded, payload_len);
        assert!((stats.files[0].progress - 1.0).abs() < f64::EPSILON);

        assert!(TorrentHandle::is_finished(&handle).await);
        assert!(handle.is_file_complete(0).await);
        assert!(!handle.is_file_complete(1).await, "out-of-range file");
    }

    #[tokio::test]
    async fn stats_report_zero_progress_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        // Create the torrent from a payload OUTSIDE the download dir so the
        // session has none of the data.
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        let payload_len = 64 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let download_dir = tmp.path().join("dl");
        let (_backend, handle) = backend_with_torrent(&download_dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(stats.has_metadata);
        assert!(!stats.is_finished);
        assert_eq!(stats.downloaded, 0);
        assert_eq!(stats.files.len(), 1);
        assert_eq!(stats.files[0].downloaded, 0);
        assert_eq!(stats.files[0].progress, 0.0);

        assert!(!TorrentHandle::is_finished(&handle).await);
        assert!(!handle.is_file_complete(0).await);
    }

    // Multi-thread flavor: FileStream reads go through block_in_place.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_is_ready_on_seeded_torrent() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "seeded torrent must be ready: {}", r.reason);
        assert_eq!(r.reason, "stream-read");
        assert_eq!(r.piece, 0);
        assert_eq!((r.ready_pieces, r.target_pieces), (1, 1));

        // Mid-file offset: piece index = offset / piece_length (single-file
        // torrent, so the file starts at torrent offset 0).
        let offset = 40_000u64;
        let r = handle
            .wait_for_piece_ready(
                0,
                offset,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "mid-file offset must be ready: {}", r.reason);
        assert_eq!(r.piece, (offset / 16384) as i32);
    }

    // Exercises the get_file_reader -> stream_with_options wiring: every intent
    // must produce a positive lookahead window (stream_with_options asserts
    // lookahead_bytes > 0), so a successful open+read confirms the intent-sized
    // window is applied rather than rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_file_reader_applies_intent_sized_lookahead() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use tokio::io::AsyncReadExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        // A narrow-window intent (4 MiB) and a wide-window intent (128 MiB)
        // both yield a readable stream.
        for intent in [PlaybackIntent::DirectInitial, PlaybackIntent::DirectSeek] {
            // Sanity: the helper the reader uses is positive for this intent.
            assert!(
                crate::backend::priorities::librqbit_stream_lookahead_bytes(
                    intent,
                    BufferProfile::Normal
                ) > 0,
                "lookahead must be positive for {intent:?}"
            );
            let mut reader = handle
                .get_file_reader(0, 0, 100, None, intent, BufferProfile::Normal)
                .await
                .unwrap_or_else(|e| panic!("get_file_reader failed for {intent:?}: {e:#}"));
            let mut buf = [0u8; 1];
            let n = reader.read(&mut buf).await.expect("read first byte");
            assert_eq!(n, 1, "seeded file must yield a byte for {intent:?}");
            assert_eq!(buf[0], 0, "first payload byte is (0 % 251) == 0");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_times_out_without_peers() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        write_payload(&payload, 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&tmp.path().join("dl"), &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let timeout = Duration::from_millis(300);
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                timeout,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "timeout");
        assert!(r.elapsed_ms >= 300, "elapsed_ms = {}", r.elapsed_ms);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_rejects_bad_targets() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        // Offset past the end of the file: soft failure, not Err.
        let r = handle
            .wait_for_piece_ready(
                0,
                1_000_000,
                Duration::from_secs(1),
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "piece-out-of-file-range");

        // Bad file index: structural failure -> Err.
        assert!(
            handle
                .wait_for_piece_ready(
                    7,
                    0,
                    Duration::from_secs(1),
                    PlaybackIntent::DirectInitial,
                    BufferProfile::Normal,
                )
                .await
                .is_err()
        );
    }

    /// Real-swarm integration test for the piece-yank path (needs actual
    /// piece download from live peers, which the hermetic harness cannot
    /// provide). Run manually with a well-seeded magnet link:
    ///
    /// ```sh
    /// STREAM_SERVER_TEST_MAGNET='magnet:?xt=urn:btih:...' \
    ///     cargo test -p enginefs --release wait_for_piece_ready_live_swarm -- --ignored --nocapture
    /// ```
    ///
    /// Uses a network-enabled session (DHT on, real listen port), so it must
    /// stay #[ignore]d in CI.
    #[ignore = "requires network and STREAM_SERVER_TEST_MAGNET; see doc comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_live_swarm() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let magnet = std::env::var("STREAM_SERVER_TEST_MAGNET")
            .expect("set STREAM_SERVER_TEST_MAGNET to a magnet link");
        let tmp = tempfile::tempdir().unwrap();
        let (backend, _restored) = LibrqbitBackend::new(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::production_in(tmp.path()),
        )
        .await
        .expect("network session");
        let handle = backend
            .add_torrent(TorrentSource::Url(magnet), vec![])
            .await
            .expect("add magnet");
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                Duration::from_secs(120),
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .expect("structural failure");
        eprintln!("readiness: {r:?}");
        assert!(r.ready, "first piece did not arrive: {}", r.reason);
        assert_eq!(r.reason, "stream-read");
    }

    #[tokio::test(start_paused = true)]
    async fn await_initialized_blocks_until_ready_then_succeeds() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let wait = {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        let gate = tokio::spawn(await_initialized("abc", Duration::from_secs(60), wait));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!gate.is_finished(), "gate must block while initializing");
        notify.notify_one();
        gate.await.unwrap().expect("gate opens once initialized");
    }

    #[tokio::test(start_paused = true)]
    async fn await_initialized_times_out_and_reports_failures() {
        let never = std::future::pending::<anyhow::Result<()>>();
        let started = tokio::time::Instant::now();
        let err = await_initialized("abc", Duration::from_secs(5), never)
            .await
            .expect_err("must time out");
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        match err {
            TorrentInitError::TimedOut {
                info_hash,
                timeout_secs,
            } => {
                assert_eq!(info_hash, "abc");
                assert_eq!(timeout_secs, 5);
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }

        let failed = async { Err(anyhow::anyhow!("disk exploded")) };
        let err = await_initialized("abc", Duration::from_secs(5), failed)
            .await
            .expect_err("init failure must propagate");
        match err {
            TorrentInitError::Failed { info_hash, reason } => {
                assert_eq!(info_hash, "abc");
                assert_eq!(reason, "disk exploded");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_selection_coalesces_and_applies_after_gate() {
        use std::sync::atomic::AtomicUsize;
        let applied = Arc::new(parking_lot::Mutex::new(Vec::<u32>::new()));
        let applies = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let slot: Arc<DeferredSelection<u32>> = DeferredSelection::new();

        let gate = |notify: &Arc<tokio::sync::Notify>| {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        let apply = |applied: &Arc<parking_lot::Mutex<Vec<u32>>>, applies: &Arc<AtomicUsize>| {
            let applied = applied.clone();
            let applies = applies.clone();
            move |op: u32| {
                let applied = applied.clone();
                let applies = applies.clone();
                async move {
                    applies.fetch_add(1, Ordering::SeqCst);
                    applied.lock().push(op);
                }
            }
        };

        slot.defer(1, gate(&notify), apply(&applied, &applies));
        slot.defer(2, gate(&notify), apply(&applied, &applies));
        slot.defer(3, gate(&notify), apply(&applied, &applies));
        assert!(slot.has_pending());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            applies.load(Ordering::SeqCst),
            0,
            "nothing applies before the gate"
        );

        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            *applied.lock(),
            vec![3],
            "latest op wins, older ones coalesce away"
        );
        assert!(!slot.has_pending());

        // A direct apply supersedes a parked op.
        slot.defer(4, gate(&notify), apply(&applied, &applies));
        assert_eq!(slot.supersede(), Some(4));
        assert!(!slot.has_pending());
        // Let the spawned waiter register on the Notify before waking it
        // (notify_waiters only reaches already-registered waiters).
        tokio::time::sleep(Duration::from_secs(1)).await;
        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*applied.lock(), vec![3]);

        // A failed gate drops the parked op with a warning instead of applying.
        let failing = async {
            Err(TorrentInitError::TimedOut {
                info_hash: "x".into(),
                timeout_secs: 1,
            })
        };
        slot.defer(5, failing, apply(&applied, &applies));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*applied.lock(), vec![3]);
        assert!(!slot.has_pending());
    }

    /// Regression for the lost-update edge in the waiter's Err path: a
    /// `defer` landing between the waiter's `take` and its
    /// `waiter_running.store(false, ..)` must not be swallowed by a loop
    /// that keeps re-checking it against the (now stale) gate result -- the
    /// waiter must hand the slot back so the *next* `defer` spawns a fresh
    /// waiter with a fresh gate instead.
    ///
    /// The real race is between two OS threads landing on two adjacent,
    /// non-yielding instructions (a `Mutex::take` and an `AtomicBool::store`),
    /// which cannot be reproduced deterministically through async task
    /// scheduling -- nothing yields in between for another task to run. So
    /// this test reconstructs the exact interleaving by hand, one step at a
    /// time (this test module is a descendant of the defining module, so
    /// `DeferredSelection`'s fields are visible): it performs the waiter's
    /// `take` (finding nothing, as if it had just drained everything), then
    /// -- standing in for the racing thread -- calls the real `defer` to
    /// queue a fresh op while `waiter_running` is still true, exactly as
    /// `defer` itself does when it observes an already-running waiter, and
    /// only then performs the waiter's release. This is the one ordering
    /// `handle_gate_error`'s single, non-looping `take` can never itself
    /// reproduce (it only ever takes once, before it releases), which is
    /// exactly why the fix is correct: unlike the old drain-and-recheck loop,
    /// there is no code path left that could re-take and drop this op under
    /// the stale verdict.
    #[tokio::test(start_paused = true)]
    async fn deferred_selection_defer_racing_gate_error_is_not_lost() {
        let slot: Arc<DeferredSelection<u32>> = DeferredSelection::new();
        let applied = Arc::new(parking_lot::Mutex::new(Vec::<u32>::new()));
        let apply = {
            let applied = applied.clone();
            move |op: u32| {
                let applied = applied.clone();
                async move {
                    applied.lock().push(op);
                }
            }
        };

        // A waiter is running and has just taken the last op it had (or
        // started with none) -- `pending` is empty, `waiter_running` is true.
        slot.waiter_running.store(true, Ordering::Release);
        assert!(slot.pending.lock().take().is_none());

        // Right here, in the gap before the waiter releases the slot, a real
        // `defer` call races in with a fresh op. It observes `waiter_running`
        // still true, so it queues the op and returns without spawning a
        // second waiter -- trusting the existing one to drain it.
        let never_used_gate = std::future::pending::<std::result::Result<(), TorrentInitError>>();
        slot.defer(7, never_used_gate, apply.clone());
        assert!(
            slot.has_pending(),
            "the racing defer must have queued its op"
        );

        // The waiter concludes its Err verdict and releases the slot -- the
        // exact tail `handle_gate_error` performs after taking whatever was
        // pending *at the time it ran* (nothing, in this interleaving).
        slot.waiter_running.store(false, Ordering::Release);

        // Op 7 must have survived: nothing re-took it under the stale Err,
        // and the slot must be free so the next trigger spawns a fresh
        // waiter with a fresh gate.
        assert!(
            slot.has_pending(),
            "op queued during the error handoff must not be dropped"
        );
        assert!(
            !slot.waiter_running.load(Ordering::Acquire),
            "the slot must be free for the next defer to spawn a fresh waiter"
        );

        // The next trigger (as a subsequent reconcile/prepare call would
        // issue) must pick it up and apply it -- proving the op is not
        // permanently stranded, only deferred to that next trigger.
        let notify = Arc::new(tokio::sync::Notify::new());
        let gate = {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        slot.defer(7, gate, apply);
        tokio::time::sleep(Duration::from_secs(1)).await;
        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            *applied.lock(),
            vec![7],
            "op queued during the error handoff must still be applied by the next waiter"
        );
        assert!(!slot.has_pending());
    }

    /// Regression for the first-play readiness race: prepare/reconcile and the
    /// reader are called the instant the torrent is added, without the test
    /// waiting for `wait_until_initialized` first. The gate must make the
    /// selection stick and the reader open regardless of whether librqbit is
    /// still hash-checking (8 MiB of payload widens that window).
    #[tokio::test(flavor = "multi_thread")]
    async fn selection_and_reader_wait_for_initializing_torrent() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        use tokio::io::AsyncReadExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        let was_initializing = handle.is_initializing();

        // Reconcile (request-path activation) defers while initializing ...
        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(1),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        // ... and prepare blocks until the torrent is ready, then applies.
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert!(!handle.is_initializing());
        assert_eq!(handle.handle.only_files(), Some(vec![1]));

        let mut reader = handle
            .get_file_reader(
                1,
                0,
                1,
                None,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .expect("reader opens after initialization");
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf[0], 0);

        // A deferred reconcile that raced initialization must settle to the
        // latest selection state, never leave a stale parked op behind.
        let slot = handle.deferred_selection();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while slot.has_pending() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!slot.has_pending(), "deferred op must drain after init");
        assert_eq!(handle.handle.only_files(), Some(vec![1]));
        assert!(
            was_initializing,
            "test must actually exercise the initializing-torrent gate, not degrade into a \
             happy-path test where the torrent was already ready by the first call"
        );
    }

    /// A reconcile deferred during initialization is applied afterwards even
    /// when no request-path prepare follows it.
    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_reconcile_applies_on_real_torrent() {
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;

        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(0),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while handle.handle.only_files() != Some(vec![0]) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.handle.only_files(), Some(vec![0]));
        assert!(!handle.deferred_selection().has_pending());
    }

    #[test]
    fn plan_only_files_rules() {
        use SelectionOp::*;
        // Nothing pinned: the pre-pin rules hold unchanged.
        let none = BTreeSet::new();
        let plan = |current: Option<&[usize]>, file_count: usize, op: SelectionOp| {
            plan_only_files(current, file_count, &none, op)
        };
        let set = |v: &[usize]| Some(v.iter().copied().collect::<HashSet<usize>>());

        // Single-file torrents: never touch selection, for any op.
        assert_eq!(plan(None, 1, Prepare(0)), None);
        assert_eq!(plan(Some(&[0]), 1, Clear(0)), None);
        assert_eq!(
            plan(
                None,
                0,
                Reconcile {
                    active: Some(0),
                    hot: None
                }
            ),
            None
        );

        // Prepare: exclusive selection.
        assert_eq!(plan(None, 3, Prepare(1)), set(&[1]));
        assert_eq!(plan(Some(&[0, 2]), 3, Prepare(1)), set(&[1]));
        // Prepare out of range: apply nothing.
        assert_eq!(plan(None, 3, Prepare(3)), None);

        // Clear: refuse to empty the set.
        assert_eq!(plan(Some(&[1]), 3, Clear(1)), None);
        // Clear of a stale file after a switch: no-op.
        assert_eq!(plan(Some(&[0]), 3, Clear(1)), None);
        // Clear with no selection at all: no-op.
        assert_eq!(plan(None, 3, Clear(1)), None);
        // Clear leaving a non-empty remainder applies it.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(1)), set(&[0]));

        // Reconcile: union of active and hot, never empty.
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: Some(0),
                    hot: Some(2)
                }
            ),
            set(&[0, 2])
        );
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            None
        );
        // Out-of-range indices are dropped; empty result applies nothing.
        assert_eq!(
            plan(
                None,
                3,
                Reconcile {
                    active: Some(9),
                    hot: Some(1)
                }
            ),
            set(&[1])
        );
        assert_eq!(
            plan(
                None,
                3,
                Reconcile {
                    active: Some(9),
                    hot: None
                }
            ),
            None
        );
        // Pin adds to the current selection; with nothing selected ("all
        // wanted") it narrows to just the pin. Out of range: nothing.
        assert_eq!(plan(Some(&[1]), 3, Pin(2)), set(&[1, 2]));
        assert_eq!(plan(None, 3, Pin(2)), set(&[2]));
        assert_eq!(plan(None, 3, Pin(3)), None);
    }

    /// The pinned set is unioned into every plan, so playback switching
    /// (exclusive `Prepare`, `Reconcile` to another file, `Clear` after a
    /// stream ends) never deselects an offline download.
    #[test]
    fn plan_only_files_unions_pinned_into_every_branch() {
        use SelectionOp::*;
        let pinned: BTreeSet<usize> = [0].into_iter().collect();
        let plan = |current: Option<&[usize]>, file_count: usize, op: SelectionOp| {
            plan_only_files(current, file_count, &pinned, op)
        };
        let set = |v: &[usize]| Some(v.iter().copied().collect::<HashSet<usize>>());

        // Single-file torrents: still never touched, pinned or not.
        assert_eq!(plan(None, 1, Prepare(0)), None);
        assert_eq!(plan(None, 1, Pin(0)), None);

        // Prepare is exclusive *plus* the pin.
        assert_eq!(plan(None, 3, Prepare(1)), set(&[0, 1]));
        assert_eq!(plan(Some(&[2]), 3, Prepare(0)), set(&[0]));

        // Clear never drops the pinned file, even when it is the only one
        // selected or a stale cleanup targets it.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(0)), None);
        assert_eq!(plan(Some(&[0]), 3, Clear(0)), None);
        // Clearing another file keeps the pin in the remainder.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(1)), set(&[0]));
        assert_eq!(plan(Some(&[1, 2]), 3, Clear(1)), set(&[0, 2]));

        // Reconcile chains the pin; with no active/hot file the pin alone
        // is the want-set instead of "apply nothing".
        assert_eq!(
            plan(
                Some(&[0]),
                3,
                Reconcile {
                    active: Some(2),
                    hot: Some(1)
                }
            ),
            set(&[0, 1, 2])
        );
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            set(&[0])
        );

        // Pin keeps the current selection and adds the new pin.
        let both: BTreeSet<usize> = [0, 2].into_iter().collect();
        assert_eq!(
            plan_only_files(Some(&[1]), 3, &both, Pin(2)),
            set(&[0, 1, 2])
        );

        // An out-of-range pinned index (metadata mismatch) is dropped, and
        // a plan that is empty apart from it is a no-op.
        let stale: BTreeSet<usize> = [7].into_iter().collect();
        assert_eq!(
            plan_only_files(Some(&[1]), 3, &stale, Prepare(2)),
            set(&[2])
        );
        assert_eq!(
            plan_only_files(
                None,
                3,
                &stale,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            None
        );
    }

    /// End-to-end on a real multi-file torrent: a pinned file stays in
    /// librqbit's `only_files` through the whole playback lifecycle of
    /// another file, and leaves it only after an unpin plus a reconcile.
    /// `stats()` reports the pin per file and torrent-wide.
    #[tokio::test(flavor = "multi_thread")]
    async fn pinned_file_survives_playback_switching_on_real_torrent() {
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 48 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 64 * 1024).await;
        write_payload(&content_dir.join("c.bin"), 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let selection = |h: &LibrqbitHandle| {
            let mut v = h.handle.only_files().unwrap_or_default();
            v.sort_unstable();
            v
        };
        let reconcile = |h: LibrqbitHandle, active: Option<usize>| async move {
            h.reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: active,
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        };

        // Pin narrows "everything wanted" to the pin.
        handle.pin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);
        // Idempotent.
        handle.pin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Playback of another file: exclusive prepare keeps the pin ...
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 1]);
        // ... reconcile to yet another file keeps it ...
        reconcile(handle.clone(), Some(2)).await;
        assert_eq!(selection(&handle), vec![0, 2]);
        // ... and clearing the pinned file is refused.
        handle.clear_file_streaming(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 2]);

        // A handle re-created by get_torrent shares the pin set.
        let again = backend.get_torrent(&handle.info_hash).await.unwrap();
        let stats = TorrentHandle::stats(&again).await;
        assert_eq!(stats.pinned_files, vec![0]);
        assert!(stats.files[0].pinned);
        assert!(!stats.files[1].pinned && !stats.files[2].pinned);
        // Seeded torrent: every file is complete.
        assert!(stats.files.iter().all(|f| f.complete), "{:?}", stats.files);

        // Unpin alone leaves the selection; the next reconcile drops it.
        again.unpin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 2]);
        assert!(TorrentHandle::stats(&handle).await.pinned_files.is_empty());
        reconcile(handle.clone(), Some(2)).await;
        assert_eq!(selection(&handle), vec![2]);

        // Out-of-range pin is a structural error and records nothing.
        assert!(handle.pin_file(3).await.is_err());
        assert!(TorrentHandle::stats(&handle).await.pinned_files.is_empty());
    }

    /// Forgetting a file's pieces against the real backend: the have-set
    /// loses exactly that file's pieces, the boundary piece its neighbour
    /// shares included, and while the claim stands nothing is queued. Once
    /// it is released the neighbour's half of the boundary piece is wanted
    /// again, and a re-selection of the file has something to download --
    /// which is what a re-pin after a delete needs.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_files_pieces_forgets_them_until_they_are_wanted_again() {
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        const PIECE: u64 = 16 * 1024;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        // Neither a whole number of pieces, so whichever order the
        // filesystem lists them in, the two share a boundary piece.
        write_payload(&content_dir.join("a.bin"), 40 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 56 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;
        // Reclaim is what makes drop_file_pieces available, and it is set
        // only on a storage that can release a piece -- so this test needs
        // one, where the plain filesystem session (reclaim off) would refuse
        // the drop (see `dropping_pieces_without_reclaim_is_refused_by_name`).
        let (_backend, handle) = reclaiming_backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();
        let lengths: Vec<u64> = handle
            .handle
            .with_metadata(|m| m.file_infos.iter().map(|f| f.len).collect())
            .unwrap();
        let total: u64 = lengths.iter().sum();
        let stats = handle.handle.stats();
        assert!(stats.finished, "seeded: {stats}");
        assert_eq!(stats.file_progress, lengths);

        // The first file goes, the second is pinned and stays wanted.
        handle.pin_file(0).await.unwrap();
        handle.pin_file(1).await.unwrap();
        handle.unpin_file(0).await.unwrap();
        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: None,
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        assert_eq!(handle.handle.only_files(), Some(vec![1]));

        let dropped = handle
            .drop_file_pieces(0)
            .await
            .expect("a live torrent added here can drop")
            .expect("librqbit keeps a have-set");
        let file_pieces = lengths[0].div_ceil(PIECE) as usize;
        assert_eq!(dropped.pieces().len(), file_pieces, "{dropped:?}");
        let stats = handle.handle.stats();
        assert_eq!(stats.file_progress[0], 0, "the file is not had: {stats}");
        assert!(!handle.is_file_complete(0).await);
        assert!(!TorrentHandle::stats(&handle).await.files[0].complete);
        // The boundary piece went with it: the neighbour lost its share.
        let boundary_share = PIECE - lengths[0] % PIECE;
        assert_eq!(
            stats.file_progress[1],
            lengths[1] - boundary_share,
            "{stats}"
        );
        assert_eq!(
            stats.progress_bytes,
            total - file_pieces as u64 * PIECE,
            "{stats}"
        );
        assert!(
            stats.finished,
            "while the claim stands nothing is wanted back, not even the \
             boundary piece: {stats}"
        );

        // Releasing the claim re-queues what is still selected -- the
        // neighbour's boundary piece -- and nothing of the dropped file.
        drop(dropped);
        let stats = handle.handle.stats();
        assert!(
            !stats.finished,
            "the neighbour wants its boundary piece: {stats}"
        );
        assert_eq!(stats.file_progress[0], 0, "{stats}");

        // Selecting the file again wants all of it.
        handle.pin_file(0).await.unwrap();
        let stats = handle.handle.stats();
        assert!(!stats.finished, "{stats}");
        assert_eq!(stats.file_progress[0], 0, "{stats}");
        assert_eq!(
            stats.progress_bytes,
            total - file_pieces as u64 * PIECE,
            "nothing was downloaded in a session with no peers: {stats}"
        );
    }

    /// A reclaim torrent the session restores comes back paused whatever it
    /// was doing at shutdown (librqbit forces it, and we persist
    /// `piece_reclaim`), and the reconciler starts it again once the
    /// want-set is back -- so it goes on downloading. A torrent left paused
    /// would download nothing; here a seeder reaches the started torrent
    /// and it finishes.
    ///
    /// The order is the whole of `reconcile::Conditions::settled`. Before
    /// `restore_pinned_downloads` the ladder answers `Stop` for this
    /// torrent, because a reclaim torrent restored without its want-set
    /// wants every hole in its storage and a seeder reaching it would refill
    /// holes the caller is about to drop; after it, `Run`. Both halves are
    /// asserted, and both are asserted on what the torrent is *doing* --
    /// `run_state`, never `is_paused()`, which across an initial check is
    /// wrong in both directions.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restored_reclaim_torrent_is_started_once_its_want_set_is_back() {
        use crate::backend::TorrentBackend;
        let tmp = tempfile::tempdir().unwrap();

        // The payload, and a single-file torrent of it, whole pieces so the
        // filesystem ordering never matters.
        let content = tmp.path().join("content");
        tokio::fs::create_dir_all(&content).await.unwrap();
        write_payload(&content.join("movie.bin"), 256 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content.join("movie.bin")).await;

        let client_dir = tmp.path().join("client");
        let opts = || TestSessionOptions {
            default_storage: Some(reclaimable_storage()),
            persist: true,
            listen_loopback: true,
        };

        // First run: a client that can reclaim (so `piece_reclaim` is set and
        // persisted) and persists its session, with no data and no peers --
        // the torrent is added incomplete.
        {
            let (backend, restored) =
                LibrqbitBackend::new_for_tests_with(client_dir.clone(), opts())
                    .await
                    .unwrap();
            assert!(backend.sets_piece_reclaim());
            assert!(restored.is_empty(), "nothing to restore yet");
            let handle = backend
                .add_torrent(TorrentSource::Bytes(torrent_bytes.clone()), vec![])
                .await
                .unwrap();
            handle.handle.wait_until_initialized().await.unwrap();
            assert!(
                !handle.handle.stats().finished,
                "no data and no peers, so nothing is had"
            );
            // Let persistence write the record and the bitfield before the
            // session is dropped, so the restart has something to restore.
            let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
            let session_json = client_dir.join("session.json");
            while !session_json.exists() && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(session_json.exists(), "the session was persisted");
        }

        // Restart: a new client over the same dir restores the torrent, and a
        // reclaim torrent comes back paused.
        let (backend, restored) = LibrqbitBackend::new_for_tests_with(client_dir.clone(), opts())
            .await
            .unwrap();
        assert_eq!(restored.len(), 1, "the torrent came back");
        let hash = restored.keys().next().unwrap().clone();
        restored[&hash]
            .handle
            .wait_until_initialized()
            .await
            .unwrap();
        assert!(
            restored[&hash].handle.is_paused(),
            "a restored reclaim torrent comes back paused"
        );
        let client_addr = backend
            .session
            .listen_addr()
            .expect("the client listens for the seeder");

        let mut efs = crate::BackendEngineFS::new_with_backend(
            backend,
            restored,
            client_dir.join("cache"),
            client_dir.clone(),
        );
        // Declared, so the decision is about this test's inputs rather than
        // about however much room the machine running it happens to have.
        efs.set_free_space_probe(|_| Ok(u64::MAX));

        // Before the want-set is back: the reconciler leaves it stopped,
        // whatever else is true of it.
        assert_eq!(
            efs.reconcile_tick().await,
            vec![(hash.clone(), crate::reconcile::Decision::Stop)],
            "an unsettled torrent is not started"
        );
        assert_eq!(
            efs.get_engine(&hash)
                .await
                .expect("the restored engine")
                .handle
                .run_state(),
            RunState::Paused,
            "and it really is still stopped"
        );

        // The want-set goes back on, and the next pass starts it.
        efs.restore_pinned_downloads().await;
        assert_eq!(
            efs.reconcile_tick().await,
            vec![(hash.clone(), crate::reconcile::Decision::Run)]
        );
        let engine = efs.get_engine(&hash).await.expect("the restored engine");
        assert_eq!(
            engine.handle.run_state(),
            RunState::Live,
            "the restored torrent is running again"
        );

        // A seeder with the whole file dials the resumed client, which then
        // downloads it -- the point being that a paused torrent would not.
        let seeder = librqbit::Session::new_with_opts(
            content.clone(),
            librqbit::SessionOptions {
                dht: None,
                persistence: None,
                listen: Some(librqbit::ListenerOptions {
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .expect("seeder session");
        seeder
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes.clone())),
                Some(librqbit::AddTorrentOptions {
                    paused: false,
                    output_folder: Some(content.to_str().unwrap().to_owned()),
                    overwrite: true,
                    // The seeder dials the client, which is listening.
                    initial_peers: Some(vec![client_addr]),
                    ..Default::default()
                }),
            )
            .await
            .expect("seeder add");

        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        loop {
            if engine.handle.handle.stats().finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the resumed torrent never finished downloading: {}",
                engine.handle.handle.stats()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The other side of that restart, and the one nothing on this branch
    /// covered: with seeding turned off, a torrent the last process left
    /// stopped stays stopped.
    ///
    /// The reconciler starts anything the ladder says should run, the
    /// previous process's pause included -- that is the point of it -- so
    /// the only thing standing between "seeding is off and nobody is
    /// watching" and a restart that starts every torrent there is is the
    /// idle arm, and the idle arm's grace is measured from
    /// `Engine::last_active_at`. That used to be initialised to the clock,
    /// which for a restored torrent is a claim about a past this process
    /// never saw: it read as "used a moment ago", so `idle_for` was zero,
    /// the idle arm could not fire, and the anti-flap dwell does not apply
    /// to a torrent this process has never moved. Every restart therefore
    /// announced, found peers and downloaded every stopped torrent for a
    /// whole `INACTIVE_TORRENT_PAUSE_GRACE` before stopping it again --
    /// over a metered connection and a television's disk, with the setting
    /// that exists to prevent exactly that turned on. `None` is the honest
    /// answer and the arm reads it as quiet.
    ///
    /// Driven over a real persisted session because the restart is where
    /// the defect lives, and asserted on `run_state` and on the backend's
    /// own byte counters, never on a flag this code wrote.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_with_seeding_off_does_not_start_what_the_last_process_stopped() {
        use crate::backend::TorrentBackend;
        let tmp = tempfile::tempdir().unwrap();

        let content = tmp.path().join("content");
        tokio::fs::create_dir_all(&content).await.unwrap();
        write_payload(&content.join("movie.bin"), 256 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content.join("movie.bin")).await;

        let client_dir = tmp.path().join("client");
        let opts = || TestSessionOptions {
            default_storage: Some(reclaimable_storage()),
            persist: true,
            listen_loopback: true,
        };

        // First run: the torrent is added and persisted, incomplete.
        {
            let (backend, restored) =
                LibrqbitBackend::new_for_tests_with(client_dir.clone(), opts())
                    .await
                    .unwrap();
            assert!(restored.is_empty(), "nothing to restore yet");
            let handle = backend
                .add_torrent(TorrentSource::Bytes(torrent_bytes.clone()), vec![])
                .await
                .unwrap();
            handle.handle.wait_until_initialized().await.unwrap();
            let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
            let session_json = client_dir.join("session.json");
            while !session_json.exists() && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(session_json.exists(), "the session was persisted");
        }

        // The restart, with the user's seeding switch off.
        let (backend, restored) = LibrqbitBackend::new_for_tests_with(client_dir.clone(), opts())
            .await
            .unwrap();
        assert_eq!(restored.len(), 1, "the torrent came back");
        let hash = restored.keys().next().unwrap().clone();
        restored[&hash]
            .handle
            .wait_until_initialized()
            .await
            .unwrap();
        let client_addr = backend
            .session
            .listen_addr()
            .expect("the client listens for the seeder");

        let mut efs = crate::BackendEngineFS::new_with_backend(
            backend,
            restored,
            client_dir.join("cache"),
            client_dir.clone(),
        );
        efs.set_free_space_probe(|_| Ok(u64::MAX));
        efs.set_seeding_enabled(false).await;

        // The want-set is back, so `settled` is not what is holding it:
        // what holds it is that nothing in this process has used it.
        efs.restore_pinned_downloads().await;
        assert_eq!(
            efs.reconcile_tick().await,
            vec![(hash.clone(), crate::reconcile::Decision::Stop)],
            "seeding is off and nobody has watched this torrent"
        );
        let engine = efs.get_engine(&hash).await.expect("the restored engine");
        assert_eq!(
            engine.handle.run_state(),
            RunState::Paused,
            "and it really is still stopped"
        );

        // A seeder with the whole file dials it, exactly as one would after
        // a restart that announced. A started torrent would take the bytes.
        let seeder = librqbit::Session::new_with_opts(
            content.clone(),
            librqbit::SessionOptions {
                dht: None,
                persistence: None,
                listen: Some(librqbit::ListenerOptions {
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .expect("seeder session");
        seeder
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes.clone())),
                Some(librqbit::AddTorrentOptions {
                    paused: false,
                    output_folder: Some(content.to_str().unwrap().to_owned()),
                    overwrite: true,
                    initial_peers: Some(vec![client_addr]),
                    ..Default::default()
                }),
            )
            .await
            .expect("seeder add");

        // The real clock throughout, and deliberately: `reconcile_tick_at`
        // exists so a real session can reach the idle arm without sitting
        // out the grace, but it hands in a `now` the engine's own stamp
        // never saw, which is the very comparison under test here. What
        // makes the wait unnecessary instead is that the honest answer is
        // available on the *first* tick -- nothing has used this torrent,
        // so there is no grace left to run out.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            assert_eq!(
                efs.reconcile_tick().await,
                vec![(hash.clone(), crate::reconcile::Decision::Stop)],
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            engine.handle.run_state(),
            RunState::Paused,
            "and it stays stopped while the seeder is knocking"
        );
        let stats = engine.handle.handle.stats();
        assert!(
            !stats.finished && stats.progress_bytes == 0,
            "a stopped torrent takes no bytes from the seeder: {stats}"
        );
    }

    /// A seeder session for `torrent_bytes` on an ephemeral loopback port,
    /// uploading at `upload_bps` so a download from a swarm of them outlasts
    /// what a test asserts about its peers. No DHT, no trackers: the only way
    /// it and a client meet is an address one of them is handed.
    pub(super) async fn slow_seeder(
        content_dir: &std::path::Path,
        torrent_bytes: &[u8],
        upload_bps: u32,
    ) -> Arc<librqbit::Session> {
        let seeder = librqbit::Session::new_with_opts(
            content_dir.to_path_buf(),
            librqbit::SessionOptions {
                dht: None,
                persistence: None,
                listen: Some(librqbit::ListenerOptions {
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                disable_local_service_discovery: true,
                ratelimits: librqbit::limits::LimitsConfig {
                    upload_bps: std::num::NonZeroU32::new(upload_bps),
                    download_bps: None,
                },
                ..Default::default()
            },
        )
        .await
        .expect("seeder session");
        let handle = seeder
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::copy_from_slice(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    output_folder: Some(content_dir.to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("seeder add")
            .into_handle()
            .expect("seeder handle");
        handle
            .wait_until_initialized()
            .await
            .expect("seeder checks");
        seeder
    }

    /// The footprint's arithmetic, with no swarm to wait for: `Lean` puts
    /// `LEAN_PEER_LIMIT` on every torrent the session holds, `Full` puts the
    /// *configured* limit back (a session opened from settings has one of
    /// its own, so this would catch a `Full` that restored librqbit's
    /// default instead), a torrent added while lean takes the lean cap
    /// before it is ever live, and one added while full takes the
    /// configured one.
    ///
    /// The cap is the whole lever -- parking the surplus and re-dialling it
    /// is what librqbit does with it -- so this is the half of
    /// `set_footprint` that can be pinned without a network:
    /// `lean_parks_the_surplus_and_full_re_dials_it` below is the one test
    /// that shows the cap actually moving peers.
    #[tokio::test(flavor = "multi_thread")]
    async fn footprint_caps_every_torrent_and_the_next_one_added() {
        use crate::backend::{Footprint, LEAN_PEER_LIMIT, TorrentBackend};
        /// Above `LEAN_PEER_LIMIT`, as every real profile's is (the
        /// shipped default derives 100), so lean is a shrink.
        const CONFIGURED: usize = 100;

        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        write_payload(&src.join("b.bin"), 32 * 1024).await;
        let (first_bytes, _) = make_torrent(&src.join("a.bin")).await;
        let (second_bytes, _) = make_torrent(&src.join("b.bin")).await;

        // Opened the way the settings open one, so the configured peer
        // limit is a real value rather than `None`. No DHT, no LSD, no
        // peers: nothing here waits for the network.
        let (backend, _) = LibrqbitBackend::new_with_settings(
            tmp.path().join("dl"),
            TorrentListenPort::Ephemeral,
            Vec::new(),
            BootstrapResolvers::offline(),
            SessionTuning {
                dht: false,
                lsd: false,
                peer_limit: Some(CONFIGURED),
                ..SessionTuning::default()
            },
        )
        .await
        .expect("hermetic session");
        assert_eq!(backend.footprint(), Footprint::Full);
        assert_eq!(backend.caps.lock().limit(), CONFIGURED);

        let first = backend
            .add_torrent(TorrentSource::Bytes(first_bytes), Vec::new())
            .await
            .expect("add while full");
        assert_eq!(first.handle.shared.peer_limit(), CONFIGURED);

        // Lean reaches the torrent already there.
        backend.set_footprint(Footprint::Lean);
        assert_eq!(backend.footprint(), Footprint::Lean);
        assert_eq!(first.handle.shared.peer_limit(), LEAN_PEER_LIMIT);

        // And the one added afterwards, whatever state it is in: the cap
        // lives on `ManagedTorrentShared`, so a torrent still hash-checking
        // carries it into its live state.
        let second = backend
            .add_torrent(TorrentSource::Bytes(second_bytes), Vec::new())
            .await
            .expect("add while lean");
        assert_eq!(second.handle.shared.peer_limit(), LEAN_PEER_LIMIT);

        // A repeated `Lean` changes nothing (and, in the swarm, prunes
        // nothing -- see the test below).
        backend.set_footprint(Footprint::Lean);
        assert_eq!(backend.footprint(), Footprint::Lean);
        assert_eq!(first.handle.shared.peer_limit(), LEAN_PEER_LIMIT);
        assert_eq!(second.handle.shared.peer_limit(), LEAN_PEER_LIMIT);

        // Full puts the configured limit back on both.
        backend.set_footprint(Footprint::Full);
        assert_eq!(backend.footprint(), Footprint::Full);
        assert_eq!(first.handle.shared.peer_limit(), CONFIGURED);
        assert_eq!(second.handle.shared.peer_limit(), CONFIGURED);
    }

    /// Going lean prunes the peer table of the addresses it was only
    /// remembering: three peers that never answered are `Dead`, and `Lean`
    /// forgets all three (`known` 3 -> 0) while `seen`, which only ever
    /// grows, still says they were met.
    ///
    /// No swarm and no timing luck: the dials are to closed loopback ports,
    /// which are refused rather than left to a timeout, and librqbit's
    /// reconnect backoff starts at ten seconds -- so once every address is
    /// `Dead` there is a wide window in which nothing moves it back. This
    /// is where the pruning is pinned; the swarm test only has to show that
    /// the peers the *cap* parks are not pruned with them.
    #[tokio::test(flavor = "multi_thread")]
    async fn going_lean_forgets_the_peers_it_was_only_remembering() {
        use crate::backend::{Footprint, TorrentBackend, TorrentHandle};
        const DEAD: usize = 3;

        let tmp = tempfile::tempdir().unwrap();
        write_payload(&tmp.path().join("payload.bin"), 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&tmp.path().join("payload.bin")).await;

        // Ports nothing listens on: seen, dialled, refused, and left in the
        // table waiting out a backoff. Discard ports (9) are never bound.
        let dead: Vec<std::net::SocketAddr> = (0..DEAD)
            .map(|i| (std::net::Ipv4Addr::LOCALHOST, 9 + i as u16).into())
            .collect();

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        // Straight to the session, for `initial_peers` (see
        // `stats_connected_seeders_mirrors_librqbits_live_seeder_count`).
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(dead),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = backend.wrap(inner);

        // Every address dialled and refused: nothing queued, connecting or
        // live, so the whole table is `Dead`.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        let dead_table = loop {
            let s = TorrentHandle::stats(&handle).await;
            let d = s.peer_discovery;
            if d.known as usize == DEAD && d.queued + d.connecting + d.live == 0 {
                break d;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the dials to closed ports never all came back dead \
                 within {TEST_WAIT_BOUND:?}: {d:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(dead_table.seen as usize, DEAD, "{dead_table:?}");

        backend.set_footprint(Footprint::Lean);
        let pruned = TorrentHandle::stats(&handle).await.peer_discovery;
        assert_eq!(pruned.known, 0, "the dead addresses are gone: {pruned:?}");
        assert_eq!(
            pruned.seen as usize, DEAD,
            "`seen` is cumulative and does not un-count them: {pruned:?}"
        );
    }

    /// The wiring, against a real swarm and the one test here that needs
    /// one: `Lean` lowers the cap, the surplus peers hang up but stay in
    /// the table -- parked, not forgotten, which is what `Full` re-dials --
    /// the download (and so the seeding) goes on from the peers left, a
    /// repeated `Lean` prunes nothing, and `Full` brings the swarm back
    /// past the lean cap.
    ///
    /// Only the cap is asserted exactly: it is ours to set, and it is read
    /// from the torrent rather than inferred from a peer count. Everything
    /// about the peers is asserted as a *side of the cap*, never as an
    /// exact number, because how many of a dozen loopback sessions are
    /// connected at any instant is the network's business and a loaded
    /// runner's. The precondition -- more live peers than the lean cap, so
    /// there is a surplus to park at all -- is waited for on its own and
    /// says, when it fails, that the environment never produced it.
    #[tokio::test(flavor = "multi_thread")]
    async fn lean_parks_the_surplus_and_full_re_dials_it() {
        use crate::backend::{Footprint, LEAN_PEER_LIMIT, TorrentBackend, TorrentHandle};
        const SEEDERS: usize = LEAN_PEER_LIMIT + 4;

        /// Poll the handle's stats until `pred` holds; the bound is only
        /// there so a regression fails instead of hanging.
        async fn wait_for(
            handle: &LibrqbitHandle,
            what: &str,
            mut pred: impl FnMut(&EngineStats) -> bool,
        ) -> EngineStats {
            let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
            loop {
                let s = crate::backend::TorrentHandle::stats(handle).await;
                if pred(&s) {
                    return s;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what} did not happen within {TEST_WAIT_BOUND:?}: peers={} discovery={:?}",
                    s.peers,
                    s.peer_discovery
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        let src = tempfile::tempdir().unwrap();
        let payload = src.path().join("payload.bin");
        // Big enough, at the seeders' pace, that the torrent is still
        // downloading when the assertions are done -- a torrent that
        // finished would hang up on the seeders itself, which is not what
        // this test is about. 12 x 16 KiB/s is ~85 s for 16 MiB, and lean
        // only slows that down.
        write_payload(&payload, 16 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let mut seeders = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..SEEDERS {
            let seeder = slow_seeder(src.path(), &torrent_bytes, 16 * 1024).await;
            peers.push(seeder.listen_addr().expect("seeder listens"));
            seeders.push(seeder);
        }

        let dl = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.path().to_path_buf())
            .await
            .expect("hermetic session");
        assert_eq!(backend.footprint(), Footprint::Full);
        // Straight to the session, for `initial_peers` (see
        // `stats_connected_seeders_mirrors_librqbits_live_seeder_count`).
        // These addresses are the only ones this session will ever know:
        // no DHT, no trackers, no LSD -- so a peer that comes back after
        // `Full` can only be one the cap parked.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes.clone())),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(peers),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = backend.wrap(inner);
        assert_eq!(
            handle.handle.shared.peer_limit(),
            librqbit::DEFAULT_PEER_LIMIT
        );

        // The precondition, on its own bound: without more live peers than
        // the lean cap there is no surplus to park and the test has nothing
        // to say. A failure here is the environment, not the footprint.
        let full = wait_for(
            &handle,
            "the swarm outgrowing the lean cap (the environment never \
             connected more than LEAN_PEER_LIMIT of the seeders)",
            |s| s.peers as usize > LEAN_PEER_LIMIT,
        )
        .await;

        // Lean, right on the heels of that read: the surplus is parked
        // before the call returns (librqbit ranks and hangs up on it there
        // and then, and takes the spare permits away so nothing can go live
        // over the cap behind it), so this is read straight after rather
        // than waited for. A count that only *eventually* falls under the
        // cap is one the swarm shed on its own, which is what a bound here
        // would have accepted.
        backend.set_footprint(Footprint::Lean);
        assert_eq!(backend.footprint(), Footprint::Lean);
        assert_eq!(handle.handle.shared.peer_limit(), LEAN_PEER_LIMIT);
        let lean = TorrentHandle::stats(&handle).await;
        assert!(
            (lean.peers as usize) <= LEAN_PEER_LIMIT,
            "{} peers were live before going lean and {} still are: {:?}",
            full.peers,
            lean.peers,
            lean.peer_discovery
        );
        // Parked, not forgotten: the pruning a lean does runs *before* the
        // cap drops, so the peers that were live a moment ago survive it,
        // and the table still holds more addresses than the cap now lets be
        // live. Those are what `Full` re-dials below.
        assert!(
            lean.peer_discovery.known as usize > LEAN_PEER_LIMIT,
            "the surplus was parked, not forgotten: {:?}",
            lean.peer_discovery
        );

        // Seeding and downloading go on from the peers left.
        let fetched_before = handle.transfer_totals().fetched;
        wait_for(&handle, "the download going on with the peers left", |_| {
            handle.transfer_totals().fetched > fetched_before
        })
        .await;

        // A second `Lean` is a no-op: it must not prune the peers the first
        // one parked, or `Full` would have nobody to re-dial. Nothing but a
        // prune shrinks the table, so "no smaller" is the assertion.
        let before_repeat = TorrentHandle::stats(&handle).await.peer_discovery;
        backend.set_footprint(Footprint::Lean);
        let after_repeat = TorrentHandle::stats(&handle).await.peer_discovery;
        assert_eq!(handle.handle.shared.peer_limit(), LEAN_PEER_LIMIT);
        assert!(
            after_repeat.known >= before_repeat.known,
            "a repeated Lean pruned the parked peers: {before_repeat:?} -> {after_repeat:?}"
        );

        // Full: the cap goes back at once, and the parked peers -- the only
        // addresses this session has ever known -- are re-dialled, so the
        // swarm grows back past the lean cap.
        backend.set_footprint(Footprint::Full);
        assert_eq!(backend.footprint(), Footprint::Full);
        assert_eq!(
            handle.handle.shared.peer_limit(),
            librqbit::DEFAULT_PEER_LIMIT
        );
        wait_for(&handle, "the parked peers coming back", |s| {
            s.peers as usize > LEAN_PEER_LIMIT
        })
        .await;
        drop(seeders);
    }

    /// A torrent added without `piece_reclaim` -- which is every torrent on
    /// the shipped session, whose filesystem storage cannot release a piece
    /// so the option is never set -- cannot forget a piece, and the refusal
    /// says why rather than reading as a generic backend error. The delete
    /// path logs it and goes on deleting; the next restart re-checks the
    /// torrent from disk.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_pieces_without_reclaim_is_refused_by_name() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        write_payload(&dir.join("payload.bin"), 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&dir.join("payload.bin")).await;
        // The shipped session's filesystem storage cannot release a piece,
        // so `new_for_tests` opens with reclaim off, as production does.
        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        assert!(!backend.sets_piece_reclaim());
        // Straight to the session with default options, as this session's
        // every add and every restore builds them.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        inner.wait_until_initialized().await.unwrap();
        let handle = backend
            .get_torrent(&inner.info_hash().as_string())
            .await
            .unwrap();
        assert!(handle.is_file_complete(0).await, "seeded");

        let error = handle
            .drop_file_pieces(0)
            .await
            .expect_err("no reclaim on this torrent");
        let text = format!("{error:#}");
        assert!(text.contains("added without piece reclaim"), "{text}");
        assert!(text.contains("piece_reclaim"), "{text}");
        assert_eq!(
            handle.handle.stats().file_progress,
            vec![32 * 1024],
            "and nothing was dropped"
        );
    }

    /// `complete` follows the per-file progress: nothing on disk means no
    /// file is complete (and none is pinned by default).
    #[tokio::test]
    async fn stats_report_incomplete_unpinned_files_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        write_payload(&src_dir.join("payload.bin"), 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&src_dir.join("payload.bin")).await;
        let (_backend, handle) = backend_with_torrent(&tmp.path().join("dl"), &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(!stats.files[0].complete);
        assert!(!stats.files[0].pinned);
        assert!(stats.pinned_files.is_empty());
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["pinnedFiles"], serde_json::json!([]));
        assert_eq!(json["files"][0]["pinned"], false);
        assert_eq!(json["files"][0]["complete"], false);
    }

    /// A pin issued while the torrent is still hash-checking is parked on
    /// the deferred-selection path and applied once librqbit accepts
    /// selection updates, so a "download" pressed during startup sticks.
    #[tokio::test(flavor = "multi_thread")]
    async fn pin_file_defers_while_initializing() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        let was_initializing = handle.is_initializing();
        handle.pin_file(1).await.unwrap();
        // Recorded immediately, applied once initialized.
        assert_eq!(TorrentHandle::stats(&handle).await.pinned_files, vec![1]);
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while handle.handle.only_files() != Some(vec![1]) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.handle.only_files(), Some(vec![1]));
        assert!(!handle.deferred_selection().has_pending());
        assert!(
            was_initializing,
            "test must exercise the initializing gate, not a torrent that was already ready"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multifile_selection_lifecycle() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        // Multi-file torrents land in <download_dir>/<torrent name>/, so seed
        // the payloads exactly there by creating the torrent from that dir.
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 48 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();
        assert_eq!(handle.file_count().await, 2);

        let selection = |h: &LibrqbitHandle| {
            let mut v = h.handle.only_files().unwrap_or_default();
            v.sort_unstable();
            v
        };

        // Prepare selects exclusively.
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![1]);

        // Clearing the only selected file would empty the set -> no-op.
        handle.clear_file_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![1]);

        // Reconcile switches to the active file.
        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(0),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Stale clear of a file that is no longer selected -> no-op.
        handle.clear_file_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Gating must not starve the selected, streamed file.
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "selected file must stay readable: {}", r.reason);

        // Out-of-range prepare is a structural error.
        assert!(handle.prepare_file_for_streaming(2).await.is_err());
    }

    #[tokio::test]
    async fn single_file_torrent_selection_is_untouched() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        handle.prepare_file_for_streaming(0).await.unwrap();
        handle.clear_file_streaming(0).await.unwrap();
        assert_eq!(handle.handle.only_files(), None);
    }

    /// `Session::delete(_, false)` leaves the output folder behind even
    /// when it is empty; `remove_torrent` cleans that up -- and only that:
    /// a folder with data in it and the session root itself stay.
    #[tokio::test]
    async fn remove_torrent_removes_empty_output_folder_only() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        write_payload(&src.join("b.bin"), 32 * 1024).await;
        let (multi_bytes, multi_hash) = make_torrent(&src).await;
        let single_payload = tmp.path().join("single.bin");
        write_payload(&single_payload, 16 * 1024).await;
        let (single_bytes, single_hash) = make_torrent(&single_payload).await;

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes.clone()), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        single.handle.wait_until_initialized().await.unwrap();
        assert_eq!(
            single.handle.output_folder(),
            dl,
            "single-file torrents write into the root"
        );
        let multi_dir = multi.handle.output_folder().to_path_buf();
        assert_eq!(multi_dir, dl.join("src"));
        assert!(multi_dir.is_dir());

        // Nothing downloaded: the multi-file torrent's folder is empty
        // (drop whatever librqbit pre-created) and goes with the torrent.
        tokio::fs::remove_dir_all(&multi_dir).await.unwrap();
        tokio::fs::create_dir_all(&multi_dir).await.unwrap();
        backend.remove_torrent(&multi_hash).await.unwrap();
        assert!(!multi_dir.exists(), "empty output folder must be removed");

        // The root is never removed, however empty, and a single-file
        // torrent's data survives in it.
        for entry in std::fs::read_dir(&dl).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }
        backend.remove_torrent(&single_hash).await.unwrap();
        assert!(dl.is_dir(), "session root must survive");

        // A folder that still holds data is left alone.
        tokio::fs::create_dir_all(&dl.join("src")).await.unwrap();
        write_payload(&dl.join("src").join("a.bin"), 32 * 1024).await;
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        backend.remove_torrent(&multi_hash).await.unwrap();
        assert!(dl.join("src").join("a.bin").is_file());
    }

    /// `file_path` is the torrent's output folder joined with the file's
    /// relative name -- straight in the session root for a single-file
    /// torrent, under the torrent's own folder for a multi-file one -- and
    /// points at the real bytes. Out of range: None. `get_file_path` is the
    /// same path as a string.
    #[tokio::test]
    async fn file_path_points_at_the_file_on_disk() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();

        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (single_bytes, _) = make_torrent(&payload).await;
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 16 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 24 * 1024).await;
        let (multi_bytes, _) = make_torrent(&content_dir).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        let a = torrent_file_index(&multi_bytes, "a.bin");
        let b = torrent_file_index(&multi_bytes, "b.bin");

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();

        let path = single.file_path(0).await.expect("single-file path");
        assert_eq!(path, dir.join("payload.bin"));
        assert_eq!(
            single.get_file_path(0).await.as_deref(),
            Some(path.to_string_lossy().as_ref())
        );
        let bytes = tokio::fs::read(&path).await.expect("path exists on disk");
        assert_eq!(bytes.len(), 32 * 1024);
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
        assert_eq!(single.file_path(1).await, None, "out of range");

        assert_eq!(
            multi.file_path(b).await.as_deref(),
            Some(content_dir.join("b.bin").as_path())
        );
        assert_eq!(
            tokio::fs::metadata(multi.file_path(a).await.unwrap())
                .await
                .unwrap()
                .len(),
            16 * 1024
        );
        assert_eq!(multi.file_path(2).await, None);
    }

    /// `add_torrent_placed` hands librqbit the placement: the torrent's
    /// files live in exactly `output_folder` (no name sub-folder), only the
    /// listed files are wanted, `output_folder()` reports the folder, and
    /// data already present there is picked up by the hash check
    /// (`overwrite: true`). An out-of-range `only_files` index is refused
    /// at add time.
    /// Storage that opens without complaint and then fails the initial
    /// check -- how librqbit actually reaches its Error state, since a
    /// storage that cannot be opened at all fails the add itself instead.
    struct BrokenStorage(String);

    impl librqbit::storage::TorrentStorage for BrokenStorage {
        fn init(
            &mut self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> Result<()> {
            Ok(())
        }
        fn pread_exact(&self, _file_id: usize, _offset: u64, _buf: &mut [u8]) -> Result<()> {
            anyhow::bail!("{}", self.0)
        }
        fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> Result<()> {
            Ok(())
        }
        fn remove_file(&self, _file_id: usize, _filename: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn remove_directory_if_empty(&self, _path: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn ensure_file_length(&self, _file_id: usize, _length: u64) -> Result<()> {
            Ok(())
        }
        fn take(&self) -> Result<Box<dyn librqbit::storage::TorrentStorage>> {
            anyhow::bail!("{}", self.0)
        }
    }

    #[derive(Clone)]
    struct BrokenStorageFactory(String);

    impl librqbit::storage::StorageFactory for BrokenStorageFactory {
        type Storage = BrokenStorage;
        fn create(
            &self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> Result<Self::Storage> {
            Ok(BrokenStorage(self.0.clone()))
        }
        fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
            use librqbit::storage::StorageFactoryExt;
            self.clone().boxed()
        }
    }

    /// librqbit says WHY a torrent it put in the Error state is stuck
    /// (`TorrentStats.error`), and that reason is the `{e:?}` of an anyhow
    /// chain naming absolute cache and download paths. The client gets a
    /// fixed message instead -- non-empty, so the download screen can say
    /// more than "error", and path-free; the chain goes to the log alone,
    /// and only once per distinct error, since statistics are polled for
    /// as long as the broken download is on screen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_torrent_error_reaches_the_client_without_the_server_paths() {
        use crate::backend::TorrentHandle;
        use librqbit::storage::StorageFactoryExt;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;

        let backend = LibrqbitBackend::new_for_tests(tmp.path().join("dl"))
            .await
            .expect("hermetic session");
        // The shape of a real librqbit storage failure: an anyhow chain
        // naming the absolute path it could not use.
        let librqbit_error = format!(
            "error opening {:?} in read/write mode",
            tmp.path().join("dl").join("src").join("a.bin")
        );
        // The server path as the error renders it. `Debug` on a path escapes
        // the separator, so on Windows the chain holds `C:\\dir\\dl` where
        // `Path::to_str` would give `C:\dir\dl`: match the formatting the
        // error itself used rather than the raw path, or the check passes
        // vacuously on one platform and fails on the other.
        let server_path = format!("{:?}", tmp.path());
        let server_path = server_path.trim_matches('"');
        assert!(
            librqbit_error.contains(server_path),
            "the fixture error names the server path: {librqbit_error}"
        );
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes),
                Some(librqbit::AddTorrentOptions {
                    storage_factory: Some(BrokenStorageFactory(librqbit_error.clone()).boxed()),
                    ..Default::default()
                }),
            )
            .await
            .expect("the add succeeds; the check is what fails");
        let (librqbit::AddTorrentResponse::Added(_, managed)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, managed)) = response
        else {
            panic!("torrent not added");
        };
        let handle = LibrqbitHandle {
            handle: managed,
            info_hash: hash.clone(),
            session: backend.session.clone(),
            deferred_selections: backend.deferred_selections.clone(),
            pinned_files: backend.pinned_files.clone(),
            reported_errors: backend.reported_errors.clone(),
            stream_positions: backend.stream_positions.clone(),
            swarm_scraper: backend.swarm_scraper.clone(),
        };

        let mut stats = handle.stats().await;
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while stats.phase != StartupPhase::Error && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stats = handle.stats().await;
        }
        assert_eq!(stats.phase, StartupPhase::Error, "{:?}", stats.phase);
        assert!(
            handle.is_in_error_state().await && !handle.is_out_of_space().await,
            "a storage failure is the error state, and not the device's fault"
        );
        let reported = stats.error.expect("the reason reaches the client");
        assert!(!reported.is_empty());
        assert!(
            !reported.contains(server_path)
                && !reported.contains(tmp.path().to_str().unwrap())
                && !reported.contains("a.bin")
                && !reported.contains(&hash),
            "no server path leaks into the response: {reported}"
        );
        assert!(
            handle
                .reported_errors
                .lock()
                .get(&hash)
                .is_some_and(|logged| logged.contains(server_path)),
            "the full chain is kept for the log, and logged once"
        );

        // Recovered: the record goes, so the next error is logged again.
        assert_eq!(handle.client_torrent_error(None), None);
        assert!(handle.reported_errors.lock().is_empty());
    }

    #[tokio::test]
    async fn add_torrent_placed_uses_the_folder_and_want_set() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        write_payload(&src.join("b.bin"), 48 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        // (Both files are whole 16 KiB pieces, so neither order makes them
        // share a boundary piece.)
        let a = torrent_file_index(&bytes, "a.bin");
        let b = torrent_file_index(&bytes, "b.bin");

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let folder = tmp.path().join("offline").join(&hash);
        tokio::fs::create_dir_all(&folder).await.unwrap();
        // Pre-seed the wanted file where the placement points.
        tokio::fs::copy(src.join("b.bin"), folder.join("b.bin"))
            .await
            .unwrap();

        let handle = backend
            .add_torrent_placed(
                TorrentSource::Bytes(bytes.clone()),
                vec![],
                TorrentPlacement {
                    output_folder: Some(folder.clone()),
                    only_files: Some(vec![b]),
                },
            )
            .await
            .expect("add with placement");
        assert_eq!(handle.output_folder(), Some(folder.clone()));
        assert_eq!(handle.handle.only_files(), Some(vec![b]));
        assert_eq!(
            handle.file_path(b).await.as_deref(),
            Some(folder.join("b.bin").as_path())
        );
        handle.handle.wait_until_initialized().await.unwrap();
        let stats = handle.stats().await;
        assert!(
            stats.files[b].complete,
            "pre-seeded file verified: {stats:?}"
        );
        assert!(!stats.files[a].complete);
        assert!(
            !dl.join("src").exists(),
            "nothing lands in the session root"
        );

        backend.remove_torrent(&hash).await.unwrap();
        let err = match backend
            .add_torrent_placed(
                TorrentSource::Bytes(bytes),
                vec![],
                TorrentPlacement {
                    output_folder: Some(folder),
                    only_files: Some(vec![2]),
                },
            )
            .await
        {
            Ok(_) => panic!("out-of-range only_files must be refused"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("out of range"), "{err:#}");
    }

    /// `relocate_torrent` moves a torrent's files -- multi-file: out of its
    /// `<root>/<name>` folder (which goes once empty); single-file: out of
    /// the root itself -- into the placement's folder, re-adds it there
    /// wanting the placement's files, and librqbit's re-check finds the
    /// moved data complete. Already in place: same handle, nothing moved.
    #[tokio::test]
    async fn relocate_torrent_moves_the_data_and_rechecks_it_in_place() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dl = tmp.path().join("dl");
        // Seed both torrents in the session root as if streamed there.
        let multi_src = dl.join("show");
        tokio::fs::create_dir_all(&multi_src).await.unwrap();
        write_payload(&multi_src.join("e1.bin"), 40 * 1024).await;
        write_payload(&multi_src.join("e2.bin"), 24 * 1024).await;
        let (multi_bytes, multi_hash) = make_torrent(&multi_src).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        let e1 = torrent_file_index(&multi_bytes, "e1.bin");
        let e2 = torrent_file_index(&multi_bytes, "e2.bin");
        let single_src = dl.join("movie.bin");
        write_payload(&single_src, 20 * 1024).await;
        let (single_bytes, single_hash) = make_torrent(&single_src).await;

        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        assert_eq!(multi.output_folder(), Some(multi_src.clone()));
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        single.handle.wait_until_initialized().await.unwrap();
        assert!(multi.stats().await.files.iter().all(|f| f.complete));

        let offline = tmp.path().join("offline");
        let multi_target = offline.join(&multi_hash);
        let moved = backend
            .relocate_torrent(
                &multi_hash,
                TorrentPlacement {
                    output_folder: Some(multi_target.clone()),
                    only_files: Some(vec![e2]),
                },
                vec![],
            )
            .await
            .expect("relocate multi-file torrent");
        assert_eq!(moved.output_folder(), Some(multi_target.clone()));
        assert_eq!(moved.handle.only_files(), Some(vec![e2]));
        assert!(multi_target.join("e1.bin").is_file());
        assert!(multi_target.join("e2.bin").is_file());
        assert!(!multi_src.exists(), "emptied source folder is removed");
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(
            stats.files.iter().all(|f| f.complete),
            "moved data verified by the re-check: {stats:?}"
        );
        assert_eq!(
            moved.file_path(e2).await.as_deref(),
            Some(multi_target.join("e2.bin").as_path())
        );
        assert_eq!(backend.list_torrents().await.len(), 2);

        // Already there: same torrent, no re-add.
        let again = backend
            .relocate_torrent(
                &multi_hash,
                TorrentPlacement {
                    output_folder: Some(multi_target.clone()),
                    only_files: Some(vec![e1]),
                },
                vec![],
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&again.handle, &moved.handle));

        let single_target = offline.join(&single_hash);
        let moved = backend
            .relocate_torrent(
                &single_hash,
                TorrentPlacement {
                    output_folder: Some(single_target.clone()),
                    only_files: None,
                },
                vec![],
            )
            .await
            .expect("relocate single-file torrent");
        assert!(single_target.join("movie.bin").is_file());
        assert!(!single_src.exists());
        assert!(dl.is_dir(), "the session root stays");
        moved.handle.wait_until_initialized().await.unwrap();
        assert!(moved.stats().await.files[0].complete);
        assert_eq!(
            moved.file_path(0).await.as_deref(),
            Some(single_target.join("movie.bin").as_path())
        );
    }

    /// librqbit pre-sizes every wanted file when a torrent goes live, so a
    /// torrent added in the root without data still has full-length
    /// placeholders there. Relocating it onto a folder that already holds
    /// the real bytes must keep those: the placeholder is dropped, the
    /// destination file stays and verifies complete.
    #[tokio::test]
    async fn relocate_torrent_keeps_a_file_already_at_the_destination() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        tokio::fs::create_dir_all(&src).await.unwrap();
        // Whole pieces per file, whichever order the torrent lists them
        // in: no boundary piece is shared with e1, whose data is absent.
        write_payload(&src.join("e1.bin"), 32 * 1024).await;
        write_payload(&src.join("e2.bin"), 16 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // Readdir order decides the file indices -- look them up.
        let e1 = torrent_file_index(&bytes, "e1.bin");
        let e2 = torrent_file_index(&bytes, "e2.bin");

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        // Added without data: the root folder gets empty placeholders.
        let handle = backend
            .add_torrent(TorrentSource::Bytes(bytes), vec![])
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        let root_folder = dl.join("show");
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while !root_folder.join("e2.bin").exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            root_folder.join("e2.bin").is_file(),
            "librqbit pre-sizes the files"
        );
        assert!(!handle.stats().await.files[e2].complete);

        // The destination already holds the real e2.bin.
        let target = tmp.path().join("offline").join(&hash);
        tokio::fs::create_dir_all(&target).await.unwrap();
        tokio::fs::copy(src.join("e2.bin"), target.join("e2.bin"))
            .await
            .unwrap();

        let moved = backend
            .relocate_torrent(
                &hash,
                TorrentPlacement {
                    output_folder: Some(target.clone()),
                    only_files: Some(vec![e2]),
                },
                vec![],
            )
            .await
            .expect("relocate");
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(stats.files[e2].complete, "destination data kept: {stats:?}");
        assert!(!stats.files[e1].complete);
        assert!(!root_folder.exists(), "placeholders dropped, folder gone");
        let bytes = tokio::fs::read(target.join("e2.bin")).await.unwrap();
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
    }

    /// A file of the relocated torrent without a single verified byte is a
    /// pre-sized sparse placeholder (librqbit sizes every wanted file at
    /// init, and a plain add wants everything): it is dropped with the old
    /// folder, never moved -- a cross-device copy would write its whole
    /// nominal length as zeros into the destination. The file with data
    /// moves and verifies.
    #[tokio::test]
    async fn relocate_torrent_drops_empty_placeholders_instead_of_moving_them() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        tokio::fs::create_dir_all(&src).await.unwrap();
        // Whole pieces per file, whichever order the torrent lists them
        // in: e1's verified pieces never spill into e2.
        write_payload(&src.join("e1.bin"), 32 * 1024).await;
        write_payload(&src.join("e2.bin"), 16 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // Readdir order decides the file indices -- look them up.
        let e1 = torrent_file_index(&bytes, "e1.bin");
        let e2 = torrent_file_index(&bytes, "e2.bin");
        // Only e1's data is in the session root.
        tokio::fs::remove_file(src.join("e2.bin")).await.unwrap();

        let dl = tmp.path().join("dl");
        let root_folder = dl.join("show");
        tokio::fs::create_dir_all(&root_folder).await.unwrap();
        tokio::fs::rename(src.join("e1.bin"), root_folder.join("e1.bin"))
            .await
            .unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(TorrentSource::Bytes(bytes), vec![])
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while !root_folder.join("e2.bin").exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            root_folder.join("e2.bin").is_file(),
            "librqbit pre-sizes the unwanted-so-far file"
        );
        let stats = handle.stats().await;
        assert!(
            stats.files[e1].complete && !stats.files[e2].complete,
            "{stats:?}"
        );

        let target = tmp.path().join("offline").join(&hash);
        let moved = backend
            .relocate_torrent(
                &hash,
                TorrentPlacement {
                    output_folder: Some(target.clone()),
                    only_files: Some(vec![e1]),
                },
                vec![],
            )
            .await
            .expect("relocate");
        assert!(target.join("e1.bin").is_file(), "the data moved");
        // librqbit's storage opens (creates, empty) every file of the
        // re-added torrent, so the test is the length, not the existence.
        let e2_len = tokio::fs::metadata(target.join("e2.bin"))
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(
            e2_len, 0,
            "the placeholder was not carried into the destination"
        );
        assert!(
            !root_folder.exists(),
            "placeholder dropped, old folder gone"
        );
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(stats.files[e1].complete, "moved data verified: {stats:?}");
        assert!(!stats.files[e2].complete);
    }

    /// While a torrent is still Initializing there is no chunk tracker to
    /// ask, so a file's own allocation decides: a sparse placeholder has no
    /// blocks, a file with bytes written has.
    #[cfg(unix)]
    #[tokio::test]
    async fn has_data_to_move_falls_back_to_allocated_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let sparse = tmp.path().join("sparse.bin");
        std::fs::File::create(&sparse)
            .unwrap()
            .set_len(8 * 1024 * 1024)
            .unwrap();
        let written = tmp.path().join("written.bin");
        write_payload(&written, 64 * 1024).await;
        let sparse_meta = std::fs::metadata(&sparse).unwrap();
        let written_meta = std::fs::metadata(&written).unwrap();

        assert!(!has_data_to_move(None, &sparse_meta), "no blocks, no data");
        assert!(has_data_to_move(None, &written_meta));
        // Known have-bytes win over the allocation either way.
        assert!(has_data_to_move(Some(1), &sparse_meta));
        assert!(!has_data_to_move(Some(0), &written_meta));
    }

    /// `remove_torrent_and_files` on a torrent placed in its own folder
    /// takes the pre-sized files and the folder with it; `remove_torrent`
    /// keeps them.
    #[tokio::test]
    async fn remove_torrent_and_files_takes_the_placed_folder_with_it() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("movie.bin"), 20 * 1024).await;
        let (bytes, hash) = make_torrent(&src.join("movie.bin")).await;
        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let place = |folder: &std::path::Path| TorrentPlacement {
            output_folder: Some(folder.to_path_buf()),
            only_files: Some(vec![0]),
        };

        let kept = tmp.path().join("offline").join("kept");
        let handle = backend
            .add_torrent_placed(TorrentSource::Bytes(bytes.clone()), vec![], place(&kept))
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        assert!(kept.join("movie.bin").is_file(), "pre-sized placeholder");
        backend.remove_torrent(&hash).await.unwrap();
        assert!(
            kept.join("movie.bin").is_file(),
            "remove_torrent keeps files"
        );

        let gone = tmp.path().join("offline").join("gone");
        let handle = backend
            .add_torrent_placed(TorrentSource::Bytes(bytes), vec![], place(&gone))
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        assert!(gone.join("movie.bin").is_file());
        backend.remove_torrent_and_files(&hash).await.unwrap();
        assert!(!gone.exists(), "files and folder removed: {gone:?}");
        assert!(backend.list_torrents().await.is_empty());
        assert!(dl.is_dir(), "session root untouched");
    }

    /// The cross-device fallback of `move_file` copies then removes the
    /// source, and leaves no partial target behind when the copy fails.
    #[tokio::test]
    async fn copy_then_remove_moves_the_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("nested").join("dst.bin");
        write_payload(&src, 8 * 1024).await;
        tokio::fs::create_dir_all(dst.parent().unwrap())
            .await
            .unwrap();
        super::copy_then_remove(&src, &dst).await.unwrap();
        assert!(!src.exists());
        let bytes = tokio::fs::read(&dst).await.unwrap();
        assert_eq!(bytes.len(), 8 * 1024);
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));

        let missing = tmp.path().join("missing.bin");
        let target = tmp.path().join("partial.bin");
        assert!(super::copy_then_remove(&missing, &target).await.is_err());
        assert!(!target.exists());
        assert!(
            super::move_file(&dst, &tmp.path().join("back.bin"))
                .await
                .is_ok()
        );
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn remove_torrent_drops_cached_torrent_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let cache_dir = dir.join(".cache");
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let cached = cache_dir.join(format!("{hash}.torrent"));
        tokio::fs::write(&cached, &torrent_bytes).await.unwrap();

        backend.remove_torrent(&hash).await.unwrap();
        assert!(!cached.exists(), "cached .torrent should be removed");
        // Data files are kept (delete_files=false).
        assert!(payload.exists(), "payload must survive remove_torrent");
    }
}
