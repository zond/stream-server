use anyhow::Context;
pub use auth::ServerAuth;
use axum::{
    Router,
    http::{StatusCode, header},
    routing::{get, post},
};
pub use cache_cleaner::{CacheUsage, EvictionReport};
use enginefs::EngineFS;
pub use enginefs::backend::{EngineStats, TorrentListenPort};
#[doc(hidden)]
pub use enginefs::pretend_volume_space;
pub use enginefs::{PIN_FREE_SPACE_MARGIN, PinDownloadError, UnpinOutcome};
pub use routes::downloads::DownloadInfo;
#[doc(hidden)]
pub use routes::stream::pretend_available_space;
pub use routes::system::{FileNotFound, ServerSettings, resolved_path};
pub use state::AppState;
use std::{
    future::{IntoFuture, pending},
    io::IsTerminal,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
/// Re-exported so an embedder can name the return type of
/// [`ServerHandle::lan_media_base_url`] without depending on `url` itself.
pub use url::Url;

/// Default log directives, applied WITHOUT any environment variable. The
/// application code lives in the `stream_server` lib crate (targets
/// `stream_server::*`); `server` covers the thin `server` bin
/// (src/main.rs). Both must be listed or the lib rename silently filters
/// out every log line. `RUST_LOG` only overrides this when it is set to a
/// non-empty value; an unset or blank `RUST_LOG` keeps this.
///
/// `librqbit` is here at WARN because it is the only place a storage
/// failure is reported at all: a piece that cannot be written (a full or
/// unwritable cache volume) is librqbit's own error, and with the crate
/// unlisted it was filtered out entirely -- a phone whose downloads were
/// failing logged not one line about it. enginefs reports the *torrent*
/// error state on top of that (`torrent_error_state`), but only when
/// something polls the torrent's statistics, so it cannot be relied on to
/// notice a disk problem by itself. WARN, not INFO: librqbit is chatty
/// per-peer at INFO and would drown the log.
/// `librqbit_dht::dht` and `librqbit_upnp` are turned back down to ERROR on
/// top of that, because both retry a network operation forever and WARN on
/// every single attempt. `librqbit_dht::dht`'s only WARN is the bootstrap
/// retry notifier (`dht.rs`'s `bootstrap_hostname_with_backoff`), which on a
/// network that drops the DHT's UDP fires for every host, forever -- a real
/// 28-minute Android session was hundreds of identical lines and no
/// conclusion. `librqbit_upnp`'s are the SSDP-discovery and port-forward
/// loops, which retry on a fixed interval whatever happens. Neither is
/// actionable per attempt, and the *state* both of them are trying to
/// describe is now reported once, properly, by
/// `diagnostics::dht_health` (DHT) or is simply not a problem (UPnP is
/// only enabled for a fixed listen port at all -- see
/// `TorrentListenPort::wants_upnp_forwarding`). ERROR rather than OFF so a
/// genuinely fatal DHT error still lands: `librqbit_dht`'s persistence
/// warnings are on a different target and keep their level.
pub(crate) const DEFAULT_LOG_FILTER: &str = "server=info,stream_server=info,tower_http=info,\
     enginefs=info,librqbit=warn,librqbit_dht::dht=error,librqbit_upnp=error";

pub const DEFAULT_HTTP_PORT: u16 = 11470;
pub const DEFAULT_HTTPS_PORT: u16 = 12470;

pub mod jni;

mod archives;
mod auth;
mod cache_cleaner;
mod diagnostics;
mod https;
mod lan_media;
mod proxy_cache;
mod proxy_streams;
mod routes;
mod ssdp;
mod state;
mod tui;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub https_addr: Option<SocketAddr>,
    pub public_base_url: Option<String>,
    /// Settings, logs and certificates. `None`
    /// uses the platform config dir (needs `HOME`/`XDG_*`); embedders must
    /// set it explicitly.
    pub config_dir: Option<PathBuf>,
    /// Torrent downloads, session/DHT state and archive caches. `None`
    /// means `config_dir/cache` when `config_dir` is set, otherwise the
    /// platform cache dir. No environment variable is consulted once
    /// `config_dir` is given.
    pub cache_dir: Option<PathBuf>,
    pub use_tui: bool,
    pub init_logging: bool,
    pub manage_process_globals: bool,
    pub listen_for_ctrl_c: bool,
    pub print_startup: bool,
    pub exit_process_on_shutdown_timeout: bool,
    pub enable_cache_cleaner: bool,
    pub enable_memory_sampler: bool,
    pub enable_ssdp_discovery: bool,
    pub graceful_shutdown_timeout: Duration,
    /// How the control API authenticates (media routes are always open).
    /// Defaults to a per-launch generated token; see [`ServerAuth`].
    pub auth: ServerAuth,
    /// The port librqbit's incoming BitTorrent listener binds:
    /// [`TorrentListenPort::Ephemeral`] for [`Self::embedded`] (any number of
    /// embedded servers coexist), the fixed `42000..42010` range for
    /// [`Self::binary_default`].
    pub torrent_listen_port: TorrentListenPort,
    /// Where the LAN media listener binds when it runs: a second HTTP
    /// listener serving [`lan_media_routes`] and nothing else, so a
    /// Chromecast or other receiver on the local network can fetch the bytes
    /// of what this device is playing while the control API -- and every
    /// route that creates or fetches anything -- stays on the loopback
    /// listener only (see [`crate::lan_media`]).
    ///
    /// `None` -- the default for both [`Self::embedded`] and
    /// [`Self::binary_default`] -- means there is no LAN listener at all and
    /// [`ServerHandle::set_lan_media`] has nothing to start. `Some(addr)`
    /// (typically `0.0.0.0:0`, letting the OS pick the port) is where
    /// [`ServerHandle::set_lan_media`] binds it per cast session, subject to
    /// the `lanMediaEnabled` setting. Nothing is bound at startup: the
    /// address says where a cast may put the listener, not that one is up,
    /// so a port in use fails the cast that asked for it, not the server.
    pub lan_media_addr: Option<SocketAddr>,
    /// Whether DHT bootstrap *names* are resolved to address literals before
    /// librqbit sees them (system resolver, then DNS over HTTPS, then a
    /// cache next to the routing table -- see
    /// `enginefs::backend::dht_bootstrap`). `true` for both
    /// [`Self::embedded`] and [`Self::binary_default`]: the Android embed is
    /// exactly the case this exists for, since that is where the system
    /// resolver was observed returning nothing.
    ///
    /// `false` does no DNS and no HTTP at start-up, leaving the names for
    /// librqbit to resolve itself. **Tests set this**, so `cargo test` makes
    /// no DNS query and no DoH request of its own.
    pub resolve_dht_bootstrap_names: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::embedded()
    }
}

impl ServerConfig {
    pub fn embedded() -> Self {
        Self {
            http_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_HTTP_PORT)),
            https_addr: None,
            public_base_url: None,
            config_dir: None,
            cache_dir: None,
            use_tui: false,
            init_logging: false,
            manage_process_globals: false,
            listen_for_ctrl_c: false,
            print_startup: false,
            exit_process_on_shutdown_timeout: false,
            enable_cache_cleaner: true,
            enable_memory_sampler: false,
            enable_ssdp_discovery: false,
            graceful_shutdown_timeout: Duration::from_secs(3),
            auth: ServerAuth::Generated,
            torrent_listen_port: TorrentListenPort::Ephemeral,
            lan_media_addr: None,
            resolve_dht_bootstrap_names: true,
        }
    }

    pub fn binary_default() -> Self {
        Self {
            http_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_HTTP_PORT)),
            https_addr: Some(SocketAddr::from(([0, 0, 0, 0], DEFAULT_HTTPS_PORT))),
            public_base_url: Some(format!("http://127.0.0.1:{DEFAULT_HTTP_PORT}")),
            config_dir: None,
            cache_dir: None,
            use_tui: false,
            init_logging: true,
            manage_process_globals: true,
            listen_for_ctrl_c: true,
            print_startup: true,
            exit_process_on_shutdown_timeout: true,
            enable_cache_cleaner: true,
            enable_memory_sampler: true,
            enable_ssdp_discovery: true,
            graceful_shutdown_timeout: Duration::from_secs(3),
            auth: ServerAuth::Generated,
            torrent_listen_port: TorrentListenPort::default(),
            lan_media_addr: None,
            resolve_dht_bootstrap_names: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSource {
    CtrlC,
    Tui,
    External,
}

/// What [`run`] reports once its HTTP listener is bound and the server is
/// ready to answer requests.
pub struct Started {
    pub bound_http_addr: SocketAddr,
    pub state: AppState,
    /// The server's own tokio runtime; library calls run on it.
    pub runtime: tokio::runtime::Handle,
}

pub struct ServerHandle {
    http_addr: SocketAddr,
    bound_http_addr: SocketAddr,
    state: AppState,
    runtime: tokio::runtime::Handle,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    join: std::thread::JoinHandle<anyhow::Result<Option<ShutdownSource>>>,
}

impl ServerHandle {
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn bound_http_addr(&self) -> SocketAddr {
        self.bound_http_addr
    }

    /// The bearer token the control routes require this launch
    /// (`Authorization: Bearer <token>`), or `None` when
    /// [`ServerAuth::Disabled`] left them open.
    pub fn auth_token(&self) -> Option<&str> {
        self.state.auth_token.as_deref()
    }

    /// The URL the server advertises (`settings.baseUrl`): `public_base_url`
    /// if configured, else `http://<connectable bound address>`.
    pub fn base_url(&self) -> &str {
        &self.state.base_url
    }

    /// Current settings -- the `values` of `GET /settings`.
    pub fn settings(&self) -> anyhow::Result<ServerSettings> {
        let state = self.state.clone();
        self.block_on_server(async move { state.settings.read().await.clone() })
    }

    /// Where the HTTPS listener is bound, or `None` while it is not running
    /// -- which is the case until `/get-https` (or
    /// [`Self::install_https_certificate`]) has put a certificate on disk,
    /// and always when [`ServerConfig::https_addr`] is unset. With a
    /// configured port of `0` this is the port the OS assigned.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        let state = self.state.clone();
        self.block_on_server(async move { state.https.bound_addr().await })
            .ok()
            .flatten()
    }

    /// Serve `cert_pem`/`key_pem` over HTTPS: written to the config dir and
    /// the HTTPS listener started -- or restarted, so the new certificate is
    /// the one presented -- on [`ServerConfig::https_addr`]. Returns the
    /// bound address. This is the second half of `GET /get-https`, which
    /// fetches the certificate from Stremio's API first; an embedder that
    /// obtains a certificate some other way installs it here. Refused when
    /// no HTTPS address is configured.
    pub fn install_https_certificate(
        &self,
        cert_pem: &str,
        key_pem: &str,
    ) -> anyhow::Result<SocketAddr> {
        let state = self.state.clone();
        let cert_pem = cert_pem.to_string();
        let key_pem = key_pem.to_string();
        self.block_on_server(async move {
            state
                .https
                .install_certificate(&state, &cert_pem, &key_pem)
                .await
        })?
    }

    /// Apply `patch` exactly as `POST /settings` would (same keys, same
    /// validation and merge semantics, same engine update and persistence)
    /// and return the resulting settings. The `bt*` settings report the HTTP
    /// route puts in its response is available to an embedder through
    /// [`Self::update_settings_with_report`].
    pub fn update_settings(&self, patch: serde_json::Value) -> anyhow::Result<ServerSettings> {
        Ok(self.update_settings_with_report(patch)?.0)
    }

    /// [`Self::update_settings`], also returning the
    /// [`enginefs::backend::BtSettingsReport`] the torrent session gave for
    /// the `bt*` values -- the same report the `POST /settings` response
    /// carries as `btSettings`, so an embedder can tell which of its
    /// settings took effect, which wait for a restart, and which the backend
    /// never honours.
    pub fn update_settings_with_report(
        &self,
        patch: serde_json::Value,
    ) -> anyhow::Result<(ServerSettings, enginefs::backend::BtSettingsReport)> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::system::update_settings(&state, &patch).await })?
    }

    /// Whether the mainline DHT works on this host, exactly what the `dht`
    /// key of `GET /stats.json` answers (see `routes::system::dht_status`).
    ///
    /// The DHT is a peer *source*, not a requirement: a torrent with working
    /// trackers downloads fine without one. A network that drops the DHT's
    /// UDP -- carrier-grade NAT, a firewalled mobile APN, a captive portal --
    /// leaves `ever_bootstrapped` false forever, which is what a client
    /// should surface as "DHT unavailable, using trackers only" rather than
    /// as an error. Cheap: two routing-table length reads.
    pub fn dht_status(&self) -> enginefs::backend::DhtStatus {
        routes::system::dht_status(&self.state)
    }

    /// Whether this server is using the connection while nothing is playing
    /// -- what a client's "working in the background" indicator shows, in
    /// each direction, exactly what `routes::system::background_traffic`
    /// answers.
    ///
    /// Two halves and their disjunction, not two signals: `downloading` and
    /// `uploading` are each "that direction's peer counters grew over the
    /// last window and nothing was playing over it", `active` is either.
    /// The conjunction with playback is taken in `routes::system` over the
    /// engine fields on purpose -- an embedder reading traffic and playback
    /// separately would sample them a moment apart and get a light that
    /// flickers whenever they disagree.
    ///
    /// Library-only: there is no control route for it, because the consumer
    /// is the embedding client rather than anything speaking HTTP, and
    /// `enginefs::traffic::BackgroundTraffic` is `serde`-serializable like
    /// every other type crossing this boundary. Cheap enough to poll every
    /// second or two -- per torrent that exists, one read of librqbit's live
    /// stats snapshot (a handful of counters) and the three live playback
    /// fields, nothing built, no idle clock touched -- and it creates
    /// nothing: no engine, no magnet add. The verdict changes when
    /// a window closes (`enginefs::traffic::TRAFFIC_WINDOW`) or the moment
    /// playback is seen, whichever comes first; asking faster than the
    /// window is otherwise answered from the standing reading.
    pub fn background_traffic(&self) -> anyhow::Result<enginefs::traffic::BackgroundTraffic> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::system::background_traffic(&state).await })
    }

    /// Torrent-level stats, exactly what `GET /{infoHash}/stats.json?tr=...`
    /// answers (see `routes::system::engine_stats`): `trackers` are the
    /// `tr=` values -- normalised exactly as the route normalises them
    /// (`tracker:` prefixes stripped, `dht:` entries dropped, trimmed), so a
    /// stream's `sources` list can be passed as is -- and are only used when
    /// this call is the one that creates the engine; a magnet still resolving
    /// reports `phase: resolvingMetadata` immediately, a failed add
    /// `phase: error`.
    pub fn engine_stats(
        &self,
        info_hash: &str,
        trackers: &[String],
    ) -> anyhow::Result<EngineStats> {
        let state = self.state.clone();
        let info_hash = info_hash.to_string();
        let trackers = trackers.to_vec();
        self.block_on_server(async move {
            routes::system::engine_stats(&state, &info_hash, trackers).await
        })
    }

    /// Per-file stats, exactly what `GET /{infoHash}/{fileIdx}/stats.json?tr=...`
    /// answers for an explicit index (see `routes::system::file_stats`;
    /// `trackers` as in [`Self::engine_stats`]). Fails
    /// with [`FileNotFound`] (the route's 404) for an index the torrent does
    /// not have once its metadata is known.
    pub fn file_stats(
        &self,
        info_hash: &str,
        file_idx: usize,
        trackers: &[String],
    ) -> anyhow::Result<EngineStats> {
        let state = self.state.clone();
        let info_hash = info_hash.to_string();
        let trackers = trackers.to_vec();
        let stats = self.block_on_server(async move {
            routes::system::file_stats(&state, &info_hash, &file_idx.to_string(), trackers, &[])
                .await
        })?;
        Ok(stats?)
    }

    /// Pin `file_idx` of `info_hash` as an offline download (see
    /// `routes::downloads::pin_download`, which the download control route
    /// shares): created through the magnet registry with `trackers` (as in
    /// [`Self::engine_stats`]) when new, placed under `settings.downloadsDir`
    /// when set, kept wanted and exempt from eviction, persisted across
    /// restarts. Fails with [`PinDownloadError`] -- `InsufficientSpace`
    /// below [`PIN_FREE_SPACE_MARGIN`], `FileNotFound` for a bad index.
    pub fn pin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        trackers: &[String],
    ) -> anyhow::Result<DownloadInfo> {
        let state = self.state.clone();
        let info_hash = info_hash.to_string();
        let trackers = trackers.to_vec();
        let info = self.block_on_server(async move {
            routes::downloads::pin_download(&state, &info_hash, file_idx, trackers).await
        })?;
        Ok(info?)
    }

    /// Drop the pin on `file_idx` of `info_hash` (see
    /// `routes::downloads::unpin_download`, which
    /// `DELETE /{infoHash}/{fileIdx}/download?deleteFiles=1` shares).
    /// [`UnpinOutcome::unpinned`] says whether a pin was cleared -- false
    /// for an unknown torrent or an unpinned file -- and
    /// [`UnpinOutcome::deleted_files`] whether data actually went, which is
    /// not simply `delete_files` echoed back. With `delete_files` the data
    /// goes too: the whole torrent when this was its last pin, only that
    /// file while other pins hold, the `<downloadsDir>/<infoHash>` folder
    /// for a pin whose torrent the backend does not have, and a `file_idx`
    /// the torrent does not have is refused with
    /// [`PinDownloadError::FileNotFound`] rather than taken for the whole
    /// torrent. Without it only the pin goes and the engine becomes an
    /// ordinary, evictable one again.
    pub fn unpin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        delete_files: bool,
    ) -> anyhow::Result<UnpinOutcome> {
        let state = self.state.clone();
        let info_hash = info_hash.to_string();
        let outcome = self.block_on_server(async move {
            routes::downloads::unpin_download(&state, &info_hash, file_idx, delete_files).await
        })?;
        Ok(outcome?)
    }

    /// Every pinned download, exactly what `GET /downloads.json` answers
    /// (see `routes::downloads::downloads`).
    pub fn downloads(&self) -> anyhow::Result<Vec<DownloadInfo>> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::downloads::downloads(&state).await })
    }

    /// Where `file_idx` of `info_hash` is on disk (the `path` of its
    /// [`Self::downloads`] entry), for handing a finished download to a
    /// local player. `None` when the torrent is not managed right now or
    /// the backend does not know the path yet; never creates an engine.
    pub fn download_path(
        &self,
        info_hash: &str,
        file_idx: usize,
    ) -> anyhow::Result<Option<String>> {
        let state = self.state.clone();
        let info_hash = info_hash.to_string();
        self.block_on_server(async move {
            routes::downloads::download_path(&state, &info_hash, file_idx).await
        })
    }

    /// What the cache currently occupies against its configured limit, using
    /// the same occupancy accounting the cleaner uses ([`cache_cleaner::occupied_bytes`]
    /// -- allocated blocks, not apparent length), exactly what `GET /cache.json`
    /// answers (see `routes::cache::cache_usage`). [`CacheUsage::protected_bytes`]
    /// and `protected_files` are what a live engine or a pinned download is
    /// holding right now, so a caller can tell "over the limit but nothing
    /// is evictable" apart from "a clean would help" without running one.
    ///
    /// Walks the cache tree, but only with `stat` calls -- no file reads --
    /// and only as many of them as there are files currently in the cache;
    /// it is the same walk the background cleaner already performs on every
    /// debounced or hourly pass, so one call per "Storage" screen open or
    /// manual refresh is cheap. It is not bounded or cached here, so do not
    /// poll it on a sub-second timer -- a few seconds between calls is
    /// plenty for a UI.
    pub fn cache_usage(&self) -> anyhow::Result<CacheUsage> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::cache::cache_usage(&state).await })
    }

    /// Run one eviction pass immediately and report what it freed, exactly
    /// what `POST /cache/clean` answers (see `routes::cache::clean_cache_now`).
    /// Respects exactly the protections the scheduled sweep does -- a pinned
    /// download's files and anything a live engine is writing are never
    /// touched, however far over the limit the cache is;
    /// [`EvictionReport::shortfall_message`] is the line to show the user
    /// when that leaves it still over: cleaning cannot reclaim what a live
    /// engine or a pin protects, and the fix is to stop the stream or unpin
    /// the download, not to run the clean again.
    pub fn clean_cache_now(&self) -> anyhow::Result<EvictionReport> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::cache::clean_cache_now(&state).await })?
    }

    /// End every proxied stream the client marked with `token`, and answer
    /// how many that was -- exactly what
    /// `POST /proxy-streams/{token}/close` does, through the same function.
    ///
    /// The token is the client's own: it mints one per player and puts it in
    /// the `/proxy` URL that player is given (`p=`). Closing it here makes
    /// that player's read fail at once instead of waiting out
    /// `network-timeout`, which is generous on purpose, **and retires the
    /// token**: a later `/proxy` request carrying it is refused with `410
    /// Gone` rather than given a fresh stream. Both halves are needed --
    /// ffmpeg reconnects through the URL it already has, so the broken read
    /// alone would only be a stutter.
    ///
    /// Ask the player to quit before calling this, not after: a cancelled
    /// demuxer has already ended its own read, so the close finds nothing
    /// live and answers 0, which is the outcome to want. Zero is an ordinary
    /// answer anyway -- the player may already have finished -- and closing
    /// twice is harmless. See [`crate::proxy_streams`] for what this does
    /// not end: a demuxer wedged on something other than the read.
    pub fn close_proxy_streams(&self, token: &str) -> usize {
        self.state.proxy_streams.close(token)
    }

    /// Tell the server whether the app it lives in is in the background.
    ///
    /// `true` puts the torrent session on a lean footprint
    /// ([`enginefs::backend::Footprint::Lean`]): every torrent keeps
    /// running -- it goes on seeding, and a pinned download goes on
    /// downloading -- but with [`enginefs::backend::LEAN_PEER_LIMIT`] peers
    /// instead of the configured limit, the surplus hung up least useful
    /// first, and its peer table pruned of every `Dead` and `NotNeeded`
    /// entry. That is the share of a backgrounded server's memory that was
    /// measured to be both the largest and the one still growing (per-peer
    /// buffers, tasks and table entries), and the OS's low-memory killer
    /// takes the fattest background process first. Nothing is paused and
    /// nothing on disk is touched, so a stream request that arrives while
    /// lean is served like any other, from fewer peers.
    ///
    /// The pruning is not free, and only its `NotNeeded` half is cheap:
    /// those peers were hung up on and the table kept them to remember not
    /// to dial them again, while a `Dead` entry is a peer waiting out a
    /// backoff *to be re-dialled*, and forgetting one cancels that
    /// reconnect -- it returns when a source names it again or it dials us.
    /// See [`enginefs::backend::Footprint`] for why both go.
    ///
    /// `false` restores the configured limit and re-queues the parked
    /// peers, onto the tail of the same queue every discovered address
    /// joins: they come back, but behind whatever backlog is already
    /// waiting, not first.
    ///
    /// Call it from the app's lifecycle hooks (hidden/paused → `true`,
    /// resumed → `false`). Idempotent, cheap, synchronous (no runtime
    /// hop: atomics, a read lock per torrent and one message per surplus
    /// peer), and safe before any torrent exists -- a torrent added while
    /// lean starts lean. The app's own side of going to the background
    /// (pausing the player, dropping image caches) is the app's business.
    pub fn set_background(&self, background: bool) {
        let footprint = if background {
            enginefs::backend::Footprint::Lean
        } else {
            enginefs::backend::Footprint::Full
        };
        self.state.engine.set_footprint(footprint);
        if !Arc::ptr_eq(&self.state.engine, &self.state.download_engine) {
            self.state.download_engine.set_footprint(footprint);
        }
    }

    /// Whether [`Self::set_background`] last put the server in the
    /// background.
    pub fn is_background(&self) -> bool {
        self.state.engine.footprint() == enginefs::backend::Footprint::Lean
    }

    /// The live-peer cap librqbit is enforcing on `info_hash` right now --
    /// the one lever [`Self::set_background`] moves -- or `None` when this
    /// server holds no such torrent.
    ///
    /// Doc-hidden, not part of the embedder API: it exists so a test can
    /// watch the footprint arrive on a torrent the server actually holds
    /// without a swarm to count peers in, reading the same
    /// `ManagedTorrentShared::peer_limit` the enginefs tests read. Nothing
    /// is created or started by asking.
    #[doc(hidden)]
    pub fn torrent_peer_limit(&self, info_hash: &str) -> Option<usize> {
        let state = self.state.clone();
        let info_hash = info_hash.to_lowercase();
        self.block_on_server(async move {
            let engine = state.engine.get_engine(&info_hash).await?;
            Some(engine.handle.handle.shared.peer_limit())
        })
        .ok()
        .flatten()
    }

    /// The address librqbit accepts peer connections on, or `None` when
    /// the session is not listening. With [`TorrentListenPort::Ephemeral`]
    /// this is the port the OS assigned; a fixed range reports the port
    /// that was free.
    pub fn torrent_listen_addr(&self) -> Option<SocketAddr> {
        self.state.engine.backend.session.listen_addr()
    }

    /// How many proxied streams are being read right now, over all tokens:
    /// the number of players actually attached to this server through
    /// `/proxy`, which nothing outside the process could work out before.
    ///
    /// Cheap: one map length, no runtime hop.
    pub fn proxy_streams_live(&self) -> usize {
        self.state.proxy_streams.live()
    }

    /// Start or stop the LAN media listener (see [`crate::lan_media`]): a
    /// second HTTP listener on [`ServerConfig::lan_media_addr`] serving
    /// [`lan_media_routes`] and nothing else, for handing the bytes of what
    /// this device already plays to a Chromecast or other receiver on the
    /// local network. Returns the address it is bound to afterwards --
    /// `Some` after a successful start, `None` after a stop.
    ///
    /// Meant to be called around a cast session, so the LAN surface exists
    /// only while something is actually casting. Both directions are
    /// idempotent.
    ///
    /// `set_lan_media(true)` fails when the `lanMediaEnabled` setting is
    /// `false` (the default -- an operator can forbid the listener outright),
    /// when [`ServerConfig::lan_media_addr`] is unset, or when the bind
    /// fails.
    ///
    /// `set_lan_media(false)` **aborts** the listener rather than draining
    /// it: when it returns, the listener socket is closed and the port is
    /// free, so nothing new can reach the LAN surface -- but a response that
    /// was already streaming keeps running to its end on its own connection
    /// task. Stopping ends new fetches, not the fetch in progress; see
    /// [`lan_media::LanMedia::stop`] for why, and for what stopping the bytes
    /// too would cost. The loopback listener and every request in flight on
    /// it are untouched.
    pub fn set_lan_media(&self, enabled: bool) -> anyhow::Result<Option<SocketAddr>> {
        let state = self.state.clone();
        self.block_on_server(async move {
            if !enabled {
                state.lan_media.stop().await;
                return Ok(None);
            }
            anyhow::ensure!(
                state.settings.read().await.lan_media_enabled,
                "the lanMediaEnabled setting forbids the LAN media listener; \
                 set it through POST /settings (or update_settings) first"
            );
            state.lan_media.start(&state).await.map(Some)
        })?
    }

    /// The address the LAN media listener is bound to, or `None` when it is
    /// not running. With a configured port of `0` this is the port the OS
    /// assigned.
    pub fn lan_media_addr(&self) -> Option<SocketAddr> {
        let state = self.state.clone();
        self.block_on_server(async move { state.lan_media.bound_addr().await })
            .ok()
            .flatten()
    }

    /// Whether the LAN media listener is running right now.
    pub fn lan_media_running(&self) -> bool {
        self.lan_media_addr().is_some()
    }

    /// How many requests have reached the LAN media listener since the
    /// current cast session began -- per session, not per process. Every
    /// [`ServerHandle::set_lan_media`] start resets it, including one that
    /// finds the listener already running (casting to a second receiver
    /// mid-session does exactly that, and the count it wants is its own),
    /// and a stop resets it too, so this reads zero while nothing is
    /// listening.
    ///
    /// It answers one question nothing else can: whether the receiver ever
    /// came back for the stream. A receiver told an address it cannot route
    /// to never reports an error -- the connect hangs -- so from the outside
    /// it is indistinguishable from one that is buffering, and this is the
    /// only signal that separates them. Zero, well after a load, means the
    /// address was wrong and the cast cannot recover; non-zero means the
    /// receiver reached this device and whatever went wrong afterwards is
    /// about the media, not the network.
    ///
    /// Cheap: one relaxed atomic load, no runtime hop, safe to poll.
    pub fn lan_media_requests_served(&self) -> u64 {
        self.state.lan_media.requests_served()
    }

    /// The base URL to hand a receiver at `for_peer` (e.g.
    /// `http://192.168.1.20:11471/`), so a media URL built on it is one that
    /// receiver can actually reach: the host is the local interface sharing
    /// `for_peer`'s subnet, taken from the same interface enumeration
    /// `GET /network-info` answers from, since the first interface on a host
    /// with a VPN or a container bridge is regularly the wrong one. A
    /// listener bound to one specific address reports that address as is.
    ///
    /// `None` when the LAN media listener is not running -- which is also the
    /// answer that says a cast URL cannot be built yet.
    pub fn lan_media_base_url(&self, for_peer: IpAddr) -> Option<Url> {
        let state = self.state.clone();
        self.block_on_server(async move { state.lan_media.base_url_for(for_peer).await })
            .ok()
            .flatten()
    }

    /// Run `fut` on the server's runtime and wait for it. The engines spawn
    /// tasks and expect the server's multi-threaded runtime, so library calls
    /// never execute on the caller's thread. This blocks the calling thread;
    /// do not call it from an async task on a runtime with no spare threads.
    fn block_on_server<F>(&self, fut: F) -> anyhow::Result<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv()
            .map_err(|_| anyhow::anyhow!("server runtime is gone (has the server stopped?)"))
    }

    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.shutdown_tx.try_send(()) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("server is already stopped"))
            }
        }
    }

    pub fn join(self) -> anyhow::Result<Option<ShutdownSource>> {
        self.join
            .join()
            .map_err(|_| anyhow::anyhow!("server thread panicked"))?
    }
}

pub fn start(cfg: ServerConfig) -> anyhow::Result<ServerHandle> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let thread_cfg = cfg.clone();

    let join = std::thread::Builder::new()
        .name("stream-server".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run(thread_cfg, shutdown_rx, Some(ready_tx)))
        })?;

    let Started {
        bound_http_addr,
        state,
        runtime,
    } = match ready_rx.blocking_recv() {
        Ok(started) => started,
        Err(_) => {
            return match join.join() {
                Ok(result) => match result {
                    Ok(_) => Err(anyhow::anyhow!("server exited before reporting ready")),
                    Err(err) => Err(err),
                },
                Err(_) => Err(anyhow::anyhow!(
                    "server thread panicked before reporting ready"
                )),
            };
        }
    };

    Ok(ServerHandle {
        http_addr: connectable_addr(bound_http_addr),
        bound_http_addr,
        state,
        runtime,
        shutdown_tx,
        join,
    })
}

/// Resolve the config and cache directories from `cfg` alone whenever it names
/// a `config_dir`: an unset `cache_dir` then lands *inside* the config dir
/// instead of consulting the OS user directories. Embedders (Android in
/// particular) run without `HOME`/`XDG_*` and no passwd fallback, so nothing
/// on the startup path may depend on an environment-derived location. Only
/// when neither directory is given (the desktop binary) do we fall back to the
/// platform defaults.
fn resolve_dirs(cfg: &ServerConfig) -> anyhow::Result<(PathBuf, PathBuf)> {
    let config_dir = match cfg.config_dir.clone() {
        Some(path) => path,
        None => dirs::config_dir()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Could not find config directory; set ServerConfig::config_dir explicitly"
                )
            })?
            .join("stremio-server"),
    };
    let cache_dir = match cfg.cache_dir.clone() {
        Some(path) => path,
        None if cfg.config_dir.is_some() => config_dir.join("cache"),
        None => dirs::cache_dir()
            .map(|dir| dir.join("stremio-server"))
            .unwrap_or_else(|| config_dir.join("cache")),
    };
    Ok((config_dir, cache_dir))
}

pub async fn run(
    cfg: ServerConfig,
    mut external_shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    ready_tx: Option<tokio::sync::oneshot::Sender<Started>>,
) -> anyhow::Result<Option<ShutdownSource>> {
    let listener = tokio::net::TcpListener::bind(cfg.http_addr)
        .await
        .with_context(|| format!("failed to bind HTTP listener on {}", cfg.http_addr))?;
    let bound_http_addr = listener.local_addr()?;
    let public_http_addr = connectable_addr(bound_http_addr);
    let base_url = cfg
        .public_base_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", public_http_addr));

    let (tui_log_layer, tui_rx) = if cfg.use_tui {
        let (tx, rx) = crossbeam_channel::bounded(1000);
        (Some(tui::log_layer::TuiLogLayer::new(tx)), Some(rx))
    } else {
        (None, None)
    };

    let (config_dir, cache_dir) = resolve_dirs(&cfg)?;
    let log_dir = config_dir.join("logs");

    tokio::fs::create_dir_all(&config_dir).await?;
    tokio::fs::create_dir_all(&cache_dir).await?;
    tokio::fs::create_dir_all(&log_dir).await?;

    diagnostics::logging::init_process_start();
    if cfg.manage_process_globals {
        diagnostics::logging::install_panic_hook();
    }

    let mut startup_log_paths = None;
    if cfg.init_logging {
        let log_writers = diagnostics::logging::open_log_writers(&log_dir)?;
        let human_log_path = log_writers.human_path.clone();
        let archive_log_path = log_writers.archive_path.clone();
        let json_log_path = log_writers.json_path.clone();
        let human_writer = log_writers.human_writer;
        let archive_writer = log_writers.archive_writer;
        let json_writer = log_writers.json_writer;
        let guards = log_writers.guards;

        let log_filter = std::env::var("RUST_LOG")
            .ok()
            .filter(|directives| !directives.trim().is_empty())
            .map(tracing_subscriber::EnvFilter::new)
            .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER));
        let registry = tracing_subscriber::registry().with(log_filter);
        let human_file_layer = tracing_subscriber::fmt::layer()
            .with_writer(human_writer)
            .with_ansi(false);
        let archive_file_layer = tracing_subscriber::fmt::layer()
            .with_writer(archive_writer)
            .with_ansi(false);
        let json_file_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(json_writer)
            .with_ansi(false);

        let init_result = if let Some(layer) = tui_log_layer {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .with(layer)
                .try_init()
        } else if std::io::stdout().is_terminal() {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .with(tracing_subscriber::fmt::layer())
                .try_init()
        } else {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .try_init()
        };

        if init_result.is_ok() {
            diagnostics::logging::store_log_guards(guards);
            startup_log_paths = Some((human_log_path, archive_log_path, json_log_path));
        }
    }

    tracing::info!("Config Dir: {:?}", config_dir);
    tracing::info!("Cache/Download Dir: {:?}", cache_dir);
    tracing::info!("Log Dir: {:?}", log_dir);
    if cfg.manage_process_globals {
        diagnostics::logging::install_native_crash_handler(&log_dir);
    }
    if let Some((human_log_path, archive_log_path, json_log_path)) = startup_log_paths {
        diagnostics::logging::log_startup_context(
            &config_dir,
            &cache_dir,
            &log_dir,
            &human_log_path,
            &archive_log_path,
            &json_log_path,
        );
    }

    let default_settings = routes::system::ServerSettings {
        cache_root: cache_dir.to_string_lossy().to_string(),
        ..routes::system::ServerSettings::default()
    };

    // One `SettingsFile` for the process: the tracker refresher writes
    // through it from here on, and `AppState` takes the same one below, so
    // the two never race for the file (see `state::SettingsFile`).
    let settings_file = Arc::new(state::SettingsFile::new(config_dir.join("settings.json")));
    let settings = settings_file.load(&default_settings);
    let settings_arc = Arc::new(tokio::sync::RwLock::new(settings.clone()));
    let tracker_storage = Arc::new(state::TrackerStorageBridge::new(
        settings_arc.clone(),
        settings_file.clone(),
    ));

    let backend_config = enginefs::backend::BackendConfig {
        listen_port: cfg.torrent_listen_port.clone(),
        cache: enginefs::backend::priorities::EngineCacheConfig {
            size: routes::system::cache_size_bytes(settings.cache_size),
            enabled: true,
        },
        growler: enginefs::backend::Growler::default(),
        peer_search: enginefs::backend::PeerSearch {
            min: settings.bt_min_peers_for_stable,
            ..Default::default()
        },
        swarm_cap: enginefs::backend::SwarmCap::default(),
        speed_profile: enginefs::backend::TorrentSpeedProfile {
            bt_download_speed_hard_limit: settings.bt_download_speed_hard_limit,
            bt_download_speed_soft_limit: settings.bt_download_speed_soft_limit,
            bt_handshake_timeout: settings.bt_handshake_timeout,
            bt_max_connections: settings.bt_max_connections,
            bt_min_peers_for_stable: settings.bt_min_peers_for_stable,
            bt_request_timeout: settings.bt_request_timeout,
        },
        privacy: enginefs::backend::TorrentPrivacyConfig {
            bt_enable_dht: settings.bt_enable_dht,
            bt_enable_pex: settings.bt_enable_pex,
            bt_enable_lsd: settings.bt_enable_lsd,
            bt_encryption_mode: settings.bt_encryption_mode,
            bt_anonymous_mode: settings.bt_anonymous_mode,
            bt_allow_multiple_connections_per_ip: settings.bt_allow_multiple_connections_per_ip,
            bt_listen_interfaces: settings.bt_listen_interfaces.clone(),
            bt_outgoing_interfaces: settings.bt_outgoing_interfaces.clone(),
            bt_outgoing_port: settings.bt_outgoing_port,
            bt_num_outgoing_ports: settings.bt_num_outgoing_ports,
            bt_proxy_type: settings.bt_proxy_type,
            bt_proxy_host: settings.bt_proxy_host.clone(),
            bt_proxy_port: settings.bt_proxy_port,
            bt_proxy_username: settings.bt_proxy_username.clone(),
            bt_proxy_password: settings.bt_proxy_password.clone(),
            bt_proxy_hostnames: settings.bt_proxy_hostnames,
            bt_proxy_peer_connections: settings.bt_proxy_peer_connections,
            bt_proxy_tracker_connections: settings.bt_proxy_tracker_connections,
            bt_proxy_send_host_in_connect: settings.bt_proxy_send_host_in_connect,
            bt_validate_https_trackers: settings.bt_validate_https_trackers,
            bt_ssrf_mitigation: settings.bt_ssrf_mitigation,
        },
        dht_bootstrap_nodes: settings.dht_bootstrap_nodes.clone().unwrap_or_default(),
        dht_bootstrap_dns: if cfg.resolve_dht_bootstrap_names {
            enginefs::backend::dht_bootstrap::DhtBootstrapDns::Resolve
        } else {
            enginefs::backend::dht_bootstrap::DhtBootstrapDns::Off
        },
    };

    // One engine. `AppState` carries two fields, `engine` and
    // `download_engine`, from a design that meant to pair a memory-only
    // stream engine with a disk-backed download engine; librqbit sessions
    // always persist to disk and no memory-only storage was ever built, so
    // `EngineFS::new_disk_backed` is `new_with_storage` under another name
    // and the same `Arc` goes in both fields. (There used to be a retry
    // through `new_with_storage` when `new_disk_backed` failed, described as
    // falling back to memory-only mode -- the same constructor, failing the
    // same way, so a failure here is a failure.) Whatever reads the two
    // fields reads one instance twice, which `Arc::ptr_eq` lets it skip.
    let download_engine = Arc::new(
        EngineFS::new_disk_backed(cache_dir.clone(), backend_config, Some(tracker_storage)).await?,
    );
    let engine = download_engine.clone();

    let mut state = AppState::new_with_shared_settings_log_dir_and_download_engine(
        engine,
        download_engine,
        settings_arc.clone(),
        config_dir.clone(),
        log_dir.clone(),
    );
    state.settings_file = settings_file;
    state.base_url = base_url.clone();
    state.http_addr = public_http_addr;
    state.auth_token = cfg.auth.resolve()?.map(Arc::from);
    state.lan_media = Arc::new(lan_media::LanMedia::new(cfg.lan_media_addr));
    state.https = Arc::new(https::HttpsListener::new(cfg.https_addr, &config_dir));
    match state.auth_token.as_deref() {
        Some(token) => {
            tracing::info!("control API requires `Authorization: Bearer <token>`");
            // The token is a secret and must never reach `tracing`: the log
            // files (the append-only archive included) would keep it. A
            // generated token has no other way to reach the operator of the
            // standalone binary, so it goes to stdout once; a `--token` /
            // `STREAM_SERVER_TOKEN` token is already known to whoever set it,
            // and an embedder reads `ServerHandle::auth_token`.
            if cfg.print_startup && cfg.auth == ServerAuth::Generated {
                println!("control API token: {token}");
            }
        }
        None => tracing::warn!("control API authentication is disabled; every route is open"),
    }

    let mut cleared_downloads_dir = false;
    {
        let mut settings = settings_arc.write().await;
        state.engine.set_seeding_enabled(settings.seeding_enabled);
        state
            .download_engine
            .set_seeding_enabled(settings.seeding_enabled);
        // A persisted downloadsDir that cannot be used any more (unmounted
        // drive, permissions) is cleared rather than kept as a setting the
        // engines silently ignore: pins fall back to the cache root,
        // `GET /settings` says so, and so does the settings file (below,
        // once the lock is released) -- an embedder reading it sees the
        // same value, and the next boot does not warn again.
        if let Some(raw) = settings.downloads_dir.clone() {
            match routes::system::prepare_downloads_dir(&raw, &routes::system::cache_roots(&state))
                .await
            {
                Ok(path) => {
                    state.engine.set_downloads_dir(Some(path.clone()));
                    state.download_engine.set_downloads_dir(Some(path));
                }
                Err(error) => {
                    tracing::warn!(
                        downloads_dir = %raw,
                        error = %format!("{error:#}"),
                        "downloadsDir is unusable; clearing it (downloads go to the cache root)"
                    );
                    settings.downloads_dir = None;
                    cleared_downloads_dir = true;
                }
            }
        }
    }
    if cleared_downloads_dir && let Err(error) = state.save_settings().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "could not persist the cleared downloadsDir"
        );
    }

    // The proxy cache's own launch-time sweep, before the router can serve a
    // request that writes into it. It removes the temporary files a kill left
    // mid-write and nothing else -- everything else under that root is cache
    // that is meant to survive a restart. See `proxy_cache::sweep`.
    {
        let root = state.proxy_cache.root().to_path_buf();
        if let Err(error) = tokio::task::spawn_blocking(move || proxy_cache::sweep(&root)).await {
            tracing::warn!(%error, "the proxy cache sweep did not finish");
        }
    }

    let mut background_tasks = Vec::new();
    // The engines' tracker refresher is a forever loop like the rest, so it
    // belongs in the list this function aborts. Detached, it is still parked
    // on its hourly `interval.tick()` when the runtime goes down, and a task
    // parked on a timer is what panics there: the time driver fires every
    // pending timer as it shuts down, and a worker that polls the task it
    // just woke asserts on the shut-down driver ("A Tokio 1.x context was
    // found, but it is being shutdown") -- caught by tokio, so only noise,
    // but noise a real panic can hide in. See
    // `TrackerManager::take_refresh_task`. The task has one owner, so it is
    // taken, not read: both engines are asked because `AppState` allows two,
    // and the second call yields `None` here, where they are the same `Arc`.
    background_tasks.extend(state.engine.take_tracker_refresh_task());
    background_tasks.extend(state.download_engine.take_tracker_refresh_task());
    // And the engines' housekeeping sweep, the other forever loop they start
    // for themselves. It is parked on a 15-second sleep between passes rather
    // than an hourly one, so of the two it is the likelier to be holding a
    // timer when the driver goes down.
    background_tasks.extend(state.engine.take_sweep_task());
    background_tasks.extend(state.download_engine.take_sweep_task());
    // And the reconciler, which recomputes what every torrent should be
    // doing and makes it so -- stopping one before it runs the volume out,
    // starting it again when there is room. One per engine, and one for
    // both where they are the same `Arc` (a second reconciler would only
    // race the first for the same torrents).
    background_tasks.push(state.engine.start_reconciler());
    if !Arc::ptr_eq(&state.engine, &state.download_engine) {
        background_tasks.push(state.download_engine.start_reconciler());
    }
    if cfg.enable_cache_cleaner {
        background_tasks.push(cache_cleaner::start(Arc::new(state.clone())));
    }
    if cfg.enable_memory_sampler {
        background_tasks.push(diagnostics::start_memory_sampler(state.clone()));
    }
    if cfg.enable_ssdp_discovery {
        background_tasks.push(diagnostics::logging::spawn_logged(
            "ssdp-discovery",
            crate::ssdp::start_discovery(state.devices.clone()),
        ));
    }
    // Unconditional and unconfigurable: two routing-table length reads on a
    // timer, and the only thing that ever states whether the DHT works here.
    // See `diagnostics::dht_health`.
    background_tasks.push(diagnostics::dht_health::start(state.stream_engine()));

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
    if cfg.use_tui
        && let Some(rx) = tui_rx
    {
        tui::start_tui(Arc::new(state.clone()), rx, shutdown_tx);
    }

    let app = build_router(state.clone());

    // The LAN media listener is deliberately not started here. It exists for
    // the length of a cast session (see `lan_media`), and
    // `ServerHandle::set_lan_media` is the one path that binds it -- which is
    // also the only path that consults the `lanMediaEnabled` veto. Binding a
    // configured address at boot put the LAN surface up before any cast had
    // asked, with the persisted veto loaded a few lines above and ignored,
    // and made a port already in use fatal to the loopback server the
    // embedder actually needed. A bind failure is now the cast caller's
    // error and nobody else's; the handle below is for the stop at shutdown.
    let lan_media = state.lan_media.clone();

    // The HTTPS listener comes up at boot only when an earlier `/get-https`
    // left its certificate on disk; otherwise that route starts it when it
    // fetches one (see `https`). The same control block serves both, so the
    // route answers with the port that is actually bound. Before the ready
    // signal, like the plain listener: a configured address with a
    // certificate is either serving or has failed the start by the time
    // `start` hands back a handle.
    let https_listener = state.https.clone();
    https_listener.start_if_certificate_present(&state).await?;

    tracing::info!("listening on {}", bound_http_addr);
    if cfg.print_startup {
        println!("listening on {}", bound_http_addr);
        println!("EngineFS server started at {}", base_url);
    }
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(Started {
            bound_http_addr,
            state,
            runtime: tokio::runtime::Handle::current(),
        });
    }

    let (shutdown_started_tx, mut shutdown_started_rx) =
        tokio::sync::oneshot::channel::<ShutdownSource>();
    let listen_for_ctrl_c = cfg.listen_for_ctrl_c;
    let shutdown = async move {
        let source = tokio::select! {
            _ = maybe_ctrl_c(listen_for_ctrl_c) => {
                tracing::info!("Ctrl+C received, shutting down");
                ShutdownSource::CtrlC
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received from TUI, shutting down");
                ShutdownSource::Tui
            }
            _ = external_shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received from external controller, shutting down");
                ShutdownSource::External
            }
        };

        let _ = shutdown_started_tx.send(source);
    };

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .into_future();

    tokio::pin!(server);

    let shutdown_source = tokio::select! {
        result = &mut server => {
            result?;
            match shutdown_started_rx.try_recv() {
                Ok(source) => Some(source),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => None,
            }
        }
        Ok(source) = &mut shutdown_started_rx => {
            match tokio::time::timeout(cfg.graceful_shutdown_timeout, &mut server).await {
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    if cfg.exit_process_on_shutdown_timeout {
                        tracing::warn!(
                            ?source,
                            timeout_secs = cfg.graceful_shutdown_timeout.as_secs(),
                            "Shutdown taking too long, forcing process exit"
                        );
                        std::process::exit(0);
                    }

                    tracing::warn!(
                        ?source,
                        timeout_secs = cfg.graceful_shutdown_timeout.as_secs(),
                        "Shutdown taking too long, dropping server future so restart can continue"
                    );
                }
            }
            Some(source)
        }
    };

    for task in background_tasks {
        task.abort();
    }
    lan_media.stop().await;
    https_listener.stop().await;

    Ok(shutdown_source)
}

async fn maybe_ctrl_c(enabled: bool) {
    if enabled {
        let _ = tokio::signal::ctrl_c().await;
    } else {
        pending::<()>().await;
    }
}

fn connectable_addr(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        _ => addr,
    }
}

fn peer_from_request(req: &axum::extract::Request) -> Option<SocketAddr> {
    req.extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
}

async fn fallback_handler(req: axum::extract::Request) -> impl axum::response::IntoResponse {
    diagnostics::logging::log_unhandled(
        "no matching route (404)",
        StatusCode::NOT_FOUND.as_u16(),
        peer_from_request(&req),
        req.method(),
        req.uri(),
        Some(req.version()),
        req.headers(),
    );
    StatusCode::NOT_FOUND
}

async fn method_not_allowed_handler(
    req: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    diagnostics::logging::log_unhandled(
        "method not allowed for matched route (405)",
        StatusCode::METHOD_NOT_ALLOWED.as_u16(),
        peer_from_request(&req),
        req.method(),
        req.uri(),
        Some(req.version()),
        req.headers(),
    );
    StatusCode::METHOD_NOT_ALLOWED
}

/// The proxy cache's chunk size, exported doc-hidden so the tests that pin
/// what it stores can speak in whole chunks instead of repeating a number
/// that is the store's to choose (see `proxy_cache::CHUNK_BYTES`).
#[doc(hidden)]
pub use proxy_cache::CHUNK_BYTES as PROXY_CACHE_CHUNK_BYTES;

pub fn build_router(state: AppState) -> Router {
    let control = control_router().route_layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth::require_bearer,
    ));

    Router::new()
        .merge(media_router())
        .merge(control)
        .fallback(fallback_handler)
        .method_not_allowed_fallback(method_not_allowed_handler)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                tracing::info_span!(
                    "request",
                    method = %request.method(),
                    path = request.uri().path(),
                )
            }),
        )
        .layer(cors_layer())
        .with_state(state)
}

/// CORS for every route the server serves.
///
/// `CorsLayer::permissive()` answers `*` to all four lists. That is not quite
/// enough here:
///
/// * A Google Cast receiver plays through a browser media element, so a media
///   request is a CORS request even for a plain MP4 as soon as tracks are
///   involved, and Google's receiver CORS requirements name
///   `Content-Type`, `Accept-Encoding` and `Range` as the request headers the
///   server has to allow. Naming them is the guarantee; a wildcard only works
///   for as long as the receiver's fetch implementation expands it.
/// * The `*` wildcard never covers `Authorization` (the Fetch standard
///   excludes it by name), so a browser-hosted client could not send the
///   control API's bearer header at all under `permissive()`.
/// * A player that seeks needs `Content-Range`, `Content-Length` and
///   `Accept-Ranges` readable from script, so they are exposed by name too.
/// * `Location` goes with them. `/proxy` relays the `Location` of a `3xx` it
///   declines to follow (`routes::proxy`), and that header exists to be read
///   -- it is the only thing saying where the origin sent us. Unexposed, the
///   one client shape that reads response headers by name, a browser-hosted
///   one, could not see it, which is the shape the relay was for.
///
/// Methods stay a wildcard: every method this server answers is one of the
/// safelisted ones or is preflighted, and `*` is honoured for methods
/// everywhere.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::ACCEPT,
            header::ACCEPT_ENCODING,
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::RANGE,
        ])
        .expose_headers([
            header::ACCEPT_RANGES,
            header::CONTENT_DISPOSITION,
            header::CONTENT_ENCODING,
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::CONTENT_TYPE,
            header::LOCATION,
        ])
        .max_age(Duration::from_secs(24 * 60 * 60))
}

/// The router the LAN media listener serves (see [`crate::lan_media`]):
/// [`lan_media_routes`] and the two unhandled-request fallbacks, with the
/// same tracing and CORS layers the loopback router carries.
///
/// [`control_router`] is deliberately absent -- not merged and left behind the
/// bearer middleware, but *not mounted at all*. A control path on this
/// listener is an unknown path: it answers `404`, never `401`, so the LAN
/// cannot even learn which control routes exist, let alone reach settings,
/// downloads, stats or the torrent session with a guessed or leaked token.
///
/// Two-segment paths that are not media -- `/{infoHash}/create`,
/// `/cache/clean`, `/proxy/x`, `/ftp/x`, `/rar/create` -- collide with the
/// `/{infoHash}/{fileIdx}` pattern and are answered by that route: `405`
/// for a method it does not take, and for a `GET` or `HEAD` the LAN stream
/// handler's `404`, because it looks the first segment up as an info hash
/// among the torrents that exist and creates nothing (see
/// [`routes::stream::EngineAccess`]). Nothing about the collision needs
/// shadowing: there is no doomed magnet add to pre-empt when the handler
/// cannot start one.
///
/// This is deliberately *not* [`media_router`] minus a couple of routes --
/// see [`lan_media_routes`] for why.
fn build_lan_media_router(state: AppState) -> Router {
    let lan_media = state.lan_media.clone();
    Router::new()
        .merge(lan_media_routes())
        .fallback(fallback_handler)
        .method_not_allowed_fallback(method_not_allowed_handler)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "lan-request",
                        method = %request.method(),
                        path = request.uri().path(),
                    )
                })
                // A span alone is not an event: it decorates the lines a
                // handler writes and emits none of its own, so a receiver
                // that fetches and a receiver that never connects looked
                // exactly alike in the log -- a listener that starts, runs
                // and stops with nothing in between. This is the line that
                // tells the two apart, and the peer is the one field that
                // says whether the address we handed out was reachable.
                .on_request(move |request: &axum::extract::Request, _: &tracing::Span| {
                    lan_media.record_request();
                    tracing::info!(
                        method = %request.method(),
                        path = %request.uri().path(),
                        peer = %peer_from_request(request)
                            .map_or_else(|| "unknown".to_string(), |peer| peer.to_string()),
                        "LAN media request"
                    );
                }),
        )
        .layer(cors_layer())
        .with_state(state)
}

/// The two byte-serving routes a player fetches directly: a torrent file's
/// bytes, plain or under the `/stream` alias stremio-core also builds. On
/// loopback the first request for a hash is also what creates its torrent,
/// with the request's `tr=` trackers.
fn stream_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/stream/{infoHash}/{fileIdx}",
            get(routes::stream::stream_video).head(routes::stream::head_stream_video),
        )
        .route(
            "/{infoHash}/{fileIdx}",
            get(routes::stream::stream_video).head(routes::stream::head_stream_video),
        )
}

/// [`stream_routes`] as the LAN media listener mounts them: the same two
/// paths, over torrents this server already has, and nothing else. An
/// unknown hash is a `404` and `tr=` is ignored, so a LAN peer can fetch
/// what this device is playing but cannot make it start anything -- see
/// [`routes::stream::EngineAccess`].
fn lan_stream_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/stream/{infoHash}/{fileIdx}",
            get(routes::stream::lan_stream_video).head(routes::stream::lan_head_stream_video),
        )
        .route(
            "/{infoHash}/{fileIdx}",
            get(routes::stream::lan_stream_video).head(routes::stream::lan_head_stream_video),
        )
}

/// The archive formats' prefixes, each mounting `router` -- one handler set
/// serves RAR/ZIP/7Z/TAR/TGZ alike.
fn archive_prefixes(router: impl Fn() -> Router<AppState>) -> Router<AppState> {
    Router::new()
        .nest("/rar", router())
        .nest("/zip", router())
        .nest("/7zip", router())
        .nest("/tar", router())
        .nest("/tgz", router())
}

/// The whole archive API: a session created from an archive named by URL or
/// torrent file (`routes::archive::session_router`) and the members read out
/// of it (`routes::archive::stream_router`).
fn archive_routes() -> Router<AppState> {
    archive_prefixes(routes::archive::router)
}

/// The byte-serving half of [`archive_routes`] alone: members of a session
/// the loopback listener already created.
fn archive_stream_routes() -> Router<AppState> {
    archive_prefixes(routes::archive::stream_router)
}

/// The whole NZB API: a session created from an NZB and the news servers the
/// caller names (`routes::nzb::session_router`) and the files read out of it
/// (`routes::nzb::stream_router`).
fn nzb_routes() -> Router<AppState> {
    Router::new().nest("/nzb", routes::nzb::router())
}

/// The byte-serving half of [`nzb_routes`] alone.
fn nzb_stream_routes() -> Router<AppState> {
    Router::new().nest("/nzb", routes::nzb::stream_router())
}

/// The `/local-addon` stub (see `routes::local_addon`): not media bytes, but
/// harmless and open for the same reason it lives in [`media_router`] at all
/// -- default profiles, legacy clients included, call it as an addon, and it
/// exposes nothing.
fn local_addon_routes() -> Router<AppState> {
    Router::new().nest("/local-addon", routes::local_addon::router())
}

/// Routes that hand media bytes to a player. They are OPEN (no bearer token):
/// players fetch the URLs stremio-core builds for them
/// (types/resource/stream.rs) and cannot attach headers. The one non-media
/// exception is the `/local-addon` stub: default profiles -- legacy clients
/// included -- call it as an addon, and it exposes nothing.
///
/// `/proxy` and `/ftp` are also here, and are also open for the same
/// header-less-caller reason -- but neither serves media bytes *from this
/// server*: both fetch an arbitrary caller-supplied remote URL (`/proxy` over
/// HTTP(S), `/ftp` over HTTP(S) or via a spawned `curl` for FTP/FTPS) and
/// stream back whatever answers. That makes each an open proxy, which is
/// fine on the loopback listener -- only this host's own stremio-core can
/// reach it -- but not on the LAN one. The archive and NZB `/create` routes
/// and the torrent-creating first request of [`stream_routes`] are the same
/// kind of thing in a smaller way. See [`lan_media_routes`], which is the
/// allow-list that keeps all of them off the LAN.
fn media_router() -> Router<AppState> {
    Router::new()
        .merge(stream_routes())
        .merge(archive_routes())
        .merge(nzb_routes())
        .merge(routes::proxy::router())
        .nest("/ftp", routes::ftp::router())
        .merge(local_addon_routes())
}

/// The LAN media listener's route allow-list (see [`crate::lan_media`]):
/// exactly what a cast receiver needs, which is the bytes of something this
/// device is already playing, and nothing else.
///
/// The test of a route belonging here is that it serves bytes the loopback
/// side has already arranged and cannot be made to arrange anything: the
/// receiver is an unauthenticated stranger on the network, so whatever it
/// can reach, anyone on the network can. That rules out every route that
/// fetches a caller-named URL (`/proxy`, `/ftp`, the archive and NZB
/// `/create`s, which download an archive or open TCP connections to news
/// servers the caller names), every route that starts a torrent (the
/// loopback [`stream_routes`], whose first request for a hash creates it
/// with the caller's trackers -- the LAN gets [`lan_stream_routes`], which
/// only look one up), and the `/local-addon` stub, which no receiver calls.
/// What is left is byte-serving over sessions and torrents that exist:
/// [`lan_stream_routes`], [`archive_stream_routes`], [`nzb_stream_routes`].
///
/// Deliberately spelled as *what is safe*, not as [`media_router`] minus the
/// hazardous routes: a plain `media_router() - proxy - ftp` reads correctly
/// today, but it means a route added to [`media_router`] for some other
/// reason is on the LAN by default, and an author who never touches this
/// function has no reason to notice. A new group must be added *here* by
/// name before the LAN listener serves it -- the silent default is
/// exclusion, not inclusion -- and it must pass the test above. Keep it this
/// way; see the `AGENTS.md` "Routes" entry for the same rule stated for
/// `media_router` vs. `control_router`.
fn lan_media_routes() -> Router<AppState> {
    Router::new()
        .merge(lan_stream_routes())
        .merge(archive_stream_routes())
        .merge(nzb_stream_routes())
}

/// Everything that is not media bytes: what stremio-core's StreamingServer
/// model calls through `Env::fetch`, plus the app/test status routes. Every
/// route here requires `Authorization: Bearer <token>` (see `auth`); a new
/// route goes here unless it serves media bytes to a player.
fn control_router() -> Router<AppState> {
    Router::new()
        .route("/heartbeat", get(routes::system::heartbeat))
        .route("/stats.json", get(routes::system::get_stats))
        .route("/network-info", get(routes::system::network_info))
        .route("/device-info", get(routes::system::device_info))
        .route(
            "/settings",
            get(routes::system::get_settings).post(routes::system::set_settings),
        )
        .route("/create", post(routes::engine::create_engine))
        .route("/{infoHash}/create", post(routes::engine::create_magnet))
        .route(
            "/{infoHash}/stats.json",
            get(routes::system::get_engine_stats),
        )
        .route(
            "/{infoHash}/{idx}/stats.json",
            get(routes::system::get_file_stats),
        )
        .route("/get-https", get(routes::system::get_https))
        .route("/downloads.json", get(routes::downloads::get_downloads))
        .route(
            "/{infoHash}/{fileIdx}/download",
            post(routes::downloads::post_download).delete(routes::downloads::delete_download),
        )
        .route(
            "/proxy-streams/{token}/close",
            post(routes::proxy::close_proxy_streams),
        )
        .route("/cache.json", get(routes::cache::get_cache_usage))
        .route("/cache/clean", post(routes::cache::post_clean_cache))
        .nest("/casting", routes::casting::router())
}

#[cfg(test)]
mod default_log_filter_tests {
    use super::DEFAULT_LOG_FILTER;

    /// Every crate whose diagnostics a field report depends on must be in
    /// the default directives: an unlisted crate is filtered out entirely,
    /// with nothing to say it happened. `librqbit` is the one that reports
    /// a failed piece write, which is how a full or unwritable cache
    /// volume becomes visible at all.
    #[test]
    fn the_default_directives_cover_every_crate_that_reports_trouble() {
        for crate_name in ["server", "stream_server", "enginefs", "librqbit"] {
            assert!(
                DEFAULT_LOG_FILTER
                    .split(',')
                    .any(|directive| directive.starts_with(&format!("{crate_name}="))),
                "{crate_name} is missing from {DEFAULT_LOG_FILTER}"
            );
        }
        // Parses as directives at all.
        tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER);
    }

    /// The two chronically-noisy librqbit targets are silenced at WARN while
    /// the rest of `librqbit` keeps it -- the piece-write failure that makes
    /// a full cache volume visible is a `librqbit::` WARN and must survive.
    ///
    /// Asserted through a real subscriber rather than by reading the string,
    /// because it depends on `EnvFilter` preferring the more specific
    /// directive (`librqbit_dht::dht=error`) over the prefix one
    /// (`librqbit=warn`) for the same event -- which is the whole mechanism.
    #[test]
    fn the_repeating_dht_and_upnp_warnings_are_filtered_out_but_librqbit_warn_is_not() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default)]
        struct Collect(Arc<Mutex<Vec<String>>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Collect {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0
                    .lock()
                    .unwrap()
                    .push(event.metadata().target().to_string());
            }
        }

        let collected = Collect::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER))
            .with(collected.clone());
        tracing::subscriber::with_default(subscriber, || {
            // The two the field log drowned in.
            tracing::warn!(target: "librqbit_dht::dht", "error in bootstrap");
            tracing::warn!(target: "librqbit_upnp", "failed to run SSDP/UPNP discovery");
            // Must still get through.
            tracing::error!(target: "librqbit_dht::dht", "dht: error in get_peers_root()");
            tracing::warn!(target: "librqbit_dht::persistence", "cannot deserialize routing table");
            tracing::warn!(target: "librqbit::torrent_state", "error writing piece");
            tracing::info!(target: "stream_server::lib", "listening");
        });

        let targets = collected.0.lock().unwrap().clone();
        assert_eq!(
            targets,
            vec![
                "librqbit_dht::dht",
                "librqbit_dht::persistence",
                "librqbit::torrent_state",
                "stream_server::lib",
            ]
        );
    }
}
