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
pub use enginefs::piece_store::PinSet;
#[doc(hidden)]
pub use enginefs::pretend_volume_space;
pub use enginefs::{PIN_FREE_SPACE_MARGIN, PinDownloadError, UnpinOutcome};
use futures_util::future::BoxFuture;
pub use routes::downloads::DownloadInfo;
// What `ServerHandle::open_drive_file` answers. Named here because `routes`
// is private, so a type only reachable through it is one an embedder can
// call the method but not write down the result of -- and `DriveError`
// with them, because `DriveOpenError::Drive` carries one and an embedder
// that cannot name it cannot construct or match the case that matters.
pub use routes::drive::{DriveFileOpened, DriveOpenError};
#[doc(hidden)]
pub use routes::stream::{pretend_available_space, pretend_available_space_readings};
pub use routes::system::{FileNotFound, ServerSettings, resolved_path};
pub use sources::drive::DriveError;
pub use state::AppState;
use std::{
    future::IntoFuture,
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
/// application code lives in the `stream_server` lib crate, so its targets
/// are `stream_server::*` and that is the name that must be listed here --
/// the `server` directive beside it was the deleted `server` binary's, and
/// a rename of the lib would silently filter out every log line the same
/// way. `RUST_LOG` only overrides this when it is set to a non-empty
/// value; an unset or blank `RUST_LOG` keeps this.
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
pub(crate) const DEFAULT_LOG_FILTER: &str = "stream_server=info,tower_http=info,\
     enginefs=info,librqbit=warn,librqbit_dht::dht=error,librqbit_upnp=error";

/// The port stremio-core's default profile builds `streaming_server_url`
/// from (`http://127.0.0.1:11470/`), which is why it is what
/// [`ServerConfig::default`] binds: a client that has never been told
/// otherwise looks here.
///
/// **An embedder that reads the bound address back should ask for `0`
/// instead**, and xtremio does. Core's URL is retargeted at whatever this
/// server actually bound the moment it is known, so the number buys nothing
/// downstream -- while binding it can lose to a desktop Stremio, to a second
/// instance of the same app, or to whatever else holds 11470. The constant
/// stays because it is the client-facing contract, not because anything
/// here needs a fixed port.
pub const DEFAULT_HTTP_PORT: u16 = 11470;

/// The directory an **older build** kept its archive extractions in,
/// `<cacheRoot>/.archives`. Nothing writes it now: a member of a container
/// is ranges of the container (`crate::translators`), so there is nothing
/// to extract and nothing to download whole. The name survives for one
/// purpose, in `run`: deleting what such a build left behind.
const LEGACY_ARCHIVE_SCRATCH_DIR: &str = ".archives";

mod auth;
mod cache_budget;
mod cache_cleaner;
mod devices;
mod diagnostics;
pub mod images;
// What an embedder needs to apply this switch itself. xtremio starts this
// server with `init_logging: false` and owns the process's one subscriber,
// so [`diagnostics::logging::set_diagnostics_trace`] has no filter to
// reload there: the setting is persisted and nothing in the log changes.
// Exporting the builder and the two targets is what lets that embedder
// build the same filter this crate would have built, from the same
// strings, rather than keeping a second copy of them that drifts.
pub use diagnostics::logging::{PROXY_TRACE_TARGET, RETENTION_TRACE_TARGET, log_filter};
mod lan_media;
mod proxy_cache;
mod proxy_retention;
mod proxy_streams;
mod routes;
/// **The seam every translated container reads through**: a file whose
/// bytes something else fetched and keeps -- a torrent's piece store, the
/// proxy cache -- asked for by byte range and never copied anywhere. See
/// the module's own documentation and `docs/translated-sources.md`.
///
/// Public, and `doc(hidden)`, so the integration tests can build the
/// sources and the counting wrapper the index bounds are stated in. It is
/// not part of the embeddable API.
#[doc(hidden)]
pub mod sources;
mod state;
pub mod stream_numbers;
/// What a container says about the bytes inside it: the translators (see
/// the module's own documentation and `docs/translated-sources.md`).
///
/// Public, and `doc(hidden)`, for the same reason [`sources`] is: the
/// integration tests state what indexing a real archive may read, and say
/// it in the same types the routes do. Not part of the embeddable API.
#[doc(hidden)]
pub mod translators;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    /// Settings, logs and certificates. `None`
    /// uses the platform config dir (needs `HOME`/`XDG_*`); embedders must
    /// set it explicitly.
    pub config_dir: Option<PathBuf>,
    /// Torrent downloads, session/DHT state and archive caches. `None`
    /// means `config_dir/cache` when `config_dir` is set, otherwise the
    /// platform cache dir. No environment variable is consulted once
    /// `config_dir` is given.
    pub cache_dir: Option<PathBuf>,
    pub init_logging: bool,
    /// Whether this server installs the process-wide crash machinery: the
    /// panic hook that writes a panic to the log files, and the native
    /// crash handler that catches a SIGSEGV/SIGABRT the Rust hook never
    /// sees (`diagnostics::logging`). Off by default, because they are
    /// *process* globals and the host process may have its own -- but they
    /// are the only crash-dump machinery in this tree, so an embedder
    /// whose host installs none should turn this on to get a post-mortem
    /// out of a torrent or codec crash instead of a silent kill.
    pub manage_process_globals: bool,
    pub graceful_shutdown_timeout: Duration,
    /// How the control API authenticates (media routes are always open).
    /// Defaults to a per-launch generated token; see [`ServerAuth`].
    pub auth: ServerAuth,
    /// What the embedder says is pinned for offline: info hash to the file
    /// indices it wants kept. Handed in rather than read from a file,
    /// because the one client that pins already keeps that list as the
    /// downloads the user asked for, and two records of one fact are two
    /// records that can disagree -- see `enginefs::piece_store::pin_record`.
    ///
    /// **`None` is "nobody told me", and it is not an empty set.** The
    /// launch sweep deletes everything the set does not claim, before the
    /// session opens, so silence has to mean *keep everything*: nothing is
    /// swept, every restored torrent is kept and reported as pinned, and
    /// nothing is deleted for want of a claim. An embedder whose own record
    /// would not read passes `None` and warns its user; passing an empty
    /// map instead says "the user has pinned nothing", which deletes their
    /// downloads.
    pub pins: Option<enginefs::piece_store::PinSet>,
    /// The port librqbit's incoming BitTorrent listener binds.
    /// [`TorrentListenPort::Ephemeral`] by default, so any number of
    /// embedded servers (and the tests) coexist; an embedder that needs a
    /// fixed, forwardable port sets [`TorrentListenPort::Fixed`] itself.
    pub torrent_listen_port: TorrentListenPort,
    /// Where the LAN media listener binds when it runs: a second HTTP
    /// listener serving [`lan_media_routes`] and nothing else, so a
    /// Chromecast or other receiver on the local network can fetch the bytes
    /// of what this device is playing while the control API -- and every
    /// route that creates or fetches anything -- stays on the loopback
    /// listener only (see [`crate::lan_media`]).
    ///
    /// `None` -- the default -- means there is no LAN listener at all and
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
    /// `enginefs::backend::dht_bootstrap`). `true` by default: the Android
    /// embed is exactly the case this exists for, since that is where the
    /// system resolver was observed returning nothing.
    ///
    /// `false` does no DNS and no HTTP at start-up, leaving the names for
    /// librqbit to resolve itself. **Tests set this**, so `cargo test` makes
    /// no DNS query and no DoH request of its own.
    pub resolve_dht_bootstrap_names: bool,
    /// Whether a torrent is added with the built-in and fetched public
    /// tracker lists, or with nothing but the trackers a request named.
    ///
    /// The other half of [`ServerConfig::resolve_dht_bootstrap_names`]:
    /// together they are what "makes no outbound request of its own" means,
    /// and neither is enough alone. Defaults to `true`; tests set it false.
    pub use_public_trackers: bool,
    /// Whether a torrent announces itself to the local network by BEP-14
    /// multicast, or is found only by the ways a request named.
    ///
    /// The third of the same family, and the one nobody thought to switch
    /// off: with the other two false a test still shouted every info hash
    /// it held at the network it was running on, and -- because a fixture
    /// built from deterministic bytes has a deterministic info hash --
    /// **two concurrent test runs found each other and swapped pieces**.
    /// The symptom was a store holding more than the test's own seeder had
    /// ever uploaded, which is a measurement no assertion can save.
    ///
    /// This is the **default** for `btEnableLsd` rather than a veto over
    /// it: a viewer who has asked for local discovery keeps it, and the
    /// settings report goes on describing what the session actually does.
    /// Defaults to `true`; tests set it false.
    pub enable_local_service_discovery: bool,
    /// Where this embedder's Google Drive pairing service is: the `POST`
    /// that turns a refresh token into an access token, because the OAuth
    /// client secret lives there and never on the device (see
    /// `routes::drive` and `sources::drive`).
    ///
    /// `None` -- the default -- means `POST /drive/create` refuses with
    /// `noPairingService` and nothing else changes. This repository ships
    /// no such service and must not invent one: a wrong endpoint is a
    /// refresh token posted to somebody else's host.
    pub drive_refresh_endpoint: Option<url::Url>,
    /// Where Drive itself is, for the pairing service above. `None` is
    /// Google (`sources::drive::GOOGLE_DRIVE_API`), which is every shipped
    /// build; a test points it at a loopback fake.
    ///
    /// `#[doc(hidden)]`-in-spirit and deliberately not a per-request
    /// parameter: a caller who could name this origin would have the
    /// server fetch an arbitrary host under a credential it renews, and
    /// cache the answer under the vouch `DriveSource` makes. See
    /// `routes::drive::DriveEndpoints`.
    pub drive_api_base: Option<url::Url>,
}

/// The one configuration: a server inside a host process. There used to be a
/// second, `binary_default()`, for the standalone daemon -- all interfaces,
/// HTTPS, SSDP, a Ctrl+C handler -- and it went with the daemon.
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_HTTP_PORT)),
            pins: None,
            config_dir: None,
            cache_dir: None,
            init_logging: false,
            manage_process_globals: false,
            graceful_shutdown_timeout: Duration::from_secs(3),
            auth: ServerAuth::Generated,
            torrent_listen_port: TorrentListenPort::Ephemeral,
            lan_media_addr: None,
            resolve_dht_bootstrap_names: true,
            use_public_trackers: true,
            enable_local_service_discovery: true,
            drive_refresh_endpoint: None,
            drive_api_base: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSource {
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

    /// This server's torrent engine, and the runtime it runs on, for a
    /// test that has to reach a `crate::sources::TorrentFileSource` rather
    /// than go through a route.
    ///
    /// `#[doc(hidden)]` and no part of the embeddable API: an embedder
    /// drives torrents through the methods below. The runtime comes with
    /// the engine because the two cannot be used apart -- librqbit's
    /// session spawns on the runtime it was started on, and a reader
    /// polled anywhere else is a `Handle::current` away from a panic.
    #[doc(hidden)]
    pub fn engine_for_tests(&self) -> (Arc<enginefs::EngineFS>, tokio::runtime::Handle) {
        (self.state.engine.clone(), self.runtime.clone())
    }

    pub fn bound_http_addr(&self) -> SocketAddr {
        self.bound_http_addr
    }

    /// The bearer token the control routes require this launch
    /// (`Authorization: Bearer <token>`). A started server always has one:
    /// the `Option` is only what an `AppState` no `run` filled in carries,
    /// and `ServerAuth::Disabled` -- which used to make it `None` and leave
    /// the control routes open -- is gone.
    pub fn auth_token(&self) -> Option<&str> {
        self.state.auth_token.as_deref()
    }

    /// The URL the server advertises (`settings.baseUrl`):
    /// `http://<connectable bound address>`. There used to be a
    /// `public_base_url` override for the daemon, which set it to the
    /// address it was binding anyway; without it this cannot disagree with
    /// the listener.
    pub fn base_url(&self) -> &str {
        &self.state.base_url
    }

    /// Current settings -- the `values` of `GET /settings`.
    pub fn settings(&self) -> anyhow::Result<ServerSettings> {
        let state = self.state.clone();
        self.block_on_server(async move { state.settings.read().await.clone() })
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

    /// **Tell the server how long the film is**, without saying where the
    /// player is in it -- which is all a cast can say, the receiver doing
    /// the reading and reporting only seconds, and which is all anything
    /// says now: where a viewer is, is where its reads are. The length is
    /// the one fact about a viewing this server cannot work out for itself,
    /// and it is what sizes the retention window and the stream's own
    /// read-ahead, both of which are stated in seconds of film; see
    /// [`enginefs::EngineFS::on_duration`].
    ///
    /// `file_idx` is the `{fileIdx}` segment of the player's URL as the
    /// core wrote it, `-1` included, and `filters` its `f=` values: an
    /// addon that names no file leaves the choice to this server, so the
    /// file the length is about is the one the stream route resolved it
    /// to, by the same rule (`routes::compat::resolve_file_idx`, through
    /// [`stream_numbers::StreamFile`]). Reported against the addon's index
    /// instead, a length for such a stream never arrived at all -- and
    /// those are most streams.
    pub async fn note_duration(
        &self,
        info_hash: &str,
        file_idx: &str,
        filters: &[String],
        duration: std::time::Duration,
    ) {
        let Some(file) = crate::stream_numbers::StreamFile::parse(file_idx, || filters.to_vec())
        else {
            return;
        };
        let Some(file_idx) = file.resolve(&self.state.engine, info_hash).await else {
            return;
        };
        self.state
            .engine
            .on_duration(info_hash, file_idx, duration)
            .await;
    }

    /// **Tell the server a player opened on this torrent**, so what it goes
    /// on to say about buffering is counted for this video and not the
    /// last; see [`enginefs::EngineFS::on_player_opened`]. Any player, any
    /// file of the torrent: the count is the torrent's.
    pub async fn note_player_opened(&self, info_hash: &str) {
        self.state.engine.on_player_opened(info_hash).await;
    }

    /// **Tell the server the player is buffering after having played** --
    /// its popup is up and the frames have stopped. The one thing a player
    /// knows that this server cannot see; each report has the backend
    /// split one more piece ahead of the reader for the rest of this video.
    /// See [`enginefs::EngineFS::on_player_stalled`].
    pub async fn note_player_stalled(&self, info_hash: &str) {
        self.state.engine.on_player_stalled(info_hash).await;
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

    /// What this server holds of the stream a player is playing, exactly
    /// what `GET /stream-numbers.json?url=...` answers (see
    /// `crate::stream_numbers`): the cache around the playhead, and for a
    /// torrent the set committed for sharing and what the session has
    /// moved.
    ///
    /// `url` is the URL handed to the player, and its shape is what decides
    /// which store answers -- a torrent stream, a proxied one, or neither,
    /// which is `None` and means a stream this server is not holding rather
    /// than an error. Every number is measured when it is asked for and
    /// nothing is kept: the transfer totals are **this session's**, and the
    /// ratio taken from them must be labelled as one.
    ///
    /// It creates nothing and does not count as a poll, so it cannot hold a
    /// torrent out of the idle sweep; it is not free, though, since the
    /// window is counted from a listing of the stream's own directories on
    /// the blocking pool. Ask it while a panel is open, not for the life of
    /// the process.
    pub fn stream_numbers(
        &self,
        url: &str,
    ) -> anyhow::Result<Option<stream_numbers::StreamNumbers>> {
        let state = self.state.clone();
        let url = url.to_string();
        self.block_on_server(
            async move { routes::stream_numbers::stream_numbers(&state, &url).await },
        )
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
    /// [`Self::engine_stats`]) when new, kept wanted and exempt from
    /// eviction, persisted across restarts. The torrent does not move --
    /// pinning is retention, not a location. Fails with
    /// [`PinDownloadError`] -- `InsufficientSpace` below
    /// [`PIN_FREE_SPACE_MARGIN`], `FileNotFound` for a bad index.
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
    /// file while other pins hold, the piece-store directory for a pin whose
    /// torrent the backend does not have, and a `file_idx`
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

    /// What the torrent backend calls `file_idx` of `info_hash` (the `path`
    /// of its [`Self::downloads`] entry). `None` when the torrent is not
    /// managed right now or the backend does not know the path yet; never
    /// creates an engine.
    ///
    /// A name, not a file: torrent data is stored one file per piece
    /// (`enginefs::piece_store`), so nothing is written at this path and
    /// handing it to a local player would hand it a path that does not
    /// exist. A finished download plays through the media routes like any
    /// other. Nothing above the backend chooses the folder in it either.
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

    /// Open a file in a paired Google Drive and hand back a URL a player
    /// can fetch -- exactly what `POST /drive/create` answers (see
    /// `routes::drive::open_file`, which the route shares), with the
    /// stream path made absolute on this server.
    ///
    /// **This is how an embedder asks, and why the credential never
    /// becomes a request.** The refresh token is an argument to a function
    /// call inside this process: it is not a query string, not a path
    /// segment, not a header, and not a line anything logs. What comes
    /// back is a URL carrying a random key, which is the whole of what the
    /// player is told.
    ///
    /// A dead pairing is [`routes::drive::DriveOpenError::is_pair_again`]
    /// -- terminal, and the one thing a caller must be able to tell
    /// without reading English, because the answer to it is a new QR and
    /// the answer to everything else is to try again.
    pub fn open_drive_file(
        &self,
        file_id: &str,
        refresh_token: &str,
        name: Option<String>,
    ) -> anyhow::Result<Result<routes::drive::DriveFileOpened, routes::drive::DriveOpenError>> {
        let state = self.state.clone();
        let base = self.state.base_url.trim_end_matches('/').to_string();
        let file_id = file_id.to_string();
        let refresh_token = refresh_token.to_string();
        self.block_on_server(async move {
            routes::drive::open_file(&state, &file_id, &refresh_token, name)
                .await
                .map(|opened| routes::drive::DriveFileOpened {
                    url: format!("{base}{}", opened.url),
                    ..opened
                })
        })
    }

    /// What the cache currently occupies against its configured limit, in
    /// the one occupancy accounting this repository has
    /// (`enginefs::chunk_store::occupied_bytes` -- allocated blocks, not
    /// apparent length), exactly what `GET /cache.json` answers (see
    /// `routes::cache::cache_usage`). [`CacheUsage::protected_bytes`] and
    /// `protected_files` are what a pin or the stream being played is
    /// holding right now, so a caller can tell "over the limit but nothing
    /// is disposable" apart from "a clean would help" without running one.
    ///
    /// Walks nothing: the owners of the cache count what they hold as they
    /// write it, and the only filesystem work is one `read_dir` of the
    /// store root for what no live store speaks for and a `stat` of each
    /// staged copy a live store is writing. Cheap enough for a
    /// "Storage" screen open or a manual refresh; it is not cached here, so
    /// do not poll it on a sub-second timer.
    pub fn cache_usage(&self) -> anyhow::Result<CacheUsage> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::cache::cache_usage(&state).await })
    }

    /// Give back everything nobody is playing and nobody is reading, now,
    /// and report what is left -- exactly what `POST /cache/clean` answers
    /// (see `routes::cache::clean_cache_now`). A pinned download's files
    /// and the window of the stream being played are never touched, however
    /// far over the limit the cache is; [`EvictionReport::shortfall_message`]
    /// is the line to show the user when that leaves it still over: nothing
    /// can reclaim what a pin or a live window keeps, and the fix is to stop
    /// the stream or unpin the download, not to run the clean again.
    pub fn clean_cache_now(&self) -> anyhow::Result<EvictionReport> {
        let state = self.state.clone();
        self.block_on_server(async move { routes::cache::clean_cache_now(&state).await })?
    }

    /// The cap the budget publisher last stated, as the owners of the cache
    /// read it (`cache_budget::publish_now`), or `None` while nothing caps
    /// it at all.
    ///
    /// **The one place the published cap is observable from outside.** It
    /// is not [`Self::cache_usage`]'s `limit_bytes`: that is the same
    /// arithmetic run again for the answer a client reads, while this is
    /// the number the retention policies of both owners are sized from --
    /// so a publisher that stopped reading what the cache holds would size
    /// every window on a volume's free space alone and nothing a client
    /// asked would say so. Here for the tests that pin it, and cheap: a
    /// read of one cell.
    pub fn published_cache_budget(&self) -> Option<u64> {
        match self.state.engine.cache_budget().get() {
            enginefs::retention::CacheBudget::Bytes(bytes) => Some(bytes),
            enginefs::retention::CacheBudget::Unknown
            | enginefs::retention::CacheBudget::Unbounded => None,
        }
    }

    /// How many open bodies the proxy cache is answering right now
    /// (`proxy_retention::ProxyRetention::reads`) -- an entity one of them
    /// is inside is live, so nothing takes its bytes while it stands.
    ///
    /// Not [`Self::proxy_streams_live`], which counts the reads a client can
    /// close by token: that registration goes the moment a body ends, while
    /// the read itself lives on until hyper drops the response -- so a pass
    /// in between is still answered "somebody is inside all of this". That
    /// gap is nanoseconds on an idle machine and milliseconds on a loaded
    /// one, which makes it exactly the kind of thing a test has to be able
    /// to wait for rather than hope past.
    pub fn proxy_cache_reads(&self) -> usize {
        self.state.proxy_cache.retention().reads()
    }

    /// The cap the proxy cache's retention owner is enforcing right now, or
    /// `None` for a cache nothing bounds
    /// (`enginefs::retention::owner::Retention::cap`).
    ///
    /// **Not [`Self::cache_usage`]'s `limit_bytes`.** That one is the same
    /// arithmetic (`cache_budget::CacheLimit::effective`) run again over a
    /// fresh reading of the volume, so it says what a publication *would*
    /// state; this is the number a pass will actually measure against,
    /// read out of the cell every publication writes. A test whose whole
    /// premise is that a stream ran past its budget has to assert the
    /// second one: the first is true on a roomy volume whether or not
    /// anything ever reached the owner.
    pub fn proxy_cache_cap(&self) -> Option<u64> {
        self.state.proxy_cache.retention().cap()
    }

    /// Wait until the proxy cache has nothing left on the blocking pool: no
    /// chunk on its way to the disk and no retention pass on its way round
    /// it (`proxy_cache::DiskWork`).
    ///
    /// **This is here for what watches the cache from outside**, which is to
    /// say the tests, and it is the answer to a question they could not
    /// otherwise ask. A chunk is written from a task nobody joins, so the
    /// last chunks of a body land after the player has read the last byte of
    /// it and in whatever order the pool ran them in; the pass that reclaims
    /// round the final playhead lands after that again. A test that counts
    /// the files in the cache root without this is counting a directory that
    /// is still moving, and it will read the same policy as two different
    /// answers depending on how loaded the machine is.
    ///
    /// It is a wait on a condition and not on a clock: once a body has been
    /// read to its end, nothing can start work this count has not already
    /// seen, so a count of nothing is a disk that has stopped. `within`
    /// bounds it so a regression fails instead of hanging.
    pub fn proxy_cache_settled(&self, within: std::time::Duration) -> anyhow::Result<()> {
        let state = self.state.clone();
        self.block_on_server(async move {
            tokio::time::timeout(within, state.proxy_cache.settled()).await
        })?
        .map_err(|_| anyhow::anyhow!("the proxy cache was still writing after {within:?}"))
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

    /// Ask the reconciler what it wants of `info_hash` *as the timer would
    /// ask it*, now, and answer the ladder's verdict --
    /// `EngineFS::reconcile_hash` with `Trigger::Timer`. `None` when this
    /// server holds no such torrent.
    ///
    /// Doc-hidden, not part of the embedder API. It is here because the
    /// reconciler's decision is otherwise unobservable from outside, and a
    /// test that wants to assert one had to *sleep past a tick and look at
    /// the torrent afterwards* -- which is a weak oracle for a `Run` and no
    /// oracle at all for a `Stop`. A negative read off the swarm flag
    /// cannot tell "the ladder said stop" from "the ladder said run and
    /// the actuator's `RECONCILE_MIN_DWELL` swallowed the start", and the
    /// dwell is fifteen seconds -- longer than any sleep a test should be
    /// spending. The verdict returned here is taken before the actuator
    /// runs, so it is the policy under test rather than its shadow.
    ///
    /// It is the timer's question and not a playback's on purpose: the two
    /// are measured against different free-space lines
    /// (`reconcile::line`), and the hysteresis only exists on the timer's
    /// side. Asking costs one volume probe (`probed` starts false, so the
    /// declaration a test made with `pretend_volume_space` is read fresh)
    /// and takes that torrent's reconcile lock, so a driven pass and the
    /// background loop cannot interleave.
    ///
    /// **It acts, like any other pass.** This is not a dry run: the
    /// decision is applied through the same actuator the tick uses. On a
    /// torrent already in the state the verdict asks for -- which is what
    /// these assertions are about -- that is no call at all.
    #[doc(hidden)]
    pub fn reconcile_as_timer(
        &self,
        info_hash: &str,
    ) -> anyhow::Result<Option<enginefs::reconcile::Verdict>> {
        let engine = self.state.engine.clone();
        let info_hash = info_hash.to_lowercase();
        self.block_on_server(async move {
            engine
                .reconcile_hash(&info_hash, enginefs::reconcile::Trigger::Timer)
                .await
        })
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
            // The `lanMediaEnabled` veto is `start`'s to check, under the
            // lock a revocation's stop takes.
            state.lan_media.start(&state).await.map(Some)
        })?
    }

    /// The address the LAN media listener is bound to, or `None` when it is
    /// not running. With a configured port of `0` this is the port the OS
    /// assigned.
    ///
    /// Cheap: no runtime hop, and never waits for a start or a stop.
    pub fn lan_media_addr(&self) -> Option<SocketAddr> {
        self.state.lan_media.bound_addr()
    }

    /// Whether the LAN media listener is running right now. Cheap, like
    /// [`Self::lan_media_addr`].
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
    let base_url = format!("http://{}", public_http_addr);

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
    if cfg.init_logging && !diagnostics::logging::log_files_installed() {
        let log_writers = diagnostics::logging::open_log_writers(&log_dir)?;
        let human_log_path = log_writers.human_path.clone();
        let json_log_path = log_writers.json_path.clone();
        let human_writer = log_writers.human_writer;
        let json_writer = log_writers.json_writer;
        let guards = log_writers.guards;

        // `RUST_LOG` or the default, with the retention trace off until the
        // settings say otherwise (`diagnosticsTrace`, applied below once
        // they are loaded); the handle is what lets the setting change a
        // filter that is already installed.
        let log_directives = std::env::var("RUST_LOG")
            .ok()
            .filter(|directives| !directives.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_string());
        let (log_filter, log_filter_handle) = tracing_subscriber::reload::Layer::new(
            diagnostics::logging::log_filter(&log_directives, false),
        );
        let registry = tracing_subscriber::registry().with(log_filter);
        let human_file_layer = tracing_subscriber::fmt::layer()
            .with_writer(human_writer)
            .with_ansi(false);
        let json_file_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(json_writer)
            .with_ansi(false);

        let stdout_layer = std::io::stdout()
            .is_terminal()
            .then(tracing_subscriber::fmt::layer);
        let init_result = registry
            .with(human_file_layer)
            .with(json_file_layer)
            .with(stdout_layer)
            .try_init();

        if init_result.is_ok() {
            diagnostics::logging::store_log_guards(guards);
            diagnostics::logging::store_log_filter(log_directives, log_filter_handle);
            startup_log_paths = Some((human_log_path, json_log_path));
        }
    }

    tracing::info!("Config Dir: {:?}", config_dir);
    tracing::info!(
        "Configured cache dir (the default torrent-data root): {:?}",
        cache_dir
    );
    tracing::info!("Log Dir: {:?}", log_dir);
    if cfg.manage_process_globals {
        diagnostics::logging::install_native_crash_handler(&log_dir);
    }
    if let Some((human_log_path, json_log_path)) = startup_log_paths {
        diagnostics::logging::log_startup_context(
            &config_dir,
            &cache_dir,
            &log_dir,
            &human_log_path,
            &json_log_path,
        );
    }

    let default_settings = routes::system::ServerSettings {
        cache_root: cache_dir.to_string_lossy().to_string(),
        // The *default*, not an override: a viewer who has asked for local
        // discovery keeps it, and what the settings report says is what
        // the session does. An embedder that must not announce -- a test --
        // starts on a config directory of its own, so the default is the
        // whole story there.
        bt_enable_lsd: cfg.enable_local_service_discovery,
        ..routes::system::ServerSettings::default()
    };

    // One `SettingsFile` for the process: the tracker refresher writes
    // through it from here on, and `AppState` takes the same one below, so
    // the two never race for the file (see `state::SettingsFile`).
    let settings_file = Arc::new(state::SettingsFile::new(config_dir.join("settings.json")));
    let mut settings = settings_file.load(&default_settings);

    // The one torrent-data root, prepared before anything is opened on it:
    // the piece store, the session's own records and the proxy cache all
    // live under it, and it is the whole of what a usage figure counts. A
    // persisted `cacheRoot` that cannot be used any more (unmounted drive,
    // permissions) falls back to the configured default rather than
    // stopping the server, and the fallback is what gets persisted below --
    // so the setting never claims a root the data is not in.
    let torrent_data_root =
        match routes::system::prepare_torrent_data_root(&settings.cache_root).await {
            Ok(root) => root,
            Err(error) => {
                tracing::warn!(
                    cache_root = %settings.cache_root,
                    error = %format!("{error:#}"),
                    "cacheRoot is unusable; falling back to the configured cache directory"
                );
                routes::system::prepare_torrent_data_root(&cache_dir.to_string_lossy()).await?
            }
        };
    tracing::info!(
        torrent_data_root = %torrent_data_root.display(),
        "torrent data root (settings.cacheRoot)"
    );
    let corrected_cache_root = settings.cache_root != torrent_data_root.to_string_lossy();
    if corrected_cache_root {
        settings.cache_root = torrent_data_root.to_string_lossy().into_owned();
    }

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
        public_trackers: if cfg.use_public_trackers {
            enginefs::backend::PublicTrackers::Use
        } else {
            enginefs::backend::PublicTrackers::Off
        },
    };

    // One engine, opened on the one torrent-data root. `AppState` used to
    // carry two fields, `engine` and `download_engine`, from a design that
    // meant to pair a memory-only stream engine with a disk-backed download
    // engine; librqbit sessions always persist to disk and no memory-only
    // storage was ever built, so `EngineFS::new_disk_backed` was
    // `new_with_storage` under another name and the same `Arc` went in both
    // fields -- every reader read one instance twice and skipped the
    // duplicate with `Arc::ptr_eq`. (There was also a retry through
    // `new_with_storage` when `new_disk_backed` failed, described as falling
    // back to memory-only mode: the same constructor, failing the same way.
    // A failure here is a failure.)
    let engine = Arc::new(
        EngineFS::new_disk_backed(
            torrent_data_root.clone(),
            backend_config,
            Some(tracker_storage),
            cfg.pins.clone(),
        )
        .await?,
    );

    let mut state = AppState::new_with_shared_settings_and_log_dir(
        engine,
        settings_arc.clone(),
        config_dir.clone(),
        log_dir.clone(),
    );
    state.settings_file = settings_file;
    state.base_url = base_url.clone();
    state.http_addr = public_http_addr;
    state.auth_token = Some(Arc::from(cfg.auth.resolve()?));
    state.lan_media = Arc::new(lan_media::LanMedia::new(cfg.lan_media_addr));
    // Where a Drive file's two services are, fixed at the server rather
    // than named per request -- see `routes::drive::DriveEndpoints`. A
    // build whose embedder named no pairing service keeps `None` and
    // refuses a create; nothing else about it changes.
    state.drive = cfg.drive_refresh_endpoint.clone().map(|refresh| {
        Arc::new(match cfg.drive_api_base.clone() {
            Some(api_base) => routes::drive::DriveEndpoints::against(refresh, api_base),
            None => routes::drive::DriveEndpoints::at(refresh),
        })
    });
    // The token is a secret and must never reach `tracing`: the log files
    // would keep it, and the kept launches' archives with them. An embedder
    // reads `ServerHandle::auth_token` instead. The deleted daemon printed it
    // to stdout; nothing in a host process reads stdout.
    tracing::info!("control API requires `Authorization: Bearer <token>`");

    let (seeding_enabled, diagnostics_trace) = {
        let settings = settings_arc.read().await;
        (settings.seeding_enabled, settings.diagnostics_trace)
    };
    // The filter was installed with the trace off, which is also what a
    // fresh process means by `false`; only `true` has anything to apply.
    if diagnostics_trace {
        diagnostics::logging::set_diagnostics_trace(true);
    }
    // The engine's flag starts on and the session opens uploading, so with
    // sharing off in the settings nothing stops the uploads until this
    // line. It stores the flag and applies the upload switch, and nothing
    // else: no torrent is reconciled here. The settings guard is gone by
    // then, taken only for the read above.
    state.engine.set_seeding_enabled(seeding_enabled).await;
    // The root the session was actually opened on, persisted, so
    // `GET /settings`, the settings file and the next boot all name the
    // directory the data is in. Only a value that was already something else
    // is written -- an unusable one that was replaced by the default, or a
    // spelling that resolved to another (a symlinked prefix, a Windows 8.3
    // name).
    if corrected_cache_root && let Err(error) = state.save_settings().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "could not persist the torrent-data root the session opened on"
        );
    }

    // The proxy cache's own launch-time sweep, before the router can serve a
    // request that writes into it. It empties the root: a proxied entity is
    // kept while something is playing it and disposable the moment anything
    // else opens, and a process that has served nothing is playing nothing --
    // so every chunk a previous run left is a byte no owner here would ever
    // count or reclaim. See `proxy_cache::sweep`.
    //
    // Not part of the engine's boot ordering, and it does not need to be:
    // the piece store's sweep has to precede the session because opening it
    // seeds each store's held set off the disk, and a piece deleted after
    // that stays counted as held for the life of the process. This root is
    // the other cache, the session knows nothing of it, and the only thing
    // in this process that ever writes here is a `/proxy` response -- so
    // "before the router serves" is the whole of the requirement, and it is
    // also what makes `ProxyRetention::occupancy` -- what *this* process
    // wrote -- equal to what is on the disk from the first byte.
    {
        let root = state.proxy_cache.root().to_path_buf();
        if let Err(error) = tokio::task::spawn_blocking(move || proxy_cache::sweep(&root)).await {
            tracing::warn!(%error, "the proxy cache sweep did not finish");
        }
    }

    // And what an **older build** left under `<cacheRoot>/.archives`: the
    // extraction cache the archive layer used to keep, which this server
    // does not write any more (see `crate::translators` -- a member is
    // ranges of the container, and nothing is extracted). On an install
    // upgraded into this build the directory can be gigabytes, about twice
    // the size of every archive played since that build's last clean exit
    // -- the download and the extraction -- and **nothing else will ever
    // take it**: no retention owner speaks for those bytes, no cache
    // figure counts them, and the piece store's own legacy sweep walks
    // `<cacheRoot>/rqbit-downloads`, one level below this. So it goes
    // here, once, on the first launch of a build that has no use for it.
    {
        let stale = torrent_data_root.join(LEGACY_ARCHIVE_SCRATCH_DIR);
        if let Err(error) =
            tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&stale) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            })
            .await
        {
            tracing::warn!(%error, "the sweep of a previous build's archive scratch did not finish");
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
    // taken, not read.
    background_tasks.extend(state.engine.take_tracker_refresh_task());
    // And the engine's housekeeping sweep, the other forever loop it starts
    // for itself. It is parked on a 15-second sleep between passes rather
    // than an hourly one, so of the two it is the likelier to be holding a
    // timer when the driver goes down.
    background_tasks.extend(state.engine.take_sweep_task());
    // And the reconciler, which recomputes what every torrent should be
    // doing and makes it so -- stopping one before it runs the volume out,
    // starting it again when there is room. One, for the one engine (a
    // second would only race the first for the same torrents).
    background_tasks.push(state.engine.start_reconciler());
    // And the switch task: the moment a viewer opens something else, what
    // they left is disposable, and this is what takes it off the disk
    // without waiting for the next tick. See `drop_slack_on_switch_and_bell`
    // for what it listens to and why each owner answers which signal.
    background_tasks.push({
        let engine = state.engine.clone();
        let proxy_cache = state.proxy_cache.clone();
        let changed = engine.live().changed();
        let bell = engine.slack_bell().clone();
        let torrents = {
            let engine = engine.clone();
            move || {
                let engine = engine.clone();
                Box::pin(async move {
                    engine.drop_slack().await;
                }) as BoxFuture<'static, ()>
            }
        };
        let proxied = move || {
            let proxy_cache = proxy_cache.clone();
            Box::pin(async move {
                proxy_cache.retention().drop_slack().await;
            }) as BoxFuture<'static, ()>
        };
        // And the translated containers, which is where this server's
        // sessions get the rule the map itself does not hold: a session
        // stays while its own container is what is playing, and a lease
        // out on one outranks that (`translators::session`).
        // And the Drive files, which are the same kind of session over one
        // file instead of a container and take the same rule: the file
        // that is playing stays, a body reading one outranks the rule, and
        // a session that goes takes its credential with it.
        let sessions = {
            let translated = state.translated_archives.clone();
            let drive = state.drive_files.clone();
            move |reading: &enginefs::retention::live::Reading| {
                translated.retain(|session| session.is_live(reading));
                drive.retain(|session| session.is_live(reading));
            }
        };
        tokio::spawn(drop_slack_on_switch_and_bell(
            changed, bell, torrents, proxied, sessions,
        ))
    });
    // And the cache budget, which the cache cleaner used to state on its
    // way out of a walk. Unconditional like the DHT health check: a process
    // that has published no budget holds no retention policy over a relayed
    // stream at all. See `cache_budget`.
    // Awaited, not merely spawned: `start` states the budget before it
    // returns, so it exists before the router below can serve a request
    // into the proxy cache rather than a moment after.
    background_tasks.push(cache_budget::start(Arc::new(state.clone())).await);
    // Unconditional and unconfigurable: two routing-table length reads on a
    // timer, and the only thing that ever states whether the DHT works here.
    // See `diagnostics::dht_health`.
    background_tasks.push(diagnostics::dht_health::start(state.engine.clone()));

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

    tracing::info!("listening on {}", bound_http_addr);
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(Started {
            bound_http_addr,
            state,
            runtime: tokio::runtime::Handle::current(),
        });
    }

    let (shutdown_started_tx, mut shutdown_started_rx) =
        tokio::sync::oneshot::channel::<ShutdownSource>();
    let shutdown = async move {
        external_shutdown_rx.recv().await;
        tracing::info!("Shutdown signal received from external controller, shutting down");
        let _ = shutdown_started_tx.send(ShutdownSource::External);
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
                    // Never `std::process::exit` here. The deleted daemon
                    // did, and it stayed behind after the daemon went: this
                    // server is one thread of a host process with its own
                    // work and its own exit, and a slow shutdown must cost
                    // that process a dropped future, not its life.
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

    Ok(shutdown_source)
}

/// What a viewer opening something else costs the thing they left: the
/// slack drops that do not wait for the reconciler's tick, and the
/// translated sessions of containers nobody is in any more.
///
/// **Both owners answer a switch**, because the liveness cell is one cell:
/// a proxied body opening makes a torrent's file slack and a torrent stream
/// opening makes the proxied body slack, and the proxy has no tick of its
/// own -- this is the only thing that ever ends one of its entities.
///
/// **Only the proxy answers the bell.** The torrent side's slack passes
/// ride the same tick that takes the volume reading the bell is rung from
/// (`enginefs::retention::SlackBell`), so running them here as well would
/// be the same pass twice in the same instant; the proxy has no such tick,
/// and a volume filling under a paused player is exactly the case where
/// nothing opens and so nothing switches.
///
/// One task and one `select!`, so the two never run at once: the passes
/// take each entity's turn and would be safe concurrently, but a bell that
/// rings while a switch is mid-pass has nothing to add -- the switch's own
/// pass covers every slack entity there is.
///
/// **The sessions go on a switch and on nothing else.** A translated
/// container's session is the index of bytes the two owners above have just
/// been told are slack, and it has the same life for the same reason
/// (`enginefs::retention::live`, and `translators::session` for what that
/// means for a map that used to hold a ten-minute idle clock). It is handed
/// the reading the switch landed on rather than asking the cell again:
/// every consumer of one switch answers the same reading, as everywhere
/// else. It is not called on the bell -- a volume running low is a reason
/// to give bytes back, and an index in memory is not bytes on the volume.
///
/// The three are taken as closures rather than as the owners themselves so
/// that this shape can be tested for what it does with each signal, which
/// is the whole of what it is.
///
/// The receiver is made by the caller, and `watch::Sender::subscribe` marks
/// the value it was made on as seen: the first wake-up is the first real
/// switch after that, including one that lands before this task is first
/// polled -- which a `mark_unchanged()` in here would swallow.
async fn drop_slack_on_switch_and_bell(
    mut switched: tokio::sync::watch::Receiver<Option<enginefs::retention::live::LiveEntity>>,
    bell: Arc<enginefs::retention::SlackBell>,
    torrents: impl Fn() -> BoxFuture<'static, ()>,
    proxied: impl Fn() -> BoxFuture<'static, ()>,
    sessions: impl Fn(&enginefs::retention::live::Reading),
) {
    loop {
        tokio::select! {
            moved = switched.changed() => {
                // The cell is gone, which means the engine is: there is
                // nothing left to drop the slack of.
                if moved.is_err() {
                    return;
                }
                // The reading is taken and the borrow dropped before
                // anything is awaited: it holds the cell's own lock, and
                // the writer of the next switch would wait on it.
                let reading = {
                    let value = switched.borrow_and_update();
                    value
                        .clone()
                        .map_or_else(
                            enginefs::retention::live::Reading::nothing,
                            enginefs::retention::live::Reading::of,
                        )
                };
                // First, because it is synchronous and instant and frees
                // memory, while the two below go to the disk.
                sessions(&reading);
                torrents().await;
                proxied().await;
            }
            () = bell.rung() => proxied().await,
        }
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
                    // Not the path as it stands: `/proxy` and `/ftp` carry
                    // the caller's target URL -- and its `h=` headers --
                    // *in the path*, and this span decorates every line
                    // those routes write. See `routes::util::log_path`.
                    path = routes::util::log_path(request.uri().path()),
                )
            }),
        )
        // **No CORS layer on loopback.** Nothing that reads this listener is
        // a browser: the embedder's player (mpv), its HTTP client and
        // stremio-core all ignore CORS, and the Cast receiver -- the one
        // browser media element in the picture -- is on another device and
        // reads the LAN listener, which keeps its layer. What a wildcard
        // `Access-Control-Allow-Origin` did buy here was for everybody
        // else on the device: loopback is not "only this app" -- any page
        // a browser on the device has open, and on Android any app, reaches
        // it -- and the open routes fetch what the caller names. Under `*`
        // a web page could read LAN and intranet URLs through `/proxy`
        // (`?d=http://192.168.1.1/admin`, with headers of its choosing in
        // `h=`), `/ftp`, an archive create and a torrent-creating stream
        // `GET`. Without it the browser still sends the request -- this is
        // not a request barrier -- but the page cannot read what comes
        // back.
        .with_state(state)
}

/// CORS for the LAN media listener, the one a Cast receiver reads.
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
/// * A player that seeks needs `Content-Range`, `Content-Length` and
///   `Accept-Ranges` readable from script, so they are exposed by name too.
///
/// Nothing on that listener takes a bearer header or relays a `Location`,
/// so neither is named: those were for a browser-hosted client of the
/// loopback listener, which no longer answers CORS at all (see
/// [`build_router`]).
///
/// Methods stay a wildcard: every method this server answers is one of the
/// safelisted ones or is preflighted, and `*` is honoured for methods
/// everywhere.
fn lan_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::ACCEPT,
            header::ACCEPT_ENCODING,
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
        ])
        .max_age(Duration::from_secs(24 * 60 * 60))
}

/// The router the LAN media listener serves (see [`crate::lan_media`]):
/// [`lan_media_routes`] and the two unhandled-request fallbacks, with the
/// tracing layer the loopback router carries and the CORS layer it does not
/// ([`lan_cors_layer`]: a Cast receiver is a browser media element).
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
                        path = routes::util::log_path(request.uri().path()),
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
                        path = %routes::util::log_path(request.uri().path()),
                        peer = %peer_from_request(request)
                            .map_or_else(|| "unknown".to_string(), |peer| peer.to_string()),
                        "LAN media request"
                    );
                }),
        )
        .layer(lan_cors_layer())
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

/// The archive formats' prefixes, each mounting `router` for the format it
/// names -- which is what picks the translator (`routes::archive::Format`).
fn archive_prefixes(
    router: impl Fn(routes::archive::Format) -> Router<AppState>,
) -> Router<AppState> {
    use routes::archive::Format;
    Router::new()
        .nest("/rar", router(Format::Rar))
        .nest("/zip", router(Format::Zip))
        .nest("/7zip", router(Format::SevenZ))
        .nest("/tar", router(Format::Tar))
        .nest("/tgz", router(Format::TarGz))
        .nest("/iso", router(Format::Iso))
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
/// HTTP(S), `/ftp` over FTP/FTPS and nothing else) and
/// stream back whatever answers. That makes each an open proxy, which is
/// fine on the loopback listener -- only this host's own stremio-core can
/// reach it -- but not on the LAN one. The archive `/create` routes and the
/// torrent-creating first request of [`stream_routes`] are the same
/// kind of thing in a smaller way. See [`lan_media_routes`], which is the
/// allow-list that keeps all of them off the LAN.
fn media_router() -> Router<AppState> {
    Router::new()
        .merge(stream_routes())
        .merge(archive_routes())
        .merge(routes::proxy::router())
        .nest("/ftp", routes::ftp::router())
        // The byte-serving half of the Drive API, and only that half: the
        // create is a control route because its body carries the account's
        // grant (`routes::drive`). This URL carries a random key and no
        // credential, which is what lets a player fetch it.
        .merge(routes::drive::stream_routes())
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
/// fetches a caller-named URL (`/proxy`, `/ftp`, the archive `/create`s,
/// which download an archive), every route that starts a torrent (the
/// loopback [`stream_routes`], whose first request for a hash creates it
/// with the caller's trackers -- the LAN gets [`lan_stream_routes`], which
/// only look one up), and the `/local-addon` stub, which no receiver calls.
/// What is left is byte-serving over sessions and torrents that exist:
/// [`lan_stream_routes`] and [`archive_stream_routes`].
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
}

/// What `POST /create` may weigh: a hex-encoded `.torrent` in a JSON
/// envelope, so about half of this is the file.
const MAX_CREATE_BODY: usize = 32 * 1024 * 1024;

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
        .route(
            "/create",
            post(routes::engine::create_engine)
                // A `.torrent` arrives here hex-encoded inside a JSON
                // object, so the body is twice the file plus the envelope,
                // and axum's default limit of 2 MB refused every torrent
                // over about a megabyte with a `413` the client could only
                // read as "the server would not take this file". A torrent
                // that large is a very large one -- 1 MB of piece hashes is
                // a 20,000-piece torrent -- and this is a token-protected
                // route, so the limit is the one that stops a runaway body
                // rather than one that judges the file.
                .layer(axum::extract::DefaultBodyLimit::max(MAX_CREATE_BODY)),
        )
        // The create half of the Drive API. **Control, and for the one
        // reason**: its body carries the account's refresh token, which is
        // a live credential to somebody's Drive -- so it wants the bearer
        // like every other control route, and the token stays in a body
        // that nothing logs rather than in a path that everything does.
        // See `routes::drive`.
        .merge(routes::drive::create_routes())
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
        .route(
            "/stream-numbers.json",
            get(routes::stream_numbers::get_stream_numbers),
        )
        .route("/cache.json", get(routes::cache::get_cache_usage))
        .route("/cache/clean", post(routes::cache::post_clean_cache))
        .nest("/casting", routes::casting::router())
}

#[cfg(test)]
mod slack_task_tests {
    use super::drop_slack_on_switch_and_bell;
    use enginefs::retention::SlackBell;
    use enginefs::retention::live::{Live, LiveEntity};
    use futures_util::future::BoxFuture;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Which signal makes which owner give its slack back, and which one
    /// takes the sessions.
    ///
    /// A **switch** is all three: the cell is one cell, so a proxied body
    /// opening makes a torrent's file disposable and a torrent stream
    /// opening makes a proxied body disposable, and either way a container
    /// indexed from what was playing is a container nobody is in. The
    /// **bell** is the proxy's alone -- the torrent's slack passes ride the
    /// same tick that takes the volume reading the bell was rung from, and
    /// running them here as well would be that pass twice in one instant,
    /// while the proxy has no tick at all; and an index in memory is not
    /// bytes on the volume the bell was rung for.
    ///
    /// Asserted on the passes rather than on the owners, because that is
    /// the whole of what this task is: two signals and which call each of
    /// them makes.
    #[tokio::test]
    async fn a_switch_drops_both_owners_slack_and_the_bell_drops_the_proxys() {
        let live = Arc::new(Live::new());
        let bell = Arc::new(SlackBell::default());
        let torrents = Arc::new(AtomicUsize::new(0));
        let proxied = Arc::new(AtomicUsize::new(0));
        let count = |counter: &Arc<AtomicUsize>| {
            let counter = counter.clone();
            move || {
                let counter = counter.clone();
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                }) as BoxFuture<'static, ()>
            }
        };
        // What the sessions side was handed, switch by switch: the
        // reading, so that a task passing a stale or re-read one shows up
        // here rather than as a session taken while its own container was
        // playing.
        let readings: Arc<std::sync::Mutex<Vec<enginefs::retention::live::Reading>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = |readings: &Arc<std::sync::Mutex<Vec<enginefs::retention::live::Reading>>>| {
            let readings = readings.clone();
            move |reading: &enginefs::retention::live::Reading| {
                readings
                    .lock()
                    .expect("no panic held it")
                    .push(reading.clone());
            }
        };
        let task = tokio::spawn(drop_slack_on_switch_and_bell(
            live.changed(),
            bell.clone(),
            count(&torrents),
            count(&proxied),
            record(&readings),
        ));
        let until = |counter: &Arc<AtomicUsize>, want: usize| {
            let counter = counter.clone();
            async move {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while counter.load(Ordering::SeqCst) < want {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("only {} passes ran", counter.load(Ordering::SeqCst)));
            }
        };

        live.open(LiveEntity::Proxy { dir: "/one".into() }, false);
        until(&torrents, 1).await;
        until(&proxied, 1).await;

        assert_eq!(
            readings.lock().expect("no panic held it").as_slice(),
            &[enginefs::retention::live::Reading::of(LiveEntity::Proxy {
                dir: "/one".into()
            })],
            "the sessions side was handed the reading the switch landed on"
        );

        bell.ring();
        until(&proxied, 2).await;
        assert_eq!(
            torrents.load(Ordering::SeqCst),
            1,
            "the torrent side answers the tick the reading came from, not the bell"
        );
        assert_eq!(
            readings.lock().expect("no panic held it").len(),
            1,
            "a volume running low is not a viewer opening something else"
        );

        // And the value as it stands when the task starts is not a change:
        // a task that treated it as one would drop the slack of whatever
        // was playing when the server came up.
        let quiet = tokio::spawn(drop_slack_on_switch_and_bell(
            live.changed(),
            Arc::new(SlackBell::default()),
            count(&torrents),
            count(&proxied),
            record(&readings),
        ));
        tokio::task::yield_now().await;
        assert_eq!(torrents.load(Ordering::SeqCst), 1, "nothing switched");

        task.abort();
        quiet.abort();
    }
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
        for crate_name in ["stream_server", "enginefs", "librqbit"] {
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
