use anyhow::Result;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncSeek};

use priorities::PlaybackIntent;

pub mod librqbit;

pub mod metadata;
pub mod priorities;

pub trait FileStreamTrait: AsyncRead + AsyncSeek + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send + Sync> FileStreamTrait for T {}

#[derive(Debug, Clone)]
pub enum TorrentSource {
    Url(String),
    Bytes(Vec<u8>),
}

/// Where a backend puts a torrent's data and what it wants at first. The
/// default is the backend's own placement (its session root, everything
/// wanted); an offline download (`BackendEngineFS::pin_download`) names a
/// per-torrent folder and the pinned file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentPlacement {
    /// Directory the torrent's files are written to (the torrent's own
    /// folder, not a parent of it); `None` = the backend's default.
    pub output_folder: Option<std::path::PathBuf>,
    /// Initial want-set; `None` = everything.
    pub only_files: Option<Vec<usize>>,
}

#[async_trait::async_trait]
pub trait TorrentBackend: Send + Sync {
    type Handle: TorrentHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle>;

    /// [`Self::add_torrent`] with an explicit [`TorrentPlacement`]. The
    /// default ignores the placement -- the only possible answer for a
    /// backend with one root and no per-file selection -- so callers that
    /// care check [`TorrentHandle::output_folder`] afterwards instead of
    /// assuming.
    async fn add_torrent_placed(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
        _placement: TorrentPlacement,
    ) -> Result<Self::Handle> {
        self.add_torrent(source, trackers).await
    }

    /// Move a managed torrent to `placement` (whose `output_folder` is
    /// required): its files end up under the new folder and the backend
    /// manages it from there, wanting `placement.only_files`, re-checking
    /// whatever data was moved. Returns the handle for the torrent in its
    /// new place; the old handle is dead afterwards. The pin set survives
    /// (it is why the torrent moves). Backends without per-torrent
    /// placement refuse.
    async fn relocate_torrent(
        &self,
        info_hash: &str,
        _placement: TorrentPlacement,
        _trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        anyhow::bail!("backend cannot relocate torrent {info_hash}")
    }

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle>;
    /// Stop managing the torrent, keeping whatever it wrote on disk.
    async fn remove_torrent(&self, info_hash: &str) -> Result<()>;
    /// [`Self::remove_torrent`] that also deletes the torrent's files and
    /// its (then empty) per-torrent folder -- for a torrent whose data is
    /// known to be worthless, like one added only for a pin that was then
    /// refused. The default keeps the files, as for a backend that cannot
    /// tell them apart from other data.
    async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
        self.remove_torrent(info_hash).await
    }
    async fn list_torrents(&self) -> Vec<String>;
    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics;
    /// The backend's view of the DHT (see [`DhtStatus`]). The default is "no
    /// DHT", the right answer for a backend that has none.
    fn dht_status(&self) -> DhtStatus {
        DhtStatus::default()
    }
    fn set_seeding_enabled(&self, _enabled: bool) {}
}

#[async_trait::async_trait]
pub trait TorrentHandle: Send + Sync + Clone {
    fn info_hash(&self) -> String;
    fn name(&self) -> Option<String>;

    async fn stats(&self) -> EngineStats;
    async fn add_trackers(&self, trackers: Vec<String>) -> Result<()>;
    /// Cheap check for whether the torrent has finished downloading its wanted
    /// data. Unlike `stats()`, this must not rebuild the full statistics or walk
    /// every piece -- it is called on the hot stream-start path. Defaults to
    /// `false` (treat as still needing the swarm) for backends that cannot tell.
    async fn is_finished(&self) -> bool {
        false
    }
    /// Whether this handle owns file selection, resume, and idle-pause
    /// lifecycle internally.
    fn manages_playback_lifecycle(&self) -> bool {
        false
    }
    /// Record HLS activity without forcing shared backends to implement their
    /// own lease controller; a native-lifecycle backend overrides this.
    async fn refresh_hls_activity(&self, _file_idx: usize, _source: &'static str) -> Result<()> {
        Ok(())
    }
    /// End HLS activity immediately. A native-lifecycle backend overrides this
    /// to cancel the selected generation and confirm a normal torrent pause.
    async fn end_hls_activity(&self, _file_idx: usize, _reason: &'static str) -> Result<()> {
        Ok(())
    }
    /// Cheap per-file completion check used to avoid probing sparse local files.
    async fn is_file_complete(&self, _file_idx: usize) -> bool {
        false
    }
    /// Resume torrent activity after an idle pause.
    async fn resume_torrent(&self) -> Result<()> {
        Ok(())
    }
    /// Pause torrent activity when no stream is currently using it.
    async fn pause_torrent(&self) -> Result<()> {
        Ok(())
    }
    /// Throttle (or restore) the torrent's upload rate to control seeding
    /// WITHOUT disconnecting peers. Pausing a torrent disconnects every peer,
    /// and after a long idle the swarm cannot be reliably re-acquired (tracker
    /// min-announce-intervals reject the reannounce and the DHT routing table
    /// decays), which stalls the next episode's download indefinitely. Clamping
    /// upload instead stops seeding while keeping the torrent connected, so a
    /// newly-requested file downloads immediately from the existing peers.
    /// `true` = clamp upload to a trickle; `false` = restore unlimited upload.
    async fn set_upload_throttled(&self, _throttled: bool) -> Result<()> {
        Ok(())
    }
    /// Keep a file minimally wanted so it continues downloading in the
    /// background while higher-priority playback windows serve the current read.
    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }
    /// Reconcile wanted files for multi-file torrents. Backends that cannot
    /// apply per-file priorities may leave this as a no-op.
    async fn reconcile_file_priorities(&self, _plan: TorrentFilePriorityPlan) -> Result<()> {
        Ok(())
    }
    /// Keep `file_idx` wanted regardless of playback selection: every later
    /// `prepare_file_for_streaming` / `clear_file_streaming` /
    /// `reconcile_file_priorities` keeps it in the want-set until
    /// `unpin_file`. This is the backend half of an offline download; the
    /// engine layer (`BackendEngineFS::pin_download`) also exempts the torrent
    /// from idle removal and the seeding-disabled pause. Backends that cannot
    /// select files per file (or always want everything) may leave this as a
    /// no-op. Err only for a provably-bad file index.
    async fn pin_file(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }
    /// Undo `pin_file`. Only forgets the pin: the file stays wanted until
    /// the next selection update recomputes the want-set without it, which
    /// the caller drives (`BackendEngineFS::unpin_download` reconciles).
    async fn unpin_file(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }
    /// The file's on-disk path (the torrent's output folder joined with the
    /// file's relative name), for handing a completed download to a local
    /// player. `None` when the backend does not know (no metadata yet, bad
    /// index, or a backend without a per-file path). Reads still go through
    /// `get_file_reader`, which blocks on pieces a sparse file would not.
    async fn file_path(&self, _file_idx: usize) -> Option<std::path::PathBuf> {
        None
    }
    /// The directory the torrent's files are written to (what
    /// [`TorrentPlacement::output_folder`] resolved to, or the backend's
    /// default). `None` when the backend does not know.
    fn output_folder(&self) -> Option<std::path::PathBuf> {
        None
    }
    /// The torrent's piece length -- the unit a read blocks on, so the one
    /// number that makes "this read waited 28 seconds" legible. `None`
    /// until metadata resolves, or for a backend without pieces.
    fn piece_length(&self) -> Option<u64> {
        None
    }
    /// `buffer` is the viewer's read-ahead choice (the `bufferProfile`
    /// setting or the stream request's `buffer=` override); it scales the
    /// playback windows this reader is opened with, never the startup one.
    async fn get_file_reader(
        &self,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
        bitrate: Option<u64>,
        intent: priorities::PlaybackIntent,
        buffer: priorities::BufferProfile,
    ) -> Result<Box<dyn FileStreamTrait>>;
    async fn get_files(&self) -> Vec<BackendFileInfo>;
    async fn file_count(&self) -> usize {
        self.get_files().await.len()
    }
    /// [`Self::file_path`] as a string (lossy for non-UTF-8 names).
    async fn get_file_path(&self, file_idx: usize) -> Option<String> {
        self.file_path(file_idx)
            .await
            .map(|path| path.to_string_lossy().into_owned())
    }
    /// Prepare a file for streaming by setting its priority and waiting for initial pieces.
    /// This should be called BEFORE reading from the file.
    /// Returns Ok(()) when initial pieces are available, or Err on timeout.
    async fn prepare_file_for_streaming(&self, file_idx: usize) -> Result<()>;
    /// Clear streaming state for a file (set priority to 0, clear piece deadlines).
    /// Called when switching to a different file to ensure exclusive downloading.
    async fn clear_file_streaming(&self, file_idx: usize) -> Result<()>;
    /// Wait until the first piece needed for the requested offset is readable.
    async fn wait_for_piece_ready(
        &self,
        file_idx: usize,
        offset: u64,
        timeout: Duration,
        intent: priorities::PlaybackIntent,
        buffer: priorities::BufferProfile,
    ) -> Result<PieceReadiness>;
}

#[derive(Debug, Clone)]
pub struct TorrentFilePriorityPlan {
    pub active_file: Option<usize>,
    pub hot_file: Option<HotFilePriorityPlan>,
    pub generation: u64,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct HotFilePriorityPlan {
    pub file_idx: usize,
    pub start_offset: u64,
    pub priority: u8,
    pub intent: PlaybackIntent,
    pub bitrate_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PieceReadiness {
    pub ready: bool,
    pub piece: i32,
    pub ready_pieces: u32,
    pub target_pieces: u32,
    pub elapsed_ms: u64,
    pub peers: u64,
    pub download_rate: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BackendMemoryDiagnostics {
    pub native_storage_bytes: u64,
    pub native_storage_pieces: u64,
    pub native_total_read_bytes: u64,
    pub native_total_write_bytes: u64,
    pub rust_piece_cache_entries: u64,
    pub rust_piece_cache_bytes: u64,
    pub waiter_keys: u64,
    pub waiter_wakers: u64,
    pub torrents: Vec<TorrentMemoryDiagnostics>,
}

/// What the mainline DHT looks like from this host, as the backend sees it.
///
/// The DHT is a *peer source*, not a requirement: a torrent with working
/// trackers downloads fine without one. On a network that drops the UDP the
/// DHT needs -- carrier-grade NAT, a captive portal, a firewalled mobile APN
/// -- bootstrap simply never completes, and librqbit retries forever. This is
/// the one place that state is observable, so a client can say "DHT
/// unavailable, using trackers only" instead of the server logging a warning
/// per retry for the length of the session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DhtStatus {
    /// Whether a DHT is running at all (`false` when the backend was built
    /// without one -- the hermetic test session, for instance).
    pub enabled: bool,
    /// Nodes in the IPv4 routing table right now.
    pub nodes: u64,
    /// Nodes in the IPv6 routing table right now.
    pub nodes_v6: u64,
    /// Whether either routing table has *ever* been non-empty this session.
    /// Sticky: a table that empties out again (peers aged out, the network
    /// changed) still counts as having bootstrapped once, which is the
    /// difference between "the DHT is idle" and "the DHT never worked here".
    pub ever_bootstrapped: bool,
}

impl DhtStatus {
    /// A DHT that can answer a `get_peers` right now: running, with at least
    /// one node to ask.
    pub fn is_usable(&self) -> bool {
        self.enabled && (self.nodes + self.nodes_v6) > 0
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TorrentMemoryDiagnostics {
    pub info_hash: String,
    pub native_storage_bytes: u64,
    pub native_storage_pieces: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackendFileInfo {
    pub name: String,
    pub length: u64,
}

/// Byte progress of the single piece an open reader is sitting on -- the
/// piece that has to arrive before that reader can advance.
///
/// A piece is the unit that becomes readable, and on a multi-gigabyte
/// torrent it is 8-16 MiB. Whole verified pieces are all the have-bitfield
/// can show, so a player waiting on its first piece could only ever be told
/// 0% or 100%. librqbit counts a piece's 16 KiB chunks as they are written,
/// which is what this turns into bytes a client can render directly --
/// "waiting for the first piece, 6.2 of 16 MB" -- without knowing anything
/// about chunks.
///
/// **The numbers can go backwards.** A chunk counts as downloaded the moment
/// it is written, *not* when it is verified: the hash is only checked once
/// every chunk is in, and a piece that fails is discarded, dropping the
/// count back to zero. [`Self::verified`] is the only field that means
/// "known good" -- a full `downloaded_bytes` on its own means nothing more
/// than "complete enough to be hashed". A client should hold a nearly-full
/// bar where it is until `verified`, and never animate a decrease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InFlightPiece {
    /// The piece's index in the torrent (not in the file).
    pub index: u64,
    /// Bytes of the piece written to disk so far, verified or not. Never
    /// more than [`Self::total_bytes`].
    pub downloaded_bytes: u64,
    /// The piece's real length: `piece_length` for every piece but the
    /// torrent's last, which is short.
    pub total_bytes: u64,
    /// The piece is fully downloaded *and* passed its hash check, i.e. it is
    /// in the have-bitfield and can be served. While this is false the piece
    /// is never ready, however close `downloaded_bytes` is to
    /// `total_bytes`.
    pub verified: bool,
}

impl InFlightPiece {
    /// Build one from librqbit's `PieceChunkProgress` counts.
    ///
    /// Chunks are a fixed `chunk_size` (16 KiB) each **except the last chunk
    /// of the torrent's last piece**, which is short -- so chunks-to-bytes
    /// is not a flat multiplication and the product is clamped to
    /// `piece_bytes`, the piece's real length. A fully downloaded short last
    /// piece therefore reports exactly its own length rather than a rounded-
    /// up one, and no piece can ever report more bytes than it has.
    pub fn from_chunks(
        index: u64,
        downloaded_chunks: u32,
        chunk_size: u64,
        piece_bytes: u64,
        verified: bool,
    ) -> Self {
        Self {
            index,
            downloaded_bytes: (downloaded_chunks as u64)
                .saturating_mul(chunk_size)
                .min(piece_bytes),
            total_bytes: piece_bytes,
            verified,
        }
    }
}

// Stremio-compatible stats structures
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsFile {
    pub name: String,
    pub path: String,
    pub length: u64,
    pub offset: u64,
    pub downloaded: u64,
    /// Progress 0.0 to 1.0 (from C++ file_progress)
    pub progress: f64,
    /// Bytes of this file's initial priority window (the head of the file a
    /// fresh stream fetches first, see
    /// `priorities::librqbit_stream_lookahead_bytes(DirectInitial, ..)`) that are
    /// already verified on disk. Omitted while the torrent has no piece map
    /// (resolving metadata / hash-checking / error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_window_ready_bytes: Option<u64>,
    /// Size of that initial window: `min(startup window, file length)`.
    /// Omitted together with `initial_window_ready_bytes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_window_bytes: Option<u64>,
    /// Byte progress of the piece an open reader on *this* file is sitting
    /// on, see [`InFlightPiece`]. Omitted unless a reader has been opened on
    /// the file and the torrent has a chunk map (live or paused).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_flight_piece: Option<InFlightPiece>,
    /// The file is pinned as an offline download (`TorrentHandle::pin_file`):
    /// kept wanted regardless of playback selection.
    #[serde(default)]
    pub pinned: bool,
    /// Every byte of the file is verified on disk (`downloaded == length`,
    /// from the backend's per-file progress). False while the torrent has no
    /// piece map yet (resolving metadata / hash-checking).
    #[serde(default)]
    pub complete: bool,
}

/// Coarse torrent startup phase for pre-playback progress UIs (the official
/// Stremio UI polls `stats.json` for its loading overlay). Purely additive to
/// the server.js-compatible fields; derived from the backend's torrent state
/// plus, once a stream file is chosen (`EngineStats::focus_stream_file`),
/// that file's initial-window readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum StartupPhase {
    /// No torrent metadata yet (magnet still resolving its info dictionary).
    #[default]
    ResolvingMetadata,
    /// Metadata known; the backend is hash-checking existing data on disk
    /// (librqbit `Initializing`). `checked_bytes`/`check_total_bytes` track it.
    Checking,
    /// Live (or paused) but the focused file's initial priority window is not
    /// fully on disk yet. `initial_window_ready_bytes`/`initial_window_bytes`
    /// track it.
    Buffering,
    /// The focused file's initial window is fully on disk (or the whole
    /// torrent is finished): playback can start without stalling on the head.
    Ready,
    /// The backend put the torrent into an error state; it will not progress
    /// without intervention.
    Error,
}

/// Peer-discovery breakdown straight from the backend's per-torrent peer
/// counters, so a client can tell "nobody found yet" from "found but not
/// connected" while `Buffering`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerDiscovery {
    /// Distinct peer addresses learned from DHT/trackers/PEX so far.
    pub seen: u64,
    /// Known peers waiting for a connection slot.
    pub queued: u64,
    /// Outgoing connections currently being established.
    pub connecting: u64,
    /// Peers with a completed handshake we exchange data with.
    pub live: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct Growler {
    pub flood: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pulse: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSearch {
    pub max: u64,
    pub min: u64,
    pub sources: Vec<String>,
}

impl Default for PeerSearch {
    fn default() -> Self {
        Self {
            max: 200,
            min: 40,
            sources: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct SwarmCap {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_peers: Option<u64>,
}

/// Torrent speed profile settings from frontend (stremio-web)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSpeedProfile {
    /// Hard limit on download speed (bytes/sec)
    pub bt_download_speed_hard_limit: f64,
    /// Soft limit on download speed (bytes/sec)
    pub bt_download_speed_soft_limit: f64,
    /// Handshake timeout (ms)
    pub bt_handshake_timeout: u64,
    /// Maximum connections
    pub bt_max_connections: u64,
    /// Minimum peers for stable
    pub bt_min_peers_for_stable: u64,
    /// Request timeout (ms)
    pub bt_request_timeout: u64,
}

pub const DEFAULT_BT_MAX_CONNECTIONS: u64 = 800;
pub const LEGACY_UNLIMITED_BT_MAX_CONNECTIONS: u64 = 65535;
pub const MAX_EFFECTIVE_BT_CONNECTIONS: u64 = 1200;
pub const MIN_EFFECTIVE_BT_CONNECTIONS: u64 = 80;

impl TorrentSpeedProfile {
    pub fn effective_connection_limits(&self) -> (i32, i32, bool) {
        let requested = self.bt_max_connections;
        let normalized = if requested == 0 || requested >= LEGACY_UNLIMITED_BT_MAX_CONNECTIONS {
            DEFAULT_BT_MAX_CONNECTIONS
        } else {
            requested.clamp(MIN_EFFECTIVE_BT_CONNECTIONS, MAX_EFFECTIVE_BT_CONNECTIONS)
        };

        let per_torrent = (normalized / 4).clamp(40, 200).min(normalized).max(1);

        (
            normalized as i32,
            per_torrent as i32,
            normalized != requested,
        )
    }
}

impl Default for TorrentSpeedProfile {
    fn default() -> Self {
        Self {
            bt_download_speed_hard_limit: 0.0, // 0 = unlimited
            bt_download_speed_soft_limit: 0.0, // 0 = unlimited
            bt_handshake_timeout: 20000,       // 20s - faster failure for dead peers
            bt_max_connections: DEFAULT_BT_MAX_CONNECTIONS,
            bt_min_peers_for_stable: 5, // Lower barrier to entry
            bt_request_timeout: 10000,  // 10s
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum TorrentEncryptionMode {
    #[default]
    Allow,
    Require,
    Disable,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum TorrentProxyType {
    #[default]
    None,
    Socks4,
    Socks5,
    Socks5Password,
    Http,
    HttpPassword,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentPrivacyConfig {
    pub bt_enable_dht: bool,
    pub bt_enable_pex: bool,
    pub bt_enable_lsd: bool,
    pub bt_encryption_mode: TorrentEncryptionMode,
    pub bt_anonymous_mode: bool,
    pub bt_allow_multiple_connections_per_ip: bool,
    pub bt_listen_interfaces: String,
    pub bt_outgoing_interfaces: String,
    pub bt_outgoing_port: u16,
    pub bt_num_outgoing_ports: u16,
    pub bt_proxy_type: TorrentProxyType,
    pub bt_proxy_host: String,
    pub bt_proxy_port: u16,
    pub bt_proxy_username: String,
    pub bt_proxy_password: String,
    pub bt_proxy_hostnames: bool,
    pub bt_proxy_peer_connections: bool,
    pub bt_proxy_tracker_connections: bool,
    pub bt_proxy_send_host_in_connect: bool,
    pub bt_validate_https_trackers: bool,
    pub bt_ssrf_mitigation: bool,
}

impl Default for TorrentPrivacyConfig {
    fn default() -> Self {
        Self {
            bt_enable_dht: true,
            bt_enable_pex: true,
            bt_enable_lsd: true,
            bt_encryption_mode: TorrentEncryptionMode::default(),
            bt_anonymous_mode: false,
            bt_allow_multiple_connections_per_ip: false,
            bt_listen_interfaces: "0.0.0.0:42000-42010,[::]:42000-42010".to_string(),
            bt_outgoing_interfaces: String::new(),
            bt_outgoing_port: 0,
            bt_num_outgoing_ports: 0,
            bt_proxy_type: TorrentProxyType::default(),
            bt_proxy_host: String::new(),
            bt_proxy_port: 0,
            bt_proxy_username: String::new(),
            bt_proxy_password: String::new(),
            bt_proxy_hostnames: true,
            bt_proxy_peer_connections: false,
            bt_proxy_tracker_connections: true,
            bt_proxy_send_host_in_connect: false,
            bt_validate_https_trackers: true,
            bt_ssrf_mitigation: true,
        }
    }
}

/// Ports the standalone binary tries, in order, for librqbit's incoming
/// BitTorrent listener ([`TorrentListenPort::Fixed`] default). Mirrors the
/// pre-9.0.1 librqbit `listen_port_range: 42000..42010` fallback.
pub const DEFAULT_LISTEN_PORT_RANGE: std::ops::Range<u16> = 42000..42010;

/// Which port librqbit's incoming BitTorrent (TCP) listener binds.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TorrentListenPort {
    /// Try each port of the range in order and keep the first that binds;
    /// fail if none does. A stable, forwardable port for a long-running
    /// desktop instance (`ServerConfig::binary_default`).
    Fixed(std::ops::Range<u16>),
    /// Port 0: the OS picks a free port (librqbit reads the bound address
    /// back and announces that port). For embedded servers and tests, so any
    /// number of sessions coexist on one machine
    /// (`ServerConfig::embedded`).
    Ephemeral,
}

impl Default for TorrentListenPort {
    fn default() -> Self {
        Self::Fixed(DEFAULT_LISTEN_PORT_RANGE)
    }
}

impl TorrentListenPort {
    /// The ports to try, in order.
    pub fn candidates(&self) -> Vec<u16> {
        match self {
            Self::Fixed(range) => range.clone().collect(),
            Self::Ephemeral => vec![0],
        }
    }

    /// Whether a UPnP port-forwarding request for this listener is worth
    /// making at all.
    ///
    /// Only for [`Self::Fixed`]. A forwarded mapping is a lease on the
    /// router for one external port number, and it is only worth anything if
    /// the same port comes back next launch -- which is precisely what
    /// `Fixed` means and what the long-running desktop instance
    /// (`ServerConfig::binary_default`) uses. An [`Self::Ephemeral`] listener
    /// binds a different port every launch, so each run asks the router for a
    /// fresh mapping and leaves the previous one to expire on its own; the
    /// embedders that use it (the Android JNI cdylib, tests) get nothing
    /// durable out of it. On Android the request cannot succeed at all --
    /// SSDP discovery is multicast the app sandbox is not permitted to send
    /// ("Operation not permitted (os error 1)" in a real device log), and on
    /// a cellular APN there is no gateway to ask -- while librqbit's
    /// forwarder retries on an interval *forever*, so every failure is a
    /// `librqbit_upnp` WARN for the whole life of the process.
    pub fn wants_upnp_forwarding(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct BackendConfig {
    /// Where librqbit listens for incoming peers; read once at session start.
    pub listen_port: TorrentListenPort,
    pub cache: priorities::EngineCacheConfig,
    pub growler: Growler,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    pub speed_profile: TorrentSpeedProfile,
    pub privacy: TorrentPrivacyConfig,
    /// DHT bootstrap nodes (`host:port`), read once at session start like
    /// `listen_port`. Empty uses the backend's own default set (see
    /// `librqbit::DEFAULT_DHT_BOOTSTRAP_NODES`); the `dhtBootstrapNodes`
    /// server setting REPLACES it entirely when non-empty.
    pub dht_bootstrap_nodes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connections: Option<u64>,
    pub dht: bool,
    pub growler: Growler,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handshake_timeout: Option<u64>,
    pub path: String,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    pub tracker: bool,
    pub r#virtual: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub last_started: String,
    pub num_found: u64,
    pub num_found_uniq: u64,
    pub num_requests: u64,
    pub url: String,
    /// Seeders this tracker reported at the last scrape (BEP-48). Absent
    /// from the JSON when the tracker has not answered -- absent, not zero,
    /// because a swarm with no seeders is a real and different state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seeders: Option<u64>,
    /// Leechers this tracker reported; same rules as `seeders`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leechers: Option<u64>,
    /// Completed downloads this tracker has recorded; same rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStats {
    pub name: String,
    pub info_hash: String,
    pub files: Vec<StatsFile>,
    pub sources: Vec<Source>,
    pub opts: StatsOptions,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub unchoked: u64,
    pub peers: u64,
    pub queued: u64,
    pub unique: u64,
    pub connection_tries: u64,
    pub peer_search_running: bool,
    pub stream_len: u64,
    pub stream_name: String,
    pub stream_progress: f64,
    pub swarm_connections: u64,
    pub swarm_paused: bool,
    /// NOT a swarm-size estimate, despite the name: it is currently just an
    /// alias of `peers` (connected peers), kept because it is part of the
    /// server.js-compatible wire shape stremio-core parses. For "how many of
    /// the peers we are connected to have the whole file", read
    /// [`EngineStats::connected_seeders`].
    pub swarm_size: u64,
    /// Connected peers whose bitfield covers the whole torrent, i.e. peers we
    /// can get any piece from. Not the swarm's seeder count: we do not scrape
    /// trackers, so this only ever counts peers of our own connections.
    #[serde(default)]
    pub connected_seeders: u64,
    /// Seeders in the **whole swarm** as the trackers report them, from our
    /// own BEP-48/BEP-15 scrapes, aggregated with `max` over the trackers
    /// that answered (see `crate::scrape`).
    ///
    /// `None` -- serialized as `null`, never `0` -- whenever we do not know:
    /// a DHT-only torrent with no trackers, a private torrent (never
    /// scraped), trackers that have not answered yet or at all, or a last
    /// success too old to present. A client must be able to tell that apart
    /// from a swarm that really has no seeders.
    ///
    /// Not [`EngineStats::connected_seeders`], which counts peers of our own
    /// connections and is bounded by `peers`.
    #[serde(default)]
    pub swarm_seeders: Option<u64>,
    /// Leechers in the whole swarm; same rules as
    /// [`EngineStats::swarm_seeders`].
    #[serde(default)]
    pub swarm_leechers: Option<u64>,
    /// Age in seconds of the freshest scrape the swarm figures rest on, so a
    /// client can say how current they are. `None` exactly when they are.
    #[serde(default)]
    pub swarm_scrape_age_secs: Option<u64>,
    /// All wanted pieces are downloaded (libtorrent `is_finished`). A finished
    /// torrent is only seeding and can be paused; an unfinished one still needs
    /// the swarm to download data or fetch metadata.
    pub is_finished: bool,
    /// Torrent metadata is available (false for a freshly added magnet that is
    /// still resolving its info dictionary).
    pub has_metadata: bool,
    /// Startup phase, see [`StartupPhase`]. Torrent-level from the backend
    /// (`resolvingMetadata`/`checking`/`error`, or `buffering`/`ready` by
    /// whole-torrent completion), refined per stream file by
    /// [`EngineStats::focus_stream_file`].
    #[serde(default)]
    pub phase: StartupPhase,
    /// Bytes hash-checked so far; `Some` only while `phase == checking`.
    #[serde(default)]
    pub checked_bytes: Option<u64>,
    /// Bytes the hash check covers (torrent total); `Some` only while
    /// `phase == checking`.
    #[serde(default)]
    pub check_total_bytes: Option<u64>,
    /// The focused stream file's `StatsFile::initial_window_ready_bytes`;
    /// `Some` only in `buffering`/`ready` once a stream file is focused.
    #[serde(default)]
    pub initial_window_ready_bytes: Option<u64>,
    /// The focused stream file's `StatsFile::initial_window_bytes`.
    #[serde(default)]
    pub initial_window_bytes: Option<u64>,
    /// The torrent's piece length, `None` until metadata resolves.
    ///
    /// A piece is the unit that becomes readable, and on a multi-gigabyte
    /// torrent it is 8-16 MiB -- bigger than the whole startup window, so
    /// `initial_window_ready_bytes` can only read 0 or "all of it" and a
    /// percentage built from the pair sits at 0% for tens of seconds while
    /// the download runs perfectly. A client should render the wait in
    /// pieces ("waiting for the first piece (16 MiB)"), optionally with an
    /// ETA from `download_speed`, rather than as a stalled percentage.
    #[serde(default)]
    pub piece_length: Option<u64>,
    /// The focused stream file's `StatsFile::in_flight_piece`: sub-piece
    /// progress for the one piece a reader is waiting on, so a client can
    /// draw a bar that moves inside a 16 MiB piece instead of a percentage
    /// stuck at 0. `null` -- never a zeroed object -- whenever we do not
    /// know: no reader open on the file, no metadata yet, or a torrent
    /// without a chunk map (resolving/checking/error). See
    /// [`InFlightPiece`] for why the number can regress.
    #[serde(default)]
    pub in_flight_piece: Option<InFlightPiece>,
    /// Peer-discovery counters, see [`PeerDiscovery`].
    #[serde(default)]
    pub peer_discovery: PeerDiscovery,
    /// Why `phase` is `error`, when anything knows: a magnet whose add
    /// ended without an engine (see [`EngineStats::magnet_add_failed`]),
    /// or a torrent the backend put in an error state -- a broken
    /// download folder, a full disk. Always a client-safe message; the
    /// backend's own error text names server paths and stays in the log
    /// (`librqbit::TORRENT_ERROR_MESSAGE`). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Indices of the files pinned as offline downloads (ascending); the
    /// same files carry `StatsFile::pinned`.
    #[serde(default)]
    pub pinned_files: Vec<usize>,
}

impl EngineStats {
    /// Stats for a magnet whose metadata is still being resolved, i.e. before
    /// the backend has a torrent to report on: `phase` is
    /// `resolvingMetadata`, `has_metadata` is false, there are no files and no
    /// stream, and `sources` lists the trackers the add was started with.
    /// Peer counters are 0 -- librqbit consumes the discovery stream
    /// internally while resolving, so nothing honest can be reported yet.
    pub fn resolving_metadata(info_hash: &str, trackers: &[String]) -> Self {
        Self {
            name: String::new(),
            info_hash: info_hash.to_string(),
            files: Vec::new(),
            sources: trackers
                .iter()
                .map(|url| Source {
                    url: url.clone(),
                    ..Source::default()
                })
                .collect(),
            opts: StatsOptions {
                connections: None,
                dht: true,
                growler: Growler::default(),
                handshake_timeout: None,
                path: String::new(),
                peer_search: PeerSearch::default(),
                swarm_cap: SwarmCap::default(),
                timeout: None,
                tracker: true,
                r#virtual: false,
            },
            download_speed: 0.0,
            upload_speed: 0.0,
            downloaded: 0,
            uploaded: 0,
            unchoked: 0,
            peers: 0,
            queued: 0,
            unique: 0,
            connection_tries: 0,
            peer_search_running: true,
            stream_len: 0,
            stream_name: String::new(),
            stream_progress: 0.0,
            swarm_connections: 0,
            swarm_paused: false,
            swarm_size: 0,
            connected_seeders: 0,
            swarm_seeders: None,
            swarm_leechers: None,
            swarm_scrape_age_secs: None,
            is_finished: false,
            has_metadata: false,
            phase: StartupPhase::ResolvingMetadata,
            checked_bytes: None,
            check_total_bytes: None,
            initial_window_ready_bytes: None,
            initial_window_bytes: None,
            piece_length: None,
            in_flight_piece: None,
            peer_discovery: PeerDiscovery::default(),
            error: None,
            pinned_files: Vec::new(),
        }
    }

    /// Stats for a magnet whose add failed (metadata timeout, backend error)
    /// and has not been retried: the `resolving_metadata` shape with `phase`
    /// `error` and `error` carrying the reason, so a poller can stop waiting.
    pub fn magnet_add_failed(info_hash: &str, trackers: &[String], error: &str) -> Self {
        Self {
            phase: StartupPhase::Error,
            error: Some(error.to_string()),
            ..Self::resolving_metadata(info_hash, trackers)
        }
    }

    /// Refine `phase` for the file the client is about to play: in
    /// `buffering`/`ready` copy that file's initial-window progress and
    /// in-flight piece to the top level and flip the phase on whether the
    /// window is fully on disk.
    /// Other phases (and files without a window, e.g. no piece map) are left
    /// untouched. Out-of-range indices are a no-op.
    pub fn focus_stream_file(&mut self, file_idx: usize) {
        if !matches!(self.phase, StartupPhase::Buffering | StartupPhase::Ready) {
            return;
        }
        let Some(file) = self.files.get(file_idx) else {
            return;
        };
        let (Some(ready), Some(total)) =
            (file.initial_window_ready_bytes, file.initial_window_bytes)
        else {
            return;
        };
        self.initial_window_ready_bytes = Some(ready);
        self.initial_window_bytes = Some(total);
        self.in_flight_piece = file.in_flight_piece;
        self.phase = if ready >= total {
            StartupPhase::Ready
        } else {
            StartupPhase::Buffering
        };
    }
}
