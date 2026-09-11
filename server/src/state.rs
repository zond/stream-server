use crate::routes::system::ServerSettings;
use enginefs::EngineFS;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    /// The one torrent engine: every route and the diagnostics read this. There used to be a second field,
    /// `download_engine`, holding the same `Arc` -- see `run()`.
    pub engine: Arc<EngineFS>,
    pub settings: Arc<RwLock<ServerSettings>>,
    /// `settings.json` on disk and the one way anything writes it (see
    /// [`SettingsFile`]). Shared with the tracker refresher's
    /// [`TrackerStorageBridge`], which is built before this state is.
    pub settings_file: Arc<SettingsFile>,
    pub config_dir: PathBuf,
    pub log_dir: PathBuf,
    pub base_url: String,
    pub http_addr: SocketAddr,
    /// Bearer token the control routes require; `None` leaves them open.
    pub auth_token: Option<Arc<str>>,
    /// Archive sessions, swept when idle (see `crate::archives::sessions`);
    /// a swept session's downloaded archive goes with it.
    pub archive_cache: crate::archives::sessions::Sessions<crate::archives::ArchiveSession>,
    /// NZB sessions, swept when idle (see `crate::archives::sessions`).
    pub nzb_sessions:
        crate::archives::sessions::Sessions<crate::archives::nzb::session::NzbSession>,
    pub devices: Arc<RwLock<Vec<crate::ssdp::Device>>>,
    /// The proxied streams players are reading right now, so a client can
    /// end its own player's (see `crate::proxy_streams`).
    pub proxy_streams: Arc<crate::proxy_streams::ProxyStreams>,
    /// What `/proxy` has already fetched, on disk (see
    /// `crate::proxy_cache`). Rooted inside the engine's `download_dir`, so
    /// it is counted in the same usage figure and capped by the same
    /// published budget -- and every byte of it is disposable the moment a
    /// viewer opens something else, with the two exceptions its retention
    /// owner makes: a chunk under the live stream's window and one an open
    /// body has been framed to deliver and has not yet
    /// (`crate::proxy_retention`).
    pub proxy_cache: Arc<crate::proxy_cache::ProxyCache>,
    /// The optional LAN media listener shared by `run` and `ServerHandle`
    /// (see `crate::lan_media`). Constructed disabled; `run` replaces it with
    /// one carrying `ServerConfig::lan_media_addr`.
    pub lan_media: Arc<crate::lan_media::LanMedia>,
    /// The HTTPS listener `/get-https` starts and answers for (see
    /// `crate::https`). Constructed with no address; `run` replaces it with
    /// one carrying `ServerConfig::https_addr`.
    pub https: Arc<crate::https::HttpsListener>,
    /// The standing reading behind `ServerHandle::background_traffic` (see
    /// `routes::system::background_traffic`). One per server, not per
    /// caller: the verdict is a comparison against the last reading, and
    /// two callers with two of these would each consume windows the other
    /// never sees.
    pub traffic_window: Arc<enginefs::traffic::TrafficWindow>,
}

impl AppState {
    #[allow(unused)]
    pub fn new(engine: Arc<EngineFS>, settings: ServerSettings, config_dir: PathBuf) -> Self {
        let log_dir = config_dir.join("logs");
        Self::new_with_shared_settings_and_log_dir(
            engine,
            Arc::new(RwLock::new(settings)),
            config_dir,
            log_dir,
        )
    }

    #[allow(unused)]
    pub fn new_with_shared_settings(
        engine: Arc<EngineFS>,
        settings: Arc<RwLock<ServerSettings>>,
        config_dir: PathBuf,
    ) -> Self {
        let log_dir = config_dir.join("logs");
        Self::new_with_shared_settings_and_log_dir(engine, settings, config_dir, log_dir)
    }

    pub fn new_with_shared_settings_and_log_dir(
        engine: Arc<EngineFS>,
        settings: Arc<RwLock<ServerSettings>>,
        config_dir: PathBuf,
        log_dir: PathBuf,
    ) -> Self {
        let settings_file = Arc::new(SettingsFile::new(config_dir.join("settings.json")));
        // The published cap, shared and not copied: `/proxy`'s cache and
        // the piece store are two adapters over one chunk store on one
        // volume, and the retention policy over them is sized from one
        // number.
        let proxy_cache = Arc::new(crate::proxy_cache::ProxyCache::new(
            &engine.download_dir,
            engine.cache_budget(),
            engine.live().clone(),
        ));
        let https = Arc::new(crate::https::HttpsListener::new(None, &config_dir));

        Self {
            engine,
            settings,
            settings_file,
            config_dir,
            log_dir,
            base_url: "http://127.0.0.1:11470".to_string(),
            http_addr: SocketAddr::from(([127, 0, 0, 1], 11470)),
            auth_token: None,
            archive_cache: crate::archives::sessions::Sessions::new(
                crate::archives::SESSION_IDLE_TIMEOUT,
            ),
            nzb_sessions: crate::archives::sessions::Sessions::new(
                crate::archives::SESSION_IDLE_TIMEOUT,
            ),
            devices: Arc::new(RwLock::new(Vec::new())),
            proxy_streams: Arc::new(crate::proxy_streams::ProxyStreams::new()),
            proxy_cache,
            lan_media: Arc::new(crate::lan_media::LanMedia::new(None)),
            https,
            traffic_window: Arc::new(enginefs::traffic::TrafficWindow::new()),
        }
    }

    /// Put the live settings on disk -- see [`SettingsFile::save`].
    pub async fn save_settings(&self) -> anyhow::Result<()> {
        self.settings_file.save(&self.settings).await
    }

    /// [`SettingsFile::load`] for the `settings.json` under `config_dir`.
    pub fn load_settings(config_dir: &Path, defaults: &ServerSettings) -> ServerSettings {
        SettingsFile::new(config_dir.join("settings.json")).load(defaults)
    }
}

/// `settings.json` on disk, and the one way anything writes it.
///
/// Two writers used to reach the file on their own: `AppState::save_settings`
/// (every `POST /settings` and `update_settings`) and the tracker
/// refresher's `save_trackers`, each a plain `tokio::fs::write` -- an
/// `open(O_TRUNC)` and a `write` -- with nothing between them. Two of those
/// in flight at once (a settings change during the startup tracker refresh,
/// or `force_refresh`, which writes twice back to back) left the file as one
/// writer's bytes with the other's tail behind them, which does not parse;
/// and a kill between the truncate and the write left it empty. Either way
/// the next launch read nothing it could use, fell through to the defaults
/// with an INFO line, and the next save overwrote the evidence -- every
/// setting reset, including the proxy and anonymity ones, and nobody told.
///
/// So writes go through one place, and that place does two things. It
/// writes to a uniquely named temporary file beside the target and renames
/// it into place, so the file on disk is always a whole serialization --
/// the old one or the new one, never a mixture and never empty. And it holds
/// a
/// mutex from serialization through rename, which the rename alone does
/// not buy: two atomic writers racing can still land the *older*
/// serialization last, and a user's change would be undone by a tracker
/// refresh that had serialized a moment before it. Under the lock the
/// order of serialization is the order on disk.
pub struct SettingsFile {
    path: PathBuf,
    /// Serialises writers -- see the type doc for why the rename is not
    /// enough on its own.
    writer: tokio::sync::Mutex<()>,
}

impl SettingsFile {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            writer: tokio::sync::Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Serialize `settings` as they are now and put them on disk, whole.
    pub async fn save(&self, settings: &RwLock<ServerSettings>) -> anyhow::Result<()> {
        let _writer = self.writer.lock().await;
        let json = serde_json::to_vec_pretty(&*settings.read().await)?;
        write_whole(&self.path, &json).await?;
        tracing::info!("Settings saved to {:?}", self.path);
        Ok(())
    }

    /// The settings on disk, or `defaults` when there is no file.
    ///
    /// A file that is there but will not parse is a different case from a
    /// missing one and is treated as such: the defaults still stand in, but
    /// at WARN rather than INFO, and the file is moved aside to
    /// `settings.json.corrupt-<unix time>` first -- the next save would
    /// otherwise overwrite the only record of what the settings were, and a
    /// user asking why every setting reset would find nothing.
    pub fn load(&self, defaults: &ServerSettings) -> ServerSettings {
        let content = match std::fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("Using default settings");
                return defaults.clone();
            }
            Err(error) => {
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "settings.json could not be read; using default settings"
                );
                return defaults.clone();
            }
        };
        let mut settings = match serde_json::from_str::<ServerSettings>(&content) {
            Ok(settings) => settings,
            Err(error) => {
                let set_aside = self.set_aside_corrupt();
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    set_aside = ?set_aside,
                    "settings.json does not parse; using default settings and keeping the file"
                );
                return defaults.clone();
            }
        };
        tracing::info!("Loaded settings from {:?}", self.path);
        // Respect the file, but a cache_root it does not name is the
        // runtime's to fill in.
        if settings.cache_root.is_empty() {
            settings.cache_root = defaults.cache_root.clone();
        }
        if settings.bt_max_connections == 0
            || settings.bt_max_connections >= enginefs::backend::LEGACY_UNLIMITED_BT_MAX_CONNECTIONS
        {
            tracing::info!(
                previous_bt_max_connections = settings.bt_max_connections,
                normalized_bt_max_connections = enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS,
                "Normalizing legacy torrent connection setting for multi-client stability"
            );
            settings.bt_max_connections = enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS;
        }
        settings
    }

    /// Move an unparsable `settings.json` to `settings.json.corrupt-<unix
    /// time>` so the next save cannot overwrite it; `None` when the move
    /// failed (then the file stays where it is and the next save does).
    fn set_aside_corrupt(&self) -> Option<PathBuf> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_secs())
            .unwrap_or(0);
        let mut set_aside = self.path.clone();
        set_aside
            .as_mut_os_string()
            .push(format!(".corrupt-{stamp}"));
        match std::fs::rename(&self.path, &set_aside) {
            Ok(()) => Some(set_aside),
            Err(error) => {
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "could not set the unparsable settings.json aside"
                );
                None
            }
        }
    }
}

/// Write `bytes` to `path` through a uniquely named temporary file in the
/// same directory and a rename, so a crash leaves the old file intact and
/// concurrent writers never see each other's temporary file.
async fn write_whole(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut tmp = path.to_path_buf();
    tmp.as_mut_os_string().push(format!(
        ".tmp-{}-{}",
        std::process::id(),
        NEXT_TMP.fetch_add(1, Ordering::Relaxed)
    ));
    tokio::fs::write(&tmp, bytes).await?;
    if let Err(error) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(error.into());
    }
    Ok(())
}

/// Wrapper for TrackerStorage that bridges sync trait with async AppState
/// This is created before EngineFS and passed to it for tracker persistence
pub struct TrackerStorageBridge {
    settings: Arc<RwLock<ServerSettings>>,
    /// The same [`SettingsFile`] `AppState` writes through -- one writer
    /// path, or a tracker refresh and a settings change race for the file.
    settings_file: Arc<SettingsFile>,
}

impl TrackerStorageBridge {
    pub fn new(settings: Arc<RwLock<ServerSettings>>, settings_file: Arc<SettingsFile>) -> Self {
        Self {
            settings,
            settings_file,
        }
    }
}

impl enginefs::TrackerStorage for TrackerStorageBridge {
    fn get_cached_trackers(&self) -> Vec<String> {
        // Use blocking_read for sync access from async context
        // This is safe because we're only reading small data
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                // We're in an async context, use block_in_place
                tokio::task::block_in_place(|| {
                    h.block_on(async {
                        let settings = self.settings.read().await;
                        settings.cached_trackers.clone()
                    })
                })
            }
            Err(_) => {
                // Not in async context, shouldn't happen but return empty
                Vec::new()
            }
        }
    }

    fn get_last_updated(&self) -> i64 {
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => tokio::task::block_in_place(|| {
                h.block_on(async {
                    let settings = self.settings.read().await;
                    settings.trackers_last_updated
                })
            }),
            Err(_) => 0,
        }
    }

    fn get_source_url(&self) -> String {
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => tokio::task::block_in_place(|| {
                h.block_on(async {
                    let settings = self.settings.read().await;
                    settings.trackers_source_url.clone()
                })
            }),
            Err(_) => crate::routes::system::default_trackers_url(),
        }
    }

    fn save_trackers(&self, trackers: Vec<String>, timestamp: i64) {
        let settings = self.settings.clone();
        let settings_file = self.settings_file.clone();

        // The trait is sync and this is called from the refresher's task;
        // the update and the save run on the runtime.
        tokio::spawn(async move {
            {
                let mut guard = settings.write().await;
                guard.cached_trackers = trackers;
                guard.trackers_last_updated = timestamp;
            }
            match settings_file.save(&settings).await {
                Ok(()) => tracing::debug!("Saved cached trackers to settings"),
                Err(error) => {
                    tracing::error!("Failed to save settings after tracker update: {error:#}")
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults(dir: &Path) -> ServerSettings {
        ServerSettings {
            cache_root: dir.join("cache").to_string_lossy().into_owned(),
            ..ServerSettings::default()
        }
    }

    /// Many writers, through both paths the process has -- a settings
    /// change and a tracker refresh -- while a reader keeps opening the
    /// file: every read is a whole serialization, never one writer's bytes
    /// with another's behind them and never an empty file, and the file at
    /// the end is the settings as they finally are, not an older state that
    /// happened to land last. Nothing of the writing is left beside it.
    ///
    /// The tracker list is made large on purpose: a multi-megabyte write
    /// through `open(O_TRUNC)` + `write` is open to a reader for as long as
    /// it takes, which is what made the old shape observable here at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_leave_a_whole_and_current_settings_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = Arc::new(SettingsFile::new(dir.path().join("settings.json")));
        let settings = Arc::new(RwLock::new(ServerSettings {
            cached_trackers: (0..50_000)
                .map(|i| format!("udp://tracker-{i}.invalid:6969/announce"))
                .collect(),
            ..defaults(dir.path())
        }));
        file.save(&settings).await.unwrap();

        let writing = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let reader = {
            let path = file.path().to_path_buf();
            let writing = writing.clone();
            tokio::task::spawn_blocking(move || {
                let mut reads = 0u32;
                while writing.load(Ordering::Relaxed) {
                    let bytes = std::fs::read(&path).expect("the file is always there");
                    serde_json::from_slice::<ServerSettings>(&bytes).unwrap_or_else(|error| {
                        panic!(
                            "read {reads} saw a partial settings.json ({} bytes): {error}",
                            bytes.len()
                        )
                    });
                    reads += 1;
                }
                reads
            })
        };

        let mut writers = Vec::new();
        for round in 0..40u32 {
            let file = file.clone();
            let settings = settings.clone();
            writers.push(tokio::spawn(async move {
                if round % 2 == 0 {
                    // A settings change: mutate under the lock, then save,
                    // the shape `update_settings` has.
                    settings.write().await.bt_max_connections = 100 + u64::from(round);
                } else {
                    // A tracker refresh: the stamp moves.
                    settings.write().await.trackers_last_updated = i64::from(round);
                }
                file.save(&settings).await.unwrap();
            }));
        }
        for writer in writers {
            writer.await.unwrap();
        }
        writing.store(false, Ordering::Relaxed);
        let reads = reader.await.unwrap();
        assert!(
            reads > 0,
            "the reader never got a look in, so it proved nothing"
        );

        let on_disk: ServerSettings =
            serde_json::from_str(&std::fs::read_to_string(file.path()).unwrap()).unwrap();
        let live = settings.read().await.clone();
        assert_eq!(
            serde_json::to_value(&on_disk).unwrap(),
            serde_json::to_value(&live).unwrap(),
            "the last write on disk is the last state, not an older one that landed late"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name != "settings.json")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary files left behind: {leftovers:?}"
        );
    }

    /// A file that will not parse is set aside under a name the next save
    /// will not touch, and the defaults stand in; a file that is not there
    /// is the ordinary first launch and sets nothing aside.
    #[tokio::test]
    async fn an_unparsable_settings_file_is_set_aside_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let file = SettingsFile::new(dir.path().join("settings.json"));
        let defaults = defaults(dir.path());

        assert_eq!(
            serde_json::to_value(file.load(&defaults)).unwrap(),
            serde_json::to_value(&defaults).unwrap(),
            "no file: the defaults"
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        // What a kill between truncate and write, or two interleaved
        // writers, used to leave behind.
        let garbage = r#"{"cacheSize": 0, "btMaxCo"#;
        std::fs::write(file.path(), garbage).unwrap();
        let loaded = file.load(&defaults);
        assert_eq!(
            serde_json::to_value(&loaded).unwrap(),
            serde_json::to_value(&defaults).unwrap(),
            "unparsable: the defaults"
        );
        assert!(
            !file.path().exists(),
            "the corrupt file is no longer where the next save lands"
        );
        let set_aside: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(set_aside.len(), 1, "{set_aside:?}");
        let name = set_aside[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.starts_with("settings.json.corrupt-"), "{name}");
        assert_eq!(
            std::fs::read_to_string(&set_aside[0]).unwrap(),
            garbage,
            "kept byte for byte"
        );

        // And the next save writes a fresh file beside it, not over it.
        let settings = RwLock::new(loaded);
        file.save(&settings).await.unwrap();
        assert!(file.path().exists());
        assert!(set_aside[0].exists());
    }
}
