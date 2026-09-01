use crate::backend::{
    BackendFileInfo, BackendMemoryDiagnostics, EngineStats, FileStreamTrait, Growler, PeerSearch,
    PieceReadiness, StatsFile, StatsOptions, SwarmCap, TorrentBackend, TorrentHandle,
    TorrentSource,
};
use anyhow::{Context, Result};
use librqbit::{ManagedTorrent, Session};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub struct LibrqbitBackend {
    pub session: Arc<Session>,
    download_dir: PathBuf,
}

impl LibrqbitBackend {
    pub async fn new(download_dir: PathBuf) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        debug!(path = ?download_dir, "Storing downloads");

        let session_opts = librqbit::SessionOptions {
            listen_port_range: Some(42000..42010),
            enable_upnp_port_forwarding: true,
            persistence: Some(librqbit::SessionPersistenceConfig::Json {
                folder: Some(download_dir.clone()),
            }),
            peer_opts: Some(librqbit::PeerConnectionOptions {
                connect_timeout: Some(Duration::from_secs(10)),
                read_write_timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            }),
            ..Default::default()
        };

        let session = Session::new_with_opts(download_dir.clone(), session_opts).await?;
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
                download_dir,
            },
            restored_handles,
        ))
    }

    /// Hermetic constructor for tests: no listen port, no DHT, no persistence,
    /// no UPnP — never binds the production 42000-42010 range or touches the
    /// network. Not compiled into release builds.
    #[cfg(test)]
    pub async fn new_for_tests(download_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&download_dir).await?;
        let session_opts = librqbit::SessionOptions {
            disable_dht: true,
            disable_dht_persistence: true,
            listen_port_range: None,
            enable_upnp_port_forwarding: false,
            persistence: None,
            ..Default::default()
        };
        let session = Session::new_with_opts(download_dir.clone(), session_opts).await?;
        Ok(Self {
            session,
            download_dir,
        })
    }
}

pub struct LibrqbitHandle {
    pub handle: Arc<ManagedTorrent>,
    pub info_hash: String,
    /// Kept so the handle can apply per-file selection via
    /// `Session::update_only_files` (librqbit persists file selection on the
    /// session, not the torrent handle).
    session: Arc<Session>,
}

#[async_trait::async_trait]
impl TorrentBackend for LibrqbitBackend {
    type Handle = LibrqbitHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let add_torrent = match source {
            TorrentSource::Url(url) => librqbit::AddTorrent::Url(url.into()),
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

        let info_hash = handle.info_hash().as_string();
        Ok(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
        })
    }

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash).ok()?;
        let handle = self.session.get(id)?;
        let info_hash = handle.info_hash().as_string();
        Some(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
        })
    }

    async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        // delete_files=false keeps downloaded data on disk, matching the
        // libtorrent backend's remove_torrent(handle, false).
        self.session
            .delete(id, false)
            .await
            .with_context(|| format!("failed to remove torrent {info_hash}"))?;
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

    async fn list_torrents(&self) -> Vec<String> {
        self.session.with_torrents(|iter| {
            iter.map(|(_id, handle)| handle.info_hash().as_string())
                .collect()
        })
    }

    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
        BackendMemoryDiagnostics::default()
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
            .and_then(|m| m.info.name.as_ref().map(|n| n.to_string()))
    }

    async fn stats(&self) -> EngineStats {
        let stats = self.handle.stats();
        let (download_speed, upload_speed) = if let Some(ref live) = stats.live {
            (
                live.download_speed.mbps * 1_048_576.0 / 8.0,
                live.upload_speed.mbps * 1_048_576.0 / 8.0,
            )
        } else {
            (0.0, 0.0)
        };

        let (downloaded, uploaded) = if let Some(ref live) = stats.live {
            (live.snapshot.fetched_bytes, live.snapshot.uploaded_bytes)
        } else {
            (0, 0)
        };

        let peers = stats
            .live
            .as_ref()
            .map(|l| l.snapshot.peer_stats.live as u64)
            .unwrap_or(0);

        let mut files = Vec::new();
        let mut total_size = 0u64;
        let mut offset = 0u64;
        if let Some(m) = self.handle.metadata.load_full()
            && let Ok(iter) = m.info.iter_file_details()
        {
            for f in iter {
                let filename = f.filename.to_string().unwrap_or_default();
                files.push(StatsFile {
                    name: filename.clone(),
                    path: filename,
                    length: f.len,
                    offset,
                    downloaded: 0, // TODO: Implement per-file progress for librqbit if needed
                    progress: 0.0,
                });
                total_size += f.len;
                offset += f.len;
            }
        }

        EngineStats {
            name: self.name().unwrap_or_else(|| "Unknown".to_string()),
            info_hash: self.info_hash(),
            files,
            sources: vec![],
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
            queued: 0,
            unique: peers,
            connection_tries: 0,
            peer_search_running: true,
            stream_len: total_size,
            stream_name: "".to_string(),
            stream_progress: if total_size > 0 {
                downloaded as f64 / total_size as f64
            } else {
                0.0
            },
            swarm_connections: peers,
            swarm_paused: false,
            swarm_size: peers,
            is_finished: total_size > 0 && downloaded >= total_size,
            has_metadata: total_size > 0,
        }
    }

    async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
        Ok(())
    }

    async fn get_file_reader(
        &self,
        file_idx: usize,
        _start_offset: u64,
        _priority: u8,
        _bitrate: Option<u64>,
        _intent: crate::backend::priorities::PlaybackIntent,
    ) -> Result<Box<dyn FileStreamTrait>> {
        let stream = self
            .handle
            .clone()
            .stream(file_idx)
            .context("Failed to stream from librqbit")?;
        Ok(Box::new(stream))
    }

    async fn get_files(&self) -> Vec<BackendFileInfo> {
        let mut files = Vec::new();
        if let Some(m) = self.handle.metadata.load_full()
            && let Ok(iter) = m.info.iter_file_details()
        {
            for f in iter {
                files.push(BackendFileInfo {
                    name: f.filename.to_string().unwrap_or_default(),
                    length: f.len,
                });
            }
        }
        files
    }

    async fn get_file_path(&self, _file_idx: usize) -> Option<String> {
        // librqbit doesn't expose local file paths easily
        // Return None to fall back to HTTP URL probing
        None
    }

    async fn prepare_file_for_streaming(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn clear_file_streaming(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn wait_for_piece_ready(
        &self,
        _file_idx: usize,
        _offset: u64,
        _timeout: Duration,
        _intent: crate::backend::priorities::PlaybackIntent,
    ) -> Result<PieceReadiness> {
        Ok(PieceReadiness {
            ready: true,
            piece: -1,
            ready_pieces: 1,
            target_pieces: 1,
            elapsed_ms: 0,
            peers: 0,
            download_rate: 0,
            reason: "librqbit-reader".to_string(),
        })
    }
}

impl Clone for LibrqbitHandle {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            info_hash: self.info_hash.clone(),
            session: self.session.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::TorrentBackend;

    /// Write `len` patterned bytes to `path` (deterministic, non-trivial data
    /// so piece hashes are meaningful).
    pub(super) async fn write_payload(path: &std::path::Path, len: usize) {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(path, &data).await.expect("write payload");
    }

    /// Create a .torrent for `path` with small pieces so tests stay fast.
    /// Returns (serialized torrent bytes, info hash as hex). librqbit 8.1.1
    /// does not export `CreateTorrentResult`, so the value cannot be named
    /// outside the crate — extract what we need immediately.
    pub(super) async fn make_torrent(path: &std::path::Path) -> (Vec<u8>, String) {
        let t = librqbit::create_torrent(
            path,
            librqbit::CreateTorrentOptions {
                name: None,
                piece_length: Some(16384),
            },
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
