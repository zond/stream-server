use crate::backend::{
    EngineStats, PeerStat, SubtitleTrack, TorrentHandle, priorities::PlaybackIntent,
};
use crate::cache::DataCache;
use anyhow::Context;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::AsyncSeekExt;
use tokio::sync::{Mutex, OnceCell};

use crate::files::FileHandle;
use regex::Regex;

type OpensubHashCell = Arc<OnceCell<Result<String, String>>>;
type OpensubHashInflight = HashMap<usize, OpensubHashCell>;

/// Season/episode hints sent by stremio-video's createTorrent.js as
/// `guessFileIdx: {season, episode}` (stremio-core: `CreatedTorrent.guess_file_idx`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SeriesInfo {
    pub season: Option<usize>,
    pub episode: Option<usize>,
}

impl SeriesInfo {
    /// True when at least one of season/episode is present — an empty
    /// `guessFileIdx: {}` (movies) must not trigger episode-tag matching,
    /// otherwise resolution strings like `1920x1080` get treated as tags.
    pub fn has_hints(&self) -> bool {
        self.season.is_some() || self.episode.is_some()
    }
}

/// Extensions the guesser considers playable media (mirrors server.js).
const GUESS_MEDIA_EXTENSIONS: [&str; 14] = [
    ".mkv", ".avi", ".mp4", ".wmv", ".mov", ".mpg", ".ts", ".webm", ".flac", ".mp3", ".wav",
    ".wma", ".aac", ".ogg",
];

/// Guess which file in `files` should be played, mirroring server.js's
/// `guessFileIdx`: among media files, prefer ones whose name carries an
/// episode tag (`SxxEyy` or `NxM`, case-insensitive) matching `series_info`;
/// otherwise fall back to the largest media file. Non-media files are never
/// chosen. Ties on size resolve to the lowest file index. Returns an index
/// into `files`, or `None` when the list holds no media file at all.
pub fn guess_file_index_in(
    files: &[crate::backend::BackendFileInfo],
    series_info: Option<&SeriesInfo>,
) -> Option<usize> {
    use std::cmp::Reverse;
    use std::sync::LazyLock;

    static RE_SXE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[sS](\d+)[eE](\d+)").unwrap());
    static RE_XX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)x(\d+)").unwrap());

    let media_files: Vec<(usize, u64, String)> = files
        .iter()
        .enumerate()
        .filter_map(|(idx, file)| {
            let filename = file.name.to_lowercase();
            if GUESS_MEDIA_EXTENSIONS
                .iter()
                .any(|ext| filename.ends_with(ext))
            {
                Some((idx, file.length, filename))
            } else {
                None
            }
        })
        .collect();

    if media_files.is_empty() {
        return None;
    }

    if let Some(series) = series_info.filter(|series| series.has_hints()) {
        let mut candidates = Vec::new();
        for (idx, length, filename) in &media_files {
            let mut found_s = None;
            let mut found_e = None;

            if let Some(caps) = RE_SXE.captures(filename) {
                found_s = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
                found_e = caps.get(2).and_then(|m| m.as_str().parse::<usize>().ok());
            } else if let Some(caps) = RE_XX.captures(filename) {
                found_s = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
                found_e = caps.get(2).and_then(|m| m.as_str().parse::<usize>().ok());
            }

            let s_match = series.season.is_none() || found_s == series.season;
            let e_match = series.episode.is_none() || found_e == series.episode;

            if s_match && e_match && (found_s.is_some() || found_e.is_some()) {
                candidates.push((*idx, *length));
            }
        }
        if !candidates.is_empty() {
            return candidates
                .into_iter()
                .max_by_key(|&(idx, len)| (len, Reverse(idx)))
                .map(|(idx, _)| idx);
        }
    }

    // Fallback to the largest media file
    media_files
        .into_iter()
        .max_by_key(|&(idx, len, _)| (len, Reverse(idx)))
        .map(|(idx, _, _)| idx)
}

#[cfg(test)]
mod guess_tests {
    use super::{SeriesInfo, guess_file_index_in};
    use crate::backend::BackendFileInfo;

    fn f(name: &str, length: u64) -> BackendFileInfo {
        BackendFileInfo {
            name: name.to_string(),
            length,
        }
    }

    fn si(season: Option<usize>, episode: Option<usize>) -> SeriesInfo {
        SeriesInfo { season, episode }
    }

    #[test]
    fn sxe_tag_matches_requested_season_episode() {
        let files = [
            f("Show.S01E01.mkv", 100),
            f("Show.S02E05.mkv", 200),
            f("Show.S02E06.mkv", 150),
        ];
        let info = si(Some(2), Some(5));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn nxm_tag_matches_requested_season_episode() {
        let files = [
            f("Ep.2x07.mkv", 100),
            f("Ep.3x07.mkv", 120),
            f("Ep.3x08.mkv", 110),
        ];
        let info = si(Some(3), Some(7));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn season_only_hint_picks_largest_matching_season() {
        let files = [
            f("A.S01E01.mkv", 100),
            f("B.S02E01.mkv", 100),
            f("C.S02E05.mkv", 300),
        ];
        let info = si(Some(2), None);
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(2));
    }

    #[test]
    fn episode_only_hint_picks_largest_matching_episode() {
        let files = [
            f("A.S01E05.mkv", 100),
            f("B.S02E05.mkv", 200),
            f("C.S03E01.mkv", 100),
        ];
        let info = si(None, Some(5));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn size_tie_resolves_to_lowest_index() {
        let files = [f("A.S01E01.mkv", 100), f("B.S01E01.mkv", 100)];
        let info = si(Some(1), Some(1));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(0));
    }

    #[test]
    fn non_media_files_never_chosen_even_when_largest() {
        let files = [
            f("big.nfo", 999_999),
            f("movie.mkv", 100),
            f("subs.srt", 5_000),
            f("installer.exe", 8_000),
        ];
        // Movie case: no hints, must fall back to the only media file.
        assert_eq!(guess_file_index_in(&files, None), Some(1));
    }

    #[test]
    fn non_media_with_matching_tag_still_excluded() {
        // A .nfo carrying the requested SxxEyy tag must not win over the media file.
        let files = [f("Show.S01E01.nfo", 999_999), f("Show.S01E01.mkv", 100)];
        let info = si(Some(1), Some(1));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn empty_series_info_does_not_read_resolution_as_tag() {
        // guessFileIdx:{} (movie) => has_hints()==false: a "1920x1080" filename
        // must NOT be treated as season 1920 episode 1080; falls back to largest.
        let files = [f("Movie.1920x1080.mkv", 100), f("Movie.Extras.mkv", 200)];
        let info = SeriesInfo::default();
        assert!(!info.has_hints());
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn no_episode_match_falls_back_to_largest_media() {
        let files = [f("A.S01E01.mkv", 100), f("B.S02E02.mkv", 300)];
        let info = si(Some(9), Some(9));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn zero_media_files_returns_none() {
        let files = [f("a.txt", 100), f("b.nfo", 200)];
        assert_eq!(
            guess_file_index_in(&files, Some(&si(Some(1), Some(1)))),
            None
        );
        assert_eq!(guess_file_index_in(&[], None), None);
    }
}

pub struct Engine<H: TorrentHandle> {
    pub info_hash: String,
    pub handle: H,
    pub last_accessed: AtomicU64,
    pub active_streams: Arc<AtomicUsize>,
    opensub_hash_cache: Mutex<HashMap<usize, String>>,
    opensub_hash_inflight: Mutex<OpensubHashInflight>,
    pub data_cache: DataCache,
    /// Whether this torrent was paused by the idle seeding-disabled policy.
    /// A new playback request resumes it before making a file wanted.
    pub idle_paused: AtomicBool,
}

impl<H: TorrentHandle> Engine<H> {
    pub fn new_with_handle(handle: H, info_hash: &str) -> Self {
        Self {
            info_hash: info_hash.to_string(),
            handle,
            last_accessed: AtomicU64::new(crate::now_secs()),
            active_streams: Arc::new(AtomicUsize::new(0)),
            opensub_hash_cache: Mutex::new(HashMap::new()),
            opensub_hash_inflight: Mutex::new(HashMap::new()),
            data_cache: moka::future::Cache::builder()
                .weigher(|_key, value: &Arc<Vec<u8>>| value.len() as u32)
                .max_capacity(64 * 1024 * 1024) // 64MB cache per engine
                .build(),
            idle_paused: AtomicBool::new(false),
        }
    }

    pub fn touch(&self) {
        self.last_accessed
            .store(crate::now_secs(), Ordering::SeqCst);
    }

    pub fn find_file_by_regex(&self, regex_str: &str) -> Option<usize> {
        let _re = Regex::new(regex_str).ok()?;
        // This is tricky now as find_file_by_regex was librqbit specific.
        // For now, we'll assume we can list files.
        None
    }

    pub async fn guess_file_index(
        &self,
        series_info: Option<&crate::engine::SeriesInfo>,
    ) -> Option<usize> {
        let files = self.handle.get_files().await;
        guess_file_index_in(&files, series_info)
    }

    pub async fn get_statistics(&self) -> EngineStats {
        self.touch();
        let mut stats = self.handle.stats().await;

        let guessed_file_idx = self.guess_file_index(None).await.unwrap_or(0);

        // Get stream info for the guessed file
        if guessed_file_idx < stats.files.len() {
            let file = &stats.files[guessed_file_idx];
            stats.stream_name = file.name.clone();
            stats.stream_len = file.length;

            // Derive stream progress from the streaming file's own
            // downloaded/length, NOT the torrent's total_wanted set. During cold
            // start the file baseline is dropped to 0 so only a few metadata
            // pieces are "wanted"; total_wanted_done/total_wanted then spikes
            // toward 100% just before playback until the head priorities widen
            // the wanted set again. The per-file fraction is stable across those
            // priority changes (downloaded counts verified pieces in range).
            if file.length > 0 {
                stats.stream_progress = (file.downloaded as f64 / file.length as f64).min(1.0);
            }

            tracing::debug!(
                "get_statistics: file_idx={} file.progress={:.2}% total_done={} stream_progress={:.2}%",
                guessed_file_idx,
                file.progress * 100.0,
                stats.downloaded,
                stats.stream_progress * 100.0
            );
        } else {
            tracing::debug!(
                "get_statistics: guessed_file_idx {} >= stats.files.len() {}",
                guessed_file_idx,
                stats.files.len()
            );
        }

        stats
    }

    pub async fn get_file(
        self: &Arc<Self>,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
    ) -> Option<FileHandle<H>> {
        let intent = if priority == 255 {
            PlaybackIntent::InternalProbe
        } else if priority == 0 {
            PlaybackIntent::Background
        } else if start_offset == 0 {
            PlaybackIntent::DirectInitial
        } else {
            PlaybackIntent::DirectSeek
        };
        self.get_file_with_intent(file_idx, start_offset, priority, intent)
            .await
    }

    pub async fn get_file_with_intent(
        self: &Arc<Self>,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
        intent: PlaybackIntent,
    ) -> Option<FileHandle<H>> {
        let startup = Instant::now();
        tracing::debug!(
            "[STREAMING] Preparing file {} for playback (offset={}, intent={:?})",
            file_idx,
            start_offset,
            intent
        );

        self.touch();

        let files = self.handle.get_files().await;
        if file_idx >= files.len() {
            return None;
        }

        let length = files[file_idx].length;
        let name = files[file_idx].name.clone();

        if !self.handle.manages_playback_lifecycle()
            && priority != 255
            && start_offset == 0
            && matches!(intent, PlaybackIntent::DirectInitial)
        {
            let prepare_start = Instant::now();
            if let Err(e) = self.handle.prepare_file_for_streaming(file_idx).await {
                tracing::warn!("get_file: prepare_file_for_streaming failed: {}", e);
                // Continue anyway - the file reader will block on pieces as needed
            } else {
                tracing::info!(
                    "startup: direct prepare_file_for_streaming completed in {:?} for file {}",
                    prepare_start.elapsed(),
                    file_idx
                );
            }
        } else {
            tracing::debug!(
                "get_file: skipping prepare_file_for_streaming for file {} offset {} priority {}",
                file_idx,
                start_offset,
                priority
            );
        }

        let reader_start = Instant::now();
        let reader = self
            .handle
            .get_file_reader(file_idx, start_offset, priority, None, intent)
            .await
            .ok()?;
        tracing::debug!(
            "startup: get_file_reader returned in {:?} for file {} offset {} (total={:?})",
            reader_start.elapsed(),
            file_idx,
            start_offset,
            startup.elapsed()
        );

        self.active_streams.fetch_add(1, Ordering::SeqCst);

        // Use raw reader directly for better performance
        // The torrent backend reads from local files, caching adds overhead
        Some(FileHandle::new(length, name, reader, self.clone()))
    }

    pub async fn get_opensub_hash(self: &Arc<Self>, file_idx: usize) -> anyhow::Result<String> {
        if !self.handle.manages_playback_lifecycle() {
            return self.calculate_opensub_hash(file_idx).await;
        }
        if let Some(hash) = self.opensub_hash_cache.lock().await.get(&file_idx).cloned() {
            return Ok(hash);
        }

        let inflight = {
            let mut hashes = self.opensub_hash_inflight.lock().await;
            hashes
                .entry(file_idx)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let result = inflight
            .get_or_init(|| async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    self.calculate_opensub_hash(file_idx),
                )
                .await
                .map_err(|_| "OpenSub hash reader timed out after 15 seconds".to_string())?
                .map_err(|error| format!("{error:#}"))
            })
            .await
            .clone();
        {
            let mut hashes = self.opensub_hash_inflight.lock().await;
            if hashes
                .get(&file_idx)
                .is_some_and(|current| Arc::ptr_eq(current, &inflight))
            {
                hashes.remove(&file_idx);
            }
        }
        match result {
            Ok(hash) => {
                self.opensub_hash_cache
                    .lock()
                    .await
                    .insert(file_idx, hash.clone());
                Ok(hash)
            }
            Err(error) => Err(anyhow::anyhow!(error)),
        }
    }

    async fn calculate_opensub_hash(self: &Arc<Self>, file_idx: usize) -> anyhow::Result<String> {
        let files = self.handle.get_files().await;
        if file_idx >= files.len() {
            return Err(anyhow::anyhow!("File not found"));
        }
        let file_len = files[file_idx].length;

        let file_opt = self.get_file(file_idx, 0, 255).await;
        let mut file = file_opt.context("failed to get file handle")?;

        let chunk_size = 65536u64;
        let head_size = std::cmp::min(file_len, chunk_size);
        let tail_size = std::cmp::min(file_len, chunk_size);

        let mut head = vec![0u8; head_size as usize];
        use tokio::io::AsyncReadExt;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        file.read_exact(&mut head).await?;

        let mut tail = vec![0u8; tail_size as usize];
        let start_pos = file_len.saturating_sub(chunk_size);
        file.seek(std::io::SeekFrom::Start(start_pos)).await?;
        file.read_exact(&mut tail).await?;

        let mut hash = file_len;

        for chunk in head.chunks(8) {
            let mut buf = [0u8; 8];
            let len = chunk.len();
            buf[..len].copy_from_slice(chunk);
            hash = hash.wrapping_add(u64::from_le_bytes(buf));
        }

        for chunk in tail.chunks(8) {
            let mut buf = [0u8; 8];
            let len = chunk.len();
            buf[..len].copy_from_slice(chunk);
            hash = hash.wrapping_add(u64::from_le_bytes(buf));
        }

        Ok(format!("{:016x}", hash))
    }

    pub async fn find_subtitle_tracks(&self) -> Vec<SubtitleTrack> {
        tracing::info!(
            "[SUBTITLES] find_subtitle_tracks called for info_hash={}",
            self.info_hash
        );
        let mut tracks = Vec::new();
        let files = self.handle.get_files().await;

        // 1. Find external subtitle files in the torrent
        for (idx, file) in files.iter().enumerate() {
            let filename = file.name.clone();
            let path = std::path::PathBuf::from(&filename);
            if let Some(ext) = path.extension()
                && let Some(ext_str) = ext.to_str()
            {
                let ext_lower = ext_str.to_lowercase();
                if ["srt", "vtt", "sub", "idx", "txt", "ssa", "ass"].contains(&ext_lower.as_str()) {
                    tracing::info!("[SUBTITLES] Found external subtitle: {}", filename);
                    tracks.push(SubtitleTrack {
                        id: idx,
                        name: filename,
                        size: file.length,
                    });
                }
            }
        }

        // Embedded-subtitle demuxing (ffmpeg/ffprobe) has been removed: the
        // client opens the video directly and selects embedded tracks itself.
        // Only external subtitle files are reported here.
        tracks
    }

    pub async fn get_peer_stats(&self) -> Vec<PeerStat> {
        // Placeholder for now, handle stats() returns EngineStats which has peers count but not per-peer stats yet
        Vec::new()
    }
}
