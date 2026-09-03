use crate::state::AppState;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

pub fn start(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        debug!("Cache cleaner started");

        // Channel for file system events
        let (tx, mut rx) = mpsc::channel::<()>(100);

        // Setup Watcher
        // We use a sync watcher bridge to async channel
        let tx_clone = tx.clone();
        let mut watcher = match RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| {
                match res {
                    Ok(event) => {
                        // Filter interesting events
                        if matches!(
                            event.kind,
                            notify::EventKind::Create(_)
                                | notify::EventKind::Modify(_)
                                | notify::EventKind::Remove(_)
                        ) {
                            let _ = tx_clone.blocking_send(());
                        }
                    }
                    Err(e) => error!("Watch error: {:?}", e),
                }
            },
            notify::Config::default(),
        ) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return;
            }
        };

        // Initial watch
        // We might need to retry if directory doesn't exist yet
        let mut download_dirs = vec![
            state.engine.download_dir.clone(),
            state.download_engine.download_dir.clone(),
        ];
        download_dirs.sort();
        download_dirs.dedup();
        for download_dir in &download_dirs {
            if let Err(e) = watcher.watch(download_dir, RecursiveMode::Recursive) {
                warn!("Failed to watch download dir {:?}: {}", download_dir, e);
                // We will try to re-watch inside the loop if needed (omitted for brevity, relying on fallback poll)
            }
        }

        // Timer for fallback polling (1 hour)
        let mut poll_interval = tokio::time::interval(Duration::from_secs(3600));

        // State for debouncing
        let debounce_duration = Duration::from_secs(60); // Wait 60s after last activity to clean
        let mut active_cleaning_timer = Box::pin(tokio::time::sleep(Duration::MAX)); // Inactive initially

        loop {
            tokio::select! {
                // 1. Fallback / Periodic Poll
                _ = poll_interval.tick() => {
                    debug!("Periodic cache clean trigger");
                    if let Err(e) = clean_cache(&state).await {
                        error!("Cache cleaner error: {}", e);
                    }
                    // Re-ensure watch if needed
                    for download_dir in [&state.engine.download_dir, &state.download_engine.download_dir] {
                        if let Err(e) = watcher.watch(download_dir, RecursiveMode::Recursive) {
                            debug!("Retry watch {:?}: {}", download_dir, e);
                        }
                    }
                }

                // 2. File System Event
                Some(_) = rx.recv() => {
                    // Reset debounce timer
                    active_cleaning_timer = Box::pin(tokio::time::sleep(debounce_duration));
                }

                // 3. Debounce Timer Fired
                _ = &mut active_cleaning_timer => {
                    debug!("Debounced cache clean trigger");
                    if let Err(e) = clean_cache(&state).await {
                        error!("Cache cleaner error: {}", e);
                    }
                    // Reset timer to infinite
                    active_cleaning_timer = Box::pin(tokio::time::sleep(Duration::MAX));
                }
            }
        }
    })
}

async fn clean_cache(state: &Arc<AppState>) -> anyhow::Result<()> {
    let settings = state.settings.read().await;
    let limit = crate::routes::system::cache_size_bytes(settings.cache_size);
    drop(settings); // Release lock

    let mut download_dirs = vec![
        state.engine.download_dir.clone(),
        state.download_engine.download_dir.clone(),
    ];
    download_dirs.sort();
    download_dirs.dedup();
    if download_dirs
        .iter()
        .all(|download_dir| !download_dir.exists())
    {
        return Ok(());
    }

    // 1. Identify protected files: everything a live engine writes, at the
    // paths the backend reports (a pinned engine stays live, so its data is
    // protected for as long as the pin holds).
    let mut protected_paths: HashSet<_> =
        state.engine.protected_paths().await.into_iter().collect();
    protected_paths.extend(state.download_engine.protected_paths().await);

    // 2. The downloads dir is not cache: offline downloads live there
    // because the user asked for them, and a pin whose torrent the backend
    // did not restore has no engine to protect its files. Nothing under it
    // is walked -- which matters only when it sits inside a cache root,
    // since the cleaner walks nothing else.
    let mut downloads_dirs: Vec<_> = [
        state.engine.downloads_dir(),
        state.download_engine.downloads_dir(),
    ]
    .into_iter()
    .flatten()
    .collect();
    downloads_dirs.sort();
    downloads_dirs.dedup();

    evict(&download_dirs, &protected_paths, limit, &downloads_dirs)
        .await
        .map(|_report| ())
}

/// What a cache root costs on disk, as the cleaner must count it.
///
/// librqbit pre-allocates every file it wants at its **full** length, so a
/// part-streamed film is a multi-gigabyte apparent length over a handful of
/// allocated blocks -- `Metadata::len` on such a file describes the movie,
/// not the phone. A device reporting 17 GB of cache had 3.85 GB on it, and
/// the cleaner spent every run trying to evict its way under a limit the
/// disk was never over. (enginefs learned the same lesson about progress:
/// count what the backend allocated, never `metadata().len()`.)
///
/// On Unix `st_blocks` is the allocated block count in 512-byte units *by
/// definition* -- the unit is POSIX, not the filesystem's block size -- so
/// `blocks() * 512` is the occupancy including any tail slack. Windows has
/// no equivalent through `std` (it needs `GetCompressedFileSize` or
/// `FSCTL_QUERY_ALLOCATED_RANGES` through the Win32 API), so there the
/// apparent length stands in, exactly as it did everywhere before: it is an
/// over-estimate for a sparse file, which errs towards cleaning too eagerly
/// rather than letting a disk fill.
pub(crate) fn occupied_bytes(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

/// What one [`evict`] run found and did, in occupancy bytes
/// ([`occupied_bytes`]) throughout.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct EvictionReport {
    /// Occupancy of the walked roots once eviction finished.
    pub total: u64,
    /// How much of `total` eviction may never touch: files a live engine
    /// reports (a pinned download's engine is never swept, so its files are
    /// in here for as long as the pin holds).
    pub protected: u64,
    /// How many files that is.
    pub protected_files: usize,
    /// Occupancy reclaimed by the size rule.
    pub freed: u64,
    /// How many files that took.
    pub deleted: usize,
    /// The limit this run was given (0 = none).
    pub limit: u64,
}

impl EvictionReport {
    /// The line to log when the run ended still over the limit, naming what
    /// protection kept -- "cleaned up 0 files, freed 0 bytes" on a phone
    /// that is filling up says nothing about *why*, and the why is always
    /// that the rest of the cache belongs to a live or pinned torrent.
    /// `None` when the run got under the limit (or had none).
    pub fn shortfall_message(&self) -> Option<String> {
        if self.limit == 0 || self.total <= self.limit {
            return None;
        }
        Some(format!(
            "Cache size {} still exceeds limit {} after freeing {} bytes from {} files: \
             {} bytes in {} files are protected (a live torrent is writing them, or a pinned \
             download keeps them) and cannot be evicted",
            self.total, self.limit, self.freed, self.deleted, self.protected, self.protected_files,
        ))
    }
}

/// Walk `download_dirs` and evict what is neither protected nor a session
/// artefact: first every file older than 30 days, then -- while the rest
/// exceeds `limit` (0 = no limit) -- the least recently modified files.
/// Sizes are occupancy, not apparent length (see [`occupied_bytes`]).
/// Nothing under `downloads_dirs` is walked at all: those files are
/// offline downloads, not cache, so they are neither evicted nor counted
/// towards `limit`. Only strict descendants of a walked root are pruned
/// that way -- a `downloads_dirs` entry that is a root or above it is
/// warned about and ignored, since pruning it would leave nothing walked
/// and stop every eviction rule for that root. That is also why an unset
/// `downloadsDir` cannot switch the cleaner off: it leaves `downloads_dirs`
/// empty rather than defaulting to the cache root the engines write in.
async fn evict(
    download_dirs: &[std::path::PathBuf],
    protected_paths: &HashSet<std::path::PathBuf>,
    limit: u64,
    downloads_dirs: &[std::path::PathBuf],
) -> anyhow::Result<EvictionReport> {
    // 2. Scan and Evict immediately based on age (30 days)
    let thirty_days = Duration::from_secs(30 * 24 * 60 * 60);
    let now = std::time::SystemTime::now();

    let mut files = Vec::new();
    let mut total_size = 0u64;
    let mut protected_size = 0u64;
    let mut protected_files = 0usize;

    for download_dir in download_dirs {
        if !download_dir.exists() {
            continue;
        }

        // Only strict descendants of this root are pruned. `filter_entry`
        // applies its predicate to the root entry as well and a rejected
        // directory ends the walk (`skip_current_dir`), so a downloads dir
        // that is this root, or above it, would silently switch BOTH
        // eviction rules off for it -- nothing walked, nothing aged out,
        // `cacheSize` never enforced. `prepare_downloads_dir` refuses such
        // a setting; a persisted or externally-set one is warned about and
        // ignored here, and what is under the root is treated as cache.
        let pruned: Vec<std::path::PathBuf> = downloads_dirs
            .iter()
            .filter(|dir| dir.as_path() != download_dir.as_path() && dir.starts_with(download_dir))
            .cloned()
            .collect();
        for dir in downloads_dirs {
            if download_dir.starts_with(dir) {
                warn!(
                    downloads_dir = %dir.display(),
                    cache_root = %download_dir.display(),
                    "downloadsDir is at or above a torrent cache root; it cannot be told apart from cache and is not spared from eviction"
                );
            }
        }

        let mut entries = walkdir::WalkDir::new(download_dir)
            .into_iter()
            .filter_entry(|entry| entry.depth() == 0 || !is_under(entry.path(), &pruned));

        loop {
            match entries.next() {
                Some(Ok(entry)) => {
                    if entry.file_type().is_file() {
                        let path = entry.path().to_path_buf();
                        if is_session_artifact(&path, download_dir) {
                            continue;
                        }
                        // Is protected?
                        let is_protected = is_path_protected(&path, protected_paths);

                        if let Ok(metadata) = entry.metadata() {
                            // Occupancy, not apparent length: librqbit
                            // pre-allocates wanted files at full size.
                            let size = occupied_bytes(&metadata);

                            if is_protected {
                                total_size += size;
                                protected_size += size;
                                protected_files += 1;
                                continue;
                            }

                            if let Ok(modified) = metadata.modified() {
                                // Check AGE
                                let age = now
                                    .duration_since(modified)
                                    .unwrap_or(Duration::from_secs(0));
                                if age > thirty_days {
                                    info!("File older than 30 days, deleting: {:?}", path);
                                    if let Err(e) = tokio::fs::remove_file(&path).await {
                                        error!("Failed to delete file {:?}: {}", path, e);
                                        // Count it in total size since we failed to delete?
                                        // Or ignore? Let's count it to be safe for cache limit.
                                        total_size += size;
                                    } else {
                                        // Successfully deleted, do not add to total_size
                                        // Try to clean empty parent dir
                                        if let Some(parent) = path.parent() {
                                            remove_empty_parents(parent, download_dir).await;
                                        }
                                    }
                                } else {
                                    // Keep for potentially size-based eviction
                                    total_size += size;
                                    files.push((path, size, modified));
                                }
                            } else {
                                // Could not read time, keep it but count size
                                total_size += size;
                                files.push((path, size, std::time::SystemTime::UNIX_EPOCH));
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    debug!("Error walking directory: {}", e);
                }
                None => break,
            }
        }
    }

    // 3. Size-based Eviction
    let mut deleted_count = 0usize;
    let mut freed_space = 0u64;
    if limit > 0 && total_size > limit {
        info!(
            "Cache size {} exceeds limit {}. Cleaning up...",
            total_size, limit
        );

        // Sort by modification time (oldest first)
        files.sort_by_key(|a| a.2);

        for (path, size, _) in files {
            if total_size <= limit {
                break;
            }

            if size > limit {
                info!(
                    "cache soft limit exceeded by single retained file: {:?} size={} limit={}",
                    path, size, limit
                );
                continue;
            }

            debug!("Deleting old file (size limit): {:?}", path);
            if let Err(e) = tokio::fs::remove_file(&path).await {
                error!("Failed to delete file {:?}: {}", path, e);
            } else {
                total_size = total_size.saturating_sub(size);
                freed_space += size;
                deleted_count += 1;

                if let Some(parent) = path.parent()
                    && let Some(root) = download_dirs.iter().find(|root| path.starts_with(root))
                {
                    remove_empty_parents(parent, root).await;
                }
            }
        }

        info!(
            "Cleaned up {} files, freed {} bytes. New size: {}",
            deleted_count, freed_space, total_size
        );
    }

    let report = EvictionReport {
        total: total_size,
        protected: protected_size,
        protected_files,
        freed: freed_space,
        deleted: deleted_count,
        limit,
    };
    if let Some(message) = report.shortfall_message() {
        warn!("{message}");
    }

    Ok(report)
}

/// Whether `path` (under the session root `root`) is state the torrent
/// session or the engine keeps next to the payload rather than payload: it
/// is neither aged out nor counted against the cache limit. librqbit's
/// `session.json` and per-torrent `<info hash>.torrent` / `.bitv`
/// (fastresume bitfield) records at the top level, with their `.tmp`
/// siblings; everything under `.metadata/` and `.cache/`; and the engine's
/// `pinned-downloads.json` with its `.json.tmp-*` temp files. All of these
/// change only when the session does, so by mtime a stable pin set or a
/// finished pinned download would be the first casualties.
fn is_session_artifact(path: &std::path::Path, root: &std::path::Path) -> bool {
    if path
        .components()
        .any(|component| matches!(component.as_os_str().to_str(), Some(".metadata" | ".cache")))
    {
        return true;
    }
    if path.parent() != Some(root) {
        return false;
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let name = name.strip_suffix(".tmp").unwrap_or(name);
    if name == "session.json" || name == "pinned-downloads.json" {
        return true;
    }
    if let Some(rest) = name.strip_prefix("pinned-downloads.json.tmp-") {
        return !rest.is_empty();
    }
    match name.rsplit_once('.') {
        Some((stem, "torrent" | "bitv")) => {
            stem.len() == 40 && stem.bytes().all(|b| b.is_ascii_hexdigit())
        }
        _ => false,
    }
}

/// Whether `path` is one of `roots` or lives under one, component-wise (as
/// in [`is_path_protected`]).
fn is_under(path: &std::path::Path, roots: &[std::path::PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

/// A file is protected from eviction when its full path is in `protected` or
/// when it lives under a protected directory. Uses `Path::starts_with`, which
/// matches whole path components — so `/dl/Movie2/x.mkv` is NOT shielded by a
/// protected `/dl/Movie` entry, only a true `/dl/Movie/...` descendant is.
fn is_path_protected(path: &std::path::Path, protected: &HashSet<std::path::PathBuf>) -> bool {
    protected.contains(path) || protected.iter().any(|p| path.starts_with(p))
}

async fn remove_empty_parents(mut dir: &std::path::Path, root: &std::path::Path) {
    while dir != root {
        if tokio::fs::remove_dir(dir).await.is_err() {
            break;
        }
        let Some(parent) = dir.parent() else {
            break;
        };
        dir = parent;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EvictionReport, evict, is_path_protected, is_session_artifact, occupied_bytes,
        remove_empty_parents,
    };
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn session_artifacts_are_recognised_at_the_top_level_only() {
        let root = Path::new("/dl");
        for name in [
            "session.json",
            "session.json.tmp",
            "pinned-downloads.json",
            "pinned-downloads.json.tmp-4242-7",
            &format!("{HASH}.torrent"),
            &format!("{HASH}.bitv"),
            &format!("{HASH}.bitv.tmp"),
        ] {
            assert!(is_session_artifact(&root.join(name), root), "{name}");
            assert!(
                !is_session_artifact(&root.join("show").join(name), root),
                "a payload file named {name} inside a torrent folder is payload"
            );
        }
        assert!(is_session_artifact(
            &root.join(".cache").join("x.torrent"),
            root
        ));
        assert!(is_session_artifact(&root.join(".metadata").join("y"), root));
        for name in [
            "movie.mkv",
            "movie.torrent",
            "notes.json",
            "pinned-downloads.json.tmp-",
            "0123.bitv",
        ] {
            assert!(!is_session_artifact(&root.join(name), root), "{name}");
        }
    }

    fn write_aged(path: &Path, bytes: &[u8], age: Duration) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
    }

    /// What `evict` will count this file as. Derived, never hardcoded: a
    /// 4 KiB payload occupies one block on ext4 and rather more on a
    /// filesystem with a bigger allocation unit, so a limit written as a
    /// literal would be a filesystem assumption, not an assertion.
    fn occupancy(path: &Path) -> u64 {
        occupied_bytes(&std::fs::metadata(path).unwrap())
    }

    /// A limit that `keep` fits under and `keep` + `evictable` does not, so
    /// the size rule has to take exactly the evictable file.
    fn limit_between(keep: &Path, evictable: &Path) -> u64 {
        occupancy(keep) + occupancy(evictable) / 2
    }

    /// The session's own records live in the walked root but are not cache:
    /// neither the 30-day rule nor the size limit touches them, while stale
    /// payload beside them still goes.
    #[tokio::test]
    async fn evict_leaves_the_session_records_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let records = [
            root.join("session.json"),
            root.join("pinned-downloads.json"),
            root.join(format!("{HASH}.bitv")),
            root.join(format!("{HASH}.torrent")),
            root.join(".cache").join(format!("{HASH}.torrent")),
        ];
        for record in &records {
            write_aged(record, b"{}", forty_days);
        }
        let stale = root.join("old-show").join("e1.mkv");
        write_aged(&stale, &[0u8; 4096], forty_days);
        let recent = root.join("recent.mkv");
        write_aged(&recent, &[0u8; 4096], Duration::from_secs(60));

        evict(std::slice::from_ref(&root), &HashSet::new(), 0, &[])
            .await
            .unwrap();
        for record in &records {
            assert!(
                record.is_file(),
                "{} survives the age rule",
                record.display()
            );
        }
        assert!(!stale.exists(), "stale payload aged out");
        assert!(recent.is_file());

        // Size pressure: the records are the oldest files but never LRU
        // casualties, and they do not count towards the size either.
        let newer = root.join("newer.mkv");
        write_aged(&newer, &[0u8; 4096], Duration::from_secs(1));
        let limit = limit_between(&newer, &recent);
        evict(std::slice::from_ref(&root), &HashSet::new(), limit, &[])
            .await
            .unwrap();
        for record in &records {
            assert!(
                record.is_file(),
                "{} survives the size rule",
                record.display()
            );
        }
        assert!(!recent.exists(), "oldest payload evicted first");
        assert!(newer.is_file(), "back under the limit");
    }

    #[tokio::test]
    async fn remove_empty_parents_prunes_up_to_but_not_including_root() {
        let root = tempfile::tempdir().unwrap();
        let a = root.path().join("a");
        let b = a.join("b");
        let c = b.join("c");
        std::fs::create_dir_all(&c).unwrap();

        remove_empty_parents(&c, root.path()).await;

        assert!(!c.exists(), "empty leaf removed");
        assert!(!b.exists(), "empty parent removed");
        assert!(!a.exists(), "empty grandparent removed");
        assert!(root.path().exists(), "download root never removed");
    }

    #[tokio::test]
    async fn remove_empty_parents_stops_at_non_empty_directory() {
        let root = tempfile::tempdir().unwrap();
        let b = root.path().join("a").join("b");
        let c = b.join("c");
        std::fs::create_dir_all(&c).unwrap();
        let keep = b.join("keep.txt");
        std::fs::write(&keep, b"x").unwrap();

        remove_empty_parents(&c, root.path()).await;

        assert!(!c.exists(), "empty leaf removed");
        assert!(b.exists(), "non-empty sibling dir kept");
        assert!(keep.exists(), "unrelated file untouched");
    }

    #[tokio::test]
    async fn remove_empty_parents_never_removes_root_even_when_empty() {
        let root = tempfile::tempdir().unwrap();
        let a = root.path().join("a");
        std::fs::create_dir_all(&a).unwrap();

        // Climbing from `a` empties the root, but the loop must stop at it.
        remove_empty_parents(&a, root.path()).await;

        assert!(!a.exists());
        assert!(root.path().exists(), "root preserved even when empty");
    }

    #[test]
    fn is_path_protected_uses_component_wise_prefix() {
        let mut set = HashSet::new();
        set.insert(PathBuf::from("/dl/Movie/video.mkv"));
        set.insert(PathBuf::from("/dl/Series"));

        // Exact protected path.
        assert!(is_path_protected(Path::new("/dl/Movie/video.mkv"), &set));
        // A descendant of a protected directory.
        assert!(is_path_protected(Path::new("/dl/Series/S01/ep1.mkv"), &set));
        // Component-wise prefix: /dl/Series2 is NOT under /dl/Series.
        assert!(!is_path_protected(Path::new("/dl/Series2/ep.mkv"), &set));
        // Wholly unrelated file.
        assert!(!is_path_protected(Path::new("/dl/Other/x.mkv"), &set));
    }

    /// The downloads dir holds offline downloads, not cache. Even when it
    /// sits inside a walked cache root, nothing under it is aged out, and
    /// its bytes never count towards the size limit -- otherwise a finished
    /// download would evict the cache around it, and a pin whose torrent
    /// the backend did not restore (no engine, so no protected path) would
    /// be deleted after 30 days.
    #[tokio::test]
    async fn evict_never_walks_the_downloads_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let offline = root.join("offline");
        let download = offline.join(HASH).join("movie.mkv");
        write_aged(&download, &[0u8; 8192], forty_days);
        let stale = root.join("old-show").join("e1.mkv");
        write_aged(&stale, &[0u8; 4096], forty_days);
        let fresh = root.join("fresh.mkv");
        write_aged(&fresh, &[0u8; 4096], Duration::from_secs(60));
        let downloads_dirs = [offline.clone()];

        evict(
            std::slice::from_ref(&root),
            &HashSet::new(),
            0,
            &downloads_dirs,
        )
        .await
        .unwrap();
        assert!(download.is_file(), "an old download is not cache");
        assert!(!stale.exists(), "stale cache beside it still goes");
        assert!(fresh.is_file());

        // Size rule: the download is the biggest and oldest file there is,
        // and neither is evicted nor counted -- the cache below it is
        // already under the limit. The limit is exactly what the surviving
        // cache occupies, so counting the download would push it over.
        evict(
            std::slice::from_ref(&root),
            &HashSet::new(),
            occupancy(&fresh),
            &downloads_dirs,
        )
        .await
        .unwrap();
        assert!(download.is_file(), "never an LRU casualty");
        assert!(
            fresh.is_file(),
            "the download's bytes must not count towards the limit"
        );
        assert!(offline.join(HASH).is_dir(), "and its folder stays");
    }

    /// `WalkDir::filter_entry` runs its predicate on the walk root too,
    /// and a rejected directory ends the walk there -- so a downloads dir
    /// that IS a cache root, or sits above it, must never prune it.
    /// Pruning only strict descendants keeps both eviction rules running
    /// for the root (`prepare_downloads_dir` refuses such a setting; this
    /// is what keeps an unexpected one from switching the cleaner off).
    #[tokio::test]
    async fn evict_walks_a_root_its_downloads_dir_covers() {
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        for above in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().join("rqbit-downloads");
            let stale = root.join("old-show").join("e1.mkv");
            write_aged(&stale, &[0u8; 4096], forty_days);
            let older = root.join("older.mkv");
            write_aged(&older, &[0u8; 1024], Duration::from_secs(3600));
            let fresh = root.join("fresh.mkv");
            write_aged(&fresh, &[0u8; 4096], Duration::from_secs(60));
            let downloads_dirs = if above {
                vec![tmp.path().to_path_buf()]
            } else {
                vec![root.clone()]
            };

            evict(
                std::slice::from_ref(&root),
                &HashSet::new(),
                0,
                &downloads_dirs,
            )
            .await
            .unwrap();
            assert!(
                !stale.exists(),
                "downloads dir {downloads_dirs:?}: the age rule still runs in the walked root"
            );

            evict(
                std::slice::from_ref(&root),
                &HashSet::new(),
                limit_between(&fresh, &older),
                &downloads_dirs,
            )
            .await
            .unwrap();
            assert!(
                !older.exists(),
                "downloads dir {downloads_dirs:?}: the size rule still runs in the walked root"
            );
            assert!(fresh.is_file(), "back under the limit");
        }
    }

    /// A pinned download in the cache root (no downloads dir configured)
    /// keeps its engine -- the idle sweeper skips pinned torrents -- so its
    /// files come through `protected_paths` and neither rule touches them,
    /// however old they are.
    #[tokio::test]
    async fn evict_keeps_the_files_a_pinned_engine_reports() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let pinned = root.join("Show").join("e1.mkv");
        write_aged(&pinned, &[0u8; 8192], forty_days);
        let stale = root.join("Show").join("e2.mkv");
        write_aged(&stale, &[0u8; 4096], forty_days);
        let protected: HashSet<PathBuf> = HashSet::from([pinned.clone()]);

        evict(std::slice::from_ref(&root), &protected, 0, &[])
            .await
            .unwrap();
        assert!(pinned.is_file(), "the pinned file survives the age rule");
        assert!(!stale.exists(), "its unpinned neighbour does not");

        evict(std::slice::from_ref(&root), &protected, 1024, &[])
            .await
            .unwrap();
        assert!(pinned.is_file(), "and the size rule");
    }

    /// librqbit pre-allocates each file it wants at its full length, so the
    /// cache root is full of sparse files whose `len()` is the whole film
    /// and whose allocated blocks are a fraction of it. A phone reported
    /// 17 GB of cache and gave back 3.85 GB when it was cleared, and the
    /// cleaner evicted real files trying to get under a limit the disk had
    /// never crossed. Occupancy is what counts.
    #[cfg(unix)]
    #[tokio::test]
    async fn evict_counts_allocated_blocks_not_the_pre_allocated_length() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        std::fs::create_dir_all(&root).unwrap();

        // A 4 GiB file with one block of it actually written -- what a
        // just-started stream of a big film looks like on disk.
        let apparent = 4u64 << 30;
        let sparse = root.join("film.mkv");
        let mut file = std::fs::File::create(&sparse).unwrap();
        file.set_len(apparent).unwrap();
        file.seek(SeekFrom::Start(apparent - 1)).unwrap();
        file.write_all(&[1]).unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        drop(file);
        // Measured straight from `st_blocks`, not through the helper under
        // test: this guard only skips filesystems that materialised the
        // hole, and it must not be able to skip because the helper is
        // wrong.
        let allocated = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&sparse).unwrap().blocks() * 512
        };
        if allocated >= apparent {
            // No sparse-file support on this filesystem; there is nothing
            // to assert about occupancy that would not be a tautology.
            return;
        }

        let neighbour = root.join("subtitles.srt");
        write_aged(&neighbour, &[0u8; 4096], Duration::from_secs(60));

        // Between the real occupancy and the apparent one. Counting `len()`
        // put the cache 4 GiB over this: the sparse film is then skipped by
        // the single-file-larger-than-the-limit rule, so the eviction fell
        // on the only other candidate and freed nothing that mattered.
        let limit = 1u64 << 30;
        let report = evict(std::slice::from_ref(&root), &HashSet::new(), limit, &[])
            .await
            .unwrap();

        assert!(
            report.total < 1 << 20,
            "4 GiB of apparent length counted as {} bytes",
            report.total
        );
        assert_eq!(report.deleted, 0, "nothing needed evicting");
        assert_eq!(report.freed, 0);
        assert!(neighbour.is_file(), "no innocent file was evicted");
        assert!(sparse.is_file());
        assert_eq!(report.shortfall_message(), None, "the cache is not over");
    }

    /// The field condition: no `downloadsDir` is configured, so downloads
    /// would land in the very root the engines stream into -- and because
    /// an unset setting leaves `downloads_dirs` empty rather than pointing
    /// at that root, the walk still covers it. Ordinary streamed cache
    /// there is reclaimable; a pinned download and the file a live engine
    /// is writing are not.
    #[tokio::test]
    async fn evict_reclaims_unpinned_cache_sharing_the_root_with_downloads() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let cold = root.join("Cold").join("e1.mkv");
        write_aged(&cold, &[0u8; 4096], Duration::from_secs(7200));
        let warm = root.join("Warm").join("e1.mkv");
        write_aged(&warm, &[0u8; 4096], Duration::from_secs(60));
        let pinned = root.join("Pinned").join("movie.mkv");
        write_aged(&pinned, &[0u8; 8192], Duration::from_secs(9000));
        let live = root.join("Live").join("movie.mkv");
        write_aged(&live, &[0u8; 8192], Duration::from_secs(9000));
        let protected: HashSet<PathBuf> = HashSet::from([pinned.clone(), live.clone()]);

        let protected_bytes = occupancy(&pinned) + occupancy(&live);
        let cold_bytes = occupancy(&cold);
        let limit = protected_bytes + limit_between(&warm, &cold);

        let report = evict(std::slice::from_ref(&root), &protected, limit, &[])
            .await
            .unwrap();

        assert!(
            !cold.exists(),
            "the coldest ordinary cache in the shared root is reclaimable"
        );
        assert!(warm.is_file(), "and only as much of it as the limit needed");
        assert!(pinned.is_file(), "a pinned download is never cache");
        assert!(live.is_file(), "nor is what a live engine is writing");
        assert_eq!(report.deleted, 1);
        assert_eq!(report.freed, cold_bytes);
        assert_eq!(report.protected, protected_bytes);
        assert_eq!(report.protected_files, 2);
        assert!(report.total <= limit, "back under the limit");
        assert_eq!(report.shortfall_message(), None);
    }

    /// When the whole overage is protected there is nothing to evict, and
    /// "Cleaned up 0 files, freed 0 bytes" on a phone that is filling up
    /// explains none of it. The run says how many bytes protection holds.
    #[tokio::test]
    async fn evict_says_when_everything_over_the_limit_is_protected() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let pinned = root.join("Pinned").join("movie.mkv");
        write_aged(&pinned, &[0u8; 8192], Duration::from_secs(600));
        let live = root.join("Live").join("movie.mkv");
        write_aged(&live, &[0u8; 8192], Duration::from_secs(600));
        let protected: HashSet<PathBuf> = HashSet::from([pinned.clone(), live.clone()]);
        let protected_bytes = occupancy(&pinned) + occupancy(&live);
        let limit = protected_bytes / 2;

        let report = evict(std::slice::from_ref(&root), &protected, limit, &[])
            .await
            .unwrap();

        assert!(pinned.is_file());
        assert!(live.is_file());
        assert_eq!(report.deleted, 0);
        assert_eq!(report.freed, 0);
        assert_eq!(report.protected, protected_bytes);
        assert_eq!(report.protected_files, 2);
        assert_eq!(report.total, protected_bytes);

        let message = report.shortfall_message().expect("still over the limit");
        assert!(
            message.contains(&format!(
                "Cache size {protected_bytes} still exceeds limit {limit}"
            )),
            "{message}"
        );
        assert!(
            message.contains(&format!("{protected_bytes} bytes in 2 files are protected")),
            "{message}"
        );
    }

    #[test]
    fn shortfall_message_is_only_for_a_run_that_stayed_over_the_limit() {
        let under = EvictionReport {
            total: 10,
            limit: 20,
            ..EvictionReport::default()
        };
        assert_eq!(under.shortfall_message(), None);
        let unlimited = EvictionReport {
            total: u64::MAX,
            limit: 0,
            ..EvictionReport::default()
        };
        assert_eq!(unlimited.shortfall_message(), None, "0 = no limit");
        let over = EvictionReport {
            total: 30,
            limit: 20,
            protected: 25,
            protected_files: 3,
            freed: 5,
            deleted: 1,
        };
        assert!(over.shortfall_message().is_some());
    }
}
