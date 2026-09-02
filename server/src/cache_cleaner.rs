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

    // 1. Identify protected files matching current active engines
    let mut protected_paths = HashSet::new();

    for (root, engines) in [
        (
            state.engine.download_dir.clone(),
            state.engine.get_all_statistics().await,
        ),
        (
            state.download_engine.download_dir.clone(),
            state.download_engine.get_all_statistics().await,
        ),
    ] {
        for (_, stats) in engines {
            if !stats.files.is_empty() {
                for file in stats.files {
                    let path = root.join(&file.path);
                    protected_paths.insert(path);
                }
            } else {
                let path = root.join(&stats.name);
                protected_paths.insert(path);
            }
        }
    }

    // 2. Scan and Evict immediately based on age (30 days)
    let thirty_days = Duration::from_secs(30 * 24 * 60 * 60);
    let now = std::time::SystemTime::now();

    let mut files = Vec::new();
    let mut total_size = 0u64;

    for download_dir in &download_dirs {
        if !download_dir.exists() {
            continue;
        }

        let mut entries = walkdir::WalkDir::new(download_dir).into_iter();

        loop {
            match entries.next() {
                Some(Ok(entry)) => {
                    if entry.file_type().is_file() {
                        let path = entry.path().to_path_buf();
                        if path
                            .components()
                            .any(|component| component.as_os_str() == ".metadata")
                        {
                            continue;
                        }
                        // Is protected?
                        let is_protected = is_path_protected(&path, &protected_paths);

                        if let Ok(metadata) = entry.metadata() {
                            let size = metadata.len();

                            if is_protected {
                                total_size += size;
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
    if limit > 0 && total_size > limit {
        info!(
            "Cache size {} exceeds limit {}. Cleaning up...",
            total_size, limit
        );

        // Sort by modification time (oldest first)
        files.sort_by_key(|a| a.2);

        let mut deleted_count = 0;
        let mut freed_space = 0;

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

    Ok(())
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
    use super::{is_path_protected, remove_empty_parents};
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

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
}
