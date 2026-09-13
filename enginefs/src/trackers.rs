use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::tracker_prober::TrackerProber;

const DEFAULT_TRACKERS_URL: &str =
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt";
const REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60); // Check every hour

/// Trait for tracker persistence (implemented by AppState or similar)
/// This allows enginefs to persist trackers without depending on server crate
///
/// Synchronous, and an implementation may block: the server's bridge answers
/// from a `block_on` of its settings lock. [`TrackerManager`] asks it on the
/// blocking pool for that reason -- see `ask_storage`.
pub trait TrackerStorage: Send + Sync {
    /// Get cached trackers from settings
    fn get_cached_trackers(&self) -> Vec<String>;
    /// Get Unix timestamp when trackers were last updated
    fn get_last_updated(&self) -> i64;
    /// Get the URL to fetch tracker list from
    fn get_source_url(&self) -> String;
    /// Save trackers and timestamp to persistent storage
    fn save_trackers(&self, trackers: Vec<String>, timestamp: i64);
}

#[derive(Clone)]
pub struct TrackerManager {
    trackers: Arc<RwLock<Vec<String>>>,
    storage: Option<Arc<dyn TrackerStorage>>,
    /// The periodic refresh task, held until its owner takes it -- see
    /// [`TrackerManager::take_refresh_task`]. Shared with every clone (the
    /// task holds one itself) so the manager the engine keeps can hand out
    /// the task a constructor spawned.
    refresh_task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
}

impl Default for TrackerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackerManager {
    /// Create a new TrackerManager without persistence (legacy behavior)
    /// A manager that fetches nothing and knows no trackers.
    ///
    /// For a process that must make no outbound request of its own -- the
    /// other half of `DhtBootstrapDns::Off`. [`TrackerManager::new`] spawns
    /// a task whose first act is an HTTP GET of the default tracker list,
    /// before anybody has added a torrent, so "offline" cannot be arranged
    /// after the fact by clearing the list.
    pub fn offline() -> Self {
        Self {
            trackers: Arc::new(RwLock::new(Vec::new())),
            storage: None,
            refresh_task: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    pub fn new() -> Self {
        let instance = Self {
            trackers: Arc::new(RwLock::new(Vec::new())),
            storage: None,
            refresh_task: Arc::new(parking_lot::Mutex::new(None)),
        };

        // Initial fetch and periodic refresh
        let manager = instance.clone();
        instance.set_refresh_task(tokio::spawn(async move {
            // Initial fetch
            if let Err(e) = manager
                .refresh_trackers_internal(DEFAULT_TRACKERS_URL)
                .await
            {
                warn!(error = %e, "Failed to fetch initial trackers");
            }

            let mut interval = tokio::time::interval(REFRESH_CHECK_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = manager
                    .refresh_trackers_internal(DEFAULT_TRACKERS_URL)
                    .await
                {
                    warn!(error = %e, "Failed to refresh trackers");
                }
            }
        }));

        instance
    }

    /// Create a new TrackerManager with persistence support
    pub fn new_with_storage(storage: Arc<dyn TrackerStorage>) -> Self {
        let instance = Self {
            trackers: Arc::new(RwLock::new(Vec::new())),
            storage: Some(storage.clone()),
            refresh_task: Arc::new(parking_lot::Mutex::new(None)),
        };

        // Initial load from cache + periodic refresh
        let manager = instance.clone();
        instance.set_refresh_task(tokio::spawn(async move {
            // Try to load from cache first
            manager.load_from_cache().await;

            // Check if refresh is needed
            if let Err(e) = manager.refresh_if_needed().await {
                warn!(error = %e, "Failed initial tracker refresh");
            }

            // Periodic check for refresh
            let mut interval = tokio::time::interval(REFRESH_CHECK_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = manager.refresh_if_needed().await {
                    warn!(error = %e, "Failed to refresh trackers");
                }
            }
        }));

        instance
    }

    fn set_refresh_task(&self, task: JoinHandle<()>) {
        *self.refresh_task.lock() = Some(task);
    }

    /// Take the periodic refresh task, so whoever shuts this process down can
    /// abort it -- the way `server::run` cancels every other forever-loop it
    /// starts. Returns the task once: the second caller gets `None`.
    ///
    /// What aborting it avoids: the `tokio::time::interval` above is built
    /// once, before the loop, so between refreshes the task is *parked* on
    /// `interval.tick()` with a timer registered on the runtime's time
    /// driver. Shutting that driver down marks it shut down and then fires
    /// every pending timer to the end of time
    /// (`runtime::time::Handle::shutdown` -> `process_at_time(u64::MAX)`),
    /// which wakes every task parked on one; a worker that polls such a task
    /// before the scheduler drops it trips `TimerEntry::poll_elapsed`'s
    /// `assert!(!self.driver().is_shutdown())` and panics with "A Tokio 1.x
    /// context was found, but it is being shutdown". Tokio catches it and the
    /// process survives, so it is only noise -- but it is the kind of noise a
    /// real panic hides in, and which of the wake and the drop wins is why it
    /// appears on some shutdowns and not others. A task that has been aborted
    /// is not there to be woken.
    ///
    /// **An abort lands at an await that is pending, and at nothing else.**
    /// A task inside a poll -- blocked in a storage call, say -- is only
    /// flagged, and runs on to its next pending await; if the runtime was
    /// shut down meanwhile, the first thing it reaches is that same timer,
    /// armed on a dead driver. So nothing in this task blocks inside a
    /// poll: the storage is asked on the blocking pool (`ask_storage`).
    pub fn take_refresh_task(&self) -> Option<JoinHandle<()>> {
        self.refresh_task.lock().take()
    }

    /// Load trackers from cache (settings)
    async fn load_from_cache(&self) {
        if let Some(ref storage) = self.storage {
            let Ok(cached) = ask_storage(storage, |storage| storage.get_cached_trackers()).await
            else {
                return;
            };
            if !cached.is_empty() {
                info!(count = cached.len(), "Loaded cached trackers from settings");
                let mut guard = self.trackers.write().await;
                *guard = cached;
            }
        }
    }

    /// Check if trackers need refresh (older than 24 hours) and refresh if needed
    async fn refresh_if_needed(&self) -> anyhow::Result<()> {
        let Some(ref storage) = self.storage else {
            // No storage, just fetch
            return self.refresh_trackers_internal(DEFAULT_TRACKERS_URL).await;
        };

        let last_updated = ask_storage(storage, |storage| storage.get_last_updated()).await?;
        // A clock before 1970 (dead RTC on Android/embedded) makes
        // duration_since fail; treat it as the epoch instead of panicking —
        // cached trackers are then reused, or fetched when none exist.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let age_secs = now - last_updated;

        // Check if trackers are fresh (less than 24 hours old)
        if age_secs < REFRESH_INTERVAL.as_secs() as i64 {
            // Ensure we have trackers loaded
            let guard = self.trackers.read().await;
            if !guard.is_empty() {
                debug!(
                    age_hours = age_secs / 3600,
                    "Trackers still fresh, skipping refresh"
                );
                return Ok(());
            }
            drop(guard);

            // Trackers empty, try to load from cache
            let cached = ask_storage(storage, |storage| storage.get_cached_trackers()).await?;
            if !cached.is_empty() {
                let mut guard = self.trackers.write().await;
                *guard = cached;
                return Ok(());
            }
        }

        // Need to refresh - fetch, rank, and persist
        let source_url = ask_storage(storage, |storage| storage.get_source_url()).await?;
        info!(url = %source_url, "Fetching and ranking trackers");

        let raw_trackers = self.fetch_trackers(&source_url).await?;
        if raw_trackers.is_empty() {
            warn!("Fetched empty tracker list");
            return Ok(());
        }

        debug!(
            count = raw_trackers.len(),
            "Fetched raw trackers, ranking by RTT"
        );

        // Rank trackers by RTT
        let ranked = TrackerProber::rank_trackers(raw_trackers).await;

        // Filter out unreachable trackers (those with max duration from failed probes)
        // Keep top 20 fastest trackers
        let top_trackers: Vec<String> = ranked.into_iter().take(20).collect();

        info!(count = top_trackers.len(), "Ranked and cached top trackers");

        // Persist to storage
        let saved = top_trackers.clone();
        ask_storage(storage, move |storage| storage.save_trackers(saved, now)).await?;

        // Update in-memory cache
        let mut guard = self.trackers.write().await;
        *guard = top_trackers;

        Ok(())
    }

    /// Internal refresh without persistence (legacy behavior)
    async fn refresh_trackers_internal(&self, url: &str) -> anyhow::Result<()> {
        let raw_trackers = self.fetch_trackers(url).await?;
        if !raw_trackers.is_empty() {
            debug!(count = raw_trackers.len(), "Fetched trackers");
            let mut guard = self.trackers.write().await;
            *guard = raw_trackers;
        }
        Ok(())
    }

    /// Fetch trackers from URL.
    ///
    /// Through [`crate::http_client_builder`], like every other HTTPS client
    /// here: the list is fetched from GitHub over TLS on every refresh.
    async fn fetch_trackers(&self, url: &str) -> anyhow::Result<Vec<String>> {
        let response = crate::http_client_builder()
            .build()?
            .get(url)
            .send()
            .await?;
        let text = response.text().await?;

        let mut trackers = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if !line.is_empty() {
                trackers.push(line.to_string());
            }
        }

        Ok(trackers)
    }

    /// Get the current list of trackers
    pub async fn get_trackers(&self) -> Vec<String> {
        let guard = self.trackers.read().await;
        if guard.is_empty() {
            // Fallback if empty (shouldn't happen if fetch works, but good to have)
            Vec::new()
        } else {
            guard.clone()
        }
    }

    /// Force refresh trackers now (useful for manual refresh via API)
    pub async fn force_refresh(&self) -> anyhow::Result<()> {
        if let Some(ref storage) = self.storage {
            // Reset last updated to force refresh
            storage.save_trackers(Vec::new(), 0);
        }
        self.refresh_if_needed().await
    }
}

/// Ask `storage` one question, on the blocking pool.
///
/// The trait is synchronous and the server's implementation blocks (a
/// `block_on` of its settings lock under `block_in_place`). Asked inside
/// the refresh task's poll, that block held the poll open, and an abort
/// cannot land inside a poll: on a server stopped as it started, the task
/// was still in it when the runtime shut down, then went on to its
/// interval and panicked on the dead driver ("A Tokio 1.x context was
/// found, but it is being shutdown"). Awaited here, the task is parked on
/// a join handle while the storage answers, and an abort ends it there.
/// `Err` when the question panicked or the runtime is going away.
async fn ask_storage<T: Send + 'static>(
    storage: &Arc<dyn TrackerStorage>,
    question: impl FnOnce(&dyn TrackerStorage) -> T + Send + 'static,
) -> anyhow::Result<T> {
    let storage = storage.clone();
    Ok(tokio::task::spawn_blocking(move || question(storage.as_ref())).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Storage whose cached list is fresh, so a refresh loop that does get
    /// polled finds nothing to fetch: no test here may reach the network.
    struct FreshCache;

    impl TrackerStorage for FreshCache {
        fn get_cached_trackers(&self) -> Vec<String> {
            vec!["udp://tracker.invalid:6969/announce".to_string()]
        }

        fn get_last_updated(&self) -> i64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        }

        fn get_source_url(&self) -> String {
            unreachable!("a fresh cache is never refetched")
        }

        fn save_trackers(&self, _trackers: Vec<String>, _timestamp: i64) {}
    }

    /// Which of the storage's answers [`BlockingStorage`] holds up.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Question {
        /// The first read of the cached list, from `load_from_cache`.
        CachedList,
        /// The second, from `refresh_if_needed` on a fresh but empty list.
        CachedListAgain,
        LastUpdated,
        SourceUrl,
    }

    /// Storage that blocks its thread on one answer until the test lets it
    /// go: the shape of the server's bridge, which answers from a
    /// `block_on` of its settings lock.
    ///
    /// The cached list reads empty the first time and full the second, and
    /// the list is fresh unless the source URL is the question -- so each
    /// question is reached, and none of them leads to a fetch except the
    /// URL's, whose answer is not a URL at all.
    struct BlockingStorage {
        blocks: Question,
        cached_reads: std::sync::atomic::AtomicUsize,
        entered: std::sync::mpsc::SyncSender<()>,
        release: parking_lot::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl BlockingStorage {
        fn answer(&self, question: Question) {
            if question == self.blocks {
                let _ = self.entered.try_send(());
                let _ = self.release.lock().recv();
            }
        }
    }

    impl TrackerStorage for BlockingStorage {
        fn get_cached_trackers(&self) -> Vec<String> {
            let first = self
                .cached_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0;
            if first {
                self.answer(Question::CachedList);
                Vec::new()
            } else {
                self.answer(Question::CachedListAgain);
                FreshCache.get_cached_trackers()
            }
        }

        fn get_last_updated(&self) -> i64 {
            self.answer(Question::LastUpdated);
            if self.blocks == Question::SourceUrl {
                0
            } else {
                FreshCache.get_last_updated()
            }
        }

        fn get_source_url(&self) -> String {
            self.answer(Question::SourceUrl);
            "not a url".to_string()
        }

        fn save_trackers(&self, _trackers: Vec<String>, _timestamp: i64) {}
    }

    /// An abort ends the refresh loop while the storage is still answering,
    /// whichever answer it is.
    ///
    /// Asked inside the task's poll, a storage that blocks held the poll
    /// open, and an abort only flags a task that is inside one: the loop ran
    /// on until the storage let go. On a server stopped as it started, the
    /// runtime shut down in that gap and the loop's next step armed its
    /// interval on the dead driver -- the "A Tokio 1.x context was found,
    /// but it is being shutdown" panic. A question asked without
    /// `ask_storage` times out here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_abort_lands_while_the_storage_is_still_answering() {
        for blocks in [
            Question::CachedList,
            Question::CachedListAgain,
            Question::LastUpdated,
            Question::SourceUrl,
        ] {
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let manager = TrackerManager::new_with_storage(Arc::new(BlockingStorage {
                blocks,
                cached_reads: Default::default(),
                entered: entered_tx,
                release: parking_lot::Mutex::new(release_rx),
            }));
            let task = manager
                .take_refresh_task()
                .expect("the constructor hands its refresh loop over");

            tokio::task::spawn_blocking(move || entered_rx.recv())
                .await
                .expect("the wait for the storage call did not panic")
                .unwrap_or_else(|_| panic!("the refresh loop never asked {blocks:?}"));
            task.abort();
            // A bound on a failure, not a wait on a success: a task parked
            // on an await is cancelled by the first worker to pick it up.
            let ended = tokio::time::timeout(Duration::from_secs(10), task).await;
            let _ = release_tx.send(());

            let error = ended
                .unwrap_or_else(|_| {
                    panic!("the abort must land while {blocks:?} is still being answered")
                })
                .expect_err("an endless loop cannot have finished on its own");
            assert!(
                error.is_cancelled(),
                "{blocks:?}: ended by {error}, not cancellation"
            );
        }
    }

    /// The refresh loop must belong to someone who can stop it: detached, it
    /// arms its next interval while the runtime is shutting down and panics.
    /// Aborting the task it hands over must end it by cancellation.
    #[tokio::test]
    async fn the_refresh_loop_is_handed_over_and_stops_when_aborted() {
        let manager = TrackerManager::new_with_storage(Arc::new(FreshCache));

        let task = manager
            .take_refresh_task()
            .expect("the constructor must hand its refresh loop over, not detach it");
        task.abort();

        let error = task
            .await
            .expect_err("an endless loop cannot have finished on its own");
        assert!(error.is_cancelled(), "ended by {error}, not cancellation");
        assert!(
            manager.take_refresh_task().is_none(),
            "the task has one owner; a second taker must not get it too"
        );
    }
}
