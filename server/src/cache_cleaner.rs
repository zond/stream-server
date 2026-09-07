use crate::state::AppState;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

/// How long after the first filesystem event the cleaner waits before it
/// walks the cache.
const CLEAN_DEBOUNCE: Duration = Duration::from_secs(60);

/// How often the cleaner looks for a torrent the backend stopped because the
/// volume ran out of space.
///
/// It cannot wait for [`CLEAN_DEBOUNCE`]: a stopped torrent writes nothing, so
/// there is no filesystem event left to arm the debounce with, and the hourly
/// fallback is ninety minutes of black screen. The check itself is one lock
/// read per live engine and no I/O (`EngineFS::out_of_space_torrents`), which
/// is what makes a short interval affordable. What follows a positive answer
/// is a full cache walk, which is not -- so [`DiskFullRecovery`] runs it once
/// per situation rather than once per tick.
const DISK_FULL_POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Fallback sweep for a cache nothing is writing to. Long on purpose: with
/// the debounce bounded, anything that touches the cache schedules a clean
/// within [`CLEAN_DEBOUNCE`], so this only has to catch a server that has
/// been idle since startup and one whose watch could not be established.
const CLEAN_FALLBACK_INTERVAL: Duration = Duration::from_secs(3600);

/// Whether a debounced clean is already due.
///
/// The first event after a quiet period arms the timer; later events do
/// **not** push it back. Re-arming on every event meant a torrent that
/// keeps writing -- which is what a torrent does for as long as anyone is
/// watching -- deferred the clean indefinitely, leaving only the hourly
/// fallback, so the cache limit went unenforced for exactly as long as the
/// device was in use. Bounding the wait at one debounce interval is what
/// makes the limit hold on a phone.
#[derive(Debug, Default, PartialEq, Eq)]
struct CleanSchedule {
    armed: bool,
}

impl CleanSchedule {
    /// Records a filesystem event. `true` when it armed the timer, i.e. the
    /// caller should start a [`CLEAN_DEBOUNCE`] sleep; `false` when a clean
    /// is already due and the deadline must be left where it is.
    fn on_event(&mut self) -> bool {
        !std::mem::replace(&mut self.armed, true)
    }

    /// The debounced clean has fired; the next event arms a fresh one.
    fn on_clean(&mut self) {
        self.armed = false;
    }
}

/// Free space on the cache's volume that the cleaner keeps the torrent
/// cache out of.
///
/// Not a fresh guess: `routes::stream::ensure_download_disk_ready` already
/// refuses to stream to disk unless this much is free on top of what the
/// request needs -- a failed check runs one pass of this cleaner and, if the
/// disk is still short, answers the stream `507 Insufficient Storage`. (It
/// used to say it "degraded the request to memory-only"; there is no
/// memory-only engine, and the fallback re-selected the same disk-backed
/// one.) Below this line the server has therefore already decided the disk
/// is unusable, so it is exactly the line the cleaner must keep the cache out
/// of -- one constant, so the check that gives up on the disk and the
/// cleaner whose job is to stop it coming to that cannot drift apart. (One
/// constant, two readings: the cleaner asks `fs4::available_space`, which
/// is `statvfs` on the path, while `ensure_download_disk_ready` matches the
/// path against `sysinfo`'s mount list behind a 3-second cache. Same
/// question, different syscall.)
///
/// **It is a target, not a guarantee, and nothing here can make it one.**
/// The cleaner only deletes; it cannot throttle a writer. librqbit writes
/// the file it wants straight through this line to ENOSPC between passes,
/// which is the whole reason [`recover_out_of_space_torrents`] exists, and
/// on the device that prompted all this Available went to nothing rather
/// than stopping at 512 MiB. Offline downloads are a third writer with a
/// margin of its own -- `enginefs::PIN_FREE_SPACE_MARGIN`, 500 MiB,
/// checked once when a pin is accepted and against a directory this
/// cleaner never walks -- so a pin can settle the volume below this line
/// by design, and several accepted together can take it to zero. What this
/// number does hold is where the *cache* is evicted back to once a pass
/// runs.
pub(crate) const CACHE_FREE_SPACE_FLOOR: u64 = 512 * 1024 * 1024;

/// What caps the cache on one run: what the operator configured and what the
/// filesystem can still give.
///
/// `settings.cacheSize` on its own is `u64::MAX` unless somebody set a number
/// (`routes::system::cache_size_bytes`), so on a 4 GB television the cleaner
/// evicted nothing and librqbit wrote until the filesystem refused -- and
/// that refusal arrives as a fatal torrent error, mid-film. The enforced cap
/// is therefore the smaller of the two, which is a number even when
/// `cacheSize` is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheLimit {
    /// `settings.cacheSize` in bytes: `u64::MAX` when unset, and 0 for the
    /// "no limit" the eviction rule has always read it as.
    configured: u64,
    /// Bytes the volume holding the cache will still give an unprivileged
    /// writer, or `None` when it could not be read.
    available: Option<u64>,
}

impl CacheLimit {
    /// A limit with no filesystem reading behind it: what the cleaner
    /// enforced before it had one, and what it falls back to when the volume
    /// cannot be probed. Written that way only by the tests -- `cache_roots`
    /// always carries whatever the probe returned, `None` included.
    #[cfg(test)]
    const fn configured(configured: u64) -> Self {
        Self {
            configured,
            available: None,
        }
    }

    /// The cap to enforce against `occupied` bytes of cache, or `None` for no
    /// cap at all.
    ///
    /// `occupied + available` is what the volume would offer if the cache
    /// were empty, so holding [`CACHE_FREE_SPACE_FLOOR`] of that back leaves
    /// the most the cache may occupy without the free space crossing the
    /// floor. Saturating throughout: a volume already under the floor yields
    /// a cap below current occupancy, which is exactly the case where
    /// something has to be evicted -- and where an unsaturated subtraction
    /// would have wrapped to a cap of "everything".
    fn effective(&self, occupied: u64) -> Option<u64> {
        let configured = (self.configured != 0).then_some(self.configured);
        let from_disk = self.available.map(|available| {
            occupied
                .saturating_add(available)
                .saturating_sub(CACHE_FREE_SPACE_FLOOR)
        });
        match (configured, from_disk) {
            (Some(configured), Some(from_disk)) => Some(configured.min(from_disk)),
            (Some(only), None) | (None, Some(only)) => Some(only),
            (None, None) => None,
        }
    }

    /// Whether the filesystem, rather than the operator, is what caps the
    /// cache at `occupied` bytes -- the fact worth a log line, since it is
    /// the device overruling a setting.
    fn disk_bound(&self, occupied: u64) -> bool {
        match (self.effective(occupied), self.configured) {
            (Some(effective), 0) => effective < u64::MAX,
            (Some(effective), configured) => effective < configured,
            (None, _) => false,
        }
    }
}

/// Bytes the volume holding `path` will still give an unprivileged writer, or
/// `None` when that cannot be read.
///
/// `fs4::available_space` -> `rustix::fs::statvfs` -> `f_frsize * f_bavail`,
/// the "Available" column of `df` (which excludes the blocks reserved for
/// root, so it is what this process may actually write). One syscall against
/// the path itself, no mount table to parse. rustix reaches it two ways and
/// both are shipped here: on Linux its `linux_raw` backend issues the
/// `statfs` syscall and converts, and on Android its build script picks the
/// `libc` backend, which calls bionic's `statvfs` -- verified by building
/// this call for `aarch64-linux-android` and by running a bionic-linked
/// `statvfs` probe against a real directory, which agreed with glibc and with
/// `df` to the block.
///
/// Failure -- a filesystem that refuses the call, a path on no volume the OS
/// can name -- is `None`, never 0: an unreadable volume must not be read as
/// "no room" and evict a healthy cache. The configured `cacheSize` then
/// stands alone, exactly as it did before any of this existed. Which paths
/// fail is the platform's business, not this function's: `statvfs` wants
/// the path to exist, so on Unix a root not yet created is unreadable, while
/// Windows resolves the volume from the drive letter (`GetVolumePathNameW`)
/// and answers for a directory nothing has made yet. The tests therefore
/// exercise the `None` through the probe seam of [`budgets_by_volume`], not
/// by finding a path the OS will refuse.
fn available_space(path: &std::path::Path) -> Option<u64> {
    match fs4::available_space(path) {
        Ok(available) => Some(available),
        Err(e) => {
            debug!(
                path = %path.display(),
                error = %e,
                "could not read the cache volume's free space; enforcing the configured cacheSize alone"
            );
            None
        }
    }
}

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

        // Fallback poll for a cache nothing is writing to (the first tick
        // fires immediately, so startup always gets a sweep).
        let mut poll_interval = tokio::time::interval(CLEAN_FALLBACK_INTERVAL);

        // Looks for torrents a full disk stopped; the first tick fires
        // immediately, so a server started while the disk is full recovers
        // them rather than waiting out the interval first.
        let mut disk_full_poll = tokio::time::interval(DISK_FULL_POLL_INTERVAL);

        let mut schedule = CleanSchedule::default();
        let mut disk_full_recovery = DiskFullRecovery::default();
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
                    // Arm the debounce, but never push an armed one back:
                    // see `CleanSchedule`.
                    if schedule.on_event() {
                        active_cleaning_timer = Box::pin(tokio::time::sleep(CLEAN_DEBOUNCE));
                    }
                }

                // 3. A torrent the backend stopped for want of disk space
                _ = disk_full_poll.tick() => {
                    recover_out_of_space_torrents(&state, &mut disk_full_recovery).await;
                }

                // 4. Debounce Timer Fired
                _ = &mut active_cleaning_timer => {
                    debug!("Debounced cache clean trigger");
                    schedule.on_clean();
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

/// A full disk is a signal to clean, not to stop.
///
/// librqbit treats a write that hits ENOSPC as fatal: it stops the torrent and
/// leaves it in an error state. That is what killed a film ninety minutes in
/// against a swarm of 459 seeds -- nothing was wrong with the swarm, the
/// device was simply out of room. So when a torrent is stopped for that
/// reason, evict what the cleaner would have evicted anyway and put it back to
/// work.
///
/// The restart is conditional on the clean actually reclaiming something. A
/// torrent restarted onto a disk that is still full errors again within
/// seconds, and that is a loop rather than a recovery; when there is nothing
/// left to evict, [`EvictionReport::shortfall_message`] has already said what
/// protection is holding, and the torrent stays stopped where a client can
/// report it honestly.
/// What the disk-full poll has already tried and failed to make room for.
///
/// A stopped torrent stays stopped and stays in the backend's error state,
/// so `out_of_space_torrents` answers with the same hash on every tick.
/// When the clean that follows frees nothing there is nothing to be done
/// about it and nothing new to say -- yet the arm walked the whole cache
/// tree, a `metadata()` per file, and emitted two WARN lines, four times a
/// minute. For a *pinned* offline download, which the idle sweeper never
/// removes, that never ends: some five and a half thousand WARN pairs a
/// day into the append-only log archive, on a device that is out of disk.
/// The DHT taught this repo the same lesson -- an unreachable DHT is
/// reported once, not once per retry.
///
/// So a hash the cleaner could not make room for is remembered here and
/// the tick does nothing at all until the stopped set changes. What
/// re-arms it is the torrent leaving the error state (restarted, removed
/// or swept) or a later clean actually reclaiming something: both are
/// changes to the situation the refusal described, and the next failure
/// then gets a fresh pass and a fresh line.
#[derive(Debug, Default, PartialEq, Eq)]
struct DiskFullRecovery {
    exhausted: HashSet<String>,
}

impl DiskFullRecovery {
    /// Whether `stopped` holds a torrent this has not already given up on.
    /// Forgets anything no longer stopped as it goes, so a torrent that
    /// recovers and later fills the disk again is a fresh case.
    fn has_new_work(&mut self, stopped: &HashSet<String>) -> bool {
        self.exhausted.retain(|hash| stopped.contains(hash));
        stopped.len() > self.exhausted.len()
    }

    /// Nothing could be evicted for these: said once, then quiet.
    fn give_up(&mut self, stopped: &HashSet<String>) {
        self.exhausted.extend(stopped.iter().cloned());
    }

    /// A pass reclaimed space, so every earlier refusal described a device
    /// that no longer exists.
    fn room_was_made(&mut self) {
        self.exhausted.clear();
    }
}

async fn recover_out_of_space_torrents(state: &AppState, recovery: &mut DiskFullRecovery) {
    // The stream and download engines are often the same `Arc`; asking one
    // twice would restart a torrent that is already live again and log the
    // backend's complaint about it.
    let engines: Vec<Arc<enginefs::EngineFS>> =
        if Arc::ptr_eq(&state.engine, &state.download_engine) {
            vec![state.engine.clone()]
        } else {
            vec![state.engine.clone(), state.download_engine.clone()]
        };

    let mut stopped = Vec::new();
    for engine in &engines {
        for info_hash in engine.out_of_space_torrents().await {
            stopped.push((engine.clone(), info_hash));
        }
    }
    if stopped.is_empty() {
        recovery.room_was_made();
        return;
    }
    // Every tick after a pass that could not help would walk the whole
    // cache and say the same two things again; see [`DiskFullRecovery`].
    let hashes: HashSet<String> = stopped.iter().map(|(_, hash)| hash.clone()).collect();
    if !recovery.has_new_work(&hashes) {
        return;
    }

    warn!(
        torrents = stopped.len(),
        "a torrent stopped for want of disk space; cleaning the cache to make room"
    );
    let report = match clean_cache(state).await {
        Ok(report) => report,
        Err(e) => {
            error!("Cache cleaner error: {}", e);
            return;
        }
    };
    if !report.made_room() {
        recovery.give_up(&hashes);
        warn!(
            torrents = stopped.len(),
            "nothing could be evicted, so the stopped torrents stay stopped rather than failing again; \
             not reported again until the situation changes"
        );
        return;
    }
    recovery.room_was_made();

    for (engine, info_hash) in stopped {
        match engine.restart_after_error(&info_hash).await {
            Ok(true) => info!(
                info_hash = %info_hash,
                freed = report.freed,
                "restarted a torrent a full disk had stopped"
            ),
            Ok(false) => debug!(
                info_hash = %info_hash,
                "the torrent was gone by the time space had been reclaimed"
            ),
            Err(e) => warn!(
                info_hash = %info_hash,
                error = %format!("{e:#}"),
                "could not restart a torrent a full disk had stopped"
            ),
        }
    }
}

/// The budgets, protections and keep-set both [`clean_cache`] and [`usage`]
/// need, gathered once so the two read `AppState` the same way and can
/// never disagree about what the cache *is*.
struct CacheRoots {
    /// One entry per volume the walked roots live on. Usually one.
    budgets: Vec<CacheBudget>,
    protected_paths: HashSet<std::path::PathBuf>,
    /// Directories the cleaner was handed and may therefore never delete,
    /// however empty eviction leaves them -- see [`remove_empty_parents`].
    /// Every root *before* [`outermost`] collapsed them, which is the point:
    /// a `downloadsDir` inside a cache root is no longer a walked root, and
    /// the walked root is no longer a sufficient stop condition for it.
    keep_dirs: HashSet<std::path::PathBuf>,
    /// Every root any budget walks, so each walk can stop where another
    /// one's root begins -- see [`evict`]. Only roots on *different* volumes
    /// are ever both in here and nested, since [`budgets_by_volume`]
    /// collapses the ones that share a volume into a single walk.
    boundaries: HashSet<std::path::PathBuf>,
}

/// The walked roots on one volume, and the cap they share.
///
/// A cap is a statement about a *volume*: [`CacheLimit::effective`] reads that
/// volume's free space and holds [`CACHE_FREE_SPACE_FLOOR`] of it back. So the
/// roots on one volume can be weighed against one number and the roots on
/// another cannot. What this replaced summed every root's occupancy into a
/// single total and weighed it against the *tightest* free-space figure of all
/// of them -- correct while there is one volume, which is the phone this was
/// written for, and wrong on a desktop with `downloadsDir` on an external
/// drive. There the min is the external drive's, the sum carries the system
/// disk's cache, and every pass evicts a film nobody is short of space for to
/// answer a shortage on a drive it can reclaim nothing from: what fills a
/// download drive is pinned, and protection keeps it.
///
/// `settings.cacheSize` becomes a per-volume allowance by the same argument.
/// It was never defined across volumes -- there has only ever been one budget
/// -- and of the two readings available, this is the one that cannot have a
/// full downloads volume evict a healthy cache root to nothing.
struct CacheBudget {
    roots: Vec<std::path::PathBuf>,
    limit: CacheLimit,
}

/// Group `roots` by the volume they are on, and give each group a cap of its
/// own: `configured` (the operator's `cacheSize`, per volume) against the
/// tightest free-space reading among that group's roots -- which is a real
/// minimum now, since roots that share a volume share its free space and only
/// differ by which of them could be probed at all.
///
/// Roots whose volume cannot be identified -- one that does not exist yet, a
/// filesystem that will not answer -- share a single group. That is what the
/// cleaner did with every root before, so it is the conservative answer for
/// the paths where the question has no answer, rather than a new behaviour.
///
/// [`outermost`] collapses each group *after* it is grouped, never before,
/// and that order is the whole point. The collapse throws away a root that
/// sits inside another, `prepare_downloads_dir` deliberately permits a
/// `downloadsDir` below a cache root, and `WalkDir` crosses mount points --
/// so a downloads dir that is an external drive mounted under the cache root
/// used to disappear into it and be walked, counted and capped as part of a
/// volume its bytes are not on. A cap that is a statement about one volume
/// cannot be built out of roots that have already stopped being separable.
/// Grouped first, roots that really do share a volume still collapse to one
/// walk, and roots that do not stay two budgets -- which is why every walk
/// has to stop where another budget's root begins (see [`evict`]).
///
/// `volume_of` and `available` are parameters so a test can describe two
/// volumes without needing two.
fn budgets_by_volume(
    roots: &[std::path::PathBuf],
    configured: u64,
    volume_of: impl Fn(&std::path::Path) -> Option<u64>,
    available: impl Fn(&std::path::Path) -> Option<u64>,
) -> Vec<CacheBudget> {
    let mut by_volume: std::collections::BTreeMap<Option<u64>, Vec<std::path::PathBuf>> =
        std::collections::BTreeMap::new();
    for root in roots {
        by_volume
            .entry(volume_of(root))
            .or_default()
            .push(root.clone());
    }
    by_volume
        .into_values()
        .map(|roots| {
            let available = roots.iter().filter_map(|root| available(root)).min();
            CacheBudget {
                roots: outermost(&roots),
                limit: CacheLimit {
                    configured,
                    available,
                },
            }
        })
        .collect()
}

async fn cache_roots(state: &AppState) -> CacheRoots {
    let settings = state.settings.read().await;
    let limit = crate::routes::system::cache_size_bytes(settings.cache_size);
    drop(settings); // Release lock

    // Every root the cleaner walks: the two engines' cache roots and the
    // downloads dir, which is now one of them.
    //
    // It was not always. The downloads dir used to be pruned out of the walk
    // on the grounds that what is there is an offline download the user asked
    // for, not cache. That was right while a download was a whole file the
    // client could play from disk. It is not right any more: downloads are
    // stored as pieces like everything else, an old plain-file download under
    // the downloads dir is neither migrated nor read, and left unwalked it
    // would be orphaned *and* immortal -- bytes nothing can play and nothing
    // can reclaim, on the device where space runs out. Protection, not
    // exclusion, is what keeps a live or pinned download safe now, and
    // `protected_paths` covers dormant pins for exactly that reason.
    let mut download_dirs = vec![
        state.engine.download_dir.clone(),
        state.download_engine.download_dir.clone(),
    ];
    download_dirs.extend(
        [
            state.engine.downloads_dir(),
            state.download_engine.downloads_dir(),
        ]
        .into_iter()
        .flatten(),
    );
    download_dirs.sort();
    download_dirs.dedup();
    // Every directory the cleaner was *given*, kept before the collapse below
    // throws the inner ones away: these are configuration -- the engines'
    // cache roots and the resolved `downloadsDir` -- and eviction emptying one
    // must not take the directory with it.
    let keep_dirs: HashSet<_> = download_dirs.iter().cloned().collect();

    // Everything a live engine writes, at the paths the backend reports (a
    // pinned engine stays live, so its data is protected for as long as
    // the pin holds).
    let mut protected_paths: HashSet<_> =
        state.engine.protected_paths().await.into_iter().collect();
    protected_paths.extend(state.download_engine.protected_paths().await);

    // One budget per volume, each collapsed to its outermost roots. The two
    // engines normally share one directory and the downloads dir is under it,
    // so this is normally a single budget over a single walked root and the
    // whole of what follows is what it always was.
    let budgets = budgets_by_volume(
        &download_dirs,
        limit,
        |path| enginefs::volume_id(path).ok(),
        available_space,
    );
    // Where it is not -- a downloads dir on a drive mounted under the cache
    // root -- the two survive as separate roots, and neither walk may wander
    // into the other: `WalkDir` would happily cross the mount and count the
    // drive's bytes against the cache root's cap, which is the reading the
    // per-volume budgets exist to stop.
    let boundaries: HashSet<_> = budgets
        .iter()
        .flat_map(|budget| budget.roots.iter().cloned())
        .collect();

    CacheRoots {
        budgets,
        protected_paths,
        keep_dirs,
        boundaries,
    }
}

/// Run one eviction pass now and report what it found and freed. Shared by
/// the background scheduler in [`start`] and, through
/// `routes::cache::clean_cache_now`, `ServerHandle::clean_cache_now` and
/// `POST /cache/clean` -- the on-demand path takes exactly this function,
/// so it can never diverge from the scheduled sweep's protections.
pub(crate) async fn clean_cache(state: &AppState) -> anyhow::Result<EvictionReport> {
    let roots = cache_roots(state).await;
    let mut reports = Vec::with_capacity(roots.budgets.len());
    for budget in &roots.budgets {
        // A volume whose roots do not exist yet has nothing to walk, but its
        // cap is still part of what this run enforced.
        if budget
            .roots
            .iter()
            .all(|download_dir| !download_dir.exists())
        {
            reports.push(EvictionReport {
                limit: budget.limit.effective(0),
                ..EvictionReport::default()
            });
            continue;
        }
        reports.push(
            evict(
                &budget.roots,
                &roots.protected_paths,
                &roots.keep_dirs,
                &roots.boundaries,
                budget.limit,
            )
            .await?,
        );
    }
    let report = reports
        .into_iter()
        .reduce(EvictionReport::combined_with)
        .unwrap_or_default();
    state.last_eviction.record(&report);
    Ok(report)
}

/// The report of the last pass, kept for a reader that wants to know what
/// the cache occupies without walking it (see [`LastEviction::get`]).
///
/// The memory sampler is that reader. It used to walk the whole download
/// dir itself, synchronously, on the runtime, every thirty seconds -- twice
/// the cleaner's debounce, and a hundred and twenty times its hourly
/// fallback on an idle device -- for two numbers it then logged once a
/// minute at most. The cleaner has just counted the same tree: while
/// something is writing, a minute ago; while nothing is, whenever the tree
/// last changed, which is when the figure last could have. So the sampler
/// reads this, with its age, and walks nothing.
#[derive(Default)]
pub struct LastEviction(std::sync::Mutex<Option<(std::time::Instant, EvictionReport)>>);

impl LastEviction {
    pub(crate) fn record(&self, report: &EvictionReport) {
        if let Ok(mut last) = self.0.lock() {
            *last = Some((std::time::Instant::now(), report.clone()));
        }
    }

    /// The last pass's report and how long ago that pass finished, or
    /// `None` before the first pass has run (the first fallback tick fires at
    /// startup, so that is a few seconds at most).
    pub fn get(&self) -> Option<(Duration, EvictionReport)> {
        self.0
            .lock()
            .ok()?
            .as_ref()
            .map(|(at, report)| (at.elapsed(), report.clone()))
    }
}

/// What the cache currently occupies against its configured limit
/// ([`CacheUsage`]), without touching the filesystem: the same walk
/// [`evict`] does -- same session-artifact and downloads-dir exclusions,
/// same occupancy accounting ([`occupied_bytes`]), same protection rule --
/// but nothing is aged out or evicted. Shared by `routes::cache::cache_usage`
/// (`ServerHandle::cache_usage` and `GET /cache.json`).
pub(crate) async fn usage(state: &AppState) -> CacheUsage {
    let roots = cache_roots(state).await;
    // The walk is synchronous filesystem work -- see [`evict`] for why it is
    // off the runtime -- and a `GET /cache.json` is a request a worker is
    // serving.
    tokio::task::spawn_blocking(move || {
        roots
            .budgets
            .iter()
            .map(|budget| {
                scan_usage(
                    &budget.roots,
                    &roots.protected_paths,
                    &roots.boundaries,
                    budget.limit,
                )
            })
            .reduce(CacheUsage::combined_with)
            .unwrap_or_default()
    })
    .await
    .unwrap_or_else(|error| {
        // A panic in the walk, in a debug build; the release profile aborts
        // the process instead. Nothing to report but that nothing was read.
        error!("the cache usage scan did not finish: {error}");
        CacheUsage::default()
    })
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

/// What the cache currently occupies against its configured limit
/// ([`usage`]), in the same occupancy accounting eviction uses
/// ([`occupied_bytes`]). `serde`-serializable so it crosses the `GET
/// /cache.json` / `ServerHandle::cache_usage` boundary as is.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheUsage {
    /// Occupancy of the walked cache roots right now.
    pub total_bytes: u64,
    /// The limit actually enforced, in the same accounting: the smaller of
    /// `settings.cacheSize` and what the volume can give while keeping
    /// [`CACHE_FREE_SPACE_FLOOR`] free. `None` only when neither caps
    /// anything -- `cacheSize` unlimited (JSON `null`) *and* the volume's
    /// free space unreadable.
    pub limit_bytes: Option<u64>,
    /// How much of `total_bytes` a clean pass may never touch right now: a
    /// live engine is writing it, or a pin keeps it (live or dormant). When
    /// this equals `total_bytes` and the cache is still over `limit_bytes`,
    /// nothing is evictable -- cleaning cannot help until playback stops or
    /// something is unpinned.
    pub protected_bytes: u64,
    /// How many files that is.
    pub protected_files: usize,
}

impl CacheUsage {
    /// Fold another volume's scan into this one, for the single answer
    /// `GET /cache.json` gives. See [`EvictionReport::combined_with`], which
    /// folds the same way and for the same reasons -- `limit_bytes`
    /// especially: an uncapped volume leaves the run as a whole uncapped,
    /// because its occupancy is in `total_bytes` and nothing bounds it.
    ///
    /// Note what that costs, and why it costs nothing here: summed
    /// `total_bytes` against summed `limit_bytes` cannot say whether a
    /// *volume* is over, because a roomy one's slack pays off a full one's
    /// shortfall. Usage asks no such question -- it reports numbers and
    /// leaves the reading to the client. Anything added here that does ask it
    /// needs a per-volume figure carried through the fold, the way
    /// [`EvictionReport::over_limit`] is.
    fn combined_with(self, other: Self) -> Self {
        Self {
            total_bytes: self.total_bytes.saturating_add(other.total_bytes),
            limit_bytes: self
                .limit_bytes
                .zip(other.limit_bytes)
                .map(|(a, b)| a.saturating_add(b)),
            protected_bytes: self.protected_bytes.saturating_add(other.protected_bytes),
            protected_files: self.protected_files + other.protected_files,
        }
    }
}

/// The read-only half of [`evict`]'s walk: every payload file's occupancy
/// and protection status, with nothing aged out or deleted. Mirrors
/// `evict`'s session-artifact exclusion and protection rule exactly, so
/// `usage` and a `clean_cache` run right after it agree about what the
/// cache contains.
fn scan_usage(
    download_dirs: &[std::path::PathBuf],
    protected_paths: &HashSet<std::path::PathBuf>,
    boundaries: &HashSet<std::path::PathBuf>,
    limit: CacheLimit,
) -> CacheUsage {
    #[cfg(test)]
    WALKED_ON_THIS_THREAD.set(true);
    let mut total = 0u64;
    let mut protected = 0u64;
    let mut protected_files = 0usize;

    for download_dir in download_dirs {
        if !download_dir.exists() {
            continue;
        }

        let mut entries = walk_within(download_dir, boundaries);

        loop {
            match entries.next() {
                Some(Ok(entry)) => {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let path = entry.path();
                    if is_session_artifact(path, download_dir) {
                        continue;
                    }
                    let Ok(metadata) = entry.metadata() else {
                        continue;
                    };
                    let size = occupied_bytes(&metadata);
                    total += size;
                    if is_path_protected(path, protected_paths) {
                        protected += size;
                        protected_files += 1;
                    }
                }
                Some(Err(e)) => {
                    debug!("Error walking directory: {}", e);
                }
                None => break,
            }
        }
    }

    CacheUsage {
        total_bytes: total,
        limit_bytes: limit.effective(total).filter(|limit| *limit != u64::MAX),
        protected_bytes: protected,
        protected_files,
    }
}

/// What one [`evict`] run found and did, in occupancy bytes
/// ([`occupied_bytes`]) throughout. `serde`-serializable so it crosses the
/// `POST /cache/clean` / `ServerHandle::clean_cache_now` boundary as is.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvictionReport {
    /// Occupancy of the walked roots once eviction finished.
    pub total: u64,
    /// How much of `total` eviction may never touch: files a live engine
    /// reports (a pinned download's engine is never swept, so its files are
    /// in here for as long as the pin holds), plus the download folder of
    /// every dormant pin, which has no engine to report anything.
    pub protected: u64,
    /// How many files that is.
    pub protected_files: usize,
    /// Occupancy this run reclaimed, by either rule: the 30-day sweep and
    /// the size rule both count here. They were the size rule's alone until
    /// [`Self::made_room`] started deciding whether a torrent a full disk
    /// stopped goes back to work -- a pass that aged out a stale film has
    /// made room for it, and reporting 0 left the torrent stopped on a
    /// device that had just gained a gigabyte.
    pub freed: u64,
    /// How many files that took.
    pub deleted: usize,
    /// The limit this run enforced: the smaller of `settings.cacheSize` and
    /// what the volume could give while keeping [`CACHE_FREE_SPACE_FLOOR`]
    /// free, so on a device with no `cacheSize` set this is still a number.
    /// `None` only when neither caps anything -- `cacheSize` unlimited *and*
    /// the volume's free space unreadable, matching
    /// [`CacheUsage::limit_bytes`].
    ///
    /// Not a `u64` with 0 for "none": a cap of exactly 0 is reachable -- any
    /// volume whose occupancy plus free space is under the floor gets one --
    /// and it is the tightest cap there is, the opposite of no cap. Read as
    /// the sentinel it silenced [`Self::shortfall_message`] on the one
    /// device that needed it and told a client the cache was unlimited.
    pub limit: Option<u64>,
    /// How far over its cap this run ended, and the only thing
    /// [`Self::shortfall_message`] reads: `total - limit` for one volume's
    /// run, and the *sum* of that across the volumes that are over once runs
    /// are folded together.
    ///
    /// Kept rather than derived from `total` and `limit`, because after the
    /// fold neither of those describes any volume any more. Their sums say
    /// what the device holds and what it was allowed in total, which is what
    /// a client shows; they cannot say whether some volume is stuck, and
    /// deriving the answer from them let a system disk's slack pay off a
    /// download drive that was full -- one report saying the pass had got
    /// under the limit while the drive it could not write to was over its own
    /// by exactly as much as before.
    pub over_limit: u64,
}

impl EvictionReport {
    /// Fold another volume's run into this one, so a caller of
    /// `POST /cache/clean` gets one report for the pass.
    ///
    /// Sizes add. The limits add only while every volume has one: an uncapped
    /// volume's occupancy is in `total` with nothing bounding it, so an
    /// aggregate cap would be a number the run never enforced.
    ///
    /// Shortfalls add too, and separately -- that is what `over_limit` is
    /// for. Summed `total` against summed `limit` is not the question "is any
    /// volume still stuck?": a system disk with room to spare subsidises a
    /// download drive that is full, and the fold answers that the pass got
    /// under the limit while the drive nothing can write to is over its own
    /// cap by exactly as much as it was.
    fn combined_with(self, other: Self) -> Self {
        Self {
            total: self.total.saturating_add(other.total),
            protected: self.protected.saturating_add(other.protected),
            protected_files: self.protected_files + other.protected_files,
            freed: self.freed.saturating_add(other.freed),
            deleted: self.deleted + other.deleted,
            limit: self
                .limit
                .zip(other.limit)
                .map(|(a, b)| a.saturating_add(b)),
            over_limit: self.over_limit.saturating_add(other.over_limit),
        }
    }

    /// Whether this run reclaimed anything -- the condition
    /// [`recover_out_of_space_torrents`] restarts a stopped torrent on. A
    /// clean that freed nothing has not changed the device's mind, so
    /// restarting into it would only reproduce the error the torrent already
    /// has.
    ///
    /// Across volumes this is "some volume gained room", not "the volume the
    /// stopped torrent writes to did": a pass is one report, and which volume
    /// a stopped torrent's output folder is on is not something this layer is
    /// told. A restart onto a volume that is still full reproduces the ENOSPC,
    /// the torrent stops again, and `recover_out_of_space_torrents` reports it
    /// once and leaves it -- the same net that already catches a restart that
    /// was simply too early.
    pub fn made_room(&self) -> bool {
        self.freed > 0
    }

    /// The line to log when the run ended still over the limit, naming what
    /// protection kept -- "cleaned up 0 files, freed 0 bytes" on a phone
    /// that is filling up says nothing about *why*, and the why is always
    /// that the rest of the cache belongs to a live or pinned torrent.
    /// Since the downloads dir is walked too, that now includes an offline
    /// download the user has not unpinned.
    /// `None` when every volume the run covered got under its limit (or had
    /// none).
    ///
    /// It reads [`Self::over_limit`] and not `total` against `limit`, so that
    /// one volume being stuck survives being folded together with volumes
    /// that are fine -- the numbers it reports are how far over the pass
    /// ended and what it could not touch, both of which add up honestly.
    pub fn shortfall_message(&self) -> Option<String> {
        if self.over_limit == 0 {
            return None;
        }
        Some(format!(
            "Cache is {} bytes over its limit after freeing {} bytes from {} files: \
             {} bytes in {} files are protected (a live torrent is writing them, or a pinned \
             download keeps them) and cannot be evicted",
            self.over_limit, self.freed, self.deleted, self.protected, self.protected_files,
        ))
    }
}

/// Walk `download_dirs` and evict what is neither protected nor a session
/// artefact: first every file older than 30 days, then -- while the rest
/// exceeds what [`CacheLimit::effective`] allows for the occupancy found --
/// the least recently modified files.
/// Sizes are occupancy, not apparent length (see [`occupied_bytes`]).
///
/// Every root handed in is walked to the bottom, the downloads dir included
/// (`cache_roots` puts it in the list) -- to the bottom of *this volume*: a
/// walk stops where another budget's root begins ([`walk_within`]), which is
/// the only place a root can be nested inside another one by the time
/// [`budgets_by_volume`] is done. Nothing else is excluded by *where* it
/// lives; what a run may not touch is decided by `protected_paths` alone, and
/// that is the only thing between the cleaner and a download somebody is
/// watching. Callers that add a root must therefore make sure whatever must
/// survive is named there -- see `EngineFS::protected_paths`, which covers
/// live engines and the dormant pins that have no engine to speak for them.
async fn evict(
    download_dirs: &[std::path::PathBuf],
    protected_paths: &HashSet<std::path::PathBuf>,
    keep_dirs: &HashSet<std::path::PathBuf>,
    boundaries: &HashSet<std::path::PathBuf>,
    limit: CacheLimit,
) -> anyhow::Result<EvictionReport> {
    // 1. Walk. On the blocking pool, not on the worker this future is
    // running on: `walkdir` plus a `statx` per file is synchronous I/O over
    // the whole tree -- sixteen thousand files for 4 GB of proxy cache, on
    // eMMC, with a dentry cache that memory pressure keeps evicting -- and
    // the pass runs once a minute for as long as a torrent is writing. A
    // worker held for the length of that walk is a worker not serving range
    // requests; on a four-core television work-stealing hides one, on a
    // smaller runtime nothing does. The inputs are cloned for the closure
    // because the sets are borrowed from `CacheRoots` and the walk outlives
    // the borrow's poll.
    let walk = WalkInputs {
        download_dirs: download_dirs.to_vec(),
        protected_paths: protected_paths.clone(),
        boundaries: boundaries.clone(),
        max_age: Duration::from_secs(30 * 24 * 60 * 60),
        now: std::time::SystemTime::now(),
    };
    let Walked {
        files,
        aged_out,
        mut total_size,
        protected_size,
        protected_files,
    } = tokio::task::spawn_blocking(move || walk.run())
        .await
        .map_err(|error| anyhow::anyhow!("the cache walk did not finish: {error}"))?;

    // 2. Evict what the age rule found (30 days). What it reclaims counts
    // towards what the run freed, exactly as the size rule's does:
    // `EvictionReport::made_room` is what decides whether a torrent ENOSPC
    // stopped goes back to work, and a pass that took a gigabyte off the
    // disk has made room whichever rule took it.
    let mut aged_out_bytes = 0u64;
    let mut aged_out_files = 0usize;
    for (path, size) in aged_out {
        info!("File older than 30 days, deleting: {:?}", path);
        if let Err(e) = tokio::fs::remove_file(&path).await {
            error!("Failed to delete file {:?}: {}", path, e);
            // Still on the disk, so still counted against the limit.
            total_size += size;
        } else {
            aged_out_bytes += size;
            aged_out_files += 1;
            if let Some(parent) = path.parent() {
                remove_empty_parents(parent, keep_dirs).await;
            }
        }
    }

    // 3. Size-based Eviction, against the cap the device actually allows.
    // The walk above may have deleted aged-out files, so the volume now has
    // a little more room than the probe behind `limit` recorded; that only
    // makes the cap tighter than it needs to be, which errs towards cleaning.
    //
    // Whether the *device* is what caps decides how the single-file rule
    // below is read, so it is asked once, against the occupancy the walk
    // actually found.
    let disk_bound = limit.disk_bound(total_size);
    if disk_bound {
        info!(
            configured = limit.configured,
            available = ?limit.available,
            floor = CACHE_FREE_SPACE_FLOOR,
            effective = ?limit.effective(total_size),
            "the cache volume's free space, not cacheSize, is what caps the cache"
        );
    }
    let limit = limit.effective(total_size);
    let mut deleted_count = 0usize;
    let mut freed_space = 0u64;
    if let Some(limit) = limit
        && total_size > limit
    {
        info!(
            "Cache size {} exceeds limit {}. Cleaning up...",
            total_size, limit
        );

        // Oldest first: the walk sorted them.
        for (path, size, _) in files {
            if total_size <= limit {
                break;
            }

            // A file bigger than the whole cap is kept -- but only while
            // the cap is the operator's. `cacheSize` is a preference, and
            // an operator who set one smaller than the film they are
            // watching meant "do not hoard", not "re-fetch this from the
            // swarm after every pass"; that is the soft limit this rule
            // has always served.
            //
            // A cap the filesystem imposes is not soft: the bytes are not
            // there. And it is `occupied + available - floor`, so a single
            // cached film is routinely larger than the whole of it -- on
            // the device this was written for, the only evictable file was
            // 700 MB against a 463 MB cap. Skipping it freed nothing,
            // `made_room` was therefore false, and the torrent ENOSPC had
            // stopped was never restarted: the rule refused to evict
            // exactly when the disk-derived cap is the one that binds,
            // which is the whole case the cap exists for.
            if size > limit && !disk_bound {
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

                if let Some(parent) = path.parent() {
                    remove_empty_parents(parent, keep_dirs).await;
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
        freed: freed_space + aged_out_bytes,
        deleted: deleted_count + aged_out_files,
        limit,
        over_limit: limit.map_or(0, |limit| total_size.saturating_sub(limit)),
    };
    if let Some(message) = report.shortfall_message() {
        warn!("{message}");
    }

    Ok(report)
}

/// What [`evict`] hands the blocking pool: the roots to walk and the rules to
/// sort what it finds by. Owned, because the walk runs on another thread.
struct WalkInputs {
    download_dirs: Vec<std::path::PathBuf>,
    protected_paths: HashSet<std::path::PathBuf>,
    boundaries: HashSet<std::path::PathBuf>,
    /// The age rule: a file last modified longer ago than this goes.
    max_age: Duration,
    now: std::time::SystemTime,
}

/// What the walk found, sorted into what [`evict`] does with it. Sizes are
/// occupancy ([`occupied_bytes`]).
struct Walked {
    /// Evictable by the size rule, oldest modification first, with the
    /// occupancy and modification time of each. A file whose time could not
    /// be read sorts oldest -- it is counted, and the first to go.
    files: Vec<(std::path::PathBuf, u64, std::time::SystemTime)>,
    /// Past the age rule, to be deleted whatever the size rule says.
    aged_out: Vec<(std::path::PathBuf, u64)>,
    /// Occupancy of everything that stays unless the size rule takes it:
    /// `files` plus the protected. The aged-out are *not* in it -- they are
    /// as good as gone -- and `evict` adds one back if its deletion fails.
    total_size: u64,
    protected_size: u64,
    protected_files: usize,
}

impl WalkInputs {
    /// The walk itself: synchronous, the whole of the filesystem reading a
    /// pass does, and nothing else -- no deletion happens here, so the
    /// blocking thread holds no decision the async half has to wait on.
    /// Mirrors [`scan_usage`]'s exclusion and protection rules exactly.
    fn run(self) -> Walked {
        #[cfg(test)]
        WALKED_ON_THIS_THREAD.set(true);
        let mut walked = Walked {
            files: Vec::new(),
            aged_out: Vec::new(),
            total_size: 0,
            protected_size: 0,
            protected_files: 0,
        };
        for download_dir in &self.download_dirs {
            if !download_dir.exists() {
                continue;
            }
            for entry in walk_within(download_dir, &self.boundaries) {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        debug!("Error walking directory: {}", e);
                        continue;
                    }
                };
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path().to_path_buf();
                if is_session_artifact(&path, download_dir) {
                    continue;
                }
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                // Occupancy, not apparent length: librqbit pre-allocates
                // wanted files at full size.
                let size = occupied_bytes(&metadata);
                if is_path_protected(&path, &self.protected_paths) {
                    walked.total_size += size;
                    walked.protected_size += size;
                    walked.protected_files += 1;
                    continue;
                }
                let modified = metadata.modified().ok();
                let age = modified.map(|modified| {
                    self.now
                        .duration_since(modified)
                        .unwrap_or(Duration::from_secs(0))
                });
                if age.is_some_and(|age| age > self.max_age) {
                    walked.aged_out.push((path, size));
                } else {
                    walked.total_size += size;
                    walked.files.push((
                        path,
                        size,
                        modified.unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                    ));
                }
            }
        }
        // Oldest first, for the size rule.
        walked.files.sort_by_key(|(_, _, modified)| *modified);
        walked
    }
}

#[cfg(test)]
thread_local! {
    /// Whether the cache walk ran on the thread that reads this. Set by the
    /// walk, on whichever thread it runs on; read by a test on its own
    /// thread, where a current-thread runtime would have run an inline walk.
    /// A thread-local so parallel tests cannot see each other's walks.
    static WALKED_ON_THIS_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether `path` (under the session root `root`) is state the torrent
/// session or the engine keeps next to the payload rather than payload: it
/// is neither aged out nor counted against the cache limit. librqbit's
/// `session.json` and per-torrent `<info hash>.torrent` / `.bitv`
/// (fastresume bitfield) records at the top level, with their `.tmp`
/// siblings; everything under `.metadata/` and `.cache/`; the engine's
/// `pinned-downloads.json` with its `.json.tmp-*` temp files; and the two
/// DHT files, librqbit's `dht.json` routing table and our
/// `dht-bootstrap.json` bootstrap address cache. All of these change only
/// when the session does, so by mtime a stable pin set, a finished pinned
/// download or a DHT that has been up for a while would be the first
/// casualties -- and both DHT files exist precisely so a later start does
/// not have to reach the network, which evicting them defeats.
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
    if matches!(
        name,
        "session.json" | "pinned-downloads.json" | "dht.json" | "dht-bootstrap.json"
    ) {
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

/// Walk `root` to the bottom, but never down into a directory that is one of
/// `boundaries` -- the roots the other budgets walk.
///
/// `WalkDir` crosses mount points, and it has to: the cache root is one
/// filesystem and everything the cleaner is responsible for in it must be
/// reachable. But a root on another volume is another budget's, weighed
/// against another volume's free space, and a walk that descended into it
/// would count and evict its files against a cap that says nothing about the
/// disk they are on -- exactly what [`budgets_by_volume`] splits the roots up
/// to prevent. Within one volume there is nothing to stop at: those roots
/// collapsed into this one before the walk began.
///
/// The root itself is at depth 0 and is therefore never its own boundary.
fn walk_within<'a>(
    root: &std::path::Path,
    boundaries: &'a HashSet<std::path::PathBuf>,
) -> walkdir::FilterEntry<walkdir::IntoIter, impl FnMut(&walkdir::DirEntry) -> bool + 'a> {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_entry(move |entry| {
            entry.depth() == 0 || !entry.file_type().is_dir() || !boundaries.contains(entry.path())
        })
}

/// The roots that are not inside another root, component-wise. `WalkDir` has
/// no idea that two roots overlap, so walking a parent and its child would
/// count, age and evict everything under the child twice; keeping only the
/// outermost makes that one walk. Assumes `roots` is already deduplicated, so
/// two spellings of the same path do not cancel each other out.
fn outermost(roots: &[std::path::PathBuf]) -> Vec<std::path::PathBuf> {
    roots
        .iter()
        .filter(|dir| {
            !roots
                .iter()
                .any(|other| other != *dir && dir.starts_with(other))
        })
        .cloned()
        .collect()
}

/// A file is protected from eviction when its full path is in `protected` or
/// when it lives under a protected directory. Uses `Path::starts_with`, which
/// matches whole path components — so `/dl/Movie2/x.mkv` is NOT shielded by a
/// protected `/dl/Movie` entry, only a true `/dl/Movie/...` descendant is.
fn is_path_protected(path: &std::path::Path, protected: &HashSet<std::path::PathBuf>) -> bool {
    protected.contains(path) || protected.iter().any(|p| path.starts_with(p))
}

/// Prune the directories a deletion left empty, upwards, stopping at the first
/// one that is not empty -- and never at all on a directory in `keep`.
///
/// `keep` is every directory the cleaner was handed: the engines' cache roots
/// and the resolved `downloadsDir`. The walked root alone used to be the stop
/// condition, and that was right only while every configured directory was a
/// walked root. It stopped being right when [`outermost`] began collapsing a
/// `downloadsDir` under a cache root into that cache root: the walked root is
/// then the cache root, and the loop walks through the downloads dir on its way
/// up to it, deleting it the moment eviction has emptied it. That directory is
/// settings -- `routes::system::prepare_downloads_dir` resolved and validated
/// it and the engines were told about it -- and the cleaner is not the thing
/// that gets to remove it.
async fn remove_empty_parents(mut dir: &std::path::Path, keep: &HashSet<std::path::PathBuf>) {
    while !keep.contains(dir) {
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
        CACHE_FREE_SPACE_FLOOR, CacheLimit, CacheUsage, CleanSchedule, DiskFullRecovery,
        EvictionReport, LastEviction, WALKED_ON_THIS_THREAD, available_space, budgets_by_volume,
        evict, is_path_protected, is_session_artifact, occupied_bytes, outermost,
        remove_empty_parents, scan_usage,
    };
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba98";

    #[test]
    fn session_artifacts_are_recognised_at_the_top_level_only() {
        let root = Path::new("/dl");
        for name in [
            "session.json",
            "session.json.tmp",
            "pinned-downloads.json",
            "pinned-downloads.json.tmp-4242-7",
            // Both halves of "what the DHT knew last time": librqbit's
            // routing table and our bootstrap address cache. Tiny, rarely
            // rewritten, and the whole point of them is to survive.
            "dht.json",
            "dht-bootstrap.json",
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

    /// `evict` where every walked root is also a directory to keep and the
    /// only boundary: the ordinary shape, one budget with nothing collapsed
    /// into anything and no second volume to stop at. The tests about the
    /// collapse and about two volumes build their own sets and call `evict`
    /// directly.
    async fn evict_roots(
        download_dirs: &[PathBuf],
        protected_paths: &HashSet<PathBuf>,
        limit: CacheLimit,
    ) -> anyhow::Result<EvictionReport> {
        let keep: HashSet<PathBuf> = download_dirs.iter().cloned().collect();
        evict(download_dirs, protected_paths, &keep, &keep, limit).await
    }

    /// [`scan_usage`] for one budget whose roots are the only ones walked.
    fn scan_roots(
        download_dirs: &[PathBuf],
        protected_paths: &HashSet<PathBuf>,
        limit: CacheLimit,
    ) -> CacheUsage {
        let boundaries = download_dirs.iter().cloned().collect();
        scan_usage(download_dirs, protected_paths, &boundaries, limit)
    }

    /// The keep-set of a plain root: what `cache_roots` builds when no
    /// root sits inside another.
    fn keep_only(dirs: &[&Path]) -> HashSet<PathBuf> {
        dirs.iter().map(|dir| dir.to_path_buf()).collect()
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

    /// The walk is the whole of a pass's filesystem reading, and it does
    /// not run on the runtime's thread. `#[tokio::test]` is a current-thread
    /// runtime, which runs this future on the test's own thread: an inline
    /// walk would set the marker *here*, one on the blocking pool sets it on
    /// a pool thread this thread never sees.
    #[tokio::test]
    async fn the_walk_runs_off_the_runtime_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let film = root.join(HASH).join("film.mkv");
        write_aged(&film, &[0u8; 4096], Duration::ZERO);

        WALKED_ON_THIS_THREAD.set(false);
        let report = evict_roots(
            &[root.clone()],
            &HashSet::new(),
            CacheLimit::configured(u64::MAX),
        )
        .await
        .unwrap();
        assert_eq!(
            report.total,
            occupancy(&film),
            "the walk did run, and found the file"
        );
        assert!(
            !WALKED_ON_THIS_THREAD.get(),
            "the cache walk ran on the runtime's own thread"
        );
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

        evict_roots(
            std::slice::from_ref(&root),
            &HashSet::new(),
            CacheLimit::configured(0),
        )
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
        evict_roots(
            std::slice::from_ref(&root),
            &HashSet::new(),
            CacheLimit::configured(limit),
        )
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

        remove_empty_parents(&c, &keep_only(&[root.path()])).await;

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

        remove_empty_parents(&c, &keep_only(&[root.path()])).await;

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
        remove_empty_parents(&a, &keep_only(&[root.path()])).await;

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

    /// Old plain-file downloads are evictable, because nothing else will
    /// ever reclaim them.
    ///
    /// They are not migrated to the piece store and they are not read: a
    /// download is played from pieces now. Left out of the walk, as the
    /// downloads dir used to be, they would be orphaned *and* immortal on
    /// the device where space runs out. So the downloads dir is a walked
    /// root like any other (`cache_roots` puts it in the list), and what
    /// survives there survives because it is protected, not because of where
    /// it lives.
    #[tokio::test]
    async fn evict_reclaims_the_old_downloads_nothing_else_would() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let offline = tmp.path().join("offline");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let abandoned = offline.join(HASH).join("movie.mkv");
        write_aged(&abandoned, &[0u8; 8192], forty_days);
        // A dormant pin's folder: no engine speaks for it, so
        // `EngineFS::protected_paths` names the folder itself.
        let pinned_folder = offline.join(OTHER_HASH);
        let pinned = pinned_folder.join("kept.mkv");
        write_aged(&pinned, &[0u8; 8192], forty_days);
        let protected: HashSet<PathBuf> = HashSet::from([pinned_folder.clone()]);
        let roots = [root.clone(), offline.clone()];

        let report = evict_roots(&roots, &protected, CacheLimit::configured(0))
            .await
            .unwrap();
        assert!(
            !abandoned.exists(),
            "an old download nothing pins is 40 days of dead weight"
        );
        assert!(pinned.is_file(), "the dormant pin's folder is protected");
        assert_eq!(report.protected_files, 1);
        assert!(report.freed >= 8192, "{report:?}");

        // And it counts towards the limit now, so the size rule can reach a
        // download the age rule was not old enough to take.
        let recent = offline.join(HASH).join("recent.mkv");
        write_aged(&recent, &[0u8; 8192], Duration::from_secs(3600));
        let report = evict_roots(
            &roots,
            &protected,
            CacheLimit::configured(occupancy(&pinned)),
        )
        .await
        .unwrap();
        assert!(!recent.exists(), "{report:?}");
        assert!(pinned.is_file(), "never an LRU casualty");
    }

    /// The configured `downloadsDir` is settings, not cache: eviction may
    /// empty it, never remove it.
    ///
    /// `remove_empty_parents` prunes the directories a deletion left behind,
    /// upwards, and it used to stop only at the walked root. That was enough
    /// while the downloads dir was a root of its own; it stopped being enough
    /// when `outermost` started collapsing a downloads dir under a cache root
    /// into the cache root, because the root is then the cache root and the
    /// loop walks straight through the downloads dir on its way there. The
    /// server resolved and validated that path (`prepare_downloads_dir`) and
    /// handed it to the engines, so deleting it is the cleaner overruling a
    /// setting.
    #[tokio::test]
    async fn evict_never_prunes_the_configured_downloads_dir_away() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let offline = root.join("offline");
        let stale = offline.join(HASH).join("movie.mkv");
        write_aged(&stale, &[0u8; 8192], Duration::from_secs(40 * 24 * 60 * 60));

        // Exactly what `cache_roots` hands `evict`: one walked root, because
        // the downloads dir collapsed into it, and both directories to keep.
        let keep = HashSet::from([root.clone(), offline.clone()]);
        evict(
            std::slice::from_ref(&root),
            &HashSet::new(),
            &keep,
            &HashSet::from([root.clone()]),
            CacheLimit::configured(0),
        )
        .await
        .unwrap();

        assert!(!stale.exists(), "the stale download still goes");
        assert!(
            !offline.join(HASH).exists(),
            "and so does the torrent folder it emptied"
        );
        assert!(offline.is_dir(), "but the downloads dir itself stays");
        assert!(root.is_dir());
    }

    /// Roots on two volumes are two budgets, never one.
    ///
    /// The occupancy of every walked root used to be summed into one number
    /// and weighed against a single free-space figure -- the *tightest* of the
    /// roots, since one volume running out is enough to stop a write. That is
    /// right while every root is on one volume, which is the phone this was
    /// written for and is not the desktop with `downloadsDir` on an external
    /// drive. There the min is the external drive's, the sum includes the
    /// system disk's cache, and the cleaner evicts a healthy volume's film to
    /// answer a shortage on a volume it never touches -- and cannot fix, since
    /// what fills the download drive is pinned and protected.
    #[tokio::test]
    async fn each_volume_gets_a_budget_of_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("cache");
        let offline = tmp.path().join("offline");
        let film = cache.join(HASH).join("film.mkv");
        write_aged(&film, &[0u8; 8192], Duration::from_secs(60));
        let stale = offline.join(OTHER_HASH).join("leftovers.bin");
        write_aged(&stale, &[0u8; 8192], Duration::from_secs(60));
        let stale_occupancy = occupancy(&stale);

        // Two volumes: the cache root's has a terabyte, the downloads
        // drive is right up against the floor.
        let on_the_download_drive = |path: &Path| path.starts_with(&offline);
        let budgets = budgets_by_volume(
            &[cache.clone(), offline.clone()],
            // No `cacheSize` set, so the disk is the only thing capping
            // anything -- the case the tightest-of-all minimum made worst.
            0,
            |path| Some(if on_the_download_drive(path) { 2 } else { 1 }),
            |path| {
                Some(if on_the_download_drive(path) {
                    0
                } else {
                    1 << 40
                })
            },
        );
        assert_eq!(budgets.len(), 2, "one budget per volume");

        let mut reports = Vec::new();
        for budget in &budgets {
            reports.push(
                evict_roots(&budget.roots, &HashSet::new(), budget.limit)
                    .await
                    .unwrap(),
            );
        }
        let per_volume: Vec<u64> = reports
            .iter()
            .map(|report| report.limit.expect("every volume is capped here"))
            .collect();
        let report = reports
            .into_iter()
            .reduce(EvictionReport::combined_with)
            .unwrap();
        assert!(
            film.is_file(),
            "the roomy volume's cache is not what a full download drive costs"
        );
        assert!(!stale.exists(), "the full volume evicts its own");
        assert_eq!(report.freed, stale_occupancy, "{report:?}");
        assert_eq!(
            report.limit,
            Some(per_volume.iter().sum()),
            "and the one report a client gets adds the volumes' caps up, {report:?}"
        );
    }

    /// The grouping has to happen before the collapse, because the collapse
    /// is what destroys the question.
    ///
    /// `outermost` throws away a root that sits inside another, and
    /// `prepare_downloads_dir` deliberately permits a `downloadsDir` *below* a
    /// cache root. On a desktop that dir is an external drive mounted there --
    /// so collapsed first, the drive stopped being a root at all and was never
    /// asked which volume it was on: its bytes were walked as part of the cache
    /// root and capped by the cache root's free space, a cap about a volume they
    /// are not on. That is the exact reading per-volume budgets exist to stop.
    /// Grouped first, the mount is its own budget, while a plain subdirectory
    /// that really does share the volume still collapses into one walk.
    #[test]
    fn roots_are_grouped_by_volume_before_they_are_collapsed() {
        let cache = PathBuf::from("/c/rqbit-downloads");
        let archive = cache.join("archive");
        let offline = cache.join("offline");
        let external = |path: &Path| path.starts_with(&offline);
        let budgets = budgets_by_volume(
            &[cache.clone(), archive.clone(), offline.clone()],
            0,
            |path| Some(if external(path) { 2 } else { 1 }),
            |path| Some(if external(path) { 0 } else { 1 << 40 }),
        );

        assert_eq!(
            budgets.len(),
            2,
            "the mount under the cache root is a volume of its own"
        );
        assert_eq!(
            budgets[0].roots,
            vec![cache.clone()],
            "a subdirectory sharing the volume is still one walk, not two"
        );
        assert_eq!(
            budgets[0].limit.available,
            Some(1 << 40),
            "and the cache root is not capped by the drive mounted inside it"
        );
        assert_eq!(budgets[1].roots, vec![offline.clone()]);
        assert_eq!(budgets[1].limit.available, Some(0), "which has its own cap");
    }

    /// And the two budgets stay two walks: `WalkDir` crosses mount points, so
    /// without a stop the cache root's walk would count and evict the download
    /// drive's files all over again, against a cap that says nothing about the
    /// disk they are on -- undoing the split the budgets just made.
    #[tokio::test]
    async fn a_walk_stops_where_another_volumes_root_begins() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("rqbit-downloads");
        let offline = cache.join("offline");
        // Both stale, so the age rule alone decides: a cap of 0 would spare
        // either of them as a single file bigger than the whole cap.
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let film = cache.join(HASH).join("film.mkv");
        write_aged(&film, &[0u8; 4096], forty_days);
        let download = offline.join(OTHER_HASH).join("movie.mkv");
        write_aged(&download, &[0u8; 8192], forty_days);

        let external = |path: &Path| path.starts_with(&offline);
        let budgets = budgets_by_volume(
            &[cache.clone(), offline.clone()],
            0,
            |path| Some(if external(path) { 2 } else { 1 }),
            |_| Some(1 << 40),
        );
        let boundaries: HashSet<PathBuf> = budgets
            .iter()
            .flat_map(|budget| budget.roots.iter().cloned())
            .collect();

        let totals: Vec<u64> = budgets
            .iter()
            .map(|budget| {
                scan_usage(
                    &budget.roots,
                    &HashSet::new(),
                    &boundaries,
                    CacheLimit::configured(u64::MAX),
                )
                .total_bytes
            })
            .collect();
        assert_eq!(
            totals,
            vec![occupancy(&film), occupancy(&download)],
            "each volume's occupancy is its own"
        );

        // And the destructive half: the cache root ages out its own film and
        // cannot reach across the mount for the equally stale download.
        let keep: HashSet<PathBuf> = HashSet::from([cache.clone(), offline.clone()]);
        evict(
            &budgets[0].roots,
            &HashSet::new(),
            &keep,
            &boundaries,
            CacheLimit::configured(0),
        )
        .await
        .unwrap();
        assert!(!film.exists(), "the cache root evicts its own");
        assert!(
            download.is_file(),
            "the download drive is another budget's to answer for"
        );
    }

    /// The shape every device this actually ships on has, and the guarantee
    /// that grouping changed nothing for it: all the roots on one volume are
    /// one budget with one cap, as they were when there was only ever one.
    /// Roots whose volume cannot be identified at all -- one that does not
    /// exist yet, a filesystem that will not answer -- also share a budget,
    /// which is the same conservative answer the cleaner gave every root
    /// before it asked the question.
    #[test]
    fn roots_that_cannot_be_told_apart_share_one_budget() {
        let roots = [
            PathBuf::from("/c/rqbit-downloads"),
            PathBuf::from("/c/offline"),
        ];
        let tightest = |path: &Path| Some(if path.ends_with("offline") { 100 } else { 900 });

        for volume_of in [
            // One volume, identified.
            (|_: &Path| Some(1)) as fn(&Path) -> Option<u64>,
            // No volume identifiable for either.
            |_: &Path| None,
        ] {
            let budgets = budgets_by_volume(&roots, 42, volume_of, tightest);
            assert_eq!(budgets.len(), 1);
            assert_eq!(budgets[0].roots, roots);
            assert_eq!(budgets[0].limit.configured, 42);
            assert_eq!(
                budgets[0].limit.available,
                Some(100),
                "the tightest reading among roots that share a budget"
            );
        }
    }

    /// A downloads dir inside a cache root (or a cache root inside the
    /// downloads dir) must be walked once, not twice: `WalkDir` does not know
    /// the two overlap, and a doubled walk would count every file twice
    /// against `cacheSize` and try to evict each one twice.
    #[test]
    fn overlapping_roots_collapse_to_the_outermost() {
        let dirs = |paths: &[&str]| -> Vec<PathBuf> {
            let mut v: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
            v.sort();
            v.dedup();
            outermost(&v)
        };
        assert_eq!(
            dirs(&["/c/rqbit-downloads", "/c/rqbit-downloads/offline"]),
            vec![PathBuf::from("/c/rqbit-downloads")],
            "the downloads dir under a cache root"
        );
        assert_eq!(
            dirs(&["/c/rqbit-downloads", "/c"]),
            vec![PathBuf::from("/c")],
            "and a cache root under the downloads dir"
        );
        assert_eq!(
            dirs(&["/c/rqbit-downloads", "/c/rqbit-downloads"]),
            vec![PathBuf::from("/c/rqbit-downloads")],
            "two spellings of one root do not cancel each other out"
        );
        assert_eq!(
            dirs(&["/a/dl", "/b/downloads"]),
            vec![PathBuf::from("/a/dl"), PathBuf::from("/b/downloads")],
            "disjoint roots are both kept"
        );
        assert_eq!(
            dirs(&["/c/dl", "/c/dl2"]),
            vec![PathBuf::from("/c/dl"), PathBuf::from("/c/dl2")],
            "component-wise: /c/dl2 is not under /c/dl"
        );
    }

    /// The whole walk, over a downloads dir that really is inside a cache
    /// root: one pass, each file counted once.
    #[tokio::test]
    async fn a_downloads_dir_inside_a_cache_root_is_walked_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let offline = root.join("offline");
        let download = offline.join(HASH).join("movie.mkv");
        write_aged(&download, &[0u8; 8192], Duration::from_secs(60));
        let cache = root.join("fresh.mkv");
        write_aged(&cache, &[0u8; 4096], Duration::from_secs(60));

        let mut roots = vec![root.clone(), offline.clone()];
        roots.sort();
        roots.dedup();
        let roots = outermost(&roots);
        assert_eq!(roots, vec![root.clone()]);

        let usage = scan_roots(&roots, &HashSet::new(), CacheLimit::configured(u64::MAX));
        assert_eq!(
            usage.total_bytes,
            occupancy(&download) + occupancy(&cache),
            "the download counts, and counts once"
        );
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

        evict_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit::configured(0),
        )
        .await
        .unwrap();
        assert!(pinned.is_file(), "the pinned file survives the age rule");
        assert!(!stale.exists(), "its unpinned neighbour does not");

        evict_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit::configured(1024),
        )
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
        let report = evict_roots(
            std::slice::from_ref(&root),
            &HashSet::new(),
            CacheLimit::configured(limit),
        )
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

    /// [`scan_usage`] is `usage`'s read-only walk (`usage` itself only adds
    /// the `AppState` plumbing `evict`'s callers already do). It must count
    /// a sparse file's occupancy honestly too: a "Storage" screen reading
    /// `len()` would report the whole film as cached before a single byte
    /// past its first block had landed on disk.
    #[cfg(unix)]
    #[test]
    fn usage_reports_occupancy_not_apparent_length_for_a_sparse_file() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        std::fs::create_dir_all(&root).unwrap();

        let apparent = 4u64 << 30;
        let sparse = root.join("film.mkv");
        let mut file = std::fs::File::create(&sparse).unwrap();
        file.set_len(apparent).unwrap();
        file.seek(SeekFrom::Start(apparent - 1)).unwrap();
        file.write_all(&[1]).unwrap();
        drop(file);
        let allocated = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&sparse).unwrap().blocks() * 512
        };
        if allocated >= apparent {
            // No sparse-file support on this filesystem -- nothing to
            // assert about occupancy that would not be a tautology.
            return;
        }

        let usage = scan_roots(
            std::slice::from_ref(&root),
            &HashSet::new(),
            CacheLimit::configured(0),
        );

        assert_eq!(usage.total_bytes, allocated, "occupancy, not len()");
        assert!(
            usage.total_bytes < 1 << 20,
            "4 GiB of apparent length reported as {} bytes",
            usage.total_bytes
        );
        assert_eq!(usage.protected_bytes, 0);
        assert_eq!(usage.protected_files, 0);
    }

    /// A caller explaining "over the limit but nothing is evictable" needs
    /// `protected_bytes`/`protected_files` to name exactly what a live
    /// engine or a pin is holding -- the same set `evict` never touches.
    #[tokio::test]
    async fn usage_reports_the_protected_bytes_a_live_engine_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let pinned = root.join("Pinned").join("movie.mkv");
        write_aged(&pinned, &[0u8; 8192], Duration::from_secs(600));
        let free = root.join("Free").join("e1.mkv");
        write_aged(&free, &[0u8; 4096], Duration::from_secs(600));
        let protected: HashSet<PathBuf> = HashSet::from([pinned.clone()]);
        let pinned_bytes = occupancy(&pinned);
        let free_bytes = occupancy(&free);

        // `u64::MAX` is what `cache_size_bytes(None)` produces for an
        // unlimited cache -- the only value `scan_usage` treats as "no
        // limit" (unlike `evict`'s own `limit == 0` shortfall check: `0` is
        // a distinct, explicit zero-size cap, per `ServerSettings.cache_size`
        // -- `Some(0.0)`, not `None` -- and `CacheUsage` must not blur the
        // two the way `EvictionReport::shortfall_message` does).
        let usage = scan_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit::configured(u64::MAX),
        );

        assert_eq!(usage.total_bytes, pinned_bytes + free_bytes);
        assert_eq!(usage.protected_bytes, pinned_bytes);
        assert_eq!(usage.protected_files, 1);
        assert_eq!(usage.limit_bytes, None, "u64::MAX means unlimited");

        // Reading usage never deletes anything, unlike a clean pass.
        assert!(pinned.is_file());
        assert!(free.is_file());

        // A `CacheUsage` crosses `GET /cache.json` and `ServerHandle::cache_usage`
        // as JSON, camelCase like every other response type.
        let json = serde_json::to_value(&usage).unwrap();
        assert_eq!(json["totalBytes"], pinned_bytes + free_bytes);
        assert_eq!(json["protectedBytes"], pinned_bytes);
        assert_eq!(json["protectedFiles"], 1);
        assert_eq!(json["limitBytes"], serde_json::Value::Null);
    }

    /// The field condition: no `downloadsDir` is configured, so downloads
    /// land in the very root the engines stream into. Ordinary streamed
    /// cache there is reclaimable; a pinned download and the file a live
    /// engine is writing are not -- and protection is the only thing that
    /// tells them apart, since every root is walked to the bottom.
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

        let report = evict_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit::configured(limit),
        )
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

        let report = evict_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit::configured(limit),
        )
        .await
        .unwrap();

        assert!(pinned.is_file());
        assert!(live.is_file());
        assert_eq!(report.deleted, 0);
        assert_eq!(report.freed, 0);
        assert_eq!(report.protected, protected_bytes);
        assert_eq!(report.protected_files, 2);
        assert_eq!(report.total, protected_bytes);

        assert_eq!(report.over_limit, protected_bytes - limit);
        let message = report.shortfall_message().expect("still over the limit");
        assert!(
            message.contains(&format!(
                "Cache is {} bytes over its limit",
                protected_bytes - limit
            )),
            "{message}"
        );
        assert!(
            message.contains(&format!("{protected_bytes} bytes in 2 files are protected")),
            "{message}"
        );
    }

    /// A torrent writes continuously while it is being watched. Re-arming
    /// the debounce on every write pushed the clean past the end of
    /// playback, so the size limit was only ever enforced by the hourly
    /// fallback. Only the first event after a clean may arm the timer.
    #[test]
    fn a_stream_of_events_does_not_defer_the_debounced_clean() {
        let mut schedule = CleanSchedule::default();
        assert!(schedule.on_event(), "the first event arms the timer");
        for _ in 0..1_000 {
            assert!(
                !schedule.on_event(),
                "a clean is already due; the deadline stays put"
            );
        }
        schedule.on_clean();
        assert!(schedule.on_event(), "and the next event arms a fresh one");
    }

    #[test]
    fn shortfall_message_is_only_for_a_run_that_stayed_over_the_limit() {
        let under = EvictionReport {
            total: 10,
            limit: Some(20),
            ..EvictionReport::default()
        };
        assert_eq!(under.shortfall_message(), None);
        let unlimited = EvictionReport {
            total: u64::MAX,
            limit: None,
            ..EvictionReport::default()
        };
        assert_eq!(unlimited.shortfall_message(), None, "nothing caps it");
        let over = EvictionReport {
            total: 30,
            limit: Some(20),
            over_limit: 10,
            protected: 25,
            protected_files: 3,
            freed: 5,
            deleted: 1,
        };
        assert!(over.shortfall_message().is_some());

        // A cap of exactly 0 is the tightest cap there is, not the absence
        // of one: any volume whose occupancy plus free space is under the
        // floor gets one, and that is the device most in need of the line
        // that says what is holding the cache up.
        let capped_at_nothing = EvictionReport {
            total: 4096,
            protected: 4096,
            protected_files: 1,
            limit: Some(0),
            over_limit: 4096,
            ..EvictionReport::default()
        };
        assert!(
            capped_at_nothing.shortfall_message().is_some(),
            "a cap of 0 is a cap, and this run is over it"
        );
    }

    /// A disk with nothing left to evict is reported once, not four times a
    /// minute for the life of the process. A stopped torrent stays in the
    /// backend's error state, so without this every tick walked the whole
    /// cache tree and wrote the same two WARN lines again -- unbounded for
    /// a pinned download, which the idle sweeper never removes.
    #[test]
    fn a_disk_with_nothing_left_to_evict_is_reported_once() {
        let a = HASH.to_string();
        let b = HASH.replace('0', "f");
        let mut recovery = DiskFullRecovery::default();

        let stuck: HashSet<String> = [a.clone()].into_iter().collect();
        assert!(recovery.has_new_work(&stuck), "the first sight of it");
        recovery.give_up(&stuck);
        assert!(
            !recovery.has_new_work(&stuck),
            "and quiet on every tick after"
        );
        assert!(!recovery.has_new_work(&stuck));

        // A second torrent running out of space has not been tried, so it
        // is worth a pass even though the first one is still stuck.
        let both: HashSet<String> = [a.clone(), b.clone()].into_iter().collect();
        assert!(recovery.has_new_work(&both));
        recovery.give_up(&both);
        assert!(!recovery.has_new_work(&both));

        // One recovering by other means leaves only what was already tried.
        let only_b: HashSet<String> = [b.clone()].into_iter().collect();
        assert!(!recovery.has_new_work(&only_b));

        // Nothing stopped at all forgets everything, so the same torrent
        // filling the disk again later gets a fresh pass and a fresh line.
        assert!(!recovery.has_new_work(&HashSet::new()));
        assert!(recovery.has_new_work(&only_b));

        // So does a clean that actually reclaimed something: the device the
        // refusal described is not the device any more.
        recovery.give_up(&only_b);
        assert!(!recovery.has_new_work(&only_b));
        recovery.room_was_made();
        assert!(recovery.has_new_work(&only_b));
    }

    /// A run is one report to the client, but a shortfall is one volume's.
    ///
    /// Folding summed `total` against summed `limit`, so a system disk with
    /// room to spare could pay off a download drive that is full: the client
    /// asked whether cleaning had got the device under its limit, was told
    /// yes, and the drive it could not write to was over its own cap by
    /// exactly as much as before.
    #[test]
    fn one_volumes_shortfall_survives_another_volumes_slack() {
        let full = EvictionReport {
            total: 100,
            protected: 100,
            protected_files: 1,
            freed: 0,
            deleted: 0,
            limit: Some(50),
            over_limit: 50,
        };
        let roomy = EvictionReport {
            total: 10,
            limit: Some(1000),
            ..EvictionReport::default()
        };
        assert!(
            full.shortfall_message().is_some(),
            "the volume that is over says so on its own"
        );
        assert_eq!(roomy.shortfall_message(), None);

        let combined = full.clone().combined_with(roomy.clone());
        assert!(
            combined.shortfall_message().is_some(),
            "and the pass as a whole still says it: {combined:?}"
        );
        assert_eq!(
            roomy.clone().combined_with(roomy).shortfall_message(),
            None,
            "two volumes with room are still a pass with nothing to report"
        );
    }

    /// The shape the two sentinels cross the wire in. `POST /cache/clean`,
    /// `ServerHandle::clean_cache_now` and the FFI call behind the app's
    /// storage screen all read this JSON, and the app distinguishes "no cap"
    /// from "a cap of nothing" by exactly this difference -- so it is pinned
    /// here rather than left to serde's defaults being what one assumes.
    #[test]
    fn the_reported_limit_crosses_as_null_or_a_number_never_a_sentinel() {
        let json = |limit| {
            serde_json::to_value(EvictionReport {
                limit,
                ..EvictionReport::default()
            })
            .unwrap()["limit"]
                .clone()
        };
        assert_eq!(json(None), serde_json::Value::Null, "no cap at all");
        assert_eq!(json(Some(0)), serde_json::json!(0), "a cap of nothing");
        assert_eq!(json(Some(1024)), serde_json::json!(1024));

        // And the shortfall beside it, camelCase like the rest: it is what a
        // client has to read to know some volume is stuck, since after a fold
        // `total` against `limit` cannot tell it.
        let over = serde_json::to_value(EvictionReport {
            over_limit: 4096,
            ..EvictionReport::default()
        })
        .unwrap();
        assert_eq!(over["overLimit"], serde_json::json!(4096));

        // And back, since the same type is what a library embedder reads.
        for limit in [None, Some(0), Some(1024)] {
            let report = EvictionReport {
                limit,
                ..EvictionReport::default()
            };
            let round_tripped: EvictionReport =
                serde_json::from_value(serde_json::to_value(&report).unwrap()).unwrap();
            assert_eq!(round_tripped, report);
        }
    }

    /// The cap is the smaller of the two, and which one binds depends only
    /// on the numbers: plenty of room and `cacheSize` governs; a nearly full
    /// volume and the filesystem does, whatever `cacheSize` says -- including
    /// the unset `u64::MAX` that let a 4 GB television fill up.
    #[test]
    fn the_effective_limit_is_the_smaller_of_the_setting_and_the_volume() {
        let gib = 1024 * 1024 * 1024;

        // Room to spare: 8 GiB free, so the disk would allow occupancy up to
        // 1 + 8 - 0.5 = 8.5 GiB and the 2 GiB setting is what bites.
        let roomy = CacheLimit {
            configured: 2 * gib,
            available: Some(8 * gib),
        };
        assert_eq!(roomy.effective(gib), Some(2 * gib));
        assert!(!roomy.disk_bound(gib));

        // The owner's box: nothing configured, 3 GiB of cache and 523 MiB
        // free. The cache may keep what it has plus the free space above the
        // floor -- 11 MiB of headroom, not the 1.4 GiB film.
        let television = CacheLimit {
            configured: u64::MAX,
            available: Some(523 * 1024 * 1024),
        };
        assert_eq!(
            television.effective(3 * gib),
            Some(3 * gib + 523 * 1024 * 1024 - CACHE_FREE_SPACE_FLOOR)
        );
        assert!(television.disk_bound(3 * gib));

        // A generous setting on the same box does not buy room the device
        // does not have.
        let configured_too_high = CacheLimit {
            configured: 10 * gib,
            ..television
        };
        assert_eq!(
            configured_too_high.effective(3 * gib),
            television.effective(3 * gib)
        );
        assert!(configured_too_high.disk_bound(3 * gib));
    }

    /// The floor is never eaten into, and the arithmetic that keeps it out of
    /// reach saturates rather than wrapping: a volume already below the floor
    /// asks for eviction below current occupancy, and one with no room at all
    /// asks for everything -- neither may come out as "no limit".
    #[test]
    fn the_free_space_floor_is_never_eaten_into() {
        // Whatever occupancy the cache is at, the cap leaves the floor free.
        for occupied in [0u64, 1, 4096, 1_000_000_000] {
            for available in [0u64, 1, CACHE_FREE_SPACE_FLOOR, 5_000_000_000] {
                let limit = CacheLimit {
                    configured: u64::MAX,
                    available: Some(available),
                };
                let effective = limit.effective(occupied).unwrap();
                let free_at_the_cap = occupied + available - effective.min(occupied + available);
                assert!(
                    free_at_the_cap >= CACHE_FREE_SPACE_FLOOR.min(occupied + available),
                    "occupied={occupied} available={available} effective={effective}"
                );
            }
        }

        // Below the floor: the cap is under what is there, so the size rule
        // has work to do rather than seeing "already under the limit".
        let squeezed = CacheLimit {
            configured: u64::MAX,
            available: Some(1024),
        };
        assert!(squeezed.effective(4096).unwrap() < 4096);

        // Nothing left at all: a cap of 0 that must still read as a cap.
        let full = CacheLimit {
            configured: u64::MAX,
            available: Some(0),
        };
        assert_eq!(full.effective(0), Some(0));
        assert!(full.disk_bound(0));
    }

    /// An unreadable volume leaves the configured limit exactly as it was --
    /// never 0 free space, which would evict a healthy cache on the strength
    /// of a failed syscall.
    ///
    /// The refusal is injected through the probe parameter of
    /// `budgets_by_volume` rather than staged with a path the OS will not
    /// answer for, because there is no such path on every platform: `statvfs`
    /// fails on a directory not yet created, but Windows names the volume from
    /// the drive letter and answers for it with the drive's real free space.
    /// (That is exactly how this test used to fail there, with the real
    /// number where `None` was expected -- the property held; the fixture did
    /// not.) What the cleaner does with a probe that answers `None` is the
    /// property, and it is the same on both.
    #[test]
    fn an_unreadable_volume_leaves_the_configured_limit_alone() {
        assert_eq!(CacheLimit::configured(1024).effective(4096), Some(1024));
        assert_eq!(
            CacheLimit::configured(u64::MAX).effective(4096),
            Some(u64::MAX)
        );
        assert_eq!(CacheLimit::configured(0).effective(4096), None);
        assert!(!CacheLimit::configured(0).disk_bound(4096));

        // The seam the real probe is wired through: a volume whose free space
        // cannot be read gets a budget with no filesystem reading behind it,
        // so the configured cap -- or no cap -- is what it enforces.
        let roots = [PathBuf::from("/c/rqbit-downloads")];
        let volume_of = |_: &Path| Some(1);
        let unreadable = |_: &Path| None;
        for (configured, expected) in [(1024, Some(1024)), (u64::MAX, Some(u64::MAX)), (0, None)] {
            let budgets = budgets_by_volume(&roots, configured, volume_of, unreadable);
            assert_eq!(budgets.len(), 1);
            assert_eq!(budgets[0].limit, CacheLimit::configured(configured));
            assert_eq!(budgets[0].limit.effective(4096), expected);
            assert!(!budgets[0].limit.disk_bound(4096));
        }

        // Whereas a real directory answers with a real number -- on every
        // platform, which is all that can be said of the real probe here.
        let tmp = tempfile::tempdir().unwrap();
        assert!(available_space(tmp.path()).unwrap() > 0);
    }

    /// The whole point, end to end: with `cacheSize` unset -- the setting the
    /// owner's device was running -- a cache on a volume with barely any room
    /// left is evicted down to the floor, where before it was left alone
    /// until librqbit hit ENOSPC. Protection still wins.
    #[tokio::test]
    async fn a_nearly_full_volume_caps_a_cache_with_no_cache_size_set() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let stale = root.join("old-show").join("e1.mkv");
        write_aged(&stale, &[0u8; 4096], Duration::from_secs(7200));
        let recent = root.join("recent.mkv");
        write_aged(&recent, &[0u8; 4096], Duration::from_secs(60));
        let stale_occupancy = occupancy(&stale);
        let occupied = stale_occupancy + occupancy(&recent);

        // Unlimited by setting, and the walk found nothing to age out.
        let unlimited = CacheLimit::configured(u64::MAX);
        let report = evict_roots(std::slice::from_ref(&root), &HashSet::new(), unlimited)
            .await
            .unwrap();
        assert_eq!(
            report.deleted, 0,
            "nothing caps it without a free-space reading"
        );
        assert_eq!(report.total, occupied);

        // Now put the volume half a file *below* the floor -- which is what
        // a film streaming onto a nearly full device does. The cap lands
        // between the two files, so the older one has to go to buy the floor
        // back.
        let squeezed = CacheLimit {
            configured: u64::MAX,
            available: Some(CACHE_FREE_SPACE_FLOOR - stale_occupancy / 2),
        };
        let report = evict_roots(std::slice::from_ref(&root), &HashSet::new(), squeezed)
            .await
            .unwrap();
        assert!(
            !stale.exists(),
            "the least recently modified file goes first"
        );
        assert!(recent.is_file(), "and only as much as the cap needs");
        assert_eq!(report.freed, stale_occupancy);
        assert!(
            report.limit.is_some_and(|limit| limit >= report.total),
            "the run reports the cap it enforced, and ended under it"
        );

        // A live torrent's file is still untouchable, whatever the volume
        // says: a full disk may not delete what is being written, and the run
        // reports what protection held rather than a clean that did nothing.
        let protected: HashSet<_> = [recent.clone()].into_iter().collect();
        let report = evict_roots(
            std::slice::from_ref(&root),
            &protected,
            CacheLimit {
                configured: u64::MAX,
                available: Some(CACHE_FREE_SPACE_FLOOR - occupancy(&recent) / 4),
            },
        )
        .await
        .unwrap();
        assert!(recent.is_file(), "protection outranks a full volume");
        assert_eq!(report.protected_files, 1);
        assert!(report.shortfall_message().is_some());
    }

    /// The single-file rule is a concession to a *soft* limit, and the
    /// disk-derived cap is not one. Every cached film is bigger than
    /// `occupied + available - floor` on a device that is nearly full, so
    /// keeping the rule there refused to evict the only candidate there was
    /// -- freeing nothing, on the exact volume the cap was added for.
    ///
    /// Written with the cap between the two files rather than with real film
    /// sizes: what matters is that the evictable file is larger than the
    /// whole cap, which is what a 700 MB film against 463 MB of headroom is.
    #[tokio::test]
    async fn a_file_bigger_than_the_whole_cap_still_goes_when_the_disk_is_the_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let stale = root.join("last-week").join("film.mkv");
        write_aged(&stale, &[0u8; 4096], Duration::from_secs(20 * 24 * 60 * 60));
        let live = root.join("tonight").join("film.mkv");
        write_aged(&live, &[0u8; 4096], Duration::from_secs(60));
        let occupied = occupancy(&stale) + occupancy(&live);

        // Free space chosen so the cap lands *below* either file: the volume
        // is 1 KiB short of what it would need for the floor to hold with
        // the cache as it stands.
        let cap = 1024;
        let squeezed = CacheLimit {
            configured: u64::MAX,
            available: Some(CACHE_FREE_SPACE_FLOOR + cap - occupied),
        };
        assert_eq!(squeezed.effective(occupied), Some(cap));
        assert!(
            occupancy(&stale) > cap,
            "the point of the test is a file larger than the whole cap"
        );

        let protected: HashSet<_> = [live.clone()].into_iter().collect();
        let report = evict_roots(std::slice::from_ref(&root), &protected, squeezed)
            .await
            .unwrap();

        assert!(
            !stale.exists(),
            "the stale film is what there is to reclaim"
        );
        assert!(live.is_file(), "and a live torrent's file is still not it");
        assert_eq!(report.freed, occupancy(&live));
        assert!(
            report.made_room(),
            "so the torrent a full disk stopped can be restarted"
        );
        assert!(
            report.shortfall_message().is_some(),
            "the run still ends over the cap, and says what held"
        );
    }

    /// The 30-day rule reclaims space too, and `made_room` has to see it.
    /// Otherwise a pass that deleted a stale film reported `freed: 0`, the
    /// torrent a full disk had stopped was left stopped, and the next tick
    /// had nothing left to age out and a cache now under its cap -- so the
    /// torrent stayed dead on a volume the cleaner had just emptied for it.
    #[tokio::test]
    async fn what_the_age_rule_reclaimed_counts_as_room_made() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let ancient = root.join("last-month").join("film.mkv");
        write_aged(
            &ancient,
            &[0u8; 4096],
            Duration::from_secs(40 * 24 * 60 * 60),
        );
        let reclaimable = occupancy(&ancient);

        // No cap of any kind: the size rule cannot run, so whatever this
        // pass reports having freed came from the age rule alone.
        let report = evict_roots(
            std::slice::from_ref(&root),
            &HashSet::new(),
            CacheLimit::configured(0),
        )
        .await
        .unwrap();

        assert!(!ancient.exists());
        assert_eq!(report.freed, reclaimable);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.total, 0);
        assert!(
            report.made_room(),
            "so the torrent a full disk stopped is restarted"
        );
    }

    /// A stopped torrent is restarted only when the clean actually reclaimed
    /// something. Restarting onto a disk that is still full reproduces the
    /// same ENOSPC within seconds, and a loop is worse than a stopped torrent
    /// a client can report honestly.
    #[test]
    fn a_torrent_is_restarted_only_when_the_clean_made_room() {
        let freed_nothing = EvictionReport {
            total: 4096,
            protected: 4096,
            protected_files: 1,
            limit: Some(1024),
            over_limit: 3072,
            ..EvictionReport::default()
        };
        assert!(!freed_nothing.made_room());
        assert!(
            freed_nothing.shortfall_message().is_some(),
            "and the run says what protection held instead"
        );

        let freed_something = EvictionReport {
            total: 1024,
            freed: 4096,
            deleted: 1,
            limit: Some(2048),
            ..EvictionReport::default()
        };
        assert!(freed_something.made_room());
        assert!(freed_something.shortfall_message().is_none());
    }

    /// What the memory sampler reads instead of walking: nothing before the
    /// first pass, and after it the pass's own report with its age.
    #[test]
    fn the_last_report_is_kept_with_its_age() {
        let last = LastEviction::default();
        assert!(last.get().is_none(), "no pass has run");

        let report = EvictionReport {
            total: 4096,
            protected: 1024,
            protected_files: 1,
            limit: Some(1 << 30),
            ..EvictionReport::default()
        };
        last.record(&report);
        let (age, kept) = last.get().expect("a pass has run");
        assert_eq!(kept, report);
        assert!(age < Duration::from_secs(60), "recorded just now");

        let next = EvictionReport {
            total: 2048,
            ..report.clone()
        };
        last.record(&next);
        assert_eq!(
            last.get().map(|(_, kept)| kept.total),
            Some(2048),
            "the latest pass wins"
        );
    }
}
