use crate::state::AppState;
use futures_util::future::BoxFuture;
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

/// How far under the cap a pass run on behalf of a stopped torrent evicts
/// ([`recover_out_of_space_torrents`]), so the torrent it restarts has room
/// to run before it is stopped again. The watch stops a torrent a few MB
/// under the floor and the cap is the floor, so a pass to the cap alone
/// would free those few MB, restart the torrent, and be rung again within
/// the second -- a full cache walk per second for as long as there is old
/// cache to drain a proxy chunk at a time. At least the engine's resume
/// margin, so the watch agrees the volume has recovered; more, so the walks
/// are seconds apart at the least.
const RECOVERY_HEADROOM: u64 = 4 * enginefs::FREE_SPACE_RESUME_MARGIN;

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
/// cache out of -- `enginefs`'s constant, re-exported, because it is one
/// line read three ways and the three may not drift apart.
///
/// `routes::stream::ensure_download_disk_ready` refuses to stream a torrent
/// that still wants bytes unless this much is free -- a failed check runs
/// one pass of this cleaner and, if the disk is still short, answers the
/// stream `507 Insufficient Storage`. Below this line the server has
/// therefore already decided the disk is unusable, so it is exactly the line
/// the cleaner must keep the cache out of. (One constant, two readings: the
/// cleaner asks `fs4::available_space`, which is `statvfs` on the path,
/// while `ensure_download_disk_ready` matches the path against `sysinfo`'s
/// mount list behind a 3-second cache. Same question, different syscall.)
///
/// The third reader is the engine's reconciler, whose free-space arm this
/// is (`enginefs::reconcile::desired`), and it is what turns the target into
/// something close to a guarantee. The cleaner only deletes; it cannot
/// throttle a writer, and librqbit writes the file it wants straight through
/// this line to ENOSPC between passes -- on the device that prompted all
/// this, Available went to nothing in 40 s rather than stopping at 512 MiB.
/// The reconciler stops a writing torrent when the volume falls under the
/// floor and rings [`recover_out_of_space_torrents`], so what this cleaner
/// is handed is a torrent paused a few MB under the line, not one dead at
/// zero. Offline downloads keep a margin of their own
/// (`enginefs::PIN_FREE_SPACE_MARGIN`, 500 MiB, checked once when a pin is
/// accepted), so a pin can settle the volume below this line by design; the
/// reconciler stops it there like any other writer.
pub(crate) use enginefs::CACHE_FREE_SPACE_FLOOR;

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
/// The test for what an unreadable volume does therefore writes the `None`
/// straight into a [`CacheLimit`] rather than finding a path the OS will
/// refuse.
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

/// How many rings of the doorbell below may be in flight before the rest are
/// dropped. Anything above one is slack, not capacity: see [`ring_doorbell`].
const DOORBELL_DEPTH: usize = 100;

/// What the filesystem watcher does with an event: ring the cleaner's
/// doorbell, once, for anything that changed the cache.
///
/// **It must never block, whatever the doorbell's state.** This runs on
/// notify's own event-loop thread, and that is the thread
/// [`Watcher::watch`] hands a registration to and then waits for an answer
/// from. The cleaner calls `watch` from inside its `select!` -- to re-arm a
/// watch that was lost -- so blocking here on a full channel stops both
/// sides at once: the event-loop thread waits for the cleaner to drain the
/// doorbell, the cleaner waits for the event-loop thread to acknowledge a
/// watch, and neither is ever going to move. The runtime worker the cleaner
/// was polled on is then held for the life of the process, which is a
/// server that never finishes shutting down (dropping the runtime waits its
/// blocking pool out) and, long before that, a cache limit nothing enforces
/// any more. A hundred piece files written into a fresh cache is enough to
/// fill the channel and reach it.
///
/// Dropping the ring is the right answer and not a compromise: this is a
/// doorbell, not a queue. Every message on it says the same thing --
/// something under the cache changed -- and the receiver coalesces the lot
/// into one debounced pass ([`CleanSchedule`]). A channel already holding
/// [`DOORBELL_DEPTH`] of them is carrying that message a hundred times over,
/// so the hundred-and-first adds nothing a pass would do differently.
fn ring_doorbell(doorbell: &mpsc::Sender<()>, res: Result<Event, notify::Error>) {
    match res {
        Ok(event) => {
            // Filter interesting events
            if matches!(
                event.kind,
                notify::EventKind::Create(_)
                    | notify::EventKind::Modify(_)
                    | notify::EventKind::Remove(_)
            ) {
                let _ = doorbell.try_send(());
            }
        }
        Err(e) => error!("Watch error: {:?}", e),
    }
}

pub fn start(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        debug!("Cache cleaner started");

        // Channel for file system events
        let (tx, mut rx) = mpsc::channel::<()>(DOORBELL_DEPTH);

        // Setup Watcher
        // We use a sync watcher bridge to async channel
        let tx_clone = tx.clone();
        let mut watcher = match RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| ring_doorbell(&tx_clone, res),
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
        let download_dir = state.engine.download_dir.clone();
        if let Err(e) = watcher.watch(&download_dir, RecursiveMode::Recursive) {
            warn!("Failed to watch download dir {:?}: {}", download_dir, e);
            // We will try to re-watch inside the loop if needed (omitted for brevity, relying on fallback poll)
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
                    if let Err(e) = watcher.watch(&download_dir, RecursiveMode::Recursive) {
                        debug!("Retry watch {:?}: {}", download_dir, e);
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

                // 3. A torrent the backend stopped for want of disk space,
                //    found by the poll -- or, 3a, one the reconciler
                //    just stopped, which rings this the moment it does: the
                //    torrent's readers are parked until the pass restarts it,
                //    and a poll interval of parking is a player buffering.
                _ = disk_full_poll.tick() => {
                    recover_out_of_space_torrents(&state, &mut disk_full_recovery).await;
                }
                _ = state.engine.out_of_space_signal() => {
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
    let stopped: Vec<String> = state.engine.out_of_space_torrents().await;
    if stopped.is_empty() {
        recovery.room_was_made();
        return;
    }
    // Every tick after a pass that could not help would walk the whole
    // cache and say the same two things again; see [`DiskFullRecovery`].
    let hashes: HashSet<String> = stopped.iter().cloned().collect();
    if !recovery.has_new_work(&hashes) {
        return;
    }

    warn!(
        torrents = stopped.len(),
        "a torrent stopped for want of disk space; cleaning the cache to make room"
    );
    let report = match clean_cache_with_headroom(state, RECOVERY_HEADROOM).await {
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

    for info_hash in stopped {
        match state.engine.restart_from_error(&info_hash).await {
            Ok(true) => info!(
                info_hash = %info_hash,
                freed = report.freed,
                "restarted a torrent a full disk had killed"
            ),
            // Two ordinary outcomes share this arm and neither is a
            // failure: the torrent was gone by the time space had been
            // reclaimed (evicted whole, or swept), or it is merely stopped
            // rather than dead -- and a stopped one is the reconciler's to
            // start again, from a reading of the volume it takes itself on
            // its next pass, which is the whole reason there is only one
            // caller of `Session::unpause` for that case.
            Ok(false) => debug!(
                info_hash = %info_hash,
                "the torrent was not in the backend's error state; nothing restarted from here"
            ),
            Err(e) => warn!(
                info_hash = %info_hash,
                error = %format!("{e:#}"),
                "could not restart a torrent a full disk had stopped"
            ),
        }
    }
}

/// The root, its cap and the protections both [`clean_cache`] and [`usage`]
/// need, gathered once so the two read `AppState` the same way and can
/// never disagree about what the cache *is*.
struct CacheRoots {
    /// The one torrent-data root (`settings.cacheRoot`, as the engine
    /// reports it -- never as the setting spells it). Everything a torrent
    /// puts on disk is under it, so this is the whole of what the cleaner
    /// counts and evicts from.
    root: std::path::PathBuf,
    /// The piece store inside that root. **The cleaner does not walk it.**
    /// Torrent payload is one file per piece under a directory shape only
    /// the store knows, so the store is asked what it holds and asked to
    /// let a piece go; the walk covers the rest of the root -- the proxy
    /// cache and whatever whole-file downloads an earlier version left.
    store: enginefs::piece_store::StoreRoot,
    /// The cap for the volume that root is on: the operator's `cacheSize`
    /// against what the filesystem can give (see [`CacheLimit::effective`]).
    /// It used to be one cap per volume, because the removed `downloadsDir`
    /// setting could put a second root on a second disk and a cap is a
    /// statement about *one*
    /// volume -- summing two disks' occupancy against the tightest free-space
    /// reading of the two had the cleaner evicting a healthy system disk's
    /// cache to answer a shortage on an external drive it could reclaim
    /// nothing from. With one root there is one volume and one cap.
    limit: CacheLimit,
    /// What the retention policy will let this pass take, piece by piece
    /// (`EngineFS::reclaim_gate`). It replaced two lists of info hashes the
    /// cleaner used to reason from -- the protected and the dead -- because
    /// those were one question in two spellings, and the question is the
    /// policy's: *have we told a peer about this piece?*
    gate: enginefs::retention::ReclaimGate,
    /// Torrents stopped for want of disk space, to be evicted whole through
    /// the engine when nothing else can go -- see [`evict`]. Not a class of
    /// protection: such a torrent keeps its piece map and announces it
    /// again the moment it resumes, so the gate refuses its pieces like any
    /// other announced ones, and this is the *other* kind of eviction
    /// rather than an exception to the first.
    stopped: Vec<StoppedTorrent>,
}

async fn cache_roots(state: &AppState) -> CacheRoots {
    let configured = {
        let settings = state.settings.read().await;
        crate::routes::system::cache_size_bytes(settings.cache_size)
    };

    // The one root, taken from the engine and not from `settings.cacheRoot`:
    // the session was opened on a root and cannot be moved off it, so the
    // setting is where the data will be after the next start and the engine
    // is where it is now.
    let root = state.engine.download_dir.clone();

    // What the policy will part with, piece by piece -- and, separately,
    // the torrents a full disk stopped, which go whole or not at all.
    // `evict_stopped_torrent` re-checks both halves of that condition (still
    // stopped, still unpinned) before it takes one, so this list is a
    // candidate set and not a verdict.
    let verdicts = state.engine.reclaim_verdicts().await;
    let mut gate = verdicts.gate;
    // The *same* gate, told what the other adapter over the chunk store is
    // holding. One policy answers for everything this pass walks: the piece
    // store's answer arrives per torrent and per index, the proxy cache's
    // per directory and per chunk, and both are the one question -- is
    // anything speaking for these bytes?
    state.proxy_cache.retention().fill_gate(&mut gate);
    let stopped = verdicts
        .stopped_for_space
        .into_iter()
        .map(|info_hash| StoppedTorrent {
            engine: state.engine.clone(),
            info_hash,
        })
        .collect();

    CacheRoots {
        limit: CacheLimit {
            configured,
            available: available_space(&root),
        },
        store: state.engine.piece_store(),
        root,
        gate,
        stopped,
    }
}

/// `EngineFS::release_pieces` in the shape [`evict`] takes, so a test can
/// hand it something else -- and so the cleaner has no way to reach a piece
/// file except through the engine, which is where the have-set interlock
/// lives.
fn releaser(
    engine: Arc<enginefs::EngineFS>,
) -> impl for<'a> Fn(&'a str, u32) -> BoxFuture<'a, bool> {
    move |info_hash: &str, piece: u32| {
        let engine = engine.clone();
        let info_hash = info_hash.to_string();
        Box::pin(async move { engine.release_pieces(&info_hash, &[piece]).await > 0 })
    }
}

/// A torrent stopped for want of disk space, with the engine that can evict
/// it whole (`EngineFS::evict_stopped_torrent`). The scan buckets what the
/// store holds for it separately, so [`evict`] knows what evicting one would
/// reclaim without asking again.
struct StoppedTorrent {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
}

impl StoppedTorrent {
    /// `evict_stopped_torrent` on this torrent's engine, in the shape
    /// [`evict`] takes so a test can hand it something else.
    fn evictor(&self) -> impl for<'a> Fn(&'a str) -> BoxFuture<'a, anyhow::Result<bool>> + '_ {
        move |info_hash: &str| {
            let engine = self.engine.clone();
            let info_hash = info_hash.to_string();
            Box::pin(async move { engine.evict_stopped_torrent(&info_hash).await })
        }
    }
}

/// Run one eviction pass now and report what it found and freed. Shared by
/// the background scheduler in [`start`] and, through
/// `routes::cache::clean_cache_now`, `ServerHandle::clean_cache_now` and
/// `POST /cache/clean` -- the on-demand path takes exactly this function,
/// so it can never diverge from the scheduled sweep's protections.
pub(crate) async fn clean_cache(state: &AppState) -> anyhow::Result<EvictionReport> {
    clean_cache_with_headroom(state, 0).await
}

/// [`clean_cache`], evicting to `headroom` bytes under the cap rather than
/// to the cap -- for the pass run on behalf of a stopped torrent (see
/// [`RECOVERY_HEADROOM`]).
async fn clean_cache_with_headroom(
    state: &AppState,
    headroom: u64,
) -> anyhow::Result<EvictionReport> {
    let roots = cache_roots(state).await;
    // A root that does not exist yet has nothing to walk, but its cap is
    // still what this run enforced.
    let report = if !roots.root.exists() {
        EvictionReport {
            limit: roots.limit.effective(0),
            ..EvictionReport::default()
        }
    } else {
        let stopped: Vec<String> = roots
            .stopped
            .iter()
            .map(|torrent| torrent.info_hash.clone())
            .collect();
        let evictors: Vec<_> = roots.stopped.iter().map(StoppedTorrent::evictor).collect();
        let release = releaser(state.engine.clone());
        // The live promise, asked at each unlink. `roots.gate` was filled
        // before the walk below and goes stale under a reader that seeks.
        let retention = state.proxy_cache.retention().clone();
        let still_free = move |path: &std::path::Path| retention.still_free(path);
        evict(
            &roots.root,
            &roots.store,
            &roots.gate,
            &release,
            &still_free,
            &stopped,
            &evictors,
            roots.limit,
            headroom,
        )
        .await?
    };
    // The cap this pass enforced is the budget the retention policy is
    // sized against -- one number, computed once, by the layer that owns
    // "how much room is there". A policy that recomputed it would have the
    // two evicting against different limits.
    state.engine.set_cache_budget(report.limit);
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
/// ([`CacheUsage`]), reading exactly what [`evict`] reads: the same
/// [`WalkInputs`], so there is one implementation of "what the cache
/// contains" and not two that have to be kept agreeing.
///
/// The difference is a rule, not a walk: nothing may age out here (the age
/// rule is what a pass *acts* on, and this pass acts on nothing), so the
/// whole of what is on disk is in the total. Shared by
/// `routes::cache::cache_usage` (`ServerHandle::cache_usage` and
/// `GET /cache.json`).
pub(crate) async fn usage(state: &AppState) -> CacheUsage {
    let roots = cache_roots(state).await;
    let limit = roots.limit;
    let inputs = WalkInputs {
        download_dir: roots.root,
        store: roots.store,
        gate: roots.gate,
        // Not a rule this run: nothing is evicted, so nothing is set aside
        // for the engine. A stopped torrent's pieces are counted like any
        // other cache, which is what they are until a pass decides to take
        // them.
        stopped: Vec::new(),
        max_age: Duration::MAX,
        now: std::time::SystemTime::now(),
    };
    // The scan is synchronous filesystem work -- see [`evict`] for why it is
    // off the runtime -- and a `GET /cache.json` is a request a worker is
    // serving.
    tokio::task::spawn_blocking(move || scan_usage(inputs, limit))
        .await
        .unwrap_or_else(|error| {
            // A panic in the scan, in a debug build; the release profile aborts
            // the process instead. Nothing to report but that nothing was read.
            error!("the cache usage scan did not finish: {error}");
            CacheUsage::default()
        })
}

/// [`usage`]'s reading, without the `AppState` plumbing: the same
/// [`WalkInputs::run`] a clean pass makes its decisions from, read as
/// occupancy against the cap rather than acted on.
fn scan_usage(inputs: WalkInputs, limit: CacheLimit) -> CacheUsage {
    let walked = inputs.run();
    CacheUsage {
        total_bytes: walked.total_size,
        limit_bytes: limit
            .effective(walked.total_size)
            .filter(|limit| *limit != u64::MAX),
        protected_bytes: walked.protected_size,
        protected_files: walked.protected_files,
    }
}

/// What a cache root costs on disk, as the cleaner must count it -- and
/// **the one copy of that arithmetic**, which is
/// [`enginefs::chunk_store::occupied_bytes`]. The cleaner, the piece sweep
/// and the proxy cache each carried their own; three readings of "how much
/// would deleting this give back" are three numbers that can disagree.
///
/// librqbit's filesystem storage pre-allocates every file it wants at its
/// **full** length, so a part-streamed film is a multi-gigabyte apparent
/// length over a handful of allocated blocks -- `Metadata::len` on such a
/// file describes the movie, not the phone. A device reporting 17 GB of cache
/// had 3.85 GB on it, and the cleaner spent every run trying to evict its way
/// under a limit the disk was never over. (enginefs learned the same lesson
/// about progress: count what the backend allocated, never
/// `metadata().len()`.)
///
/// The session does not run on that storage any more -- it is the chunk
/// store, which pre-allocates nothing and whose files are exactly as long as
/// the bytes in them -- so nothing this server *writes* is sparse today. This
/// still has to count occupancy, for two reasons that are not going away:
/// the walked roots are full of whole-file downloads earlier versions
/// pre-allocated and nothing migrates, and a rule that reads a length as a
/// cost is one bad add away from the same 17 GB reading again.
pub(crate) use enginefs::chunk_store::occupied_bytes;

/// What the cache currently occupies against its configured limit
/// ([`usage`]), in the same occupancy accounting eviction uses
/// ([`occupied_bytes`]). `serde`-serializable so it crosses the `GET
/// /cache.json` / `ServerHandle::cache_usage` boundary as is.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheUsage {
    /// Occupancy of the walked root right now.
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

/// What one [`evict`] run found and did, in occupancy bytes
/// ([`occupied_bytes`]) throughout. `serde`-serializable so it crosses the
/// `POST /cache/clean` / `ServerHandle::clean_cache_now` boundary as is.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvictionReport {
    /// Occupancy of the walked root once eviction finished.
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
    /// How far over its cap this run ended (`total - limit`), and the only
    /// thing [`Self::shortfall_message`] reads. Reported rather than left to
    /// the client to derive, so that "still stuck" is one field and not a
    /// comparison every reader has to get right.
    pub over_limit: u64,
}

impl EvictionReport {
    /// Whether this run reclaimed anything -- the condition
    /// [`recover_out_of_space_torrents`] restarts a stopped torrent on. A
    /// clean that freed nothing has not changed the device's mind, so
    /// restarting into it would only reproduce the error the torrent already
    /// has.
    pub fn made_room(&self) -> bool {
        self.freed > 0
    }

    /// The line to log when the run ended still over the limit, naming what
    /// protection kept -- "cleaned up 0 files, freed 0 bytes" on a phone
    /// that is filling up says nothing about *why*, and the why is always
    /// that the rest of the cache belongs to a live or pinned torrent.
    /// That includes a pinned download the user has not unpinned. `None`
    /// when the run got under its limit (or had none).
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
/// the files in `evict_first`, and then the least recently modified of the
/// rest. `evict_first` is what a dead torrent left behind
/// (`EvictionClasses::dead`): bytes nothing will read or resume into, which
/// on a full device are what stands between the user and the next stream,
/// so they go before a film somebody might watch again.
///
/// Last of all, and only when the pass could free nothing else, a torrent
/// stopped for want of disk space is evicted *whole* -- torrent and files,
/// by the `i`th of `evictors` for the `i`th of `stopped`, which is its
/// engine's `evict_stopped_torrent`. Last, not first, on purpose. While the
/// pass can evict anything else, doing that makes the room the stopped
/// torrent needs to resume with its progress, which `recover_out_of_space_torrents`
/// then does -- the film somebody is watching plays on, at the cost of a film
/// nobody is. Only once nothing else can go are its bytes worthless: they
/// are progress into a file this volume cannot finish, and they are what
/// stands between the user and their next stream (their retry of the same
/// title included). "Nothing else could go" is read from this pass -- the
/// age rule and the size rule freed nothing -- rather than from the cap
/// alone, so a pass that freed *something* restarts the torrent into that
/// room and the next pass, if there is one, gets to judge again.
///
/// `headroom` lowers the cap this run evicts to (see [`RECOVERY_HEADROOM`]).
/// Sizes are occupancy, not apparent length (see [`occupied_bytes`]).
///
/// `download_dir` -- the one torrent-data root -- is covered to the bottom,
/// in two halves: walked, except for the piece store, which is asked (see
/// [`WalkInputs::run`]). Nothing is excluded by *where* it lives; what a run
/// may not touch is decided by `protected` alone, and that is the only thing
/// between the cleaner and a download somebody is watching. Whatever must
/// survive has to be named there -- see `EngineFS::protected_torrents`,
/// which covers live engines and the dormant pins that have no engine to
/// speak for them.
#[allow(clippy::too_many_arguments)]
async fn evict<E, R, S>(
    download_dir: &std::path::Path,
    store: &enginefs::piece_store::StoreRoot,
    gate: &enginefs::retention::ReclaimGate,
    release: &R,
    still_free: &S,
    stopped: &[String],
    evictors: &[E],
    limit: CacheLimit,
    headroom: u64,
) -> anyhow::Result<EvictionReport>
where
    E: for<'a> Fn(&'a str) -> BoxFuture<'a, anyhow::Result<bool>>,
    R: for<'a> Fn(&'a str, u32) -> BoxFuture<'a, bool>,
    S: Fn(&std::path::Path) -> bool,
{
    debug_assert_eq!(stopped.len(), evictors.len());
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
        download_dir: download_dir.to_path_buf(),
        store: store.clone(),
        gate: gate.clone(),
        stopped: stopped.to_vec(),
        max_age: Duration::from_secs(30 * 24 * 60 * 60),
        now: std::time::SystemTime::now(),
    };
    let Walked {
        files,
        aged_out,
        mut total_size,
        protected_size,
        protected_files,
        stopped: stopped_found,
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
    for (item, size) in aged_out {
        info!("Older than 30 days, deleting: {}", item);
        match reclaim(&item, release, still_free, download_dir).await {
            Ok(true) => {
                aged_out_bytes += size;
                aged_out_files += 1;
            }
            Ok(false) => {
                debug!("Nothing left to delete for {}", item);
                // The bytes are not this pass's to claim, and it cannot
                // tell "another pass took it" from "it is still there and
                // no delete of ours can reach it". Only the second is
                // safe to assume, so the size goes back into the total
                // rather than out of the report as freed.
                total_size += size;
            }
            Err(e) => {
                error!("Failed to delete {}: {}", item, e);
                // Still on the disk, so still counted against the limit.
                total_size += size;
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
    let limit = limit
        .effective(total_size)
        .map(|limit| limit.saturating_sub(headroom));
    let mut deleted_count = 0usize;
    let mut freed_space = 0u64;
    if let Some(limit) = limit
        && total_size > limit
    {
        info!(
            "Cache size {} exceeds limit {}. Cleaning up...",
            total_size, limit
        );

        // What a dead torrent left behind first, then oldest first: the
        // walk sorted them.
        for (item, size, _) in files {
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
                    "cache soft limit exceeded by single retained file: {} size={} limit={}",
                    item, size, limit
                );
                continue;
            }

            debug!("Deleting (size limit): {}", item);
            match reclaim(&item, release, still_free, download_dir).await {
                Ok(true) => {
                    total_size = total_size.saturating_sub(size);
                    freed_space += size;
                    deleted_count += 1;
                }
                // Nothing left the disk here, so nothing comes off the
                // total and nothing goes into what this pass freed; the
                // rule moves on to the next candidate.
                Ok(false) => debug!("Nothing left to delete for {}", item),
                Err(e) => error!("Failed to delete {}: {}", item, e),
            }
        }

        info!(
            "Cleaned up {} files, freed {} bytes. New size: {}",
            deleted_count, freed_space, total_size
        );

        // 4. Nothing else could go, and the run is still over: a torrent
        // stopped for want of space goes whole, largest first, until the
        // run is under. See the doc above for why this is last.
        if total_size > limit && deleted_count == 0 && aged_out_files == 0 {
            let mut candidates: Vec<usize> = (0..stopped.len())
                .filter(|&i| stopped_found[i].bytes > 0)
                .collect();
            candidates.sort_by_key(|&i| std::cmp::Reverse(stopped_found[i].bytes));
            for i in candidates {
                if total_size <= limit {
                    break;
                }
                let info_hash = &stopped[i];
                let found = &stopped_found[i];
                match evictors[i](info_hash).await {
                    Ok(true) => {
                        info!(
                            info_hash = %info_hash,
                            bytes = found.bytes,
                            files = found.files,
                            "nothing else could be evicted; a torrent stopped for want of disk space went whole, with its partial download"
                        );
                        total_size = total_size.saturating_sub(found.bytes);
                        freed_space += found.bytes;
                        deleted_count += found.files;
                    }
                    Ok(false) => debug!(
                        info_hash = %info_hash,
                        "the stopped torrent was gone, restarted or pinned by the time the pass reached it"
                    ),
                    Err(e) => warn!(
                        info_hash = %info_hash,
                        error = %format!("{e:#}"),
                        "could not evict a torrent stopped for want of disk space"
                    ),
                }
            }
        }
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

/// One thing a pass may reclaim, and -- because the two are reclaimed by
/// different owners -- which kind it is.
///
/// This is the whole of the layering the cleaner sees. A piece is named by
/// info hash and index and nothing else: the cleaner cannot build its path,
/// does not know that pieces are bucketed a thousand to a directory, and
/// unlinks nothing of the store's. It used to walk that tree and
/// `remove_file` what it found, which put the store's directory shape in
/// this crate as well as in `enginefs::piece_store`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Reclaimable {
    /// A file the cleaner walked and unlinks itself: the proxy cache, and
    /// whatever whole-file downloads an earlier version of this server left
    /// in the root.
    File(std::path::PathBuf),
    /// One piece of one torrent. Deleted by the store
    /// (`StoreRoot::delete_piece`), which is the only thing that knows where
    /// it is.
    Piece { info_hash: String, piece: u32 },
}

impl std::fmt::Display for Reclaimable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
            Self::Piece { info_hash, piece } => write!(f, "{info_hash} piece {piece}"),
        }
    }
}

/// What [`evict`] hands the blocking pool: the root to walk, the store to
/// ask, and the rules to sort what they answer by. Owned, because the scan
/// runs on another thread.
struct WalkInputs {
    download_dir: std::path::PathBuf,
    store: enginefs::piece_store::StoreRoot,
    /// Which of the store's pieces the retention policy will part with, and
    /// which of them sort to the front. Nothing the *walk* finds goes
    /// through it: since the piece store became the session's storage,
    /// everything a live torrent owns is a piece, and everything else under
    /// the root is cache with no one to speak for it.
    gate: enginefs::retention::ReclaimGate,
    /// Each torrent stopped for want of space, in the caller's order.
    /// Counted into their own buckets ([`Walked::stopped`]), never into the
    /// reclaimable list -- the engine, not the cleaner, takes those.
    stopped: Vec<String>,
    /// The age rule: a file last modified longer ago than this goes.
    max_age: Duration,
    now: std::time::SystemTime,
}

/// What the scan found, sorted into what [`evict`] does with it. Sizes are
/// occupancy ([`occupied_bytes`]).
struct Walked {
    /// Evictable by the size rule, in the order the rule takes them: what a
    /// dead torrent left behind first, then oldest modification first, with
    /// the occupancy and modification time of each. Something whose time
    /// could not be read sorts oldest -- it is counted, and the first of its
    /// class to go.
    files: Vec<(Reclaimable, u64, std::time::SystemTime)>,
    /// Past the age rule, to be deleted whatever the size rule says.
    aged_out: Vec<(Reclaimable, u64)>,
    /// Occupancy of everything that stays unless the size rule takes it:
    /// `files` plus the protected. The aged-out are *not* in it -- they are
    /// as good as gone -- and `evict` adds one back if its deletion fails.
    total_size: u64,
    protected_size: u64,
    protected_files: usize,
    /// What the store holds for each stopped torrent, in
    /// [`WalkInputs::stopped`]'s order: in `total_size`, in no other class.
    stopped: Vec<StoppedFound>,
}

/// Occupancy the walk found under one stopped torrent's paths.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StoppedFound {
    bytes: u64,
    files: usize,
}

impl WalkInputs {
    /// The scan itself: synchronous, the whole of the filesystem reading a
    /// pass does, and nothing else -- no deletion happens here, so the
    /// blocking thread holds no decision the async half has to wait on.
    ///
    /// Two sources, because the root has two kinds of thing in it. The walk
    /// covers everything but the piece store: the proxy cache, and the
    /// whole-file downloads an earlier version wrote and nothing migrates.
    /// The store is *asked* what it holds, and answers in pieces rather than
    /// in paths -- which is what keeps the bucketed directory shape in
    /// `enginefs::piece_store` and out of this crate.
    fn run(self) -> Walked {
        #[cfg(test)]
        WALKED_ON_THIS_THREAD.set(true);
        let mut walked = Walked {
            files: Vec::new(),
            aged_out: Vec::new(),
            total_size: 0,
            protected_size: 0,
            protected_files: 0,
            stopped: vec![StoppedFound::default(); self.stopped.len()],
        };
        self.walk_the_rest(&mut walked);
        self.ask_the_store(&mut walked);
        // The size rule's order: a dead torrent's pieces first, then oldest
        // first. The sort is stable, so equal keys keep the scan's order.
        let gate = &self.gate;
        walked.files.sort_by_key(|(item, _, modified)| {
            let first = match item {
                Reclaimable::Piece { info_hash, .. } => gate.goes_first(info_hash),
                Reclaimable::File(_) => false,
            };
            (!first, *modified)
        });
        walked
    }

    /// Everything under the torrent-data root that is not the piece store.
    ///
    /// `filter_entry` prunes the store's directory rather than skipping its
    /// files one by one: descending into it would be a `statx` per piece for
    /// a listing the store gives in one pass, and the point is that this
    /// walk never meets a piece file at all.
    fn walk_the_rest(&self, walked: &mut Walked) {
        if !self.download_dir.exists() {
            return;
        }
        let entries = walkdir::WalkDir::new(&self.download_dir)
            .into_iter()
            .filter_entry(|entry| !self.store.holds(entry.path()));
        for entry in entries {
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
            if is_session_artifact(&path, &self.download_dir) {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            // Occupancy, not apparent length: an earlier version's librqbit
            // pre-allocated wanted files at full size, and those files are
            // still here.
            let size = occupied_bytes(&metadata);
            // The gate, asked about a walked file exactly as it is asked
            // about a piece the store reported. A chunk inside a live
            // proxied stream's window is the bytes under a player's head and
            // the scan-back and read-ahead either side of it; taking one
            // costs that player a broken read and costs the origin the same
            // fetch again. A torrent's reader is protected from this by
            // librqbit -- the cleaner's delete goes through `drop_pieces`,
            // which will not forget a piece a reader is waiting on -- and a
            // proxied one has no backend to refuse, so the refusal is here.
            if !self.gate.releases_file(&path) {
                walked.total_size += size;
                walked.protected_size += size;
                walked.protected_files += 1;
                continue;
            }
            self.sort_one(
                walked,
                Reclaimable::File(path),
                size,
                metadata.modified().ok(),
            );
        }
    }

    /// What the piece store holds, per torrent, as the store reports it --
    /// and, per piece, whether the retention policy will part with it.
    ///
    /// The question is asked of the gate one piece at a time, which is the
    /// granularity a reclaim actually has, and it is the only question
    /// asked: a piece the policy keeps is counted and set aside, a piece it
    /// releases is ordinary cache. A torrent stopped for want of space is
    /// counted into its own bucket before either, because the *engine* and
    /// not the cleaner is what may take one of those, and it takes it
    /// whole.
    ///
    /// A stray -- something in the store that is not a piece file -- is
    /// counted and never offered up. It is on the disk, so it must show in
    /// the total or the cache would read smaller than it is; and only
    /// `piece_store::sweep` can say whether it is debris, so this pass has
    /// no business unlinking it. It is counted as protected for the same
    /// reason: nothing this pass can do will free it.
    fn ask_the_store(&self, walked: &mut Walked) {
        let contents = self.store.scan();
        let root_strays: u64 = contents.strays.iter().map(occupied_bytes).sum();
        walked.total_size += root_strays;
        walked.protected_size += root_strays;
        walked.protected_files += contents.strays.len();
        for torrent in contents.torrents {
            let stray_bytes: u64 = torrent.strays.iter().map(occupied_bytes).sum();
            walked.total_size += stray_bytes;
            if let Some(i) = self
                .stopped
                .iter()
                .position(|info_hash| *info_hash == torrent.info_hash)
            {
                for piece in &torrent.pieces {
                    let bytes: u64 = piece.files().map(occupied_bytes).sum();
                    walked.total_size += bytes;
                    walked.stopped[i].bytes += bytes;
                    walked.stopped[i].files += piece.files().count();
                }
                walked.stopped[i].bytes += stray_bytes;
                walked.stopped[i].files += torrent.strays.len();
                continue;
            }
            walked.protected_size += stray_bytes;
            walked.protected_files += torrent.strays.len();
            for piece in torrent.pieces {
                let bytes: u64 = piece.files().map(occupied_bytes).sum();
                // `StoreRoot::stat` promises this: a chunk whose index a
                // `u32` could not hold is one no piece index names, so it
                // comes back as a stray and never as a piece. Counted and
                // set aside if that promise ever changed, because a piece
                // this pass cannot address is a piece it cannot free.
                let Ok(index) = u32::try_from(piece.index) else {
                    walked.total_size += bytes;
                    walked.protected_size += bytes;
                    walked.protected_files += piece.files().count();
                    continue;
                };
                if !self.gate.releases(&torrent.info_hash, index) {
                    walked.total_size += bytes;
                    walked.protected_size += bytes;
                    walked.protected_files += piece.files().count();
                    continue;
                }
                let modified = piece.modified();
                self.sort_one(
                    walked,
                    Reclaimable::Piece {
                        info_hash: torrent.info_hash.clone(),
                        piece: index,
                    },
                    bytes,
                    modified,
                );
            }
        }
    }

    /// The age rule, applied to one reclaimable thing: past it, and it goes
    /// whatever the size rule says; short of it, it is counted and queued
    /// for the size rule in modification order.
    fn sort_one(
        &self,
        walked: &mut Walked,
        item: Reclaimable,
        size: u64,
        modified: Option<std::time::SystemTime>,
    ) {
        let age = modified.map(|modified| {
            self.now
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0))
        });
        if age.is_some_and(|age| age > self.max_age) {
            walked.aged_out.push((item, size));
        } else {
            walked.total_size += size;
            walked.files.push((
                item,
                size,
                modified.unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            ));
        }
    }
}

/// Take one reclaimable thing off the disk, and say whether anything
/// actually left it.
///
/// A walked file the cleaner unlinks itself, pruning the directories the
/// deletion emptied. **A piece it never unlinks at all**: it asks the
/// engine, which has the backend forget the piece and only then lets the
/// store take it (`EngineFS::release_pieces`). The cleaner used to call the
/// store directly, which was safe only for as long as everything it was
/// allowed to touch belonged to no torrent in the session; the policy now
/// offers it pieces of torrents that are, and unlinking one of those behind
/// librqbit's back leaves it advertising a piece it does not have.
///
/// **`Ok(false)` is not success and it is not failure: it is "there were no
/// bytes here to take".** The walk's answer and the delete's are two
/// readings of the disk taken a moment apart, and nothing serialises passes
/// -- a 507'd stream runs a whole `clean_cache` on the request task while
/// the background loop is in one -- so both may scan the same piece and only
/// one of them can take it. Whichever loses must not book the bytes: what
/// `EvictionReport::freed` decides is whether a torrent stopped by ENOSPC is
/// restarted, and restarting it onto a disk that gained nothing is the loop
/// `DiskFullRecovery` exists to stop. A piece the backend refuses to forget
/// answers the same way, and for a reason of the same shape: the bytes are
/// still there and no delete of ours may reach them.
async fn reclaim<R, S>(
    item: &Reclaimable,
    release: &R,
    still_free: &S,
    download_dir: &std::path::Path,
) -> std::io::Result<bool>
where
    R: for<'a> Fn(&'a str, u32) -> BoxFuture<'a, bool>,
    S: Fn(&std::path::Path) -> bool,
{
    match item {
        Reclaimable::File(path) => {
            // Asked again here, not only in the gate this pass carries. That
            // gate was filled before a walkdir over the whole root and before
            // every delete ahead of this one; a reader that seeks meanwhile
            // promises chunks the snapshot calls free. The torrent arm below
            // has always re-asked -- `release` goes to the engine, which
            // consults the live policy -- and this is its sibling.
            if !still_free(path) {
                return Ok(false);
            }
            // A walked file that is already gone stays an `Err(NotFound)`,
            // as it has always been: not counted either way, and this arm
            // has no second reading of the disk to reconcile with the
            // walk's.
            tokio::fs::remove_file(path).await?;
            if let Some(parent) = path.parent() {
                remove_empty_parents(parent, download_dir).await;
            }
            Ok(true)
        }
        Reclaimable::Piece { info_hash, piece } => Ok(release(info_hash, *piece).await),
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

/// Prune the directories a deletion left empty, upwards, stopping at the first
/// one that is not empty -- and never at `keep`, the torrent-data root
/// itself. That directory is configuration (`settings.cacheRoot`, prepared at
/// startup and opened on by the session), and the cleaner is not the thing
/// that gets to remove it, however empty eviction leaves it.
async fn remove_empty_parents(mut dir: &std::path::Path, keep: &std::path::Path) {
    while dir != keep {
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
        CACHE_FREE_SPACE_FLOOR, CacheLimit, CleanSchedule, DiskFullRecovery, Event, EvictionReport,
        LastEviction, WALKED_ON_THIS_THREAD, WalkInputs, available_space, evict,
        is_session_artifact, mpsc, occupied_bytes, remove_empty_parents, ring_doorbell, scan_usage,
    };
    use enginefs::piece_store::{FileSpec, PieceLayout, PieceStore, StoreRoot};
    use enginefs::retention::ReclaimGate;
    use futures_util::future::BoxFuture;
    use notify::EventKind;
    use notify::event::CreateKind;
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba98";

    /// The piece store inside a torrent-data root, exactly as the engine
    /// builds it.
    fn store(root: &Path) -> StoreRoot {
        StoreRoot::in_download_dir(root)
    }

    /// One piece file of `info_hash`, written where the store will find it
    /// and aged, and its path so a test can say whether it is still there.
    ///
    /// Placed through the store's own `PieceStore`, never by spelling the
    /// bucketed layout out here: a test that hardcoded `<hash>/<n/1000>/<n>`
    /// would be a second copy of the very knowledge this layering exists to
    /// keep in one place, and would keep passing after the shape changed
    /// under it.
    fn piece_store_for(root: &Path, info_hash: &str, pieces: u32) -> PieceStore {
        // One file spanning the whole torrent: the layout only has to be
        // wide enough to name the piece, since nothing here reads through it.
        let piece_len = 4u64 << 20;
        let total = piece_len * u64::from(pieces);
        let layout = PieceLayout::new(
            piece_len,
            total,
            [FileSpec {
                len: total,
                padding: false,
            }],
        )
        .unwrap();
        PieceStore::new(store(root).torrent_dir(info_hash), Arc::new(layout))
    }

    fn write_piece(root: &Path, info_hash: &str, piece: u32, len: usize, age: Duration) -> PathBuf {
        let path = piece_store_for(root, info_hash, piece + 1).piece_path(piece);
        write_aged(&path, &vec![0u8; len], age);
        path
    }

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

    /// A file the live promise refuses is not unlinked, whatever the gate
    /// this pass is carrying says.
    ///
    /// `evict`'s gate is filled once, before the walk and before every
    /// delete ahead of this one, so a reader that seeks meanwhile promises
    /// chunks the gate calls free. The torrent arm has always re-asked --
    /// `release` goes to the engine and it consults the live policy -- and
    /// this is the file arm's sibling. Without it the promise is a reading
    /// rather than a refusal, and the player loses the bytes under its head.
    #[tokio::test]
    async fn a_file_the_live_promise_refuses_survives_the_pass() {
        let tmp = tempfile::tempdir().expect("a scratch root");
        let root = tmp.path();
        let kept = root.join("entity").join("0").join("4");
        let taken = root.join("entity").join("0").join("9");
        // `kept` is the older, so the size rule reaches for it first and the
        // refusal is what it runs into.
        std::fs::create_dir_all(kept.parent().unwrap()).unwrap();
        std::fs::write(&kept, vec![0u8; 64 * 1024]).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(&taken, vec![0u8; 64 * 1024]).unwrap();

        // A cap between one file and two, so the rule evicts exactly one --
        // and above a single file's size, or the "bigger than the whole cap
        // is kept" arm above would skip both.
        let limit = CacheLimit::configured(100_000);
        let gate = ReclaimGate::default();
        let kept_for_closure = kept.clone();
        let report = evict(
            root,
            &store(root),
            &gate,
            &store_releaser(store(root)),
            // The live answer, taken at the door and disagreeing with the
            // gate about the file the rule wants -- which is what a seek
            // does while the walk is still running.
            &move |path: &Path| path != kept_for_closure,
            &[],
            &no_evictors(),
            limit,
            0,
        )
        .await
        .expect("a pass");

        assert!(
            kept.exists(),
            "the live promise refused the file the rule reached for: report={report:?}"
        );
        assert!(
            !taken.exists(),
            "so it took the next one instead, rather than giving up: report={report:?}"
        );
    }

    /// `evict` over the one torrent-data root, with nothing stopped and
    /// nothing to evict first: the shape a pass has whenever no torrent is
    /// in the backend's error state.
    async fn evict_root(
        download_dir: &Path,
        gate: &ReclaimGate,
        limit: CacheLimit,
    ) -> anyhow::Result<EvictionReport> {
        evict(
            download_dir,
            &store(download_dir),
            gate,
            &store_releaser(store(download_dir)),
            // No proxy retention behind these tests, so nothing is promised
            // and the gate they build is the whole answer.
            &|_: &std::path::Path| true,
            &[],
            &no_evictors(),
            limit,
            0,
        )
        .await
    }

    /// The inputs `usage` builds: the same scan a pass makes its decisions
    /// from, with nothing to age out and no rules to order by.
    fn usage_inputs(download_dir: &Path, gate: &ReclaimGate) -> WalkInputs {
        WalkInputs {
            download_dir: download_dir.to_path_buf(),
            store: store(download_dir),
            gate: gate.clone(),
            stopped: Vec::new(),
            max_age: Duration::MAX,
            now: SystemTime::now(),
        }
    }

    /// A gate that announces (and so keeps) every piece of `hashes`, and
    /// parts with everything else -- the answer `EngineFS::reclaim_gate`
    /// gives for a live engine nothing is streaming.
    fn torrents(hashes: &[&str]) -> ReclaimGate {
        let mut gate = ReclaimGate::default();
        for hash in hashes {
            gate.insert_announced((*hash).to_string());
        }
        gate
    }

    /// A gate for torrents the backend stopped with an error: they
    /// announce nothing, so every piece of them may go and goes first.
    fn dead_torrents(hashes: &[&str]) -> ReclaimGate {
        let mut gate = ReclaimGate::default();
        for hash in hashes {
            gate.insert_dead((*hash).to_string());
        }
        gate
    }

    /// A releaser standing in for an engine that will not forget the piece
    /// -- the backend still believes it has it -- recording what it was
    /// asked for.
    fn refusing_releaser(
        asked: Arc<std::sync::Mutex<Vec<(String, u32)>>>,
    ) -> impl for<'a> Fn(&'a str, u32) -> BoxFuture<'a, bool> {
        move |info_hash: &str, piece: u32| {
            asked.lock().unwrap().push((info_hash.to_string(), piece));
            Box::pin(async { false })
        }
    }

    /// A releaser for a test with no engine behind it: the store on its
    /// own, which is what the production one ends at once the backend has
    /// agreed to forget the piece.
    fn store_releaser(store: StoreRoot) -> impl for<'a> Fn(&'a str, u32) -> BoxFuture<'a, bool> {
        move |info_hash: &str, piece: u32| {
            let store = store.clone();
            let info_hash = info_hash.to_string();
            Box::pin(async move { store.delete_pieces_for_tests(&info_hash, [piece]) > 0 })
        }
    }

    /// The shape `evict` takes an evictor in, for a run with none.
    type Evictor = fn(&str) -> BoxFuture<'_, anyhow::Result<bool>>;

    /// The evictor list for a run with no stopped torrents.
    fn no_evictors() -> Vec<Evictor> {
        Vec::new()
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

    /// What `evict` counted for a set of files, measured before the pass
    /// took them.
    fn occupancy_of(paths: &[&Path]) -> u64 {
        paths.iter().map(|path| occupancy(path)).sum()
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
        let report = evict_root(
            &root,
            &ReclaimGate::default(),
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

        evict_root(&root, &ReclaimGate::default(), CacheLimit::configured(0))
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
        evict_root(
            &root,
            &ReclaimGate::default(),
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

    /// A plain-file download an earlier version wrote is evictable, because
    /// nothing else will ever reclaim it.
    ///
    /// It is not migrated to the piece store and it is not read: a download
    /// is played from pieces now. Under a root left out of the walk -- as
    /// the separate `downloadsDir` used to be -- such a file would be
    /// orphaned *and* immortal on the device where space runs out. The one
    /// root is covered to the bottom, and what survives there survives
    /// because it is protected, not because of where it lives.
    #[tokio::test]
    async fn evict_reclaims_a_plain_file_download_nothing_else_would() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let abandoned = root.join(HASH).join("movie.mkv");
        write_aged(&abandoned, &[0u8; 8192], forty_days);
        // A dormant pin: no engine speaks for it, so
        // `EngineFS::protected_torrents` names the torrent itself.
        let pinned = write_piece(&root, OTHER_HASH, 0, 8192, forty_days);
        let protected = torrents(&[OTHER_HASH]);

        let report = evict_root(&root, &protected, CacheLimit::configured(0))
            .await
            .unwrap();
        assert!(
            !abandoned.exists(),
            "an old download nothing pins is 40 days of dead weight"
        );
        assert!(pinned.is_file(), "the dormant pin's pieces are protected");
        assert_eq!(report.protected_files, 1);
        assert!(report.freed >= 8192, "{report:?}");

        // And it counts towards the limit, so the size rule can reach a
        // download the age rule was not old enough to take.
        let recent = root.join(HASH).join("recent.mkv");
        write_aged(&recent, &[0u8; 8192], Duration::from_secs(3600));
        let report = evict_root(
            &root,
            &protected,
            CacheLimit::configured(occupancy(&pinned)),
        )
        .await
        .unwrap();
        assert!(!recent.exists(), "{report:?}");
        assert!(pinned.is_file(), "never an LRU casualty");
    }

    /// The torrent-data root is configuration, not cache: eviction may empty
    /// it, never remove it. Every directory eviction leaves empty *under* it
    /// goes, so an evicted torrent leaves no husk behind.
    #[tokio::test]
    async fn evict_empties_the_root_without_removing_it() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let stale = root.join(HASH).join("movie.mkv");
        write_aged(&stale, &[0u8; 8192], Duration::from_secs(40 * 24 * 60 * 60));

        evict_root(&root, &ReclaimGate::default(), CacheLimit::configured(0))
            .await
            .unwrap();

        assert!(!stale.exists(), "the stale download goes");
        assert!(
            !root.join(HASH).exists(),
            "and so does the torrent folder it emptied"
        );
        assert!(root.is_dir(), "but the root itself stays");
    }

    /// The cleaner reaches the piece store **only through the store**, and
    /// this is what that buys.
    ///
    /// * A piece and the staged half of a re-download over it are one thing
    ///   to reclaim, because `StoreRoot::delete_piece` takes them together:
    ///   the pass reports one deletion, and no half-piece is left behind
    ///   shadowing nothing.
    /// * A file in the store the store does not recognise is counted -- it
    ///   is on the disk, and a cache that under-reports itself is how
    ///   invisible disk usage starts -- and left alone. Only
    ///   `piece_store::sweep` can say whether it is debris; a cleaner
    ///   walking the tree itself would have unlinked it like any other
    ///   cache file.
    ///
    /// The counting half matters as much as the deleting half: while the
    /// cleaner walked the store *and* asked it, every piece was counted
    /// twice and the cache read double its size.
    #[tokio::test]
    async fn the_cleaner_reclaims_the_store_only_through_the_store() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let pieces = piece_store_for(&root, HASH, 1);

        let complete = write_piece(&root, HASH, 0, 8192, forty_days);
        let staged = pieces.staging_path(0);
        write_aged(&staged, &[0u8; 4096], forty_days);
        // A name the store never wrote, in the bucket beside them.
        let stray = complete.parent().unwrap().join("notes");
        write_aged(&stray, &[0u8; 4096], forty_days);
        // Measured before the pass takes them.
        let piece_bytes = occupancy_of(&[&complete, &staged]);
        let stray_bytes = occupancy(&stray);

        let report = evict_root(&root, &ReclaimGate::default(), CacheLimit::configured(0))
            .await
            .unwrap();

        assert!(!complete.exists(), "the piece went");
        assert!(!staged.exists(), "and the staged half of it went with it");
        assert!(
            stray.is_file(),
            "the cleaner unlinks nothing of the store's on its own account"
        );
        assert_eq!(
            report.deleted, 1,
            "one piece reclaimed, not one file per copy: {report:?}"
        );
        assert_eq!(report.freed, piece_bytes);
        assert_eq!(
            report.total, stray_bytes,
            "what is left is exactly the stray, counted once"
        );
    }

    /// A torrent whose directory holds nothing but *staged* pieces still
    /// gives its blocks back, and the pass has to book them.
    ///
    /// That is the ordinary state of a dormant pin nothing ran `init` on
    /// this boot: the scan reports the piece (both copies are one thing to
    /// reclaim), the pass offers it, the delete takes the `.part` and the
    /// volume really gains the space. While the store counted only the
    /// *complete* copy as having left, this pass reported nothing freed --
    /// and `EvictionReport::freed` is the whole of `made_room()`, so
    /// `DiskFullRecovery` would keep a torrent stopped by ENOSPC stopped on
    /// a disk that had just gained room, or restart it on one that had not.
    ///
    /// Measured on the volume either side of the pass, never on what the
    /// store thinks it removed.
    #[tokio::test]
    async fn a_piece_that_was_only_ever_staged_books_what_the_volume_gave_back() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let staged = piece_store_for(&root, HASH, 1).staging_path(0);
        write_aged(&staged, &[0u8; 65536], forty_days);
        let staged_bytes = occupancy(&staged);
        assert!(staged_bytes > 0, "the staged piece is really on the disk");

        let report = evict_root(&root, &ReclaimGate::default(), CacheLimit::configured(0))
            .await
            .unwrap();

        assert!(!staged.exists(), "the staged piece went");
        assert_eq!(
            report.deleted, 1,
            "and the pass knows a piece left the disk: {report:?}"
        );
        assert_eq!(
            report.freed, staged_bytes,
            "with the bytes the volume actually gave back: {report:?}"
        );
        assert_eq!(report.total, 0, "nothing of it is left to count");
    }

    /// Two passes over one disk book its bytes once.
    ///
    /// Nothing serialises passes: `routes::stream` runs a whole
    /// `clean_cache` on the request task every time `ensure_download_disk_ready`
    /// refuses a stream -- i.e. on a full disk, for every 507 -- while the
    /// background loop is in one of its own. Both scan, both see the same
    /// piece, and only one of them can take it. What the loser must not do is
    /// report the bytes as freed: `EvictionReport::freed` is the whole of
    /// `made_room()`, `DiskFullRecovery` clears its exhausted set on that and
    /// `restart_from_error` puts the ENOSPC torrents back on a disk that
    /// gained nothing -- the restart loop the guard exists to stop, with the
    /// guard unable to latch because every pass "made room". The same total
    /// is recorded in `LastEviction` and read back as the cache's occupancy.
    ///
    /// Both eviction rules are here: the aged-out piece goes by the 30-day
    /// rule and the fresh ones by the size rule, and each books what it took
    /// in its own arm.
    ///
    /// The assertion is against the disk, not against an expected division
    /// of labour: whichever pass gets to a piece first, what the volume gave
    /// up is what the two reports may claim between them.
    #[test]
    fn two_passes_over_one_disk_book_its_bytes_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let pieces = [
            write_piece(&root, HASH, 0, 8192, Duration::from_secs(40 * 24 * 60 * 60)),
            write_piece(&root, HASH, 1, 8192, Duration::from_secs(600)),
            write_piece(&root, HASH, 2, 8192, Duration::from_secs(60)),
        ];
        // Measured before the passes take them, and a cap of one piece's
        // worth so the size rule has work to do and does not read a single
        // file as bigger than the whole cache.
        let occupancy: Vec<u64> = pieces.iter().map(|path| occupancy(path)).collect();
        let limit = CacheLimit::configured(occupancy[0]);

        // One blocking thread, so the filesystem work of the two passes is a
        // queue: both walks are in it before either delete, which is the
        // interleaving the race produces on its own, made to happen every
        // time.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (first, second) = runtime.block_on(async {
            let gate = ReclaimGate::default();
            tokio::join!(
                evict_root(&root, &gate, limit),
                evict_root(&root, &gate, limit),
            )
        });
        let (first, second) = (first.unwrap(), second.unwrap());

        let gone: Vec<usize> = (0..pieces.len()).filter(|&i| !pieces[i].exists()).collect();
        assert!(
            !gone.is_empty(),
            "the passes evicted nothing, so there is nothing to have mis-booked: {first:?} / {second:?}"
        );
        assert_eq!(
            first.freed + second.freed,
            gone.iter().map(|&i| occupancy[i]).sum::<u64>(),
            "the volume gave these bytes up once, and that is all the two passes may claim between them: {first:?} / {second:?}"
        );
        assert_eq!(
            first.deleted + second.deleted,
            gone.len(),
            "one deletion counted per piece that really left: {first:?} / {second:?}"
        );
    }

    /// A pinned download keeps its engine -- the idle sweeper skips pinned
    /// torrents -- so it comes through `protected_torrents` and neither rule
    /// touches its pieces, however old they are.
    ///
    /// The pieces are never unlinked by this crate even when they *are*
    /// evictable: the store holds the layout, and the cleaner asks it.
    #[tokio::test]
    async fn evict_keeps_the_pieces_a_pinned_engine_reports() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let forty_days = Duration::from_secs(40 * 24 * 60 * 60);
        let pinned = write_piece(&root, HASH, 0, 8192, forty_days);
        let stale = write_piece(&root, OTHER_HASH, 7, 4096, forty_days);
        let protected = torrents(&[HASH]);

        evict_root(&root, &protected, CacheLimit::configured(0))
            .await
            .unwrap();
        assert!(pinned.is_file(), "the pinned piece survives the age rule");
        assert!(
            !stale.exists(),
            "an unprotected torrent's piece does not -- and the store deleted it"
        );

        evict_root(&root, &protected, CacheLimit::configured(1024))
            .await
            .unwrap();
        assert!(pinned.is_file(), "and the size rule");
    }

    /// What a dead torrent left behind is the first thing the size rule
    /// takes, however recently it was written: on the television that
    /// prompted this, two torrents that had died of a storage bug held
    /// 700 MB the cleaner reported as protected, and every later stream
    /// failed for the space. Ordinary cache -- a film somebody might watch
    /// again -- goes only once those are gone.
    #[tokio::test]
    async fn a_dead_torrents_pieces_go_before_anything_else() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let old_film = root.join("Old").join("film.mkv");
        write_aged(
            &old_film,
            &[0u8; 8192],
            Duration::from_secs(7 * 24 * 60 * 60),
        );
        let dead = write_piece(&root, HASH, 0, 8192, Duration::from_secs(60));
        let dead_occupancy = occupancy(&dead);
        let occupied = occupancy(&old_film) + dead_occupancy;

        // Room for exactly one of the two: by age alone the old film would
        // go; the dead torrent's piece goes instead.
        let report = evict(
            &root,
            &store(&root),
            &dead_torrents(&[HASH]),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &[],
            &no_evictors(),
            CacheLimit::configured(occupied - dead_occupancy / 2),
            0,
        )
        .await
        .unwrap();
        assert!(!dead.exists(), "the dead torrent's piece went first");
        assert!(old_film.is_file(), "and the older film stayed");
        assert_eq!(report.deleted, 1);
        assert_eq!(report.freed, dead_occupancy);

        // A dead torrent is named once and every piece of it qualifies,
        // however many there are.
        let p1 = write_piece(&root, OTHER_HASH, 0, 4096, Duration::from_secs(30));
        let p2 = write_piece(&root, OTHER_HASH, 2500, 4096, Duration::from_secs(30));
        let report = evict(
            &root,
            &store(&root),
            &dead_torrents(&[OTHER_HASH]),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &[],
            &no_evictors(),
            CacheLimit::configured(occupancy(&old_film) + occupancy(&p1)),
            0,
        )
        .await
        .unwrap();
        assert!(old_film.is_file());
        assert_eq!(report.deleted, 1, "one piece was enough");
        assert!(
            !(p1.exists() && p2.exists()),
            "and it came from the dead torrent"
        );
    }

    /// The cleaner asks the policy per *piece*, not per torrent. A torrent
    /// something is streaming is live, announced and protected in the old
    /// reading of the word -- and the policy will still part with every
    /// piece outside the playback window, because those are the ones it has
    /// told nobody about. Committed pieces are what it keeps, and what the
    /// pass has to leave alone: they are advertised, and taking one is the
    /// advertise-then-refuse the whole design is about.
    ///
    /// This is the half of `eviction_classes` that could not survive: a
    /// list of hashes can only say all or nothing about a torrent.
    #[tokio::test]
    async fn evict_takes_the_pieces_a_policy_released_and_keeps_the_ones_it_committed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let mut pieces = Vec::new();
        for piece in 0..6u32 {
            pieces.push(write_piece(
                &root,
                HASH,
                piece,
                4096,
                Duration::from_secs(60),
            ));
        }

        // A stream on pieces 0..6 whose window has released and committed 2
        // and 3. Everything else of it is window or read-ahead: held,
        // readable, announced to nobody.
        let mut gate = ReclaimGate::default();
        gate.insert_policy(HASH.to_string(), 0..6, [2, 3].into_iter().collect());

        // Room for two pieces, so the size rule wants four gone and the
        // only thing deciding *which* four is the gate.
        let keep = occupancy_of(&[&pieces[2], &pieces[3]]);
        let report = evict_root(&root, &gate, CacheLimit::configured(keep))
            .await
            .unwrap();

        for (piece, path) in pieces.iter().enumerate() {
            let committed = piece == 2 || piece == 3;
            assert_eq!(
                path.is_file(),
                committed,
                "piece {piece} (committed: {committed})"
            );
        }
        assert_eq!(report.deleted, 4);
        assert_eq!(
            report.protected,
            occupancy_of(&[&pieces[2], &pieces[3]]),
            "what the policy kept is what the report calls protected"
        );
        assert_eq!(report.total, report.protected);
    }

    /// Debris in the store is counted **and** reported as protected,
    /// whoever it belongs to.
    ///
    /// It is on the volume, so the total has to show it or the cache reads
    /// smaller than it is; and no pass can free it -- only
    /// `piece_store::sweep` can say whether it is debris, and a delete
    /// addressed to it would look under the name the store *would* have
    /// written and free nothing. So a caller told "over the limit and
    /// nothing is evictable" has to see it in what protection holds, or the
    /// two numbers do not add up and the shortfall has no explanation.
    ///
    /// This used to depend on whose torrent it was: a protected torrent's
    /// debris was protected and everybody else's was invisible in that
    /// column. Protection is per piece now and debris is not a piece, so
    /// the question does not arise -- and the reason the old code gave
    /// applies to all of it.
    #[tokio::test]
    async fn debris_in_the_store_is_counted_and_reported_as_what_no_pass_can_free() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        // A real piece, so the pass has something it *can* take.
        let piece = write_piece(&root, HASH, 0, 4096, Duration::from_secs(60));
        // And debris: a name in the store the store would never have
        // written, so nothing can address it back.
        let debris = store(&root).torrent_dir(HASH).join("0").join("00");
        write_aged(&debris, &[0u8; 4096], Duration::from_secs(60));

        // Room for one of the two, so the pass has to take something.
        let debris_bytes = occupancy(&debris);
        let report = evict_root(
            &root,
            &ReclaimGate::default(),
            CacheLimit::configured(debris_bytes),
        )
        .await
        .unwrap();

        assert!(!piece.is_file(), "the piece the gate released went");
        assert!(debris.is_file(), "the debris could not go");
        assert_eq!(report.total, debris_bytes);
        assert_eq!(
            report.protected, debris_bytes,
            "and it is named as what is holding the cache over its limit"
        );
        assert_eq!(report.protected_files, 1);
    }

    /// The cleaner never unlinks a piece file. It asks, and a refusal is a
    /// piece still on the disk and bytes it must not book as freed.
    ///
    /// The refusal is the backend declining to forget the piece -- which is
    /// the interlock doing its job, since a piece librqbit still believes it
    /// has is one it is advertising and will serve. Booking those bytes as
    /// freed is how a torrent stopped by ENOSPC gets restarted onto a disk
    /// that gained nothing, which is a loop and not a recovery.
    #[tokio::test]
    async fn a_piece_the_engine_will_not_release_stays_on_the_disk_and_is_not_booked_as_freed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        // Past the age rule, so the pass wants it whatever the cap is and
        // the only thing between it and the disk is the refusal.
        let piece = write_piece(&root, HASH, 0, 4096, Duration::from_secs(31 * 24 * 60 * 60));
        let occupied = occupancy(&piece);
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));

        let refuse = refusing_releaser(asked.clone());
        let report = evict(
            &root,
            &store(&root),
            &ReclaimGate::default(),
            &refuse,
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &[],
            &no_evictors(),
            CacheLimit::configured(u64::MAX),
            0,
        )
        .await
        .unwrap();

        assert_eq!(
            *asked.lock().unwrap(),
            vec![(HASH.to_lowercase(), 0)],
            "the cleaner asked rather than unlinking"
        );
        assert!(piece.is_file(), "and the refusal left the bytes alone");
        assert_eq!(report.freed, 0, "nothing was booked as freed");
        assert_eq!(report.deleted, 0);
        assert_eq!(report.total, occupied, "and they are still counted");
    }

    /// An evictor standing in for the engine: records the hash and removes
    /// the torrent's data whole, as `evict_stopped_torrent` would through
    /// the session, answering `verdict`.
    fn fake_evictor(
        calls: &Arc<std::sync::Mutex<Vec<String>>>,
        root: &Path,
        verdict: bool,
    ) -> impl for<'a> Fn(&'a str) -> BoxFuture<'a, anyhow::Result<bool>> {
        let calls = calls.clone();
        let store = store(root);
        move |hash: &str| {
            calls.lock().unwrap().push(hash.to_string());
            let dir = store.torrent_dir(hash);
            Box::pin(async move {
                if verdict {
                    tokio::fs::remove_dir_all(&dir).await?;
                }
                Ok(verdict)
            })
        }
    }

    /// A torrent stopped for want of disk space goes whole -- through its
    /// engine, not by unlinking -- and only when the pass could free nothing
    /// else: while it can, that is the room the stopped torrent resumes
    /// into, and the film being watched is worth more than one nobody is.
    /// Its bytes are never reported as protected, since a pass can take
    /// them.
    #[tokio::test]
    async fn a_torrent_stopped_for_space_goes_whole_only_when_nothing_else_can() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let old_film = root.join("Old").join("film.mkv");
        write_aged(
            &old_film,
            &[0u8; 8192],
            Duration::from_secs(7 * 24 * 60 * 60),
        );
        let partial = write_piece(&root, HASH, 0, 8192, Duration::from_secs(10));
        let old_occupancy = occupancy(&old_film);
        let partial_occupancy = occupancy(&partial);
        let stopped = vec![HASH.to_string()];
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));

        // Over by half the old film: the old film goes, the stopped torrent
        // stays -- with room to resume into.
        let report = evict(
            &root,
            &store(&root),
            &ReclaimGate::default(),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &stopped,
            &[fake_evictor(&calls, &root, true)],
            CacheLimit::configured(old_occupancy + partial_occupancy - old_occupancy / 2),
            0,
        )
        .await
        .unwrap();
        assert!(!old_film.exists());
        assert!(
            partial.is_file(),
            "the stopped torrent resumes into that room"
        );
        assert!(calls.lock().unwrap().is_empty(), "the engine was not asked");
        assert_eq!(report.freed, old_occupancy);
        assert_eq!(
            report.protected, 0,
            "a stopped torrent's bytes are not protected"
        );
        assert_eq!(report.total, partial_occupancy);

        // Nothing else left and still over: the stopped torrent goes whole,
        // through the engine, and the run ends under.
        let report = evict(
            &root,
            &store(&root),
            &ReclaimGate::default(),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &stopped,
            &[fake_evictor(&calls, &root, true)],
            CacheLimit::configured(partial_occupancy / 2),
            0,
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![HASH.to_string()]);
        assert!(!partial.exists());
        assert_eq!(report.freed, partial_occupancy);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.total, 0);
        assert_eq!(report.over_limit, 0);
        assert!(report.made_room());

        // An engine that declines (the torrent restarted or was pinned
        // meanwhile) leaves the run over, and honest about it.
        write_piece(&root, HASH, 0, 8192, Duration::from_secs(10));
        calls.lock().unwrap().clear();
        let report = evict(
            &root,
            &store(&root),
            &ReclaimGate::default(),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &stopped,
            &[fake_evictor(&calls, &root, false)],
            CacheLimit::configured(partial_occupancy / 2),
            0,
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![HASH.to_string()]);
        assert!(partial.is_file());
        assert_eq!(report.freed, 0);
        assert_eq!(report.over_limit, partial_occupancy - partial_occupancy / 2);
        assert!(report.shortfall_message().is_some());
    }

    /// The pass run for a stopped torrent evicts to `headroom` under the
    /// cap, so the torrent it restarts has room to run before the watch
    /// stops it again -- a pass to the cap alone frees the few MB the
    /// torrent overshot by, and is rung again within the second.
    #[tokio::test]
    async fn headroom_lowers_the_cap_a_recovery_pass_evicts_to() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let older = root.join("older.mkv");
        write_aged(&older, &[0u8; 8192], Duration::from_secs(7200));
        let newer = root.join("newer.mkv");
        write_aged(&newer, &[0u8; 8192], Duration::from_secs(60));
        let occupied = occupancy(&older) + occupancy(&newer);

        // Exactly at the cap: an ordinary pass evicts nothing.
        let report = evict_root(
            &root,
            &ReclaimGate::default(),
            CacheLimit::configured(occupied),
        )
        .await
        .unwrap();
        assert_eq!(report.deleted, 0);

        // The same cap with headroom: the pass makes that much room, oldest
        // first, and reports the cap it actually evicted to.
        let report = evict(
            &root,
            &store(&root),
            &ReclaimGate::default(),
            &store_releaser(store(&root)),
            // No proxy retention behind these tests.
            &|_: &std::path::Path| true,
            &[],
            &no_evictors(),
            CacheLimit::configured(occupied),
            occupancy(&older) / 2,
        )
        .await
        .unwrap();
        assert!(!older.exists());
        assert!(newer.is_file());
        assert_eq!(report.limit, Some(occupied - occupancy(&newer) / 2));
        assert!(report.made_room());
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
        let report = evict_root(
            &root,
            &ReclaimGate::default(),
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

    /// [`scan_usage`] is `usage`'s reading (`usage` itself only adds the
    /// `AppState` plumbing `evict`'s callers already do). It must count a
    /// sparse file's occupancy honestly too: a "Storage" screen reading
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

        let usage = scan_usage(
            usage_inputs(&root, &ReclaimGate::default()),
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
        let pinned = write_piece(&root, HASH, 0, 8192, Duration::from_secs(600));
        let free = root.join("Free").join("e1.mkv");
        write_aged(&free, &[0u8; 4096], Duration::from_secs(600));
        let protected = torrents(&[HASH]);
        let pinned_bytes = occupancy(&pinned);
        let free_bytes = occupancy(&free);

        // `u64::MAX` is what `cache_size_bytes(None)` produces for an
        // unlimited cache -- the only value `scan_usage` treats as "no
        // limit" (unlike `evict`'s own `limit == 0` shortfall check: `0` is
        // a distinct, explicit zero-size cap, per `ServerSettings.cache_size`
        // -- `Some(0.0)`, not `None` -- and `CacheUsage` must not blur the
        // two the way `EvictionReport::shortfall_message` does).
        let usage = scan_usage(
            usage_inputs(&root, &protected),
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

    /// The field condition: downloads land in the very root the engine
    /// streams into, because there is only one. Ordinary streamed cache
    /// there is reclaimable; a pinned download and what a live engine is
    /// writing are not -- and protection is the only thing that tells them
    /// apart, since the root is covered to the bottom.
    #[tokio::test]
    async fn evict_reclaims_unpinned_cache_sharing_the_root_with_downloads() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let cold = root.join("Cold").join("e1.mkv");
        write_aged(&cold, &[0u8; 4096], Duration::from_secs(7200));
        let warm = root.join("Warm").join("e1.mkv");
        write_aged(&warm, &[0u8; 4096], Duration::from_secs(60));
        let pinned = write_piece(&root, HASH, 0, 8192, Duration::from_secs(9000));
        let live = write_piece(&root, OTHER_HASH, 1, 8192, Duration::from_secs(9000));
        let protected = torrents(&[HASH, OTHER_HASH]);

        let protected_bytes = occupancy(&pinned) + occupancy(&live);
        let cold_bytes = occupancy(&cold);
        let limit = protected_bytes + limit_between(&warm, &cold);

        let report = evict_root(&root, &protected, CacheLimit::configured(limit))
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
        let pinned = write_piece(&root, HASH, 0, 8192, Duration::from_secs(600));
        let live = write_piece(&root, OTHER_HASH, 1, 8192, Duration::from_secs(600));
        let protected = torrents(&[HASH, OTHER_HASH]);
        let protected_bytes = occupancy(&pinned) + occupancy(&live);
        let limit = protected_bytes / 2;

        let report = evict_root(&root, &protected, CacheLimit::configured(limit))
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

    /// **A ring the cleaner has no room for is dropped, not waited on.**
    ///
    /// The handler runs on notify's event-loop thread, and that is the
    /// thread `Watcher::watch` posts a registration to and then waits for
    /// an answer from -- while the cleaner, the doorbell's only reader,
    /// calls `watch` from inside its own `select!` to re-arm a lost watch.
    /// So a handler that parks on a full doorbell parks the cleaner with
    /// it, for the life of the process: the cache limit stops being
    /// enforced, and the runtime worker the cleaner was polled on is never
    /// given back, so dropping the server's runtime -- which waits its
    /// blocking pool out -- never finishes and `ServerHandle::join` never
    /// returns. Seeding a hundred piece files into a fresh cache is enough
    /// to fill the doorbell, which is an ordinary torrent.
    #[test]
    fn a_doorbell_nobody_is_answering_is_rung_past_rather_than_waited_on() {
        /// A bound so a regression fails instead of hanging the suite, not
        /// a timing assertion: what it waits for is one non-blocking send.
        const HANDLER_BOUND: Duration = Duration::from_secs(30);

        // A doorbell with a ring already on it and nothing draining it.
        // The receiver stays alive, so a send finds the channel full rather
        // than closed -- closed is the easy case and not the one that hung.
        let (doorbell, _cleaner) = mpsc::channel::<()>(1);
        doorbell.try_send(()).expect("the doorbell starts empty");

        let (rang, answered) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("notify-event-loop".to_string())
            .spawn(move || {
                ring_doorbell(
                    &doorbell,
                    Ok(Event::new(EventKind::Create(CreateKind::File))),
                );
                let _ = rang.send(());
            })
            .expect("a thread to stand in for notify's event loop");
        answered.recv_timeout(HANDLER_BOUND).expect(
            "the handler must come back from a full doorbell; parking on it deadlocks the \
             cleaner against the watcher thread and hangs the server's shutdown for ever",
        );

        // And it is a real ring wherever there is room for one, or a cache
        // being written to would never reach the cleaner at all.
        let (doorbell, mut cleaner) = mpsc::channel::<()>(1);
        ring_doorbell(
            &doorbell,
            Ok(Event::new(EventKind::Create(CreateKind::File))),
        );
        assert!(
            cleaner.try_recv().is_ok(),
            "a piece file appearing under the cache rings the cleaner"
        );
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
        // client reads to know the cache is still stuck.
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
    /// The `None` is written straight into the [`CacheLimit`] rather than
    /// staged with a path the OS will not answer for, because there is no
    /// such path on every platform: `statvfs` fails on a directory not yet
    /// created, but Windows names the volume from the drive letter and
    /// answers for it with the drive's real free space. (That is exactly how
    /// this test used to fail there, with the real number where `None` was
    /// expected -- the property held; the fixture did not.) What the cleaner
    /// does with a probe that answered `None` is the property, and it is the
    /// same on both.
    #[test]
    fn an_unreadable_volume_leaves_the_configured_limit_alone() {
        assert_eq!(CacheLimit::configured(1024).effective(4096), Some(1024));
        assert_eq!(
            CacheLimit::configured(u64::MAX).effective(4096),
            Some(u64::MAX)
        );
        assert_eq!(CacheLimit::configured(0).effective(4096), None);
        assert!(!CacheLimit::configured(0).disk_bound(4096));

        // What `cache_roots` builds when the root's free space cannot be
        // read: a cap with no filesystem reading behind it, so the
        // configured cap -- or no cap -- is what it enforces.
        for (configured, expected) in [(1024, Some(1024)), (u64::MAX, Some(u64::MAX)), (0, None)] {
            let limit = CacheLimit {
                configured,
                available: None,
            };
            assert_eq!(limit, CacheLimit::configured(configured));
            assert_eq!(limit.effective(4096), expected);
            assert!(!limit.disk_bound(4096));
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
        let recent = write_piece(&root, HASH, 0, 4096, Duration::from_secs(60));
        let stale_occupancy = occupancy(&stale);
        let occupied = stale_occupancy + occupancy(&recent);

        // Unlimited by setting, and the walk found nothing to age out.
        let unlimited = CacheLimit::configured(u64::MAX);
        let report = evict_root(&root, &ReclaimGate::default(), unlimited)
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
        let report = evict_root(&root, &ReclaimGate::default(), squeezed)
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

        // A live torrent's data is still untouchable, whatever the volume
        // says: a full disk may not delete what is being written, and the run
        // reports what protection held rather than a clean that did nothing.
        let protected = torrents(&[HASH]);
        let report = evict_root(
            &root,
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
        let live = write_piece(&root, HASH, 0, 4096, Duration::from_secs(60));
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

        let report = evict_root(&root, &torrents(&[HASH]), squeezed)
            .await
            .unwrap();

        assert!(
            !stale.exists(),
            "the stale film is what there is to reclaim"
        );
        assert!(live.is_file(), "and a live torrent's data is still not it");
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
        let report = evict_root(&root, &ReclaimGate::default(), CacheLimit::configured(0))
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
