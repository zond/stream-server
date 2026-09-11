//! What the torrent-data volume may hold, and the one place the process
//! says so.
//!
//! **Publishing a budget is not the same job as evicting.** It used to be
//! the tail of one: the cache cleaner walked sixteen thousand files to
//! decide what to delete, and on its way out told the engine what the cap
//! was. Everything downstream of the number depends on that having
//! happened -- `enginefs::retention::CacheBudget::Unknown` installs no
//! policy at all, so a process that has never published one holds no window
//! over a proxied stream and reclaims none of it -- and so the whole of the
//! retention story hung off a walk whose only purpose was eviction.
//!
//! So the two are separated here. The number lives in this module, with a
//! trigger of its own ([`start`]) that takes one `statvfs` and walks
//! nothing.
//!
//! # What supplies each input
//!
//! [`CacheLimit::effective`] wants two numbers.
//!
//! `available` is a `statvfs` of the volume ([`available_space`]): one
//! syscall, no walk, taken fresh at every publication.
//!
//! `occupied` is what the cache holds, and **the things that write the
//! cache are what count it**. The piece store keeps a bit per piece it
//! holds and the layout that gives each bit a size, so a torrent's
//! occupancy is a sum over words in memory
//! (`enginefs::EngineFS::cache_occupancy`); the proxy cache adds what each
//! chunk occupied as it lands and takes it off again when the owner unlinks
//! it (`crate::proxy_retention::ProxyRetention::occupancy`). Neither costs
//! a syscall, so the cap can be restated on a timer against a reading of
//! the cache that is current rather than against whatever a walk last
//! found.
//!
//! That is what this module was waiting for. The figure it used to publish
//! was the last eviction pass's count, which is right to within whatever
//! the cache has done since -- and **0 before the first walk finished**,
//! which is not a stale figure that is close but one wrong by the whole of
//! the cache: a four-gigabyte television already holding four gigabytes
//! with six hundred megabytes free stated a cap of eighty-eight megabytes,
//! so every stream in the first minutes of the process ran under a window
//! that size. On a device with sixteen thousand files on eMMC those minutes
//! were the whole of a film. The owners' count has no such window: a
//! process that has held a piece for a millisecond can say so.
//!
//! What it does not count is what nothing in this process wrote -- a
//! torrent held in Error, a directory a previous run left, the strays.
//! Reading those costs a `read_dir`, so they are read on demand for
//! `GET /cache.json` (`enginefs::EngineFS::cache_holdings`) and not on the
//! minute timer. The cap is therefore stated over what the session holds,
//! which understates the volume by whatever is unadopted -- the safe
//! direction, since a smaller `occupied` is a tighter cap, and the launch
//! sweep is what makes it nothing.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::debug;

use crate::state::AppState;

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
pub(crate) struct CacheLimit {
    /// `settings.cacheSize` in bytes: `u64::MAX` when unset, and 0 for the
    /// "no limit" the eviction rule has always read it as.
    pub(crate) configured: u64,
    /// Bytes the volume holding the cache will still give an unprivileged
    /// writer, or `None` when it could not be read.
    pub(crate) available: Option<u64>,
}

impl CacheLimit {
    /// A limit with no filesystem reading behind it: what the cleaner
    /// enforced before it had one, and what it falls back to when the volume
    /// cannot be probed. Written that way only by the tests -- `cache_roots`
    /// always carries whatever the probe returned, `None` included.
    #[cfg(test)]
    pub(crate) const fn configured(configured: u64) -> Self {
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
    pub(crate) fn effective(&self, occupied: u64) -> Option<u64> {
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
    pub(crate) fn disk_bound(&self, occupied: u64) -> bool {
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
pub(crate) fn available_space(path: &std::path::Path) -> Option<u64> {
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

/// How often the process restates the budget on its own, with nothing
/// writing to the cache and no pass running.
///
/// One `statvfs` per tick and no walk, so the interval is set by how long
/// the cap may be wrong for rather than by what it costs: the number it
/// re-reads is what the *rest* of the device has done to the volume, and a
/// player is inside a window for minutes at a time. A minute is the same
/// order as `cache_cleaner::CLEAN_DEBOUNCE`, so a cache something is
/// writing to is restated about as often as it was before this existed,
/// and an idle one -- which used to wait out the hourly fallback -- is now
/// restated on the minute like any other.
const BUDGET_INTERVAL: Duration = Duration::from_secs(60);

/// The cap a publication states, from the three readings behind it.
///
/// `configured` is `settings.cacheSize`, `available` one `statvfs` of the
/// volume, and `occupied` what the owners of the cache say they hold -- see
/// this module's header for what each of the three costs.
fn cap_to_publish(configured: u64, available: Option<u64>, occupied: u64) -> Option<u64> {
    CacheLimit {
        configured,
        available,
    }
    .effective(occupied)
}

/// Publish `limit` as the cache budget.
///
/// **The one writer.** Every publication in the process goes through here,
/// which is what makes the cap one number rather than one per caller: a
/// `RetentionBudget::set` beside this one is a second copy of a figure the
/// torrent half and the proxy half both read, and two copies is how two
/// layers come to evict against different limits.
///
/// It is handed the shared cell (`EngineFS::cache_budget`) rather than an
/// engine to tell, because the cell is the dependency.
pub(crate) fn publish(budget: &enginefs::retention::RetentionBudget, limit: Option<u64>) {
    budget.set(limit);
}

/// Read the volume and publish what it allows, now, without walking
/// anything.
///
/// **The one publisher.** `cacheSize` from the settings, `available` from
/// one `statvfs`, `occupied` from the two owners that count the cache as
/// they write it -- so this is as cheap on the minute timer as it is after
/// `POST /settings`, and there is no second path with a different reading
/// behind it. Returns the cap it stated, or `None` for no cap -- which is
/// not the same as having published nothing, since a publication of `None`
/// is still a publication.
pub(crate) async fn publish_now(state: &AppState) -> Option<u64> {
    let configured = {
        let settings = state.settings.read().await;
        crate::routes::system::cache_size_bytes(settings.cache_size)
    };
    // The root the session was opened on, not `settings.cacheRoot`, for the
    // reason `cache_cleaner::cache_roots` gives: the setting is where the
    // data will be after the next start, the engine is where it is now.
    let root = &state.engine.download_dir;
    let occupied = state.engine.cache_occupancy() + state.proxy_cache.retention().occupancy();
    let cap = cap_to_publish(configured, available_space(root), occupied);
    publish(&state.engine.cache_budget(), cap);
    cap
}

/// The budget's own trigger: state it now, and restate it every
/// [`BUDGET_INTERVAL`] thereafter.
///
/// Started unconditionally, and deliberately not under the cache cleaner's
/// switch: a server whose cleaner is off still relays streams into the
/// proxy cache, and without a published budget nothing bounds what they
/// leave behind.
///
/// **The first publication is this function's own, before it returns**,
/// rather than the first tick of the task it spawns. Both would happen at
/// about the same moment, and "about" is the difference between a claim a
/// test can make and one it cannot: awaited here, the budget exists before
/// `run` builds the router, so the first request of the process is served
/// by a bounded cache rather than by one that will be bounded shortly.
/// That is the whole point on a device with sixteen thousand cache files,
/// where the first walk of the root is minutes away
/// (`server/tests/proxy.rs`,
/// `a_stream_relayed_before_anything_has_walked_the_cache_is_still_bounded`).
pub async fn start(state: Arc<AppState>) -> JoinHandle<()> {
    let cap = publish_now(&state).await;
    debug!(
        ?cap,
        "published the cache budget from a reading of the volume"
    );
    tokio::spawn(restate_every(BUDGET_INTERVAL, move || {
        let state = state.clone();
        async move {
            let cap = publish_now(&state).await;
            debug!(
                ?cap,
                "published the cache budget from a reading of the volume"
            );
        }
    }))
}

/// The metronome under [`start`]: `restate` once every `every`, and not
/// before the first `every` has passed.
///
/// A tokio interval's first tick is immediate, and [`start`] has just
/// published before it spawns this, so that tick is taken here rather than
/// restating the same reading of the same volume microseconds later. Apart
/// from that this is the loop it looks like; it is a function so that the
/// interval can be pinned with the clock paused, without an `AppState` to
/// publish into.
async fn restate_every<F, Fut>(every: Duration, mut restate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut ticks = tokio::time::interval(every);
    ticks.tick().await;
    loop {
        ticks.tick().await;
        restate().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BUDGET_INTERVAL, CACHE_FREE_SPACE_FLOOR, CacheLimit, available_space, cap_to_publish,
        publish, restate_every,
    };
    use enginefs::piece_store::layout::FileSpec;
    use enginefs::piece_store::{PieceLayout, PieceStore, StoreRegistry, StoreRoot};
    use enginefs::retention::{CacheBudget, RetentionBudget};
    use std::sync::Arc;
    use std::time::Duration;

    const MIB: u64 = 1024 * 1024;
    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    /// **A process that has counted nothing still states a cap, and one
    /// held piece moves it by that piece's size.**
    ///
    /// The budget used to be the tail of an eviction pass, so until the
    /// first walk of the root finished the occupancy behind every cap was
    /// nobody's count, read as 0 -- and on a device whose cache is most of
    /// what is on the volume that is wrong by the whole of the cache. It is
    /// still the right answer for a cache that really holds nothing, which
    /// is the first half here. What the second half pins is that the figure
    /// no longer waits for a walk: the store books a piece the instant it
    /// lands, so the very next publication states a cap that piece's size
    /// bigger.
    #[test]
    fn a_cache_nothing_has_counted_is_capped_at_the_volumes_headroom() {
        let free = CACHE_FREE_SPACE_FLOOR + 8 * MIB;
        let tmp = tempfile::tempdir().unwrap();
        let registry = Arc::new(StoreRegistry::new(StoreRoot::in_download_dir(tmp.path())));
        assert_eq!(
            registry.occupancy(),
            0,
            "a session holding nothing holds nothing, which is not the same \
             as its being uncappable"
        );
        assert_eq!(
            cap_to_publish(u64::MAX, Some(free), registry.occupancy()),
            Some(8 * MIB),
            "the free space above the floor, with nothing assumed on top of it"
        );

        // One torrent of four one-mebibyte pieces, and one of them held.
        let layout = Arc::new(
            PieceLayout::new(MIB, 4 * MIB, [FileSpec::payload(4 * MIB)]).expect("a layout"),
        );
        let store = PieceStore::under(Arc::clone(&registry), HASH, layout);
        store.init_for_tests().unwrap();
        assert_eq!(registry.occupancy(), 0, "registered, and holding nothing");
        let piece = store.piece_path(0);
        std::fs::create_dir_all(piece.parent().unwrap()).unwrap();
        std::fs::write(&piece, vec![7u8; MIB as usize]).unwrap();
        // The seed a start makes, over a directory that now has a piece in
        // it: the same bit a completion would set.
        store.init_for_tests().unwrap();

        assert_eq!(registry.occupancy(), MIB);
        assert_eq!(
            cap_to_publish(u64::MAX, Some(free), registry.occupancy()),
            Some(9 * MIB),
            "what the cache already holds is room it may keep on holding, \
             from the moment it holds it"
        );
    }

    /// **The cap is sized from both owners of the cache, added.**
    ///
    /// A torrent's pieces and a proxied URL's chunks are two disjoint sets
    /// of bytes on one volume, and a cap sized from either alone is a cap
    /// over half the cache: a server relaying nothing would state the same
    /// number as one relaying a film. Neither reading costs a syscall,
    /// which is what lets [`super::publish_now`] take both on the minute.
    #[tokio::test]
    async fn the_cap_is_sized_from_both_owners_of_the_cache() {
        let free = CACHE_FREE_SPACE_FLOOR + 8 * MIB;
        let tmp = tempfile::tempdir().unwrap();
        let registry = Arc::new(StoreRegistry::new(StoreRoot::in_download_dir(tmp.path())));
        let layout = Arc::new(
            PieceLayout::new(MIB, 4 * MIB, [FileSpec::payload(4 * MIB)]).expect("a layout"),
        );
        let store = PieceStore::under(Arc::clone(&registry), HASH, layout);
        store.init_for_tests().unwrap();
        let piece = store.piece_path(0);
        std::fs::create_dir_all(piece.parent().unwrap()).unwrap();
        std::fs::write(&piece, vec![7u8; MIB as usize]).unwrap();
        store.init_for_tests().unwrap();

        let proxy = crate::proxy_retention::ProxyRetention::new(
            Arc::default(),
            Arc::default(),
            Arc::default(),
        );
        proxy.counted(2 * MIB);

        assert_eq!(
            cap_to_publish(
                u64::MAX,
                Some(free),
                registry.occupancy() + proxy.occupancy()
            ),
            Some(11 * MIB),
            "the volume's headroom plus what both owners hold"
        );
    }

    /// The publication itself: what the timer in [`super::start`] does on
    /// its first tick, before anything has written to the cache.
    #[test]
    fn a_budget_published_from_the_volume_alone_is_a_budget_like_any_other() {
        let budget = RetentionBudget::default();
        assert_eq!(
            budget.get(),
            CacheBudget::Unknown,
            "nothing has published one yet"
        );

        let cap = cap_to_publish(u64::MAX, Some(CACHE_FREE_SPACE_FLOOR + 8 * MIB), 0);
        publish(&budget, cap);
        assert_eq!(
            budget.get(),
            CacheBudget::Bytes(8 * MIB),
            "a reading of the volume is a cap whether or not a walk produced it"
        );
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

    /// **The cap is restated on the minute, and not before the first one.**
    ///
    /// The interval is set by how long the cap may be wrong for -- the
    /// number it re-reads is what the rest of the device did to the volume
    /// -- and a walk longer than it is overtaken with certainty, which the
    /// module doc leans on. So the value is pinned, not only the loop: a
    /// longer interval is a cap wrong for longer, a shorter one is a
    /// `statvfs` the device does not need.
    #[tokio::test(start_paused = true)]
    async fn the_budget_is_restated_once_a_minute_and_not_at_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        assert_eq!(BUDGET_INTERVAL, Duration::from_secs(60));

        let restated = Arc::new(AtomicUsize::new(0));
        let counter = restated.clone();
        let ticking = tokio::spawn(restate_every(BUDGET_INTERVAL, move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
        // Let the task reach its first await.
        tokio::task::yield_now().await;
        assert_eq!(
            restated.load(Ordering::SeqCst),
            0,
            "the publication at startup is `start`'s own; the metronome adds none"
        );

        tokio::time::advance(BUDGET_INTERVAL - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            restated.load(Ordering::SeqCst),
            0,
            "not before a minute has passed"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(restated.load(Ordering::SeqCst), 1, "once, on the minute");

        tokio::time::advance(BUDGET_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(
            restated.load(Ordering::SeqCst),
            2,
            "and again a minute later"
        );
        ticking.abort();
    }
}
