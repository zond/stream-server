//! What the torrent-data volume may hold, and the one place the process
//! says so.
//!
//! **Publishing a budget is not the same job as evicting.** It used to be
//! the tail of one: the cache cleaner walked sixteen thousand files to
//! decide what to delete, and on its way out told the engine what the cap
//! was ([`publish`]). Everything downstream of the number depends on that
//! having happened -- `enginefs::retention::CacheBudget::Unknown` installs
//! no policy at all, so a process that has never published one holds no
//! window over a proxied stream and reclaims none of it -- and so the whole
//! of the retention story hung off a walk whose only purpose was eviction.
//!
//! So the two are separated here. The number and the order it is stated in
//! ([`CachePasses`]) live in this module, with a trigger of their own
//! ([`start`]) that takes one `statvfs` and walks nothing; eviction is one
//! more caller of [`publish`], stating the occupancy it happens to have
//! just counted.
//!
//! # What supplies each input
//!
//! [`CacheLimit::effective`] wants two numbers.
//!
//! `available` is a `statvfs` of the volume ([`available_space`]): one
//! syscall, no walk, taken fresh at every publication.
//!
//! `occupied` is what the cache holds, and there is no cheap source for it
//! -- nothing counts the bytes as they are written, so the only honest
//! reading is a walk of the root and a listing of the piece store
//! (`cache_cleaner::WalkInputs::run`). What this module does instead of
//! demanding one is to lean on the fact that **`occupied + available` does
//! not move when the cache does**: every byte the cache writes comes off
//! `available` and goes onto `occupied`, and every byte eviction takes does
//! the reverse, so the sum only changes when something that is *not* the
//! cache writes to the volume. A stale `occupied` against a fresh
//! `available` is therefore right to within what the rest of the device did
//! in the meantime, which is a far weaker dependency than waiting for a
//! walk.
//!
//! So [`occupancy_last_counted`] takes the last figure something counted
//! (`cache_cleaner::LastEviction`), and **nothing may drop that figure**:
//! [`publish_counted`] records it outside the claim the cap is published
//! under. The pass that walks is the only thing in the process that counts
//! the cache, while the publisher that overtakes it walked nothing -- one
//! `statvfs` and a publication microseconds later -- so every walk longer
//! than [`BUDGET_INTERVAL`] is overtaken with certainty, which on the
//! device this exists for (sixteen thousand files on eMMC, a statx each,
//! against a dentry cache memory pressure keeps evicting) is every walk. A
//! count that rode on the cap's claim would therefore be dropped for the
//! life of the process, and the fallback below would be the permanent
//! answer instead of the first minute's.
//!
//! Before anything has counted at all, that fallback is 0, and it is worth
//! being plain about what 0 costs rather than calling it merely tight. It
//! is not a stale figure that is close: it is wrong by the whole of the
//! cache. A four-gigabyte television already holding four gigabytes with
//! six hundred megabytes free states a cap of eighty-eight megabytes --
//! the free space above the floor and nothing else -- and that is a real
//! policy rather than none (`enginefs::retention::policy_for`), so pieces
//! of the stream outside a window that size become reclaimable and are
//! refetched if the player seeks back into them, and the proxy's window is
//! the same figure. What it is not is the stored cache being thrown away:
//! eviction sizes its cap from the occupancy its own walk counted
//! (`cache_cleaner::clean_cache_with_headroom`) and never from the
//! published number, so nothing deletes the tree on the strength of this.
//!
//! It is still the right absence, for two reasons rather than one. The
//! alternative is `CacheBudget::Unknown`, which installs no policy at all,
//! and an unbounded stream is what filled the volume this whole module
//! exists for. And the window in which 0 is the answer now ends at the
//! first walk, because no publisher can drop that walk's count -- which is
//! what makes it the first minute of a process rather than a state it can
//! be stuck in.
//!
//! What would have to change if the walk went away: something must still
//! keep [`occupancy_last_counted`]'s figure honest, or the disk arm of the
//! cap drifts by however much the cache has grown since the last count. A
//! per-entity owner that knows what it holds could report it; a running
//! total in the chunk store could too. Neither exists today, and this
//! module names the hole rather than hiding it.

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

/// The occupancy a publication that has not counted the cache itself uses:
/// what the last thing that *did* count it found, and 0 before anything
/// has.
///
/// See this module's header for why a stale figure is usable at all, what
/// 0 costs and why it is still the safe absence.
/// `cache_cleaner::LastEviction` is the supplier today, and the only one,
/// which is why [`publish_counted`] never lets a count be dropped; the
/// header says what would have to replace it.
fn occupancy_last_counted(last: &crate::cache_cleaner::LastEviction) -> u64 {
    last.get().map_or(0, |(_, report)| report.total)
}

/// The cap a publication states, from the two readings behind it.
///
/// `configured` is `settings.cacheSize`, `available` one `statvfs` of the
/// volume, and `last` whatever most recently counted the cache -- see this
/// module's header for why the third may be as old as it is, and what
/// stands in for it before anything has counted at all.
fn cap_to_publish(
    configured: u64,
    available: Option<u64>,
    last: &crate::cache_cleaner::LastEviction,
) -> Option<u64> {
    CacheLimit {
        configured,
        available,
    }
    .effective(occupancy_last_counted(last))
}

/// Publish `limit` as the cache budget, under `pass`'s place in the order
/// volume readings were taken, running `alongside` under the same claim.
///
/// **The one writer.** Every publication in the process goes through here,
/// which is what makes [`CachePasses`]'s ordering a property of the process
/// and not of one call site: a `RetentionBudget::set` beside this one is
/// exactly the bug that comment describes.
///
/// It is handed the shared cell (`EngineFS::cache_budget`) rather than an
/// engine to tell, because the cell is the dependency: the torrent half and
/// the proxy half read that one place, and a second copy of the number is
/// how two layers come to evict against different limits.
///
/// Publishing is all this does: what a pass *counted* is not published
/// here and is not dropped with the cap -- see [`publish_counted`], which
/// is the entry point a walk uses.
pub(crate) fn publish(
    passes: &CachePasses,
    budget: &enginefs::retention::RetentionBudget,
    pass: CachePass,
    limit: Option<u64>,
) {
    passes.publish(pass, || budget.set(limit));
}

/// Publish the cap a pass computed *and* keep the occupancy it counted,
/// whatever happens to the cap.
///
/// The asymmetry is the whole of this function. A cap is an answer about
/// the volume, so the newest reading of the volume wins and an older one is
/// dropped ([`CachePasses`]). A count is an answer about the *cache*, and
/// the only things that produce one are these walks: there is no second
/// supplier, so a dropped count is not replaced by anything, it is
/// subtracted. Ordering it by when the volume was read would also be
/// ordering it by the wrong key -- a walk is a reading of the tree taken
/// over its whole length, and the fresher of two is the one that finished
/// later, which is when this is called. So the count is written straight
/// in, last walk to finish wins, and the cap goes on through the claim.
///
/// What that costs, stated rather than implied: the report
/// `diagnostics::cache_figures` shows can now come from a pass
/// whose cap was dropped, so `report.limit` there may not be the cap in
/// force. It describes the walk it came from, which is what a diagnostic is
/// for; a count nobody kept described nothing.
pub(crate) fn publish_counted(
    passes: &CachePasses,
    budget: &enginefs::retention::RetentionBudget,
    last: &crate::cache_cleaner::LastEviction,
    pass: CachePass,
    report: &crate::cache_cleaner::EvictionReport,
) {
    last.record(report);
    publish(passes, budget, pass, report.limit);
}

/// Read the volume and publish what it allows, now, without walking
/// anything.
///
/// The entry point the process's own trigger uses ([`start`]) and the one
/// that does not need an eviction pass to have run: `cacheSize` from the
/// settings, `available` from one `statvfs`, `occupied` from whatever last
/// counted the cache ([`occupancy_last_counted`]). Returns the cap it
/// stated, or `None` for no cap -- which is not the same as having
/// published nothing, since a publication of `None` is still a publication.
pub(crate) async fn publish_now(state: &AppState) -> Option<u64> {
    // Numbered before the settings and the volume are read, which is what
    // this orders: see [`CachePasses`].
    let pass = state.cache_passes.begin();
    let configured = {
        let settings = state.settings.read().await;
        crate::routes::system::cache_size_bytes(settings.cache_size)
    };
    // The root the session was opened on, not `settings.cacheRoot`, for the
    // reason `cache_cleaner::cache_roots` gives: the setting is where the
    // data will be after the next start, the engine is where it is now.
    let root = &state.engine.download_dir;
    let cap = cap_to_publish(configured, available_space(root), &state.last_eviction);
    publish(&state.cache_passes, &state.engine.cache_budget(), pass, cap);
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
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(BUDGET_INTERVAL);
        // The interval's first tick is immediate and the publication above
        // has just happened, so it is taken here rather than restating the
        // same reading of the same volume microseconds later.
        ticks.tick().await;
        loop {
            ticks.tick().await;
            let cap = publish_now(&state).await;
            debug!(
                ?cap,
                "published the cache budget from a reading of the volume"
            );
        }
    })
}

/// Which pass's reading of the volume is the newest.
///
/// Passes overlap. The sweep every launch takes runs while the first
/// request is being served, a writer arms a debounced one, a client asks
/// for one over `POST /cache/clean`, a stopped torrent rings one -- and the
/// pass that finishes last is not the pass that started last, because a
/// walk of sixteen thousand files takes as long as it takes and `cacheSize`
/// can be changed while it runs.
///
/// Whoever finished last used to publish, so a pass that had read the cap
/// before it was lowered could put the old number back over the new one.
/// What that costs is not a slightly wrong cap: the retention policy is
/// sized from this number, and a budget that covers the entity installs no
/// policy at all ([`crate::proxy_retention`]), so a proxied stream measured
/// against a stale ten gigabytes is not bounded by a window at all -- every
/// chunk of it stays on the disk until the next pass republishes, which on
/// a cache nothing is writing to is an hour away.
///
/// So a pass takes a number before it reads the volume and publishes only
/// while nothing newer has: an older reading is dropped rather than
/// overwriting a newer one. Its eviction still happened -- deleting what
/// was over a cap that has since risen costs a refetch and nothing else --
/// and it is only the *reading* that is stale.
#[derive(Default)]
pub struct CachePasses {
    /// Numbers handed out, in the order passes started reading.
    started: std::sync::atomic::AtomicU64,
    /// The newest one that has published, so an older one can tell that it
    /// has been overtaken.
    published: std::sync::atomic::AtomicU64,
}

/// One pass's place in the order they started reading the volume in.
#[derive(Clone, Copy, Debug)]
pub struct CachePass(u64);

impl CachePasses {
    /// Number a pass that is about to read the volume.
    pub fn begin(&self) -> CachePass {
        CachePass(
            self.started
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1,
        )
    }

    /// Publish `pass`'s reading, if nothing newer has published one.
    ///
    /// The claim and the publication are one call so that there is nowhere
    /// to publish from that does not go through the order: an unguarded
    /// `RetentionBudget::set` beside this one is exactly the bug -- which
    /// is why [`super::publish`] is the only thing in the process that
    /// calls this, and everything that wants to state a cap goes through
    /// that.
    ///
    /// **One call is not one operation, and this does not totally order two
    /// publications.** Two passes numbered five and six can each win the
    /// claim -- five takes it while `published` is nought, six takes it
    /// while `published` is five -- and then run their closures the other
    /// way round, so six's cap lands first and five's stale one lands over
    /// it. What the claim removes is the *long* window: the reading used to
    /// be published after a walk of sixteen thousand files, and the race is
    /// now the few instructions between the `fetch_max` and the call. It is
    /// not nothing, and the cost when it is lost is what [`CachePasses`]
    /// describes -- a cap nobody has read the volume for, standing until the
    /// next pass republishes. Closing it means holding the claim across the
    /// closure under a mutex, which is a small change and an untestable one:
    /// the interleaving is a few instructions wide, so no deterministic test
    /// distinguishes the two. It is written down here rather than implied
    /// away.
    pub fn publish(&self, pass: CachePass, publish: impl FnOnce()) {
        if self
            .published
            .fetch_max(pass.0, std::sync::atomic::Ordering::Relaxed)
            < pass.0
        {
            publish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CACHE_FREE_SPACE_FLOOR, CacheLimit, CachePasses, available_space, cap_to_publish,
        occupancy_last_counted, publish, publish_counted,
    };
    use crate::cache_cleaner::{EvictionReport, LastEviction};
    use enginefs::retention::{CacheBudget, RetentionBudget};

    const MIB: u64 = 1024 * 1024;

    /// **A process that has walked nothing still states a cap.**
    ///
    /// The budget used to be the tail of an eviction pass, so until the
    /// first walk of the root finished there was no budget at all -- and
    /// `CacheBudget::Unknown` installs no retention policy, so every chunk
    /// of every stream relayed in that window stayed on the disk. On a
    /// device with sixteen thousand cache files on eMMC that window is not
    /// short. The occupancy such a publication has is nobody's count, which
    /// is read as 0: the smallest occupancy there can be, so the tightest
    /// cap the volume could justify, which is the side to be wrong on.
    #[test]
    fn a_cache_nothing_has_counted_is_still_capped_at_the_volumes_own_headroom() {
        let free = CACHE_FREE_SPACE_FLOOR + 8 * MIB;
        let nothing_counted = LastEviction::default();
        assert_eq!(
            occupancy_last_counted(&nothing_counted),
            0,
            "nothing has counted the cache, which is not the same as its being uncappable"
        );
        assert_eq!(
            cap_to_publish(u64::MAX, Some(free), &nothing_counted),
            Some(8 * MIB),
            "the free space above the floor, with nothing assumed on top of it"
        );

        // And once something has counted it, those bytes are room the cap
        // may include: the same volume reading, a bigger cap.
        let counted = LastEviction::default();
        counted.record(&EvictionReport {
            total: 3 * MIB,
            ..EvictionReport::default()
        });
        assert_eq!(occupancy_last_counted(&counted), 3 * MIB);
        assert_eq!(
            cap_to_publish(u64::MAX, Some(free), &counted),
            Some(11 * MIB),
            "what the cache already holds is room it may keep on holding"
        );
    }

    /// The publication itself, with no pass behind it: what the timer in
    /// [`super::start`] does on its first tick, before anything has written
    /// to the cache or walked it.
    #[test]
    fn a_budget_published_from_the_volume_alone_is_a_budget_like_any_other() {
        let passes = CachePasses::default();
        let budget = RetentionBudget::default();
        assert_eq!(
            budget.get(),
            CacheBudget::Unknown,
            "nothing has published one yet"
        );

        let cap = cap_to_publish(
            u64::MAX,
            Some(CACHE_FREE_SPACE_FLOOR + 8 * MIB),
            &LastEviction::default(),
        );
        publish(&passes, &budget, passes.begin(), cap);
        assert_eq!(
            budget.get(),
            CacheBudget::Bytes(8 * MIB),
            "a reading of the volume is a cap whether or not a walk produced it"
        );
    }

    /// **The publisher with its own trigger is not a second, unguarded
    /// writer -- and the walk it overtakes still hands over its count.**
    ///
    /// [`CachePasses`] exists because readings of the volume overlap and
    /// the one taken last is not the one published last. A publication that
    /// took its own reading and wrote the cell directly would reintroduce
    /// exactly that: the minute timer's reading, taken before a client
    /// lowered `cacheSize` over `POST /cache/clean`, landing on top of the
    /// pass that answered the client. So this publisher takes a number
    /// first and publishes through the same claim.
    ///
    /// The count is the other half, and it goes the other way. The timer
    /// takes one `statvfs` and publishes microseconds later, while a walk
    /// numbers itself and publishes sixteen thousand files afterwards, so
    /// the walk is the one that gets overtaken -- every time, on the device
    /// this is for. Dropping its cap costs a stale reading of the volume,
    /// which the next tick corrects. Dropping its *count* costs the only
    /// count there is: nothing else in the process walks the tree, so
    /// [`occupancy_last_counted`] would answer 0 for ever and every tick
    /// after it would state the volume's bare headroom as the whole budget.
    #[test]
    fn an_overtaken_walk_loses_its_cap_but_not_the_count_it_made() {
        let passes = CachePasses::default();
        let budget = RetentionBudget::default();
        let last = LastEviction::default();
        let free = CACHE_FREE_SPACE_FLOOR + 8 * MIB;
        let gib = 1024 * MIB;

        // A walk numbers itself and starts reading the tree.
        let walk = passes.begin();
        // The minute timer, while it is still going: nothing counted, so
        // the cap it states is the free space above the floor and nothing
        // else.
        let tick = passes.begin();
        publish(
            &passes,
            &budget,
            tick,
            cap_to_publish(u64::MAX, Some(free), &last),
        );
        assert_eq!(
            budget.get(),
            CacheBudget::Bytes(8 * MIB),
            "the tick states what a volume nobody has counted allows"
        );

        // And now the walk finishes: three gigabytes of cache counted, and
        // a cap that says the cache may keep them.
        let walked = EvictionReport {
            total: 3 * gib,
            limit: Some(3 * gib + 8 * MIB),
            ..EvictionReport::default()
        };
        publish_counted(&passes, &budget, &last, walk, &walked);

        assert_eq!(
            budget.get(),
            CacheBudget::Bytes(8 * MIB),
            "the older reading of the volume is still dropped rather than \
             published over the newer one"
        );
        assert_eq!(
            last.get().map(|(_, report)| report.total),
            Some(3 * gib),
            "but what it counted is not dropped with it: nothing else counts"
        );
        assert_eq!(
            cap_to_publish(u64::MAX, Some(free), &last),
            Some(3 * gib + 8 * MIB),
            "so the next tick states the cache's own room and not the \
             volume's bare headroom"
        );
    }

    /// **The cap that stands is the newest reading of the volume, not the
    /// last pass to finish.**
    ///
    /// Passes overlap -- the sweep every launch takes runs while the first
    /// request is being served, and `cacheSize` can be lowered while a walk
    /// of sixteen thousand files is still going. The pass that finished
    /// last used to publish, so the launch sweep's ten gigabytes could land
    /// on top of the eight megabytes a client had just asked for. The
    /// retention policy is sized from that number and installs no policy at
    /// all for a budget that covers the entity, so what the stale cap cost
    /// was not a slightly wrong bound but no bound: every chunk of every
    /// proxied stream stayed on the disk until something wrote to the cache
    /// and armed the next pass.
    #[test]
    fn a_pass_that_finishes_late_does_not_publish_its_cap_over_a_newer_one() {
        let passes = CachePasses::default();
        let launch = passes.begin();
        let asked_for = passes.begin();
        let published = std::cell::RefCell::new(Vec::new());
        passes.publish(asked_for, || {
            published.borrow_mut().push("the cap asked for")
        });
        passes.publish(launch, || published.borrow_mut().push("the launch sweep's"));
        let next = passes.begin();
        passes.publish(next, || published.borrow_mut().push("the pass after both"));
        assert_eq!(
            *published.borrow(),
            ["the cap asked for", "the pass after both"],
            "the older reading, finishing last, is the one that is dropped"
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
}
