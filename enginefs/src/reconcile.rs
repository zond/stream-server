//! Whether a torrent should be running, recomputed from what is true now.
//!
//! There is one persisted pause bit in the backend and there are two
//! policies that pause: the idle one (nothing playing and seeding turned
//! off) and the free-space one (the volume the torrent writes to is under
//! [`CACHE_FREE_SPACE_FLOOR`]). A starting playback must lift the first and
//! must not lift the second, so every earlier round of this code kept an
//! in-memory record of *why* a torrent was paused -- `Engine::idle_paused`,
//! `Engine::stopped_for_space`, the backend's own set of the hashes it
//! idle-paused -- and asked that record before acting.
//!
//! Those records are claims about the past, held in memory that starts
//! empty while the pause they describe is persisted and survives the
//! restart. After a restart "empty" reads as "nobody paused it", so the
//! call sites that consult them are dead code in exactly the situation they
//! exist for, and a stored "stopped for space" would in any case be a claim
//! about a disk that may have been emptied since.
//!
//! So nothing here remembers anything. [`desired`] is a pure function of
//! conditions that are all readable now, and the set of facts that has to
//! survive a restart is empty. That works because both policies really are
//! functions of the present, and because there is no user-initiated pause
//! anywhere in this server's routes: no caller can say "leave this one
//! stopped, I meant it".

use crate::backend::RunState;
use crate::{CACHE_FREE_SPACE_FLOOR, FREE_SPACE_RESUME_MARGIN, FREE_SPACE_WATCH_INTERVAL};
use std::sync::Arc;
use std::time::Duration;

/// How often the reconciler recomputes every torrent's decision.
///
/// The same number as [`FREE_SPACE_WATCH_INTERVAL`], and deliberately the
/// same one: the free-space arm of [`desired`] is the watch's policy, and a
/// slower tick would let a torrent write further past the floor before
/// anything noticed (at 20 MB/s that is 40 MB per second of tick). Written
/// as an alias rather than a second `from_secs(2)` so the two cannot drift.
pub const RECONCILE_INTERVAL: Duration = FREE_SPACE_WATCH_INTERVAL;

/// What the reconciler wants of a torrent -- never why, and never what call
/// to make: mapping this onto the backend is the caller's job, because the
/// call that gets there depends on what the torrent is doing now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The torrent should be downloading (or seeding).
    Run,
    /// The torrent should not be running.
    Stop,
    /// Not ours: whatever state it is in, this reconcile has no opinion and
    /// must issue no call.
    Leave,
}

/// What made a decision be taken now. It changes three things -- which
/// free-space line the volume is measured against ([`line`]), whether the
/// anti-flap dwell applies, and whether the idle arm is walked at all
/// ([`verdict`]) -- and all three differences are the same difference: a
/// decision taken because *somebody is waiting for it* is answering a
/// person, while a decision taken by the timer is answering nobody. The
/// third is the one that is not a concession but a correctness rule: the
/// idle arm reads registers the asker may still be writing.
///
/// [`line`]: line
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// The reconciler's own tick, over every torrent.
    Timer,
    /// Somebody is waiting on this decision right now.
    ///
    /// Named for its main case, which is a playback starting on this
    /// torrent -- and it is also the pin of an offline download and the
    /// focus of a stream. All three are somebody about to read from this
    /// torrent, which is what the trigger is for: a torrent they are owed
    /// is measured against the floor rather than the resume line, and is
    /// not made to wait out a dwell that exists to protect an announce
    /// budget.
    ///
    /// The test is *waiting*, not *asked*. A user moving the seeding switch
    /// asked for something too, and that is still a [`Self::Timer`]
    /// decision: nothing is about to open a reader, so there is no reason
    /// to spend either concession on it.
    PlaybackStart,
}

/// Everything [`desired`] is allowed to know: all of it readable now, none
/// of it a record of something this process did earlier.
#[derive(Debug, Clone, Copy)]
pub struct Conditions {
    /// What the backend's state machine says the torrent is doing --
    /// [`crate::backend::TorrentHandle::run_state`], never its `paused`
    /// flag, which across an initial check is wrong in both directions.
    pub run_state: RunState,
    /// This process has re-applied its want-set to this torrent.
    ///
    /// The one record in the whole design that is a claim about something
    /// *this process did*, and it is here because starting false is the
    /// correct answer rather than a hole. A backend that sets piece reclaim
    /// ([`crate::backend::TorrentBackend::sets_piece_reclaim`]) restores
    /// every torrent paused and wanting every hole in its storage, because
    /// the piece-level want-set did not survive the record; until the
    /// caller has put the want-set back, starting one would have a seeder
    /// refill holes it is about to drop. A fresh process has re-applied
    /// nothing, so `false` at start is exactly true -- which is what every
    /// record this design deleted could not say.
    ///
    /// Not to be confused with a *settled reading* of
    /// [`Self::run_state`], which is about the backend's state machine and
    /// nothing to do with this process: see [`desired`]'s first two arms.
    pub settled: bool,
    /// A stream, a file read or a multi-file selection is live on this
    /// torrent.
    pub playing: bool,
    /// Some file of it is pinned as an offline download.
    pub pinned: bool,
    /// The user's seeding setting (session-wide).
    pub seeding_enabled: bool,
    /// The info dictionary is known. A magnet that is still resolving one
    /// has no files, no length and nothing to write.
    pub has_metadata: bool,
    /// The torrent has everything it wants, so it writes nothing.
    pub finished: bool,
    /// Free bytes on the volume the torrent writes to, or `None` when the
    /// probe failed. `None` is "unknown", never "full".
    pub available: Option<u64>,
    /// How long since anything was **using** this torrent -- a stream, a
    /// file read, a multi-file selection.
    ///
    /// `None` for a torrent nothing has been seen using at all -- a
    /// restored one, most often, since a torrent a previous process left
    /// behind comes back with no reading this process can vouch for
    /// (`Engine::forget_last_active`). That is an absence, not a zero and
    /// not a fresh `now`: filling it in would claim the torrent was active
    /// at boot and hand it a whole grace period on every restart, which is
    /// the same claim every record this design deleted was making. A
    /// torrent *this* process added is stamped with its creation instant,
    /// which is a real observation -- nothing can have used it in an
    /// interval that did not exist.
    ///
    /// It is **not** the registry's idle-eviction clock, which counts
    /// lookups: see `Engine::last_active_at`.
    ///
    /// The idle arm reads `None` as quiet: a torrent nobody is watching may
    /// be paused whether or not we can say for how long, and after a restart
    /// there is no recent stream for the grace to protect.
    pub idle_for: Option<Duration>,
}

/// Whether this torrent should be running, from the conditions alone.
///
/// The ladder, in order, and why each arm is where it is:
///
/// 1. **Not a settled reading -> [`Decision::Stop`].** An `Initializing` reading is
///    not a state anything may conclude from: where the torrent ends up
///    when its check finishes is decided by a `start_paused` captured when
///    the check began, which is not observable from out here (see
///    [`crate::backend::TorrentHandle::run_state`]). `Gone` is a torrent
///    the backend holds no state for at all. Neither is a reading that says
///    "this should be running", and the honest answer to "should it be
///    running?" is therefore no -- which is also the safe direction, since
///    the one way a check can end by surprise is the fastresume divergence
///    that takes a torrent *live*. What no caller may do is turn this into
///    a pause call on an initializing torrent: pausing one wedges it
///    (`file_ops.rs:113` bails the check, `mod.rs:590-593` returns `Ok`
///    without changing the state, and `wait_until_initialized` then polls a
///    torrent that has no check running for ever). An actuator acts on
///    settled readings; this arm exists so the arms below cannot read
///    `playing` or `available` off an unsettled one and conclude `Run`.
/// 2. **Want-set not re-applied -> [`Decision::Stop`].**
///    [`Conditions::settled`]: a torrent this process has not put its
///    want-set back on must not be started, because under piece reclaim it
///    would come up wanting every hole in its storage. Above the `Error`
///    arm, and it costs that arm nothing: the actuator's `Stop` calls
///    nothing on a torrent that is not running, so for an errored torrent
///    the two answers differ only in that this one also lets a read refusal
///    lapse -- which is right, since a torrent nobody has settled is not a
///    statement about a disk.
/// 3. **`Error` -> [`Decision::Leave`].** A torrent the backend stopped
///    with an error is the cache cleaner's business
///    (`recover_out_of_space_torrents` reclaims space and restarts it) or
///    nobody's. Pausing it is meaningless and starting it would race the
///    cleaner.
/// 4. **No metadata -> [`Decision::Run`].** A resolving magnet must stay
///    connected to the swarm: the thing it is fetching is the info
///    dictionary, it writes no file data while it does, and stopping it is
///    how you make a magnet that never resolves. Above the free-space arm
///    for that reason -- it cannot fill a disk.
/// 5. **Writing, and the volume is under the line -> [`Decision::Stop`].**
///    Above `playing`, which is the point: librqbit writes the file it
///    wants straight to `ENOSPC` and calls that a fatal torrent error, so a
///    stream that is playing is exactly the torrent that will run the
///    volume to zero. A finished torrent writes nothing and so is never
///    stopped by this arm. Which line, and what an unreadable probe means,
///    is `line` and [`volume_is_short`].
/// 6. **Playing or pinned -> [`Decision::Run`].** Someone is watching it,
///    or someone asked for it offline.
/// 7. **Seeding off and idle, on a [`Trigger::Timer`] -> [`Decision::Stop`].**
///    The idle policy: with seeding disabled and nothing playing, what a
///    running torrent is doing is fetching a film nobody is watching while
///    we have promised to upload nothing. `crate::INACTIVE_TORRENT_PAUSE_GRACE`
///    of quiet first, so a player that stops one segment and starts the next
///    does not stop and start the torrent with it -- and a torrent nothing
///    has been seen using at all ([`Conditions::idle_for`] `None`, a restored
///    one) counts as quiet, because there is no recent stream for the grace
///    to protect. The only arm the trigger can switch off, and the reason is
///    that this is the only arm whose inputs the *asker* is still writing:
///    see the comment on it.
/// 8. Otherwise **[`Decision::Run`]**.
pub fn desired(conditions: &Conditions, trigger: Trigger) -> Decision {
    verdict(conditions, trigger).decision
}

/// A [`desired`] decision together with the arm of the ladder that took it.
///
/// The arm is computed, never remembered -- it comes out of the same walk
/// of the same ladder as the decision, and is gone by the time the call
/// returns to anything that could store it. It is here because the two
/// stops mean different things to different readers: only the free-space
/// one is a statement about the device, which is what the statistics report
/// to a client (`Engine::is_stopped_for_space`), what the cache cleaner
/// evicts for, and -- while the idle policy still has an owner of its own
/// -- the only stop this reconciler is allowed to make.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    /// What should happen to the torrent.
    pub decision: Decision,
    /// The free-space arm is what decided. Only a [`Decision::Stop`] ever
    /// carries it.
    pub for_space: bool,
}

/// [`desired`] with the arm that took it, which is the whole ladder; the
/// arms and the reasons for their order are documented on [`desired`].
pub fn verdict(conditions: &Conditions, trigger: Trigger) -> Verdict {
    let arm = |decision| Verdict {
        decision,
        for_space: false,
    };
    match conditions.run_state {
        RunState::Initializing { .. } | RunState::Gone => return arm(Decision::Stop),
        RunState::Error if conditions.settled => return arm(Decision::Leave),
        RunState::Error | RunState::Live | RunState::Paused => {}
    }
    if !conditions.settled {
        return arm(Decision::Stop);
    }
    if !conditions.has_metadata {
        return arm(Decision::Run);
    }
    if volume_is_short(
        line(trigger, conditions.run_state),
        conditions.has_metadata,
        conditions.finished,
        conditions.available,
    ) {
        return Verdict {
            decision: Decision::Stop,
            for_space: true,
        };
    }
    // An unreadable volume is not a full one. For the timer that means no
    // opinion at all -- it will ask again in two seconds, and a probe that
    // has started failing is a broken environment, not evidence about a
    // disk. For a playback it means the user who is waiting gets their
    // stream: refusing to start a torrent because `statvfs` failed would
    // make an unreadable volume look like a server that plays nothing.
    if !conditions.finished && conditions.available.is_none() {
        return arm(match trigger {
            Trigger::Timer => Decision::Leave,
            Trigger::PlaybackStart => Decision::Run,
        });
    }
    if conditions.playing || conditions.pinned {
        return arm(Decision::Run);
    }
    // `None` is quiet: nothing has been seen using this torrent, so there is
    // no recent stream for the grace period to protect.
    let quiet = conditions
        .idle_for
        .is_none_or(|idle| idle >= crate::INACTIVE_TORRENT_PAUSE_GRACE);
    // And the idle policy is the timer's alone. `PlaybackStart` means
    // somebody is about to open a reader on *this* torrent, and "nothing
    // is playing" is never an answer to that: `playing` is read from
    // registers the caller may still be in the middle of writing, so a
    // caller that asks before it has finished registering gets the very
    // torrent it named stopped under it. `BackendEngineFS::focus_torrent`
    // is such a caller -- it writes no register at all -- and was safe
    // only because the one production call site happens to run
    // `on_stream_start` two lines earlier.
    //
    // The alternative was to let that caller stamp `Engine::last_active_at`
    // for itself, which buys the same answer by *inventing* the reading the
    // idle arm then treats as an observation, and buys it for a whole
    // `crate::INACTIVE_TORRENT_PAUSE_GRACE`. This costs one tick instead: a
    // torrent started for a reader that never comes is stopped by the next
    // `Timer` pass, `RECONCILE_INTERVAL` later, from registers that were
    // actually read.
    if trigger == Trigger::Timer && !conditions.seeding_enabled && quiet {
        return arm(Decision::Stop);
    }
    arm(Decision::Run)
}

/// The free-space arm of [`desired`] on its own: whether the volume this
/// torrent writes to is too short, against `line`, for it to be running.
///
/// `wants_to_write` is the first half -- only a torrent that still has data
/// to fetch can take a volume down, so a magnet that has not resolved its
/// info dictionary (no files, no length) and a torrent that has everything
/// it wants are both outside this arm however little room is left. The
/// second half is the reading against `line`.
///
/// It is a function of its own, and not a line inside the ladder, because
/// the ladder is not the only caller: `Engine::is_stopped_for_space` asks
/// the same question of a torrent that is already stopped, for the
/// statistics a client polls and for the cache cleaner's eviction classes.
/// A second copy of the test in either place is a policy in two halves that
/// drift, which is how the free-space watch came to skip the very torrents
/// the stream route was answering `507` for.
///
/// **The line is the caller's, and the two callers do not want the same
/// one.** The ladder asks "should I start this torrent?" and takes its line
/// from [`line`], which is the hysteresis; everything that asks "is this
/// device short?" -- the statistics a client reads, the cleaner's eviction
/// classes -- asks at [`CACHE_FREE_SPACE_FLOOR`], the same number
/// `ensure_download_disk_ready` answers `507` under and the cleaner keeps
/// free. Handing the hysteresis to those callers reports a volume the rest
/// of the server is happy with as out of disk, and takes the files of every
/// torrent that happens to be paused inside the band out of the protected
/// class.
pub fn volume_is_short(
    line: u64,
    has_metadata: bool,
    finished: bool,
    available: Option<u64>,
) -> bool {
    has_metadata && !finished && available.is_some_and(|available| available < line)
}

/// The free-space line the *ladder's* decision is measured against, and the
/// whole of the hysteresis.
///
/// One line would flap: a torrent stopped at the floor is started again the
/// moment the volume reads a byte over it, writes for two seconds, crosses
/// back under and is stopped again -- and each stop drops its peers while
/// each start re-announces. The classic fix is a stored "I stopped this
/// one" bit, and that bit is exactly what this design refuses to keep.
///
/// It does not need one: a Schmitt trigger's memory can be its own output,
/// and the output here is observable. A torrent that is **already running**
/// is measured against the floor, so it keeps running until the volume
/// actually falls under it; a torrent that is **stopped** is measured
/// against the floor plus [`FREE_SPACE_RESUME_MARGIN`], so it is not
/// started again until there is room to run into. Inside the band each
/// keeps doing what it is doing, which is the definition of no flapping.
///
/// A [`Trigger::PlaybackStart`] is measured against the floor whatever the
/// torrent is doing: the margin is there to stop a *timer* restarting
/// something into a nearly-full volume for no one, and a user pressing play
/// on a volume that has room above the floor is owed their stream.
///
/// It is the ladder's alone. A reader that is not deciding whether to make
/// a start/stop call wants [`CACHE_FREE_SPACE_FLOOR`] -- see
/// [`volume_is_short`].
pub(crate) fn line(trigger: Trigger, observed: RunState) -> u64 {
    match (trigger, observed) {
        (Trigger::PlaybackStart, _) | (_, RunState::Live) => CACHE_FREE_SPACE_FLOOR,
        _ => resume_line(),
    }
}

/// The last free-space reading of every volume the reconciler has looked
/// at, and how long each has been too short to run a torrent on.
///
/// This is not a record of anything the process decided. It is the
/// reconciler's most recent *observation* of a live input -- at most one
/// [`RECONCILE_INTERVAL`] old -- kept because the question "is this torrent
/// stopped because its volume is full?" is asked far more often than the
/// volume can usefully be measured: every `stats.json` poll asks it, and so
/// does every pass of the cache cleaner, over every engine. A `statvfs` per
/// asker would be a syscall per torrent per poll for a number that changes
/// on the scale of seconds.
///
/// Keyed by output folder rather than by torrent, because that is what a
/// volume is: two torrents writing to one folder share one reading and one
/// `Reading::short_since`, and a bound counted per torrent would start
/// the clock again for each of them (see `Self::short_for`).
pub struct Volumes {
    /// Where a torrent that names no output folder of its own writes --
    /// the engine's download directory.
    default_folder: std::path::PathBuf,
    readings: parking_lot::Mutex<std::collections::HashMap<std::path::PathBuf, Reading>>,
}

#[derive(Clone, Copy)]
struct Reading {
    /// Free bytes at the last probe, or `None` when that probe failed.
    /// `None` is "unknown" everywhere, never "full".
    available: Option<u64>,
    /// The clock reading when this volume was first seen with less than
    /// [`CACHE_FREE_SPACE_FLOOR`] + [`FREE_SPACE_RESUME_MARGIN`] free.
    ///
    /// That line, and not the floor, because this is the number the stall
    /// bound is judged on: a stopped torrent is not started again until the
    /// volume clears the margin, so between the floor and the margin its
    /// readers are still waiting for something that is not coming.
    short_since: Option<u64>,
}

impl Volumes {
    pub(crate) fn new(default_folder: std::path::PathBuf) -> Self {
        Self {
            default_folder,
            readings: Default::default(),
        }
    }

    /// The folder a torrent writes to: its own if the backend names one,
    /// otherwise the engine's download directory.
    pub(crate) fn folder_of(
        &self,
        output_folder: Option<std::path::PathBuf>,
    ) -> std::path::PathBuf {
        output_folder.unwrap_or_else(|| self.default_folder.clone())
    }

    /// Record a probe of `folder` taken at `now_secs`.
    ///
    /// A failed probe (`available` of `None`) leaves [`Reading::short_since`]
    /// exactly as it was: it is evidence neither that the volume filled nor
    /// that it cleared, and starting or clearing the stall clock on a
    /// `statvfs` that stopped answering would fail a player's reads because
    /// of a broken environment rather than because of a full disk.
    pub(crate) fn record(&self, folder: &std::path::Path, available: Option<u64>, now_secs: u64) {
        let mut readings = self.readings.lock();
        let reading = readings.entry(folder.to_path_buf()).or_insert(Reading {
            available: None,
            short_since: None,
        });
        reading.available = available;
        match available {
            Some(available) if available < resume_line() => {
                reading.short_since.get_or_insert(now_secs);
            }
            Some(_) => reading.short_since = None,
            None => {}
        }
    }

    /// The last reading of `folder`, or `None` for a volume nothing has
    /// probed yet and for one whose probe failed. Both are "unknown", which
    /// is what [`volume_is_short`] refuses to treat as full.
    pub fn available(&self, folder: &std::path::Path) -> Option<u64> {
        self.readings.lock().get(folder).and_then(|r| r.available)
    }

    /// How long `folder` has been under the line a stopped torrent has to
    /// see cleared before anything starts it again; `None` when it is not.
    ///
    /// Per volume and not per torrent, which is the point of it. The bound
    /// exists to fail reads that nothing is ever going to complete, and
    /// what decides that is the state of the disk, not when this particular
    /// torrent happened to be stopped: a second torrent stopped on the same
    /// full volume a minute later has readers as doomed as the first one's,
    /// and counting from its own stop would give them a fresh twenty
    /// seconds of spinning for a disk that has been full the whole time.
    pub(crate) fn short_for(&self, folder: &std::path::Path, now_secs: u64) -> Option<Duration> {
        let since = self
            .readings
            .lock()
            .get(folder)
            .and_then(|r| r.short_since)?;
        Some(Duration::from_secs(now_secs.saturating_sub(since)))
    }
}

/// The line a volume has to clear before a stopped torrent is started
/// again, which is [`floor`]'s upper arm.
fn resume_line() -> u64 {
    CACHE_FREE_SPACE_FLOOR.saturating_add(FREE_SPACE_RESUME_MARGIN)
}

/// One lock per info hash, so that reconciling one torrent never queues
/// behind reconciling another.
///
/// The obvious shape -- one `tokio::Mutex` around the whole policy -- is
/// what this replaces, and it was not merely inelegant: the pause call it
/// guarded is `Session::pause`, which flushes the session's persistence
/// file before it returns, so every torrent's decision waited on every
/// other torrent's disk write. A playback starting on one hash could sit
/// behind the idle arm's slow stop of an unrelated one.
///
/// Entries live only while a caller holds or waits for one, exactly as
/// `BackendEngineFS::pin_locks` does, so an engine that reconciles a
/// thousand torrents over a session keeps no thousand mutexes.
#[derive(Default)]
pub(crate) struct HashLocks {
    locks: parking_lot::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl HashLocks {
    /// Take this hash's lock, waiting for whoever holds it. The guard
    /// releases it -- and forgets the entry if nobody else wants it -- when
    /// it is dropped.
    pub(crate) async fn lock(&self, info_hash: &str) -> HashLockGuard<'_> {
        let lock = self
            .locks
            .lock()
            .entry(info_hash.to_string())
            .or_default()
            .clone();
        let guard = Some(Arc::clone(&lock).lock_owned().await);
        HashLockGuard {
            locks: self,
            info_hash: info_hash.to_string(),
            lock,
            guard,
        }
    }

    /// How many hashes currently have a lock in the map.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.lock().len()
    }
}

/// A held [`HashLocks`] entry.
pub(crate) struct HashLockGuard<'a> {
    locks: &'a HashLocks,
    info_hash: String,
    lock: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for HashLockGuard<'_> {
    fn drop(&mut self) {
        // The mutex first, the map second, and never the other way round: a
        // caller that arrived after the entry was dropped but before the
        // mutex was released would build itself a *second* mutex for the
        // same hash and hold it at the same time as us. Dropping the entry
        // is safe only for an entry nobody can still reach, and the
        // `strong_count` below is read under the map lock, which is the
        // same lock a new caller needs to clone the `Arc` -- so the count
        // cannot change while it is being read. Two is ours plus the map's:
        // a waiter, or a second guard, holds a third.
        self.guard.take();
        let mut locks = self.locks.locks.lock();
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(&self.info_hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::INACTIVE_TORRENT_PAUSE_GRACE;

    const TEST_HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba98";

    /// Two reconciles of the same torrent are one at a time: the second
    /// waits for the first, and gets in the moment it lets go.
    #[tokio::test(start_paused = true)]
    async fn one_hash_is_taken_by_one_caller_at_a_time() {
        let locks = Arc::new(HashLocks::default());
        let held = locks.lock(TEST_HASH).await;

        let waiter = tokio::spawn({
            let locks = locks.clone();
            async move {
                let _second = locks.lock(TEST_HASH).await;
            }
        });
        // A whole (virtual) second of scheduler time in which the waiter
        // is polled and gets nowhere.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !waiter.is_finished(),
            "the second caller got in while the first was holding the lock"
        );

        drop(held);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the second caller waited for the lock, not for ever")
            .expect("no panic");
    }

    /// The property the whole shape exists for. One torrent's reconcile can
    /// be arbitrarily slow -- the pause it makes flushes librqbit's session
    /// file to disk -- and it must hold up no other torrent's. A single
    /// global mutex would fail this.
    #[tokio::test(start_paused = true)]
    async fn a_slow_reconcile_holds_up_only_its_own_torrent() {
        let locks = HashLocks::default();
        let _slow = locks.lock(TEST_HASH).await;

        tokio::time::timeout(Duration::from_secs(5), locks.lock(OTHER_HASH))
            .await
            .expect("another torrent's reconcile is not behind this one");
    }

    /// Entries are per call, like the pin locks': one is kept while anybody
    /// holds or waits for it, and nothing is left behind afterwards.
    #[tokio::test(start_paused = true)]
    async fn a_lock_nobody_wants_is_not_kept() {
        let locks = Arc::new(HashLocks::default());
        {
            let _held = locks.lock(TEST_HASH).await;
            assert_eq!(locks.len(), 1);

            let waiter = tokio::spawn({
                let locks = locks.clone();
                async move {
                    let _second = locks.lock(TEST_HASH).await;
                }
            });
            tokio::task::yield_now().await;
            assert_eq!(locks.len(), 1, "the waiter's entry must survive");
            drop(_held);
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("the waiter got the lock")
                .expect("no panic");
        }
        assert_eq!(locks.len(), 0, "nothing is left behind");
    }

    /// The stall clock a volume keeps, which is what decides whether a
    /// stopped torrent's parked readers have anything coming.
    ///
    /// Three of its rules had nothing behind them until this test. It runs
    /// from the moment the volume goes short and is not restarted by later
    /// short readings, or a disk that stayed full would keep handing its
    /// readers fresh patience. It is judged against the **resume line** and
    /// not the floor, because between the two a stopped torrent is still
    /// not started: its readers are still waiting for something that is not
    /// coming. And a probe that failed clears nothing -- an unreadable
    /// `statvfs` is evidence neither that the volume filled nor that it
    /// emptied, and failing a player's reads because the environment broke
    /// is not what the bound is for.
    #[test]
    fn a_volumes_stall_clock_runs_from_the_moment_it_went_short() {
        use std::path::{Path, PathBuf};
        let volumes = Volumes::new(PathBuf::from("/downloads"));
        let folder = Path::new("/downloads");

        // Room: no clock at all.
        volumes.record(folder, Some(u64::MAX), 100);
        assert_eq!(volumes.available(folder), Some(u64::MAX));
        assert_eq!(volumes.short_for(folder, 200), None);

        // Short: the clock starts, and a later short reading does not
        // restart it.
        volumes.record(folder, Some(CACHE_FREE_SPACE_FLOOR - 1), 200);
        volumes.record(folder, Some(0), 210);
        assert_eq!(
            volumes.short_for(folder, 230),
            Some(Duration::from_secs(30))
        );

        // Over the floor but inside the resume margin: still short.
        volumes.record(
            folder,
            Some(CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1),
            240,
        );
        assert_eq!(
            volumes.short_for(folder, 240),
            Some(Duration::from_secs(40))
        );

        // A probe that failed leaves the clock exactly as it was, and the
        // reading unknown.
        volumes.record(folder, None, 250);
        assert_eq!(
            volumes.short_for(folder, 250),
            Some(Duration::from_secs(50))
        );
        assert_eq!(volumes.available(folder), None);

        // Cleared: the clock stops, and going short again starts a new one.
        volumes.record(
            folder,
            Some(CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN),
            260,
        );
        assert_eq!(volumes.short_for(folder, 260), None);
        volumes.record(folder, Some(0), 300);
        assert_eq!(
            volumes.short_for(folder, 310),
            Some(Duration::from_secs(10))
        );

        // A volume nothing has probed is unknown, never short -- and a
        // torrent that names no folder of its own is measured against the
        // engine's download directory.
        let elsewhere = Path::new("/elsewhere");
        assert_eq!(volumes.available(elsewhere), None);
        assert_eq!(volumes.short_for(elsewhere, 310), None);
        assert_eq!(volumes.folder_of(None), PathBuf::from("/downloads"));
        assert_eq!(
            volumes.folder_of(Some(PathBuf::from("/elsewhere"))),
            PathBuf::from("/elsewhere")
        );
    }

    /// A torrent with nothing wrong with it: running, alive, watched by
    /// nobody, on a roomy volume, with seeding on. Every test below changes
    /// the one condition it is about.
    fn healthy() -> Conditions {
        Conditions {
            run_state: RunState::Live,
            settled: true,
            playing: false,
            pinned: false,
            seeding_enabled: true,
            has_metadata: true,
            finished: false,
            available: Some(u64::MAX),
            idle_for: Some(Duration::ZERO),
        }
    }

    #[test]
    fn a_torrent_with_nothing_wrong_with_it_runs() {
        assert_eq!(desired(&healthy(), Trigger::Timer), Decision::Run);
        assert_eq!(desired(&healthy(), Trigger::PlaybackStart), Decision::Run);
    }

    /// An initializing torrent is a torrent whose fate its check will
    /// decide from a `start_paused` nobody out here can see, and a `Gone`
    /// one has no state at all. Neither says "this should be running", and
    /// no condition below the top of the ladder may argue otherwise.
    #[test]
    fn an_unsettled_reading_is_never_read_as_should_be_running() {
        for run_state in [
            RunState::Initializing {
                pause_requested: false,
            },
            RunState::Initializing {
                pause_requested: true,
            },
            RunState::Gone,
        ] {
            let watched = Conditions {
                run_state,
                playing: true,
                pinned: true,
                ..healthy()
            };
            assert_eq!(desired(&watched, Trigger::Timer), Decision::Stop);
            assert_eq!(desired(&watched, Trigger::PlaybackStart), Decision::Stop);
        }
    }

    /// A torrent the backend stopped with an error belongs to the cache
    /// cleaner's recovery, which reclaims space and restarts it. Starting it
    /// here would race that; pausing it means nothing.
    #[test]
    fn an_errored_torrent_is_left_to_the_cleaner() {
        let dead = Conditions {
            run_state: RunState::Error,
            playing: true,
            available: Some(0),
            ..healthy()
        };
        assert_eq!(desired(&dead, Trigger::Timer), Decision::Leave);
        assert_eq!(desired(&dead, Trigger::PlaybackStart), Decision::Leave);

        // Only once its want-set is back, though. Until then the torrent is
        // not the cleaner's either -- the cleaner will not restart one it
        // cannot give a want-set to -- and `Leave` would hold a read
        // refusal that nothing was ever going to lift.
        let unsettled = Conditions {
            settled: false,
            ..dead
        };
        assert_eq!(desired(&unsettled, Trigger::Timer), Decision::Stop);
        assert_eq!(desired(&unsettled, Trigger::PlaybackStart), Decision::Stop);
    }

    /// A magnet that has not resolved its info dictionary is fetching that
    /// dictionary from the swarm and writing no file data. Stopping it is
    /// how a magnet never resolves -- so not even a full volume, seeding
    /// off and a day of idleness stops one.
    #[test]
    fn a_resolving_magnet_runs_whatever_else_is_true() {
        let resolving = Conditions {
            has_metadata: false,
            available: Some(0),
            seeding_enabled: false,
            idle_for: Some(Duration::from_secs(86_400)),
            ..healthy()
        };
        assert_eq!(desired(&resolving, Trigger::Timer), Decision::Run);
    }

    /// The arm the free-space watch exists for, and it is above `playing`
    /// on purpose: the torrent that fills the volume is the one being
    /// watched.
    #[test]
    fn a_writing_torrent_under_the_floor_stops_even_while_watched() {
        let starving = Conditions {
            playing: true,
            pinned: true,
            available: Some(CACHE_FREE_SPACE_FLOOR - 1),
            ..healthy()
        };
        assert_eq!(desired(&starving, Trigger::Timer), Decision::Stop);
        assert_eq!(desired(&starving, Trigger::PlaybackStart), Decision::Stop);
    }

    /// A torrent that has everything it wants writes nothing, so the floor
    /// is not about it: stopping it would cost its seeding for no bytes.
    #[test]
    fn a_finished_torrent_is_not_stopped_by_a_full_volume() {
        let seeding = Conditions {
            finished: true,
            available: Some(0),
            ..healthy()
        };
        assert_eq!(desired(&seeding, Trigger::Timer), Decision::Run);
    }

    /// A probe that failed is not a reading of zero. The timer says nothing
    /// and asks again in two seconds; a playback start gets its stream,
    /// because a server that plays nothing whenever `statvfs` fails is
    /// worse than one that writes onto a volume it cannot measure.
    #[test]
    fn an_unreadable_volume_is_unknown_not_full() {
        let unreadable = Conditions {
            available: None,
            ..healthy()
        };
        assert_eq!(desired(&unreadable, Trigger::Timer), Decision::Leave);
        assert_eq!(desired(&unreadable, Trigger::PlaybackStart), Decision::Run);
    }

    /// A torrent nothing has been seen using is quiet, and quiet at once.
    ///
    /// This is the case a restart produces: librqbit brings the torrent back,
    /// nothing has opened a stream on it in this process, and the clock the
    /// idle arm measures on starts at the process, so there is no reading to
    /// take. `None` therefore has to mean "long ago" and not "just now".
    ///
    /// It went the other way three times. `idle_paused` said who paused a
    /// torrent and started empty, so a restart read "nobody". `last_accessed`
    /// said when it was last used and was seeded to the engine's construction,
    /// so a restart read "a moment ago" -- and a stats poll refreshed it, which
    /// kept idle torrents downloading all night. Then the seed became the
    /// process start, which is `0` on this clock and reads as "active at boot",
    /// handing every restored torrent a fresh grace period on every restart.
    ///
    /// The grace period is there to protect a stream that *just* stopped and
    /// might resume. After a restart there is no such stream, so there is
    /// nothing to protect and the pause is owed immediately.
    #[test]
    fn a_torrent_never_seen_in_use_is_quiet_without_waiting_out_the_grace() {
        let restored = Conditions {
            seeding_enabled: false,
            idle_for: None,
            ..healthy()
        };
        assert_eq!(desired(&restored, Trigger::Timer), Decision::Stop);

        // And the absence is doing the work: the same torrent with a reading
        // of zero has been used, a moment ago, and keeps its grace.
        let just_used = Conditions {
            idle_for: Some(Duration::ZERO),
            ..restored
        };
        assert_eq!(desired(&just_used, Trigger::Timer), Decision::Run);
    }

    /// Playback and pins outrank the idle policy: the whole reason the idle
    /// pause is safe is that it never applies to a torrent someone is
    /// using.
    #[test]
    fn playback_and_pins_outrank_the_idle_policy() {
        let idle_and_unseeded = Conditions {
            seeding_enabled: false,
            idle_for: Some(INACTIVE_TORRENT_PAUSE_GRACE),
            ..healthy()
        };
        assert_eq!(desired(&idle_and_unseeded, Trigger::Timer), Decision::Stop);
        assert_eq!(
            desired(
                &Conditions {
                    playing: true,
                    ..idle_and_unseeded
                },
                Trigger::Timer
            ),
            Decision::Run
        );
        assert_eq!(
            desired(
                &Conditions {
                    pinned: true,
                    ..idle_and_unseeded
                },
                Trigger::Timer
            ),
            Decision::Run
        );
    }

    /// The idle arm is the timer's. A `PlaybackStart` is somebody about to
    /// open a reader on this torrent, and `playing` is read from registers
    /// that caller may still be writing -- `BackendEngineFS::focus_torrent`
    /// writes none at all -- so answering "nothing is playing, stop it"
    /// would stop the very torrent the question was asked about.
    ///
    /// Every other arm is the trigger's equal: an unsettled reading, a
    /// resolving magnet and a volume under the floor answer the same to
    /// both, and that is the point -- this is the one arm whose inputs the
    /// asker is in the middle of writing.
    #[test]
    fn the_idle_arm_is_the_timers_and_a_playback_start_never_takes_it() {
        let quiet = Conditions {
            seeding_enabled: false,
            idle_for: Some(INACTIVE_TORRENT_PAUSE_GRACE),
            ..healthy()
        };
        assert_eq!(desired(&quiet, Trigger::Timer), Decision::Stop);
        assert_eq!(desired(&quiet, Trigger::PlaybackStart), Decision::Run);

        // A torrent nothing has ever been seen using -- a restored one --
        // is the same: quiet for the timer, owed to the asker.
        let never_seen = Conditions {
            idle_for: None,
            ..quiet
        };
        assert_eq!(desired(&never_seen, Trigger::Timer), Decision::Stop);
        assert_eq!(desired(&never_seen, Trigger::PlaybackStart), Decision::Run);

        // The concession stops at this arm. A volume under the floor still
        // stops the torrent the asker is waiting for.
        let starving = Conditions {
            available: Some(CACHE_FREE_SPACE_FLOOR - 1),
            ..quiet
        };
        assert_eq!(desired(&starving, Trigger::PlaybackStart), Decision::Stop);
    }

    /// The idle arm needs both halves -- seeding off *and* quiet for the
    /// grace -- and the grace is a real wait, not a formality: a player
    /// between two segment reads must not stop and start the torrent.
    #[test]
    fn the_idle_arm_needs_seeding_off_and_the_whole_grace() {
        let quiet = Conditions {
            seeding_enabled: false,
            idle_for: Some(INACTIVE_TORRENT_PAUSE_GRACE),
            ..healthy()
        };
        assert_eq!(desired(&quiet, Trigger::Timer), Decision::Stop);
        assert_eq!(
            desired(
                &Conditions {
                    idle_for: Some(INACTIVE_TORRENT_PAUSE_GRACE - Duration::from_millis(1)),
                    ..quiet
                },
                Trigger::Timer
            ),
            Decision::Run
        );
        assert_eq!(
            desired(
                &Conditions {
                    seeding_enabled: true,
                    idle_for: Some(Duration::from_secs(86_400)),
                    ..quiet
                },
                Trigger::Timer
            ),
            Decision::Run
        );
    }

    /// The hysteresis, from both ends. A running torrent is measured
    /// against the floor; a stopped one has to see the floor plus the
    /// margin before a timer starts it. The only thing that remembers which
    /// side it is on is the torrent itself.
    #[test]
    fn a_stopped_torrent_is_started_only_a_margin_over_the_floor() {
        let running = Conditions {
            available: Some(CACHE_FREE_SPACE_FLOOR),
            ..healthy()
        };
        assert_eq!(desired(&running, Trigger::Timer), Decision::Run);

        let stopped = Conditions {
            run_state: RunState::Paused,
            ..running
        };
        assert_eq!(desired(&stopped, Trigger::Timer), Decision::Stop);
        assert_eq!(
            desired(
                &Conditions {
                    available: Some(CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1),
                    ..stopped
                },
                Trigger::Timer
            ),
            Decision::Stop
        );
        assert_eq!(
            desired(
                &Conditions {
                    available: Some(CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN),
                    ..stopped
                },
                Trigger::Timer
            ),
            Decision::Run
        );
    }

    /// The margin is a rule for timers. A user pressing play is answered
    /// from the floor itself, so a stopped torrent on a volume with room
    /// above the floor starts for them.
    #[test]
    fn a_playback_start_measures_a_stopped_torrent_against_the_floor() {
        let stopped_at_the_floor = Conditions {
            run_state: RunState::Paused,
            available: Some(CACHE_FREE_SPACE_FLOOR),
            playing: true,
            ..healthy()
        };
        assert_eq!(
            desired(&stopped_at_the_floor, Trigger::Timer),
            Decision::Stop
        );
        assert_eq!(
            desired(&stopped_at_the_floor, Trigger::PlaybackStart),
            Decision::Run
        );
    }

    /// The test the hysteresis exists for. Park the volume anywhere inside
    /// the band and run the loop the reconciler will run -- decide, let the
    /// torrent follow the decision, decide again on what it is doing now --
    /// for thirty ticks. Both starting states must sit still: a running
    /// torrent keeps running, a stopped one stays stopped, and neither
    /// changes its mind once.
    #[test]
    fn nothing_flaps_inside_the_band() {
        for start in [RunState::Live, RunState::Paused] {
            let mut observed = start;
            let mut decisions = Vec::new();
            for tick in 0..30u64 {
                // Every reading inside [FLOOR, FLOOR + MARGIN), walked
                // across the band rather than held at one value, so the
                // test is about the band and not about one number in it.
                let available = CACHE_FREE_SPACE_FLOOR + (FREE_SPACE_RESUME_MARGIN - 1) * tick / 29;
                let decision = desired(
                    &Conditions {
                        run_state: observed,
                        available: Some(available),
                        ..healthy()
                    },
                    Trigger::Timer,
                );
                observed = match decision {
                    Decision::Run => RunState::Live,
                    Decision::Stop => RunState::Paused,
                    Decision::Leave => observed,
                };
                decisions.push(decision);
            }
            assert_eq!(observed, start, "the torrent moved: {decisions:?}");
            let first = decisions[0];
            assert!(
                decisions.iter().all(|decision| *decision == first),
                "the decision changed inside the band: {decisions:?}"
            );
        }
    }
}
