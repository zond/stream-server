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

/// What made a decision be taken now. It changes one thing only -- which
/// free-space line the volume is measured against ([`floor`]) -- and it
/// exists because a decision taken *because a stream is starting* is
/// answering a user who is waiting, while a decision taken by the timer is
/// answering nobody.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// The reconciler's own tick, over every torrent.
    Timer,
    /// A playback is starting on this torrent, right now.
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
    /// A stream, a file read, an HLS lease or a multi-file selection is
    /// live on this torrent.
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
    /// How long since anything was active on this torrent.
    pub idle_for: Duration,
}

/// Whether this torrent should be running, from the conditions alone.
///
/// The ladder, in order, and why each arm is where it is:
///
/// 1. **Not settled -> [`Decision::Stop`].** An `Initializing` reading is
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
/// 2. **`Error` -> [`Decision::Leave`].** A torrent the backend stopped
///    with an error is the cache cleaner's business
///    (`recover_out_of_space_torrents` reclaims space and restarts it) or
///    nobody's. Pausing it is meaningless and starting it would race the
///    cleaner.
/// 3. **No metadata -> [`Decision::Run`].** A resolving magnet must stay
///    connected to the swarm: the thing it is fetching is the info
///    dictionary, it writes no file data while it does, and stopping it is
///    how you make a magnet that never resolves. Above the free-space arm
///    for that reason -- it cannot fill a disk.
/// 4. **Writing, and the volume is under the line -> [`Decision::Stop`].**
///    Above `playing`, which is the point: librqbit writes the file it
///    wants straight to `ENOSPC` and calls that a fatal torrent error, so a
///    stream that is playing is exactly the torrent that will run the
///    volume to zero. A finished torrent writes nothing and so is never
///    stopped by this arm. Which line, and what an unreadable probe means,
///    is [`floor`].
/// 5. **Playing or pinned -> [`Decision::Run`].** Someone is watching it,
///    or someone asked for it offline.
/// 6. **Seeding off and idle -> [`Decision::Stop`].** The idle policy: with
///    seeding disabled and nothing playing, what a running torrent is doing
///    is fetching a film nobody is watching while we have promised to
///    upload nothing. [`crate::INACTIVE_TORRENT_PAUSE_GRACE`] of quiet
///    first, so a player that stops one segment and starts the next does
///    not stop and start the torrent with it.
/// 7. Otherwise **[`Decision::Run`]**.
pub fn desired(conditions: &Conditions, trigger: Trigger) -> Decision {
    match conditions.run_state {
        RunState::Initializing { .. } | RunState::Gone => return Decision::Stop,
        RunState::Error => return Decision::Leave,
        RunState::Live | RunState::Paused => {}
    }
    if !conditions.has_metadata {
        return Decision::Run;
    }
    // `wants_to_write`: only a torrent that still has data to fetch can
    // take the volume down.
    if !conditions.finished {
        match conditions.available {
            Some(available) if available < floor(trigger, conditions.run_state) => {
                return Decision::Stop;
            }
            // An unreadable volume is not a full one. For the timer that
            // means no opinion at all -- it will ask again in two seconds,
            // and a probe that has started failing is a broken environment,
            // not evidence about a disk. For a playback it means the user
            // who is waiting gets their stream: refusing to start a torrent
            // because `statvfs` failed would make an unreadable volume look
            // like a server that plays nothing.
            None => {
                return match trigger {
                    Trigger::Timer => Decision::Leave,
                    Trigger::PlaybackStart => Decision::Run,
                };
            }
            Some(_) => {}
        }
    }
    if conditions.playing || conditions.pinned {
        return Decision::Run;
    }
    if !conditions.seeding_enabled && conditions.idle_for >= crate::INACTIVE_TORRENT_PAUSE_GRACE {
        return Decision::Stop;
    }
    Decision::Run
}

/// The free-space line this decision is measured against, and the whole of
/// the hysteresis.
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
fn floor(trigger: Trigger, observed: RunState) -> u64 {
    match (trigger, observed) {
        (Trigger::PlaybackStart, _) | (_, RunState::Live) => CACHE_FREE_SPACE_FLOOR,
        _ => CACHE_FREE_SPACE_FLOOR.saturating_add(FREE_SPACE_RESUME_MARGIN),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::INACTIVE_TORRENT_PAUSE_GRACE;

    /// A torrent with nothing wrong with it: running, alive, watched by
    /// nobody, on a roomy volume, with seeding on. Every test below changes
    /// the one condition it is about.
    fn healthy() -> Conditions {
        Conditions {
            run_state: RunState::Live,
            playing: false,
            pinned: false,
            seeding_enabled: true,
            has_metadata: true,
            finished: false,
            available: Some(u64::MAX),
            idle_for: Duration::ZERO,
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
            idle_for: Duration::from_secs(86_400),
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

    /// Playback and pins outrank the idle policy: the whole reason the idle
    /// pause is safe is that it never applies to a torrent someone is
    /// using.
    #[test]
    fn playback_and_pins_outrank_the_idle_policy() {
        let idle_and_unseeded = Conditions {
            seeding_enabled: false,
            idle_for: INACTIVE_TORRENT_PAUSE_GRACE,
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

    /// The idle arm needs both halves -- seeding off *and* quiet for the
    /// grace -- and the grace is a real wait, not a formality: a player
    /// between two segment reads must not stop and start the torrent.
    #[test]
    fn the_idle_arm_needs_seeding_off_and_the_whole_grace() {
        let quiet = Conditions {
            seeding_enabled: false,
            idle_for: INACTIVE_TORRENT_PAUSE_GRACE,
            ..healthy()
        };
        assert_eq!(desired(&quiet, Trigger::Timer), Decision::Stop);
        assert_eq!(
            desired(
                &Conditions {
                    idle_for: INACTIVE_TORRENT_PAUSE_GRACE - Duration::from_millis(1),
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
                    idle_for: Duration::from_secs(86_400),
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
