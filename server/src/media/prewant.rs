//! **Pre-want: the resume point asked for while the player opens the file**
//! (`docs/design/media-pipeline.md`, "Pre-want").
//!
//! A film resumed halfway is read three times before its first frame: the
//! file's head (the container's header), its index (wherever the container
//! keeps it -- mpv asks, and nothing here guesses), and then the data at
//! the resume point. Each is fetched only once mpv blocks on it, so on a
//! weak swarm the waits add up: 38 s, then 26 s, then seconds per piece.
//! The resume point is the one of the three the server can know ahead --
//! from where the last session on the file ended, or from the resume time
//! and the film's length -- and all three are needed before the first
//! frame, so it is asked for **at the open, beside the head**: fetched one
//! after the other the waits add up, and sharing the swarm between them
//! costs the head little and the total nothing.
//!
//! This module is the policy, and nothing in it touches a torrent: where
//! the window is and where in it the resume point is taken to be
//! ([`plan`]), the order it is asked for in ([`Plan::steps`]), and when it
//! is let go ([`Tracker`]). The reader task applies it (`super::reader`)
//! and the engine does the asking (`enginefs::engine::Engine::prewant`).

use serde::{Deserialize, Serialize};
use std::ops::Range;

/// What the app says about a playback that resumes: where, and how long
/// the film is if it knows ([`crate::ServerHandle::set_resume`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeHint {
    /// Where playback resumes, in milliseconds of film.
    pub at_ms: u64,
    /// How long the film is, in milliseconds, if the app knows: what the
    /// last playback reported, or the catalogue's runtime.
    pub runtime_ms: Option<u64>,
}

/// Where a play session on a file ended: the byte the player was last
/// reading and the film time the app said it was at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Remembered {
    /// The end of the last read the player was served.
    pub offset: u64,
    /// The film time the app said playback was at when it left.
    pub at_ms: u64,
}

/// Why a window is where it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Basis {
    /// Where the last session on the file ended, near enough in time.
    Remembered,
    /// The resume time's share of the file's length.
    Estimated,
}

/// The numbers the policy runs on. [`Rules::default`] is the policy; a test
/// shrinks them to fit a fixture's few pieces
/// ([`crate::ServerHandle::set_prewant_rules`]).
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rules {
    /// The least an estimated window reaches either side of its centre.
    pub min_half: u64,
    /// The share of the file an estimated window reaches either side, in
    /// thousandths: a variable bitrate puts the resume point a few percent
    /// away from its share of the length.
    pub half_per_mille: u64,
    /// The most a window reaches either side, whatever the file.
    pub max_half: u64,
    /// How far behind the remembered offset a remembered window starts. The
    /// player's last read is ahead of what it was showing by its own
    /// buffer (32 MiB in xtremio), and a resumed seek lands on the keyframe
    /// before the resume time.
    pub behind: u64,
    /// How far past the remembered offset a remembered window reaches.
    pub ahead: u64,
    /// How near the resume time must be to the remembered one for the
    /// remembered offset to be used.
    pub near_ms: u64,
    /// How much of the window is asked for at once ([`Plan::steps`]): the
    /// reach of the one stream the window is walked with. It is what
    /// bounds the pre-want's place in the swarm's priorities, whatever the
    /// window's size -- the backend takes one piece of each stream in
    /// turn, and this is all the pre-want's stream ever has there.
    pub step: u64,
    /// The bytes one run of reads -- from an open or a seek -- may deliver
    /// outside the window before the player is taken to be playing
    /// somewhere else, and the pre-want is let go. The head and the index
    /// are each far less.
    pub elsewhere: u64,
}

const MIB: u64 = 1024 * 1024;

impl Default for Rules {
    fn default() -> Self {
        Self {
            min_half: 64 * MIB,
            half_per_mille: 30,
            max_half: 128 * MIB,
            behind: 48 * MIB,
            ahead: 16 * MIB,
            near_ms: 60_000,
            step: 16 * MIB,
            elsewhere: 16 * MIB,
        }
    }
}

/// Where to ask for, and where in it the resume point is taken to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Everything that may be asked for.
    pub window: Range<u64>,
    /// The likeliest byte of the resume point, inside the window: what is
    /// asked for first, and outward from.
    pub centre: u64,
    /// Why the window is where it is.
    pub basis: Basis,
}

/// **Where to ask for**, in a file of `len` bytes, for a playback resuming
/// as `hint` says, or `None` when there is nothing to ask ahead for: a
/// playback from the top, or no memory near the resume time and no length
/// to share the file out by.
///
/// * **Remembered**, when the last session on this file ended within
///   [`Rules::near_ms`] of the resume time: from [`Rules::behind`] before
///   the offset it was reading to [`Rules::ahead`] past it, moved by the
///   gap in time at the film's own rate when the length is known. The
///   centre is the middle of that: the picture was somewhere in the
///   player's buffer behind its last read, so with the default numbers the
///   first two steps are the 32 MiB behind the remembered byte, the half
///   nearer it first.
/// * **Estimated** otherwise: the resume time's share of the file is the
///   centre, and either side of it the larger of [`Rules::half_per_mille`]
///   of the file and [`Rules::min_half`], never past [`Rules::max_half`].
///
/// Clamped to the file either way.
pub fn plan(
    len: u64,
    hint: ResumeHint,
    remembered: Option<Remembered>,
    rules: Rules,
) -> Option<Plan> {
    if hint.at_ms == 0 || len == 0 {
        return None;
    }
    let runtime = hint.runtime_ms.filter(|runtime| *runtime > 0);
    let at_rate = |ms: u64| -> Option<u64> {
        let runtime = runtime?;
        Some((u128::from(len) * u128::from(ms) / u128::from(runtime)).min(u128::from(len)) as u64)
    };
    let (start, centre, end, basis) =
        match remembered.filter(|kept| kept.at_ms.abs_diff(hint.at_ms) <= rules.near_ms) {
            Some(kept) => {
                let gap = at_rate(kept.at_ms.abs_diff(hint.at_ms)).unwrap_or(0);
                let read_to = if hint.at_ms >= kept.at_ms {
                    kept.offset.saturating_add(gap)
                } else {
                    kept.offset.saturating_sub(gap)
                };
                let start = read_to.saturating_sub(rules.behind);
                let end = read_to.saturating_add(rules.ahead);
                (start, start + (end - start) / 2, end, Basis::Remembered)
            }
            None => {
                let centre = at_rate(hint.at_ms)?;
                let half = (u128::from(len) * u128::from(rules.half_per_mille) / 1000) as u64;
                let half = half.max(rules.min_half).min(rules.max_half);
                (
                    centre.saturating_sub(half),
                    centre,
                    centre.saturating_add(half),
                    Basis::Estimated,
                )
            }
        };
    let window = start.min(len)..end.min(len);
    (!window.is_empty()).then(|| Plan {
        centre: centre.clamp(window.start, window.end),
        window,
        basis,
    })
}

impl Plan {
    /// **The window in the order it is asked for**: `step` bytes at a time,
    /// from the centre outward -- the step ahead of the centre, the step
    /// behind it, the next ahead, the next behind -- until both sides
    /// reach the window's ends. Each step touches what was asked before
    /// it, so what has been asked for is always one run of the file.
    ///
    /// Outward, because the centre is a guess with an error either way and
    /// a sweep from the window's front would spend its first minute on a
    /// weak swarm on bytes far before the resume point; ahead first,
    /// because a player reads forward from wherever it lands.
    pub fn steps(&self, step: u64) -> Vec<Range<u64>> {
        let step = step.max(1);
        let (mut back, mut ahead) = (self.centre, self.centre);
        let mut steps = Vec::new();
        while ahead < self.window.end || back > self.window.start {
            if ahead < self.window.end {
                let to = ahead.saturating_add(step).min(self.window.end);
                steps.push(ahead..to);
                ahead = to;
            }
            if back > self.window.start {
                let from = back.saturating_sub(step).max(self.window.start);
                steps.push(from..back);
                back = from;
            }
        }
        steps
    }
}

/// **When the pre-want is let go**, from the reads the player is served.
/// Clock-free: every transition is a read.
///
/// It is asked for from the open. It is let go when the player reaches
/// the window -- a read inside it, and from there its own stream reads
/// ahead -- or plays elsewhere: one run of reads delivering
/// [`Rules::elsewhere`] bytes outside it, which a wrong estimate looks like
/// and the head and index never do. Once let go it stays so, for the
/// reader's life.
#[derive(Debug)]
pub struct Tracker {
    window: Range<u64>,
    /// What this run of reads -- since the open or the last seek -- has
    /// delivered outside the window; `None` once the pre-want is let go.
    outside: Option<u64>,
    elsewhere: u64,
}

impl Tracker {
    /// A tracker for a pre-want of `window`, asked for from now.
    pub fn new(window: Range<u64>, rules: Rules) -> Self {
        Self {
            window,
            outside: Some(0),
            elsewhere: rules.elsewhere,
        }
    }

    /// A read was served from `begin` to `end`. Whether the pre-want is to
    /// be let go now -- answered `true` once.
    pub fn served(&mut self, begin: u64, end: u64) -> bool {
        let Some(outside) = self.outside.as_mut() else {
            return false;
        };
        if end <= begin {
            return false;
        }
        let inside = begin < self.window.end && self.window.start < end;
        *outside = outside.saturating_add(end - begin);
        let lets_go = inside || *outside >= self.elsewhere;
        if lets_go {
            self.outside = None;
        }
        lets_go
    }

    /// The reader moved: a new run of reads starts.
    pub fn seeked(&mut self) {
        if let Some(outside) = self.outside.as_mut() {
            *outside = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;

    fn hint(at_s: u64, runtime_s: Option<u64>) -> ResumeHint {
        ResumeHint {
            at_ms: at_s * 1000,
            runtime_ms: runtime_s.map(|s| s * 1000),
        }
    }

    fn planned(
        len: u64,
        hint: ResumeHint,
        remembered: Option<Remembered>,
    ) -> Option<(Range<u64>, u64, Basis)> {
        plan(len, hint, remembered, Rules::default())
            .map(|plan| (plan.window, plan.centre, plan.basis))
    }

    /// **Without a memory, the resume time's share of the file**, and
    /// either side of it the larger of 3% of the file and 64 MiB, never
    /// more than 128 MiB: a 4 GiB film resumed halfway is asked for from
    /// 2 GiB less 3% to 2 GiB plus 3%, and from 2 GiB outward.
    #[test]
    fn an_estimate_is_the_resume_times_share_of_the_file() {
        let len = 4 * GIB;
        let half = len * 30 / 1000;
        assert_eq!(
            planned(len, hint(3600, Some(7200)), None),
            Some((2 * GIB - half..2 * GIB + half, 2 * GIB, Basis::Estimated))
        );
        // A small file: 64 MiB either side, clamped at the top of the file,
        // and the centre still the estimate.
        let centre = GIB * 152 / 7200;
        assert_eq!(
            planned(GIB, hint(152, Some(7200)), None),
            Some((0..centre + 64 * MIB, centre, Basis::Estimated))
        );
        // A huge one: no more than 128 MiB either side.
        let huge = 40 * GIB;
        assert_eq!(
            planned(huge, hint(3600, Some(7200)), None),
            Some((
                20 * GIB - 128 * MIB..20 * GIB + 128 * MIB,
                20 * GIB,
                Basis::Estimated
            ))
        );
        assert_eq!(planned(len, hint(0, Some(7200)), None), None, "the top");
        assert_eq!(planned(len, hint(3600, None), None), None, "no length");
        assert_eq!(planned(len, hint(3600, Some(0)), None), None);
        assert_eq!(
            planned(len, hint(9000, Some(7200)), None),
            Some((len - half..len, len, Basis::Estimated)),
            "a resume time past the runtime is the file's end"
        );
    }

    /// **A memory near the resume time wins**: the window is around where
    /// the last session was reading -- mostly behind it, since the player
    /// read ahead of what it showed -- its centre the middle of the player's
    /// buffer behind that byte, and all of it moved by the gap in time at
    /// the film's rate. Too far away in time, the estimate stands.
    #[test]
    fn a_memory_near_the_resume_time_is_where_the_window_goes() {
        let len = 7200 * MIB;
        let kept = Remembered {
            offset: 3000 * MIB,
            at_ms: 3000 * 1000,
        };
        assert_eq!(
            planned(len, hint(3000, None), Some(kept)),
            Some((2952 * MIB..3016 * MIB, 2984 * MIB, Basis::Remembered)),
            "no length needed"
        );
        // Ten seconds later at a megabyte a second.
        assert_eq!(
            planned(len, hint(3010, Some(7200)), Some(kept)),
            Some((2962 * MIB..3026 * MIB, 2994 * MIB, Basis::Remembered))
        );
        assert_eq!(
            planned(len, hint(2990, Some(7200)), Some(kept)),
            Some((2942 * MIB..3006 * MIB, 2974 * MIB, Basis::Remembered))
        );
        assert_eq!(
            planned(len, hint(3061, Some(7200)), Some(kept)).map(|(.., basis)| basis),
            Some(Basis::Estimated),
            "past a minute away, the memory is not this playback's"
        );
        assert_eq!(planned(len, hint(3061, None), Some(kept)), None);
    }

    /// **The window is asked for from the centre outward, a step at a
    /// time, ahead first**: every step touches what was asked before it,
    /// the ends are cut to the window, and a side that has reached its end
    /// leaves the rest to the other. A remembered window's first two steps
    /// are the player's buffer behind the byte it last read.
    #[test]
    fn the_steps_go_from_the_centre_outward_ahead_first() {
        let plan = Plan {
            window: 100..450,
            centre: 200,
            basis: Basis::Estimated,
        };
        assert_eq!(
            plan.steps(100),
            vec![200..300, 100..200, 300..400, 400..450],
            "behind is done after one step; ahead goes on, cut at the end"
        );
        let all_behind = Plan {
            window: 0..250,
            centre: 250,
            basis: Basis::Estimated,
        };
        assert_eq!(all_behind.steps(100), vec![150..250, 50..150, 0..50]);
        assert_eq!(all_behind.steps(0).len(), 250, "a step is at least a byte");

        let kept = Remembered {
            offset: 3000 * MIB,
            at_ms: 3000 * 1000,
        };
        let rules = Rules::default();
        let remembered = plan_of(7200 * MIB, hint(3000, None), Some(kept), rules);
        assert_eq!(
            remembered.steps(rules.step),
            vec![
                2984 * MIB..3000 * MIB,
                2968 * MIB..2984 * MIB,
                3000 * MIB..3016 * MIB,
                2952 * MIB..2968 * MIB,
            ]
        );
        // And everything in the window is asked for exactly once.
        let estimated = plan_of(4 * GIB, hint(3600, Some(7200)), None, rules);
        let mut steps = estimated.steps(rules.step);
        assert_eq!(steps[0].start, 2 * GIB, "the centre first");
        steps.sort_by_key(|step| step.start);
        assert_eq!(
            steps.first().map(|step| step.start),
            Some(estimated.window.start)
        );
        assert_eq!(
            steps.last().map(|step| step.end),
            Some(estimated.window.end)
        );
        assert!(steps.windows(2).all(|pair| pair[0].end == pair[1].start));
    }

    fn plan_of(len: u64, hint: ResumeHint, remembered: Option<Remembered>, rules: Rules) -> Plan {
        plan(len, hint, remembered, rules).expect("a plan")
    }

    fn tracker() -> Tracker {
        Tracker::new(
            1000..2000,
            Rules {
                elsewhere: 300,
                ..Rules::default()
            },
        )
    }

    /// **Asked for from the open, and let go when the player reaches the
    /// window**: the head and the index, read elsewhere, let nothing go,
    /// a read inside the window does -- once, and for good.
    #[test]
    fn the_window_is_let_go_when_the_player_reaches_it() {
        let mut tracker = tracker();
        assert!(!tracker.served(0, 0), "nothing delivered");
        assert!(!tracker.served(0, 100), "the head");
        tracker.seeked();
        assert!(!tracker.served(5000, 5100), "the index");
        tracker.seeked();
        assert!(tracker.served(900, 1001), "the player is in the window");
        assert!(!tracker.served(1001, 1100), "and it is let go only once");
        assert!(!tracker.served(5000, 6000));
    }

    /// **A player that plays elsewhere lets the window go**: one run of
    /// reads delivering the threshold outside it. A seek starts a new run,
    /// so the head and the index, each short, never add up to it.
    #[test]
    fn a_player_playing_elsewhere_lets_the_window_go() {
        let mut tracker = tracker();
        assert!(!tracker.served(0, 100));
        assert!(!tracker.served(100, 250), "250 of 300");
        tracker.seeked();
        assert!(!tracker.served(5000, 5200), "a new run: 200");
        assert!(tracker.served(5200, 5300), "300 in one run");
        assert!(!tracker.served(1500, 1600));
    }
}
