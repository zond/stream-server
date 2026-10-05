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
//! and the film's length -- so it is asked for once the head is in, beside
//! the index read, rather than after it.
//!
//! This module is the policy, and nothing in it touches a torrent: where
//! the window is ([`window`]), when it is asked for and when it is let go
//! ([`Tracker`]). The reader task applies it (`super::reader`) and the
//! engine does the asking (`enginefs::engine::Engine::prewant`).

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
            elsewhere: 16 * MIB,
        }
    }
}

/// **Where to ask for**, in a file of `len` bytes, for a playback resuming
/// as `hint` says, or `None` when there is nothing to ask ahead for: a
/// playback from the top, or no memory near the resume time and no length
/// to share the file out by.
///
/// * **Remembered**, when the last session on this file ended within
///   [`Rules::near_ms`] of the resume time: from [`Rules::behind`] before
///   the offset it was reading to [`Rules::ahead`] past it, moved by the
///   gap in time at the film's own rate when the length is known.
/// * **Estimated** otherwise: the resume time's share of the file, and
///   either side of it the larger of [`Rules::half_per_mille`] of the file
///   and [`Rules::min_half`], never past [`Rules::max_half`].
///
/// Clamped to the file either way.
pub fn window(
    len: u64,
    hint: ResumeHint,
    remembered: Option<Remembered>,
    rules: Rules,
) -> Option<(Range<u64>, Basis)> {
    if hint.at_ms == 0 || len == 0 {
        return None;
    }
    let runtime = hint.runtime_ms.filter(|runtime| *runtime > 0);
    let at_rate = |ms: u64| -> Option<u64> {
        let runtime = runtime?;
        Some((u128::from(len) * u128::from(ms) / u128::from(runtime)).min(u128::from(len)) as u64)
    };
    let (start, end, basis) =
        match remembered.filter(|kept| kept.at_ms.abs_diff(hint.at_ms) <= rules.near_ms) {
            Some(kept) => {
                let gap = at_rate(kept.at_ms.abs_diff(hint.at_ms)).unwrap_or(0);
                let centre = if hint.at_ms >= kept.at_ms {
                    kept.offset.saturating_add(gap)
                } else {
                    kept.offset.saturating_sub(gap)
                };
                (
                    centre.saturating_sub(rules.behind),
                    centre.saturating_add(rules.ahead),
                    Basis::Remembered,
                )
            }
            None => {
                let centre = at_rate(hint.at_ms)?;
                let half = (u128::from(len) * u128::from(rules.half_per_mille) / 1000) as u64;
                let half = half.max(rules.min_half).min(rules.max_half);
                (
                    centre.saturating_sub(half),
                    centre.saturating_add(half),
                    Basis::Estimated,
                )
            }
        };
    let window = start.min(len)..end.min(len);
    (!window.is_empty()).then_some((window, basis))
}

/// What a served read asks of the pre-want.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Ask for this window now.
    Start(Range<u64>),
    /// Let the pre-want go.
    Stop,
    /// Nothing changes.
    Stay,
}

/// **When the window is asked for and when it is let go**, from the reads
/// the player is served. Clock-free: every transition is a read.
///
/// 1. **Waiting**, from the open, until the first read that delivers a
///    byte: the file's head, which the player needs before anything else,
///    has the swarm to itself. librqbit shares its priority list round
///    robin between streams, so asking alongside the head would halve the
///    head's share; asking after it costs the index read half of its share
///    instead, which the resume point needed next anyway.
/// 2. **Asking**, until the player reaches the window -- a read inside it,
///    and from there its own stream reads ahead -- or plays elsewhere: one
///    run of reads delivering [`Rules::elsewhere`] bytes outside it, which
///    a wrong estimate looks like and the head and index never do. A first
///    read already inside the window never starts it.
/// 3. **Done**, for the reader's life.
#[derive(Debug)]
pub struct Tracker {
    window: Range<u64>,
    phase: Phase,
    elsewhere: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Waiting,
    Asking {
        /// What this run of reads -- since the open or the last seek -- has
        /// delivered outside the window.
        outside: u64,
    },
    Done,
}

impl Tracker {
    /// A tracker for `window`, waiting for the first delivered byte.
    pub fn new(window: Range<u64>, rules: Rules) -> Self {
        Self {
            window,
            phase: Phase::Waiting,
            elsewhere: rules.elsewhere,
        }
    }

    /// The window this tracker asks for.
    pub fn window(&self) -> Range<u64> {
        self.window.clone()
    }

    /// A read was served from `begin` to `end`.
    pub fn served(&mut self, begin: u64, end: u64) -> Step {
        if end <= begin {
            return Step::Stay;
        }
        let inside = begin < self.window.end && self.window.start < end;
        match &mut self.phase {
            Phase::Done => Step::Stay,
            Phase::Waiting if inside => {
                self.phase = Phase::Done;
                Step::Stay
            }
            Phase::Waiting => {
                self.phase = Phase::Asking {
                    outside: end - begin,
                };
                Step::Start(self.window.clone())
            }
            Phase::Asking { .. } if inside => {
                self.phase = Phase::Done;
                Step::Stop
            }
            Phase::Asking { outside } => {
                *outside = outside.saturating_add(end - begin);
                if *outside >= self.elsewhere {
                    self.phase = Phase::Done;
                    Step::Stop
                } else {
                    Step::Stay
                }
            }
        }
    }

    /// The reader moved: a new run of reads starts.
    pub fn seeked(&mut self) {
        if let Phase::Asking { outside } = &mut self.phase {
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

    /// **Without a memory, the resume time's share of the file**, and
    /// either side of it the larger of 3% of the file and 64 MiB, never
    /// more than 128 MiB: a 4 GiB film resumed halfway is asked for from
    /// 2 GiB less 3% to 2 GiB plus 3%.
    #[test]
    fn an_estimate_is_the_resume_times_share_of_the_file() {
        let rules = Rules::default();
        let len = 4 * GIB;
        let half = len * 30 / 1000;
        assert_eq!(
            window(len, hint(3600, Some(7200)), None, rules),
            Some((2 * GIB - half..2 * GIB + half, Basis::Estimated))
        );
        // A small file: 64 MiB either side, clamped at the top of the file.
        assert_eq!(
            window(GIB, hint(152, Some(7200)), None, rules),
            Some((0..GIB * 152 / 7200 + 64 * MIB, Basis::Estimated))
        );
        // A huge one: no more than 128 MiB either side.
        let huge = 40 * GIB;
        assert_eq!(
            window(huge, hint(3600, Some(7200)), None, rules),
            Some((20 * GIB - 128 * MIB..20 * GIB + 128 * MIB, Basis::Estimated))
        );
        assert_eq!(
            window(len, hint(0, Some(7200)), None, rules),
            None,
            "from the top"
        );
        assert_eq!(
            window(len, hint(3600, None), None, rules),
            None,
            "no length"
        );
        assert_eq!(window(len, hint(3600, Some(0)), None, rules), None);
        assert_eq!(
            window(len, hint(9000, Some(7200)), None, rules),
            Some((len - half..len, Basis::Estimated)),
            "a resume time past the runtime is the file's end"
        );
    }

    /// **A memory near the resume time wins**: the window is around where
    /// the last session was reading -- mostly behind it, since the player
    /// read ahead of what it showed -- and moved by the gap in time at the
    /// film's rate. Too far away in time, the estimate stands.
    #[test]
    fn a_memory_near_the_resume_time_is_where_the_window_goes() {
        let rules = Rules::default();
        let len = 7200 * MIB;
        let kept = Remembered {
            offset: 3000 * MIB,
            at_ms: 3000 * 1000,
        };
        assert_eq!(
            window(len, hint(3000, None), Some(kept), rules),
            Some((2952 * MIB..3016 * MIB, Basis::Remembered)),
            "no length needed"
        );
        // Ten seconds later at a megabyte a second.
        assert_eq!(
            window(len, hint(3010, Some(7200)), Some(kept), rules),
            Some((2962 * MIB..3026 * MIB, Basis::Remembered))
        );
        assert_eq!(
            window(len, hint(2990, Some(7200)), Some(kept), rules),
            Some((2942 * MIB..3006 * MIB, Basis::Remembered))
        );
        assert_eq!(
            window(len, hint(3061, Some(7200)), Some(kept), rules).map(|(_, basis)| basis),
            Some(Basis::Estimated),
            "past a minute away, the memory is not this playback's"
        );
        assert_eq!(window(len, hint(3061, None), Some(kept), rules), None);
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

    /// **The head first, then the window, until the player reaches it**:
    /// nothing is asked before the first byte is served, the window is
    /// asked for after it, and a read inside the window lets it go -- for
    /// good.
    #[test]
    fn the_window_is_asked_for_after_the_head_and_let_go_when_the_player_reaches_it() {
        let mut tracker = tracker();
        assert_eq!(tracker.served(0, 0), Step::Stay, "nothing delivered yet");
        assert_eq!(
            tracker.served(0, 100),
            Step::Start(1000..2000),
            "the head is in"
        );
        assert_eq!(tracker.served(5000, 5100), Step::Stay, "the index");
        tracker.seeked();
        assert_eq!(
            tracker.served(900, 1001),
            Step::Stop,
            "the player is in the window"
        );
        assert_eq!(tracker.served(0, 100), Step::Stay, "and it stays let go");
        assert_eq!(tracker.served(5000, 6000), Step::Stay);
    }

    /// **A player that plays elsewhere lets the window go**: one run of
    /// reads delivering the threshold outside it. A seek starts a new run,
    /// so the head and the index, each short, never add up to it.
    #[test]
    fn a_player_playing_elsewhere_lets_the_window_go() {
        let mut tracker = tracker();
        assert_eq!(tracker.served(0, 100), Step::Start(1000..2000));
        assert_eq!(tracker.served(100, 250), Step::Stay, "250 of 300");
        tracker.seeked();
        assert_eq!(tracker.served(5000, 5200), Step::Stay, "a new run: 200");
        assert_eq!(tracker.served(5200, 5300), Step::Stop, "300 in one run");
        assert_eq!(tracker.served(1500, 1600), Step::Stay);
    }

    /// A first read already inside the window -- the head is the window --
    /// never asks: the player's own stream is reading there.
    #[test]
    fn a_first_read_inside_the_window_asks_for_nothing() {
        let mut tracker = Tracker::new(0..2000, Rules::default());
        assert_eq!(tracker.served(0, 100), Step::Stay);
        assert_eq!(tracker.served(5000, 5100), Step::Stay);
    }
}
