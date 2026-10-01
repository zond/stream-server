//! **Production speed** (`docs/design/renditions.md` §2.5): media time
//! written to the sink over the run's busy time, where busy time leaves out
//! the two waits the producer is not responsible for -- the sink blocking
//! (the lookahead is full: the receiver is paused or ahead) and the reader
//! waiting for bytes (the source is slow: a torrent stalling, an origin
//! taking its time). Both are measured where they happen, on the
//! producer's thread, by a [`WaitClock`] each.
//!
//! Every function here takes `now`: the clock is a parameter.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Time spent waiting, accumulated across waits, with the one in progress
/// counted up to the moment asked about.
#[derive(Default)]
pub(crate) struct WaitClock {
    inner: Mutex<(Duration, Option<Instant>)>,
}

impl WaitClock {
    fn lock(&self) -> std::sync::MutexGuard<'_, (Duration, Option<Instant>)> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A wait begins.
    pub(crate) fn enter(&self, now: Instant) {
        self.lock().1 = Some(now);
    }

    /// The wait that began ends.
    pub(crate) fn leave(&self, now: Instant) {
        let mut inner = self.lock();
        if let Some(since) = inner.1.take() {
            inner.0 += now.saturating_duration_since(since);
        }
    }

    /// Everything waited so far, the wait in progress included.
    pub(crate) fn total(&self, now: Instant) -> Duration {
        let inner = self.lock();
        inner.0
            + inner
                .1
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since))
    }
}

/// The verdict on a run too slow to keep up: how much film it made in how
/// much of its own time, over the window that failed it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TooSlow {
    pub media: Duration,
    pub busy: Duration,
}

/// The media time a sink has been written: the furthest presentation time,
/// from the first. Measured at the sink, as the samples are written, so a
/// sample waiting in the channel counts for the producer that made it.
#[derive(Default)]
pub(crate) struct MediaClock {
    inner: Mutex<Option<(i64, i64)>>,
}

impl MediaClock {
    /// A sample was written at `pts_us`.
    pub(crate) fn note(&self, pts_us: i64) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (first, high) = inner.get_or_insert((pts_us, pts_us));
        *high = (*high).max(pts_us).max(*first);
    }

    /// The film written so far.
    pub(crate) fn media(&self) -> Duration {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.map_or(Duration::ZERO, |(first, high)| {
            Duration::from_micros((high - first).max(0) as u64)
        })
    }
}

/// One run's speed: its media time against its busy time, judged over the
/// last `window` of busy time once its first segment is out.
pub(crate) struct Speed {
    window: Duration,
    /// `(busy, media)` readings since the first segment, oldest first.
    readings: VecDeque<(Duration, Duration)>,
    started: bool,
}

impl Speed {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            readings: VecDeque::new(),
            started: false,
        }
    }

    /// The first segment is out: from here on the run is judged, so a
    /// codec's start-up is never counted against it.
    pub(crate) fn start(&mut self, busy: Duration, media: Duration) {
        if !self.started {
            self.started = true;
            self.readings.push_back((busy, media));
        }
    }

    /// A reading at `busy`, with `media` written: `Some` when the last
    /// window of busy time made less than its own length of film.
    pub(crate) fn check(&mut self, busy: Duration, media: Duration) -> Option<TooSlow> {
        if !self.started {
            return None;
        }
        self.readings.push_back((busy, media));
        // The reading the window is measured from: the latest one at least
        // a window back. Older ones are no longer needed.
        while self
            .readings
            .get(1)
            .is_some_and(|(at, _)| busy.saturating_sub(*at) >= self.window)
        {
            self.readings.pop_front();
        }
        let (from_busy, from_media) = *self.readings.front()?;
        let busy_spent = busy.saturating_sub(from_busy);
        if busy_spent < self.window {
            return None;
        }
        let made = media.saturating_sub(from_media);
        (made < busy_spent).then_some(TooSlow {
            media: made,
            busy: busy_spent,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    #[test]
    fn a_wait_in_progress_counts_up_to_now() {
        let clock = WaitClock::default();
        let t0 = Instant::now();
        clock.enter(t0);
        assert_eq!(clock.total(t0 + S), S);
        clock.leave(t0 + 2 * S);
        clock.enter(t0 + 5 * S);
        assert_eq!(clock.total(t0 + 6 * S), 3 * S);
        clock.leave(t0 + 6 * S);
        assert_eq!(clock.total(t0 + 60 * S), 3 * S);
    }

    #[test]
    fn nothing_is_judged_before_the_first_segment_or_inside_a_window() {
        let mut speed = Speed::new(10 * S);
        assert_eq!(
            speed.check(100 * S, Duration::ZERO),
            None,
            "no segment out yet"
        );
        speed.start(100 * S, Duration::ZERO);
        assert_eq!(
            speed.check(109 * S, 5 * S),
            None,
            "less than a window of busy time"
        );
    }

    #[test]
    fn a_run_slower_than_real_time_over_a_window_is_too_slow() {
        let mut speed = Speed::new(10 * S);
        speed.start(S, Duration::ZERO);
        for second in 1..=10u64 {
            let verdict = speed.check(S + S * second as u32, Duration::from_millis(second * 700));
            if second < 10 {
                assert_eq!(verdict, None);
            } else {
                assert_eq!(
                    verdict,
                    Some(TooSlow {
                        media: Duration::from_millis(7000),
                        busy: 10 * S
                    })
                );
            }
        }
    }

    #[test]
    fn a_run_at_real_time_is_not() {
        let mut speed = Speed::new(10 * S);
        speed.start(Duration::ZERO, Duration::ZERO);
        for second in 1..=30u64 {
            assert_eq!(
                speed.check(S * second as u32, S * second as u32),
                None,
                "at {second} s"
            );
        }
    }

    #[test]
    fn media_is_the_furthest_written_from_the_first() {
        let clock = MediaClock::default();
        assert_eq!(clock.media(), Duration::ZERO);
        clock.note(4_000_000);
        clock.note(6_000_000);
        clock.note(5_000_000);
        assert_eq!(clock.media(), 2 * S);
    }
}
