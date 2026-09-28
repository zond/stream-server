//! The one question a client asks of the connection, and the judge that
//! answers it.
//!
//! # What the question is
//!
//! "Is this server using your connection while you are not watching?" Serving
//! a peer counts. A background offline download counts. One meaning, and no
//! taxonomy of who is at the other end -- what a viewer is being told about
//! is the connection, not the peer. It is answered in two halves, up and
//! down, because a client shows them as one light with three glyphs and
//! offers a control for each, so each half has to be honest on its own; and
//! `active` is their disjunction.
//!
//! # Why the connection's own counters
//!
//! Not counted through the torrent storage, though a peer's request and a
//! download's write both cross it: so does librqbit's initial check --
//! every restored torrent is read back off the local disk at every startup
//! to be hashed, persistence is on, and the light would come on in a
//! session with no network at all. A reading of the disk cannot be a
//! reading of the connection. What can is what librqbit already
//! counts per torrent for exactly this purpose -- bytes received from peers
//! and bytes sent to them, [`crate::backend::TransferTotals`] -- summed over
//! the torrents that exist and compared against an earlier reading of the
//! same sum. Note that this is *not* what `/stats.json` reports: its
//! `downloaded` is librqbit's `progress_bytes`, the have-bytes, which the
//! initial check drives from 0 to the payload's length without a peer
//! being asked for anything, and the peer-received counter the light reads
//! (`fetched_bytes`) is exposed nowhere else. So on a restart with saved
//! torrents the stats show `downloaded` growing while the light stays
//! dark, and that disagreement is the point: one describes the disk, the
//! other the connection. What the two do share is the set of torrents they
//! sum over -- the one engine's, so a torrent cannot be in one and not the
//! other.
//!
//! **A download of what is not a torrent** -- an addon link, a Google Drive
//! file -- has no peers and no librqbit counters, and it uses the
//! connection all the same. Its filler counts what it brings in from the
//! origin ([`Reading::fetched_over_http`]), which lights the down half
//! beside the torrents'. Only what a download fetched: a player's own
//! requests to the proxy, a poster or a subtitle are not in it. The up
//! half stays the torrents' -- nothing else here uploads.
//!
//! The counters are the live state's, so a torrent that pauses or is removed
//! takes its bytes out of the sum; a sum that dropped reads as "not grown",
//! and a torrent that stopped is not using the connection. Nothing in the
//! path may create or re-add an engine to get a reading: a light that
//! started a torrent in order to report on it would be reporting on itself.
//!
//! # Where the judge sits
//!
//! [`TrafficWindow`] is arithmetic over two readings and a flag: it knows
//! nothing about engines, and it is fed by whoever can see all of them: the
//! server, which holds the one engine, takes the sum and the conjunction
//! with "nothing playing" there, in one place, and hands them over as one
//! [`BackgroundTraffic`]
//! -- never as two signals for a client to combine, because two signals
//! crossing an FFI boundary are sampled a moment apart and a light driven
//! by the pair flickers on every disagreement.

use std::time::Duration;

use crate::backend::TransferTotals;

/// The stretch of time "traffic is moving" is judged over.
///
/// A counter that has not grown since the last reading is the measurement --
/// a single sample of a total is not a rate, and there is nothing else here
/// to take a rate from. Five seconds is short enough that the answer is about
/// now, and long enough to cover the gaps a live connection has anyway: a
/// peer that requests a block every couple of seconds, or a download between
/// two chunks, is inside one window and keeps the light lit.
///
/// What it costs is honesty at the edges, and that is the right side to be
/// on. A torrent that is stalled -- connected, announcing, waiting on peers
/// -- moves no bytes and reads as idle here, because this is a light about
/// traffic and stalled traffic is no traffic. And nothing can be said at all
/// until one window has closed.
pub const TRAFFIC_WINDOW: Duration = Duration::from_secs(5);

/// How long a window may stretch before its verdict is thrown away rather
/// than reported.
///
/// The window is closed by whoever asks, so its length is really the caller's
/// polling interval, and a caller can stop asking: an app is backgrounded, or
/// a phone suspends the whole process mid-download. What comes back is a
/// window of minutes that nobody observed the middle of -- long enough that
/// "nothing was playing" is a claim about a stretch of time we watched a
/// vanishing fraction of. Past this bound the reading is used as a fresh
/// baseline and the answer is "not moving" until a window of ordinary length
/// closes on top of it.
pub const TRAFFIC_WINDOW_STALE_AFTER: Duration = Duration::from_secs(20);

const _: () = assert!(
    TRAFFIC_WINDOW.as_secs() < TRAFFIC_WINDOW_STALE_AFTER.as_secs(),
    "a window of the ordinary length must not be stale the moment it closes"
);

/// One reading of what the light is judged from, taken by whoever can see
/// all of it -- the server, in one place (see the module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reading {
    /// The torrents' own peer counters, summed over the torrents that exist.
    pub torrents: TransferTotals,
    /// Bytes the downloads of what is not a torrent have brought in from
    /// their origins since the process started. Only ever grows.
    pub fetched_over_http: u64,
    /// Whether a player is reading from the server right now
    /// (`BackendEngineFS::playback_is_live`).
    pub playing: bool,
    /// How many streams have started since the process did
    /// (`BackendEngineFS::playback_starts`). Only ever grows, so a change
    /// between two readings is a playback between them, however briefly it
    /// lasted -- one that began and ended between two polls is otherwise
    /// seen by neither, and the bytes it pulled in light the light.
    pub playback_starts: u64,
}

/// What a client's activity light is: each direction of the connection over
/// the last window, with nothing playing over it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackgroundTraffic {
    /// `downloading || uploading` -- "we are using your connection while you
    /// are not watching", whichever way the bytes went. The one-glyph
    /// answer; the two halves below are the three-glyph one.
    pub active: bool,
    /// Bytes came in from peers over the last closed window and nothing was
    /// playing over it or since.
    pub downloading: bool,
    /// Bytes went out to peers over the last closed window and nothing was
    /// playing over it or since.
    pub uploading: bool,
    /// Whether a player is reading from this server as this is answered.
    pub playing: bool,
    /// The sums the verdict was judged from: bytes received from and sent to
    /// peers, over the torrents that exist right now, and on the way down
    /// the bytes the downloads of what is not a torrent fetched. The
    /// torrents' part is not since the process started -- a torrent that
    /// pauses or leaves takes its bytes with it -- so a rate taken between
    /// two readings is a rate only while the set of torrents held still,
    /// which the verdict's own "grew" test already allows for.
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    /// The window the halves were judged over ([`TRAFFIC_WINDOW`]), in
    /// seconds.
    pub window_secs: u64,
}

/// The state one traffic verdict is kept in between readings.
///
/// Shared, not per caller: two callers asking must get the same answer, and a
/// per-caller delta would have each of them consuming windows the other never
/// sees. A call inside the current window returns the standing verdict
/// unchanged -- which is also what keeps a light from flickering at the
/// caller's polling rate rather than at the window's -- with one exception,
/// playback, which puts the light out the moment it is seen (see
/// [`TrafficWindow::sample`]).
///
/// It carries its own epoch (a `tokio::time::Instant`, so it follows a paused
/// test clock) rather than borrowing an engine's: the window is the
/// server's, fed from the engine and from the proxy's downloads alike, and
/// it is judged against one clock whatever either of them keeps.
#[derive(Debug)]
pub struct TrafficWindow {
    epoch: tokio::time::Instant,
    state: parking_lot::Mutex<WindowState>,
}

#[derive(Debug, Default)]
struct WindowState {
    /// False until a first reading has been taken. Nothing can be said before
    /// that: one sample of a total says only what the total is.
    started: bool,
    /// When the standing verdict's window closed, and the counters then.
    at_secs: u64,
    totals: TransferTotals,
    fetched_over_http: u64,
    /// The start count at the last reading, whichever window it was in.
    playback_starts: u64,
    /// The standing verdict, per half: the counter grew over that window and
    /// nothing was seen playing at any observation of it.
    downloading: bool,
    uploading: bool,
    /// Whether anything has been seen playing since the window closed.
    seen_playing: bool,
}

impl Default for TrafficWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl TrafficWindow {
    pub fn new() -> Self {
        Self {
            epoch: tokio::time::Instant::now(),
            state: parking_lot::Mutex::default(),
        }
    }

    /// Read the connection's counters against the last reading and answer
    /// the light's question, now. Playback is judged over the same window as
    /// the traffic: a window is "not watched" only if no observation of it,
    /// or of the time since, found a player -- where an observation finds
    /// one if a player is reading as it is taken, **or if a stream started
    /// since the reading before it** ([`Reading::playback_starts`]). The
    /// second half is what catches a short playback that fell between two
    /// polls, which the first cannot see.
    ///
    /// That is deliberately the pessimistic reading, and it is what keeps the
    /// light from blaming the viewer's own playback on the background. The
    /// bytes a player's stream pulled in are in the counters after they stop
    /// watching, so a verdict that asked "is anything playing *now*" would
    /// light up for one window every time playback ends. The cost is the
    /// other direction: after playback stops, the light can take up to two
    /// windows to come on for traffic that really is ours. Claiming nothing
    /// for ten seconds is a smaller error than claiming something untrue.
    ///
    /// Playback seen inside a window puts both halves out at once rather
    /// than at the window's close, and keeps them out for the rest of it:
    /// the standing verdict is about a stretch a player has now been seen
    /// in, and reporting it a moment longer would be reporting a fact that
    /// has stopped being one.
    pub fn sample(&self, reading: Reading) -> BackgroundTraffic {
        self.sample_at(self.epoch.elapsed().as_secs(), reading)
    }

    /// [`Self::sample`] at a caller-supplied clock, for tests that want the
    /// windows exact.
    pub fn sample_at(&self, now_secs: u64, reading: Reading) -> BackgroundTraffic {
        let Reading {
            torrents: totals,
            fetched_over_http,
            playing,
            playback_starts,
        } = reading;
        let mut state = self.state.lock();
        // A stream that started since the last reading was a player here,
        // even if it has gone again. The first reading has nothing to
        // compare with, and is a baseline anyway.
        let started = state.started && playback_starts != state.playback_starts;
        state.playback_starts = playback_starts;
        let seen = playing || started;
        state.seen_playing |= seen;

        let elapsed = now_secs.saturating_sub(state.at_secs);
        if !state.started || elapsed >= TRAFFIC_WINDOW.as_secs() {
            // A first reading is a baseline and nothing else, and so is one
            // taken after a gap nobody watched -- see
            // `TRAFFIC_WINDOW_STALE_AFTER`.
            let judged = state.started && elapsed <= TRAFFIC_WINDOW_STALE_AFTER.as_secs();
            let quiet = judged && !state.seen_playing;
            state.downloading = quiet
                && (totals.fetched > state.totals.fetched
                    || fetched_over_http > state.fetched_over_http);
            state.uploading = quiet && totals.uploaded > state.totals.uploaded;
            state.started = true;
            state.at_secs = now_secs;
            state.totals = totals;
            state.fetched_over_http = fetched_over_http;
            // This observation belongs to the window that just opened.
            state.seen_playing = seen;
        }

        let downloading = state.downloading && !state.seen_playing;
        let uploading = state.uploading && !state.seen_playing;
        BackgroundTraffic {
            active: downloading || uploading,
            downloading,
            uploading,
            playing,
            bytes_downloaded: totals.fetched.saturating_add(fetched_over_http),
            bytes_uploaded: totals.uploaded,
            window_secs: TRAFFIC_WINDOW.as_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const WINDOW: u64 = TRAFFIC_WINDOW.as_secs();

    /// A window and the connection it is reading: a test moves bytes in
    /// either direction where peers would.
    struct Fixture {
        window: TrafficWindow,
        totals: Cell<TransferTotals>,
        fetched_over_http: Cell<u64>,
        playback_starts: Cell<u64>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                window: TrafficWindow::new(),
                totals: Cell::default(),
                fetched_over_http: Cell::default(),
                playback_starts: Cell::default(),
            }
        }

        /// A download of what is not a torrent brought `bytes` in.
        fn fetch_over_http(&self, bytes: u64) {
            self.fetched_over_http
                .set(self.fetched_over_http.get() + bytes);
        }

        /// A stream started, and -- as far as any reading will see -- has
        /// ended by the next one.
        fn start_a_stream(&self) {
            self.playback_starts.set(self.playback_starts.get() + 1);
        }

        fn download(&self, bytes: u64) {
            let mut totals = self.totals.get();
            totals.fetched += bytes;
            self.totals.set(totals);
        }

        fn upload(&self, bytes: u64) {
            let mut totals = self.totals.get();
            totals.uploaded += bytes;
            self.totals.set(totals);
        }

        fn ask(&self, now: u64, playing: bool) -> BackgroundTraffic {
            self.window.sample_at(
                now,
                Reading {
                    torrents: self.totals.get(),
                    fetched_over_http: self.fetched_over_http.get(),
                    playing,
                    playback_starts: self.playback_starts.get(),
                },
            )
        }
    }

    #[test]
    fn nothing_can_be_said_from_a_single_reading() {
        let f = Fixture::new();
        f.download(1_000_000);
        let first = f.ask(0, false);
        assert!(
            !first.downloading && !first.active,
            "the first reading is a baseline: a total is not a rate, and everything the \
             torrents ever moved is in it"
        );

        f.download(1);
        assert!(
            f.ask(WINDOW, false).active,
            "the second reading is the rate"
        );
    }

    /// The two halves are judged apart, because a client shows them apart:
    /// one glyph for down, one for up, one for both, and a control for each.
    #[test]
    fn each_direction_is_its_own_half() {
        let f = Fixture::new();
        f.ask(0, false);

        f.download(1);
        let down = f.ask(WINDOW, false);
        assert!(down.downloading && !down.uploading && down.active);

        f.upload(1);
        let up = f.ask(WINDOW * 2, false);
        assert!(!up.downloading && up.uploading && up.active);

        f.download(1);
        f.upload(1);
        let both = f.ask(WINDOW * 3, false);
        assert!(both.downloading && both.uploading && both.active);

        let neither = f.ask(WINDOW * 4, false);
        assert!(!neither.downloading && !neither.uploading && !neither.active);
    }

    #[test]
    fn a_counter_that_stopped_growing_puts_the_light_out() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // Nothing moves over the next window.
        let quiet = f.ask(WINDOW * 2, false);
        assert!(!quiet.downloading && !quiet.active);
    }

    /// A torrent that paused or left takes its bytes out of the sum. Less
    /// than before is not more than before, so nothing is claimed.
    #[test]
    fn a_sum_that_dropped_has_not_grown() {
        let f = Fixture::new();
        f.download(1_000);
        f.upload(1_000);
        f.ask(0, false);

        f.totals.set(TransferTotals::default());
        let after = f.ask(WINDOW, false);
        assert!(!after.downloading && !after.uploading && !after.active);
        assert_eq!((after.bytes_downloaded, after.bytes_uploaded), (0, 0));
    }

    #[test]
    fn the_verdict_holds_for_the_length_of_a_window() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // Every caller inside the window gets the standing verdict, so the
        // light cannot flicker at the polling rate -- and asking twice does
        // not consume the window the other caller is measuring.
        for now in WINDOW + 1..WINDOW * 2 {
            assert!(f.ask(now, false).active, "verdict changed at {now}");
        }
        assert!(!f.ask(WINDOW * 2, false).active);
    }

    #[test]
    fn traffic_a_player_was_there_for_is_not_background() {
        let f = Fixture::new();
        f.ask(0, true);
        f.download(1_000);

        let playing = f.ask(WINDOW, true);
        assert!(playing.playing, "the player is reported as is");
        assert!(!playing.active, "and somebody was watching the bytes move");

        // Playback ends here, and the bytes it moved are still in the totals.
        // The window they moved in saw a player, so it is not ours to claim.
        f.download(1_000);
        assert!(
            !f.ask(WINDOW * 2, false).active,
            "the light lit for the window playback was in, one window after it ended"
        );

        // A window that nobody was watching any part of, and traffic in it.
        f.download(1_000);
        assert!(f.ask(WINDOW * 3, false).active);
    }

    #[test]
    fn a_player_seen_since_the_window_closed_puts_it_out_at_once() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        f.upload(1);
        let lit = f.ask(WINDOW, false);
        assert!(lit.downloading && lit.uploading);

        let started = f.ask(WINDOW + 1, true);
        assert!(
            !started.downloading && !started.uploading && !started.active,
            "playback that starts mid-window has to take both halves with it, not wait \
             for the window to close"
        );
        assert!(
            !f.ask(WINDOW + 2, false).active,
            "and the rest of that window is spoken for, whatever the next call sees"
        );
    }

    #[test]
    fn a_gap_nobody_watched_starts_the_measurement_over() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // The process was suspended, or the caller stopped asking. Whatever
        // moved in the meantime moved over a stretch we cannot say anything
        // about -- a player could have been in any of it.
        f.download(1_000_000);
        let after_the_gap = f.ask(WINDOW + TRAFFIC_WINDOW_STALE_AFTER.as_secs() + 1, false);
        assert!(
            !after_the_gap.downloading && !after_the_gap.active,
            "a window that stretched past the stale bound is a baseline, not a verdict"
        );

        // And it is a baseline, so the next ordinary window works again.
        f.download(1);
        assert!(
            f.ask(
                WINDOW + TRAFFIC_WINDOW_STALE_AFTER.as_secs() + 1 + WINDOW,
                false
            )
            .active
        );
    }

    #[test]
    fn the_totals_are_reported_whatever_the_verdict() {
        let f = Fixture::new();
        f.download(7);
        f.upload(11);
        let answer = f.ask(0, false);
        assert_eq!((answer.bytes_downloaded, answer.bytes_uploaded), (7, 11));
        assert_eq!(answer.window_secs, WINDOW);
    }

    /// **A playback that began and ended between two readings is still a
    /// playback.** Neither reading finds a player, so the live counters
    /// alone would call the window unwatched and the bytes that player
    /// pulled in would light the light -- the false light after a short
    /// play. The start count moved between them, and that is enough.
    #[test]
    fn a_playback_between_two_readings_is_seen() {
        let f = Fixture::new();
        f.ask(0, false);

        f.start_a_stream();
        f.download(1_000);
        f.upload(1_000);
        let closed = f.ask(WINDOW, false);
        assert!(
            !closed.downloading && !closed.uploading && !closed.active,
            "a stream started inside this window; its bytes are the viewer's"
        );

        // It spoke for the window it was in and the one the closing
        // reading opened, as a player seen at a close does -- and no more.
        f.download(1_000);
        assert!(!f.ask(WINDOW * 2, false).active);
        f.download(1_000);
        assert!(
            f.ask(WINDOW * 3, false).active,
            "a count that stopped moving is no player"
        );
    }

    /// The same inside a window: a start seen mid-window puts a standing
    /// verdict out at once, like a player seen there.
    #[test]
    fn a_start_seen_mid_window_puts_the_light_out() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        f.start_a_stream();
        assert!(!f.ask(WINDOW + 1, false).active);
        assert!(
            !f.ask(WINDOW + 2, false).active,
            "and stays out for the window"
        );
    }

    /// The first reading has no count to compare with, so a server that
    /// has played before the light first asked is not taken to be playing
    /// now -- and a first reading is a baseline anyway.
    #[test]
    fn the_first_readings_count_is_a_baseline() {
        let f = Fixture::new();
        f.playback_starts.set(7);
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);
    }

    /// **A download of what is not a torrent lights the down half**, and
    /// only that half: nothing but a torrent uploads.
    #[test]
    fn a_download_over_http_lights_the_way_down() {
        let f = Fixture::new();
        f.ask(0, false);
        f.fetch_over_http(64 * 1024);
        let lit = f.ask(WINDOW, false);
        assert!(lit.downloading && !lit.uploading && lit.active);
        assert_eq!(lit.bytes_downloaded, 64 * 1024);

        // Summed with the torrents' for the report, judged apart for the
        // verdict: a torrent that left cannot hide a download that grew.
        f.download(1_000);
        f.ask(WINDOW * 2, false);
        f.totals.set(TransferTotals::default());
        f.fetch_over_http(1);
        let after = f.ask(WINDOW * 3, false);
        assert!(
            after.downloading,
            "the torrents' sum dropped, the download's grew"
        );

        assert!(!f.ask(WINDOW * 4, false).active, "and it stopped");
    }

    /// And like the torrents' bytes, what a download fetched while a
    /// player was reading is not background.
    #[test]
    fn a_download_over_http_under_a_player_is_not_background() {
        let f = Fixture::new();
        f.ask(0, true);
        f.fetch_over_http(1);
        assert!(!f.ask(WINDOW, true).active);
    }

    /// `sample` without a clock argument reads the window's own epoch, which
    /// under a paused runtime is the test's clock.
    #[tokio::test(start_paused = true)]
    async fn the_window_keeps_its_own_clock() {
        let window = TrafficWindow::new();
        let mut reading = Reading::default();
        window.sample(reading);
        reading.torrents.fetched += 1;
        assert!(
            !window.sample(reading).active,
            "no time has passed, so the first window has not closed"
        );
        tokio::time::advance(TRAFFIC_WINDOW).await;
        assert!(window.sample(reading).downloading);
    }
}
