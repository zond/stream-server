use crate::backend::{
    EngineStats, TorrentHandle,
    priorities::{BufferProfile, PlaybackIntent},
};
use crate::cache::DataCache;
use anyhow::Context;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::files::FileHandle;
use regex::Regex;

/// Season/episode hints sent by stremio-video's createTorrent.js as
/// `guessFileIdx: {season, episode}` (stremio-core: `CreatedTorrent.guess_file_idx`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SeriesInfo {
    pub season: Option<usize>,
    pub episode: Option<usize>,
}

impl SeriesInfo {
    /// True when at least one of season/episode is present — an empty
    /// `guessFileIdx: {}` (movies) must not trigger episode-tag matching,
    /// otherwise resolution strings like `1920x1080` get treated as tags.
    pub fn has_hints(&self) -> bool {
        self.season.is_some() || self.episode.is_some()
    }
}

/// Extensions the guesser considers playable media (mirrors server.js).
const GUESS_MEDIA_EXTENSIONS: [&str; 14] = [
    ".mkv", ".avi", ".mp4", ".wmv", ".mov", ".mpg", ".ts", ".webm", ".flac", ".mp3", ".wav",
    ".wma", ".aac", ".ogg",
];

/// Guess which file in `files` should be played, mirroring server.js's
/// `guessFileIdx`: among media files, prefer ones whose name carries an
/// episode tag (`SxxEyy` or `NxM`, case-insensitive) matching `series_info`;
/// otherwise fall back to the largest media file. Non-media files are never
/// chosen. Ties on size resolve to the lowest file index. Returns an index
/// into `files`, or `None` when the list holds no media file at all.
pub fn guess_file_index_in(
    files: &[crate::backend::BackendFileInfo],
    series_info: Option<&SeriesInfo>,
) -> Option<usize> {
    use std::cmp::Reverse;
    use std::sync::LazyLock;

    static RE_SXE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[sS](\d+)[eE](\d+)").unwrap());
    static RE_XX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)x(\d+)").unwrap());

    let media_files: Vec<(usize, u64, String)> = files
        .iter()
        .enumerate()
        .filter_map(|(idx, file)| {
            let filename = file.name.to_lowercase();
            if GUESS_MEDIA_EXTENSIONS
                .iter()
                .any(|ext| filename.ends_with(ext))
            {
                Some((idx, file.length, filename))
            } else {
                None
            }
        })
        .collect();

    if media_files.is_empty() {
        return None;
    }

    if let Some(series) = series_info.filter(|series| series.has_hints()) {
        let mut candidates = Vec::new();
        for (idx, length, filename) in &media_files {
            let mut found_s = None;
            let mut found_e = None;

            if let Some(caps) = RE_SXE.captures(filename) {
                found_s = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
                found_e = caps.get(2).and_then(|m| m.as_str().parse::<usize>().ok());
            } else if let Some(caps) = RE_XX.captures(filename) {
                found_s = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
                found_e = caps.get(2).and_then(|m| m.as_str().parse::<usize>().ok());
            }

            let s_match = series.season.is_none() || found_s == series.season;
            let e_match = series.episode.is_none() || found_e == series.episode;

            if s_match && e_match && (found_s.is_some() || found_e.is_some()) {
                candidates.push((*idx, *length));
            }
        }
        if !candidates.is_empty() {
            return candidates
                .into_iter()
                .max_by_key(|&(idx, len)| (len, Reverse(idx)))
                .map(|(idx, _)| idx);
        }
    }

    // Fallback to the largest media file
    media_files
        .into_iter()
        .max_by_key(|&(idx, len, _)| (len, Reverse(idx)))
        .map(|(idx, _, _)| idx)
}

#[cfg(test)]
mod guess_tests {
    use super::{SeriesInfo, guess_file_index_in};
    use crate::backend::BackendFileInfo;

    fn f(name: &str, length: u64) -> BackendFileInfo {
        BackendFileInfo {
            name: name.to_string(),
            length,
        }
    }

    fn si(season: Option<usize>, episode: Option<usize>) -> SeriesInfo {
        SeriesInfo { season, episode }
    }

    #[test]
    fn sxe_tag_matches_requested_season_episode() {
        let files = [
            f("Show.S01E01.mkv", 100),
            f("Show.S02E05.mkv", 200),
            f("Show.S02E06.mkv", 150),
        ];
        let info = si(Some(2), Some(5));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn nxm_tag_matches_requested_season_episode() {
        let files = [
            f("Ep.2x07.mkv", 100),
            f("Ep.3x07.mkv", 120),
            f("Ep.3x08.mkv", 110),
        ];
        let info = si(Some(3), Some(7));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn season_only_hint_picks_largest_matching_season() {
        let files = [
            f("A.S01E01.mkv", 100),
            f("B.S02E01.mkv", 100),
            f("C.S02E05.mkv", 300),
        ];
        let info = si(Some(2), None);
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(2));
    }

    #[test]
    fn episode_only_hint_picks_largest_matching_episode() {
        let files = [
            f("A.S01E05.mkv", 100),
            f("B.S02E05.mkv", 200),
            f("C.S03E01.mkv", 100),
        ];
        let info = si(None, Some(5));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn size_tie_resolves_to_lowest_index() {
        let files = [f("A.S01E01.mkv", 100), f("B.S01E01.mkv", 100)];
        let info = si(Some(1), Some(1));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(0));
    }

    #[test]
    fn non_media_files_never_chosen_even_when_largest() {
        let files = [
            f("big.nfo", 999_999),
            f("movie.mkv", 100),
            f("subs.srt", 5_000),
            f("installer.exe", 8_000),
        ];
        // Movie case: no hints, must fall back to the only media file.
        assert_eq!(guess_file_index_in(&files, None), Some(1));
    }

    #[test]
    fn non_media_with_matching_tag_still_excluded() {
        // A .nfo carrying the requested SxxEyy tag must not win over the media file.
        let files = [f("Show.S01E01.nfo", 999_999), f("Show.S01E01.mkv", 100)];
        let info = si(Some(1), Some(1));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn empty_series_info_does_not_read_resolution_as_tag() {
        // guessFileIdx:{} (movie) => has_hints()==false: a "1920x1080" filename
        // must NOT be treated as season 1920 episode 1080; falls back to largest.
        let files = [f("Movie.1920x1080.mkv", 100), f("Movie.Extras.mkv", 200)];
        let info = SeriesInfo::default();
        assert!(!info.has_hints());
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn no_episode_match_falls_back_to_largest_media() {
        let files = [f("A.S01E01.mkv", 100), f("B.S02E02.mkv", 300)];
        let info = si(Some(9), Some(9));
        assert_eq!(guess_file_index_in(&files, Some(&info)), Some(1));
    }

    #[test]
    fn zero_media_files_returns_none() {
        let files = [f("a.txt", 100), f("b.nfo", 200)];
        assert_eq!(
            guess_file_index_in(&files, Some(&si(Some(1), Some(1)))),
            None
        );
        assert_eq!(guess_file_index_in(&[], None), None);
    }
}

/// Why `Engine::try_get_file_with_intent` could not hand out a reader.
/// `Backend` wraps the backend's error unchanged so callers can downcast it
/// (e.g. to `crate::backend::librqbit::TorrentInitError`) for a precise
/// HTTP status.
#[derive(Debug, thiserror::Error)]
pub enum GetFileError {
    #[error("file index {file_idx} out of range ({file_count} files)")]
    FileNotFound { file_idx: usize, file_count: usize },
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

impl GetFileError {
    /// The torrent never left its initializing state (timeout or init
    /// failure), if that is what failed.
    pub fn torrent_init_error(&self) -> Option<&crate::backend::librqbit::TorrentInitError> {
        match self {
            GetFileError::Backend(e) => e.downcast_ref(),
            GetFileError::FileNotFound { .. } => None,
        }
    }
}

/// [`Engine::last_active_at`] for a torrent nothing has been seen using.
///
/// **Not a time, and deliberately not `0`.** [`crate::Clock::now_secs`] is
/// `epoch.elapsed().as_secs()` from an instant taken when *this process*
/// started, so `0` on that clock does not mean "long ago", it means "now, at
/// boot" -- and it is a reading a torrent really can have, for the whole of
/// the first second. Giving a restored torrent any reading at all says it
/// was active a moment ago, which hands it a fresh grace period on every
/// restart -- so an app that restarts often never idle-pauses anything. That
/// was the third time this design stored a value the process invented at
/// startup and then read it back as an observation; the first two were
/// `idle_paused` and `last_accessed`.
///
/// "Nothing has used this since I started" and "the last use was at my start"
/// are not the same statement: the first is about this process's knowledge,
/// the second is a claim about the world. So the absence of a reading is
/// carried as an absence -- [`Engine::quiet_for`] answers `None` -- and the
/// idle arm reads `None` as quiet, because a torrent nobody is watching is
/// eligible to be paused whether or not we can say for how long. The grace
/// period exists to protect a stream that *just* stopped and might resume;
/// after a restart there is no such recency to protect.
///
/// A wall clock would make `0` mean 1970 and read correctly, but wall clocks
/// jump -- NTP, timezones, a television whose time is wrong until the network
/// is up -- and this is a duration measurement, which is what the monotonic
/// clock is for.
const NEVER_ACTIVE: u64 = u64::MAX;

/// [`Engine::last_transition_at`] for a torrent the reconciler has never
/// started or stopped.
///
/// A sentinel, and it cannot be `0`. `0` is a real reading here --
/// [`crate::Clock::now_secs`] is `epoch.elapsed().as_secs()`,
/// so it answers `0` for the whole first second of the process -- and the
/// two claims it would then carry disagree: "this reconciler has never
/// moved this torrent", which exempts it from the dwell, and "this
/// reconciler moved it just now", which is the strongest reason to apply
/// one. The first second is not a corner. Two things reconcile inside it
/// on an ordinary boot: `server::run` applies the persisted seeding
/// setting before the reconciler's timer starts, and that call reconciles
/// every engine there is synchronously -- on a volume under the floor it
/// stops every torrent that wants to write, at reading zero -- and a
/// client that asks for a stream as the server comes up reconciles its
/// torrent again with `PlaybackStart`, which starts it, at reading zero.
/// Either transition is one the next timer pass has to wait out.
const NEVER_MOVED: u64 = u64::MAX;

/// What `stats.error` says for a torrent the reconciler's free-space arm
/// has stopped (`Engine::is_stopped_for_space`): a fixed, path-free
/// sentence, like `librqbit::TORRENT_ERROR_MESSAGE` for the backend's own
/// error state.
pub const STOPPED_FOR_SPACE_MESSAGE: &str =
    "the torrent is stopped for want of disk space; free some space and it will resume";

pub struct Engine<H: TorrentHandle> {
    pub info_hash: String,
    pub handle: H,
    /// Epoch of `last_accessed`, shared with the owning `BackendEngineFS`.
    clock: crate::Clock,
    /// The clock reading at the last time anything **looked this engine
    /// up**, written by [`Self::touch`].
    ///
    /// It counts lookups, not playback, and it has exactly one reader left:
    /// the engine eviction in the housekeeping sweep the `BackendEngineFS`
    /// constructor spawns (`crate::BackendEngineFS::take_sweep_task`'s
    /// task), which drops an engine nothing has asked about for
    /// `INACTIVE_TORRENT_REMOVE_TIMEOUT`. That reader
    /// is the legitimate one, and it is legitimate *because* the field
    /// counts lookups: what eviction protects against is forgetting a
    /// torrent some caller still holds a name for, so "somebody asked about
    /// this" is the right question there -- and the sweep asks the five
    /// activity registers separately before it removes anything, so a
    /// playing torrent is never dropped on this field's word alone.
    ///
    /// **Nothing that decides whether a torrent is idle may read it.** Its
    /// writers are every lookup (`get_engine`/`lookup_engine`,
    /// `register_engine`) and every `Engine::get_statistics`, so a client
    /// polling `GET /{infoHash}/stats.json` -- which reaches its engine
    /// through `get_engine` and then calls `get_statistics` -- resets it
    /// every few seconds. The reconciler's idle arm used to measure its
    /// grace from here, which made "somebody is asking about this torrent"
    /// mean "somebody is watching it" and kept a torrent nobody was
    /// watching downloading all night with seeding off. The idle arm has
    /// `Self::last_active_at` instead, which moves only where activity is
    /// actually observed.
    ///
    /// It is also initialised to the clock rather than left unset, which
    /// for an engine made for a torrent the *previous* process left behind
    /// is a claim about a past this process never saw. That is harmless for
    /// eviction -- it only buys a restored engine one inactivity window
    /// before the sweep may drop it, which is the right way round -- and
    /// would not be harmless for a policy that pauses, which is the other
    /// half of why the idle arm does not read it.
    pub last_accessed: AtomicU64,
    pub active_streams: Arc<AtomicUsize>,
    pub data_cache: DataCache,
    /// Whether this process has re-applied its want-set to this torrent
    /// ([`crate::reconcile::Conditions::settled`]).
    ///
    /// `true` for every engine made here, because an engine is made by an
    /// add and an add carries the want-set with it. A *restored* engine on
    /// a backend that sets piece reclaim is marked unsettled by
    /// [`BackendEngineFS::new_with_backend_and_storage`] and settled again
    /// by [`BackendEngineFS::restore_pinned_downloads`], which is the call
    /// that puts the want-set back.
    ///
    /// [`BackendEngineFS`]: crate::BackendEngineFS
    /// [`BackendEngineFS::new_with_backend_and_storage`]: crate::BackendEngineFS::new_with_backend_and_storage
    /// [`BackendEngineFS::restore_pinned_downloads`]: crate::BackendEngineFS::restore_pinned_downloads
    settled: AtomicBool,
    /// The clock reading at the last start or stop the reconciler made on
    /// this torrent, or [`NEVER_MOVED`] for a torrent it has never moved:
    /// the dwell in [`crate::BackendEngineFS::start_if_stopped`].
    ///
    /// [`crate::BackendEngineFS::start_if_stopped`]: crate::BackendEngineFS
    last_transition_at: AtomicU64,
    /// The clock reading at the last moment something was **using** this
    /// torrent -- the instant `Conditions::playing` last read true. The
    /// idle arm of the ladder measures its grace from here
    /// ([`Self::quiet_for`]), and it is the only timestamp that policy
    /// keeps.
    ///
    /// Deliberately not [`Self::last_accessed`], which is the registry's
    /// idle-eviction clock and counts *lookups*: every
    /// `GET /{infoHash}/stats.json` reaches its engine through
    /// `BackendEngineFS::get_engine`, so a client that polls the statistics
    /// resets that one. Read by the ladder, it made "somebody is asking
    /// about this torrent" mean "somebody is watching it", and a poll every
    /// few seconds -- which is what a Stremio client does while its details
    /// page is open -- kept a torrent nobody was watching downloading for
    /// ever with seeding turned off. This one moves only when
    /// `BackendEngineFS::torrent_is_active` actually reads true, so looking
    /// is not using.
    ///
    /// **There are two seeds, because the two kinds of engine can vouch for
    /// different things**, and the whole of the defect this field replaced
    /// was one value being wrong for half its callers.
    ///
    /// * An engine this process *made* is seeded with the creation instant
    ///   (`clock.now_secs()`), and that is a real observation: nothing can
    ///   have used a torrent in an interval that did not exist. Seeding it
    ///   with an absence instead paused a freshly added torrent on its
    ///   first tick with seeding off, dropping the swarm it had just
    ///   dialled and paying a re-announce at the start of playback.
    /// * An engine built over a torrent a *previous* process left behind is
    ///   given [`NEVER_ACTIVE`] by [`Self::forget_last_active`], because
    ///   this process has no reading at all. Stamping `now` there would be
    ///   the claim this design keeps having to delete -- "used at boot" for
    ///   something nobody has touched in a week -- which hands every
    ///   restored torrent a fresh grace period on every restart, so an app
    ///   that restarts often idle-pauses nothing.
    ///
    /// So the absence is carried as an absence: [`Self::quiet_for`] answers
    /// `Option`, `None` where there is no reading, and the idle arm reads
    /// `None` as quiet -- a torrent nobody is watching is eligible to be
    /// paused whether or not we can say for how long. `0` is not the seed
    /// and is not a sentinel: it is an ordinary reading meaning something
    /// used the torrent inside this process's first second.
    ///
    /// [`Self::settled`]: Engine::settled
    last_active_at: AtomicU64,
    /// Files pinned as offline downloads (`BackendEngineFS::pin_download`).
    /// While non-empty the engine is exempt from idle removal and the
    /// seeding-disabled pause; the handle keeps its own copy for the
    /// want-set planner (`TorrentHandle::pin_file`).
    pub pinned_files: parking_lot::RwLock<BTreeSet<usize>>,
    /// The last free-space reading of every volume, shared with the
    /// `BackendEngineFS` that made this engine and written by its
    /// reconciler. Whether this torrent is stopped for want of space is
    /// recomputed from it and from the torrent's own state
    /// ([`Self::is_stopped_for_space`]); nothing here remembers that it
    /// was.
    volumes: Arc<crate::reconcile::Volumes>,
    /// Reads through this engine fail with `StorageFull` instead of parking:
    /// see [`Self::refuse_reads_for_space`].
    reads_refused: AtomicBool,
    /// The waker of every read currently parked on a missing piece, by
    /// reader ([`crate::files::FileHandle`] registers on `Pending` and
    /// forgets itself on drop). The backend wakes a parked read when its
    /// piece arrives and never otherwise, so a torrent that is stopped
    /// leaves its readers parked for good unless something here wakes them.
    read_wakers: parking_lot::Mutex<HashMap<u64, std::task::Waker>>,
    next_reader_id: AtomicU64,
    /// What the cache cleaner says the torrent-data volume may hold,
    /// shared with the `BackendEngineFS` that made this engine and written
    /// by the cleaner through it. Read here to size the retention policy;
    /// never recomputed (see [`crate::retention`]).
    budget: Arc<crate::retention::RetentionBudget>,
    /// The retention policy for the file most recently streamed on this
    /// torrent, or `None` while nothing needs bounding -- no stream yet, no
    /// budget yet, a budget that covers the file, or a pin, which is a
    /// retention property and keeps everything.
    ///
    /// While it is `None` this torrent announces everything it holds and
    /// nothing of it may be reclaimed, which is exactly what this server
    /// did before the policy was wired.
    retention: parking_lot::Mutex<Option<crate::retention::FileRetention>>,
    /// What the policy above is bounding, written beside it and read
    /// instead of it.
    ///
    /// **Kept here because a pass has the policy out of its slot.**
    /// [`Self::retain`] takes the [`crate::retention::FileRetention`] for
    /// the whole of a pass -- a directory listing and two awaited backend
    /// calls -- and that pass runs on the reconciler's tick for exactly the
    /// stream a panel is asking about. Reading the slot itself would answer
    /// "no policy is bounding this stream" every couple of seconds for a
    /// stream that is bounded, and no policy is a statement, not a delay:
    /// the two rows would blink out and back. The proxy cache's
    /// `LiveStream::bounded` and `LiveStream::windows` are the same guard
    /// for the same reason.
    ///
    /// Written under [`Self::announce`] with the slot beside it, so the
    /// pair cannot disagree except for the length of a pass, and see
    /// [`crate::retention::PolicyBounds`] for what can move in that time.
    /// `None` at process start, which is what a process with no policy
    /// installed has to say.
    bounds: parking_lot::Mutex<Option<crate::retention::PolicyBounds>>,
    /// Serialises everything that changes what this torrent announces, and
    /// the deletes taken under it.
    ///
    /// **The reason the cleaner's reclaim goes through this engine at all.**
    /// Four operations touch the same pair of facts -- what we announce and
    /// what is on the disk: installing a policy ([`Self::begin_retention`],
    /// which holds a range back), dropping one ([`Self::clear_retention_locked`],
    /// which puts a range back), a retention pass ([`Self::retain`], which
    /// commits pieces *into* what we announce and reclaims the rest), and
    /// the cache cleaner's delete ([`Self::release_reclaimable`]). A piece
    /// that becomes announced between "the policy would release this" and
    /// the unlink is a piece we have told a peer about and then thrown
    /// away, which is the one thing this whole design exists to prevent --
    /// and both of the other two do exactly that on the happy path, every
    /// two seconds and on every file change. Holding this across the whole
    /// of each is what makes "what we announce is what nothing will
    /// reclaim" an invariant rather than a likelihood.
    ///
    /// A `tokio` mutex and not a `parking_lot` one because every holder
    /// awaits the backend while it holds it.
    announce: tokio::sync::Mutex<()>,
    /// Where a reader last got to: which file, and how far into it.
    ///
    /// **An absence at process start, and it has to be**: a playhead is an
    /// observation of a player, and a torrent restored from a previous run
    /// has none this process can vouch for. Filling one in would have the
    /// policy commit and advertise a window around a position nobody has
    /// ever read from. Written by [`crate::files::FileHandle`] as reads
    /// return, so it moves only where a byte really went out.
    playhead: parking_lot::Mutex<Option<(usize, u64)>>,
}

impl<H: TorrentHandle> Engine<H> {
    pub fn new_with_handle(
        handle: H,
        info_hash: &str,
        clock: crate::Clock,
        volumes: Arc<crate::reconcile::Volumes>,
        budget: Arc<crate::retention::RetentionBudget>,
    ) -> Self {
        Self {
            info_hash: info_hash.to_string(),
            handle,
            clock,
            last_accessed: AtomicU64::new(clock.now_secs()),
            active_streams: Arc::new(AtomicUsize::new(0)),
            data_cache: moka::future::Cache::builder()
                .weigher(|_key, value: &Arc<Vec<u8>>| value.len() as u32)
                .max_capacity(64 * 1024 * 1024) // 64MB cache per engine
                .build(),
            settled: AtomicBool::new(true),
            last_transition_at: AtomicU64::new(NEVER_MOVED),
            last_active_at: AtomicU64::new(clock.now_secs()),
            pinned_files: parking_lot::RwLock::new(BTreeSet::new()),
            volumes,
            reads_refused: AtomicBool::new(false),
            read_wakers: parking_lot::Mutex::new(HashMap::new()),
            next_reader_id: AtomicU64::new(1),
            budget,
            retention: parking_lot::Mutex::new(None),
            bounds: parking_lot::Mutex::new(None),
            announce: tokio::sync::Mutex::new(()),
            playhead: parking_lot::Mutex::new(None),
        }
    }

    /// Whether this process has re-applied its want-set to this torrent
    /// (see the field).
    pub(crate) fn is_settled(&self) -> bool {
        self.settled.load(Ordering::Relaxed)
    }

    /// Forget when this torrent was last used, for an engine built over a
    /// torrent a *previous* process left behind.
    ///
    /// The constructor stamps the creation instant, and for an engine this
    /// process made that is a real observation: nothing can have used a
    /// torrent in an interval that did not exist. Stamping it for a
    /// *restored* one would be the claim this design keeps having to
    /// delete -- "used at boot" for something nobody has touched in a week,
    /// which hands it a fresh grace on every restart.
    ///
    /// The two need opposite seeds, so the difference is said here rather
    /// than folded into one value that is wrong for half its callers. It
    /// cost a real bug in the other direction first: seeding every engine
    /// with the absence paused a freshly added torrent on its first tick
    /// with seeding off, dropping the swarm it had just dialled and paying
    /// a re-announce at the start of playback.
    pub(crate) fn forget_last_active(&self) {
        self.last_active_at.store(NEVER_ACTIVE, Ordering::SeqCst);
    }

    /// Mark the want-set as not yet re-applied -- a restored torrent on a
    /// backend that sets piece reclaim, before the pins come back.
    pub(crate) fn mark_unsettled(&self) {
        self.settled.store(false, Ordering::Relaxed);
    }

    /// The want-set is back on this torrent; the reconciler may start it.
    pub(crate) fn mark_settled(&self) {
        self.settled.store(true, Ordering::Relaxed);
    }

    /// The clock reading at the reconciler's last start or stop of this
    /// torrent, or `None` if it has never moved it -- which is not the same
    /// as "moved at reading zero": see [`NEVER_MOVED`].
    pub(crate) fn last_transition_at(&self) -> Option<u64> {
        match self.last_transition_at.load(Ordering::Relaxed) {
            NEVER_MOVED => None,
            at => Some(at),
        }
    }

    /// Record that the reconciler has just started or stopped this torrent.
    pub(crate) fn record_transition(&self, now: u64) {
        self.last_transition_at.store(now, Ordering::Relaxed);
    }

    /// Something is using this torrent right now (see [`Self::last_active_at`]).
    pub(crate) fn mark_active(&self, now: u64) {
        self.last_active_at.store(now, Ordering::SeqCst);
    }

    /// How long since anything was seen using this torrent, or `None` if
    /// nothing has been -- which is not the same as "nothing has used it for
    /// zero seconds". See [`NEVER_ACTIVE`] for why the absence is carried
    /// rather than filled in, and `reconcile::desired` for the idle arm
    /// reading `None` as quiet.
    pub(crate) fn quiet_for(&self, now: u64) -> Option<Duration> {
        match self.last_active_at.load(Ordering::SeqCst) {
            NEVER_ACTIVE => None,
            at => Some(Duration::from_secs(now.saturating_sub(at))),
        }
    }

    /// Whether the reconciler is holding this torrent stopped for want of
    /// disk -- which is a wider question than [`Self::is_stopped_for_space`]
    /// and has to be, because they were being asked with two different
    /// lines and the gap between them swallowed torrents whole.
    ///
    /// The ladder measures a `Paused` torrent on a timer at
    /// `crate::reconcile::line`, which is the floor plus
    /// `FREE_SPACE_RESUME_MARGIN`; the stall clock that fails its reads is
    /// started at the same line. `is_stopped_for_space` measures at the
    /// floor alone, deliberately, because eviction and the 507 gate are
    /// about whether there is room *now*.
    ///
    /// So for a volume between the two -- which is exactly where a cleaner
    /// pass leaves it, since `CacheLimit::effective` stops the instant
    /// `available` reaches the floor -- the reconciler stopped the torrent
    /// and failed its reads while every reader that asks "is anything
    /// wrong?" was told no: `stats.json` reported buffering with no error,
    /// and `out_of_space_torrents` returned nothing, so the cleaner was
    /// never asked to free the space that would end it. A pinned download
    /// stalled at whatever percent it had reached, in silence, for good.
    ///
    /// Readers that report a condition or ask for room use this. Readers
    /// that decide whether *this request* can proceed keep the floor: a
    /// playback is measured at the floor too, so the gate and the ladder
    /// agree about it.
    pub async fn held_stopped_for_space(&self) -> bool {
        let run_state = self.handle.run_state();
        matches!(run_state, crate::backend::RunState::Paused)
            && crate::reconcile::volume_is_short(
                crate::reconcile::line(crate::reconcile::Trigger::Timer, run_state),
                self.handle.has_metadata().await,
                self.handle.is_finished().await,
                self.volumes.available(),
            )
    }

    /// Whether this torrent is stopped, and stopped because the volume it
    /// writes to has no room for it.
    ///
    /// **Recomputed, never remembered.** It is the conjunction of two
    /// things that are both readable now: the backend's state machine says
    /// the torrent is stopped ([`crate::backend::TorrentHandle::run_state`],
    /// never its persisted `paused` flag, which across an initial check is
    /// wrong in both directions), and the volume it writes to is short for
    /// it ([`crate::reconcile::volume_is_short`], the same predicate the
    /// reconciler's free-space arm applies).
    ///
    /// The volume it writes to is the piece store's root
    /// (`reconcile::Volumes::data_folder`) -- for this torrent and for every
    /// other, since that is where the session's default storage puts every
    /// byte. Not the backend's output folder, which is a name with no
    /// payload under it.
    ///
    /// **At [`crate::CACHE_FREE_SPACE_FLOOR`], not at the reconciler's
    /// hysteresis line.** The two readers of this are a client's statistics
    /// (`phase: error`, [`STOPPED_FOR_SPACE_MESSAGE`]) and the cache
    /// cleaner's eviction classes, and both are asking about the *device*,
    /// which the rest of this server judges at the floor: it is what
    /// `ensure_download_disk_ready` answers `507` under and what the
    /// cleaner's cap keeps free. Judging it at floor + resume margin
    /// instead -- the line the *ladder* holds a stopped torrent at, so that
    /// it has room to run into before it is started again -- tells a client
    /// that a volume the stream route is serving from happily is out of
    /// disk, and moves every torrent that merely happens to be paused
    /// inside the band out of `EvictionClasses::protected` and into
    /// `stopped_for_space`, whose files the cleaner will unlink piecemeal
    /// under a torrent that still holds them open.
    ///
    /// It used to be a bit set when the free-space watch stopped a torrent
    /// and cleared when something started it again, and that bit was wrong
    /// in two ways that both shipped. It said nothing about a pause that
    /// had survived a restart, because the bit had not; and because the
    /// watch skipped any torrent that claimed to be idle-paused, an
    /// idle-paused torrent on a full volume never got the bit at all --
    /// while the stream route's `507` was gated on the bit alone, so a
    /// playback starting on that torrent was let through onto the full
    /// volume. Neither is possible of a question that is asked of the
    /// present.
    ///
    /// The reading is the reconciler's last probe of the volume rather than
    /// a fresh one: this is asked on every `stats.json` poll and by every
    /// cache-cleaner pass over every engine, and it is a number that moves
    /// on the scale of seconds. `false` for a volume nothing has probed yet
    /// -- unknown is not full.
    pub async fn is_stopped_for_space(&self) -> bool {
        let run_state = self.handle.run_state();
        matches!(run_state, crate::backend::RunState::Paused)
            && crate::reconcile::volume_is_short(
                crate::CACHE_FREE_SPACE_FLOOR,
                self.handle.has_metadata().await,
                self.handle.is_finished().await,
                self.volumes.available(),
            )
    }

    /// Reads through this engine park again, as they did before
    /// [`Self::refuse_reads_for_space`] failed them. The reconciler calls it
    /// when it starts a torrent: the pieces its readers were waiting for
    /// are being fetched again, so there is once more something for a
    /// parked read to wait for.
    pub(crate) fn allow_reads(&self) {
        self.reads_refused.store(false, Ordering::SeqCst);
    }

    /// Whether reads through this engine fail rather than wait.
    pub fn reads_refused(&self) -> bool {
        self.reads_refused.load(Ordering::SeqCst)
    }

    /// Fail every read on this engine, now and until `Self::allow_reads`:
    /// the parked ones are woken to find `reads_refused` set and return
    /// `StorageFull`, and a new one returns it on its first poll.
    ///
    /// A player reading a torrent that has been stopped for space would
    /// otherwise sit on a read that nothing will ever complete -- the piece
    /// it waits for is not being downloaded -- which the player shows as an
    /// endless buffering wheel. A failed read ends the response, and the
    /// player's next request meets the `507` the stream route answers for a
    /// stopped torrent.
    pub fn refuse_reads_for_space(&self) {
        self.reads_refused.store(true, Ordering::SeqCst);
        self.wake_readers();
    }

    /// Wake every parked read so it polls again.
    pub(crate) fn wake_readers(&self) {
        let wakers: Vec<_> = self.read_wakers.lock().drain().map(|(_, w)| w).collect();
        for waker in wakers {
            waker.wake();
        }
    }

    /// A fresh id for a reader that will register wakers.
    pub(crate) fn next_reader_id(&self) -> u64 {
        self.next_reader_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Reader `id` is parked; wake it with `waker` when reads are refused.
    pub(crate) fn register_read_waker(&self, id: u64, waker: std::task::Waker) {
        self.read_wakers.lock().insert(id, waker);
    }

    /// Reader `id` is gone (or served); nothing to wake.
    pub(crate) fn forget_read_waker(&self, id: u64) {
        self.read_wakers.lock().remove(&id);
    }

    /// Whether any file of this torrent is pinned as an offline download.
    /// Where a reader of `file_idx` has got to, in bytes from the start of
    /// the file. Called as each read returns, so it is the position bytes
    /// really reached a player from.
    pub(crate) fn note_playhead(&self, file_idx: usize, offset: u64) {
        *self.playhead.lock() = Some((file_idx, offset));
    }

    /// What the retention policy says about `file_idx` right now, or `None`
    /// where there is nothing to say.
    ///
    /// The three absences are all real and none of them is a zero. **No
    /// policy** is a torrent nothing is bounding -- the budget covers the
    /// file, no budget has been published yet, or a pin keeps everything --
    /// so there is no window and no committed set to have a size. **No
    /// playhead** is a torrent no reader has been inside in this process:
    /// nothing that survives a restart says where a player had got to, and
    /// inventing one from what is on the disk would put a window round a
    /// region nobody has ever read. **A playhead in another file** is a
    /// reader that has moved on, and this file's numbers left with it.
    ///
    /// Two locks and no I/O: the reading is finished against a listing of
    /// the store, which the caller takes for itself (see
    /// [`crate::retention::PolicyReading::window`]) rather than under the
    /// policy lock.
    ///
    /// Read from [`Self::bounds`] and not from the policy slot, because a
    /// pass in flight has the policy out of that slot and this is a
    /// question a panel asks every second: see the field.
    pub(crate) fn policy_reading(
        &self,
        file_idx: usize,
    ) -> Option<crate::retention::PolicyReading> {
        let (at_file, offset) = (*self.playhead.lock())?;
        if at_file != file_idx {
            return None;
        }
        let bounds = self.bounds.lock();
        let bounds = bounds.as_ref()?;
        if bounds.file_idx != file_idx {
            return None;
        }
        Some(bounds.reading(offset))
    }

    /// Install (or keep) the retention policy for a file about to be
    /// streamed, and hold its pieces back from what we announce.
    ///
    /// **Before the reader opens**, which is the only ordering that keeps a
    /// Have from ever going out for a window piece: there is no un-Have in
    /// BitTorrent, so a piece announced once is announced to every peer
    /// that was connected. Held back before the pieces exist, which
    /// librqbit allows and which is what makes "we never advertise what we
    /// might reclaim" true rather than nearly true.
    ///
    /// A pinned torrent gets no policy: a pin is a retention property, the
    /// user asked for those bytes, and they are shared like any other bytes
    /// we are keeping.
    pub(crate) async fn begin_retention(&self, file_idx: usize) {
        let _announce = self.announce.lock().await;
        self.begin_retention_locked(file_idx).await
    }

    /// [`Self::begin_retention`] with [`Self::announce`] already held.
    async fn begin_retention_locked(&self, file_idx: usize) {
        let budget = self.budget.get();
        if self.is_pinned() {
            self.clear_retention_locked().await;
            return;
        }
        if self
            .retention
            .lock()
            .as_ref()
            .is_some_and(|held| crate::retention::still_current(held, budget, file_idx))
        {
            return;
        }
        let policy = crate::retention::policy_for(&self.handle, budget, file_idx).await;
        // Whatever was held back for the file before this one goes back
        // into what we announce first, whether or not a new policy is going
        // in. Otherwise a torrent whose reader moved to another file would
        // leave the first file's range announced to nobody for the life of
        // the engine, while the cleaner's gate -- which reads "no policy
        // covers this piece" as "we announce it" -- called those same pieces
        // protected. Held back and protected at once is the one combination
        // that is never right.
        self.clear_retention_locked().await;
        let Some(retention) = policy else {
            return;
        };
        let pieces = retention.pieces();
        if let Err(error) = self
            .handle
            .set_pieces_advertised(pieces.clone(), false)
            .await
        {
            // Without the hold-back every window piece would be announced
            // and withdrawn again seconds later, which is worse than
            // sharing nothing: the policy is not installed, so nothing is
            // reclaimed either, and the torrent behaves as it did before.
            tracing::warn!(
                info_hash = %self.info_hash,
                file_idx,
                error = %format!("{error:#}"),
                "could not hold the playback window back from what we announce; this torrent is not bounded"
            );
            return;
        }
        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx,
            first = pieces.start,
            end = pieces.end,
            shape = ?retention.shape(),
            "holding a file's pieces back and bounding it to the cache budget"
        );
        self.hold(retention);
    }

    /// Put `retention` in its slot with [`Self::bounds`] beside it. The two
    /// are written together, under [`Self::announce`], and nothing else may
    /// write either.
    fn hold(&self, retention: crate::retention::FileRetention) {
        let mut slot = self.retention.lock();
        *self.bounds.lock() = Some(retention.bounds());
        *slot = Some(retention);
    }

    /// Forget the policy and put back what it was holding back.
    ///
    /// The pieces stop being reclaimable at the same moment they start
    /// being announced again, which is the only order that keeps the rule:
    /// what we announce is what nothing will reclaim.
    async fn clear_retention_locked(&self) {
        let taken = {
            let mut slot = self.retention.lock();
            // Unconditionally, so that "nothing is bounding this stream" is
            // never left standing beside an empty slot: the bounds are the
            // answer a panel gets.
            *self.bounds.lock() = None;
            slot.take()
        };
        let Some(retention) = taken else {
            return;
        };
        if let Err(error) = self
            .handle
            .set_pieces_advertised(retention.pieces(), true)
            .await
        {
            tracing::debug!(
                info_hash = %self.info_hash,
                error = %format!("{error:#}"),
                "could not put a dropped policy's pieces back into what we announce"
            );
        }
    }

    /// One retention pass: what the policy makes of where the playhead is
    /// now, and the calls that make it so. `None` when there is nothing to
    /// do -- no policy, or no reader has been anywhere yet.
    ///
    /// Takes the policy out of its slot for the length of the pass rather
    /// than holding the lock across the backend calls, so a second pass
    /// that overlaps this one does nothing instead of queueing behind it,
    /// and a [`Self::begin_retention`] that lands meanwhile wins.
    pub(crate) async fn retain(
        &self,
        store: &crate::piece_store::StoreRoot,
    ) -> Option<crate::retention::RetentionPass> {
        let _announce = self.announce.lock().await;
        // A pin taken while this file was already playing leaves the policy
        // installed: `begin_retention` is the only other place that asks,
        // and it ran before the pin existed. Without this a pinned file is
        // reclaimed under its own reader -- measured, half a 32 MiB file
        // deleted with `is_pinned()` true throughout -- and, because the
        // policy also holds its range back, the file the user asked to keep
        // is announced to nobody while librqbit re-fetches it in a loop.
        if self.is_pinned() {
            self.clear_retention_locked().await;
            return None;
        }
        let (file_idx, offset) = (*self.playhead.lock())?;
        let mut retention = self.retention.lock().take()?;
        if retention.file_idx != file_idx {
            // The playhead names a different file: a reader that has just
            // been opened on another one and has not read yet, or the last
            // read of the file this policy replaced. Either way this pass
            // has no playhead for the policy it is holding, so it leaves it
            // exactly as it is -- `begin_retention` is what replaces a
            // policy, and it puts the old range back when it does.
            self.put_back(retention);
            return None;
        }
        let pass =
            crate::retention::advance(&self.handle, store, &self.info_hash, &mut retention, offset)
                .await;
        self.put_back(retention);
        Some(pass)
    }

    /// Put a pass's policy back where it came from, unless something has
    /// installed another one meanwhile -- in which case this one is stale
    /// and so are its bounds, and neither is written.
    fn put_back(&self, retention: crate::retention::FileRetention) {
        let mut slot = self.retention.lock();
        if slot.is_none() {
            *self.bounds.lock() = Some(retention.bounds());
            *slot = Some(retention);
        }
    }

    /// What this engine will give up, right now.
    ///
    /// Computed in one place and asked in two: the cleaner's walk collects
    /// these into a [`crate::retention::ReclaimGate`], and the delete the
    /// walk goes on to ask for re-asks it here before unlinking anything.
    /// The walk's copy is a reading taken before a blocking directory walk
    /// and every delete before this one; this asking is the one that
    /// decides.
    pub(crate) fn gate_verdict(&self) -> crate::retention::TorrentGate {
        match self.retention.lock().as_ref() {
            Some(retention) => crate::retention::TorrentGate::Policy {
                pieces: retention.pieces(),
                committed: retention.committed().clone(),
            },
            None => crate::retention::TorrentGate::Announced,
        }
    }

    /// What this engine tells the cache cleaner it may take, for
    /// [`crate::retention::ReclaimGate`].
    pub(crate) fn gate_entry(&self, gate: &mut crate::retention::ReclaimGate) {
        let info_hash = self.info_hash.to_lowercase();
        match self.gate_verdict() {
            crate::retention::TorrentGate::Policy { pieces, committed } => {
                gate.insert_policy(info_hash, pieces, committed)
            }
            _ => gate.insert_announced(info_hash),
        }
    }

    /// Delete the pieces of this torrent that are *still* reclaimable, and
    /// answer how many bytes went.
    ///
    /// **The second asking, and the reason the cleaner's delete comes
    /// through the engine at all.** The gate the cleaner carries was taken
    /// before its walk; between that reading and this unlink a piece can
    /// have become announced, and two paths do it on the happy path --
    /// [`Self::retain`] commits and advertises pieces on every pass, and
    /// [`Self::begin_retention`] puts a whole range back when the reader
    /// moves to another file. Deleting a piece we have told a peer about is
    /// the one thing this design exists to prevent, so the question is
    /// asked again here, under the lock those two also take, and the answer
    /// taken from the live policy rather than from a copy of it.
    pub(crate) async fn release_reclaimable(
        &self,
        store: &crate::piece_store::StoreRoot,
        pieces: &[u32],
    ) -> usize {
        let _announce = self.announce.lock().await;
        // Only a *policy* narrows the cleaner's request. Where there is
        // none this engine has no opinion the cleaner does not already
        // have: it decided by its own rule -- a dead torrent's bytes go
        // first, a live one's are protected -- and second-guessing that
        // here would quietly make a live torrent with no reader
        // unevictable, which is a policy change and not this fix.
        //
        // The case this exists for still lands inside that: the reader
        // moving to another file installs a policy for *that* file, and
        // piece 0 of the file it left is outside the new range, so the
        // verdict refuses it. So does a piece the pass has committed since.
        let still: Vec<u32> = match self.gate_verdict() {
            verdict @ crate::retention::TorrentGate::Policy { .. } => pieces
                .iter()
                .copied()
                .filter(|piece| verdict.releases(*piece))
                .collect(),
            _ => pieces.to_vec(),
        };
        if still.len() != pieces.len() {
            tracing::debug!(
                info_hash = %self.info_hash,
                asked = pieces.len(),
                taking = still.len(),
                "pieces became announced between the cleaner's reading and its delete"
            );
        }
        let mut freed = 0;
        for run in crate::retention::runs(&still) {
            freed += crate::retention::release(&self.handle, store, &self.info_hash, run).await;
        }
        freed
    }

    pub fn is_pinned(&self) -> bool {
        !self.pinned_files.read().is_empty()
    }

    /// The pinned file indices, ascending.
    pub fn pinned_file_indices(&self) -> Vec<usize> {
        self.pinned_files.read().iter().copied().collect()
    }

    pub fn touch(&self) {
        self.last_accessed
            .store(self.clock.now_secs(), Ordering::SeqCst);
    }

    pub fn find_file_by_regex(&self, regex_str: &str) -> Option<usize> {
        let _re = Regex::new(regex_str).ok()?;
        // This is tricky now as find_file_by_regex was librqbit specific.
        // For now, we'll assume we can list files.
        None
    }

    pub async fn guess_file_index(
        &self,
        series_info: Option<&crate::engine::SeriesInfo>,
    ) -> Option<usize> {
        let files = self.handle.get_files().await;
        guess_file_index_in(&files, series_info)
    }

    pub async fn get_statistics(&self) -> EngineStats {
        self.touch();
        let mut stats = self.handle.stats().await;

        let guessed_file_idx = self.guess_file_index(None).await.unwrap_or(0);

        // Get stream info for the guessed file
        if guessed_file_idx < stats.files.len() {
            let file = &stats.files[guessed_file_idx];
            stats.stream_name = file.name.clone();
            stats.stream_len = file.length;

            // Derive stream progress from the streaming file's own
            // downloaded/length, NOT the torrent's total_wanted set. During cold
            // start the file baseline is dropped to 0 so only a few metadata
            // pieces are "wanted"; total_wanted_done/total_wanted then spikes
            // toward 100% just before playback until the head priorities widen
            // the wanted set again. The per-file fraction is stable across those
            // priority changes (downloaded counts verified pieces in range).
            if file.length > 0 {
                stats.stream_progress = (file.downloaded as f64 / file.length as f64).min(1.0);
            }

            tracing::debug!(
                "get_statistics: file_idx={} file.progress={:.2}% total_done={} stream_progress={:.2}%",
                guessed_file_idx,
                file.progress * 100.0,
                stats.downloaded,
                stats.stream_progress * 100.0
            );
            // Startup phase / initial-window readiness for the stream file.
            stats.focus_stream_file(guessed_file_idx);
        } else {
            tracing::debug!(
                "get_statistics: guessed_file_idx {} >= stats.files.len() {}",
                guessed_file_idx,
                stats.files.len()
            );
        }

        // A torrent the reconciler stopped for space is `Paused` to the
        // backend, which is `buffering` to a client -- a wheel that never
        // ends. It is an error in the sense the `error` field has always
        // had: a full disk, and the client can act on it.
        if self.held_stopped_for_space().await {
            stats.phase = crate::backend::StartupPhase::Error;
            stats
                .error
                .get_or_insert_with(|| STOPPED_FOR_SPACE_MESSAGE.to_string());
        }

        stats
    }

    /// Open a reader for `file_idx` at `start_offset`. Blocks while the
    /// backend waits for the torrent to become streamable (bounded by the
    /// backend's initialization timeout) rather than failing a first play; a
    /// torrent that never becomes ready yields `GetFileError::Backend` carrying
    /// a `TorrentInitError`.
    ///
    /// `buffer` is the viewer's read-ahead choice, which sizes the reader's
    /// playback window (see `priorities::BufferProfile`).
    pub async fn try_get_file_with_intent(
        self: &Arc<Self>,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
        intent: PlaybackIntent,
        buffer: BufferProfile,
    ) -> Result<FileHandle<H>, GetFileError> {
        let startup = Instant::now();
        tracing::debug!(
            "[STREAMING] Preparing file {} for playback (offset={}, intent={:?})",
            file_idx,
            start_offset,
            intent
        );

        self.touch();

        let files = self.handle.get_files().await;
        if file_idx >= files.len() {
            return Err(GetFileError::FileNotFound {
                file_idx,
                file_count: files.len(),
            });
        }

        let length = files[file_idx].length;
        let name = files[file_idx].name.clone();

        if !self.handle.manages_playback_lifecycle()
            && priority != 255
            && start_offset == 0
            && matches!(intent, PlaybackIntent::DirectInitial)
        {
            let prepare_start = Instant::now();
            match self.handle.prepare_file_for_streaming(file_idx).await {
                Ok(()) => tracing::info!(
                    "startup: direct prepare_file_for_streaming completed in {:?} for file {}",
                    prepare_start.elapsed(),
                    file_idx
                ),
                Err(e)
                    if e.downcast_ref::<crate::backend::librqbit::TorrentInitError>()
                        .is_some() =>
                {
                    // The torrent never became ready; opening the reader would
                    // only wait out the same timeout again.
                    return Err(GetFileError::Backend(
                        e.context("prepare_file_for_streaming"),
                    ));
                }
                Err(e) => {
                    tracing::warn!("get_file: prepare_file_for_streaming failed: {}", e);
                    // Continue anyway - the file reader will block on pieces as needed
                }
            }
        } else {
            tracing::debug!(
                "get_file: skipping prepare_file_for_streaming for file {} offset {} priority {}",
                file_idx,
                start_offset,
                priority
            );
        }

        // Before the reader, and it has to be before: the policy's window
        // pieces are held back from what we announce, and a piece held back
        // only after it completes has already had its Have go out. See
        // [`Self::begin_retention`].
        self.begin_retention(file_idx).await;

        let reader_start = Instant::now();
        let reader = self
            .handle
            .get_file_reader(file_idx, start_offset, priority, None, intent, buffer)
            .await
            .context("get_file_reader")?;
        tracing::debug!(
            "startup: get_file_reader returned in {:?} for file {} offset {} (total={:?})",
            reader_start.elapsed(),
            file_idx,
            start_offset,
            startup.elapsed()
        );

        self.active_streams.fetch_add(1, Ordering::SeqCst);

        // Use raw reader directly for better performance
        // The torrent backend reads from local files, caching adds overhead
        Ok(FileHandle::new(
            length,
            name,
            reader,
            self.clone(),
            file_idx,
            start_offset,
        ))
    }
}
