use crate::backend::{
    EngineStats, FilePieceSpan, TorrentHandle,
    priorities::{self, BufferProfile, PlaybackIntent},
};
use crate::cache::DataCache;
use crate::piece_store::{HeldSnapshot, RetentionPolicy, Share, StoreRegistry};
use crate::retention::live::{Live, Reading};
use crate::retention::owner::{Backing, Door, Install, Mode, Retention, Trigger};
use anyhow::Context;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

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
/// on an ordinary boot: the reconciler's first pass, which runs as soon as
/// it is started (`BackendEngineFS::start_reconciler`'s interval completes
/// its first tick at once) and on a volume under the floor stops every
/// torrent that wants to write, at reading zero; and a client that asks
/// for a stream as the server comes up, which reconciles its torrent with
/// `PlaybackStart` and starts it, at reading zero. Either transition is one
/// the next timer pass has to wait out.
const NEVER_MOVED: u64 = u64::MAX;

/// What `stats.error` says for a torrent the reconciler's free-space arm
/// has stopped (`Engine::is_stopped_for_space`): a fixed, path-free
/// sentence, like `librqbit::TORRENT_ERROR_MESSAGE` for the backend's own
/// error state.
pub const STOPPED_FOR_SPACE_MESSAGE: &str =
    "the torrent is stopped for want of disk space; free some space and it will resume";

/// What one torrent's policies say, as a value: the reading a test takes
/// of cells the owner keeps under its own locks.
///
/// **Test-only, and that is what is left of it.** It used to be the cache
/// cleaner's gate -- what this torrent would let a walk of the disk take --
/// asked once before the walk and again at each delete. Nothing walks the
/// disk any more and nothing outside this crate deletes a piece, so what
/// remains is the copy-out itself, which is how a test reads a policy
/// without reaching into the owner's locks. See [`Engine::standing`].
#[cfg(test)]
pub(crate) struct Standing {
    /// Every policy standing on this torrent, in file order, from one
    /// copy-out. Empty when none stands.
    pub policies: Vec<FileStanding>,
}

/// One file's standing policy, as a value.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStanding {
    /// The file the policy is over -- the key its turn is taken under.
    pub file_idx: usize,
    /// The policy itself.
    pub view: crate::retention::owner::InstalledView,
    /// What the next pass over this file will be, from the reading taken
    /// when this standing was built.
    pub mode: Mode,
}

/// What a pin or a live window keeps of one torrent ([`Engine::protects`]).
///
/// Pieces rather than bytes, and a set of file indices rather than a count,
/// because two claims can name one piece -- a pinned file and the live
/// file share their boundary piece, and two windows in one file overlap --
/// and a figure that added them up would report more protection than there
/// are bytes on the disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Protected {
    /// The held pieces nothing may take.
    pub pieces: BTreeSet<u32>,
    /// The files they are the pieces of.
    pub files: BTreeSet<usize>,
}

/// How far ahead of its opening offset a reader may fetch, as
/// [`Engine::fetch_bound`] answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetentionBound {
    /// Bytes from the opening offset to the start of the last piece the
    /// window reaches ahead of it; `None` when nothing bounds the file --
    /// no budget, a budget that covers it, a pin -- and the intent's cap is
    /// the whole of the lookahead.
    pub forward_bytes: Option<u64>,
}

/// One file of a torrent, as the thing a retention policy is over: which
/// file, where it lies in the torrent's pieces, and how long a piece is.
/// Fixed for the entity's life -- the owner compares it at a pass's re-read,
/// and only [`Retention::install`] writes it, under the turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDomain {
    file_idx: usize,
    span: FilePieceSpan,
    piece_length: u64,
}

/// The torrent side of the retention owner: what one torrent's handle, store
/// and pin set are to a [`Retention`]. One per engine, keyed by file index.
///
/// The owner does the arithmetic, the re-readings and the door; this is the
/// six calls onto the backend and the store that make its answers true, and
/// the two pure readings -- piece index from a file offset, policy from a
/// budget -- the owner makes under its state lock. Nothing here is a lock of
/// its own: `pinned` is the engine's pin set, shared, and read as a copy-out.
pub(crate) struct TorrentBacking<H: TorrentHandle> {
    handle: H,
    info_hash: String,
    /// Which entity the server is playing, shared with the whole process
    /// ([`crate::retention::live`]). Read at a slack reclaim's door, so a
    /// file the user opened again mid-delete keeps what is left of it.
    live: Arc<Live>,
    /// [`Engine::pinned_files`], the same `Arc`: a pin is a retention
    /// property, and the owner asks about it before every pass and at every
    /// door.
    pinned: Arc<parking_lot::RwLock<BTreeSet<usize>>>,
    /// Whether the embedder named a pin set at boot, shared with the whole
    /// process ([`crate::piece_store::PinsUnknown`]). While it holds, the
    /// pin set is not "empty", it is *unknown*, and an owner that reclaimed
    /// on that reading would delete the offline downloads nobody described.
    pins_unknown: Arc<crate::piece_store::PinsUnknown>,
    /// Pieces a reclaim asked the backend to forget and did not get back --
    /// a peer mid-flight on them, or a stream's lookahead over them. Shared
    /// with [`Engine::refused_reclaims`], which reports it.
    refused: Arc<AtomicUsize>,
}

impl<H: TorrentHandle> TorrentBacking<H> {
    /// Want every piece of `ranges` again, one backend call per range; a
    /// refusal is logged and the stream's own lookahead still pulls what it
    /// is about to read.
    async fn reselect(&self, ranges: &[Range<u32>]) {
        for range in ranges {
            if let Err(error) = self.handle.reselect_pieces(range.clone()).await {
                tracing::warn!(
                    info_hash = %self.info_hash,
                    first = range.start,
                    end = range.end,
                    error = %format!("{error:#}"),
                    "could not want the pieces again; the stream's lookahead still pulls what it reads"
                );
            }
        }
    }
}

impl<H: TorrentHandle> Backing for TorrentBacking<H> {
    type Key = usize;
    /// Which file, and how far into it -- the reader's own coordinates. The
    /// file is always the entity's own ([`Self::index_of`]).
    type Position = (usize, u64);
    type Domain = FileDomain;
    type Want = usize;
    /// The registry the session's stores report to: where the held set is
    /// read and where every unlink of a registered torrent's piece goes.
    type Store = Arc<StoreRegistry>;
    /// A torrent's half of the budget is committed for sharing: its pieces
    /// are what a peer asks us for. That is the whole of what differs from
    /// the proxy cache's policy, and it is this constant rather than a
    /// second policy.
    const SHARE: Share = Share::Half;
    /// The swarm fills the cache whether or not anyone reads, so the pass
    /// runs on the reconciler's tick and not on a delivered byte.
    const TRIGGER: Trigger = Trigger::External;
    /// Before the reader opens, so the hold-back precedes the pieces: there
    /// is no un-Have in BitTorrent.
    const INSTALL: Install = Install::OnOpen;

    /// What the backend says the file is, now. `None` is a torrent with no
    /// metadata to name its pieces by, or a piece length of nothing, and
    /// neither can be bounded.
    async fn resolve(&self, file_idx: usize) -> Option<FileDomain> {
        let span = self.handle.file_pieces(file_idx).await?;
        let piece_length = self.handle.piece_length().filter(|length| *length > 0)?;
        Some(FileDomain {
            file_idx,
            span,
            piece_length,
        })
    }

    fn governs(domain: &FileDomain, file_idx: usize) -> bool {
        domain.file_idx == file_idx
    }

    fn extent(domain: &FileDomain) -> Range<u32> {
        domain.span.pieces.clone()
    }

    fn policy(
        domain: &FileDomain,
        budget: u64,
        buffering: crate::piece_store::Buffering,
    ) -> anyhow::Result<RetentionPolicy> {
        RetentionPolicy::new(
            budget,
            domain.piece_length,
            domain.span.pieces.clone(),
            domain.span.bytes,
            Self::SHARE,
            buffering,
        )
    }

    /// What the file's pieces hold, which is the file to within the part of
    /// its first and last pieces a neighbour owns -- eight mebibytes of a
    /// twenty-three gigabyte film, and the bitrate this divides into is not
    /// asked to three figures.
    /// A byte offset into the file is a reader's position in it.
    fn position_at(domain: &FileDomain, offset: u64) -> Option<(usize, u64)> {
        Some((domain.file_idx, offset))
    }

    fn bytes(domain: &FileDomain) -> Option<u64> {
        Some(domain.span.bytes).filter(|bytes| *bytes > 0)
    }

    /// The torrent piece a reader at `offset` of `file` is sitting on.
    /// Always `Some`: a file's entity hears only its own bytes -- the
    /// [`FileHandle`]'s reader is on the file it reads, and the tests'
    /// `Engine::note_playhead` names the file the byte was in -- so `file`
    /// is this domain's, and a position that is not would be a reader
    /// mis-keyed, not a head that left.
    ///
    /// [`FileHandle`]: crate::files::FileHandle
    fn index_of(domain: &FileDomain, (file, offset): (usize, u64)) -> Option<u32> {
        debug_assert_eq!(
            file, domain.file_idx,
            "a file's entity was told a byte of another file"
        );
        Some(crate::retention::playhead_piece(
            &domain.span,
            domain.piece_length,
            offset,
        ))
    }

    /// **TEMPORARY**, with [`crate::retention::trace`]: the counters the
    /// owner cannot read, and the piece length a lookahead is measured in.
    fn trace(&self, domain: &FileDomain) -> Option<crate::retention::trace::Backing> {
        let transfer = self.handle.transfer_totals()?;
        Some(crate::retention::trace::Backing {
            fetched: transfer.fetched,
            verified: transfer.verified,
            refused: self.refused.load(Ordering::Relaxed),
            piece_length: domain.piece_length,
        })
    }

    /// A pin on this file: a pin is a retention property, the user asked
    /// for those bytes, and they are shared like any other bytes we keep.
    ///
    /// This file's pin and not the torrent's, because the pin set is per
    /// file. Asked of the torrent, one pinned episode of a season kept every
    /// other episode anyone streamed whole: fetched whole at the open, never
    /// windowed, never taken when the viewer moved on, and reported as slack
    /// that nothing could take. The files beside a pinned one are retained
    /// as any unpinned file is, and the pieces they share with it are kept
    /// by [`Self::alone`] and at the reclaim's door.
    ///
    /// True for everything while the pin set is unknown. A boot the embedder
    /// named no set to knows only that some of this may be pinned, and the
    /// safe reading of "some" is "all": the bytes are still there to unpin,
    /// where a pass that had taken them would have destroyed a download only
    /// the embedder could have named.
    fn keeps_everything(&self, file_idx: &usize) -> bool {
        self.pins_unknown.is_set() || self.pinned.read().contains(file_idx)
    }

    /// Whether this file is the one being played, at this instant. One
    /// borrow of the liveness cell, nothing cloned: it is asked once per
    /// run of a slack reclaim, from a blocking thread.
    fn is_live(&self, file_idx: &usize) -> bool {
        self.live.is_torrent_file(&self.info_hash, *file_idx)
    }

    /// The registered store's held set inside this file's extent -- a copy
    /// of the store's atomic words, no listing and no suspension. `None`
    /// is a torrent with no registered store: one the session holds in
    /// Error (librqbit drops its storage), or one whose `init` has not run
    /// -- unknown, which the pass concludes nothing over, and never empty.
    async fn held(&self, store: &Arc<StoreRegistry>, domain: &FileDomain) -> Option<BTreeSet<u32>> {
        store
            .held(&self.info_hash)
            .map(|held| held.in_range(Self::extent(domain)))
    }

    /// A hold-back leaves a pinned file's pieces announced. The piece an
    /// unpinned file shares with a pinned neighbour is in its extent, which
    /// its policy holds back when it is installed and its slack pass holds
    /// back again on every tick; nothing takes that piece, since it is the
    /// pinned file's ([`Self::alone`]), so the entity never empties and
    /// nothing ever lifted the hold-back. The pinned file was kept whole and
    /// one piece of it was announced to nobody for as long as the pin stood.
    ///
    /// Given back after the hold-back, from a reading of the pin set taken
    /// after it: a pin that landed while the hold-back was being made is
    /// seen, and one that lands later is seen by the next hold-back -- at
    /// the latest the slack pass the file gets once nobody plays it, which
    /// holds back again on every tick. A hold-back of a pinned span has no
    /// reclaim behind it to protect, because every reclaim reads the pin
    /// set again after this and leaves the span alone. Refused, the give
    /// back is logged and the hold-back still stands as asked: the caller's
    /// deletions depend on that, not on this.
    async fn advertise(&self, pieces: Range<u32>, on: bool) -> anyhow::Result<()> {
        self.handle
            .set_pieces_advertised(pieces.clone(), on)
            .await?;
        if on {
            return Ok(());
        }
        for span in pinned_spans(&self.handle, &self.pinned, None).await {
            let shared = pieces.start.max(span.start)..pieces.end.min(span.end);
            if shared.is_empty() {
                continue;
            }
            if let Err(error) = self
                .handle
                .set_pieces_advertised(shared.clone(), true)
                .await
            {
                tracing::warn!(
                    info_hash = %self.info_hash,
                    first = shared.start,
                    end = shared.end,
                    error = %format!("{error:#}"),
                    "could not announce a pinned file's pieces a neighbour held back; the next hold-back tries again"
                );
            }
        }
        Ok(())
    }

    /// Which seed of this torrent's piece store is in force. It moves when
    /// a restart out of an error builds a fresh store, which is the same
    /// act that builds librqbit a fresh chunk tracker and loses every
    /// hold-back the tracker carried.
    ///
    /// 0 for a torrent with no registered store: one held in Error, whose
    /// pass concluded nothing over an unknown held set a step earlier, and
    /// which announces nothing either way.
    fn epoch(&self, store: &Arc<StoreRegistry>) -> u64 {
        store.epoch(&self.info_hash).unwrap_or(0)
    }

    /// The boundary rule, and a pinned neighbour's pieces on top of it.
    /// The rule asks the backend's want-set, and the engine records a pin
    /// before it asks the backend to select the file, so for the length of
    /// that call a pinned neighbour reads as unwanted and the piece this
    /// file shares with it as this file's alone.
    async fn alone(&self, domain: &FileDomain, pieces: &[u32]) -> Vec<u32> {
        let alone = crate::retention::this_files_alone(&self.handle, domain.file_idx, pieces).await;
        let pinned = pinned_spans(&self.handle, &self.pinned, Some(domain.file_idx)).await;
        alone
            .into_iter()
            .filter(|piece| !pinned.iter().any(|span| span.contains(piece)))
            .collect()
    }

    /// The want-set, trimmed to the window: librqbit's picker fetches every
    /// selected piece of the file it is not told otherwise about, and told
    /// nothing it fills the disk past the window at the swarm's pace, for
    /// every pass to reclaim what the last tick fetched.
    ///
    /// The windows are re-selected first: a piece the window has moved onto
    /// may be one an earlier pass dropped, and a dropped piece is neither
    /// had nor wanted until something wants it again. Then every piece of
    /// the file in no window and not on the disk is dropped and left dropped
    /// ([`crate::backend::AfterRelease::LeaveDropped`]): not had, and now not
    /// wanted. What is on the disk and outside the windows is the reclaim's
    /// ([`Self::reclaim`]), which drops it the same way before it unlinks. A
    /// boundary piece a still-wanted neighbour owns bytes in is left wanted,
    /// as the reclaim leaves it held: dropping it would leave the neighbour a
    /// piece short.
    ///
    /// The stream's own lookahead is bounded to the same window at open
    /// ([`Engine::fetch_bound`]), so nothing dropped here is a piece a live
    /// stream is about to read; librqbit refuses those and would only have
    /// them re-wanted. Not asked of a torrent that is not settled: the
    /// backend bails for one under its check or in error, and the reading
    /// belongs here beside the reclaim's.
    ///
    /// `held` is the pass's reading, taken two awaited backend calls ago,
    /// and a piece outside the window can complete in that gap -- the store
    /// sets its bit, then librqbit its have-bit. Asked to drop it, librqbit
    /// does: it is a have piece, and dropping it is what the reclaim would
    /// have done a pass later. But a have piece dropped and left on the disk
    /// is a file the store counts and the backend has forgotten, sitting
    /// there until the next pass offers it again and librqbit hands the
    /// dropped piece out a second time. So what librqbit reports dropped is
    /// read against the store's set *now*, and whatever is on the disk goes
    /// under the claim, as a reclaim's pieces do. The set is read after the
    /// drop: a piece the store has is one librqbit had, never the reverse.
    ///
    /// **And that unlink asks the door first**, as the reclaim asks it
    /// before every part of every run: the pass read "not pinned" at its
    /// first step, and a pin lands in the gap as easily as a piece does. A
    /// door that answers "take nothing" -- pinned now -- releases the claim
    /// with the piece on the disk; librqbit has forgotten the piece, and the
    /// pin's next pass wants the whole file again and downloads it back over
    /// the same bytes. One piece fetched twice, against a piece of a pinned
    /// file deleted. And a piece inside any open reader's window now
    /// ([`Door::windows_now`]) stays, as the reclaim leaves it.
    async fn want(
        &self,
        store: &Arc<StoreRegistry>,
        domain: &FileDomain,
        windows: &[Range<u32>],
        held: &BTreeSet<u32>,
        door: &Door<Self>,
    ) {
        let run_state = self.handle.run_state();
        if !matches!(
            run_state,
            crate::backend::RunState::Live | crate::backend::RunState::Paused
        ) {
            tracing::debug!(
                info_hash = %self.info_hash,
                ?run_state,
                "the torrent is not settled, so its want-set is left as it is this pass"
            );
            return;
        }
        self.reselect(windows).await;
        let unwanted: Vec<u32> = Self::extent(domain)
            .filter(|piece| {
                !windows.iter().any(|window| window.contains(piece)) && !held.contains(piece)
            })
            .collect();
        let alone = self.alone(domain, &unwanted).await;
        for run in crate::retention::runs(&alone) {
            match self
                .handle
                .drop_pieces(run.clone(), crate::backend::AfterRelease::LeaveDropped)
                .await
            {
                Ok(Some(claim)) => {
                    // A neighbour pinned under the drop keeps what it shares
                    // with this file, as it does at the reclaim's door.
                    let pinned =
                        pinned_spans(&self.handle, &self.pinned, Some(domain.file_idx)).await;
                    let arrived: Vec<u32> = match (store.held(&self.info_hash), door.windows_now())
                    {
                        (Some(now), Some(windows)) => {
                            let now = now.in_range(run.clone());
                            claim
                                .pieces()
                                .iter()
                                .copied()
                                .filter(|piece| {
                                    now.contains(piece)
                                        && !windows
                                            .iter()
                                            .chain(&pinned)
                                            .any(|window| window.contains(piece))
                                })
                                .collect()
                        }
                        // No store to read, or a door that says take
                        // nothing: whatever arrived stays.
                        _ => Vec::new(),
                    };
                    if arrived.is_empty() {
                        // Nothing of these is ours to take off the disk:
                        // released at once.
                        drop(claim);
                    } else {
                        tracing::debug!(
                            info_hash = %self.info_hash,
                            pieces = arrived.len(),
                            "pieces outside the window arrived under the pass; unlinking what the backend forgot"
                        );
                        crate::retention::unlink(store, &self.info_hash, arrived, Some(claim))
                            .await;
                    }
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    info_hash = %self.info_hash,
                    first = run.start,
                    end = run.end,
                    error = %format!("{error:#}"),
                    "could not stop wanting the pieces outside the window; the swarm will fill them"
                ),
            }
        }
    }

    /// Nothing bounds the file any more: every piece of it wanted again,
    /// on a settled torrent. See [`Self::want`] for the drop this undoes.
    async fn want_all(&self, domain: &FileDomain) {
        if !matches!(
            self.handle.run_state(),
            crate::backend::RunState::Live | crate::backend::RunState::Paused
        ) {
            return;
        }
        self.reselect(&[Self::extent(domain)]).await;
    }

    /// The reclaim, asking the door before every part of every run.
    ///
    /// **The door, asked at the instant of each unlink and not a moment
    /// before it.** The decision was measured before two awaited backend
    /// calls per committed and withdrawn run and a `file_wants`, and
    /// neither of the two things it measured against is behind a lock the
    /// pass holds: the reader's note writes the playhead on every delivered
    /// byte and `pin_download` writes the pin set, and the turn stops
    /// neither. A piece becoming announced under the pass is not the door's
    /// to catch: every advertise is made under this file's turn, which the
    /// pass holds throughout.
    ///
    /// [`Door::windows_now`] answers `None` for a pin taken since the pass
    /// began, and it stops the reclaim rather than skipping a run: a pin
    /// does not un-pin mid-loop. A pinned file's pieces are the expensive
    /// ones to get wrong -- `AfterRelease::LeaveDropped` leaves them neither
    /// held nor wanted, and the pin's own reconcile short-circuits an
    /// unchanged selection, so nothing re-queues them and the download the
    /// user asked for stays short of them until a restart hash-checks the
    /// file off the disk.
    ///
    /// `Some` narrows the run by the window at the file's head and by the
    /// window round every open reader's *current* position -- a seek is a
    /// second reader on the file still playing, and the piece under it is
    /// not the pass's to take because the other reader delivered the last
    /// byte. librqbit refuses a piece its own live stream is about to read,
    /// but that is `queue_range`, the forward lookahead alone, so it cannot
    /// see the tenth of the window that sits behind the playhead for a scan
    /// back, it is empty whenever no stream is open, and
    /// `TorrentHandle::drop_pieces` promises it of no backend.
    ///
    /// Per run and not per piece: `release` is the unit that holds the
    /// claim across the unlink, and `runs` exists so that two hundred
    /// consecutive pieces are one call and not two hundred locks on the
    /// torrent. And asked again before every *part* of a run: a window at
    /// the door can fall inside a run and cut it in two, and the second
    /// part is then given back only after the first has been released -- a
    /// `drop_pieces` and an unlink batch later. Handing the second part to
    /// `release` on the same answer is the reading the door exists to
    /// refuse, one level down: a pin taken during the first part's unlink
    /// would have the second part's pieces dropped out of a download the
    /// user has just asked to keep. So a run the windows have narrowed or
    /// split goes back on the list in its parts, and each part is asked
    /// about in its own turn; only a run the door lets through whole is
    /// released.
    ///
    /// And before every one of those, the torrent's run state, read at the
    /// instant and not carried in from the decision: a torrent under its
    /// initial hash check (`Initializing`) is reading every piece it means
    /// to claim, and a piece dropped from under it is a have-bit over
    /// nothing; one in `Error` or `Gone` holds no storage for a drop to
    /// edit. librqbit's own `drop_pieces` bails in all three, so this
    /// changes what is asked rather than what happens -- but a bail per run
    /// is a warning per run per tick, and the reading belongs here, where
    /// the unlink is decided.
    async fn reclaim(
        &self,
        store: &Arc<StoreRegistry>,
        domain: &FileDomain,
        runs: Vec<Range<u32>>,
        door: Door<Self>,
    ) -> usize {
        let mut reclaimed = 0;
        let mut pending: VecDeque<Range<u32>> = runs.into();
        while let Some(run) = pending.pop_front() {
            // A neighbour pinned since the runs were planned keeps the piece
            // it shares with this file, asked here with the door and not
            // carried in from the plan: the door answers for this file's own
            // pin only, and the pinned file wants its boundary piece whole.
            let pinned = pinned_spans(&self.handle, &self.pinned, Some(domain.file_idx)).await;
            let Some(windows) = door.windows_now() else {
                break;
            };
            let run_state = self.handle.run_state();
            if !matches!(
                run_state,
                crate::backend::RunState::Live | crate::backend::RunState::Paused
            ) {
                tracing::debug!(
                    info_hash = %self.info_hash,
                    ?run_state,
                    "the torrent is not settled, so nothing of it is reclaimed this pass"
                );
                break;
            }
            let mut parts = vec![run.clone()];
            for window in windows.iter().chain(&pinned) {
                parts = parts
                    .into_iter()
                    .flat_map(|part| crate::retention::outside(part, window))
                    .collect();
            }
            if parts.len() == 1 && parts[0] == run {
                let asked = (run.end - run.start) as usize;
                let freed =
                    crate::retention::release(&self.handle, store, &self.info_hash, run).await;
                self.refused
                    .fetch_add(asked.saturating_sub(freed), Ordering::Relaxed);
                reclaimed += freed;
            } else {
                // Strictly fewer pieces than `run`, so this converges: a
                // part is released or shrinks again on every turn through
                // the loop.
                for part in parts.into_iter().rev() {
                    pending.push_front(part);
                }
            }
        }
        reclaimed
    }
}

/// The piece range of every file in `pinned` but `except`, read at this
/// instant: what a reclaim of anything else must leave where it is. A
/// file whose pieces the backend cannot name has none on the disk to keep.
async fn pinned_spans<H: TorrentHandle>(
    handle: &H,
    pinned: &parking_lot::RwLock<BTreeSet<usize>>,
    except: Option<usize>,
) -> Vec<Range<u32>> {
    let files: Vec<usize> = pinned
        .read()
        .iter()
        .copied()
        .filter(|file_idx| Some(*file_idx) != except)
        .collect();
    let mut spans = Vec::with_capacity(files.len());
    for file_idx in files {
        if let Some(span) = handle.file_pieces(file_idx).await {
            spans.push(span.pieces);
        }
    }
    spans
}

/// Give every pinned file of `pinned` that has no entity one, by installing
/// on it under the pin.
///
/// A pin is a write to the engine's pin set and a `pin_file` on the
/// backend, and neither makes an entity, so a pinned file nothing has
/// opened in this process is in no pass's list: not the passes, which walk
/// entities, and not `reclaim_rest`, which leaves a pinned file's pieces
/// where they are.
/// That is the right shape for a file that was never touched -- there is
/// nothing to undo -- and the wrong one for a file a slack pass had dropped
/// the pieces of and then forgotten, or that `reclaim_rest` had dropped as
/// outside every entity. The fork's `pin_file` re-queues only pieces of a
/// file that was not selected before, and a file whose pieces a reclaim
/// dropped is still selected, so the pin queues nothing; every piece the
/// reclaim took stays neither had nor wanted, and the download the user
/// asked for stands still until something wants the file again. The only
/// thing that does is [`Backing::want_all`], reached through
/// [`Retention::install`] -- which is what an open would do, so this does
/// what an open would: install, once, and let the pin exit of the passes
/// do the rest. Under a pin the install holds nothing back, gives back
/// whatever the forgotten entity or `reclaim_rest` was holding back, and
/// wants the whole file. Once, because the entity it makes stays -- a
/// pinned entity's passes take the pin exit and never forget it -- and a
/// file it makes no entity for (no metadata yet) is asked again next tick.
async fn adopt_pins<H: TorrentHandle>(
    retention: &Retention<TorrentBacking<H>>,
    pinned: &[usize],
    info_hash: &str,
) {
    let known: BTreeSet<usize> = retention.keys().into_iter().collect();
    for &file_idx in pinned.iter().filter(|idx| !known.contains(idx)) {
        let outcome = retention.install(file_idx, file_idx).await;
        tracing::debug!(
            info_hash,
            file_idx,
            ?outcome,
            "a pinned file nothing has opened gets an entity, and every piece of it wanted again"
        );
    }
}

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
    /// **Nothing that decides whether a torrent runs may read it.** Its
    /// writers are every lookup (`get_engine`/`lookup_engine`,
    /// `register_engine`) and every `Engine::get_statistics`, so a client
    /// polling `GET /{infoHash}/stats.json` -- which reaches its engine
    /// through `get_engine` and then calls `get_statistics` -- resets it
    /// every few seconds. The reconciler's idle arm used to measure its
    /// grace from here, which made "somebody is asking about this torrent"
    /// mean "somebody is watching it" and kept a torrent nobody was
    /// watching downloading all night with seeding off. What decides that
    /// now is the liveness cell ([`crate::retention::live`]), which is
    /// written where a stream really opens and nowhere else.
    ///
    /// It is also initialised to the clock rather than left unset, which
    /// for an engine made for a torrent the *previous* process left behind
    /// is a claim about a past this process never saw. That is harmless for
    /// eviction -- it only buys a restored engine one inactivity window
    /// before the sweep may drop it, which is the right way round -- and
    /// would not be harmless for anything that pauses, which is the other
    /// half of why nothing else reads it.
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
    /// by [`BackendEngineFS::apply_pins`], which is the call that puts the
    /// want-set back.
    ///
    /// [`BackendEngineFS`]: crate::BackendEngineFS
    /// [`BackendEngineFS::new_with_backend_and_storage`]: crate::BackendEngineFS::new_with_backend_and_storage
    /// [`BackendEngineFS::apply_pins`]: crate::BackendEngineFS::apply_pins
    settled: AtomicBool,
    /// The clock reading at the last start or stop the reconciler made on
    /// this torrent, or [`NEVER_MOVED`] for a torrent it has never moved:
    /// the dwell in [`crate::BackendEngineFS::start_if_stopped`].
    ///
    /// [`crate::BackendEngineFS::start_if_stopped`]: crate::BackendEngineFS
    last_transition_at: AtomicU64,
    /// Files pinned as offline downloads (`BackendEngineFS::pin_download`).
    /// While non-empty the engine is exempt from idle removal and the
    /// reconciler runs it; the handle keeps its own copy for the
    /// want-set planner (`TorrentHandle::pin_file`). Shared with
    /// [`TorrentBacking`], which is how the retention owner learns of a
    /// pin: read as a copy-out, never under any lock of the owner's.
    pub pinned_files: Arc<parking_lot::RwLock<BTreeSet<usize>>>,
    /// The process-wide "nobody named the pin set" condition, shared with
    /// every engine and every backing. [`Self::is_pinned`] answers from it,
    /// which is how a boot the embedder said nothing to keeps every restored
    /// torrent running and keeps every byte of it on the disk.
    pins_unknown: Arc<crate::piece_store::PinsUnknown>,
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
    /// The retention owner for this torrent's files, keyed by file index:
    /// one policy per file, resident, with its turn beside it. What
    /// `announce`, the policy slot, its mirror and the playhead used to be
    /// -- see [`crate::retention::owner`] for the lock rule and the pass.
    /// One active file per torrent is the owner's
    /// [`Retention::install`], which clears every other file first and runs
    /// one at a time over the torrent, as `announce` made it.
    pub(crate) retention: Arc<Retention<TorrentBacking<H>>>,
    /// Which entity the server is playing, shared with the whole process.
    /// Read for a fresh copy where a caller needs one of its own -- the
    /// switch task, a usage figure -- and handed to the pass by the tick,
    /// which takes one reading for the ladder and the pass together.
    live: Arc<Live>,
    /// The turn for the pieces no entity's extent covers: the files of this
    /// torrent nothing has opened in this process, which have no entity and
    /// so no turn of their own. One reclaim of them at a time per engine --
    /// the tick and the switch task both ask -- so two of them cannot offer
    /// librqbit the same run and unlink the second's bytes twice.
    rest: tokio::sync::Mutex<()>,
    /// Where a test puts what playback does while a pass runs.
    ///
    /// Called twice per pass -- before the held reading, and again after
    /// the decision and before the unlinks -- with the file's turn held and
    /// no owner lock, and nowhere in a shipped build. A test that instead
    /// queued a task and trusted an await in the pass to yield to it would
    /// be betting on scheduling: the held reading is a memory read now and
    /// suspends nowhere, and when it was a listing a blocking task that
    /// finished before its handle was first polled yielded nothing either,
    /// so the test measured the ordering it meant to break.
    /// The cell is the engine's, where the tests write it; the owner is
    /// handed a runner that reads it ([`Retention::hook`]).
    #[cfg(test)]
    pub(crate) interleave: Interleave,
    /// How many pieces the passes asked the backend to forget and were
    /// refused, summed over the engine's life: the count the fetch-ahead
    /// bound exists to keep at zero.
    ///
    /// **It is reported, not only tested.** A refusal is a piece inside an
    /// open stream's lookahead and outside the window that would keep it --
    /// the disk permanently over budget, plus a wasted `drop_pieces` per
    /// tick for as long as the stream lives -- and while this was
    /// `#[cfg(test)]` there was no way to see it happening on a device. It
    /// and the waste figure beside it are what made the 1.6 GB open
    /// visible, so they stay.
    pub(crate) refused_reclaims: Arc<AtomicUsize>,
}

/// The cell a test writes what playback does mid-pass into: see
/// [`Engine::interleave`].
#[cfg(test)]
pub(crate) type Interleave = Arc<parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>>;

impl<H: TorrentHandle> Engine<H> {
    /// What the backing hands a pass over `file_idx` as the held set: the
    /// registry's answer for the torrent, narrowed to the file. A probe for
    /// the test that pins the narrowing; the pass reaches it through the
    /// owner.
    #[cfg(test)]
    pub(crate) async fn held_in_file(
        &self,
        store: &Arc<StoreRegistry>,
        file_idx: usize,
    ) -> Option<BTreeSet<u32>> {
        let backing = TorrentBacking {
            handle: self.handle.clone(),
            info_hash: self.info_hash.clone(),
            live: self.live.clone(),
            pinned: self.pinned_files.clone(),
            pins_unknown: self.pins_unknown.clone(),
            refused: self.refused_reclaims.clone(),
        };
        let domain = backing.resolve(file_idx).await?;
        backing.held(store, &domain).await
    }

    pub fn new_with_handle(
        handle: H,
        info_hash: &str,
        clock: crate::Clock,
        volumes: Arc<crate::reconcile::Volumes>,
        budget: Arc<crate::retention::RetentionBudget>,
        live: Arc<Live>,
        pins_unknown: Arc<crate::piece_store::PinsUnknown>,
    ) -> Self {
        let pinned_files = Arc::new(parking_lot::RwLock::new(BTreeSet::new()));
        let refused_reclaims = Arc::new(AtomicUsize::new(0));
        let retention = Retention::new(
            Arc::new(TorrentBacking {
                handle: handle.clone(),
                info_hash: info_hash.to_string(),
                live: live.clone(),
                pinned: pinned_files.clone(),
                pins_unknown: pins_unknown.clone(),
                refused: refused_reclaims.clone(),
            }),
            budget,
        );
        #[cfg(test)]
        let interleave: Interleave = Arc::new(parking_lot::Mutex::new(None));
        #[cfg(test)]
        retention.hook({
            let interleave = interleave.clone();
            move || {
                if let Some(hook) = interleave.lock().as_ref() {
                    hook();
                }
            }
        });
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
            pinned_files,
            pins_unknown,
            volumes,
            reads_refused: AtomicBool::new(false),
            read_wakers: parking_lot::Mutex::new(HashMap::new()),
            next_reader_id: AtomicU64::new(1),
            retention,
            live,
            rest: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            interleave,
            refused_reclaims,
        }
    }

    /// How many pieces this engine's passes have been refused; see the
    /// field. Reported in the stream numbers a client polls.
    pub(crate) fn refused_reclaims(&self) -> usize {
        self.refused_reclaims.load(Ordering::Relaxed)
    }

    /// Whether this process has re-applied its want-set to this torrent
    /// (see the field).
    pub(crate) fn is_settled(&self) -> bool {
        self.settled.load(Ordering::Relaxed)
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

    /// Whether the reconciler is holding this torrent stopped for want of
    /// disk -- which is a wider question than [`Self::is_stopped_for_space`]
    /// and has to be, because they were being asked with two different
    /// lines and the gap between them swallowed torrents whole.
    ///
    /// The ladder measures a `Paused` torrent on a timer at
    /// `crate::reconcile::line`, which is the floor plus
    /// `FREE_SPACE_RESUME_MARGIN`; the stall clock that fails its reads is
    /// started at the same line. `is_stopped_for_space` measures at the
    /// floor alone, deliberately, because the 507 gate is about whether
    /// there is room *now*.
    ///
    /// So for a volume between the two -- which is exactly where a volume
    /// that has just given its slack back sits, since `CacheLimit::effective`
    /// stops the instant `available` reaches the floor -- the reconciler
    /// stopped the torrent and failed its reads while every reader that asks
    /// "is anything wrong?" was told no: `stats.json` reported buffering
    /// with no error. A pinned download stalled at whatever percent it had
    /// reached, in silence, for good.
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
    /// hysteresis line.** What reads this is a client's statistics
    /// (`phase: error`, [`STOPPED_FOR_SPACE_MESSAGE`]) and the stream
    /// route's disk gate, and both are asking about the *device*, which the
    /// rest of this server judges at the floor: it is what
    /// `ensure_download_disk_ready` answers `507` under and what the
    /// published cap keeps free. Judging it at floor + resume margin
    /// instead -- the line the *ladder* holds a stopped torrent at, so that
    /// it has room to run into before it is started again -- tells a client
    /// that a volume the stream route is serving from happily is out of
    /// disk.
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
    /// a fresh one: this is asked on every `stats.json` poll, and it is a
    /// number that moves on the scale of seconds. `false` for a volume nothing has probed yet
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

    /// Where a reader of `file_idx` has got to, in bytes from the start of
    /// the file, told to that file's entity and no other
    /// ([`Retention::note_position`]).
    ///
    /// The tests' spelling of a delivered byte: production bytes go through
    /// the [`FileHandle`]'s own reader, which moves that reader's playhead
    /// as well as the file's head, and a test that drives passes off the
    /// tick with no stream open has only the head to move. A file with no
    /// entity -- nothing installed on it yet -- remembers nothing, as
    /// production's ordering (install, then the reader) never asks it to.
    ///
    /// [`FileHandle`]: crate::files::FileHandle
    #[cfg(test)]
    pub(crate) fn note_playhead(&self, file_idx: usize, offset: u64) {
        self.retention.note_position(&file_idx, (file_idx, offset));
    }

    /// What the player says about itself; see [`Retention::note_playhead`].
    /// `offset` is its own byte offset into `file_idx` and `film` its own
    /// position in the picture.
    /// How long the film is, with no position; see
    /// [`Retention::note_duration`]. What a cast can state.
    pub fn told_duration(&self, file_idx: usize, duration: std::time::Duration) {
        self.retention.note_duration(&file_idx, duration);
    }

    pub fn told_playhead(
        &self,
        file_idx: usize,
        film: std::time::Duration,
        duration: Option<std::time::Duration>,
    ) {
        self.retention.note_playhead(&file_idx, film, duration);
    }

    /// What the retention policy says about `file_idx` right now, or `None`
    /// where there is nothing to say.
    ///
    /// The two absences are both real and neither is a zero. **No policy**
    /// is a torrent nothing is bounding -- the budget covers the file, no
    /// budget has been published yet, or a pin keeps everything -- so there
    /// is no window and no committed set to have a size. **No playhead** is
    /// a file no reader has been inside in this process: nothing that
    /// survives a restart says where a player had got to, and inventing one
    /// from what is on the disk would put a window round a region nobody
    /// has ever read. A reader in another file of the torrent is that
    /// file's; this file keeps the head its own last byte left it with.
    ///
    /// One copy-out of the owner's state and no I/O: the reading is finished
    /// against a listing of the store, which the caller takes for itself
    /// (see [`crate::retention::PolicyReading::window`]) rather than under
    /// any lock. A pass in flight has the policy in its cell like any other
    /// moment, so the committed count is the live one -- a pass parked in
    /// its advertise has already committed the piece it is announcing.
    pub(crate) fn policy_reading(
        &self,
        file_idx: usize,
    ) -> Option<crate::retention::PolicyReading> {
        let holding = self.retention.holding(&file_idx)?;
        let installed = holding.installed?;
        // The entity's head and not its last delivered byte: the two part
        // company exactly when a probe is or has been open, and this is the
        // reading a client draws the behind/ahead split from. On the
        // television the split read 50.3 MB behind and 4.2 ahead with the
        // player at 0:00, because mpv's read of the tail had left the
        // file's head at the end of the file.
        let playhead = TorrentBacking::<H>::index_of(&holding.domain, holding.head?)?;
        Some(crate::retention::PolicyReading::new(
            installed.pieces,
            holding.domain.piece_length,
            playhead,
            installed.committed.len(),
        ))
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
    /// The owner's [`Retention::install`]: every other file's policy is
    /// given back first, a pinned file gets no policy (a pin is a
    /// retention property), a policy that already describes this file under
    /// this budget is kept untouched, and a hold-back the backend refuses
    /// installs nothing.
    pub(crate) async fn begin_retention(&self, file_idx: usize) {
        let outcome = self.retention.install(file_idx, file_idx).await;
        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx,
            ?outcome,
            "retention policy for the file about to stream"
        );
    }

    /// How far ahead of `start_offset` a reader of `file_idx` opened there
    /// may fetch, from one reading of the owner ([`Retention::reach`]) taken
    /// after [`Self::begin_retention`]: the bytes from `start_offset` to the
    /// start of the last piece the window reaches ahead of it, or `None`
    /// when nothing bounds the file.
    ///
    /// The *start* of that piece, not its end: librqbit rounds the end of a
    /// stream's lookahead up to a whole piece, and the reader's offset inside
    /// its piece drifts as it plays, so a lookahead ending exactly at the
    /// edge reaches one piece past it half the time -- a piece the next pass
    /// would reclaim and librqbit refuse to drop. The window's last piece is
    /// fetched by the want-set ([`TorrentBacking::want`]) rather than by the
    /// stream, which is what fetches the rest of the window beyond the
    /// lookahead anyway.
    pub(crate) fn fetch_bound(&self, file_idx: usize, start_offset: u64) -> RetentionBound {
        let forward_bytes = self
            .retention
            .reach(&file_idx, (file_idx, start_offset))
            .map(|(domain, ahead)| {
                // `ahead` is never empty (`RetentionPolicy::ahead_of`), so
                // `end - 1` is the last piece the window reaches.
                (u64::from(ahead.end - 1) * domain.piece_length)
                    .saturating_sub(domain.span.offset)
                    .saturating_sub(start_offset)
            });
        RetentionBound { forward_bytes }
    }

    /// Every policy standing on this torrent, in file order, from one
    /// copy-out under the owner's locks; no I/O. Empty while nothing bounds
    /// it.
    ///
    /// Every file that has been opened has an entity of its own and may
    /// have a policy of its own, so this is a list and not an option: a
    /// reading that kept whichever one the map listed first answered for a
    /// different file from one call to the next.
    #[cfg(test)]
    fn standing_policies(&self, live: &Reading) -> Vec<FileStanding> {
        let mut policies: Vec<FileStanding> = self
            .retention
            .holdings()
            .into_iter()
            .filter_map(|(file_idx, holding)| {
                holding.installed.map(|view| FileStanding {
                    file_idx,
                    view,
                    mode: self.mode_of(live, file_idx),
                })
            })
            .collect();
        policies.sort_by_key(|policy| policy.file_idx);
        policies
    }

    /// What no pass may take of this torrent right now: the held pieces a
    /// pin or a live window keeps, and which files they belong to.
    ///
    /// The two claims a usage figure reports as protected, and the only two
    /// there are. A **pin** keeps every piece of its file for as long as it
    /// stands, whatever any window says. A **[`Mode::Live`] entity** keeps
    /// what its last pass concluded -- the windows round its heads, plus
    /// the half it has committed for sharing, which is advertised and never
    /// reclaimed. An entity nothing bounds has no policy and no window, and
    /// while it is live nothing reclaims any of it, so what it keeps is its
    /// whole extent.
    ///
    /// Everything else is slack: its bytes go at the next pass, so calling
    /// them protected would be telling a caller that a shortfall has no
    /// remedy when the remedy is already running.
    ///
    /// `held` is the reading the bytes are counted against, taken by the
    /// caller: a window over pieces we have not fetched is not occupancy,
    /// so a piece is in the answer only if the store had it when that
    /// reading was taken.
    pub(crate) async fn protects(&self, held: &HeldSnapshot) -> Protected {
        let mut protected = Protected::default();
        for file_idx in self.pinned_file_indices() {
            // A file the backend cannot name pieces for -- no metadata --
            // has nothing on the disk to protect yet.
            if let Some(span) = self.handle.file_pieces(file_idx).await {
                protected.pieces.extend(held.in_range(span.pieces));
                protected.files.insert(file_idx);
            }
        }
        let live = self.live.reading();
        for (file_idx, holding) in self.retention.holdings() {
            if !matches!(self.mode_of(&live, file_idx), Mode::Live) {
                continue;
            }
            protected.files.insert(file_idx);
            match &holding.installed {
                Some(view) => {
                    for window in &holding.windows {
                        protected.pieces.extend(held.in_range(window.clone()));
                    }
                    protected.pieces.extend(
                        view.committed
                            .iter()
                            .copied()
                            .filter(|piece| held.contains(*piece)),
                    );
                }
                None => protected
                    .pieces
                    .extend(held.in_range(holding.extent.clone())),
            }
        }
        protected
    }

    /// What a pass over `file_idx` would be for, from `live`.
    ///
    /// [`Mode::Live`] for the file the server is playing, and for a file
    /// some read is still delivering. **An open read is not liveness**: it
    /// is an in-flight response, and taking the bytes out from under one is
    /// a broken read for a player and a fetch the origin is paid for twice.
    /// It keeps that file's window for as long as the body lasts and no
    /// longer -- which is what makes the aside rule cheap, since a subtitle
    /// read while a film plays is a second entity with a reader of its own.
    ///
    /// Everything else is [`Mode::Slack`]: nobody is playing it, nobody is
    /// reading it, and its bytes go. Not a clock anywhere -- a stream that
    /// stopped is not a stream that has been replaced, and pausing for an
    /// hour changes nothing here.
    fn mode_of(&self, live: &Reading, file_idx: usize) -> Mode {
        // The readers and the count of opens off one lock: the pass compares
        // the count against the entity's to find a stream that opened after
        // this reading, and an install that landed between a reading of the
        // readers and a separate reading of the count would be inside the
        // count and not yet a reader -- an aside's install completes before
        // its reader opens -- so the pass would find the count unchanged
        // and delete under the stream. See
        // [`Retention::readers_and_opens_of`].
        let (readers, opens) = self.retention.readers_and_opens_of(&file_idx);
        if live.file_of(&self.info_hash) == Some(file_idx) || readers > 0 {
            Mode::Live
        } else {
            // The count of opens this reading was taken beside, so the pass
            // can tell a file nobody has opened from one a viewer started
            // while this tick was walking the file before it. See
            // [`Mode::Slack`].
            Mode::Slack { opens }
        }
    }

    /// The files this tick's passes run on and what each pass is for: every
    /// file with an entity, in file order, with its [`Mode`] from the one
    /// reading the tick took.
    ///
    /// Every entity, not only the ones with a policy standing. A file the
    /// viewer left has no policy after its first slack pass and still holds
    /// whatever that pass could not take -- a delete refused under a hash
    /// check -- so the entity is what has to be walked, and it is forgotten
    /// once it holds nothing ([`Retention::forget_empty`]).
    ///
    /// Each has a head of its own, and so a window of its own: the file
    /// being played where its reader is, a file with an open read where
    /// that read is. A pick of one file per tick, whichever rule picked it,
    /// left the other's window unmeasured for as long as both stood.
    fn files_to_pass(&self, live: &Reading) -> Vec<(usize, Mode)> {
        let mut keys = self.retention.keys();
        keys.sort_unstable();
        keys.into_iter()
            .map(|file_idx| {
                let mode = self.mode_of(live, file_idx);
                (file_idx, mode)
            })
            .collect()
    }

    /// One retention pass per file with a policy standing, in file order,
    /// each under its own turn: what the policy makes of where that file's
    /// head is now, and the calls that make it so. `None` when no pass
    /// concluded anything -- no policy, no reader has been anywhere in a
    /// bounded file yet, a pin, or a torrent with no registered store. Every
    /// one of those leaves the policies where they were; none of them is a
    /// pass that ran and found nothing, which is `Some` with a zeroed count.
    /// Where more than one pass concluded, the counts are summed.
    ///
    /// The file's turn from the first line of its pass to the last
    /// ([`Retention::turn`] then [`Retention::pass`]), and released before
    /// the next file's is taken (rule 4 of the owner: no two turns at once).
    /// A second pass queues behind this one and a [`Self::begin_retention`]
    /// that arrives meanwhile waits its turn. The policy stays in its cell throughout; a pass that
    /// dies at an await drops the turn like any other local and the next
    /// tick's pass runs.
    ///
    /// Not while the torrent is under its initial hash check. The check is
    /// reading every piece it means to claim, and the pass would only be
    /// refused at every door below -- the reclaim's run-state reading, the
    /// registry's own refusal -- one warning per run per tick for as long
    /// as the check takes. `None`, as for any torrent with nothing to pass
    /// over.
    pub(crate) async fn retain(
        &self,
        store: &Arc<StoreRegistry>,
        live: &Reading,
    ) -> Option<crate::retention::RetentionPass> {
        if matches!(
            self.handle.run_state(),
            crate::backend::RunState::Initializing { .. }
        ) {
            return None;
        }
        // The pinned files nothing has opened first, so that the entities
        // this makes are among the ones this tick passes over.
        adopt_pins(
            &self.retention,
            &self.pinned_file_indices(),
            &self.info_hash,
        )
        .await;
        let mut total = self.pass_over(store, &self.files_to_pass(live)).await;
        // And the files nothing has opened in this process. They have no
        // entity, so no pass walks them and no window is drawn round them;
        // on a torrent nobody is playing, every byte of them nobody pinned
        // is slack -- the twelve other episodes the swarm filled while one
        // was watched, whether or not a thirteenth is kept for offline.
        if !live.is_torrent(&self.info_hash) {
            let freed = self.reclaim_rest(store).await;
            if freed > 0 {
                total.get_or_insert_default().reclaimed += freed;
            }
        }
        total
    }

    /// A slack pass over every entity of this torrent that is neither being
    /// played nor being read, and the unpinned pieces outside every entity
    /// if the torrent itself is not being played.
    ///
    /// What the switch, the running-low bell and `POST /cache/clean` call:
    /// the same passes the tick would run two seconds later, run now,
    /// because the moment a viewer starts something else is the moment the
    /// old film became disposable. It takes its own reading of what is
    /// being played -- it is not inside a tick -- and every door below asks
    /// again at the unlink.
    pub(crate) async fn drop_slack(
        &self,
        store: &Arc<StoreRegistry>,
    ) -> Option<crate::retention::RetentionPass> {
        if matches!(
            self.handle.run_state(),
            crate::backend::RunState::Initializing { .. }
        ) {
            return None;
        }
        let live = self.live.reading();
        let slack: Vec<(usize, Mode)> = self
            .files_to_pass(&live)
            .into_iter()
            .filter(|(_, mode)| matches!(mode, Mode::Slack { .. }))
            .collect();
        let mut total = self.pass_over(store, &slack).await;
        if !live.is_torrent(&self.info_hash) {
            let freed = self.reclaim_rest(store).await;
            if freed > 0 {
                total.get_or_insert_default().reclaimed += freed;
            }
        }
        total
    }

    /// One pass per file, in the order given, each under its own turn.
    ///
    /// The file's turn from the first line of its pass to the last
    /// ([`Retention::turn`] then [`Retention::pass`]), and released before
    /// the next file's is taken (rule 4 of the owner: no two turns at once).
    async fn pass_over(
        &self,
        store: &Arc<StoreRegistry>,
        files: &[(usize, Mode)],
    ) -> Option<crate::retention::RetentionPass> {
        let mut total: Option<crate::retention::RetentionPass> = None;
        for (file_idx, mode) in files {
            let Some(claim) = self.retention.turn(file_idx).await else {
                continue;
            };
            let Some(concluded) = self
                .retention
                .pass(file_idx, store, claim, *mode)
                .await
                .concluded
            else {
                continue;
            };
            let total = total.get_or_insert_default();
            total.committed += concluded.committed;
            total.reclaimed += concluded.reclaimed;
            total.withdrawn += concluded.withdrawn;
        }
        total
    }

    /// Take the pieces no entity's extent covers off the disk, and say how
    /// many went.
    ///
    /// The pass walks entities, and an entity exists for a file something
    /// opened. What a torrent holds of the files nothing ever opened -- a
    /// season the swarm filled around the one episode that was watched, a
    /// resume bitfield's worth of a restored torrent -- is in no pass's
    /// extent and so in no pass's reclaim, and on a torrent nobody is
    /// playing there is nothing that would ever want it again -- except a
    /// pin, whose file's pieces are left where they are. A pin is per file:
    /// one episode kept for offline keeps that episode, and the rest of the
    /// season is as disposable as any unpinned torrent's.
    ///
    /// The pin set is read before every run and cuts it, rather than read
    /// once for the whole reclaim, like the liveness cell: a pin taken
    /// mid-reclaim keeps the file's pieces the runs after it would have
    /// taken. A boundary piece a pinned file shares with a file nothing
    /// opened is the pinned file's, and stays with it. And nothing at all
    /// is taken while the pin set is unknown, which reads as every file
    /// pinned.
    ///
    /// Held back before it is unlinked, run by run, like every other
    /// deletion here: there is no un-Have. And the liveness cell is read
    /// again before every run rather than carried in from the caller's
    /// reading -- a viewer who opens this torrent again mid-delete stops it
    /// where it stands, and the entity their open installs keeps the rest.
    ///
    /// One at a time per engine (`rest`): the tick and the switch task both
    /// call it, and two of them offering librqbit the same run would have
    /// the second unlink what the first had already claimed.
    async fn reclaim_rest(&self, store: &Arc<StoreRegistry>) -> usize {
        let _rest = self.rest.lock().await;
        if !matches!(
            self.handle.run_state(),
            crate::backend::RunState::Live | crate::backend::RunState::Paused
        ) {
            return 0;
        }
        let Some(held) = store.held(&self.info_hash) else {
            return 0;
        };
        let extents: Vec<Range<u32>> = self
            .retention
            .holdings()
            .into_iter()
            .map(|(_, holding)| holding.extent)
            .collect();
        let outside: Vec<u32> = held
            .all()
            .into_iter()
            .filter(|piece| !extents.iter().any(|extent| extent.contains(piece)))
            .collect();
        let mut freed = 0;
        let mut pending: VecDeque<Range<u32>> = crate::retention::runs(&outside).into();
        while let Some(run) = pending.pop_front() {
            let pinned = pinned_spans(&self.handle, &self.pinned_files, None).await;
            if self.live.is_torrent(&self.info_hash) || self.pins_unknown.is_set() {
                break;
            }
            let mut parts = vec![run.clone()];
            for span in &pinned {
                parts = parts
                    .into_iter()
                    .flat_map(|part| crate::retention::outside(part, span))
                    .collect();
            }
            if parts.len() != 1 || parts[0] != run {
                // Cut by a pin: each part is asked about again in its own
                // turn, as the reclaim's door asks about the parts a window
                // cut. Strictly fewer pieces each time, so this ends.
                for part in parts.into_iter().rev() {
                    pending.push_front(part);
                }
                continue;
            }
            if let Err(error) = self.handle.set_pieces_advertised(run.clone(), false).await {
                tracing::warn!(
                    info_hash = %self.info_hash,
                    first = run.start,
                    end = run.end,
                    error = %format!("{error:#}"),
                    "could not hold a slack torrent's pieces back from what we announce; leaving their bytes"
                );
                continue;
            }
            freed += crate::retention::release(&self.handle, store, &self.info_hash, run).await;
        }
        freed
    }

    /// Every policy standing on this torrent, as a value -- **for the
    /// tests, which is all that asks now.**
    ///
    /// It was the cache cleaner's question: what may be taken off this
    /// torrent, asked before a walk of the disk and again at every delete.
    /// The walk is gone and so is the second asking; what a caller outside
    /// this crate can still learn about the cache it asks
    /// [`Self::protects`], which is a reading of what is on the disk rather
    /// than of what is installed.
    ///
    /// One copy-out under the owner's locks and one reading of what is
    /// being played, so the policies and their modes cannot disagree about
    /// which file is live.
    #[cfg(test)]
    pub(crate) async fn standing(&self) -> Standing {
        Standing {
            policies: self.standing_policies(&self.live.reading()),
        }
    }

    /// Whether anything about this torrent is pinned -- what exempts it
    /// from idle removal and makes the reconciler run it. Not what keeps
    /// its bytes: that is per file ([`TorrentBacking::keeps_everything`]),
    /// and the files beside a pinned one are retained as any other.
    ///
    /// Always true while the pin set is unknown ([`Self::pins_unknown`]):
    /// the embedder is the only thing that can say what a pin is, and a boot
    /// it said nothing to must report and treat every restored torrent as
    /// pinned rather than as unpinned. There is no way out of it inside the
    /// process -- the next boot is told, or it is not.
    pub fn is_pinned(&self) -> bool {
        self.pins_unknown.is_set() || !self.pinned_files.read().is_empty()
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
        let bound = self.fetch_bound(file_idx, start_offset);
        // The smaller of what the intent would read ahead and what the
        // window will keep: a stream that fetched past the window would have
        // every pass reclaim what it just fetched, and librqbit refuses to
        // drop a piece a stream is about to read, so the disk would sit over
        // budget by the lookahead for the stream's life. At least one byte:
        // librqbit refuses a stream that reads nothing ahead.
        let lookahead_bytes = bound
            .forward_bytes
            .unwrap_or(u64::MAX)
            .min(priorities::librqbit_stream_lookahead_bytes(intent, buffer))
            .max(1);

        let reader_start = Instant::now();
        let reader = self
            .handle
            .get_file_reader(file_idx, start_offset, priority, None, lookahead_bytes)
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
            crate::files::Opening {
                file_idx,
                start_offset,
                intent,
                lookahead_bytes,
                buffer,
            },
        ))
    }
}

#[cfg(test)]
mod pin_tests {
    use super::*;
    use crate::backend::{BackendFileInfo, FileStreamTrait, RunState, TransferTotals};
    use crate::retention::RetentionBudget;

    const PIECE: u64 = 1000;

    /// The least a handle can be for the owner to install on it: two files
    /// of eight pieces, live, recording every range it is asked to want
    /// again.
    #[derive(Clone)]
    struct PinnedHandle {
        reselected: Arc<parking_lot::Mutex<Vec<Range<u32>>>>,
    }

    #[async_trait::async_trait]
    impl TorrentHandle for PinnedHandle {
        fn info_hash(&self) -> String {
            "pinned".to_string()
        }

        fn name(&self) -> Option<String> {
            None
        }

        async fn stats(&self) -> EngineStats {
            EngineStats::resolving_metadata("pinned", &[])
        }

        fn transfer_totals(&self) -> Option<TransferTotals> {
            None
        }

        async fn add_trackers(&self, _trackers: Vec<String>) -> anyhow::Result<()> {
            Ok(())
        }

        fn run_state(&self) -> RunState {
            RunState::Live
        }

        async fn get_file_reader(
            &self,
            _file_idx: usize,
            _start_offset: u64,
            _priority: u8,
            _bitrate: Option<u64>,
            _lookahead_bytes: u64,
        ) -> anyhow::Result<Box<dyn FileStreamTrait>> {
            anyhow::bail!("nothing here is read")
        }

        async fn get_files(&self) -> Vec<BackendFileInfo> {
            Vec::new()
        }

        async fn prepare_file_for_streaming(&self, _file_idx: usize) -> anyhow::Result<()> {
            Ok(())
        }

        async fn file_pieces(&self, file_idx: usize) -> Option<FilePieceSpan> {
            (file_idx < 2).then(|| {
                let start = file_idx as u32 * 8;
                FilePieceSpan {
                    pieces: start..start + 8,
                    offset: u64::from(start) * PIECE,
                    bytes: 8 * PIECE,
                }
            })
        }

        fn piece_length(&self) -> Option<u64> {
            Some(PIECE)
        }

        async fn reselect_pieces(&self, pieces: Range<u32>) -> anyhow::Result<usize> {
            self.reselected.lock().push(pieces.clone());
            Ok((pieces.end - pieces.start) as usize)
        }
    }

    fn owner(
        handle: PinnedHandle,
        pinned: Arc<parking_lot::RwLock<BTreeSet<usize>>>,
    ) -> Arc<Retention<TorrentBacking<PinnedHandle>>> {
        Retention::new(
            Arc::new(TorrentBacking {
                handle,
                info_hash: "pinned".to_string(),
                live: Arc::new(Live::new()),
                pinned,
                pins_unknown: Arc::default(),
                refused: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(RetentionBudget::default()),
        )
    }

    /// **A file the backend cannot place is a refusal to drop, not a
    /// backend with no have-set.** `drop_file_pieces` answered `Ok(None)`
    /// for a file whose pieces `file_pieces` could not name -- no metadata
    /// yet, or no such file -- which is what a backend that keeps no
    /// have-set answers, where its doc says `Err`: the backend has not
    /// agreed to forget anything, and the delete that asked is told so and
    /// says so.
    #[tokio::test]
    async fn a_file_the_backend_cannot_place_is_refused_by_drop_file_pieces() {
        let handle = PinnedHandle {
            reselected: Arc::default(),
        };
        assert!(
            handle.drop_file_pieces(0).await.unwrap().is_none(),
            "a file it places goes to drop_pieces, which keeps no have-set here"
        );
        assert!(handle.drop_file_pieces(2).await.is_err());
    }

    /// **A pinned file with no entity is wanted whole, once.**
    ///
    /// The case no pass reaches: a slack pass dropped the file's pieces,
    /// emptied it and forgot its entity (or `reclaim_rest` dropped them,
    /// and there never was one), and then the user pinned the file without
    /// opening it. The fork's `pin_file` re-queues nothing for a file that
    /// was selected all along, the passes walk entities and it has none,
    /// and `reclaim_rest` leaves a pinned file's pieces alone -- so nothing
    /// wanted the pieces again and the download stood still. The tick's
    /// adoption installs on it under the pin, which wants the whole file,
    /// and the entity it leaves is what stops it doing so again.
    #[tokio::test]
    async fn a_pinned_file_with_no_entity_is_wanted_whole_once() {
        let handle = PinnedHandle {
            reselected: Arc::default(),
        };
        let pinned = Arc::new(parking_lot::RwLock::new(BTreeSet::new()));
        let owner = owner(handle.clone(), pinned.clone());

        adopt_pins(&owner, &[], "pinned").await;
        assert!(
            handle.reselected.lock().is_empty(),
            "nothing pinned, nothing adopted"
        );

        pinned.write().insert(1);
        adopt_pins(&owner, &[1], "pinned").await;
        assert_eq!(
            *handle.reselected.lock(),
            vec![8..16],
            "the pinned file is wanted whole"
        );
        assert!(owner.holding(&1).is_some(), "and has an entity now");
        assert!(
            owner.holding(&0).is_none(),
            "the file nobody pinned or opened still has none"
        );

        adopt_pins(&owner, &[1], "pinned").await;
        assert_eq!(
            *handle.reselected.lock(),
            vec![8..16],
            "once: the entity it made is what every later tick's pass reaches it through"
        );
    }
}
