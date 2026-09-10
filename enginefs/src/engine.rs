use crate::backend::{
    EngineStats, FilePieceSpan, TorrentHandle,
    priorities::{self, BufferProfile, PlaybackIntent},
};
use crate::cache::DataCache;
use crate::piece_store::{RetentionPolicy, Share, StoreRegistry};
use crate::retention::owner::{
    Backing, Door, Install, InstalledView, Liveness, Retention, Trigger,
};
use anyhow::Context;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Range;
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

/// One torrent's standing with the cache cleaner: what it will give up,
/// and whether it is one of the torrents the cleaner is evicting *for*.
///
/// The walk collects one of these per engine into a
/// [`crate::retention::ReclaimGate`] and the delete asks for it again --
/// see [`Engine::standing`], which is the one place it is computed.
pub(crate) struct Standing {
    pub gate: crate::retention::TorrentGate,
    /// Every policy standing on this torrent, in file order, from the one
    /// copy-out the gate was built from -- whatever the gate's shape, so a
    /// delete can find the turn a piece is ordered under even where the pin
    /// or the error state answered for the torrent. Empty when none stands.
    pub policies: Vec<FileStanding>,
    /// The backend stopped this torrent for want of space, or the
    /// reconciler did before the backend could: the cleaner has bytes to
    /// find for it.
    pub stopped_for_space: bool,
}

/// One file's standing policy, as a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStanding {
    /// The file the policy is over -- the key its turn is taken under.
    pub file_idx: usize,
    /// The policy itself.
    pub view: InstalledView,
}

/// The file whose standing policy governs `piece`: the lowest-numbered of
/// those whose range holds it, so the boundary piece two files share is
/// always the first file's to order. `None` when no policy's range holds it.
fn file_governing(policies: &[FileStanding], piece: u32) -> Option<usize> {
    policies
        .iter()
        .find(|policy| policy.view.pieces.contains(&piece))
        .map(|policy| policy.file_idx)
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
    /// [`Engine::pinned_files`], the same `Arc`: a pin is a retention
    /// property, and the owner asks about it before every pass and at every
    /// door.
    pinned: Arc<parking_lot::RwLock<BTreeSet<usize>>>,
    /// Pieces a reclaim asked the backend to forget and did not get back --
    /// a peer mid-flight on them, or a stream's lookahead over them. Shared
    /// with [`Engine::refused_reclaims`], where a test reads it.
    #[cfg(test)]
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
    /// A policy lives as long as the active file: the next file's install
    /// clears it, and nothing else does.
    const LIVENESS: Liveness = Liveness::UntilReplaced;

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

    fn policy(domain: &FileDomain, budget: u64) -> anyhow::Result<RetentionPolicy> {
        RetentionPolicy::new(
            budget,
            domain.piece_length,
            domain.span.pieces.clone(),
            domain.span.bytes,
            Self::SHARE,
        )
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

    /// Any pin on the torrent: a pin is a retention property, the user asked
    /// for those bytes, and they are shared like any other bytes we keep.
    fn keeps_everything(&self, _file_idx: &usize) -> bool {
        !self.pinned.read().is_empty()
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

    async fn advertise(&self, pieces: Range<u32>, on: bool) -> anyhow::Result<()> {
        self.handle
            .set_pieces_advertised(pieces, on)
            .await
            .map(|_changed| ())
    }

    async fn alone(&self, domain: &FileDomain, pieces: &[u32]) -> Vec<u32> {
        crate::retention::this_files_alone(&self.handle, domain.file_idx, pieces).await
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
    /// is a file the store counts and the backend has forgotten, and the
    /// next pass cannot take it -- librqbit refuses to drop a piece twice --
    /// so it would sit there, offered and refused every tick, until a
    /// restart. So what librqbit reports dropped is read against the store's
    /// set *now*, and whatever is on the disk goes under the claim, as a
    /// reclaim's pieces do. The set is read after the drop: a piece the
    /// store has is one librqbit had, never the reverse.
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
                                        && !windows.iter().any(|window| window.contains(piece))
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
        _domain: &FileDomain,
        runs: Vec<Range<u32>>,
        door: Door<Self>,
    ) -> usize {
        let mut reclaimed = 0;
        let mut pending: VecDeque<Range<u32>> = runs.into();
        while let Some(run) = pending.pop_front() {
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
            for window in &windows {
                parts = parts
                    .into_iter()
                    .flat_map(|part| crate::retention::outside(part, window))
                    .collect();
            }
            if parts.len() == 1 && parts[0] == run {
                #[cfg(test)]
                let asked = (run.end - run.start) as usize;
                let freed =
                    crate::retention::release(&self.handle, store, &self.info_hash, run).await;
                #[cfg(test)]
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
    /// want-set planner (`TorrentHandle::pin_file`). Shared with
    /// [`TorrentBacking`], which is how the retention owner learns of a
    /// pin: read as a copy-out, never under any lock of the owner's.
    pub pinned_files: Arc<parking_lot::RwLock<BTreeSet<usize>>>,
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
    /// bound exists to keep at zero, read by the test that pins it.
    #[cfg(test)]
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
            pinned: self.pinned_files.clone(),
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
    ) -> Self {
        let pinned_files = Arc::new(parking_lot::RwLock::new(BTreeSet::new()));
        #[cfg(test)]
        let refused_reclaims = Arc::new(AtomicUsize::new(0));
        let retention = Retention::new(
            Arc::new(TorrentBacking {
                handle: handle.clone(),
                info_hash: info_hash.to_string(),
                pinned: pinned_files.clone(),
                #[cfg(test)]
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
            last_active_at: AtomicU64::new(clock.now_secs()),
            pinned_files,
            volumes,
            reads_refused: AtomicBool::new(false),
            read_wakers: parking_lot::Mutex::new(HashMap::new()),
            next_reader_id: AtomicU64::new(1),
            retention,
            #[cfg(test)]
            interleave,
            #[cfg(test)]
            refused_reclaims,
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
        let playhead = TorrentBacking::<H>::index_of(&holding.domain, holding.last_position?)?;
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
    /// given back first, a pinned torrent gets no policy (a pin is a
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
    /// One file at most has a policy installed -- [`Retention::install`]
    /// clears every other before it installs, and installs are ordered over
    /// the owner so two opens on different files cannot each miss the
    /// other. The one way two can stand is a sibling whose range the
    /// backend would not take back at its retiring (`retire_siblings` warns
    /// and goes on), and then both are here: a reading that kept the first
    /// the map listed answered for a different file from one call to the
    /// next, called the other file's held-back pieces announced, and had
    /// the delete take that one file's turn for a request about both.
    fn standing_policies(&self) -> Vec<FileStanding> {
        let mut policies: Vec<FileStanding> = self
            .retention
            .holdings()
            .into_iter()
            .filter_map(|(file_idx, holding)| {
                holding
                    .installed
                    .map(|view| FileStanding { file_idx, view })
            })
            .collect();
        policies.sort_by_key(|policy| policy.file_idx);
        policies
    }

    /// The files this tick's passes run on: every file with a policy
    /// standing, in file order. One copy-out under the owner's locks, no
    /// I/O; empty while nothing bounds the torrent.
    ///
    /// Every one, because each has a head of its own. A pass measures from
    /// its file's head, and with two policies standing -- the viewer went on
    /// to the next episode and the backend would not give the file left
    /// back -- both files have one: the file being played where its reader
    /// is, the file left where its reader last was. A pick of one file per
    /// tick, whichever rule picked it, left the other's window unmeasured
    /// for as long as both stood: nothing committed for sharing, nothing
    /// outside its window reclaimed, its want-set never trimmed. Each pass
    /// concludes on its own file's head, and a file with none concludes
    /// nothing.
    fn files_to_pass(&self) -> Vec<usize> {
        let mut standing: Vec<usize> = self
            .retention
            .holdings()
            .into_iter()
            .filter(|(_, holding)| holding.installed.is_some())
            .map(|(file_idx, _)| file_idx)
            .collect();
        standing.sort_unstable();
        standing
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
    /// A second pass queues behind this one, a [`Self::begin_retention`]
    /// that arrives meanwhile waits its turn, and the cleaner's delete waits
    /// behind both. The policy stays in its cell throughout; a pass that
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
    ) -> Option<crate::retention::RetentionPass> {
        if matches!(
            self.handle.run_state(),
            crate::backend::RunState::Initializing { .. }
        ) {
            return None;
        }
        let mut total: Option<crate::retention::RetentionPass> = None;
        for file_idx in self.files_to_pass() {
            let Some(claim) = self.retention.turn(&file_idx).await else {
                continue;
            };
            let Some(concluded) = self.retention.pass(&file_idx, store, claim).await.concluded
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

    /// What this engine tells the cache cleaner it may take -- **asked by
    /// the walk and asked again by the delete, from this one function.**
    ///
    /// The walk's copy is a reading taken before a blocking directory walk
    /// and every delete before this one, so by the time a delete arrives it
    /// can be minutes old, and [`Self::release_reclaimable`] asks again at
    /// the door. It has to ask the *same* question. A second asking that
    /// answered a narrower one -- as this used to, taking every piece the
    /// cleaner named unless a policy was in the slot to refuse it -- is not
    /// a check on the reading but a way round it: the walk protects a
    /// torrent that announces everything and a pinned one, and the delete
    /// took their pieces anyway whenever its reading had gone stale in
    /// between. So everything the walk folds into its verdict is folded in
    /// here:
    ///
    /// * a **pinned** torrent announces everything and releases nothing,
    ///   whatever policy is still installed -- a pin taken while the file
    ///   was streaming leaves the policy installed until the next pass
    ///   clears it, and the download the user asked to keep must not lose
    ///   pieces through that gap;
    /// * a torrent the backend stopped with an **error** announces nothing,
    ///   and its bytes go first;
    /// * otherwise the policy answers, or the absence of one does.
    ///
    /// A torrent stopped for **space** is not an error here: the cleaner is
    /// what gets it going again, by evicting other bytes, so it keeps its
    /// policy's protection meanwhile and is named to the cleaner separately
    /// ([`Standing::stopped_for_space`]).
    ///
    /// The policies' half is read off the owner's live cells, so a pass in
    /// flight answers `Policy` with the committed set as the pass has
    /// advanced it, never "no policy" -- and every standing policy is in
    /// the answer, in file order, so two callers reading the same cells get
    /// the same gate and the same files.
    pub(crate) async fn standing(&self) -> Standing {
        // One copy-out for the gate and the files: a gate built from one
        // reading and files listed from another could name a turn the gate
        // was not built under.
        let policies = self.standing_policies();
        if self.is_pinned() {
            return Standing {
                gate: crate::retention::TorrentGate::Announced,
                policies,
                stopped_for_space: false,
            };
        }
        let stopped_for_space =
            self.is_stopped_for_space().await || self.handle.is_out_of_space().await;
        if !stopped_for_space && self.handle.is_in_error_state().await {
            return Standing {
                gate: crate::retention::TorrentGate::Nothing,
                policies,
                stopped_for_space,
            };
        }
        let gate = if policies.is_empty() {
            crate::retention::TorrentGate::Announced
        } else {
            crate::retention::TorrentGate::Policy(
                policies
                    .iter()
                    .map(|policy| crate::retention::FilePolicy {
                        file_idx: policy.file_idx,
                        pieces: policy.view.pieces.clone(),
                        committed: policy.view.committed.clone(),
                        live: true,
                    })
                    .collect(),
            )
        };
        Standing {
            gate,
            policies,
            stopped_for_space,
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
    /// asked again here, under the file's turn -- which those two also take
    /// -- and the answer taken from the live policy rather than from a copy
    /// of it.
    ///
    /// The gate is not the only narrowing the cleaner's request needs. A
    /// policy's file shares its first and last piece with its neighbours,
    /// and the gate cannot see that -- it answers for the torrent, so a
    /// shared piece is in range and uncommitted like any other. So
    /// [`crate::retention::this_files_alone`] is asked here as well as in
    /// the pass, and for the same reason: unlinking a piece a still-wanted
    /// neighbour owns is a refetch loop, whichever caller does it.
    ///
    /// **One turn per file, and the file is the piece's.** The pieces are
    /// grouped by the policy whose range holds them, and each group is
    /// released under that file's turn, one file after another, with the
    /// question asked again under each. Taking one turn for the whole
    /// request was the value read once and trusted later: with two policies
    /// standing the turn held was one file's and the gate answered for
    /// whichever the map listed, so the other file's held-back pieces were
    /// refused as announced -- and which file's, by the map's order.
    /// A piece no policy's range holds is released under no turn, because
    /// nothing orders it: only the error state's gate gives such a piece up.
    pub(crate) async fn release_reclaimable(
        &self,
        store: &Arc<StoreRegistry>,
        pieces: &[u32],
    ) -> usize {
        // The grouping reading. It decides which turns are taken and
        // nothing else; what is taken is decided under each.
        let policies = self.standing().await.policies;
        let mut by_file: std::collections::BTreeMap<Option<usize>, Vec<u32>> =
            std::collections::BTreeMap::new();
        for piece in pieces {
            by_file
                .entry(file_governing(&policies, *piece))
                .or_default()
                .push(*piece);
        }
        let mut freed = 0;
        for (file, group) in by_file {
            // The turn is per file, and the file is the one whose policy
            // holds the piece. Taken before the asking, held across the
            // unlink: a pass on that file cannot commit a piece between
            // this reading and its release. A piece in no policy's range
            // has no turn to take and nothing that could advertise it but
            // an install, whose hold-back makes a piece less announced, not
            // more.
            let _turn = match file {
                Some(file_idx) => self.retention.turn(&file_idx).await,
                None => None,
            };
            freed += self.release_under_turn(store, file, &group).await;
        }
        freed
    }

    /// The cleaner's question, asked again at the door for one group of
    /// pieces, with the turn of `file` (the policy whose range held them at
    /// the grouping) in hand; `None` is the group no policy held.
    ///
    /// The same question as `standing`, and not a narrower one. This used to
    /// filter only where a policy was in the slot and take everything the
    /// cleaner named otherwise, on the argument that a torrent with no
    /// policy had no opinion the cleaner did not already have. But the
    /// cleaner's own rule already refuses every piece of a torrent that
    /// announces everything, so the only way it comes to ask for one is a
    /// reading that went stale on the way here: a reader opened on the
    /// torrent since the walk (which found no engine and called its bytes
    /// cache), a pin was taken since, or the policy that released the piece
    /// was cleared since. Taking the piece then was the advertise-then-refuse
    /// the gate exists to prevent, on exactly the torrents a reader has just
    /// opened -- and the delete of a download the user had just pinned.
    ///
    /// And a piece the gate still releases is taken only if the policy that
    /// holds it now is the one whose turn this is: a policy that moved to
    /// another file between the grouping and this turn -- the reader went
    /// to the next episode, and its boundary piece is in the new range --
    /// is ordered by a turn not held here, and nothing is taken on it. The
    /// cleaner's next walk asks again.
    async fn release_under_turn(
        &self,
        store: &Arc<StoreRegistry>,
        file: Option<usize>,
        pieces: &[u32],
    ) -> usize {
        let standing = self.standing().await;
        let mut still: Vec<u32> = pieces
            .iter()
            .copied()
            .filter(|piece| {
                standing.gate.releases(*piece)
                    && (matches!(standing.gate, crate::retention::TorrentGate::Nothing)
                        || file_governing(&standing.policies, *piece) == file)
            })
            .collect();
        if still.len() != pieces.len() {
            tracing::debug!(
                info_hash = %self.info_hash,
                file,
                asked = pieces.len(),
                taking = still.len(),
                "the cleaner's reading of this torrent went stale before its delete"
            );
        }
        // The gate has no answer about the boundary. It speaks for the
        // torrent, and a piece the policy's file shares with a neighbour is
        // in range and uncommitted exactly like any other, so a policy
        // offers the cleaner the same shared pieces the retention pass
        // offers itself. The same narrowing is therefore made here, or the
        // cleaner's delete starts the refetch loop the pass no longer
        // starts. Only a policy has a file to narrow by; where there is
        // none this engine already has no opinion.
        if let (Some(file_idx), crate::retention::TorrentGate::Policy(_)) = (file, &standing.gate) {
            still = crate::retention::this_files_alone(&self.handle, file_idx, &still).await;
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
            file_idx,
            start_offset,
        ))
    }
}
