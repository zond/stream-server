//! Bounding the streaming cache, and telling the truth about what we hold.
//!
//! [`crate::piece_store::policy`] is the arithmetic -- a budget, a piece
//! length, the pieces of one file, where the playhead is, and out of it a
//! window to keep, a set to share and a set to give back. This module is
//! the wiring: it holds one [`RetentionPolicy`] per torrent, feeds it the
//! playhead a reader actually reached, and turns its answers into the three
//! calls that make them true --
//!
//! * the window is **held back** from what we announce
//!   ([`crate::backend::TorrentHandle::set_pieces_advertised`]), before the
//!   reader opens and so before any of its pieces exist, because there is no
//!   un-Have in BitTorrent and a piece announced once cannot be unannounced;
//! * a piece the window **releases** is committed, and only then advertised;
//! * everything else the torrent holds of that file is **reclaimed**, which
//!   is the backend forgetting it and the store unlinking it, in that order
//!   and under one claim ([`take_claimed`]).
//!
//! So the only pieces a peer is ever told about are the ones nothing will
//! ever reclaim. That was a decision the policy could state and not perform
//! until the fork gained a have-bit that is not an announcement; it performs
//! it now.
//!
//! # What decides the budget, and what a missing one means
//!
//! The budget is the cache cleaner's -- `CacheLimit::effective`, the smaller
//! of what the operator configured and what the volume can still give. It is
//! pushed in ([`RetentionBudget::set`]) and never recomputed here: a second
//! reading of "how much room is there" that disagreed with the cleaner's
//! would have the two layers evicting against different numbers.
//!
//! **At process start it is [`CacheBudget::Unknown`], and that is true rather
//! than a placeholder**: nothing has told this process a budget yet. It is
//! not zero, which would say the cache may hold nothing, and it is not a
//! number this process invented from a disk reading of its own, which would
//! be a claim about a cap nobody set. Unknown means no policy is installed,
//! so a stream that starts before the cleaner's first pass holds what it
//! holds and announces all of it -- exactly what this server did before any
//! of this existed. The cleaner's fallback poll fires its first tick
//! immediately, so the window is the length of one cache walk.

use std::collections::BTreeSet;
use std::ops::Range;

use crate::backend::{AfterRelease, FilePieceSpan, TorrentHandle};
use crate::piece_store::{RetentionPolicy, Shape, Share, StoreRoot};

/// What the cache cleaner says the torrent-data volume may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheBudget {
    /// Nothing has told this process a budget. **An absence, not a
    /// number**: no cache pass has run yet, so there is nothing to be
    /// right or wrong about. Nothing is held back and nothing is
    /// reclaimed under it.
    #[default]
    Unknown,
    /// There is no cap: the operator set no `cacheSize` and the volume's
    /// free space could not be read. Nothing to bound, so -- as under
    /// [`Self::Unknown`] -- no policy is installed, and everything we hold
    /// is announced.
    Unbounded,
    /// This many bytes for everything under the torrent-data root.
    Bytes(u64),
}

/// The budget, shared between the `BackendEngineFS` the cleaner tells and
/// the engines that read it.
///
/// A lock and not an atomic because the value is a three-way answer and
/// "unknown" has to survive being read: an atomic would need a sentinel,
/// and a sentinel is how a value a process invents at startup gets read
/// back as an observation.
#[derive(Debug, Default)]
pub struct RetentionBudget(parking_lot::RwLock<CacheBudget>);

impl RetentionBudget {
    pub fn get(&self) -> CacheBudget {
        *self.0.read()
    }

    /// The cleaner's reading, after a pass. `None` is
    /// [`CacheBudget::Unbounded`] -- the shape `CacheLimit::effective`
    /// answers in -- and never [`CacheBudget::Unknown`], which only the
    /// absence of a pass can mean.
    pub fn set(&self, limit: Option<u64>) {
        *self.0.write() = match limit {
            Some(bytes) => CacheBudget::Bytes(bytes),
            None => CacheBudget::Unbounded,
        };
    }
}

/// The retention state of the file most recently streamed on one torrent.
///
/// One per torrent and not one per file: the server has one active file at
/// a time (`BackendEngineFS::active_file`), and the budget is a statement
/// about one volume, so two policies over one volume would each plan to
/// fill it. A stream that moves to another file of the same torrent
/// replaces this, and with it the committed set -- which is what the policy
/// already says about the set's lifetime: sharing runs between sessions and
/// stops when the next stream starts.
pub(crate) struct FileRetention {
    /// The file the policy governs; a playhead reading for any other file
    /// is not this policy's to act on.
    pub file_idx: usize,
    /// Where that file lies in the torrent's pieces, so a reader's offset
    /// becomes a piece index.
    span: FilePieceSpan,
    piece_length: u64,
    /// The budget the policy was built for. A different one is a different
    /// shape, so the policy is rebuilt rather than nudged.
    budget: u64,
    policy: RetentionPolicy,
}

impl FileRetention {
    /// The torrent piece a reader at `offset_in_file` is sitting on.
    fn playhead(&self, offset_in_file: u64) -> u32 {
        playhead_piece(&self.span, self.piece_length, offset_in_file)
    }

    /// The pieces this policy governs.
    pub fn pieces(&self) -> Range<u32> {
        self.span.pieces.clone()
    }

    /// The pieces the policy has committed: what we advertise of this
    /// file, and the whole of what nothing will reclaim.
    pub fn committed(&self) -> &BTreeSet<u32> {
        self.policy.advertised()
    }

    /// How the budget relates to the file -- always a [`Shape::Split`]
    /// here, since a policy is installed only when the budget does not
    /// cover the file.
    pub fn shape(&self) -> Shape {
        self.policy.shape()
    }

    /// What this policy is bounding, as a value that outlives the slot it
    /// lives in. See [`PolicyBounds`], which is where the reading is taken
    /// from.
    pub fn bounds(&self) -> PolicyBounds {
        PolicyBounds {
            file_idx: self.file_idx,
            span: self.span.clone(),
            piece_length: self.piece_length,
            committed: self.committed().len(),
        }
    }
}

/// What one file's policy is bounding, kept beside the policy rather than
/// read off it.
///
/// **Because a pass in flight has the policy out of its slot.** A retention
/// pass ([`crate::engine::Engine::retain`]) takes the [`FileRetention`] for
/// the length of a directory listing and two awaited backend calls, so
/// anything that asked the slot during a pass would be told there is no
/// policy at all -- and "no policy" is not a slower answer, it is a
/// different one: it says nothing is bounding this stream. The proxy cache
/// keeps `LiveStream::bounded` and `LiveStream::windows` beside its own
/// policy for exactly this reason.
///
/// **What it is, and what it is not.** It is a reading, taken at the moment
/// the policy was last written -- installed, or put back by a pass -- and
/// replaced by the next such moment, never accumulated. At process start
/// there is none, which is true: no policy has been installed. The one
/// thing here that a pass can move under it is [`Self::committed`], which
/// that pass may grow by the pieces it advertises; for the length of the
/// pass this therefore reports the committed set as it was when the pass
/// began, and every piece in that set is still committed (the set only
/// grows while a policy stands), so it is a floor and never a claim about
/// pieces that were not promised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PolicyBounds {
    /// The file the policy governs; a playhead reading for any other file
    /// is not this policy's to answer about.
    pub file_idx: usize,
    span: FilePieceSpan,
    piece_length: u64,
    committed: usize,
}

impl PolicyBounds {
    /// What the policy says for a reader at `offset_in_file`.
    ///
    /// A value, so the disk listing the numbers are finished against does
    /// not happen under the lock this was read from. See [`PolicyReading`].
    pub fn reading(&self, offset_in_file: u64) -> PolicyReading {
        PolicyReading {
            pieces: self.span.pieces.clone(),
            piece_length: self.piece_length,
            playhead: playhead_piece(&self.span, self.piece_length, offset_in_file),
            committed: self.committed,
        }
    }
}

/// The torrent piece a reader at `offset_in_file` of the file `span`
/// describes is sitting on.
fn playhead_piece(span: &FilePieceSpan, piece_length: u64, offset_in_file: u64) -> u32 {
    let absolute = span.offset.saturating_add(offset_in_file);
    let piece = absolute / piece_length;
    // The reader cannot be outside its own file, but a clamp is cheaper
    // than trusting arithmetic across a resize, and the policy clamps
    // the same way.
    piece.clamp(
        u64::from(span.pieces.start),
        u64::from(span.pieces.end.saturating_sub(1)),
    ) as u32
}

/// One reading of one file's retention policy: where the playhead is, what
/// range the policy governs, and how much of it is committed.
///
/// A value rather than a borrow of the policy, because the question it
/// exists to answer -- [`Self::window`] -- is finished against a listing of
/// the disk, and that listing must not happen under the lock the policy
/// lives behind.
///
/// **Every number here is an observation and none of them survives the
/// call.** The playhead is where a reader really got to (`None` for a
/// torrent no reader has been inside, which is why this is only ever
/// reached through one), the committed count is what the policy has
/// actually advertised, and nothing is stored: a second reading a moment
/// later is a second measurement, not a memory of this one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReading {
    pieces: Range<u32>,
    piece_length: u64,
    playhead: u32,
    committed: usize,
}

impl PolicyReading {
    /// Bytes the policy has committed: pieces we have advertised and
    /// promised never to reclaim.
    ///
    /// Piece count times piece length, so the last piece of a file counts
    /// whole. The window below is counted the same way, and the two are
    /// meant to be read against each other.
    pub fn committed_bytes(&self) -> u64 {
        (self.committed as u64).saturating_mul(self.piece_length)
    }

    /// What the store holds of this file, split at the playhead.
    ///
    /// `held` is the pieces on the disk now -- the store's own listing,
    /// taken by the caller. **Neither half is a promise**: `ahead` is
    /// read-ahead that has arrived, not read-ahead that is planned, and a
    /// stream that has fetched nothing yet has a window of zero rather than
    /// the extent the policy intends to fill. The piece under the playhead
    /// counts as ahead: it is the one a player is about to read, not one it
    /// has passed.
    ///
    /// Pieces outside this file are not this policy's and are skipped --
    /// `held` is the whole torrent's.
    pub fn window(&self, held: &BTreeSet<u32>) -> CacheWindow {
        let mut window = CacheWindow::default();
        for piece in held.range(self.pieces.clone()) {
            let half = if *piece < self.playhead {
                &mut window.behind_bytes
            } else {
                &mut window.ahead_bytes
            };
            *half = half.saturating_add(self.piece_length);
        }
        window
    }
}

/// What one stream's cache holds around the playhead, in bytes.
///
/// A live reading of a store and nothing else: what is on the disk for the
/// stream being played, split at the byte a player has actually reached.
/// Both stores answer in this shape -- the piece store counting pieces of
/// the file, the proxy cache counting chunks of the entity -- because it is
/// the same question about the same volume.
///
/// It is never anything's stored state. At process start there is no
/// playhead in either store (`enginefs::engine::Engine`'s is `None`, the
/// proxy's map is empty), so there is nothing to read one of these off,
/// which is the honest answer for a process that has watched nothing yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheWindow {
    /// Bytes of the stream we hold behind the playhead: what a scan back
    /// is served from.
    pub behind_bytes: u64,
    /// Bytes we hold from the playhead on: what playback has in hand.
    pub ahead_bytes: u64,
}

/// What one torrent stream's stores say about it right now: the cache
/// around the playhead, the set committed for sharing, and what the torrent
/// has moved this session.
///
/// Every field is measured when it is asked for and none of it is kept.
/// The transfer totals in particular are **this session's** -- librqbit's
/// own per-torrent counters, which start at zero when the torrent is added
/// to this process and are not persisted. A ratio taken from them is a
/// ratio for this run and must be labelled as one; the conventional
/// per-torrent, across-restarts ratio would need counters stored on disk,
/// and a stored counter is a claim about a past this process never saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentStreamNumbers {
    /// What the piece store holds of the file, split at the playhead, or
    /// `None` where there is no policy or no playhead to split at.
    pub window: Option<CacheWindow>,
    /// Bytes advertised and promised never to be reclaimed. Absent with
    /// [`Self::window`] and for the same reasons: with no policy installed
    /// nothing has been promised, whatever is announced.
    pub committed_bytes: Option<u64>,
    /// What this torrent has fetched and sent since it was added, in this
    /// process, or `None` where the backend has no counters to read: a
    /// torrent that is paused, still checking, stopped for space or in
    /// error. **Not zero for those** -- a torrent that has moved gigabytes
    /// and then paused has not moved nothing, and the row a client draws
    /// from a zero says it has. See
    /// [`crate::backend::TorrentHandle::transfer_totals`].
    pub transfer: Option<crate::backend::TransferTotals>,
}

/// Build the policy for a file about to be streamed, or say why there is
/// none.
///
/// `None` is not a failure: it is "nothing here needs bounding", and it is
/// the answer for a budget nobody has set, a budget that covers the file
/// ([`Shape::Whole`] -- the phone with 379 GB free and every desktop), and
/// a torrent with no metadata to name its pieces by.
pub(crate) async fn policy_for<H: TorrentHandle>(
    handle: &H,
    budget: CacheBudget,
    file_idx: usize,
) -> Option<FileRetention> {
    let CacheBudget::Bytes(budget) = budget else {
        return None;
    };
    let span = handle.file_pieces(file_idx).await?;
    let piece_length = handle.piece_length().filter(|length| *length > 0)?;
    // A torrent's half of the budget is committed for sharing: its pieces are
    // what a peer asks us for. That is the whole of what differs from the
    // proxy cache's policy (`server::proxy_retention`), and it is this
    // argument rather than a second policy.
    let policy = match RetentionPolicy::new(
        budget,
        piece_length,
        span.pieces.clone(),
        span.bytes,
        Share::Half,
    ) {
        Ok(policy) => policy,
        Err(error) => {
            tracing::warn!(
                info_hash = %handle.info_hash(),
                file_idx,
                error = %format!("{error:#}"),
                "could not size a retention policy for the file; it is neither bounded nor held back"
            );
            return None;
        }
    };
    if policy.shape() == Shape::Whole {
        // The budget covers the file. Keep all of it, share all of it,
        // reclaim none of it -- and install nothing, because an installed
        // policy is what makes a piece unadvertised and a piece reclaimable,
        // and neither is true here.
        return None;
    }
    Some(FileRetention {
        file_idx,
        span,
        piece_length,
        budget,
        policy,
    })
}

/// Whether an existing policy still describes this file under this budget.
pub(crate) fn still_current(
    retention: &FileRetention,
    budget: CacheBudget,
    file_idx: usize,
) -> bool {
    retention.file_idx == file_idx && CacheBudget::Bytes(retention.budget) == budget
}

/// What one pass of [`advance`] did, for the log and for the tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPass {
    /// Pieces that joined the committed set and are now advertised.
    pub committed: usize,
    /// Pieces the store gave back.
    pub reclaimed: usize,
    /// Pieces we had advertised and no longer hold, so no longer announce.
    pub withdrawn: usize,
}

/// What the store holds of this torrent, off the reactor.
///
/// `held` lists one directory per thousand pieces, and the pass that asks
/// holds `Engine::announce` for the whole of itself -- the lock the
/// stream-open path takes too. Blocking the reactor thread here therefore
/// stalls request handling as well as the pass, and the lock is held across
/// this either way, so nothing about what the pass excludes moves: what
/// leaves the reactor is the waiting, not the exclusion.
///
/// `None` is a pool that will not answer -- shutting down, or the task
/// panicked -- which is a pass that measured nothing: it commits nothing
/// and reclaims nothing rather than acting on a listing it does not have.
///
/// Listing is the caller's and not [`advance`]'s because it is the pass's
/// long suspension, and the playhead the decision is built from has to be
/// read on the far side of it; see `Engine::retain`.
pub(crate) async fn listing(store: &StoreRoot, info_hash: &str) -> Option<BTreeSet<u32>> {
    let store = store.clone();
    let info_hash = info_hash.to_string();
    tokio::task::spawn_blocking(move || store.held(&info_hash))
        .await
        .ok()
}

/// One pass: ask the policy where the playhead has left us, then make its
/// answer true.
///
/// The order is the policy's own: **advertise what is committed before
/// reclaiming**, because the two sets are disjoint and the commit is what
/// takes a piece out of reach of the reclaim. Then reclaim, which is the
/// backend forgetting the pieces and the store unlinking them, under the
/// claim that keeps the two atomic.
///
/// `held` is the caller's listing and `offset_in_file` must be a reading
/// taken after it: a playhead older than the listing names a window the
/// disk has already been filled past, and everything the fill wrote ahead
/// of it is then outside that window and reclaimed. Read the other way
/// round the decision names pieces the listing did not find, and
/// `RetentionPolicy::advance` takes `held` as the candidate set, so those
/// name nothing and unlink nothing.
///
/// `at_the_door` is the pass's last asking, and it is made again before
/// every run is given back rather than once for the decision. `None` is
/// "take nothing more"; `Some` is where the reader is in this file at that
/// instant. See the loop at the foot of this function for what each answer
/// refuses and why neither can be hoisted out of it.
pub(crate) async fn advance<H: TorrentHandle, D: Fn() -> Option<u64>>(
    handle: &H,
    store: &StoreRoot,
    info_hash: &str,
    retention: &mut FileRetention,
    offset_in_file: u64,
    held: &BTreeSet<u32>,
    at_the_door: D,
) -> RetentionPass {
    let playhead = retention.playhead(offset_in_file);
    let decision = retention.policy.advance(playhead, held);

    let mut pass = RetentionPass::default();
    // A committed piece is one nothing will ever reclaim, which is the
    // whole of what makes it safe to announce.
    for run in runs(&decision.committed) {
        if let Err(error) = handle.set_pieces_advertised(run.clone(), true).await {
            tracing::warn!(
                info_hash = %info_hash,
                error = %format!("{error:#}"),
                "could not announce the pieces the window released; they stay ours and unshared"
            );
            break;
        }
        pass.committed += (run.end - run.start) as usize;
    }
    // A committed piece the disk has lost behind our back cannot stay
    // announced: that is the advertise-then-refuse this exists to avoid,
    // wearing the other sign.
    for run in runs(&decision.withdrawn) {
        if handle
            .set_pieces_advertised(run.clone(), false)
            .await
            .is_ok()
        {
            pass.withdrawn += (run.end - run.start) as usize;
        }
    }
    // **The door, asked at the instant of each unlink and not a moment
    // before it.** The decision above was measured before two awaited
    // backend calls per committed and withdrawn run and a `file_wants`,
    // and neither of the two things it measured against is behind a lock
    // this pass holds: `note_playhead` writes the playhead on every
    // delivered byte and `pin_download` writes the pin set, and
    // `Engine::announce` stops neither.
    //
    // What it is *not* for: a piece becoming announced under the pass.
    // Every `set_pieces_advertised` in this workspace is made under
    // `announce`, which the pass holds throughout, so nothing can announce
    // a piece between the decision and the unlink -- that half of the
    // proxy's door is kept here by the lock, and a door built from
    // `Engine::gate_verdict_for_file` would in any case answer
    // `TorrentGate::Announced` (the policy is in this pass's hand, not in
    // the slot) and reclaim nothing, ever.
    //
    // `None` is a pin taken since `Engine::retain` asked, or a reader that
    // has left this file, and it stops the reclaim rather than skipping a
    // run: a pin does not un-pin mid-loop, and there is no window for a
    // file nobody is reading. A pinned file's pieces are the expensive
    // ones to get wrong -- `AfterRelease::LeaveDropped` leaves them
    // neither held nor wanted, and the pin's own reconcile short-circuits
    // an unchanged selection, so nothing re-queues them and the download
    // the user asked for stays short of them until a restart hash-checks
    // the file off the disk.
    //
    // `Some` narrows the run by the window at the reader's *current*
    // position. librqbit refuses a piece its own live stream is about to
    // read, and computes that refusal under the write lock it drops
    // beneath rather than before it -- but that is `queue_range`, the
    // forward lookahead alone (`MAX_STARTUP_WINDOW_BYTES`, 4 MiB), so it
    // cannot see the tenth of the window that sits behind the playhead for
    // a scan back, it is empty whenever no stream is open, and
    // `TorrentHandle::drop_pieces` promises it of no backend.
    //
    // Per run and not per piece: `release` is the unit that holds the
    // claim across the unlink, and `runs` exists so that two hundred
    // consecutive pieces are one call and not two hundred locks on the
    // torrent.
    //
    // And asked again before every *part* of a run, not once per run. The
    // window at the door can fall inside a run and cut it in two, and the
    // second part is then given back only after the first has been
    // released -- a `drop_pieces` and an unlink batch later, "a syscall
    // loop of no bounded length" on the flash of a television. Handing the
    // second part to `release` on the same answer is the reading the door
    // exists to refuse, one level down: a pin taken during the first
    // part's unlink would have the second part's pieces dropped and
    // unlinked out of a download the user has just asked to keep, and a
    // reader that walked on during it would lose the window in front of
    // it. So a run the window has narrowed or split goes back on the list
    // in its parts, and each part is asked about in its own turn; only a
    // run the door lets through whole is released.
    let mut pending: std::collections::VecDeque<Range<u32>> =
        runs(&this_files_alone(handle, retention.file_idx, &decision.reclaim).await).into();
    while let Some(run) = pending.pop_front() {
        let Some(offset) = at_the_door() else {
            break;
        };
        let window = retention.policy.window_at(retention.playhead(offset));
        let parts = outside(run.clone(), &window);
        if parts.len() == 1 && parts[0] == run {
            pass.reclaimed += release(handle, store, info_hash, run).await;
        } else {
            // Strictly fewer pieces than `run`, so this converges: a part
            // is released or shrinks again on every turn through the loop.
            for part in parts.into_iter().rev() {
                pending.push_front(part);
            }
        }
    }
    pass
}

/// The pieces of `reclaim` no other file the torrent still wants has a byte
/// in.
///
/// **A file's first and last piece are not only its own.** A piece is a
/// fixed length across the whole torrent, so a file that does not begin and
/// end on a piece boundary -- every file of a torrent without BEP-47
/// padding -- shares those two with its neighbours. The policy governs one
/// file and its reclaim set includes both of them, and nothing below here
/// refuses: librqbit's `ChunkTracker::drop_piece` consults the want-set
/// only for a piece we do *not* have, so a piece we have is dropped
/// whoever else wants it, and [`take_claimed`] then unlinks the bytes.
/// The neighbour, still selected, fetches the piece again; the next pass
/// finds it held, inside the policy's range and outside the window, and
/// reclaims it again -- a refetch loop at the boundary for as long as the
/// neighbour is wanted. Nothing is advertised over a hole while that runs
/// (the have-bit goes before the unlink), so what it costs is the
/// neighbour's bytes and the swarm's, over and over.
///
/// So the reclaim is narrowed here, by the rule
/// [`crate::piece_store::PieceStore::remove_file`] deletes a boundary piece
/// by: a piece another wanted file owns bytes in stays.
///
/// Both paths that unlink a policy's pieces come through here: [`advance`],
/// which is the pass reclaiming under its own reader, and
/// [`crate::engine::Engine::release_reclaimable`], which is the cache
/// cleaner's delete. The gate the cleaner narrows by cannot answer this
/// question -- it speaks for the torrent, and a shared piece is in range
/// and uncommitted like any other -- so leaving that path out would leave
/// the loop reachable from the cleaner alone.
pub(crate) async fn this_files_alone<H: TorrentHandle>(
    handle: &H,
    file_idx: usize,
    reclaim: &[u32],
) -> Vec<u32> {
    if reclaim.is_empty() {
        return Vec::new();
    }
    let Some(wants) = handle.file_wants().await else {
        // A backend with no file table has no boundary to name.
        return reclaim.to_vec();
    };
    let kept: Vec<u32> = reclaim
        .iter()
        .copied()
        .filter(|piece| !wants.shared_with_another(*piece, file_idx))
        .collect();
    if kept.len() != reclaim.len() {
        tracing::debug!(
            file_idx,
            shared = reclaim.len() - kept.len(),
            "leaving the boundary pieces another file of this torrent still wants"
        );
    }
    kept
}

/// Give a run of pieces back: the backend forgets them, the store unlinks
/// what it forgot, and only then is the claim released.
///
/// Pieces librqbit refuses to drop -- a peer is mid-flight on them, or a
/// live stream is about to read them -- are simply not in the claim, so
/// they are not deleted either. That is the interlock doing its second job:
/// it is also what keeps a reclaim from racing the read-ahead it would
/// otherwise delete out from under.
pub(crate) async fn release<H: TorrentHandle>(
    handle: &H,
    store: &StoreRoot,
    info_hash: &str,
    pieces: Range<u32>,
) -> usize {
    match handle
        .drop_pieces(pieces.clone(), AfterRelease::LeaveDropped)
        .await
    {
        Ok(Some(dropped)) => take_claimed(store, info_hash, dropped).await,
        // A backend with no have-set of its own for the deletion to
        // disagree with: there is nothing to interlock against, so there is
        // no claim to hold -- and the same unlink, done the same way.
        Ok(None) => unlink(store, info_hash, pieces.collect(), None).await,
        // It still believes it has them, so they are not ours to take:
        // unlinking here is exactly the advertise-then-serve-a-hole this
        // whole path exists to prevent.
        Err(error) => {
            tracing::warn!(
                info_hash = %info_hash,
                first = pieces.start,
                end = pieces.end,
                error = %format!("{error:#}"),
                "the backend would not forget the pieces, so their bytes stay on the disk"
            );
            0
        }
    }
}

/// **The one place a piece file is unlinked while something might still
/// believe the torrent has it.**
///
/// The bytes are one record of a piece and the backend's have-set is the
/// other. Unlink behind the backend's back and it goes on reporting the
/// piece complete, advertising it, and answering a peer's request with a
/// read past the end of nothing. So the have-set is edited first
/// ([`crate::backend::TorrentHandle::drop_pieces`], which hands back the
/// claim), the bytes go while the claim stands -- nothing can download one
/// of them back into the range being deleted -- and the claim is released
/// last.
///
/// Every reclaim in this server ends here: the retention policy's, through
/// [`release`], and the per-file delete an unpin does, through
/// `BackendEngineFS::delete_download_data`.
///
/// Returns how many pieces really left the disk -- either copy of one
/// counts, since either occupies blocks the volume gets back. A piece with
/// no file was not there to leave it, and is not an error; nor is one the
/// volume refuses to unlink, which is logged and stepped over rather than
/// abandoning the rest of the run: every piece in it has had its have-bit
/// cleared already, and the claim that would let a caller retry is released
/// on the way out of here.
pub(crate) async fn take_claimed(
    store: &StoreRoot,
    info_hash: &str,
    dropped: crate::backend::DroppedFilePieces,
) -> usize {
    let pieces = dropped.pieces().to_vec();
    unlink(store, info_hash, pieces, Some(dropped)).await
}

/// The unlink itself, off the reactor, with the claim -- where there is one
/// -- held across it and released on the far side.
///
/// **Off the reactor** because this is one `unlink` per piece on the flash
/// of a television, which is a syscall loop of no bounded length: run on
/// the reactor thread it stalls every other task that thread is carrying,
/// whichever door called. The retention pass compounds that -- it holds
/// `Engine::announce` across the wait, and the stream-open path takes the
/// same lock, so a pass on the reactor stops request handling for as long
/// as the volume takes to answer. The unpin's delete holds no such lock:
/// `BackendEngineFS::delete_download_data` never touches `announce`, and
/// what orders *it* against a re-download is the claim below and nothing
/// else. This is not a function under which `announce` is always held.
///
/// **The claim travels with the work rather than staying behind.** It is
/// what keeps the unlink ordered against the have-set, so it has to outlive
/// the deletion and not merely the call that started it; moved here it is
/// released on the blocking thread once the bytes are gone, which is the
/// order [`take_claimed`] exists to impose. A caller that stops awaiting
/// does not disturb that: a blocking task already started is not cancelled,
/// so the deletion finishes and the claim goes with it.
///
/// Both doors come through here so that the move off the reactor is written
/// once. The claimed one is the door every reclaim of a live torrent takes
/// and is what the tests exercise; the claimless one is for a backend that
/// keeps no have-set, which nothing in this workspace is.
async fn unlink(
    store: &StoreRoot,
    info_hash: &str,
    pieces: Vec<u32>,
    claim: Option<crate::backend::DroppedFilePieces>,
) -> usize {
    let store = store.clone();
    let hash = info_hash.to_string();
    tokio::task::spawn_blocking(move || {
        let freed = store.delete_pieces(&hash, pieces);
        // Released only now that the bytes are gone.
        drop(claim);
        freed
    })
    .await
    // The pool would not answer, so this process cannot say what left the
    // disk. Zero is the answer that claims nothing: `ENOSPC` recovery reads
    // this number to decide whether a pass made room, and a number invented
    // here restarts torrents onto a disk that may have gained nothing.
    .unwrap_or(0)
}

/// The parts of `run` that `window` does not cover: at most the pieces
/// before it and the pieces after it.
///
/// A window that falls in the middle of a run makes it two calls on the
/// torrent instead of one, which is the whole price of narrowing a run at
/// the door; a window that does not touch it leaves the single call it
/// was.
fn outside(run: Range<u32>, window: &Range<u32>) -> Vec<Range<u32>> {
    let mut kept = Vec::new();
    if run.start < window.start {
        kept.push(run.start..run.end.min(window.start));
    }
    if run.end > window.end {
        kept.push(run.start.max(window.end)..run.end);
    }
    kept
}

/// Scattered piece indices as the fewest contiguous ranges that cover them.
///
/// The backend takes a range at a time -- both the hold-back set and the
/// want-set do -- and a reclaim of two hundred consecutive pieces should be
/// one call and not two hundred locks on the torrent.
pub(crate) fn runs(pieces: &[u32]) -> Vec<Range<u32>> {
    let mut sorted: Vec<u32> = pieces.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut runs: Vec<Range<u32>> = Vec::new();
    for piece in sorted {
        match runs.last_mut() {
            Some(run) if run.end == piece => run.end = piece + 1,
            _ => runs.push(piece..piece + 1),
        }
    }
    runs
}

/// What the retention policy will let the cache cleaner take, as one
/// reading of every engine.
///
/// **The whole of what the cleaner is allowed to know.** It used to be
/// handed three lists -- protected, dead, stopped-for-space -- and to
/// decide from them which of the store's pieces it might unlink itself.
/// Those lists were three spellings of one question, and the policy is what
/// answers it: *is this piece one we have told a peer about?* A piece we
/// announce may not be taken, whoever holds it and whatever stopped it; a
/// piece we announce to nobody is cache like any other.
#[derive(Debug, Default, Clone)]
pub struct ReclaimGate {
    torrents: std::collections::HashMap<String, TorrentGate>,
    /// The windows live readers hold in the chunk stores the cleaner walks
    /// **as files** -- which is the proxy cache and nothing else, since the
    /// piece store answers by info hash and index instead.
    ///
    /// This is the same question the torrent half asks, put where the
    /// proxy's answer can come from. On a torrent, a piece a live stream is
    /// about to read is refused by librqbit itself: the cleaner's delete
    /// goes through `TorrentHandle::drop_pieces`, which will not forget a
    /// piece a reader is waiting on, so the reader's own bytes cannot be
    /// unlinked out from under it. A proxied stream has no such backend to
    /// refuse -- the cleaner holds the path and `remove_file` takes it --
    /// and the reader finds out by failing (`proxy_cache::Cached::body`
    /// ends the body in an error rather than serving a hole). So the refusal
    /// has to be here, and this is it: one gate, asked about everything the
    /// cleaner walks.
    ///
    /// Two different claims are inserted here and the difference matters to
    /// nothing but the caller: a *window* is where a playhead is and what
    /// playback is about to want, and a *promise* is the chunks an open
    /// body was framed to deliver and has not yet -- which is the other
    /// half of what librqbit's refusal gives a torrent for free. Both are
    /// "somebody is inside these bytes", so both are a range of one chunk
    /// store's directory and both are read the same way.
    windows: Vec<ReaderWindow>,
}

/// One live reader's window in a chunk store the cleaner walks by path.
#[derive(Debug, Clone)]
struct ReaderWindow {
    /// The directory the chunks are bucketed under.
    dir: std::path::PathBuf,
    /// The chunk indices the window covers.
    chunks: Range<u64>,
}

/// One torrent's answer, in the three shapes it comes in.
///
/// Asked in two places and computed in one ([`crate::engine::Engine::gate_verdict`]):
/// the cleaner's walk collects these into a [`ReclaimGate`], and the delete
/// the cleaner then asks for re-asks the same question of the same engine
/// before it unlinks anything. The walk's copy is a reading taken minutes
/// ago; the second asking is the one that decides.
#[derive(Debug, Clone)]
pub enum TorrentGate {
    /// Everything it holds is announced, so nothing may be taken. A torrent
    /// with no retention policy, and every pinned one -- a pin is a
    /// retention property, and the user asked for those bytes.
    Announced,
    /// It announces nothing at all: the backend stopped it with an error,
    /// so there is no live have-set for a deletion to disagree with, and
    /// the next start rebuilds it by asking the storage. Every piece may
    /// go, and before any other cache: nothing will ever read these bytes.
    Nothing,
    /// A policy governs `pieces` of it. Inside that range only what the
    /// policy has committed is announced; outside it the torrent still
    /// announces everything it has.
    Policy {
        pieces: Range<u32>,
        committed: BTreeSet<u32>,
    },
}

impl TorrentGate {
    /// Whether this torrent will give the piece up.
    pub fn releases(&self, piece: u32) -> bool {
        match self {
            Self::Nothing => true,
            Self::Announced => false,
            Self::Policy { pieces, committed } => {
                pieces.contains(&piece) && !committed.contains(&piece)
            }
        }
    }

    /// Whether this torrent's pieces sort to the front of the size rule:
    /// bytes nothing will ever read or resume into.
    pub fn goes_first(&self) -> bool {
        matches!(self, Self::Nothing)
    }
}

impl ReclaimGate {
    /// Record one torrent's answer.
    pub fn insert(&mut self, info_hash: String, gate: TorrentGate) {
        self.torrents.insert(info_hash, gate);
    }

    /// This torrent announces everything it holds: nothing of it may go.
    pub fn insert_announced(&mut self, info_hash: String) {
        self.insert(info_hash, TorrentGate::Announced);
    }

    /// This torrent announces nothing at all, and its bytes go first.
    pub fn insert_dead(&mut self, info_hash: String) {
        self.insert(info_hash, TorrentGate::Nothing);
    }

    /// A policy governs `pieces` of this torrent and has committed
    /// `committed` of them; the rest of that range may go, and everything
    /// outside it is still announced.
    pub fn insert_policy(
        &mut self,
        info_hash: String,
        pieces: Range<u32>,
        committed: BTreeSet<u32>,
    ) {
        self.insert(info_hash, TorrentGate::Policy { pieces, committed });
    }

    /// Whether the policy will give this piece up.
    ///
    /// A torrent the gate has never heard of is cache nobody speaks for --
    /// a previous install's, or one the idle sweep has already taken out of
    /// the session -- and it goes.
    pub fn releases(&self, info_hash: &str, piece: u32) -> bool {
        self.torrents
            .get(info_hash)
            .is_none_or(|gate| gate.releases(piece))
    }

    /// Whether this torrent's pieces sort to the front of the size rule:
    /// bytes nothing will ever read or resume into.
    pub fn goes_first(&self, info_hash: &str) -> bool {
        self.torrents
            .get(info_hash)
            .is_some_and(TorrentGate::goes_first)
    }

    /// A live reader holds `chunks` of the chunk store bucketed under `dir`.
    ///
    /// `dir` is a chunk store's directory, so the files under it are
    /// `<dir>/<index / CHUNKS_PER_DIRECTORY>/<index>` -- which is the whole
    /// of what [`Self::releases_file`] needs to read an index back off a
    /// path.
    pub fn insert_window(&mut self, dir: std::path::PathBuf, chunks: Range<u64>) {
        self.windows.push(ReaderWindow { dir, chunks });
    }

    /// Whether a file the cleaner walked may be taken.
    ///
    /// Everything the walk finds is cache with nobody to speak for it --
    /// **except** a chunk inside a window some reader is playing through
    /// right now. That one is the bytes under the player's head, or the
    /// scan-back and the read-ahead either side of it; unlinking it costs
    /// the player a broken read and costs the origin the same fetch again,
    /// which is the two things a cache is for.
    ///
    /// A path with no window over it releases, which is the answer for every
    /// byte in the root when nothing is playing.
    pub fn releases_file(&self, path: &std::path::Path) -> bool {
        if self.windows.is_empty() {
            return true;
        }
        let Some(index) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(crate::chunk_store::canonical_index)
        else {
            // Not a name a chunk store writes, so no window can be about it.
            return true;
        };
        // `<dir>/<bucket>/<index>`: the bucket is not checked against the
        // index, because a file under the wrong bucket is unreachable debris
        // rather than a chunk, and debris is not what a window is holding.
        let Some(dir) = path.parent().and_then(std::path::Path::parent) else {
            return true;
        };
        // Matched on the entity's directory *name*, not on the whole path.
        // Both sides are built from this server's own root, so they name the
        // same directory -- but not necessarily with the same spelling: on
        // Windows one may carry the 8.3 short name (`RUNNER~1`) where the
        // other has the long one, and on macOS `/var` is a symlink to
        // `/private/var`. `PathBuf` equality is a string comparison, so it
        // answered "no window is about this" for every chunk, and the
        // cleaner unlinked the bytes a player was reading.
        //
        // The name alone is enough to be sure: it is generated by the store
        // from the entity, there is one torrent-data root, and the walk that
        // produced `path` is a walk of it. And the two ways this can be wrong
        // are not the same size -- a name that matched when it should not
        // leaves a few stale bytes on the disk, while one that failed to
        // match takes bytes out from under a live reader -- so where it has
        // to lean, it leans towards keeping.
        let Some(name) = dir.file_name() else {
            return true;
        };
        !self
            .windows
            .iter()
            .any(|window| window.dir.file_name() == Some(name) && window.chunks.contains(&index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_are_the_fewest_ranges_that_cover_the_pieces() {
        assert_eq!(runs(&[]), Vec::<Range<u32>>::new());
        assert_eq!(runs(&[7]), vec![7..8]);
        assert_eq!(runs(&[3, 1, 2]), vec![1..4]);
        assert_eq!(runs(&[1, 2, 4, 5, 9]), vec![1..3, 4..6, 9..10]);
        // A caller that offered the same piece twice gets one range, not a
        // second call on the torrent for a piece already in the first.
        assert_eq!(runs(&[5, 5, 6]), vec![5..7]);
    }

    /// The door narrows a run rather than refusing it whole: what the
    /// reader has moved onto since the decision stays, and what it has left
    /// behind still goes.
    #[test]
    fn a_run_the_window_has_moved_into_is_kept_and_the_rest_of_it_goes() {
        assert_eq!(outside(2..6, &(0..1)), vec![2..6], "no overlap, one call");
        assert_eq!(outside(2..6, &(9..10)), vec![2..6]);
        assert_eq!(outside(2..6, &(4..5)), vec![2..4, 5..6], "split in two");
        assert_eq!(outside(2..6, &(0..4)), vec![4..6], "the front is spared");
        assert_eq!(outside(2..6, &(4..9)), vec![2..4], "and the tail can be");
        assert_eq!(
            outside(2..6, &(2..6)),
            Vec::<Range<u32>>::new(),
            "a run the window has covered entirely is not the pass's to take"
        );
    }

    /// The gate's default answer is what decides whether an unknown torrent
    /// is cache or is protected, and both mistakes are expensive: refusing
    /// is a disk that fills with a previous install's leftovers, releasing
    /// a live torrent's pieces is the advertise-then-refuse the whole
    /// design is about.
    #[test]
    fn a_torrent_the_gate_never_heard_of_is_cache_and_an_announced_one_is_not() {
        let mut gate = ReclaimGate::default();
        gate.insert_announced("aa".into());
        gate.insert_dead("bb".into());
        gate.insert_policy("cc".into(), 10..20, [12, 13].into_iter().collect());

        assert!(gate.releases("nobody", 0), "cache with no owner goes");
        assert!(!gate.releases("aa", 0), "what we announce stays");
        assert!(gate.releases("bb", 0), "a dead torrent announces nothing");
        assert!(gate.goes_first("bb"));
        assert!(!gate.goes_first("aa"));

        assert!(gate.releases("cc", 10), "in the range and uncommitted");
        assert!(!gate.releases("cc", 12), "committed, so announced");
        assert!(
            !gate.releases("cc", 25),
            "outside the policy's file, so still announced whole"
        );
    }

    /// The same gate, asked about a file the cleaner walked rather than a
    /// piece the store reported. The proxy cache is the one chunk store the
    /// cleaner unlinks from itself, so the refusal a live reader needs has
    /// to be expressible about a path.
    #[test]
    fn a_chunk_under_a_live_readers_window_is_not_the_cleaners_to_take() {
        let entity = std::path::PathBuf::from("/cache/.proxy/key/entity");
        let mut gate = ReclaimGate::default();
        assert!(
            gate.releases_file(&entity.join("0/7")),
            "nothing is playing, so every byte in the root is ordinary cache"
        );

        gate.insert_window(entity.clone(), 5..12);
        assert!(
            !gate.releases_file(&entity.join("0/5")),
            "the window's first"
        );
        assert!(!gate.releases_file(&entity.join("0/11")), "and its last");
        assert!(
            gate.releases_file(&entity.join("0/4")),
            "a chunk the window has moved past"
        );
        assert!(
            gate.releases_file(&entity.join("0/12")),
            "and one it has not reached"
        );

        // Another entity's chunk 7 is another entity's, whatever this one is
        // playing: the window is over one directory, not over an index.
        let other = std::path::PathBuf::from("/cache/.proxy/key/other");
        assert!(gate.releases_file(&other.join("0/7")));

        // And a name no chunk store writes is not a chunk to hold on to.
        assert!(gate.releases_file(&entity.join("0/7.tmp")));
        assert!(gate.releases_file(&entity.join("0/007")));
    }
}
