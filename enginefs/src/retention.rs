//! Bounding the streaming cache, and telling the truth about what we hold.
//!
//! [`crate::piece_store::policy`] is the arithmetic -- a budget, a piece
//! length, the pieces of one file, where the playhead is, and out of it a
//! window to keep, a set to share and a set to give back. [`owner`] is the
//! owner: it keeps one policy per file, resident and never taken out, and
//! runs the pass that feeds it the playhead a reader actually reached. This
//! module is the torrent's half of the wiring under both -- the budget cell
//! the server publishes into, the bell that says the volume is running low,
//! and the three calls that make a policy's answers true --
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
//! So while a file is being played, the only pieces of it a peer is told
//! about are ones nothing will reclaim while it is -- what
//! [`crate::piece_store::policy::RetentionPolicy::advertised`] says, and no
//! more: once nobody plays the file it is slack, and the pass takes the
//! committed half too. It holds the whole file back from what we announce
//! before it unlinks anything, so a piece is never deleted while announced.
//! That was a decision the policy could state and not perform until the
//! fork gained a have-bit that is not an announcement; it performs it now.
//!
//! # What decides the budget, and what a missing one means
//!
//! The budget is the server's -- `cache_budget::cap_to_publish`, the smaller
//! of what the operator configured and what the volume can still give,
//! against what the owners of the cache say they hold. It is pushed in
//! ([`RetentionBudget::set`]) and never recomputed here: a second reading of
//! "how much room is there" that disagreed with the published one would have
//! the two halves of the cache sized against different numbers.
//!
//! **At process start it is [`CacheBudget::Unknown`], and that is true rather
//! than a placeholder**: nothing has told this process a budget yet. It is
//! not zero, which would say the cache may hold nothing, and it is not a
//! number this process invented from a disk reading of its own, which would
//! be a claim about a cap nobody set. Unknown means no policy is installed,
//! so a stream that starts before the first publication holds what it holds
//! and announces all of it -- exactly what this server did before any of
//! this existed. `cache_budget::start` states the cap before the router
//! serves its first request, so nothing but a test ever sees the gap.

use std::ops::Range;
use std::sync::Arc;

use crate::backend::{AfterRelease, FilePieceSpan, TorrentHandle};
use crate::piece_store::{DeleteOutcome, HeldSnapshot, StoreRegistry};

pub mod live;
pub mod owner;
pub mod trace;

/// What the server says the torrent-data volume may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheBudget {
    /// Nothing has told this process a budget. **An absence, not a
    /// number**: nothing has published one yet, so there is nothing to be
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

/// The budget, shared between the `BackendEngineFS` the server publishes
/// into and the engines that read it.
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

    /// The publisher's reading. `None` is [`CacheBudget::Unbounded`] --
    /// the shape `CacheLimit::effective` answers in -- and never
    /// [`CacheBudget::Unknown`], which only the absence of a publication
    /// can mean.
    pub fn set(&self, limit: Option<u64>) {
        *self.0.write() = match limit {
            Some(bytes) => CacheBudget::Bytes(bytes),
            None => CacheBudget::Unbounded,
        };
    }
}

/// The bell the volume rings when it is running low.
///
/// One reading of one device, so one bell: the reconciler's tick takes the
/// reading every owner acts on ([`crate::reconcile::Volumes`]) and rings
/// this whenever what it read is under the line a stopped torrent has to
/// see cleared. That reading is the tick's own and not any torrent's --
/// the tick takes it before it looks at what there is to decide about, so
/// a session holding nothing but proxied chunks still rings. Everybody who
/// can give bytes back answers by dropping
/// their slack -- everything nobody is playing and nobody is reading --
/// and nobody chooses a victim: the answer to a volume running low is to
/// stop holding what is already disposable, not to pick something to take.
///
/// **The torrent side has no listener and needs none**: its slack passes
/// ride the same tick that takes the reading. What this is for is the
/// proxy cache, which has no tick of its own -- its entities are only ever
/// ended by a viewer opening something else, and a volume filling under a
/// paused player is the one case where nothing opens.
///
/// One permit and not a counter, like every other signal here: a ring
/// while nobody is waiting is remembered once, because the pass the waiter
/// runs covers every slack entity there is and running it twice for two
/// rings would find nothing the second time.
#[derive(Debug, Default)]
pub struct SlackBell(tokio::sync::Notify);

impl SlackBell {
    /// The volume is short: whoever holds slack should give it back now.
    pub fn ring(&self) {
        self.0.notify_one();
    }

    /// Completes at the next ring, or at once if one was rung since the
    /// last time this completed.
    pub async fn rung(&self) {
        self.0.notified().await
    }
}

/// The torrent piece a reader at `offset_in_file` of the file `span`
/// describes is sitting on.
pub(crate) fn playhead_piece(span: &FilePieceSpan, piece_length: u64, offset_in_file: u64) -> u32 {
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
/// exists to answer -- [`Self::window`] -- is finished against the store's
/// held set, read outside the lock the policy lives behind.
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
    /// One reading, off the owner's copy-out of a file's holding: the
    /// pieces its policy governs, the piece length, the piece under the
    /// playhead and how many pieces are committed.
    pub(crate) fn new(
        pieces: Range<u32>,
        piece_length: u64,
        playhead: u32,
        committed: usize,
    ) -> Self {
        Self {
            pieces,
            piece_length,
            playhead,
            committed,
        }
    }

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
    /// `held` is the pieces on the disk now -- the store's own held set,
    /// read by the caller. **Neither half is a promise**: `ahead` is
    /// read-ahead that has arrived, not read-ahead that is planned, and a
    /// stream that has fetched nothing yet has a window of zero rather than
    /// the extent the policy intends to fill. The piece under the playhead
    /// counts as ahead: it is the one a player is about to read, not one it
    /// has passed.
    ///
    /// Pieces outside this file are not this policy's and are skipped --
    /// `held` is the whole torrent's.
    pub fn window(&self, held: &HeldSnapshot) -> CacheWindow {
        let mut window = CacheWindow::default();
        for piece in held.in_range(self.pieces.clone()) {
            let half = if piece < self.playhead {
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
    /// How many pieces this engine's retention passes have asked the
    /// backend to forget and been refused, summed over the engine's life.
    ///
    /// **Zero is the only healthy value.** A refusal is librqbit keeping a
    /// piece inside an open stream's lookahead that this policy's window no
    /// longer covers: the disk cannot come back under budget while that
    /// stream lives, and every tick spends a `drop_pieces` to be refused
    /// again. It is not `Option`, because "no refusals" and "nothing to
    /// read" are the same fact here -- the counter is the engine's own and
    /// exists from its construction.
    pub refused_reclaims: usize,
}

/// What one retention pass did, for the log and for the tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPass {
    /// Pieces that joined the committed set and are now advertised.
    pub committed: usize,
    /// Pieces the store gave back.
    pub reclaimed: usize,
    /// Pieces we had advertised and no longer hold, so no longer announce.
    pub withdrawn: usize,
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
/// [`PieceStore`](crate::piece_store::PieceStore)'s
/// `TorrentStorage::remove_file` deletes a boundary piece by: a piece
/// another wanted file owns bytes in stays.
///
/// Every unlink of a policy's pieces comes through here: the pass asks it
/// ([`owner::Backing::alone`] for the torrent, reclaiming under its own
/// reader) whichever mode the pass is in, so the boundary rule is one rule
/// and not one per caller.
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
    store: &Arc<StoreRegistry>,
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
/// cleared already. It keeps its bit in the held set, so the next pass
/// offers it again, and librqbit hands a dropped piece to a new claim once
/// this one is released: that is the retry.
pub(crate) async fn take_claimed(
    store: &Arc<StoreRegistry>,
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
/// whichever door called. The retention pass compounds that -- it holds the
/// file's turn across the wait, and the stream-open path takes the same
/// turn, so a pass on the reactor stops request handling for as long as the
/// volume takes to answer. The unpin's delete holds no such turn:
/// `BackendEngineFS::delete_download_data` never touches the owner, and
/// what orders *it* against a re-download is the claim below and nothing
/// else. This is not a function under which a turn is always held.
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
/// once. The claimed one is the door every reclaim takes; the claimless one
/// is for a backend that keeps no have-set for a deletion to disagree with,
/// which nothing in this workspace is.
///
/// **Through the registered store, and there is no other way in now.** The
/// live store of a running torrent keeps the held set the pass reads and a
/// cache of open handles, and an unlink by path behind its back left both
/// wrong: a bit over a file that has gone, and an unlinked inode kept alive
/// by a cached descriptor until another piece took its slot. The path door
/// that did that was the cache cleaner's, which held a `&StoreRoot` and
/// deleted by name; with the cleaner gone, a hash no store is registered
/// for frees nothing here and says so. Every caller holds a claim from a
/// live torrent, so "no store" is a torrent whose storage went between the
/// drop and this call, and its directory is the next launch's sweep.
///
/// The registry refuses outright while a store is under its initial hash
/// check -- asked here, at the unlink, and not from a state read earlier: a
/// piece taken from under the check is a have-bit over nothing. Refused
/// pieces stay on the disk and in the held set, and the next pass offers
/// them again.
///
/// **The claimless door never goes through a registered store.** Its caller
/// read the torrent as one with no have-set, and that reading is older than
/// this unlink by a `spawn_blocking` at least. A store registered now is a
/// torrent librqbit holds now, with a have-set over these pieces that
/// nobody has edited, so the pieces are not this door's to take and it
/// takes nothing.
pub(crate) async fn unlink(
    store: &Arc<StoreRegistry>,
    info_hash: &str,
    pieces: Vec<u32>,
    claim: Option<crate::backend::DroppedFilePieces>,
) -> usize {
    let store = Arc::clone(store);
    let hash = info_hash.to_string();
    tokio::task::spawn_blocking(move || {
        let freed = if claim.is_none() && store.is_registered(&hash) {
            tracing::warn!(
                info_hash = %hash,
                pieces = pieces.len(),
                "a store is registered for a torrent read as holding none; not deleting without a claim"
            );
            0
        } else {
            match store.delete(&hash, &pieces) {
                DeleteOutcome::Registered { unlinked } => unlinked,
                DeleteOutcome::Refused => {
                    tracing::warn!(
                        info_hash = %hash,
                        pieces = pieces.len(),
                        "not deleting under a running hash check; the pieces stay held"
                    );
                    0
                }
                DeleteOutcome::Unregistered => {
                    tracing::warn!(
                        info_hash = %hash,
                        pieces = pieces.len(),
                        "no store is registered for a torrent whose pieces were just dropped; \
                         leaving its files to the next launch's sweep"
                    );
                    0
                }
            }
        };
        // Released only now that the bytes are gone.
        drop(claim);
        freed
    })
    .await
    // The pool would not answer, so this process cannot say what left the
    // disk. Zero is the answer that claims nothing.
    .unwrap_or(0)
}

/// The parts of `run` that `window` does not cover: at most the pieces
/// before it and the pieces after it.
///
/// A window that falls in the middle of a run makes it two calls on the
/// torrent instead of one, which is the whole price of narrowing a run at
/// the door; a window that does not touch it leaves the single call it
/// was.
pub(crate) fn outside(run: Range<u32>, window: &Range<u32>) -> Vec<Range<u32>> {
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

    /// **The claimless door never goes through a registered store.**
    ///
    /// Its caller read the torrent as one with no have-set for the deletion
    /// to disagree with, and that reading is older than the unlink by a
    /// `spawn_blocking` at least. A store registered by the time the pool
    /// picks the work up is a torrent librqbit holds *now*, with a have-set
    /// over these pieces that nobody has edited and, if `init` is still
    /// running, a check reading them: the pieces are not this door's to
    /// take, so it takes none of them and says so. Only the claimed door
    /// may edit a registered store's files.
    #[tokio::test]
    async fn a_claimless_unlink_takes_nothing_from_a_registered_store() {
        use crate::piece_store::layout::{FileSpec, PieceLayout};
        use crate::piece_store::{PieceStore, StoreRoot};
        use librqbit::storage::TorrentStorage;

        const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
        let tmp = tempfile::tempdir().unwrap();
        let registry = Arc::new(StoreRegistry::new(StoreRoot::new(
            tmp.path().join(".pieces"),
        )));
        let layout = Arc::new(PieceLayout::new(8, 24, [FileSpec::payload(24)]).expect("layout"));
        let store = PieceStore::under(Arc::clone(&registry), HASH, Arc::clone(&layout));
        std::fs::create_dir_all(store.dir()).unwrap();
        store.pwrite_all(0, 8, &[7u8; 8]).expect("write");
        store.complete_piece(1).expect("complete");
        store.init_for_tests().expect("seed and register");
        let piece = store.piece_path(1);
        assert!(piece.is_file() && registry.is_registered(HASH));

        assert_eq!(
            unlink(&registry, HASH, vec![1], None).await,
            0,
            "a store is registered, so the claimless door frees nothing"
        );
        assert!(piece.is_file(), "and the file is still there");
        assert!(
            registry
                .held(HASH)
                .expect("registered")
                .in_range(0..3)
                .contains(&1),
            "with the bit that says so"
        );

        // The storage really gone -- what a torrent in Error is once
        // librqbit's handles have dropped -- and the door still frees
        // nothing, because there is no store left to unlink through: the
        // directory is the next launch's sweep.
        drop(store);
        assert!(!registry.is_registered(HASH));
        assert_eq!(unlink(&registry, HASH, vec![1], None).await, 0);
        assert!(piece.is_file());
    }
}
