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
        let absolute = self.span.offset.saturating_add(offset_in_file);
        let piece = absolute / self.piece_length;
        // The reader cannot be outside its own file, but a clamp is cheaper
        // than trusting arithmetic across a resize, and the policy clamps
        // the same way.
        piece.clamp(
            u64::from(self.span.pieces.start),
            u64::from(self.span.pieces.end.saturating_sub(1)),
        ) as u32
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

/// One pass: ask the policy where the playhead has left us, then make its
/// answer true.
///
/// The order is the policy's own: **advertise what is committed before
/// reclaiming**, because the two sets are disjoint and the commit is what
/// takes a piece out of reach of the reclaim. Then reclaim, which is the
/// backend forgetting the pieces and the store unlinking them, under the
/// claim that keeps the two atomic.
pub(crate) async fn advance<H: TorrentHandle>(
    handle: &H,
    store: &StoreRoot,
    info_hash: &str,
    retention: &mut FileRetention,
    offset_in_file: u64,
) -> RetentionPass {
    let held = store.held(info_hash);
    let playhead = retention.playhead(offset_in_file);
    let decision = retention.policy.advance(playhead, &held);

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
    for run in runs(&decision.reclaim) {
        pass.reclaimed += release(handle, store, info_hash, run).await;
    }
    pass
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
        Ok(Some(dropped)) => take_claimed(store, info_hash, dropped),
        // A backend with no have-set of its own for the deletion to
        // disagree with: there is nothing to interlock against.
        Ok(None) => store.delete_pieces(info_hash, pieces),
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
pub(crate) fn take_claimed(
    store: &StoreRoot,
    info_hash: &str,
    dropped: crate::backend::DroppedFilePieces,
) -> usize {
    let freed = store.delete_pieces(info_hash, dropped.pieces().iter().copied());
    // Released only now that the bytes are gone.
    drop(dropped);
    freed
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
    Nothing { first: bool },
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
            Self::Nothing { .. } => true,
            Self::Announced => false,
            Self::Policy { pieces, committed } => {
                pieces.contains(&piece) && !committed.contains(&piece)
            }
        }
    }

    /// Whether this torrent's pieces sort to the front of the size rule:
    /// bytes nothing will ever read or resume into.
    pub fn goes_first(&self) -> bool {
        matches!(self, Self::Nothing { first: true })
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
        self.insert(info_hash, TorrentGate::Nothing { first: true });
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
}
