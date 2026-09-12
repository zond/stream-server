//! What we keep, and what we share: the arithmetic of it, with no filesystem
//! and no librqbit types in it.
//!
//! The store can hold a piece or give it back, and the want-set can want a
//! piece or stop wanting it. Neither of them decides *which* pieces. That
//! decision is a handful of numbers -- a budget, a piece length, the pieces of
//! one file, where the playhead is -- and it lives here on its own, beside
//! [`super::layout`] and for the same reason: it is the part that is easy to
//! get quietly wrong and easy to test exhaustively, and it should not have to
//! be reasoned about through a session, a disk and a swarm.
//!
//! # The policy
//!
//! **The budget** is bytes, per volume, and comes from the server's
//! `CacheLimit::effective` -- `min(configured, occupied + available - floor)`.
//! It is an input here and is not recomputed: a second reading of "how much
//! room is there" that disagreed with the published one would have the two
//! halves of the cache sized against different numbers.
//!
//! **If the budget covers the whole file we keep the whole file** and share
//! all of it. No split and no window. This is the phone with 379 GB free and
//! it is every desktop.
//!
//! **If it does not, it is split:**
//!
//! * One part is a rolling window around the playhead, roughly 90% ahead and
//!   10% behind, so a short scan back is served from disk instead of from the
//!   swarm.
//! * The other is committed for sharing. It is filled *opportunistically*,
//!   from pieces we already hold -- nothing here ever asks for a byte outside
//!   the playhead -- and once a piece is in it, it stays.
//!
//! Where the split falls is not a half any more. Both parts are bounded in
//! *time* first ([`Buffering`]) -- so many seconds of this stream at the rate
//! bytes are really leaving the server at -- and the budget is only the
//! ceiling. The committed part yields first and the window yields last: it
//! also has a floor, the lookahead an open stream was already granted, which
//! beats every cap here because the alternative is a stream fetching exactly
//! what the next pass deletes.
//!
//! **Only what is committed is advertised** -- once an engine can be told
//! that. That is the whole of being a good citizen here: a piece we might
//! reclaim is never announced, so we never advertise-then-refuse, which is
//! what gets a client choked. A window piece would be held and readable and
//! *not* announced, because the window moves and it will go. On a small volume
//! that means we honestly seed little; on a roomy one we seed everything.
//!
//! **That last part was a decision this module could state and not perform,
//! and it performs it now.** At the rev this crate used to pin, `have`
//! implied announced on both paths out of librqbit -- the `have` broadcast
//! and the handshake bitfield, which serialises `get_have_pieces()` whole --
//! and a window piece has to be `have` for the stream to read it, so wiring
//! this policy up would have announced every window piece and withdrawn it
//! again a few seconds later. The fork now has the third state,
//! `ManagedTorrent::set_pieces_advertised`: a suppression set on the chunk
//! tracker, independent of both the have-set and the reclaim want-set, and
//! settable *before* a piece is downloaded so no Have ever goes out for it.
//! [`crate::retention`] is what holds a window back with it and what puts a
//! committed piece into what we announce, and [`RetentionPolicy::advertised`]
//! is now what a peer really is shown.
//!
//! # Two choices worth stating, because they look arbitrary
//!
//! **Which pieces we share is drawn before playback starts, not learned from
//! it.** The committed set is a uniformly random subset of the file's pieces,
//! of the size the committed capacity allows, taken from a seed held per
//! entity for that entity's life (see [`choose`]). Random and not a stride,
//! because a stride is a lattice -- two clients with the same k differ only by
//! phase and can still overlap almost completely -- and independent draws
//! overlap only by chance, which is the property that makes peers who cannot
//! see each other sum to even coverage of a torrent. Even and not biased late,
//! because a shared late bias only moves the hole: if every client kept the
//! tail, the tail becomes the over-replicated part and the head goes scarce.
//!
//! It is the opposite of the rule that used to be here, which committed a
//! piece when the window *released* it -- the playhead having walked past it
//! -- and so settled the shared set out of the first minutes of the film. In a
//! swarm of streaming clients everyone watches from the start, so the head is
//! the most replicated part of a torrent and the tail is the scarce part: that
//! rule kept exactly the pieces nobody needs. Measured on the field device --
//! a cap of thirty-six pieces, about fifty seconds of a 23 Mbps film -- it
//! shared the first fifty seconds of a 109-minute feature and nothing else for
//! the rest of the process.
//!
//! **A drawn piece is committed and announced the moment we hold it**, verified
//! on the disk, rather than when something lets go of it. The old rule had to
//! wait: with membership decided by playback there was always a piece we might
//! still reclaim, and announcing one of those is the advertise-then-refuse this
//! module exists to prevent. With the set fixed up front there is nothing to
//! take back, so a piece can go out the moment it verifies -- which is what any
//! BitTorrent client does with every piece it completes, and it means peers
//! learn we have those bytes *while* the viewer is watching, which is when the
//! upload switch lets us serve them.
//!
//! **Nothing once committed is ever un-announced or reclaimed**, and that is
//! now a property of the design rather than a consequence of a capacity never
//! being reached. There is no un-have in BitTorrent: hiding a piece changes
//! only the bitfield a *new* peer is handed at its handshake, while a peer that
//! already holds our Have can still ask for it and, the bytes being gone, be
//! hung up on. So a capacity that shrinks under the set only stops it growing
//! ([`RetentionPolicy::observe`]), and a smaller budget adopts the whole of
//! what the bigger one announced, over its capacity and all
//! ([`RetentionPolicy::carry_into`]).
//!
//! **The set under-fills, and that is correct.** It is never fetched for: we
//! commit what we hold, and we hold what the viewer's window fetched, so a
//! viewer who stops halfway fills about half the draw and the rest of it stays
//! empty. Filling it would mean fetching bytes for the swarm rather than for
//! the viewer, which on a metered phone is a trade nobody agreed to, and an
//! unfilled cache costs nothing.
//!
//! **And the draw is uniform rather than ranked.** The design says to choose by
//! whatever is cheapest and explicitly not by rarity: measured across four live
//! swarms, rarity-based retention beat random by <=0.15%, and by 0.00% on three
//! of them, because seeder fractions of 94-100% cap the whole effect. So there
//! is no availability map here and nothing to rank.
//!
//! # What this does not decide
//!
//! Nothing durable. The committed set is per-session state held in this
//! struct: sharing runs between sessions and stops when the next stream
//! starts, and there is no set to re-adopt at launch. A piece it never
//! reclaimed still has to be claimed by the startup sweep like everything else
//! (see [`super::sweep`]), because "clean on close" does not run when Android
//! kills the app.

use std::collections::BTreeSet;
use std::ops::Range;

/// How much of the rolling window sits behind the playhead, in percent. The
/// rest is ahead.
///
/// Behind is for a scan back -- a few seconds of rewind, a player re-reading
/// its container index -- not for a real seek, which reaches the swarm again
/// whatever we do. Ahead is what keeps playback fed, so it gets the rest.
const BEHIND_PERCENT: u64 = 10;

/// How much of the budget is committed for sharing rather than spent on the
/// window.
///
/// **This is the only thing that differs between the two kinds of stream this
/// policy governs, and it is a number rather than a branch.** The budget is
/// split between what playback needs and what a peer may be offered; a
/// proxied URL response is not seeded, so nobody can be offered any of it and
/// the whole budget is window. Everything else -- the 90/10 window, the
/// release rule, the reclaim -- is the same arithmetic on both, which is what
/// "one retention policy over both stores" means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Share {
    /// Half of it: a torrent's pieces, which a peer may ask us for.
    Half,
    /// None of it. **Not "share nothing yet"** -- there is no swarm for a
    /// URL response and there never will be, so a committed set for one
    /// would be a permanently unreclaimable half of the cache held for a
    /// reader that does not exist.
    Nothing,
}

impl Share {
    /// How many of `budget` pieces this leaves for the committed set.
    ///
    /// The odd piece goes to the window: it is what keeps playback fed, and
    /// the committed half is generosity.
    fn committed_of(self, budget: u32) -> u32 {
        match self {
            Self::Half => budget / 2,
            Self::Nothing => 0,
        }
    }
}

/// What a reader on this entity has already been promised, and what the
/// window must therefore be big enough to hold.
///
/// **A window smaller than an open stream's lookahead is a disk
/// permanently over budget.** The lookahead is fixed when the reader opens
/// (`Engine::try_get_file_with_intent`) and there is no setter for it in
/// the fork, while the budget is republished every sixty seconds from the
/// volume's free space and a shrink resizes the policy under readers that
/// are already open. The stream then fetches past the window forever: the
/// fork's `drop_pieces` refuses a piece inside `streams.wanted_ranges`, so
/// the pass does not fetch-and-reclaim in a loop -- it simply never gets
/// the bytes back, and pays a wasted `drop_pieces` per tick to be refused
/// again. The window is what yields, because it is the half that can.
///
/// The number is what a reader was *granted*, not what its buffer profile
/// would have liked: the grant is already the smaller of the profile's cap
/// and the window's forward reach at the open, so this is a no-op at the
/// moment a reader opens and binds only when the budget moves under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Buffering {
    /// The largest lookahead granted to a reader open on this entity, in
    /// bytes. Zero when none is open, which is every entity nothing has
    /// read yet.
    pub lookahead_bytes: u64,
    /// How many seconds of this stream the window's forward reach may buy,
    /// or `None` for no time cap at all -- the `Maximum` buffer profile,
    /// which asks for the whole file.
    ///
    /// **This is the user's decision and the disk is only the ceiling.**
    /// Sized from the budget alone, the forward reach is a fixed fraction
    /// of `cacheSize`, so a viewer who gave the app a bigger cache bought a
    /// bigger mobile-data bill without being asked. It binds only once
    /// [`Self::bytes_per_second`] is known.
    pub window_seconds: Option<u64>,
    /// How many seconds of watched video the committed set may hold, or
    /// `None` for no cap.
    ///
    /// The committed set is what a peer may be offered and what a scan back
    /// is served from; both want the recent past, not half the volume. Half
    /// the budget is what it used to be, which on a small cache is the half
    /// the forward buffer needed.
    pub committed_seconds: Option<u64>,
    /// The rate bytes are really leaving the server at for this entity,
    /// smoothed, or `None` before anything has been measured.
    ///
    /// **An observation and not a bitrate.** It is what the reader is being
    /// handed per second, which starts at whatever the swarm can give while
    /// the player fills its own cache and settles on the real bitrate once
    /// that cache is full. Until there is one, the two time caps do not
    /// apply at all and the byte arithmetic below stands on its own.
    pub bytes_per_second: Option<u64>,
    /// The draw that decides which pieces of this entity this process
    /// offers to the swarm; see [`choose`].
    ///
    /// One per entity, held for as long as the entity is, and **random**:
    /// clients that all keep the same pieces cover a torrent as unevenly as
    /// clients that all keep its first minute. It is not persisted, because
    /// the committed set is not -- sharing runs between sessions and the
    /// next stream ends it.
    pub seed: u64,
}

impl Buffering {
    /// Bytes `seconds` of this stream comes to, or `None` with no measured
    /// rate to convert it at.
    fn over(&self, seconds: u64) -> Option<u64> {
        self.bytes_per_second
            .map(|rate| rate.saturating_mul(seconds))
    }
}

/// The smallest a time cap may come to, whatever the measured rate says.
///
/// A cap is a promise about playback, and ninety seconds of a 2 Mbps
/// talking-heads documentary is twenty-two megabytes -- enough buffer for a
/// good connection and nothing at all for a bad one, where the cost of
/// having fetched more is only disk we had anyway. So the cap never falls
/// below this, and the budget is still what bounds it from above.
const SMALLEST_TIME_CAP_BYTES: u64 = 64 * 1024 * 1024;

/// The smallest window whose forward reach ([`RetentionPolicy::ahead_of`])
/// covers `bytes`.
///
/// The reach is the window less the tenth of it that sits behind the
/// playhead, so the floor is stated on the reach and converted here rather
/// than being applied to the window directly: a floor read as a window
/// would leave the forward reach a tenth short of the lookahead it exists
/// to cover, which is the whole of what it is for.
fn window_for_reach(bytes: u64, piece_length: u64) -> u32 {
    debug_assert!(piece_length > 0, "a piece length of zero");
    let pieces = bytes.div_ceil(piece_length.max(1)).min(u64::from(u32::MAX));
    // `reach(w) = w - w * BEHIND_PERCENT / 100` is non-decreasing in `w`,
    // so this is a search and not a formula: a closed form has to round,
    // and rounding the wrong way costs a whole piece of window at every
    // budget. Climb by the deficit, which converges in a few steps, then
    // walk back to the least window that still covers it.
    let reach = |window: u64| window - window * BEHIND_PERCENT / 100;
    let mut window = pieces;
    while window < u64::from(u32::MAX) && reach(window) < pieces {
        window = (window + (pieces - reach(window))).min(u64::from(u32::MAX));
    }
    while window > pieces && reach(window - 1) >= pieces {
        window -= 1;
    }
    window as u32
}

/// A piece's rank in the draw that decides what this process shares of a
/// file: a hash of the entity's seed and the piece index, uniform over
/// `u64`.
///
/// SplitMix64's finaliser. Nothing here needs a cryptographic hash or a
/// generator with state -- what it needs is that two clients with different
/// seeds pick unrelated sets, and that one client's set is the same every
/// time it is asked.
fn rank(seed: u64, piece: u32) -> u64 {
    let mut z = seed
        .wrapping_add(u64::from(piece).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The `count` pieces of `pieces` this process will share: the ones of
/// lowest [`rank`].
///
/// **A uniformly random subset, drawn once and not sampled from playback.**
/// A stride is a lattice, so two clients that chose the same one differ
/// only by phase and can still overlap almost completely; an independent
/// draw per client overlaps only by chance, which is the property that
/// makes many peers with no way to see each other add up to even coverage
/// of a torrent. And even rather than biased late, because a shared bias
/// only moves the hole: if every client kept the tail, the tail is the
/// over-replicated part and the head goes scarce.
///
/// The lowest `count` ranks and not a threshold on the rank, so the set is
/// exactly the size asked for; and lowest-`count` is nested in `count`, so
/// a set that grows or shrinks with the budget keeps every piece it had
/// rather than being redrawn under what is already announced.
fn choose(seed: u64, pieces: Range<u32>, count: u32) -> BTreeSet<u32> {
    let total = pieces.end - pieces.start;
    if count >= total {
        return pieces.collect();
    }
    if count == 0 {
        return BTreeSet::new();
    }
    // A bounded max-heap, keyed on the rank: at most `count` entries are
    // ever held, so a torrent of millions of pieces costs one pass and the
    // memory of the set it is choosing.
    let mut best: BTreeSet<(u64, u32)> = BTreeSet::new();
    for piece in pieces {
        best.insert((rank(seed, piece), piece));
        if best.len() > count as usize {
            best.pop_last();
        }
    }
    best.into_iter().map(|(_, piece)| piece).collect()
}

/// How a budget relates to the file it has to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// The budget covers the file. Keep all of it, share all of it, drop
    /// nothing.
    Whole,
    /// It does not, so it is split: see [`Share`] for what decides where.
    Split {
        /// Pieces the rolling window may cover.
        window: u32,
        /// Pieces the committed set may hold.
        committed: u32,
    },
}

impl Shape {
    /// The most pieces this shape intends to hold at once, or `None` when it
    /// holds the whole file.
    ///
    /// The window and the committed set can overlap only in the sense that a
    /// committed piece may be read back into the window's range; they are
    /// disjoint as *sets*, because a piece is committed exactly when the window
    /// lets go of it. So the two halves sum, and the sum is the budget.
    pub fn piece_budget(self) -> Option<u32> {
        match self {
            Self::Whole => None,
            Self::Split { window, committed } => Some(window.saturating_add(committed)),
        }
    }
}

/// What one pass decided, at one playhead position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decision {
    /// The pieces the rolling window covers now: wanted and kept, and *not*
    /// advertised unless they are also committed.
    pub window: Range<u32>,
    /// Held pieces in neither the window nor the committed set. Drop them from
    /// the want-set and then delete them, in that order -- not have, not
    /// wanted, not advertised, then gone.
    pub reclaim: Vec<u32>,
    /// Pieces that joined the committed set on this pass. They may be
    /// advertised from now on, and nothing will ever reclaim them.
    pub committed: Vec<u32>,
    /// Pieces that left the committed set because we no longer hold them.
    ///
    /// Nothing here takes a committed piece away, so this is only ever the
    /// disk losing one behind our back -- but "only advertise what is
    /// committed" has to hold in both directions, and a committed piece that
    /// is gone is exactly the advertise-then-refuse this policy exists to
    /// avoid.
    pub withdrawn: Vec<u32>,
}

/// The policy for one file being streamed, carrying the committed set across
/// playhead positions.
///
/// It governs a range of *torrent* piece indices -- the pieces of the file
/// being played -- and ignores everything outside it, so the pieces of a
/// torrent's other files are neither committed nor reclaimed here. A piece on
/// the boundary between two files belongs to both, and this module cannot
/// tell: its reclaim set includes the file's first and last piece whoever
/// else owns bytes in them, and [`crate::retention`] is what takes those
/// back out before anything is deleted. The unpin path has its own answer to
/// the same question in [`PieceStore`](super::store::PieceStore)'s
/// `TorrentStorage::remove_file`, which
/// deletes a boundary piece only once every file that owns bytes in it is
/// gone.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pieces: Range<u32>,
    /// What this policy was built from, so [`Self::observe`] can re-shape
    /// it under a rate that was not known when it was built. None of them
    /// is ever re-read from the world: a second reading of the budget that
    /// disagreed with the published one would size the two halves of the
    /// cache against different numbers.
    budget_bytes: u64,
    piece_length: u64,
    bytes: u64,
    share: Share,
    shape: Shape,
    committed: BTreeSet<u32>,
    /// The pieces this policy will share, drawn when it was built and
    /// never changed except to follow the capacity up or down.
    ///
    /// **Membership is decided in advance, not sampled from playback.** A
    /// chosen piece is committed the moment we hold it -- verified on the
    /// disk -- announced from then on, and kept for the life of the policy;
    /// nothing here ever takes one back. That is exactly what a BitTorrent
    /// client with no retention at all does with every piece it completes,
    /// and it is available here only because the set is fixed up front:
    /// with membership decided by playback there was always a piece we
    /// might still reclaim, so nothing could be announced until the window
    /// had let go of it.
    ///
    /// It is never *fetched* for. We commit what we hold, and we hold what
    /// the viewer's window fetched, so a viewer who stops halfway fills
    /// about half the set and the rest of it stays empty. That is correct:
    /// an unfilled cache costs nothing, and filling it would mean fetching
    /// bytes for the swarm rather than for the viewer -- on a metered phone
    /// a trade nobody agreed to.
    ///
    /// Empty and unused under [`Shape::Whole`], where every piece of the
    /// file is shared as it arrives.
    chosen: BTreeSet<u32>,
    /// The draw [`Self::chosen`] came out of; see [`choose`].
    seed: u64,
}

impl RetentionPolicy {
    /// The policy for a stream occupying `pieces` of a chunk store and
    /// `bytes` of the disk, under a budget of `budget_bytes`, `share` of
    /// which may be committed for a peer.
    ///
    /// "Pieces" here are a torrent's when the caller is the piece store's
    /// adapter and 256 KiB chunks when it is `/proxy`'s: the arithmetic is
    /// the store's own -- an index space and a length -- and neither
    /// adapter's names for them reach in here.
    ///
    /// All four numbers have to describe the same file, and three of them
    /// overdetermine the fourth, so the disagreement is checked for rather
    /// than assumed away: `bytes` must be what `pieces` holds at this piece
    /// length, which is a full piece each except that the last may be short.
    /// A mismatch means the caller resolved the file to the wrong range, and
    /// every window and every reclaim after that would be about somebody
    /// else's bytes.
    pub fn new(
        budget_bytes: u64,
        piece_length: u64,
        pieces: Range<u32>,
        bytes: u64,
        share: Share,
        buffering: Buffering,
    ) -> anyhow::Result<Self> {
        if piece_length == 0 {
            anyhow::bail!("a piece length of zero");
        }
        if pieces.is_empty() {
            anyhow::bail!("a file with no pieces cannot be streamed");
        }
        let count = u64::from(pieces.end - pieces.start);
        let most = count.checked_mul(piece_length).ok_or_else(|| {
            anyhow::anyhow!("{count} pieces of {piece_length} bytes overflow a u64")
        })?;
        if bytes > most || bytes <= most - piece_length {
            anyhow::bail!(
                "{count} pieces of {piece_length} bytes cannot hold {bytes} bytes: \
                 that is between {} and {most}",
                most - piece_length + 1
            );
        }
        let shape = Self::shape_for(budget_bytes, piece_length, bytes, share, buffering);
        let chosen = match shape {
            Shape::Whole => BTreeSet::new(),
            Shape::Split { committed, .. } => choose(buffering.seed, pieces.clone(), committed),
        };
        Ok(Self {
            pieces,
            budget_bytes,
            piece_length,
            bytes,
            share,
            shape,
            committed: BTreeSet::new(),
            chosen,
            seed: buffering.seed,
        })
    }

    /// Split the budget, or don't.
    ///
    /// The comparison that decides is in bytes and not in pieces, because the
    /// last piece of a file is usually short and "the budget covers the file"
    /// has to mean the file and not a rounded-up multiple of it. Everything
    /// after it is in pieces, and the conversion floors: a budget that is two
    /// and a half pieces buys two.
    ///
    /// Where the split falls is [`Share`]'s and there is no branch on the
    /// kind of stream here: a torrent gives half of it to the committed set,
    /// a proxied response gives none, and what is left over is the window in
    /// both cases.
    fn shape_for(
        budget_bytes: u64,
        piece_length: u64,
        bytes: u64,
        share: Share,
        buffering: Buffering,
    ) -> Shape {
        if budget_bytes >= bytes {
            return Shape::Whole;
        }
        // `budget_bytes < bytes`, so this cannot exceed the file's own piece
        // count and cannot need more than the u32 piece indices already are.
        let budget = (budget_bytes / piece_length) as u32;
        // The floor, and the committed half is what yields to it -- to
        // nothing, if that is what it takes. Sharing is generosity; a
        // window narrower than what an open stream is already fetching is a
        // disk that can never come back under its budget. See
        // [`Buffering`].
        let floor = window_for_reach(buffering.lookahead_bytes, piece_length);
        // Three numbers, composed in one order: what the disk can hold,
        // what the viewer asked for in time, and what an open stream is
        // already fetching.
        // What the committed set could want, capped by time before the
        // window is sized: the rest of the budget is the window's, so a
        // profile with no time cap of its own (`Maximum`) takes everything
        // sharing does not need rather than stopping at half.
        let committed_cap = buffering
            .committed_seconds
            .and_then(|seconds| buffering.over(seconds))
            .map(|bytes| (bytes / piece_length) as u32);
        let wants_committed = share.committed_of(budget);
        let mut window = budget - wants_committed.min(committed_cap.unwrap_or(wants_committed));
        if let Some(cap) = buffering
            .window_seconds
            .and_then(|seconds| buffering.over(seconds))
            .map(|bytes| window_for_reach(bytes.max(SMALLEST_TIME_CAP_BYTES), piece_length))
        {
            if cap < floor {
                // The floor is bigger than the time the design wants to
                // buffer, which means MAX_SEEK_HOT_WINDOW_BYTES is. The
                // alternative to letting the floor win is a stream fetching
                // exactly what the pass then deletes, so it wins -- and the
                // fact is said out loud, because it is a statement about
                // the constants and not about this file.
                tracing::info!(
                    floor_pieces = floor,
                    cap_pieces = cap,
                    seconds = buffering.window_seconds,
                    bytes_per_second = buffering.bytes_per_second,
                    "a stream's lookahead is wider than the time cap the buffer profile asks for; the lookahead wins"
                );
            }
            window = window.min(cap);
        }
        let window = window.max(floor);
        // And what is left of the budget may be shared, for as much of it
        // as time allows. The rest is simply not used: an unfilled cache
        // costs nothing, and a starved forward buffer costs a stall.
        let mut committed = budget.saturating_sub(window).min(wants_committed);
        if let Some(cap) = committed_cap {
            committed = committed.min(cap);
        }
        Shape::Split {
            window,
            committed: if share == Share::Nothing {
                0
            } else {
                committed
            },
        }
    }

    /// Re-shape under what the entity's readers are doing now -- a rate
    /// that has been measured since this policy was built, a reader that
    /// has opened or ended -- and say whether anything moved.
    ///
    /// The budget, the file and the share are this policy's own and are not
    /// re-read: a budget that moves builds a new policy through
    /// [`Self::carry_into`], which is where a committed set over the new
    /// capacity is dealt with. Nothing here takes a committed piece away
    /// (see [`Decision::committed`]); a capacity that shrinks under one
    /// only stops the set growing.
    pub fn observe(&mut self, buffering: Buffering) -> bool {
        let shape = Self::shape_for(
            self.budget_bytes,
            self.piece_length,
            self.bytes,
            self.share,
            buffering,
        );
        let moved = shape != self.shape;
        let was = self.capacity();
        self.shape = shape;
        if self.capacity() != was {
            // Lowest-rank-`count` is nested in `count`, so this keeps every
            // piece the old set had where it grew and drops only unchosen
            // ones where it shrank -- and whatever is already committed
            // stays chosen whatever the capacity says, because nothing this
            // policy has announced is ever taken back.
            self.chosen = match self.shape {
                Shape::Whole => BTreeSet::new(),
                Shape::Split { committed, .. } => choose(self.seed, self.pieces.clone(), committed),
            };
            self.chosen.extend(self.committed.iter().copied());
        }
        moved
    }

    /// How many pieces the committed set may hold.
    fn capacity(&self) -> u32 {
        match self.shape {
            Shape::Whole => self.pieces.end - self.pieces.start,
            Shape::Split { committed, .. } => committed,
        }
    }

    /// The pieces of the file this policy governs.
    pub fn pieces(&self) -> Range<u32> {
        self.pieces.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// The committed set: what is advertised, and nothing else is.
    ///
    /// [`crate::retention`] is what makes that true -- it holds the whole
    /// file back from what the torrent announces before the reader opens,
    /// and puts a piece back only when [`Self::advance`] commits it. What
    /// we announce is exactly what nothing will reclaim while the entity is
    /// being played.
    pub fn advertised(&self) -> &BTreeSet<u32> {
        &self.committed
    }

    pub fn is_advertised(&self, piece: u32) -> bool {
        self.committed.contains(&piece)
    }

    /// Move what this policy has learned onto `next`, a policy over the same
    /// pieces under another budget.
    ///
    /// The committed set is what we announce, and a policy built afresh
    /// starts with none of it, so its first pass would find every piece
    /// this one committed outside its window, in no committed set, and
    /// reclaim it -- pieces a peer was told about minutes ago, deleted
    /// under it.
    ///
    /// **Nothing comes back over the new budget's capacity, because nothing
    /// is ever taken back.** There is no un-have in BitTorrent: hiding a
    /// piece changes only the bitfield a *new* peer is sent at its
    /// handshake, and a peer that already has our Have can still ask for
    /// it -- at which point, the bytes being gone, the fork's upload path
    /// can only hang up, there being no reject message to send. A client
    /// that advertises and then disconnects is an unreliable peer, and some
    /// clients snub or ban for it. So a smaller budget adopts what the
    /// bigger one announced, over its capacity and all: those bytes are the
    /// price of having said we had them, and they are bounded by the
    /// capacity that said it.
    pub fn carry_into(&self, next: &mut Self) {
        debug_assert_eq!(
            self.pieces, next.pieces,
            "a policy carried onto another file's pieces"
        );
        next.committed = self.committed.clone();
        // Chosen as well as committed: a piece we announce is one nothing
        // may reclaim, and [`Self::advance`] reads that off the chosen set.
        next.chosen.extend(next.committed.iter().copied());
    }

    /// Where the rolling window sits for a playhead on `piece`.
    ///
    /// The window keeps its size and slides to stay inside the file, so a
    /// playhead at the start gets all of it ahead and one at the end gets all
    /// of it behind. Shrinking it at the ends instead would give the last
    /// minutes of a film a fraction of the read-ahead the middle got, for no
    /// reason -- the budget is the same there.
    ///
    /// It is never empty, even under a budget of nothing: the piece being read
    /// is not optional, and a policy that asked for the bytes under the
    /// player's head to be deleted would be asking for a stall. So a zero
    /// budget holds one piece, which is the one place this can exceed what it
    /// was given, and it is a piece rather than a stall.
    pub fn window_at(&self, playhead: u32) -> Range<u32> {
        let total = self.pieces.end - self.pieces.start;
        let want = match self.shape {
            Shape::Whole => total,
            Shape::Split { window, .. } => window.clamp(1, total),
        };
        let playhead = playhead.clamp(self.pieces.start, self.pieces.end - 1);
        let behind = (u64::from(want) * BEHIND_PERCENT / 100) as u32;
        let mut start = playhead.saturating_sub(behind).max(self.pieces.start);
        if start.saturating_add(want) > self.pieces.end {
            // `want <= total`, so sliding back to fit cannot cross the start.
            start = self.pieces.end - want;
        }
        start..start + want
    }

    /// The pieces from `playhead` to the window's forward edge, as that edge
    /// stands once nothing clamps the window against the file's start:
    /// `playhead` and the nine tenths ahead of it, cut at the last piece.
    ///
    /// This is what a stream opened at `playhead` may fetch ahead of itself.
    /// Not [`Self::window_at`]`.end`: at the start of a file the window is
    /// clamped against piece zero, so its end is the whole window ahead of
    /// the playhead rather than nine tenths of it, and a lookahead sized from
    /// that end outruns the window for the first tenth of it as the playhead
    /// moves on -- the window's end stands still while the lookahead's
    /// advances -- fetching pieces the next pass reclaims. The reach is at
    /// most the window's end wherever the playhead is, and it moves with the
    /// playhead exactly as the unclamped window does.
    pub fn ahead_of(&self, playhead: u32) -> Range<u32> {
        let total = self.pieces.end - self.pieces.start;
        let want = match self.shape {
            Shape::Whole => total,
            Shape::Split { window, .. } => window.clamp(1, total),
        };
        let playhead = playhead.clamp(self.pieces.start, self.pieces.end - 1);
        let behind = (u64::from(want) * BEHIND_PERCENT / 100) as u32;
        let end = playhead.saturating_add(want - behind).min(self.pieces.end);
        playhead..end
    }

    /// Decide, for a playhead on `playhead` over the pieces we currently
    /// `held`, what the window covers, what joins the committed set, and what
    /// to give back.
    ///
    /// `held` is what is on disk -- the have-set, which under this design is
    /// the piece files themselves. Pieces outside this file's range are not
    /// this policy's to touch and are skipped, not reclaimed.
    ///
    /// Three things can happen to a held piece of this file, not two. A piece
    /// already in the committed set is **left alone** -- not reclaimed, and
    /// not committed a second time -- because nothing here ever takes a
    /// committed piece back (see [`Decision::committed`]); a caller that reads
    /// this as two outcomes and reclaims whatever is outside the window
    /// deletes the pieces it is advertising, which is the advertise-then-
    /// refuse the whole policy exists to avoid. Any *other* held piece is
    /// **committed** if the draw chose it ([`Self::chosen`]) and **reclaimed**
    /// if it is outside the window and the draw did not. The playhead decides
    /// neither: membership was settled when the policy was built, so a piece is
    /// committed the moment we hold it, whether the window is still over it or
    /// has never been, and a seek in either direction commits exactly the
    /// pieces it happens to have brought in.
    ///
    /// Idempotent for a fixed playhead and a fixed `held`: the second call
    /// commits nothing new -- the pieces it would commit are committed -- and
    /// reclaims the same pieces, because a reclaim is a request rather than a
    /// record. The caller is what makes that true, by deleting them and no
    /// longer holding them.
    pub fn advance(&mut self, playhead: u32, held: &BTreeSet<u32>) -> Decision {
        let window = self.window_at(playhead);
        let mut decision = Decision {
            window: window.clone(),
            ..Default::default()
        };

        // A committed piece we have stopped holding cannot stay advertised.
        let withdrawn = &mut decision.withdrawn;
        self.committed.retain(|piece| {
            if held.contains(piece) {
                true
            } else {
                withdrawn.push(*piece);
                false
            }
        });

        // Ascending, for a stable order in the decision and nothing else:
        // which pieces we share is not a function of the order they are
        // offered in any more. It was settled when the policy was built.
        for &piece in held {
            if !self.pieces.contains(&piece) || self.committed.contains(&piece) {
                continue;
            }
            // Under `Whole` the file fits, so every piece of it is shared
            // as soon as it arrives and none of it is ever reclaimed.
            if self.shape == Shape::Whole || self.chosen.contains(&piece) {
                self.committed.insert(piece);
                decision.committed.push(piece);
            } else if !window.contains(&piece) {
                decision.reclaim.push(piece);
            }
        }
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE: u64 = 1000;

    fn held(pieces: impl IntoIterator<Item = u32>) -> BTreeSet<u32> {
        pieces.into_iter().collect()
    }

    /// A file of `count` full pieces starting at piece 0.
    fn policy(budget_pieces: u64, count: u32) -> RetentionPolicy {
        RetentionPolicy::new(
            budget_pieces * PIECE,
            PIECE,
            0..count,
            u64::from(count) * PIECE,
            Share::Half,
            Buffering::default(),
        )
        .expect("a consistent file")
    }

    /// The same file under a stream nothing can be shared from: `/proxy`'s.
    fn unshared(budget_pieces: u64, count: u32) -> RetentionPolicy {
        RetentionPolicy::new(
            budget_pieces * PIECE,
            PIECE,
            0..count,
            u64::from(count) * PIECE,
            Share::Nothing,
            Buffering::default(),
        )
        .expect("a consistent file")
    }

    /// Play the stream through `playheads`, as a caller would: honour every
    /// reclaim, and fill in whatever the window covers, because covering a
    /// piece is what fetches it.
    ///
    /// Stepping matters, and several tests below need it rather than a single
    /// leap: what a policy commits is what it has held, and it holds what the
    /// windows it was walked through covered.
    fn play(
        p: &mut RetentionPolicy,
        disk: &mut BTreeSet<u32>,
        playheads: impl IntoIterator<Item = u32>,
    ) {
        for playhead in playheads {
            let d = p.advance(playhead, disk);
            for piece in &d.reclaim {
                disk.remove(piece);
            }
            disk.extend(d.window.clone());
        }
    }

    /// A ten-gigabyte cache, a 16 GiB film, and a stream delivering three
    /// mebibytes a second: the shape the three bounds compose to.
    fn timed(buffering: Buffering) -> Shape {
        const MIB: u64 = 1 << 20;
        let piece = 4 * MIB;
        RetentionPolicy::new(
            10 * 1024 * MIB,
            piece,
            0..4000,
            4000 * piece,
            Share::Half,
            buffering,
        )
        .expect("a consistent file")
        .shape()
    }

    fn watching(rate_mib: u64, seconds: Option<u64>) -> Buffering {
        const MIB: u64 = 1 << 20;
        Buffering {
            lookahead_bytes: 0,
            window_seconds: seconds,
            committed_seconds: Some(90),
            bytes_per_second: Some(rate_mib * MIB),
            seed: 0,
        }
    }

    /// **A bigger disk cache must not silently buy a bigger data bill.**
    ///
    /// Sized from the budget alone, the forward reach is 45% of
    /// `cacheSize`: a viewer who gave the app ten gigabytes got a
    /// five-gigabyte window and fetched it over whatever connection they
    /// were on. What a buffer is *for* is measured in seconds of the film,
    /// so that is the unit the cap is stated in, converted at the rate
    /// bytes are really going out.
    #[test]
    fn the_window_is_bounded_by_the_seconds_of_stream_it_buys() {
        assert_eq!(
            timed(watching(3, Some(90))),
            Shape::Split {
                window: 75,
                committed: 67
            },
            "ninety seconds of forward reach and ninety of sharing: 568 MiB of a 10 GiB cache, \
             and the rest simply not used"
        );
        assert_eq!(
            timed(watching(3, Some(4 * 60))),
            Shape::Split {
                window: 199,
                committed: 67
            },
            "the Large profile buys four minutes of the same stream"
        );
    }

    /// **`Maximum` has no time cap: it is the whole file while you are
    /// watching it.**
    ///
    /// Where the budget covers the film that is [`Shape::Whole`] and
    /// nothing is bounded at all. Where it does not, it is the widest
    /// window the budget allows -- everything sharing does not need, rather
    /// than the half a budget-sized split would leave.
    #[test]
    fn the_maximum_profile_asks_for_the_file_and_not_for_a_number_of_seconds() {
        assert_eq!(
            timed(watching(3, None)),
            Shape::Split {
                window: 2493,
                committed: 67
            },
            "no cap on the window, and it takes everything the committed set does not"
        );
        const MIB: u64 = 1 << 20;
        let piece = 4 * MIB;
        let whole = RetentionPolicy::new(
            10 * 1024 * MIB,
            piece,
            0..2000,
            2000 * piece,
            Share::Half,
            watching(3, None),
        )
        .expect("a consistent file");
        assert_eq!(
            whole.shape(),
            Shape::Whole,
            "and a film the budget covers is kept and shared whole, as it always was"
        );
    }

    /// **Where the floor and the cap disagree, the floor wins.**
    ///
    /// The alternative is a stream fetching exactly what the pass then
    /// deletes. It says something true when it happens -- that
    /// `MAX_SEEK_HOT_WINDOW_BYTES` is wider than the time the profile wants
    /// to buffer -- which is why the policy says it out loud.
    #[test]
    fn a_lookahead_wider_than_the_time_cap_wins_over_it() {
        const MIB: u64 = 1 << 20;
        assert_eq!(
            timed(Buffering {
                lookahead_bytes: 600 * MIB,
                ..watching(3, Some(90))
            }),
            Shape::Split {
                window: 166,
                committed: 67
            },
            "the window holds the 600 MiB an open stream is fetching, not the 90 seconds asked for"
        );
    }

    /// **With no rate measured, the byte arithmetic stands on its own.**
    ///
    /// A cap in seconds needs a rate to be a number of bytes, and there is
    /// none until a stream has delivered for a few seconds. Until then the
    /// shape is exactly what it was before any of this: half the budget to
    /// the window, half to sharing.
    #[test]
    fn a_stream_with_no_measured_rate_falls_back_to_the_budget() {
        assert_eq!(
            timed(Buffering {
                window_seconds: Some(90),
                committed_seconds: Some(90),
                ..Buffering::default()
            }),
            Shape::Split {
                window: 1280,
                committed: 1280
            },
            "no rate, no time cap: the budget halved, as before"
        );
    }

    /// **A rate measured after the policy was built re-shapes it in
    /// place.**
    ///
    /// The rate is not knowable at the open -- nothing has been delivered
    /// yet -- so the shape the first passes run under is the fallback one,
    /// and the pass is what folds the measurement in
    /// (`State::advance`). Nothing else about the policy is re-read: the
    /// budget is the publisher's, and a budget that moves builds a new
    /// policy.
    #[test]
    fn a_policy_re_shapes_when_a_rate_is_measured_under_it() {
        const MIB: u64 = 1 << 20;
        let piece = 4 * MIB;
        let mut p = RetentionPolicy::new(
            10 * 1024 * MIB,
            piece,
            0..4000,
            4000 * piece,
            Share::Half,
            Buffering {
                window_seconds: Some(90),
                committed_seconds: Some(90),
                ..Buffering::default()
            },
        )
        .expect("a consistent file");
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 1280,
                committed: 1280
            }
        );
        assert!(p.observe(watching(3, Some(90))), "the shape moved");
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 75,
                committed: 67
            }
        );
        assert!(
            !p.observe(watching(3, Some(90))),
            "and the same reading again moves nothing"
        );
    }

    /// **The window never reaches less far than a stream already open on
    /// the file is fetching.**
    ///
    /// The grant is cut to the window's forward reach at the open, so the
    /// floor is a no-op there by construction; what it is for is the
    /// *next* budget, republished sixty seconds later off a volume that has
    /// filled. That policy is built without the reader in view and used to
    /// be free to halve the window under it, leaving the stream fetching
    /// pieces the pass cannot take back -- the fork refuses to drop a piece
    /// inside `streams.wanted_ranges` -- so the disk sat over budget for
    /// the life of the stream and paid a refused `drop_pieces` per tick.
    #[test]
    fn the_window_covers_every_profiles_granted_lookahead_at_every_budget() {
        use crate::backend::priorities::{
            BufferProfile, PlaybackIntent, librqbit_stream_lookahead_bytes,
        };
        const MIB: u64 = 1 << 20;
        // The field device's piece length, and a file far longer than any
        // budget here so nothing is clamped against its ends.
        let piece = 4 * MIB;
        let pieces = 0..4000;
        let bytes = 4000 * piece;
        for profile in BufferProfile::ALL {
            // The most a playback reader can be granted under this profile.
            let cap = librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, profile);
            for budget_pieces in 1..=400u64 {
                let budget = budget_pieces * piece;
                let at_open = RetentionPolicy::new(
                    budget,
                    piece,
                    pieces.clone(),
                    bytes,
                    Share::Half,
                    Buffering::default(),
                )
                .expect("a consistent file");
                // What `Engine::try_get_file_with_intent` really hands the
                // stream: the intent's cap cut to the window's reach.
                let reach = at_open.ahead_of(2000);
                let granted = (u64::from(reach.end - reach.start) * piece).min(cap).max(1);
                // And the policy the next budget publication builds over
                // it, sixty seconds later on a volume that has filled: a
                // smaller budget, built without the open reader in view.
                let later = RetentionPolicy::new(
                    budget / 2,
                    piece,
                    pieces.clone(),
                    bytes,
                    Share::Half,
                    Buffering {
                        lookahead_bytes: granted,
                        ..Buffering::default()
                    },
                )
                .expect("a consistent file");
                let reach = later.ahead_of(2000);
                assert!(
                    u64::from(reach.end - reach.start) * piece >= granted,
                    "{profile:?} at {budget_pieces} pieces halved: a reach of {} for a lookahead of {granted}",
                    u64::from(reach.end - reach.start) * piece
                );
            }
        }
    }

    /// **The committed half yields to the floor, to nothing if it must.**
    ///
    /// Sharing is generosity and playback is the job. A window narrower
    /// than what an open stream is already fetching cannot come back under
    /// budget at all, so there is nothing to protect by keeping half the
    /// budget for a peer.
    #[test]
    fn the_committed_half_yields_to_the_windows_floor() {
        let buffering = Buffering {
            lookahead_bytes: 9 * PIECE,
            ..Buffering::default()
        };
        let roomy = RetentionPolicy::new(
            40 * PIECE,
            PIECE,
            0..400,
            400 * PIECE,
            Share::Half,
            buffering,
        )
        .expect("a consistent file");
        assert_eq!(
            roomy.shape(),
            Shape::Split {
                window: 20,
                committed: 20
            },
            "a reach of eighteen pieces already covers nine: the floor is a no-op"
        );

        let mut tight = RetentionPolicy::new(
            4 * PIECE,
            PIECE,
            0..400,
            400 * PIECE,
            Share::Half,
            buffering,
        )
        .expect("a consistent file");
        assert_eq!(
            tight.shape(),
            Shape::Split {
                window: 9,
                committed: 0
            },
            "the window takes the whole budget and then some, and the committed half is nothing"
        );
        let reach = tight.ahead_of(100);
        assert_eq!(
            u64::from(reach.end - reach.start) * PIECE,
            9 * PIECE,
            "and what it reaches is exactly the lookahead the stream was granted"
        );

        roomy.carry_into(&mut tight);
        let reach = tight.ahead_of(100);
        assert!(
            u64::from(reach.end - reach.start) * PIECE >= 9 * PIECE,
            "and carrying a roomier policy onto it does not shrink the reach"
        );
    }

    #[test]
    fn a_budget_that_covers_the_file_keeps_and_shares_all_of_it() {
        // The phone with 379 GB free, and every desktop.
        let mut p = policy(100, 20);
        assert_eq!(p.shape(), Shape::Whole);
        assert_eq!(p.shape().piece_budget(), None);
        assert_eq!(p.window_at(7), 0..20, "no split means no window either");

        let d = p.advance(7, &held(0..12));
        assert!(d.reclaim.is_empty(), "nothing is dropped when it all fits");
        assert_eq!(d.committed, (0..12).collect::<Vec<_>>());
        assert_eq!(p.advertised().len(), 12, "we seed everything we hold");

        // Including pieces that arrive later, and only once each.
        let d = p.advance(15, &held(0..20));
        assert_eq!(d.committed, (12..20).collect::<Vec<_>>());
        assert!(d.reclaim.is_empty() && d.withdrawn.is_empty());
        assert_eq!(p.advertised().len(), 20);
    }

    /// **A stream with no swarm spends the whole budget on the window, and
    /// that is a number and not a case.**
    ///
    /// A proxied URL response is not seeded: nothing will ever ask us for a
    /// chunk of it. A committed half for one would be half the cache held
    /// permanently -- never reclaimed, by the committed set's own rule --
    /// for a reader that does not exist, and the window playback actually
    /// needs would be half what the disk could give it. So the split is
    /// [`Share::Nothing`] and everything else about the policy is the same
    /// arithmetic the torrent gets.
    #[test]
    fn a_stream_nothing_can_be_shared_from_spends_the_whole_budget_on_the_window() {
        let shared = policy(10, 40);
        let alone = unshared(10, 40);
        assert_eq!(
            shared.shape(),
            Shape::Split {
                window: 5,
                committed: 5
            }
        );
        assert_eq!(
            alone.shape(),
            Shape::Split {
                window: 10,
                committed: 0
            },
            "twice the window, because none of it is being kept for a peer"
        );
        assert_eq!(
            shared.shape().piece_budget(),
            alone.shape().piece_budget(),
            "and the same budget: the split moved, the total did not"
        );
    }

    /// And it commits nothing, ever -- so nothing of it is exempt from the
    /// reclaim, whatever the playhead does. Played straight through, the
    /// disk holds a window and no more.
    #[test]
    fn an_unshared_stream_commits_nothing_and_holds_only_its_window() {
        let mut p = unshared(10, 40);
        let mut disk = BTreeSet::new();
        play(&mut p, &mut disk, 0..40);
        assert!(
            p.advertised().is_empty(),
            "there is no peer to have been told about any of it"
        );
        assert!(
            disk.len() <= 10,
            "{} pieces on disk, and the budget is 10",
            disk.len()
        );

        // The same walk on the sharing policy settles a permanent set, which
        // is exactly what a proxied stream must not do.
        let mut shared = policy(10, 40);
        let mut shared_disk = BTreeSet::new();
        play(&mut shared, &mut shared_disk, 0..40);
        assert!(!shared.advertised().is_empty());
    }

    /// The boundary is in bytes, because the last piece of a file is usually
    /// short: a budget one byte under the file's real size is still a split.
    #[test]
    fn the_whole_file_test_is_the_files_own_length_not_a_rounded_one() {
        let bytes = 19 * PIECE + 1;
        let exact = RetentionPolicy::new(
            bytes,
            PIECE,
            0..20,
            bytes,
            Share::Half,
            Buffering::default(),
        )
        .unwrap();
        assert_eq!(exact.shape(), Shape::Whole);
        let short = RetentionPolicy::new(
            bytes - 1,
            PIECE,
            0..20,
            bytes,
            Share::Half,
            Buffering::default(),
        )
        .unwrap();
        assert_ne!(short.shape(), Shape::Whole, "one byte short is not covered");
        // 19 pieces and a byte of budget buys 19 pieces: 10 window, 9 shared.
        assert_eq!(
            short.shape(),
            Shape::Split {
                window: 10,
                committed: 9
            }
        );
    }

    /// A budget of nothing still has to let the player read. It buys exactly
    /// the piece under the playhead and shares none of it.
    #[test]
    fn a_budget_of_nothing_keeps_the_piece_being_read_and_shares_none() {
        let mut p = policy(0, 20);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(p.shape().piece_budget(), Some(0));
        assert_eq!(
            p.window_at(7),
            7..8,
            "the bytes being read are not optional"
        );

        let d = p.advance(7, &held(0..20));
        assert!(d.committed.is_empty(), "nothing may be advertised");
        assert!(p.advertised().is_empty());
        assert_eq!(
            d.reclaim,
            (0..20).filter(|&x| x != 7).collect::<Vec<_>>(),
            "everything else goes back"
        );

        // And a budget of less than one whole piece is the same thing: the
        // conversion to pieces floors.
        let sub = RetentionPolicy::new(
            PIECE - 1,
            PIECE,
            0..20,
            20 * PIECE,
            Share::Half,
            Buffering::default(),
        )
        .unwrap();
        assert_eq!(
            sub.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(sub.window_at(0), 0..1);
    }

    /// The reach a stream is sized by is never empty and never past the
    /// file: a budget of nothing reaches the piece under the playhead
    /// (`Engine::fetch_bound` takes `end - 1`, and an empty reach would put
    /// that a piece behind the reader, or underflow at piece zero); at the
    /// file's tail it is cut at the last piece; a playhead the file does not
    /// contain is read as its last piece, as `window_at` reads it; and a
    /// policy shaped `Whole` reaches the whole rest of the file, so that a
    /// bound computed from it bounds nothing the file has.
    #[test]
    fn the_reach_is_never_empty_and_never_past_the_file() {
        let nothing = policy(0, 20);
        assert_eq!(nothing.window_at(7), 7..8);
        assert_eq!(
            nothing.ahead_of(7),
            7..8,
            "the piece being read is reached, however small the budget"
        );

        // Ten ahead of the playhead, one behind: the reach is the nine ahead
        // and the playhead's own, cut where the file ends.
        let split = policy(20, 200);
        assert_eq!(split.ahead_of(100), 100..109);
        assert_eq!(split.ahead_of(195), 195..200, "cut at the last piece");
        assert_eq!(split.ahead_of(199), 199..200);
        assert_eq!(
            split.ahead_of(250),
            199..200,
            "a playhead past the file reads as its last piece"
        );

        // A file that does not start at piece zero is measured in its own
        // pieces.
        let later = RetentionPolicy::new(
            2 * PIECE,
            PIECE,
            4..12,
            8 * PIECE,
            Share::Half,
            Buffering::default(),
        )
        .expect("a consistent file");
        assert_eq!(later.ahead_of(4), 4..5);
        assert_eq!(later.ahead_of(11), 11..12);
        assert_eq!(
            later.ahead_of(0),
            4..5,
            "a playhead before the file reads as its first piece"
        );

        let whole = RetentionPolicy::new(
            20 * PIECE,
            PIECE,
            0..20,
            20 * PIECE,
            Share::Half,
            Buffering::default(),
        )
        .expect("a consistent file");
        assert_eq!(whole.shape(), Shape::Whole);
        assert_eq!(whole.ahead_of(5), 5..20);
        assert_eq!(
            whole.ahead_of(0),
            0..18,
            "nine tenths of the whole file ahead of its start"
        );
    }

    #[test]
    fn the_window_is_ninety_percent_ahead_and_ten_behind() {
        let p = policy(20, 200);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 10,
                committed: 10
            },
            "the budget is halved, and the odd piece would go to the window"
        );
        assert_eq!(p.window_at(100), 99..109, "one behind, nine ahead");

        // A hundred pieces of window: ten behind, ninety ahead.
        let p = policy(200, 2000);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 100,
                committed: 100
            }
        );
        assert_eq!(p.window_at(1000), 990..1090);
    }

    /// At either end the window keeps its size and slides, rather than being
    /// cut in half by the edge of the file.
    #[test]
    fn a_playhead_at_either_end_gets_a_whole_window_anyway() {
        let p = policy(20, 200);
        let want = 10;

        assert_eq!(p.window_at(0), 0..10, "all of it ahead");
        assert_eq!(p.window_at(1), 0..10, "still pinned to the start");
        assert_eq!(p.window_at(2), 1..11, "and then it moves");

        assert_eq!(p.window_at(199), 190..200, "all of it behind");
        assert_eq!(p.window_at(198), 190..200);
        assert_eq!(p.window_at(197), 190..200, "197 - 1 behind = 196, slid up");

        for playhead in 0..200 {
            let w = p.window_at(playhead);
            assert_eq!(w.end - w.start, want, "the window never shrinks");
            assert!(w.contains(&playhead), "and always holds the playhead");
            assert!(w.end <= 200, "and stays inside the file");
        }

        // A playhead outside the file is clamped rather than refused: a stream
        // that has run off the end of its file is a bookkeeping mistake
        // upstairs, not a reason to stop deciding what to keep.
        assert_eq!(p.window_at(u32::MAX), p.window_at(199));
    }

    /// The window can be told to cover more pieces than the file has -- a file
    /// of one piece is the extreme -- and it is the file that wins.
    #[test]
    fn a_file_of_one_piece_is_the_whole_window_and_nothing_else() {
        // Not covered by the budget, so it splits, and the split is degenerate.
        let mut p = RetentionPolicy::new(
            PIECE / 2,
            PIECE,
            7..8,
            PIECE,
            Share::Half,
            Buffering::default(),
        )
        .unwrap();
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(p.window_at(7), 7..8);
        assert_eq!(p.window_at(0), 7..8, "clamped from below");
        assert_eq!(p.window_at(9), 7..8, "and from above");
        let d = p.advance(7, &held([7]));
        assert!(d.reclaim.is_empty() && d.committed.is_empty());

        // Covered by it, and the one piece is shared.
        let mut p =
            RetentionPolicy::new(PIECE, PIECE, 7..8, PIECE, Share::Half, Buffering::default())
                .unwrap();
        assert_eq!(p.shape(), Shape::Whole);
        let d = p.advance(7, &held([7]));
        assert_eq!(d.committed, vec![7]);
        assert!(d.reclaim.is_empty());
    }

    /// A window wider than the pieces actually on disk has nothing to give
    /// back: the sparse cache is the normal state early in a stream.
    #[test]
    fn a_window_wider_than_what_is_held_reclaims_nothing() {
        let mut p = policy(40, 200);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 20,
                committed: 20
            }
        );
        let d = p.advance(100, &held([98, 100, 103]));
        assert_eq!(d.window, 98..118);
        assert!(d.reclaim.is_empty(), "all three are inside it");
        assert!(
            d.committed.is_empty(),
            "and a piece inside the window has not been passed over yet"
        );
        // Nothing held at all is also fine.
        let d = p.advance(100, &BTreeSet::new());
        assert_eq!(
            d,
            Decision {
                window: 98..118,
                ..Default::default()
            }
        );
    }

    /// The window follows the playhead, a short scan back stays inside it,
    /// and what the window leaves behind goes -- unless it is one of the
    /// pieces this policy drew to share, which is kept and announced the
    /// moment we hold it.
    #[test]
    fn the_window_follows_the_playhead_and_gives_back_what_it_leaves() {
        let mut p = policy(20, 200);
        // window 10 (one behind, nine ahead), committed 10 -- and the ten
        // pieces of the draw, spread across the whole file.
        assert_eq!(
            p.chosen.iter().copied().collect::<Vec<_>>(),
            vec![2, 32, 33, 42, 53, 94, 142, 147, 170, 179]
        );
        let mut disk: BTreeSet<u32> = (0..40).collect();

        // Arriving on a warm cache. Piece 2 is in the draw and we hold it,
        // so it is committed and announced at once: a leftover cache is as
        // good a source for a piece we have decided to keep as playback is.
        // Everything else outside the window goes back -- 39 included, being
        // ahead of the window, because a piece we hold outside the window is
        // a piece we are not keeping.
        let d = p.advance(30, &disk);
        assert_eq!(d.window, 29..39);
        assert_eq!(
            d.committed,
            vec![2, 32, 33],
            "the drawn pieces we hold, inside the window and out of it alike"
        );
        assert_eq!(
            d.reclaim,
            (0..29)
                .filter(|piece| *piece != 2)
                .chain([39])
                .collect::<Vec<_>>()
        );
        for piece in &d.reclaim {
            disk.remove(piece);
        }
        assert_eq!(disk, held(29..39).into_iter().chain([2]).collect());

        // Playing on, a piece at a time. The window slides off the pieces
        // behind the playhead one by one, and none of those is in the draw,
        // so they go.
        let d = p.advance(31, &disk);
        assert_eq!(d.window, 30..40);
        assert_eq!(d.reclaim, vec![29], "the piece the window walked off");
        assert!(d.committed.is_empty());
        for piece in &d.reclaim {
            disk.remove(piece);
        }
        disk.extend(d.window.clone());

        // On through the two drawn pieces at 32 and 33.
        play(&mut p, &mut disk, 32..45);
        assert_eq!(
            *p.advertised(),
            held([2, 32, 33, 42]),
            "the pieces of the draw playback has reached, and no others"
        );

        // A scan back of a few seconds is served from the window itself.
        let d = p.advance(43, &disk);
        assert_eq!(d.window, 42..52);
        assert!(
            !d.reclaim.contains(&43),
            "the piece under the playhead is not a candidate"
        );
        assert!(
            d.reclaim.iter().all(|piece| !p.is_advertised(*piece)),
            "and nothing we announce is ever a candidate"
        );
    }

    /// A committed piece is never reclaimed, and a reclaimed piece is never
    /// advertised. Both directions, over a playthrough with seeks in it.
    #[test]
    fn nothing_outside_the_committed_set_is_ever_advertised() {
        let mut p = policy(30, 500);
        let Shape::Split { window, committed } = p.shape() else {
            panic!("a 500-piece file does not fit in 30 pieces");
        };
        assert_eq!((window, committed), (15, 15));
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        let mut ever_advertised: BTreeSet<u32> = BTreeSet::new();

        for playhead in (0..500).step_by(3).chain([12, 480, 3, 260, 499, 0]) {
            let d = p.advance(playhead, &disk);
            for piece in &d.reclaim {
                assert!(
                    !ever_advertised.contains(piece),
                    "piece {piece} was advertised and is now being reclaimed"
                );
                assert!(disk.remove(piece), "reclaiming a piece we do not hold");
            }
            // Everything left is held or wanted; the window fills in.
            disk.extend(d.window.clone());
            ever_advertised.extend(p.advertised().iter().copied());
            assert!(
                p.advertised().iter().all(|piece| disk.contains(piece)),
                "advertising a piece that is not on disk"
            );
            assert!(
                disk.len() <= (window + committed) as usize,
                "held {} pieces on a budget of {}",
                disk.len(),
                window + committed
            );
        }
        assert_eq!(p.advertised().len(), committed as usize);
    }

    /// Losing a committed piece behind our back takes it out of the set. It
    /// is the one thing that can shrink it, and the alternative is
    /// advertising a piece that is not there.
    ///
    /// **And it is the only one.** Nothing in this policy ever takes a
    /// piece we have announced back: there is no un-have in BitTorrent, so
    /// a piece we hide is still one a peer that has our Have may ask for,
    /// and the answer to such a request with the bytes gone is to hang up.
    #[test]
    fn a_committed_piece_we_have_lost_stops_being_advertised() {
        let mut p = policy(20, 200);
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        play(&mut p, &mut disk, 0..12);
        assert_eq!(
            *p.advertised(),
            held([2]),
            "the one piece of the draw playback has reached"
        );

        disk.remove(&2);
        let d = p.advance(11, &disk);
        assert_eq!(d.withdrawn, vec![2]);
        assert!(!p.is_advertised(2));
        assert!(p.advertised().is_empty());

        // It coming back puts it back: it is a piece of the draw and we hold
        // it, which is the whole of the rule.
        disk.insert(2);
        let d = p.advance(11, &disk);
        assert_eq!(d.committed, vec![2]);
        assert!(d.withdrawn.is_empty() && !d.reclaim.contains(&2));
        assert_eq!(*p.advertised(), held([2]));
    }

    /// The policy governs one file. A torrent's other files are held by
    /// somebody else's decision and must not be reclaimed by this one.
    #[test]
    fn pieces_outside_this_files_range_are_left_alone() {
        let mut p = RetentionPolicy::new(
            10 * PIECE,
            PIECE,
            100..200,
            100 * PIECE,
            Share::Half,
            Buffering::default(),
        )
        .expect("consistent");
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 5,
                committed: 5
            }
        );
        // The draw is over this file's pieces and nothing else's.
        assert_eq!(
            p.chosen.iter().copied().collect::<Vec<_>>(),
            vec![127, 142, 147, 170, 179]
        );
        let d = p.advance(150, &held([0, 5, 99, 100, 147, 150, 199, 200, 4000]));
        assert_eq!(d.window, 150..155);
        assert_eq!(d.committed, vec![147], "the one drawn piece we hold");
        assert_eq!(
            d.reclaim,
            vec![100, 199],
            "only pieces this file owns are considered at all"
        );

        // And a piece another file owns is never committed either, however
        // long it sits on the disk.
        let d = p.advance(155, &held([0, 5, 99, 147, 155, 200, 4000]));
        assert_eq!(d.window, 155..160);
        assert!(d.committed.is_empty() && d.reclaim.is_empty());
        assert!(!p.is_advertised(99) && !p.is_advertised(200));
    }

    /// **A drawn piece is committed the moment we hold it, and the pieces
    /// beside it in the same window are not.**
    ///
    /// Committed and the window are two protections over the same piece and
    /// both can apply at once. The old rule waited for the window to
    /// release a piece before committing it, and it had to: with membership
    /// decided by playback there was always a piece we might still reclaim,
    /// and announcing one of those is the advertise-then-refuse this whole
    /// module exists to avoid. With the set drawn up front there is nothing
    /// to take back, so a piece can be announced the moment it verifies --
    /// which is what an ordinary client does with every piece it completes,
    /// and it means peers learn we have those bytes *while* the viewer is
    /// watching, which is when the upload switch lets us serve them.
    #[test]
    fn a_drawn_piece_is_committed_as_soon_as_it_is_held() {
        let mut p = policy(20, 200);
        assert!(p.chosen.contains(&32) && p.chosen.contains(&33));
        assert!(!p.chosen.contains(&31) && !p.chosen.contains(&34));

        // A window covering 30..40, with nothing played through yet.
        let d = p.advance(31, &held(30..40));
        assert_eq!(d.window, 30..40);
        assert_eq!(
            d.committed,
            vec![32, 33],
            "announced from inside the window, while it still covers them"
        );
        assert!(
            !p.is_advertised(31) && !p.is_advertised(34),
            "and their neighbours in the same window stay held back"
        );

        // The window moves past all four. The two drawn ones stay; the two
        // beside them are reclaimed like any other window piece.
        let d = p.advance(45, &held(30..46));
        assert_eq!(
            d.committed,
            vec![42],
            "the next drawn piece the disk gained"
        );
        assert!(d.reclaim.contains(&31) && d.reclaim.contains(&34));
        assert!(!d.reclaim.contains(&32) && !d.reclaim.contains(&33));
        assert_eq!(*p.advertised(), held([32, 33, 42]));
    }

    /// **Nothing this policy has announced is ever un-announced or
    /// reclaimed while it stands.** The property the whole design of the
    /// committed set now turns on.
    ///
    /// BitTorrent has no un-have. Hiding a piece
    /// (`set_pieces_advertised`) changes only the bitfield a *new* peer is
    /// handed at its handshake; a peer that already has our Have can still
    /// ask for it, and the fork's upload path, finding the bytes gone, can
    /// only hang up -- there is no reject message. A client that does that
    /// repeatedly is an unreliable peer, and some clients snub or ban for
    /// it. So the only thing that ever takes a piece out of the set is the
    /// disk losing it behind our back, which this walk does not do.
    #[test]
    fn nothing_once_committed_is_ever_un_advertised_or_reclaimed() {
        let mut p = policy(30, 500);
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        let mut ever: BTreeSet<u32> = BTreeSet::new();
        for playhead in (0..500).step_by(3).chain([12, 480, 3, 260, 499, 0]) {
            let d = p.advance(playhead, &disk);
            assert!(
                d.withdrawn.is_empty(),
                "piece {:?} was un-announced with the disk intact",
                d.withdrawn
            );
            for piece in &d.reclaim {
                assert!(
                    !ever.contains(piece),
                    "piece {piece} was announced and is now being reclaimed"
                );
                disk.remove(piece);
            }
            disk.extend(d.window.clone());
            ever.extend(p.advertised().iter().copied());
            assert!(
                ever.iter().all(|piece| disk.contains(piece)),
                "a piece we announced is no longer on the disk"
            );
        }
        assert_eq!(
            *p.advertised(),
            ever,
            "everything ever announced is still announced"
        );
    }

    /// **Two clients over one file keep different pieces.**
    ///
    /// That is the whole reason the draw is random rather than a rule. A
    /// peer cannot see the swarm and cannot coordinate, so the only
    /// property available to it is independence: independent uniform draws
    /// by many peers sum to even coverage, where any shared rule -- keep
    /// the first minute, keep every k-th piece, keep the tail -- sums to a
    /// shared hole. Even and not biased late, because a shared late bias
    /// only moves the hole: if every client kept the tail, the tail becomes
    /// the over-replicated part and the head goes scarce.
    #[test]
    fn two_policies_over_one_file_draw_different_sets() {
        let draw = |seed: u64| {
            RetentionPolicy::new(
                20 * PIECE,
                PIECE,
                0..200,
                200 * PIECE,
                Share::Half,
                Buffering {
                    seed,
                    ..Buffering::default()
                },
            )
            .expect("a consistent file")
            .chosen
            .clone()
        };
        let ours = draw(0);
        let theirs = draw(1);
        assert_ne!(ours, theirs);
        assert_eq!(ours.len(), 10);
        assert_eq!(theirs.len(), 10);
        for set in [&ours, &theirs] {
            // Spread across the file rather than clustered anywhere: at
            // least one piece in each half, and a span covering most of it.
            assert!(set.iter().any(|piece| *piece < 100));
            assert!(set.iter().any(|piece| *piece >= 100));
            let span = set.last().expect("ten") - set.first().expect("ten");
            assert!(span > 100, "a draw spanning only {span} pieces");
        }
    }

    /// **A half-watched file fills about half the draw, and that is
    /// correct.**
    ///
    /// We commit what we hold, and we hold what the viewer's window
    /// fetched. Filling the rest would mean fetching bytes for the swarm
    /// rather than for the viewer, which on a metered phone is a trade
    /// nobody agreed to. An unfilled cache costs nothing.
    #[test]
    fn a_half_watched_file_fills_about_half_the_draw() {
        let mut p = policy(20, 400);
        let drawn = p.chosen.len();
        assert_eq!(drawn, 10);
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        play(&mut p, &mut disk, 0..200);
        assert_eq!(
            p.advertised().len(),
            5,
            "five of the ten lie in the half that was watched, read-ahead included"
        );
        play(&mut p, &mut disk, 200..400);
        assert_eq!(
            p.advertised().len(),
            drawn,
            "and a full watch fills the draw"
        );
    }

    /// Asking twice with nothing changed must not change anything the second
    /// time: a reclaim is a request, and it is the caller deleting the piece
    /// that makes it true.
    #[test]
    fn a_second_pass_over_the_same_disk_decides_the_same_thing() {
        let mut p = policy(20, 200);
        // With a release in the first of the two passes, so that "the same
        // thing" is not trivially "nothing".
        let mut disk: BTreeSet<u32> = (0..10).collect();
        p.advance(0, &disk);
        disk.extend(10..40);
        let first = p.advance(10, &disk);
        let second = p.advance(10, &disk);
        assert_eq!(first.window, second.window);
        assert_eq!(first.reclaim, second.reclaim);
        assert_eq!(
            first.committed,
            vec![32, 33],
            "the drawn pieces the disk gained between the two playheads"
        );
        assert!(
            second.committed.is_empty(),
            "already committed, and a commit is a transition"
        );
        assert!(second.withdrawn.is_empty());
    }

    #[test]
    fn a_file_whose_numbers_do_not_agree_is_refused() {
        // 20 pieces of 1000 hold between 19001 and 20000 bytes.
        assert!(
            RetentionPolicy::new(0, PIECE, 0..20, 20_000, Share::Half, Buffering::default())
                .is_ok()
        );
        assert!(
            RetentionPolicy::new(0, PIECE, 0..20, 19_001, Share::Half, Buffering::default())
                .is_ok()
        );
        let err = RetentionPolicy::new(0, PIECE, 0..20, 19_000, Share::Half, Buffering::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("between 19001 and 20000"), "{err}");
        assert!(
            RetentionPolicy::new(0, PIECE, 0..20, 20_001, Share::Half, Buffering::default())
                .is_err()
        );
        assert!(
            RetentionPolicy::new(0, 0, 0..20, 20_000, Share::Half, Buffering::default()).is_err(),
            "zero piece length"
        );
        assert!(
            RetentionPolicy::new(0, PIECE, 5..5, 0, Share::Half, Buffering::default()).is_err(),
            "no pieces"
        );
    }

    /// The halves sum to the budget, the window never exceeds its half, and
    /// nothing held ever exceeds the two together -- over every budget from
    /// nothing to more than the file.
    #[test]
    fn the_split_holds_for_every_budget() {
        let count = 64u32;
        let bytes = u64::from(count) * PIECE;
        for budget_bytes in (0..=bytes + 2 * PIECE).step_by(PIECE as usize / 8) {
            let mut p = RetentionPolicy::new(
                budget_bytes,
                PIECE,
                0..count,
                bytes,
                Share::Half,
                Buffering::default(),
            )
            .unwrap();
            match p.shape() {
                Shape::Whole => assert!(budget_bytes >= bytes),
                Shape::Split { window, committed } => {
                    assert!(budget_bytes < bytes);
                    assert_eq!(
                        u64::from(window + committed),
                        budget_bytes / PIECE,
                        "the halves must sum to the budget"
                    );
                    assert!(window >= committed, "the odd piece goes to the window");
                }
            }

            let mut disk: BTreeSet<u32> = BTreeSet::new();
            for playhead in (0..count).chain((0..count).rev()) {
                let d = p.advance(playhead, &disk);
                for piece in &d.reclaim {
                    disk.remove(piece);
                }
                disk.extend(d.window.clone());
                let ceiling = match p.shape() {
                    // One piece over, and only when the budget is nothing:
                    // the window is never empty.
                    Shape::Whole => count as usize,
                    Shape::Split { window, committed } => (window.max(1) + committed) as usize,
                };
                assert!(
                    disk.len() <= ceiling,
                    "budget {budget_bytes}: held {} of a ceiling of {ceiling}",
                    disk.len()
                );
                assert!(p.advertised().iter().all(|piece| disk.contains(piece)));
            }
        }
    }
}
