//! One owner for retention: the policy never leaves its cell, and one party
//! at a time changes the world it describes.
//!
//! Both retention drivers -- `Engine::retain` over a torrent's piece store
//! and `server::proxy_retention` over a proxied body's chunk store -- run the
//! same arithmetic ([`RetentionPolicy`]) through this one owner, and the
//! defect it is built against is **a value read at one moment, trusted at
//! another**. The policy is the case that matters: a pass that took it out
//! of its slot for its own length and died at an await -- the runtime
//! shutting down, the blocking pool refusing a task, a panic in the unlink
//! closure -- would walk off with it, and the entity would be unbounded
//! until the budget's *value* changed.
//!
//! **What an entity shares is decided once and only ever added to the
//! backend.** The viewer's playback stream draws it once, as soon as its
//! read-ahead is known ([`State::draw`], [`Retention::install_opening`],
//! [`DRAW_FALLBACK`]), and advertises it through [`Backing::advertise`], the
//! one direction there is: nothing here takes an announcement back, and a
//! [`Mode::Slack`] pass deletes nothing a peer may have been told of while
//! the backend is in the swarm ([`Backing::announced_in_swarm`]). Ending an
//! announcement is the driver's -- the torrent's reconciler stops it first
//! (`EndShares`).
//!
//! Here the policy is resident in [`State::installed`], behind a lock that
//! is never held across an await, and it is never taken out: a pass advances
//! it in place. What a pass holds instead is the entity's **turn**, a tokio
//! mutex over a zero-sized [`Turn`] token, one per entity on both sides.
//! Whoever holds the turn is the one
//! party installing, clearing or passing on that entity, and the turn *is*
//! held across that party's I/O, because that is what keeps "nothing of
//! this entity becomes advertised between the decision and the unlink"
//! true.
//! The guard is the [`Claim`], and a dead pass drops it like any other
//! local: the entity is passable again, and the policy is still in its cell.
//!
//! # Lock order
//!
//! Locks, outermost first:
//!
//! * **L1** [`Retention::entities`] (`parking_lot::Mutex`) -- lookup, insert,
//!   prune and iterate-for-holdings.
//! * **L2** [`Entity::state`] (`parking_lot::Mutex`) -- every datum,
//!   including the resident [`RetentionPolicy`].
//! * **T** [`Entity::turn`] (`tokio::sync::Mutex<Turn>`) -- the entity's turn.
//!   Protects no memory; orders this process's changes to the world outside
//!   it (what librqbit advertises, what the directory holds). The ONLY thing
//!   ever held across an await.
//! * **X** -- locks outside the owner: `pinned_files`, [`RetentionBudget`],
//!   the liveness cell, librqbit's own, the filesystem.
//! * **S** [`Retention::set_draws`] (`parking_lot::Mutex`) -- one set's
//!   draw decided at a time ([`Backing::played_set`]). Taken under T, with
//!   no owner lock held, and held with no await: under it L1 to copy the
//!   set's entities out, each sibling's L2 alone and released, then the
//!   deciding entity's own L2. Outermost of the three; nothing takes it
//!   under L1 or L2.
//!
//! 1. L1 → L2 only, and only inside [`Retention::holdings`],
//!    [`Retention::forget_empty`], `drawn_in`, [`Retention::draws`],
//!    [`Retention::draws_for`], [`Retention::withdraw_from_draws_made_for`]
//!    and [`Door::drawn`], which take L1 and read
//!    each entity's L2 under it; never L2 → L1. An entity holds its own `Arc` and never
//!    reaches the map. (Every other reader of the map --
//!    [`Retention::readers`], `lookup`, `entity` -- copies the `Arc`s out
//!    and releases L1 before touching any L2.)
//! 2. No L1 or L2 guard is ever live across an await, a tracing macro, a
//!    `&self` method of [`Backing`] or any X. The pure associated functions
//!    of [`Backing`] -- `governs`, `extent`, `policy`, `index_of` -- ARE
//!    called under L2 (`policy` from [`Reader::note`]'s decide, `index_of`
//!    from the [`Door`] and every head reading) and must take no lock and do
//!    no I/O. [`Backing::policy`] returns its `Err` as a value and the
//!    caller logs after unlock. [`Backing::keeps_everything`]
//!    (`pinned_files.read()`) is asked before L2 is taken, in the [`Door`]
//!    and in the pass.
//! 3. T is awaited (`lock().await`) only with NO owner lock held:
//!    [`Retention::pass`]'s callers, [`Retention::install`] and
//!    `Retention::clear` (test builds only) take it first. T → L2 briefly is allowed.
//!    `try_lock` on T IS allowed under L2 -- deliberately:
//!    "is a pass running" and "is this byte due" must be one reading, and
//!    **every exit of the pass** must decide `again` and either hand the
//!    claim on or drop it INSIDE its L2 block. Releasing L2 before the
//!    `try_lock`, or the claim after L2, reopens the swallowed-last-byte
//!    hole: a byte delivered between the conclusion and the release finds a
//!    pass "running", starts none, and nothing remembers that it wanted
//!    one. The one `try_lock` under L2 is [`Reader::note`]'s, on an entity
//!    it already holds; [`Retention::try_turn`] looks its entity up under
//!    L1 and is for callers holding nothing.
//! 4. T is per entity and never nested: every party takes one turn, does
//!    its work and releases it before taking another, and nothing here
//!    touches two entities in one act -- what makes a sibling slack is the
//!    liveness value ([`crate::retention::live`]), not an install. The one
//!    reading across entities is a set's draw (under S): a volume adopts
//!    the draw a sibling already holds, read under the sibling's L2 and
//!    copied out, and writes only its own state. The one write across a
//!    set's entities is a delete's ([`Retention::withdraw_from_draws_made_for`]),
//!    which takes each volume's turn in turn -- released before the next --
//!    and S under it, as a draw does.
//! 5. Writes to `installed`, `windows`, `stride` and the in-place advance of
//!    the policy require `&mut Turn`, so "written only under the turn" is a
//!    type -- with one documented exception: [`State::install_now`]
//!    ([`Install::OnDeliveredByte`]) writes `installed`, `decided` and
//!    `stride` and resets every reader's `passed_at` under L2 alone from
//!    [`Reader::note`] while a pass may hold T. Legal only because [`Share::Nothing`] holds nothing back (no
//!    foreign state to keep in step; const-asserted in [`Retention::new`]),
//!    and it is why the pass re-checks budget and domain at the post-listing
//!    re-read and before writing its windows. `decided` has no turn-writer:
//!    it is written by `install_now` and cleared with the policy.
//!
//! **Why it is deadlock-free.** L1 and L2 form a strict two-level order with
//! no awaits inside, so they wait only on each other and in one direction.
//! T is acquired only by tasks holding no owner lock, never nested with
//! another T; its holder takes only L2 (which never waits) and X (from
//! [`Backing`] calls, with no owner lock held). The two writers
//! that must never wait on T -- [`Reader::note`] /
//! [`Retention::note_position`] on every delivered byte, and pin writes --
//! touch only L1 to copy out, L2 and X, which is what the torrent's
//! parked-pass tests exercise: a pass parked inside a backend call under T
//! (`reselect_gate`, `advertise_gate`) while a note and a pin land.
//!
//! # The pass
//!
//! Two passes, and which one runs is the driver's, from one reading of what
//! is being played ([`Mode`]). [`Mode::Slack`] is the short one, written
//! out on [`Retention::slack_pass`]: re-establish that it really is slack
//! under the turn, take nothing a peer may have been told of while the
//! backend is in the swarm, drop the policy, the windows and the draw, take
//! every byte off the disk, forget the entity if nothing is left. The re-establishing is the first step and not a formality -- the
//! driver's reading is older than the turn by every unlink it has done
//! since, and this is the pass that deletes an entity whole. What follows
//! is [`Mode::Live`].
//!
//! One body with two entries. [`Retention::turn`] queues on T for the
//! torrent's tick and the proxy's slack sweep; [`Reader::note`] claims T
//! with a `try_lock` when a delivered byte moved a stride, which is the
//! proxy's trigger. Both hand a [`Claim`] to [`Retention::pass`], whose steps are:
//!
//! 1. [`Backing::keeps_everything`] → clear, conclude nothing.
//! 2. Snapshot heads and promises under L2.
//! 3. Test hook.
//! 4. The listing ([`Backing::held`]), with no owner lock held.
//! 5. RE-READ under L2, and REFUSE if the resident policy's budget or
//!    domain differs from the snapshot -- a decide ran under us. Otherwise
//!    advance the resident policy in place and build the windows.
//! 6. Nothing to announce -- the draw was advertised when it was decided
//!    (at an open, or at step 1a of a pass) and the backend announces each
//!    piece of it as it completes -- and nothing to
//!    withdraw; the draw is held in the published set so no unlink of the
//!    pass may take it. Then, no L2 held,
//!    [`Backing::want`], which trims what the backend fetches to the
//!    *want*-windows and what is on the disk, asking the same [`Door`] the
//!    reclaim asks before it unlinks anything that arrived under the pass.
//!    Two window lists come out of step 5, not one: every reader is owed a
//!    window that is not deleted under it, and what is fetched is what the
//!    detector's consumers are asking for.
//! 7. Test hook.
//! 8. [`Backing::reclaim`] with a [`Door`] that answers [`Door::shut`] from
//!    the backing and [`Door::refuses`] from the set this pass published.
//! 9. Conclude under L2: `passed_at` and the windows only if the budget and
//!    domain are still the ones measured, and `again` decided from the same
//!    `head` the pass measured from, with the SAME claim handed back to the
//!    driver.
//!
//! Every early return leaves the policy exactly where it is, and every one
//! of them -- the pin, nothing installed, the failed listing, the refused
//! re-read -- decides `again` under L2 the way step 9 does, with the claim
//! dropped or handed on inside that block ([`State::owes_a_pass`]). There
//! is no `abandon` and no `put_back`, because nothing was taken out.
//!
//! # Rules the steps above do not spell out
//!
//! * [`Retention::try_turn`] never waits, but it looks its entity up under
//!   L1, so it is NOT legal under L2 (L2 → L1 inverts rule 1); the
//!   `try_lock` rule 3 allows under L2 is [`Reader::note`]'s on an entity it
//!   already holds.
//! * `again` is decided on every exit of the pass, not only at step 9, and a
//!   pass refused at its re-read (or overtaken during its reclaim) hands the
//!   claim on outright rather than asking the stride rule of a head it never
//!   measured: step 9 alone would swallow a last byte delivered under a
//!   budget change; see [`Retention::pass`].
//! * `passed_at` is written only when the windows are (budget and domain
//!   unchanged): writing it regardless would undo the "every reader is due
//!   again" a publication promises.
//! * `clear` forgets `decided` with the policy. Without it a cleared
//!   [`Install::OnDeliveredByte`] entity is never decided again until the
//!   budget's value changes.
//! * The windows are written only if budget AND domain are unchanged. The
//!   domain check is dead as things stand, because only `install`, under the
//!   turn, writes the domain; it is kept because it is the conservative
//!   direction.
//! * Step 2 snapshots `{domain, budget}` and the head's piece, and reads
//!   heads and promises afresh at step 5 with no fallback to a step-2 copy:
//!   an entity cannot vanish under a pass, because the pass holds its `Arc`.
//! * [`Backing`] has a `Sized` bound (RPITIT with `Door<Self>` needs it).
//! * `install` creates the entity for a fresh key before its checks, so an
//!   install that answers `Unbounded` (a pin, a budget that covers the
//!   file) leaves an entity with nothing installed in the map until a
//!   slack pass finds it holding nothing ([`Retention::forget_empty`]) --
//!   one small struct per file ever installed on.
//! * An entity left unbounded wants its whole extent again
//!   ([`Backing::want_all`]). Its passes trim the backend's want-set to the
//!   window ([`Backing::want`] at step 6), and a dropped piece stays dropped
//!   until something wants it; a pin on a single-file torrent changes no
//!   selection, so without this the download the user asked to keep would
//!   stop at the window's edge. It is asked by the pass under a pin and by
//!   an install that ends with nothing installed -- not by every clear: an
//!   install that replaces one policy with another has a window to trim to
//!   at its next pass, and a file the reader left is [`Mode::Slack`], with
//!   its pieces on their way off the disk rather than something to fetch
//!   whole.
//! * Two policies on one torrent are legal: each file is its own entity
//!   with its own head, its own window and its own [`Mode`], and the one
//!   that is neither played nor read is the one whose bytes go.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::piece_store::{Buffering, Decision, RetentionPolicy, Shape, Share};
use crate::retention::{CacheBudget, RetentionBudget, runs, trace};

/// What starts a pass over an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// A delivered byte that moved its reader a stride, where the stride is
    /// the window over `passes_per_window`, at least one piece. The proxy:
    /// a proxied entity grows only as its own body is relayed, so the byte
    /// that grows it is the byte that should measure it, and twenty
    /// listings per window of playback bound the overshoot to a twentieth
    /// of the budget.
    OnMove { passes_per_window: u64 },
    /// Somebody takes the turn and calls the pass -- the reconciler's tick.
    /// The torrent: the swarm fills its cache whether or not anyone reads,
    /// so a delivered byte is not the event that grows it.
    External,
}

/// When a policy is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Install {
    /// Inside [`Reader::note`], the moment the published budget differs from
    /// the one the entity was decided under. Legal only with
    /// [`Share::Nothing`]: swapping a policy that shares nothing needs no
    /// backend call, so it can happen under L2 alone while a pass holds the
    /// turn -- see rule 5 in the module docs.
    OnDeliveredByte,
    /// By [`Retention::install`], before the reader opens, so the play
    /// session's read-ahead is known to the policy before a piece of it
    /// arrives, and what the session shares is sized beside it.
    OnOpen,
}

/// Who opened an entity, as far as what it shares goes
/// ([`Retention::install_opening`]).
///
/// **Only the viewer's playback stream shares anything** -- a request
/// carrying the player's token (`p=`). Everything else
/// that reads a torrent's file -- a subtitle fetched beside the film, a
/// volume of an archive a translated source is reading, a pin's adoption
/// -- draws nothing and announces nothing: its bytes are reclaimed as any
/// unshared bytes are, and nothing it did can make the torrent leave the
/// swarm under the film.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opener {
    /// The viewer's player, by its token. Its play session draws a shared
    /// set while a session is on the entity ([`Backing::plays`]) and once
    /// its read-ahead is known ([`DRAW_FALLBACK`]).
    Player,
    /// Anything else. Draws nothing.
    Unshared,
}

/// **What a play session's draw was made for**: the member of the entity
/// the session played ([`Backing::played_member`], `None` for the entity as
/// itself) and the set it played the entity as a volume of
/// ([`Backing::played_set`]). Recorded with the draw
/// ([`State::drawn_for`]), because a draw is made for a member: a session
/// that moves to another member of the same container -- the next episode
/// of a season in one ZIP -- plays a different film in the same entity, and
/// the first member's draw is nothing it shares. The driver counts a draw
/// as shared only while what it was made for is what is played now
/// ([`Retention::draws_for`]), and a set's volume adopts a sibling's draw
/// only when it was made for this set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrawnFor<K> {
    pub member: Option<Range<u64>>,
    pub set: Option<Vec<(K, Range<u64>)>>,
}

/// L1: the entities by key ([`Retention::entities`]).
type Entities<B> = parking_lot::Mutex<HashMap<<B as Backing>::Key, Arc<Entity<B>>>>;

/// One entity's draw as [`Retention::draws_for`] lists it: the key, the
/// pieces and what they were drawn for.
pub type DrawMade<K> = (K, BTreeSet<u32>, DrawnFor<K>);

/// **How long a play session waits for the film's rate before it draws
/// anyway**, counted from the first pass that finds playback under way (a
/// delivered byte) and the draw still undecided.
///
/// The draw has to be sized beside the stream's real read-ahead, which is
/// the film's bitrate times the seconds the viewer asked for, and the
/// bitrate is the film's size over the duration the player states
/// ([`Retention::note_duration`]) -- which it does only once it has opened
/// the media, after the first open. Drawn at that open, the set was sized
/// beside the intent's placeholder (32 MiB) while every later open was
/// granted the real read-ahead, hundreds of megabytes for a 4K film, and the
/// disk sat over its budget by the difference for the whole session. So the
/// draw waits for the rate, which costs nothing: under the announce-nothing
/// default nothing of the file is announced before it. A player that never
/// states a length (a client other than the app) would share nothing at
/// all, so after this much playback the draw is made with what the opens
/// stated.
pub const DRAW_FALLBACK: std::time::Duration = std::time::Duration::from_secs(10);

/// What a pass is for: keeping the entity's window, or taking the entity
/// off the disk.
///
/// The driver decides it, from one reading of what is being played
/// ([`crate::retention::live`]), and hands it to [`Retention::pass`]. It is
/// not a property of the entity and nothing here remembers it: the same
/// entity is live this tick and slack the next because a player opened
/// something else, and the tick after that it is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The entity is being played, or an open read is still delivering it.
    /// The pass keeps the window round the head, commits what the window
    /// has released, trims the want-set to the window and reclaims the
    /// rest.
    Live,
    /// Nothing is playing it and nothing is reading it: its play session is
    /// over. Once nothing it holds is announced in the swarm, the pass takes
    /// every byte of it off the disk. Nothing is kept: an entity that has
    /// been left is not a small cache, it is disposable.
    ///
    /// `opens` is [`Retention::opens_of`] as the driver read it when it
    /// decided this, and it travels with the mode so the pass can find out
    /// whether the decision is still true. A stream opening on the entity
    /// is [`Retention::install`], which takes the entity's turn -- so an
    /// open that lands after this reading and before the pass takes that
    /// turn is one the pass would otherwise delete under, and one that
    /// arrives later cannot get in at all. The count is compared and not
    /// re-read: asking "is it live now?" again answers about a moment that
    /// has already passed, and comparing two readings answers about the gap
    /// between them, which is the thing that can hurt.
    Slack { opens: u64 },
}

/// What a pass can tell the detector that the detector cannot see.
///
/// Every one of them is a *published* number rather than a measured one:
/// the cap the operator configured, what the volume will still give, the
/// file's size over its duration, and how many seconds of stream the
/// viewer's buffer profile buys. See `docs/design/read-pattern-retention.md`.
#[derive(Debug, Clone)]
pub struct Asking {
    /// What the whole cache may hold.
    pub budget: CacheBudget,
    /// What the volume will still give before the margin, or `None` for a
    /// volume nothing could read.
    pub headroom: Option<u64>,
    /// The file's own bitrate -- size over duration -- or `None` for a file
    /// whose length nobody has stated.
    pub ceiling: Option<u64>,
    /// How many seconds of stream a window may buy. Never "no cap": the
    /// `Maximum` profile, and an entity no reader has stated a profile for,
    /// are [`crate::backend::priorities::MAXIMUM_WINDOW_SECONDS`], a
    /// finite number the sharing arithmetic can multiply without
    /// saturating.
    pub seconds: u64,
    /// **What the pass knows must be kept whatever the consumers want**:
    /// every promise a parked read is holding, and the lookahead each open
    /// stream was granted -- the backend refuses to forget a piece inside
    /// one, so asking is a reclaim that is refused and a disk that grows.
    ///
    /// Handed *in* rather than added afterwards, because what may not be
    /// unlinked and what is offered up have to be the same reading: a
    /// reclaim chosen against a smaller set than the door refuses frees
    /// nothing at all.
    pub holding: Vec<Range<u32>>,
    /// **The committed set**, as runs: the pieces the policy has offered a
    /// peer and will never reclaim (`RetentionPolicy::advance` vetoes
    /// them). Inside the allowance and outside the reclaim, so the disk
    /// settles at the cap and not at the cap plus the committed set; see
    /// [`Self::allowance`].
    pub committed: Vec<Range<u32>>,
    /// **How many bytes the fill may put on the disk between two passes**,
    /// which is the room the allowance has to leave for it.
    ///
    /// What is on the disk when a pass measures it is what the consumers
    /// asked for plus whatever arrived since the last pass, which is a
    /// stride. An allowance that spent the whole budget would therefore sit
    /// a stride *over* it for as long as anything is downloading -- and the
    /// budget's own job is to keep the volume off the free-space floor, so
    /// being over it is the one thing it may not be. See
    /// `docs/design/read-pattern-retention.md` section 4 on why the margin is
    /// load-bearing: eviction has to trigger on approaching the line, not
    /// on crossing it.
    pub margin: u64,
    /// **The one reading of the clock this pass makes**, taken on the far
    /// side of the listing. Every backing's `reading` measures against
    /// this -- idle streams, the report throttle, the LRU -- rather than
    /// reading the clock inside the rule it then compares, which is the
    /// shape of bug that never converges.
    pub now: std::time::Instant,
}

impl Asking {
    /// **What this entity may hold**, given `held` bytes on its disk: what
    /// it holds now plus what the volume will still give before the
    /// margin, under the configured cap -- less the room the fill needs
    /// between two passes.
    ///
    /// `docs/design/read-pattern-retention.md` section 4. Its own usage has to be
    /// in there or the allowance shrinks as the cache fills and never
    /// converges -- a stream would stop well short of the disk with nothing
    /// to explain why. The cap is over the whole cache and is what an
    /// entity is bounded by when there is no reading of the volume at all;
    /// where there is one, the smaller of the two, like every other reading
    /// of this: the volume said 381 GB free on the field's phone against a
    /// configured 10.7 GB, and an allowance that took the disk's word alone
    /// let one entity's want set grow to the whole film. A budget nobody
    /// has stated yet, over a volume nothing has read, is not a licence to
    /// want everything: every stream falls to its floor, which is what a
    /// stream with no measurement gets anyway.
    ///
    /// **Less the committed set.** Those pieces are on the disk and stay
    /// there whatever the consumers ask for -- a piece offered to a peer is
    /// out of reach of every reclaim -- so windows sized to an allowance
    /// that did not count them let the disk settle at the cap *plus* the
    /// committed set, by up to `COMMITTED_SECONDS` of film. Masked under
    /// the Normal profile, whose margin happened to be as large; exposed
    /// under Maximum.
    ///
    /// **This is what the windows may spend, not where the reclaim
    /// starts.** The committed pieces are in `held` already, so measuring
    /// the overhang against this took them off twice and the disk settled
    /// at the cap less the committed set -- scrub-back given up early, for
    /// nothing. The reclaim reads [`Self::overhang`].
    ///
    /// One computation for every backing, so the scenarios test the same
    /// arithmetic the torrent and the proxy run. `piece` is the piece
    /// length and `held` how many pieces the listing found.
    ///
    /// Only `Bytes` reaches this in the shipped drivers: a policy is
    /// installed from a `Bytes` budget alone, and no policy means no pass.
    /// The other arms are the shapes the type admits, answered the safe way
    /// round -- nothing stated over nothing read is not a licence.
    pub fn allowance(&self, piece: u64, held: usize) -> u64 {
        self.disk(piece, held)
            .saturating_sub(self.committed_pieces().saturating_mul(piece))
    }

    /// **How many bytes of the `held` pieces must go**: what the disk
    /// holds over what it may hold before the margin. Where the disk
    /// settles is therefore the cap less the margin, committed pieces
    /// included -- they are part of what is held, and part of what may be.
    pub fn overhang(&self, piece: u64, held: usize) -> u64 {
        (held as u64)
            .saturating_mul(piece)
            .saturating_sub(self.disk(piece, held))
    }

    /// What the whole entity may hold on the disk, committed set and all:
    /// the cap or the volume, less the margin the fill needs.
    fn disk(&self, piece: u64, held: usize) -> u64 {
        self.within(piece, held, self.margin)
    }

    /// [`Self::disk`] with the margin stated: what the entity may hold
    /// while leaving `margin` bytes for what arrives before the next pass.
    /// What the room an asker may fill is measured from, with the margin
    /// that is not the asker's own ([`Retention::ask_ahead`]).
    pub fn within(&self, piece: u64, held: usize, margin: u64) -> u64 {
        let held = (held as u64).saturating_mul(piece);
        let available = match (self.budget, self.headroom) {
            (CacheBudget::Unbounded, _) => u64::MAX,
            (CacheBudget::Bytes(cap), Some(headroom)) => cap.min(held.saturating_add(headroom)),
            (CacheBudget::Unknown, Some(headroom)) => held.saturating_add(headroom),
            (CacheBudget::Bytes(cap), None) => cap,
            (CacheBudget::Unknown, None) => 0,
        };
        // An allowance that spent the whole budget would sit a stride over
        // it for as long as anything is downloading.
        available.saturating_sub(margin)
    }

    /// How many pieces the committed set holds.
    fn committed_pieces(&self) -> u64 {
        self.committed
            .iter()
            .map(|run| u64::from(run.end.saturating_sub(run.start)))
            .sum()
    }
}

/// What the consumers of one entity are asking of its disk.
#[derive(Debug)]
pub struct Consumers {
    /// What the streams want fetched ahead of them, in pieces.
    pub want: Vec<Range<u32>>,
    /// One bit per piece: what no unlink may touch. Read at the door
    /// without taking any lock -- see [`crate::retention::exempt`].
    pub exempt: std::sync::Arc<crate::retention::exempt::Exempt>,
    /// What to give back first, coldest by effective age, and only as much
    /// as the allowance asks for. Empty while what is held fits: the disk
    /// that is not needed for anything else is scrub-back, and giving it up
    /// early buys nothing and costs a re-fetch.
    pub reclaim: Vec<u32>,
    /// **The piece the entity is being consumed at**: where the detector's
    /// busiest stream has reached, and `None` for a backing with no
    /// detector or an entity nothing has read lately.
    ///
    /// *Who* the viewer is, among several readers of one file, is a
    /// question about behaviour and the detector is what answers it: mpv
    /// keeps a second reader crawling the container's index for as long as
    /// a film is open, reopening about once a second, so the newest read
    /// and the longest-lived read are both regularly that crawler rather
    /// than the viewer. Not told from the geometry of a range header: the
    /// two reads look identical from the wire, measured at 91 B/s for the
    /// crawler against 1.2 MB/s for the viewer.
    ///
    /// **It is a cadence and a reporting number, not a retention one.**
    /// What is kept, fetched and given back comes from [`Self::want`],
    /// [`Self::exempt`] and [`Self::reclaim`], each of which is about every
    /// stream and not just this one.
    pub at: Option<u32>,
}

/// The world outside the owner, for one kind of entity.
///
/// Small on purpose: describe the entity's index space, list what the disk
/// holds, hold pieces back from what we advertise and put them back, and
/// reclaim a set of runs through a [`Door`] the owner supplies. The owner
/// does the arithmetic, the two re-readings, the door, the throttle, the
/// grace and the budget comparison. **No `&self` method is ever called with
/// an owner lock held**, so an implementation may take any lock it likes in
/// them, including the entity's own state lock through a [`Reader`]. The
/// pure associated functions -- [`Self::governs`], [`Self::extent`],
/// [`Self::policy`], [`Self::index_of`] -- ARE called under L2 and must take
/// no lock and do no I/O (rule 2).
///
/// The `async` methods return `Send` futures because the proxy's driver
/// spawns the pass as a task; an implementation writes them as `async fn`
/// and the compiler checks the bound.
pub trait Backing: Sized + Send + Sync + 'static {
    /// Names one entity: the proxy's chunk directory path; a torrent file's
    /// index.
    type Key: Clone + Eq + Hash + Send + Sync + Debug;
    /// Where a reader is, in the backing's own coordinates: the proxy's
    /// byte offset; the torrent's `(file_idx, offset_in_file)`.
    type Position: Copy + Send + Sync + Debug;
    /// What a policy is over, fixed for the entity's life: the proxy's
    /// directory, total and target; the torrent's file, span and piece
    /// length. Compared at the pass's re-read, so a domain that changed
    /// under a listing refuses the decision.
    type Domain: Clone + PartialEq + Send + Sync;
    /// What [`Retention::install`] is asked for: the proxy's `()`; the
    /// torrent's file index.
    type Want: Copy + Send + Sync;
    /// What a pass needs handed to it and never owns: the proxy's `()`; the
    /// torrent's store root, handed to the pass as `Engine::retain` is handed
    /// one.
    type Store: Sync;
    /// How the budget is split. [`Share::Nothing`] says [`Self::advertise`]
    /// is unreachable from the pass: the committed set has capacity zero, so
    /// both lists it would be called for are empty by construction.
    const SHARE: Share;
    const TRIGGER: Trigger;
    const INSTALL: Install;

    /// Resolve what `want` names, or `None` when it names nothing that can
    /// be bounded -- a torrent with no metadata. May do I/O; called with the
    /// turn held and no owner lock.
    fn resolve(&self, want: Self::Want) -> impl Future<Output = Option<Self::Domain>> + Send;
    /// Whether an installed `domain` is the one `want` asks about. Pure.
    fn governs(domain: &Self::Domain, want: Self::Want) -> bool;
    /// The piece index space of the entity. Pure.
    fn extent(domain: &Self::Domain) -> Range<u32>;
    /// How long one piece of this entity is, or `None` for a domain that
    /// cannot say. What turns a lookahead in bytes into pieces.
    fn piece_length(domain: &Self::Domain) -> Option<u64>;
    /// How many bytes the entity is, or `None` where the backing cannot say.
    ///
    /// Only [`State::buffering`] asks, and only to turn a player's stated
    /// duration into the film's bitrate -- which is what a window measured
    /// in seconds needs. Size over duration gives that number exactly, at
    /// the first report, with nothing to converge and no read pattern to be
    /// fooled by; a rate measured from reads instead has neither property.
    fn bytes(domain: &Self::Domain) -> Option<u64> {
        let _ = domain;
        None
    }
    /// The position `offset` bytes into the entity, or `None` where the
    /// backing cannot make one.
    ///
    /// The inverse of reading an offset out of a position, and it exists
    /// for one caller: a player states where it is in the *picture*, and
    /// with the film's length and the entity's size that is an offset into
    /// the file.
    fn position_at(domain: &Self::Domain, offset: u64) -> Option<Self::Position> {
        let _ = (domain, offset);
        None
    }
    /// The policy for `domain` under `budget` bytes, or why there is none.
    /// Pure, and it returns its error rather than logging it: it is called
    /// under L2 from [`Reader::note`], and the owner logs after unlock. The
    /// owner installs only a [`Shape::Split`] -- a budget that covers the
    /// entity bounds nothing and holds nothing back.
    ///
    /// `buffering` is what the entity's open readers have already been
    /// promised ([`State::buffering`]), which the window may not be sized
    /// under.
    fn policy(
        domain: &Self::Domain,
        budget: u64,
        buffering: Buffering,
    ) -> anyhow::Result<RetentionPolicy>;
    /// The index `at` lands on under `domain`, clamped to the last, or
    /// `None` when the position names nothing in this domain's index space.
    /// An entity only ever hears its own bytes -- a [`Reader`] is opened on
    /// one entity and a [`Retention::note_position`] names one key -- so a
    /// backing whose positions always fall in the domain answers `Some`.
    /// Pure.
    fn index_of(domain: &Self::Domain, at: Self::Position) -> Option<u32>;
    /// The backing refuses to give anything of `key` up right now -- a pin.
    /// A copy-out read of a lock outside the owner, asked with no owner lock
    /// held, before L2 wherever the two meet.
    fn keeps_everything(&self, key: &Self::Key) -> bool;
    /// Whether `key` is the entity being played **at this instant**
    /// ([`crate::retention::live`]).
    ///
    /// Asked on a [`Mode::Slack`] pass, and by nothing else. Once at the
    /// top, under the turn, before the pass destroys anything: the mode was
    /// decided from a reading taken before the driver's first entity, and a
    /// viewer can have started this file since. Then again at the [`Door`],
    /// as often as the backing's [`Self::reclaim`] asks it -- once per run
    /// where that asks through [`Door::shut`], once per candidate index
    /// where it asks through [`Door::refuses`] -- because a player can open
    /// the file again while its bytes are going, and the run has to stop
    /// where it is rather than empty the window the new stream is already
    /// reading. A copy-out read like [`Self::keeps_everything`],
    /// with no owner lock held, so it is on the path of every unlink the
    /// per-index shape makes.
    ///
    /// The default is `false`: a backing with no liveness of its own has
    /// nothing that could interrupt a slack run.
    fn is_live(&self, _key: &Self::Key) -> bool {
        false
    }

    /// Whether a viewer's play session is on `key`, sharing it: the one
    /// condition under which its play session draws
    /// ([`Retention::decide_draw_if_due`]). The torrent's answer is the
    /// player-token sessions ([`crate::retention::sessions`]), which only a
    /// player's own request moves; the default is [`Self::is_live`], for a
    /// backing whose liveness is its play session.
    fn plays(&self, key: &Self::Key) -> bool {
        self.is_live(key)
    }

    /// **Where in `key` the play session plays**, when it plays a member of
    /// a container through the member path: the member's byte extent within
    /// the entity. The draw is then sized from that extent's length and
    /// made inside it ([`Self::narrowed`]), and [`Self::content_shares`] is
    /// not asked -- the member path knows the entity is a container and
    /// which bytes of it are the film. `None` -- the default -- is the
    /// entity played as itself.
    ///
    /// A copy-out read like [`Self::plays`], asked with no owner lock held.
    fn played_member(&self, _key: &Self::Key) -> Option<Range<u64>> {
        None
    }

    /// `domain` narrowed to the pieces holding `bytes` of it (offsets into
    /// the entity), with those bytes as its length: what a play session
    /// on a member draws over. `None` when `bytes` is empty or reaches
    /// past the entity, and for a backing with no members (the default).
    /// Pure, like [`Self::policy`].
    fn narrowed(_domain: &Self::Domain, _bytes: Range<u64>) -> Option<Self::Domain> {
        None
    }

    /// **The set `key` is a volume of, when the play session plays a member
    /// across several entities** -- a film in a multi-volume RAR set inside
    /// a torrent: every entity the member lies in, in the member's order,
    /// each with the member's bytes in it. One thing played: the draw is
    /// made once, over every volume's bytes of the member ([`Self::joined`]),
    /// by whichever volume's play session is due first, and every other
    /// volume adopts it rather than drawing again. [`Self::content_shares`]
    /// is not asked, as for [`Self::played_member`]. `None` -- the default
    /// -- is no set.
    ///
    /// A copy-out read like [`Self::plays`], asked with no owner lock held.
    fn played_set(&self, _key: &Self::Key) -> Option<Vec<(Self::Key, Range<u64>)>> {
        None
    }

    /// What `key` is now, for a volume of a played set no stream has opened
    /// yet: the draw is made over the whole set at once, and a volume the
    /// reader has not reached has no entity to read a domain from. `None`
    /// -- the default -- is a backing with no sets, or an entity that
    /// cannot be named now.
    fn domain_of(&self, _key: &Self::Key) -> impl Future<Output = Option<Self::Domain>> + Send {
        async { None }
    }

    /// **One domain over the pieces of all of `parts`** (each one
    /// [`Self::narrowed`] to a volume's bytes of the member), for sizing a
    /// set's draw: the union of their pieces, counted once each, as one run
    /// `0..n` of the length they hold -- and that union in order, which is
    /// what index `i` of the joined domain stands for. The parts' pieces
    /// need not be one run in the entity's index space (another file may
    /// lie between two volumes), so the draw is made over the joined index
    /// space and mapped back through the list. `None` for no parts and for
    /// a backing with no sets (the default). Pure, like [`Self::policy`].
    fn joined(_parts: &[Self::Domain]) -> Option<(Self::Domain, Vec<u32>)> {
        None
    }

    /// Whether `domain`'s content may be shared at all: `Some(false)` for an
    /// archive or a disc image, which the app plays through a translated
    /// source and which shares nothing; `None` while that cannot be told
    /// yet -- the torrent's first bytes of the file are not held -- and the
    /// play session's draw waits. The default has nothing to tell.
    fn content_shares(&self, _domain: &Self::Domain) -> impl Future<Output = Option<bool>> + Send {
        async { Some(true) }
    }
    /// The owner has just forgotten `key` ([`Retention::forget_empty`]):
    /// whatever the backing keeps per entity beside the owner's map goes
    /// with it, or it outlives the entity for the life of the process.
    /// Called with no owner lock held. The default keeps nothing.
    fn forgotten(&self, _key: &Self::Key) {}
    /// What the disk holds of the entity: the torrent's registered store's
    /// held set, read in memory; the proxy's directory, listed off the
    /// reactor. `None` is a set we do not have -- no store is registered
    /// for the torrent, the pool would not answer, the directory would not
    /// list -- and the pass concludes nothing rather than advance over an
    /// empty reading of an entity that is not empty.
    fn held(
        &self,
        store: &Self::Store,
        domain: &Self::Domain,
    ) -> impl Future<Output = Option<BTreeSet<u32>>> + Send;
    /// Advertise `pieces`: tell the swarm we will share them, each one the
    /// moment we hold it. **The only direction there is**: nothing here
    /// ever takes an announcement back, because there is no un-Have, and
    /// the backend starts every entity with nothing advertised. Under
    /// [`Share::Nothing`] never called; the proxy's implementation may
    /// `debug_assert!` that.
    fn advertise(&self, pieces: Range<u32>) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Whether the backend is in the swarm and advertises one of `pieces`
    /// -- a piece a peer may have been told we have, or will be the moment
    /// it completes. What a [`Mode::Slack`] pass asks of the pieces it is
    /// about to delete: such pieces go only once the backend has left the
    /// swarm, which is the driver's to arrange (the torrent's `EndShares`).
    /// Read from the backend, never remembered. The default: a backing with
    /// no swarm.
    fn announced_in_swarm(&self, _pieces: &[u32]) -> impl Future<Output = bool> + Send {
        async { false }
    }
    /// The subset of `pieces` this domain alone owns bytes in: the torrent's
    /// boundary rule (`this_files_alone`); the proxy's identity.
    fn alone(&self, domain: &Self::Domain, pieces: &[u32])
    -> impl Future<Output = Vec<u32>> + Send;
    /// Trim what the backend fetches to what the pass decided to fetch:
    /// want every piece of `windows` again, and stop wanting every piece of
    /// the entity that is in no window and not `held`.
    ///
    /// `windows` here is the pass's *want*-windows and not the ones the
    /// [`Door`] keeps: every open read's lookahead is kept and never
    /// ordered, because what to fetch is the detector's answer and what may
    /// not be deleted is every live read's. The committed set needs
    /// no clause of its own: [`RetentionPolicy::advance`] keeps it inside
    /// the held set it was handed, and `held` is that reading.
    ///
    /// The torrent: librqbit's picker fetches every selected piece of a file
    /// it is not told otherwise about, so without this the swarm fills the
    /// file past the window and every pass reclaims what the last two
    /// seconds fetched -- bytes the swarm paid for twice and the disk sat over
    /// budget by. A piece the window has moved onto may have been dropped by
    /// an earlier pass, so the windows are re-wanted first; held pieces
    /// outside the window are the reclaim's (step 8) and are not touched
    /// here. `held` is the pass's reading, and a piece that arrived since is
    /// the backing's to notice: what it stops wanting it must not leave on
    /// the disk, which is what `store` is for -- and an unlink there is an
    /// unlink, asked of `door` at its instant like every other one this
    /// owner makes. The proxy: a proxied body is fetched by its own response,
    /// and there is no picker to trim, so the default does nothing. Called
    /// at pass step 6 with the turn held and no owner lock.
    fn want(
        &self,
        _store: &Self::Store,
        _domain: &Self::Domain,
        _windows: &[Range<u32>],
        _held: &BTreeSet<u32>,
        _door: &Door<Self>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Want every piece of the entity again: nothing bounds it now. The
    /// counterpart of [`Self::want`] for an entity left with no policy --
    /// a policy's passes stopped wanting pieces outside its window, and a
    /// pin or a budget that covers the file wants all of it. Not asked when
    /// one policy replaces another (the next pass trims to the new window)
    /// nor for a sibling an install retires (the file the reader left).
    /// Default: nothing, for the same reason as [`Self::want`]'s. Called
    /// with the turn held and no owner lock.
    fn want_all(&self, _domain: &Self::Domain) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// **What this entity's consumers are asking of the disk**, as the
    /// detector answers it: what to fetch ahead of them, what no unlink may
    /// touch, and what to give back first if something must go.
    ///
    /// The three replace what a playhead and a window once decided. A
    /// consumer is the unbroken run of bytes it caused, so membership is a
    /// question about the disk and this is the first moment in a pass that
    /// has one -- and the backing owns the detector because the reads reach
    /// it, not the owner. A read carries its own timestamps, so nothing is
    /// lost by answering it here rather than where it was served.
    fn reading(&self, domain: &Self::Domain, held: &BTreeSet<u32>, asking: Asking) -> Consumers;

    /// For [`crate::retention::trace`]: what the backing can say about the
    /// entity that the owner cannot.
    fn trace(
        &self,
        _store: &Self::Store,
        _domain: &Self::Domain,
    ) -> Option<crate::retention::trace::Backing> {
        None
    }
    /// Take `runs` off the disk, asking `door` at the backing's own
    /// granularity **at the instant of each unlink**, and say how many
    /// pieces went. The torrent asks [`Door::shut`] before every part of
    /// every run and [`Door::refuses`] per piece, cutting the run where the
    /// door refuses one; the proxy asks [`Door::refuses`] per chunk inside
    /// one blocking closure, so the asking and the unlink cannot be
    /// separated by a suspension. A closure that dies reports what it can
    /// vouch for, which is nothing: the policy is untouched either way,
    /// because it never left its cell.
    fn reclaim(
        &self,
        store: &Self::Store,
        domain: &Self::Domain,
        runs: Vec<Range<u32>>,
        door: Door<Self>,
    ) -> impl Future<Output = usize> + Send;
}

/// The owner: every entity of one kind, with its readers, its resident
/// policy and its turn. One per proxy cache; one per torrent engine.
pub struct Retention<B: Backing> {
    backing: Arc<B>,
    /// The published cache budget, the one cell both sides read. Read before
    /// L2 and never under it.
    budget: Arc<RetentionBudget>,
    /// L1. The entities by key. Held for lookup, insert, prune and
    /// iterate-for-holdings only.
    ///
    /// Shared with every [`Door`], which reads the draws recorded in it at
    /// the unlink ([`Door::drawn`]).
    entities: Arc<Entities<B>>,
    /// S. **One set's draw decided at a time** ([`Backing::played_set`]):
    /// the volumes of a set read whether a sibling has drawn and record
    /// their own draw as one step under it, so two volumes due together
    /// cannot both find nothing drawn and draw twice. Protects no memory.
    set_draws: parking_lot::Mutex<()>,
    /// Names the next reader; compared for equality only, so it may wrap.
    next_reader: AtomicU64,
    /// The one place a test can be *inside* a pass: run after the snapshot
    /// and before the listing, and again after the decision and before the
    /// unlinks, with the turn held and no owner lock. Each side keeps its
    /// own cell where its tests write it and installs a runner here that
    /// reads it. The shipped build has neither this nor the calls to it.
    #[cfg(any(test, feature = "test-hooks"))]
    hook: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// One entity: its turn and its state, and nothing that reaches the map.
pub struct Entity<B: Backing> {
    key: B::Key,
    /// T. `Arc`, so a guard can be owned by the pass and outlive the
    /// borrow that took it.
    turn: Arc<tokio::sync::Mutex<Turn>>,
    /// L2. `Arc`, so the [`Door`] can ask it from a blocking thread.
    state: Arc<parking_lot::Mutex<State<B>>>,
}

/// Proof of holding an entity's turn. Zero-sized: it protects no memory.
/// `&mut Turn` is what every write to the policy, the windows and the
/// stride requires, so "written only under the turn" is a type.
pub struct Turn(());

/// The entity's turn, owned. Dropping it on any path -- return, `?`, panic
/// unwind, future cancellation -- hands the turn back and leaves the policy
/// where it is. This is the proxy's `running` flag made into a guard, and
/// the reason a dead pass no longer blocks every later one.
pub struct Claim {
    guard: tokio::sync::OwnedMutexGuard<Turn>,
    /// The reader whose byte took this claim, so the pass knows which head
    /// it is about: that reader's own playhead while its body is open, and
    /// the entity's last delivered byte once it has ended. `None` from
    /// [`Retention::turn`] and [`Retention::try_turn`], which are about the
    /// entity.
    about: Option<ReaderId>,
}

impl Debug for Claim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Claim").field("about", &self.about).finish()
    }
}

/// Everything the owner knows about one entity. Behind L2, never across an
/// await.
struct State<B: Backing> {
    domain: B::Domain,
    /// The policy and the budget it was built from. **Never `take()`n.**
    /// `None` is "nothing bounds this entity": no budget yet, no cap, a
    /// budget that covers it, or a pin.
    installed: Option<Installed>,
    /// The budget the entity was last decided under, [`Install::OnDeliveredByte`]
    /// only; `None` before any decision. Kept apart from `installed` because
    /// "decided under X and nothing bounds it" is a different fact from
    /// "not decided yet", and the note compares against it on every byte.
    decided: Option<CacheBudget>,
    /// How far a playhead must move before another pass is worth its
    /// listing, in pieces. [`Trigger::OnMove`] only.
    stride: u32,
    /// What the last pass concluded: one range per playhead live then.
    /// Kept across a decide, deliberately -- an empty windows beside a live
    /// reader reads as "protect the whole entity" at the gate, which is the
    /// honest answer when nothing has been measured and a wrong one the
    /// moment it means "measured against a budget one byte different".
    windows: Vec<Range<u32>>,
    /// **What may not be unlinked, as one bit per piece**, shared with
    /// every door this entity opens and with the reads that promise into
    /// it. `None` until a pass has handed one over, which is every entity
    /// no pass has run on. See [`crate::retention::exempt`].
    exempt: Option<std::sync::Arc<crate::retention::exempt::Exempt>>,
    readers: HashMap<ReaderId, ReaderState<B>>,
    /// The entity's own last delivered byte: the proxy's `last_playhead`,
    /// and a torrent file's head. Only its own bytes ever reach it -- a
    /// [`Reader`] is on one entity, [`Retention::note_position`] names one
    /// key -- so a file whose reader has gone on to another file keeps the
    /// head it last had. What a pass measures from when the detector is
    /// silent and the reader that asked has ended: one of the questions
    /// [`Self::head`] asks, and the last resort under [`Consumers::at`].
    last_position: Option<B::Position>,
    /// **Where the entity is being consumed, as the detector last said**:
    /// the piece the busiest stream had reached at the last pass
    /// ([`Consumers::at`]), or `None` when the detector was silent -- no
    /// pass yet, nothing reading it for `STREAM_DORMANT`, or the policy gone.
    /// What [`Self::holding`] reports as the head ahead of every reading
    /// off the readers: which of several readers is the viewer is a
    /// question about behaviour, and mpv's index crawler is regularly the
    /// newest reader with a head at the tail of the file.
    consumed_at: Option<u32>,
    /// How long this entity's film is, where a player has said.
    ///
    /// **A property of the film and not of a report**, which is why it sits
    /// here rather than on a position report and never goes stale: with
    /// the entity's own size ([`Backing::bytes`]) it is the bitrate, a film
    /// does not change length, and a viewer who hands playback to a
    /// receiver stops reporting a position without the film becoming any
    /// shorter.
    ///
    /// It is also the only thing a cast can state. The receiver does the
    /// reading and reports where it is in *seconds*; converting that to a
    /// byte offset needs a constant bitrate, and on the field's 23 GB film
    /// a two-percent error is 460 MB -- wider than the whole window, so the
    /// window would land off the film. So a cast says how long the film is,
    /// which sizes the window exactly, and leaves where it is to the reads,
    /// which for a receiver are plainly sequential.
    duration: Option<std::time::Duration>,
    /// **How long the film of a member this entity holds is** -- a member
    /// of this one container ([`Backing::played_member`]) or a member across
    /// a set it is a volume of ([`Backing::played_set`])
    /// ([`Retention::note_set_duration`]): what the member's draw divides
    /// the member's length by. Never this entity's own rate, as
    /// [`Self::duration`] is: a volume's length over the film's duration is
    /// the film's rate divided by the number of volumes, a container's
    /// length over an episode's is the episode's times the episodes in it,
    /// and a read-ahead sized from either would starve the player or fill
    /// the budget. A property of the film, kept like [`Self::duration`].
    set_duration: Option<std::time::Duration>,
    /// **What this entity shares for the rest of its play session**: the
    /// draw decided once the viewer's playback stream's read-ahead was known
    /// ([`Retention::decide_draw_if_due`], [`RetentionPolicy::draw`]) -- the
    /// whole extent under a budget that covers the file, nothing under a
    /// budget too tight for the read-ahead and a shared set together, or
    /// none published -- and advertised to the backend the moment it was
    /// made. `None` until then, and for an entity nothing but asides and
    /// translated sources opened.
    ///
    /// **Made once, never changed, never taken back.** Every later policy
    /// adopts it ([`RetentionPolicy::adopt_draw`]); a seek, a budget that
    /// moves, a bitrate stated later redraw nothing, because a redraw is a
    /// withdrawal or a set nobody sized the read-ahead against. It ends with
    /// the play session: a [`Mode::Slack`] pass that runs once the backend
    /// has left the swarm forgets it with the bytes.
    draw: Option<BTreeSet<u32>>,
    /// **What [`Self::draw`] was made for** ([`DrawnFor`]): `Some` exactly
    /// when the draw is. A draw whose record no longer matches what the
    /// session plays is shared no more, and is forgotten once the driver
    /// has ended it ([`Retention::forget_draw_made_for`]) -- or at once
    /// when it is empty, since nothing of it was ever advertised.
    drawn_for: Option<DrawnFor<B::Key>>,
    /// **The viewer's player opened this entity in this play session**
    /// ([`Opener::Player`]): the one kind of open whose session draws a
    /// shared set, and only while a play session is on the entity
    /// ([`Backing::plays`]). A translated source's read, a pin's adoption
    /// and a subtitle fetched beside the film draw nothing. Cleared with the
    /// draw, when the session ends.
    player_opened: bool,
    /// The widest read-ahead any open of this play session stated
    /// ([`Retention::install_opening`]'s `opening`): what a draw decided
    /// after the opens -- the rate arrived later, or the fallback ran out
    /// -- is still sized beside, whether or not that open's reader is still
    /// in the map. Cleared with the draw.
    asked: Buffering,
    /// When a pass first found this play session's draw waiting for the
    /// film's rate with playback under way ([`DRAW_FALLBACK`]), or `None`.
    /// On the pass's own clock, never read from the wall. Cleared with the
    /// draw.
    draw_waiting_since: Option<std::time::Instant>,
    /// What [`Backing::content_shares`] said of this entity's content, once
    /// it could say: `Some(false)` for an archive, whose play session draws
    /// nothing. Asked until it answers, then kept for the session.
    content: Option<bool>,
    /// **A slack pass took this entity's policy and began taking its
    /// bytes**, so pieces it dropped are neither had nor wanted
    /// ([`Backing::reclaim`] leaves them dropped). Nothing else would want
    /// them again: what a pin lands on after such a pass has no policy for
    /// [`Retention::clear_under`] to forget, and a download the user asked
    /// to keep would stand still at the pieces the pass took. So the pin
    /// exit wants the entity whole when this is set, and clears it.
    slacked: bool,
    /// **How many pieces an asker may have asked for in all**, as the last
    /// pass measured it, with the budget it was measured under
    /// ([`Reader::room`]): what the entity may hold less everything its
    /// readers hold that no reclaim can give back.
    room: Option<(CacheBudget, u64)>,
    /// The draw that decides which of this entity's pieces this process
    /// offers to the swarm ([`Buffering::seed`]).
    ///
    /// **Per entity and random, and that is the whole point of it.** A
    /// peer cannot see the swarm, so it cannot coordinate; what it can do
    /// is pick independently, and independent picks by many peers sum to
    /// even coverage where one shared rule -- keep the first minute, keep
    /// every k-th piece -- sums to a shared hole. Drawn once and kept: a
    /// redraw would orphan pieces already announced.
    seed: u64,
    /// How many streams have ever been opened on this entity: every
    /// [`Retention::install`] that reached the turn, whatever it decided
    /// there.
    ///
    /// Counted rather than remembered as a flag because the question it
    /// answers is about a *gap*: a [`Mode::Slack`] pass carries the reading
    /// its driver took, and an open that landed between that reading and
    /// the turn is the one that would have its policy and its bytes deleted
    /// under it. Every open goes through `install` and `install` takes this
    /// turn, so an open the pass does not see in this number is one that
    /// cannot start until the pass has finished.
    opens: u64,
}

/// The policy in its cell, with the budget it was built for beside it.
struct Installed {
    budget: CacheBudget,
    policy: RetentionPolicy,
}

/// One open read of one entity.
struct ReaderState<B: Backing> {
    /// The position of the last byte this read delivered, and `None` until
    /// it has delivered one. Only ever written from a byte that really went
    /// out, never from a `Range` header.
    playhead: Option<B::Position>,
    /// Where this read was opened, for the window it is owed before it has
    /// delivered anything, and `None` for a reader opened at no position
    /// (the proxy's: nothing bounds a proxied entity until its first
    /// delivered byte, so there is no window for an offset to place).
    ///
    /// A read parked on its first piece is the one the entity is buffering
    /// *for*, and a playhead written only from a delivered byte makes it
    /// invisible: with no window the pass concluded nothing, the want-set
    /// stayed the whole file, and a cold open fetched 180 MB in the seconds
    /// before the first frame.
    opened_at: Option<B::Position>,
    /// The pieces this read has promised off the disk and not yet
    /// delivered. Nothing may unlink one of them.
    promised: Range<u32>,
    /// **The pieces a stream of this reader's is asking the backend for
    /// now, with no promise to read them** ([`Reader::asks`]): a pre-want's
    /// reach. Nothing a pass keeps once the stream has moved on -- a piece
    /// fetched this way is cached data like any other -- but while it is
    /// being asked for it is a fact about the backend, as a reading
    /// stream's lookahead is: the backend will not forget a piece a stream
    /// is asking for, and what of it has not arrived is about to. Empty for
    /// every reader that reads.
    asking: Range<u32>,
    /// The piece this reader's last pass ran at, so one reader playing on
    /// does not spend another's throttle.
    passed_at: Option<u32>,
    /// What this read asks of the cache: the stream lookahead it was
    /// granted when it opened, and the seconds its buffer profile wants
    /// held. Zero and `None` for a backing that grants neither (the proxy:
    /// a proxied body is fetched by its own response and reads nothing
    /// ahead, and its viewer chooses no profile).
    ///
    /// The committed half is sized to leave room for the largest lookahead
    /// here; see [`Buffering`]. It is what the reader was *granted*, already
    /// cut to what the last pass said the entity is asking for and to the
    /// whole cache, so it binds when the budget moves under a reader that is
    /// already open.
    /// [`Buffering::bytes_per_second`] is not this read's to state and is
    /// always `None` in it; the entity states it.
    buffering: Buffering,
}

/// How many pieces to want after each one a stream is still waiting for,
/// before the configured window takes over.
///
/// It exists to keep peers busy, and the number it has to beat is one:
/// a want-set of a single piece is a download from a single peer, because
/// librqbit reserves a piece to one peer at a time. Sixteen gives a dozen
/// or more of them something to reserve -- and, as much to the point, a
/// measured speed, without which the steal that would rescue a slow piece
/// has nothing to compare.
///
/// Sixty-four megabytes at a 4 MiB piece, of which a first frame needs
/// two or three. That is the price of the swarm being busy, it is paid
/// once per stream, and it buys back the ninety seconds the field spent
/// on piece 0.
const STARTUP_RUN: u32 = 16;

impl<B: Backing> ReaderState<B> {
    /// A read just opened: nothing delivered, nothing promised.
    fn opened(opened_at: Option<B::Position>, buffering: Buffering) -> Self {
        Self {
            playhead: None,
            opened_at,
            promised: 0..0,
            asking: 0..0,
            passed_at: None,
            buffering,
        }
    }

    /// Where this read is: the last byte it delivered, or the offset it was
    /// opened at until it has delivered one. What a window is drawn round.
    fn head(&self) -> Option<B::Position> {
        self.playhead.or(self.opened_at)
    }

    /// Whether anything has been observed of this read -- a promise or a
    /// delivered byte. A reader that has been opened and has done neither
    /// is one nothing has observed, and [`Retention::readers_of`] has
    /// always counted it for nothing; the entry now exists from the open,
    /// for [`Self::opened_at`], so the question has to be asked rather than
    /// read off the map.
    fn observed(&self) -> bool {
        self.playhead.is_some() || !self.promised.is_empty()
    }
}

/// Names one [`Reader`]. Equality only.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct ReaderId(u64);

/// What [`Retention::install`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    /// A new policy bounds the entity.
    Installed,
    /// The policy already installed describes this entity under this
    /// budget, so nothing was touched.
    Kept,
    /// The policy standing over this entity was resized to a budget that
    /// moved, keeping its draw and what it had committed.
    Resized,
    /// Nothing bounds the entity: no budget yet, no cap, a budget that
    /// covers it, nothing to resolve, or a pin. Whatever was installed
    /// before has been forgotten; what the entity shares is unchanged.
    Unbounded,
}

/// What one pass did.
#[derive(Debug)]
pub struct Outcome {
    /// What the pass concluded, or `None` for a pass that concluded nothing
    /// -- a pin, nothing installed, no head in this domain before or after
    /// the listing, a listing we do not have, or a decide that ran under it.
    /// Every one of those leaves the policy exactly where it was.
    pub concluded: Option<Conclusion>,
    /// The same claim, handed back because a byte delivered while the pass
    /// held the turn started nothing and is owed a pass: the head this pass
    /// measured has moved a stride since, or the policy was replaced under
    /// the pass by a byte that was due and found the turn taken. `None` is
    /// the turn released. Decided under L2 on **every** exit of the pass
    /// and never re-acquired; see rule 3 in the module docs.
    pub again: Option<Claim>,
}

/// What a pass that ran concluded. Zeroed counts are a pass that found
/// nothing to do, which is a conclusion.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Conclusion {
    /// Pieces of the draw the pass found held and committed.
    pub committed: usize,
    /// Committed pieces the disk no longer holds. Nothing is withdrawn for
    /// them: the backend announces only what it has.
    pub lost: usize,
    /// Pieces the backing reports really left the disk.
    pub reclaimed: usize,
    /// The windows this pass concluded, one per playhead live at its
    /// re-read. Written to the entity only if the budget was still the one
    /// measured; [`Holding::windows`] says what stands.
    pub windows: Vec<Range<u32>>,
}

/// What one entity holds, as a value: everything a gate or a panel derives
/// its answer from, copied out under L2 so the answer itself is built with
/// nothing held.
pub struct Holding<B: Backing> {
    pub domain: B::Domain,
    /// The entity's whole index space, [`Backing::extent`].
    pub extent: Range<u32>,
    /// The policy, or `None` when nothing bounds the entity.
    pub installed: Option<InstalledView>,
    /// What the last pass concluded; empty before the first.
    pub windows: Vec<Range<u32>>,
    /// Every non-empty promise of an open read.
    pub promised: Vec<Range<u32>>,
    /// What every asker is asking the backend for now ([`Reader::asks`]).
    pub asking: Vec<Range<u32>>,
    /// Some reader has delivered a byte and has not ended.
    pub live_playhead: bool,
    /// Where the entity is being consumed: the detector's answer from the
    /// last pass ([`State::consumed_at`], the piece its busiest stream had
    /// reached), and only when the detector is silent the readers' head in
    /// the order [`State::head`] asks -- a live read's, else where playback
    /// last got to. `None` when nothing has ever read it.
    ///
    /// **This, and not [`Self::last_position`] or the newest reader, is
    /// what a reading of the window has to split at.** mpv keeps an index
    /// crawler open beside the viewer and reopens it about once a second,
    /// so the newest reader is regularly one parked at the tail of the
    /// file: that is the reading that had the overlay show a 4K film at
    /// 0:00 with fifty megabytes "behind" the playhead and four ahead. The
    /// detector tells the two apart by what they eat.
    pub head: Option<B::Position>,
    /// The entity's last delivered byte, from any reader, and `None` until
    /// one has gone out. Every delivered byte moves it ([`Reader::note`]);
    /// which of the readers is the viewer is the detector's question, not
    /// this field's.
    pub last_position: Option<B::Position>,
    /// The budget the entity was last decided under, on the delivered byte;
    /// `None` before any decision and under [`Install::OnOpen`].
    pub decided: Option<CacheBudget>,
}

/// The installed policy as a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledView {
    /// The budget it was built from.
    pub budget: CacheBudget,
    /// The pieces it governs.
    pub pieces: Range<u32>,
    /// What it has committed: advertised, and never to be reclaimed.
    pub committed: BTreeSet<u32>,
    /// Always a [`Shape::Split`]: a whole shape installs nothing.
    pub shape: Shape,
}

/// What a pass snapshots under L2 before the listing, and compares against
/// after it. Only which domain and which budget; the head is read again at
/// the re-read, and `at` -- the piece it stood on before the listing -- is
/// what a listing that fails is measured against when the pass asks whether
/// it owes another.
struct Begin<B: Backing> {
    domain: B::Domain,
    budget: CacheBudget,
    at: u32,
}

impl<B: Backing> Retention<B> {
    /// An owner with no entities, over `backing`, reading its cap from
    /// `budget`. `Arc`, because every [`Reader`] holds its owner.
    pub fn new(backing: Arc<B>, budget: Arc<RetentionBudget>) -> Arc<Self> {
        // `install_now` swaps a policy under L2 alone, with no backend call;
        // that is only sound for a backing that shares nothing. A backing
        // that shares and installs on the delivered byte would decide what a
        // play session shares under a lock no advertise may be made under.
        const {
            assert!(
                !matches!(B::INSTALL, Install::OnDeliveredByte)
                    || matches!(B::SHARE, Share::Nothing),
                "Install::OnDeliveredByte requires Share::Nothing: an install under the state lock alone cannot re-advertise"
            );
        }
        Arc::new(Self {
            backing,
            budget,
            entities: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            set_draws: parking_lot::Mutex::new(()),
            next_reader: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            hook: parking_lot::Mutex::new(None),
        })
    }

    /// Install the runner a pass calls at its two hook points. Each side
    /// keeps its own interleave cell where its tests write it; the runner
    /// reads that cell. Runs with the turn held and no owner lock, so it
    /// may note, promise, pin or publish a budget.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn hook(&self, runner: impl Fn() + Send + Sync + 'static) {
        *self.hook.lock() = Some(Arc::new(runner));
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn run_hook(&self) {
        // Cloned out and released before the call, so the runner may take
        // any lock this module has.
        let runner = self.hook.lock().clone();
        if let Some(runner) = runner {
            runner();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn run_hook(&self) {}

    /// The entity for `key`, created with `domain` if it did not exist. L1
    /// only, no I/O. An entity that exists keeps the domain it was made
    /// with: a key names one directory or one file, and a domain that has
    /// really changed is [`Retention::install`]'s to write, under the turn.
    /// A fresh entity has no head: a byte noted before it existed was a byte
    /// nothing was bounding, and it is not remembered for it.
    pub fn entity(&self, key: B::Key, domain: B::Domain) -> Arc<Entity<B>> {
        self.entities
            .lock()
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(Entity {
                    key,
                    turn: Arc::new(tokio::sync::Mutex::new(Turn(()))),
                    state: Arc::new(parking_lot::Mutex::new(State {
                        exempt: None,
                        domain,
                        installed: None,
                        decided: None,
                        stride: 1,
                        windows: Vec::new(),
                        readers: HashMap::new(),
                        last_position: None,
                        consumed_at: None,
                        duration: None,
                        set_duration: None,
                        draw: None,
                        drawn_for: None,
                        player_opened: false,
                        asked: Buffering::default(),
                        draw_waiting_since: None,
                        content: None,
                        slacked: false,
                        room: None,
                        opens: 0,
                        seed: share_seed(),
                    })),
                })
            })
            .clone()
    }

    fn lookup(&self, key: &B::Key) -> Option<Arc<Entity<B>>> {
        self.entities.lock().get(key).cloned()
    }

    /// Every entity's key, in no order. L1 only, no I/O: what a driver
    /// walks to decide a [`Mode`] per entity, without copying every
    /// [`Holding`] out to do it.
    pub fn keys(&self) -> Vec<B::Key> {
        self.entities.lock().keys().cloned().collect()
    }

    /// Open a reader on the entity. It records nothing until it promises or
    /// delivers a byte; a reader that does neither is one nothing has
    /// observed, and it counts for nothing.
    ///
    /// **The proxy's opener**, and it takes no offset because there is no
    /// window for one to place: a proxied entity has nothing installed on
    /// it until its first delivered byte ([`Install::OnDeliveredByte`]), so
    /// a read parked before that byte has nothing to be kept inside of.
    pub fn reader(self: &Arc<Self>, key: B::Key, domain: B::Domain) -> Reader<B> {
        Reader {
            owner: self.clone(),
            entity: self.entity(key, domain),
            id: ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed)),
            opened_at: None,
            // A proxied body is fetched by its own response and reads
            // nothing ahead of the bytes it is relaying, so there is no
            // lookahead the window has to hold, and its viewer chooses no
            // buffer profile.
            buffering: Buffering::default(),
        }
    }

    /// Open a reader on the entity `key` already has, or `None` when it has
    /// none. The torrent's opener: its install runs before the reader opens
    /// and made the entity if the file could be bounded at all, and a file
    /// that could not -- no metadata to name its pieces by -- has nothing a
    /// head would be measured against, so its bytes are not remembered. L1
    /// only, no I/O.
    ///
    /// `at` is where the read was opened and `buffering` what it asks of the
    /// cache -- the lookahead it was granted and the seconds its buffer
    /// profile wants held ([`Buffering`]). Both are recorded here, at the
    /// open, rather than waiting for a delivered byte: the policy is
    /// installed before this is called, so the very first pass over the
    /// entity can draw its window round a read that is still parked on its
    /// first piece, and a probe never gets to claim the entity's head at
    /// all.
    ///
    /// **And the caller opens the reader before it opens the stream the
    /// read will be served from**, not only before the first byte. Between
    /// the install and this call the entity stands with the open already
    /// counted and no reader on it, and every await the caller spends in
    /// there is a window for a slack pass: the count is what
    /// [`Self::readers_and_opens_of`] compares, so the pass finds it equal,
    /// finds nothing observed, and takes the policy back. If it also
    /// reclaims everything the entity holds -- which a file with nothing on
    /// the disk yet trivially satisfies -- [`Self::forget_empty`] takes the
    /// entity, and the reader opened afterwards is `None`: that read notes
    /// no byte for the rest of its life and nothing re-installs a policy,
    /// because an install runs only at an open. Opened first, the reader's
    /// `Arc` on the entity is what keeps `forget_empty` off it.
    pub fn reader_on(
        self: &Arc<Self>,
        key: &B::Key,
        at: B::Position,
        buffering: Buffering,
    ) -> Option<Reader<B>> {
        let entity = self.lookup(key)?;
        let id = ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed));
        entity
            .state
            .lock()
            .readers
            .insert(id, ReaderState::opened(Some(at), buffering));
        Some(Reader {
            owner: self.clone(),
            entity,
            id,
            opened_at: Some(at),
            buffering,
        })
    }

    /// **An asker: something that fetches pieces of `key` nobody has
    /// promised to read** -- a resumed film's pre-want, asking for the
    /// resume point while the player is still on the file's head
    /// (`Engine::prewant`). The handle says what its stream is asking the
    /// backend for now ([`Reader::asks`]) and is told how much it may ask
    /// for in all ([`Reader::room`]).
    ///
    /// **It wants, and it holds nothing.** A read's promise does two things
    /// at once: it has the piece fetched and it exempts it from every
    /// unlink until it is read. An asker's pieces get only the first. Once
    /// one is on the disk and the asker has moved on it is cached data like
    /// any other outside every reader's window: kept while there is room,
    /// given back by the same order as everything else when there is not
    /// (`retention::ledger`), and protected again only when a real read
    /// promises it. That is why an asker needs no share of the cache set
    /// aside for it, and why it may not ask for what could not be kept.
    ///
    /// **No head either**: where a pass measures from, and where the entity
    /// is said to be, are the reads' own. It is not an observed reader, so
    /// it keeps no entity live. `None` for a key with no entity, which is
    /// one nothing bounds. Dropping the handle ends the asking.
    pub fn ask_ahead(self: &Arc<Self>, key: &B::Key) -> Option<Reader<B>> {
        let entity = self.lookup(key)?;
        let id = ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed));
        entity
            .state
            .lock()
            .readers
            .insert(id, ReaderState::opened(None, Buffering::default()));
        Some(Reader {
            owner: self.clone(),
            entity,
            id,
            opened_at: None,
            buffering: Buffering::default(),
        })
    }

    /// Where a reader of `key` last got to, told without a [`Reader`]: the
    /// entity's head moves and no reader's does. L2 only, never claims the
    /// turn, never installs. A key with no entity is a byte nothing is
    /// bounding, and it is not remembered; a byte of another key is that
    /// key's and moves nothing here.
    ///
    /// Always a playback position: there is no reader here to be a probe,
    /// and the callers that have one go through [`Reader::note`], which
    /// asks what the read is for.
    pub fn note_position(&self, key: &B::Key, at: B::Position) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let mut state = entity.state.lock();
        state.last_position = Some(at);
    }

    /// **How long the entity's film is**, without saying where anything is
    /// in it; see [`State::duration`]. What a cast can state.
    ///
    /// It gives the time caps their number without measuring anything:
    /// bytes of file over seconds of film is the bitrate by definition, so
    /// a pause, a stall, a reconnect and a burst are all invisible to it.
    /// Where the viewer is, nobody states any more -- it is where the reads
    /// are ([`crate::retention::streams`]).
    ///
    /// L2 only, like [`Self::note_position`]: no turn, no install, and a
    /// key with no entity is nothing to remember.
    pub fn note_duration(&self, key: &B::Key, duration: std::time::Duration) {
        if duration.is_zero() {
            return;
        }
        let Some(entity) = self.lookup(key) else {
            return;
        };
        entity.state.lock().duration = Some(duration);
    }

    /// **How long the film of a member is**, told to its container or to
    /// one of the volumes of its set ([`State::set_duration`]): the rate
    /// the member's draw is sized with, and nothing else -- the entity's own
    /// stream keeps no rate from it. L2 only, like [`Self::note_duration`].
    pub fn note_set_duration(&self, key: &B::Key, duration: std::time::Duration) {
        if duration.is_zero() {
            return;
        }
        let Some(entity) = self.lookup(key) else {
            return;
        };
        entity.state.lock().set_duration = Some(duration);
    }

    /// [`Self::install_opening`] for an open that asks nothing of the cache
    /// beyond what the entity's readers already do, and shares nothing
    /// ([`Opener::Unshared`]): a pin's adoption, and the tests.
    pub async fn install(&self, key: B::Key, want: B::Want) -> InstallOutcome {
        self.install_opening(key, want, Buffering::default(), Opener::Unshared)
            .await
    }

    /// Install (or keep) the policy for `key` about to be streamed by a
    /// reader that will ask `opening` of the cache, and decide what the
    /// entity shares if it can be decided now. [`Install::OnOpen`]'s path.
    ///
    /// **It touches no other entity.** What is live is one cell
    /// ([`crate::retention::live`]); the file a reader left is
    /// [`Mode::Slack`] at the next pass.
    ///
    /// **The draw is decided once per play session, and only for the
    /// viewer's playback stream** ([`State::draw`], [`Opener`]): here, if
    /// the film's rate is known already or the budget covers the file, and
    /// otherwise by the first live pass that finds it known
    /// ([`DRAW_FALLBACK`]). Sized with the stream's real read-ahead, so a
    /// budget that cannot hold both it and a shared set shares nothing, and
    /// advertised to the backend at once, which announces each piece of it
    /// as it completes and nothing else of the entity. A later open adopts
    /// the draw it finds. Nothing here ever takes an announcement back.
    ///
    /// Under this key's turn alone: a pin clears the policy; a policy that
    /// already describes this domain under this budget is kept untouched;
    /// one over this domain under another budget is resized in place
    /// ([`InstallOutcome::Resized`]); otherwise the old policy is forgotten
    /// and the new one installed.
    pub async fn install_opening(
        &self,
        key: B::Key,
        want: B::Want,
        opening: Buffering,
        opener: Opener,
    ) -> InstallOutcome {
        // A key with no entity has no turn to take and nothing installed to
        // serialise against; its domain has to be resolved before there is
        // anything to lock. Two firsts on one key both resolve, converge on
        // one entity in `entity()`, and take the turn in sequence: the
        // second finds the first's policy and keeps it.
        let (entity, fresh) = match self.lookup(&key) {
            Some(entity) => (entity, None),
            None => {
                let Some(domain) = self.backing.resolve(want).await else {
                    return InstallOutcome::Unbounded;
                };
                (self.entity(key.clone(), domain.clone()), Some(domain))
            }
        };
        let mut claim = Claim {
            guard: entity.turn.clone().lock_owned().await,
            about: None,
        };
        // Counted here, under the turn and before anything is decided,
        // because what a [`Mode::Slack`] pass has to know is that a stream
        // opened -- not what the install made of it. An open that finds the
        // policy already right (`Kept`), or that cannot be bounded, is
        // still a viewer starting this file, and a pass that deleted the
        // entity under one would take the bytes it is about to read.
        entity
            .state
            .lock()
            .opened(&mut claim.guard, opening, opener);
        let outcome = self
            .install_under(&entity, &mut claim, key, want, fresh, opening)
            .await;
        // Whatever the install made of it, the play session's draw may be
        // decidable now: this open's read-ahead is known, and the film's
        // rate may be.
        self.decide_draw_if_due(&entity, &mut claim, None).await;
        outcome
    }

    /// The body of [`Self::install_opening`], with the turn held and the
    /// open counted. Every policy it puts in shares the play session's draw
    /// if one has been decided, and nothing until then.
    async fn install_under(
        &self,
        entity: &Entity<B>,
        claim: &mut Claim,
        key: B::Key,
        want: B::Want,
        fresh: Option<B::Domain>,
        opening: Buffering,
    ) -> InstallOutcome {
        let budget = self.budget.get();
        if self.backing.keeps_everything(&key) {
            // A pin is a retention property: the user asked for those
            // bytes, and they are shared whole -- the pin advertises them,
            // not the owner -- and fetched whole, which the policy's passes
            // stopped asking for beyond the window.
            self.clear_under(entity, claim);
            self.want_whole(entity).await;
            return InstallOutcome::Unbounded;
        }
        {
            let state = entity.state.lock();
            if state
                .installed
                .as_ref()
                .is_some_and(|installed| installed.budget == budget)
                && B::governs(&state.domain, want)
            {
                return InstallOutcome::Kept;
            }
        }
        // Resolved under the turn: what the backend says the file is, now. A
        // fresh entity was
        // resolved a moment ago to be made at all, and is not asked twice.
        let resolved = match fresh {
            Some(domain) => Some(domain),
            None => self.backing.resolve(want).await,
        };
        // Read under L2 and not carried from the open: a reader that opened
        // a moment ago is in the map by now, and the largest lookahead in
        // force is what the window may not be sized under -- beside the one
        // the reader about to open will be granted, which is not in the map
        // yet and is the one this open is for.
        let buffering = entity.state.lock().buffering().widest(opening);
        let sized = resolved.as_ref().and_then(|domain| match budget {
            CacheBudget::Bytes(bytes) => match B::policy(domain, bytes, buffering) {
                Ok(policy) => Some(policy),
                Err(error) => {
                    tracing::warn!(
                        key = ?key,
                        error = %format!("{error:#}"),
                        "could not size a retention policy; the entity is not bounded and shares nothing"
                    );
                    None
                }
            },
            CacheBudget::Unknown | CacheBudget::Unbounded => None,
        });
        // The budget covers the file: keep all of it and reclaim none of
        // it -- and install nothing, because an installed policy is what
        // makes a piece reclaimable, and nothing is here. What it shares is
        // the draw, which is then the whole file.
        let mut policy = sized.filter(|policy| policy.shape() != Shape::Whole);
        if let Some(policy) = policy.as_mut() {
            // The play session's draw, or -- until it is decided -- nothing:
            // the draw a policy makes on its own is sized against whatever
            // this open knew, and nothing may be committed that is not
            // advertised.
            let state = entity.state.lock();
            policy.adopt_draw(state.draw.as_ref().unwrap_or(&BTreeSet::new()));
        }
        // A budget that moved under a policy still standing over this very
        // domain. The budget is republished every minute from the free
        // space, so this is most opens, not a rare one: rebuilt, the fresh
        // policy's first pass would reclaim everything the old one had
        // committed. Resized in place, the committed set stays.
        let carried = match (&resolved, policy.as_mut()) {
            (Some(domain), Some(next)) => {
                let state = entity.state.lock();
                state
                    .installed
                    .as_ref()
                    .filter(|_| state.domain == *domain)
                    .map(|installed| installed.policy.carry_into(next))
                    .is_some()
            }
            _ => false,
        };
        if carried && let Some(next) = policy.take() {
            return self.resize_under(entity, claim, budget, next).await;
        }
        self.clear_under(entity, claim);
        // Nothing installed after this point leaves the entity unbounded,
        // and an unbounded entity is fetched whole: what the old policy's
        // passes stopped wanting is wanted again. A new policy going in
        // wants nothing here -- its first pass trims to its own window.
        let (Some(domain), Some(policy)) = (resolved, policy) else {
            self.want_whole(entity).await;
            return InstallOutcome::Unbounded;
        };
        let pieces = policy.pieces();
        debug_assert_eq!(
            pieces,
            B::extent(&domain),
            "a policy over less than the extent would govern part of a file"
        );
        tracing::debug!(
            key = ?key,
            first = pieces.start,
            end = pieces.end,
            shape = ?policy.shape(),
            "bounding an entity to the cache budget"
        );
        entity
            .state
            .lock()
            .install_policy(&mut claim.guard, domain, budget, policy);
        InstallOutcome::Installed
    }

    /// The pieces holding `domain`'s first [`crate::retention::sniff::HEAD_BYTES`]:
    /// what [`Backing::content_shares`] reads to tell an archive. Pure, so
    /// it is asked under L2.
    fn head_of(domain: &B::Domain) -> Option<Range<u32>> {
        let last = crate::retention::sniff::HEAD_BYTES.saturating_sub(1);
        let first = B::index_of(domain, B::position_at(domain, 0)?)?;
        let end = B::index_of(domain, B::position_at(domain, last)?)?;
        Some(first..end.saturating_add(1))
    }

    /// **Decide what this play session shares, if it is time to**, and
    /// advertise it. Under the turn; a no-op once decided, so the draw is
    /// made once and never grows.
    ///
    /// Only for the viewer's playback stream ([`Opener::Player`]) of an
    /// entity a play session is on ([`Backing::plays`]), and never under a pin,
    /// which shares the file whole on its own. Then it is time when any of
    /// these holds, and only then:
    ///
    /// * **the film's rate is known** -- the player has said how long it
    ///   is -- so the read-ahead every later open is granted, the bitrate
    ///   times the seconds the viewer asked for, is known too, and the draw
    ///   is sized beside it ([`State::draw_buffering`]);
    /// * **the budget covers the file**, where the draw is the whole file
    ///   whatever the read-ahead;
    /// * **nothing can be sized** -- no budget published, no cap, a file
    ///   the policy refuses -- and the session shares nothing, as before;
    /// * **playback has run [`DRAW_FALLBACK`] without a rate**, on the
    ///   clock of the live pass that asks (`now`; an open has none), and
    ///   the draw is sized beside what the opens stated.
    ///
    /// **And not before the content is known** ([`Backing::content_shares`]):
    /// the file's first bytes held and not an archive's. An archive's play
    /// session decides an empty draw -- unless the session plays a member of
    /// it ([`Backing::played_member`]): then the content is not asked, and
    /// the draw is sized and made over the member's extent alone
    /// ([`Backing::narrowed`]), its rate the extent's length over the
    /// member's duration, told for the draw alone ([`State::set_duration`]).
    ///
    /// **A member across a set is drawn once for the set**
    /// ([`Backing::played_set`]): the volume whose session is due first
    /// draws over every volume's bytes of the member ([`Backing::joined`]),
    /// sized from the member's whole length (its rate that length over the
    /// set's duration, [`State::set_duration`]), records the whole draw and
    /// advertises it; every other volume, at its own open or pass, finds
    /// that draw on a sibling and adopts it -- nothing drawn or advertised
    /// again. Each volume's policy keeps its own pieces of it
    /// ([`RetentionPolicy::adopt_draw`]); the record is the set's, on every
    /// volume, so what is shared is the union whichever volume is asked.
    ///
    /// Recorded before it is advertised, so a reading of what the entity
    /// shares ([`Retention::draws`]) that sees the backend's announcement
    /// also sees the record of it; adopted by the standing policy at once,
    /// so what it commits from here on is the draw. A refused advertise is
    /// logged and leaves the draw recorded: the pieces are then ours and
    /// unshared, which is no failure of the rule, and the driver advertises
    /// what an entity shares again when it finds the backend short of it.
    async fn decide_draw_if_due(
        &self,
        entity: &Entity<B>,
        claim: &mut Claim,
        now: Option<std::time::Instant>,
    ) {
        if B::SHARE == Share::Nothing {
            return;
        }
        // Asked before L2 (rule 2), and under the turn every open takes.
        if self.backing.keeps_everything(&entity.key) || !self.backing.plays(&entity.key) {
            return;
        }
        // A member played through the member path: the path said what the
        // file is -- a container -- and which of its bytes are the film.
        let member = self.backing.played_member(&entity.key);
        // Or a member across a set this file is one volume of.
        let set = self.backing.played_set(&entity.key);
        let made_for = DrawnFor {
            member: member.clone(),
            set: set.clone(),
        };
        // What the file is, asked of its first bytes once they are held --
        // off L2, and once per session: an archive shares nothing, and one
        // that cannot be told yet waits. Not asked for a member, whose
        // container would answer "archive" to exactly the question the
        // member path has already answered.
        let (domain, content) = {
            let state = entity.state.lock();
            if state.draw_settled(&made_for) || !state.player_opened {
                return;
            }
            (state.domain.clone(), state.content)
        };
        let content = match (member.is_some() || set.is_some(), content) {
            (true, _) => true,
            (false, Some(content)) => content,
            (false, None) => {
                let Some(content) = self.backing.content_shares(&domain).await else {
                    return;
                };
                entity.state.lock().content = Some(content);
                content
            }
        };
        // What a set's draw is made over, found before the record: the
        // volumes the reader has not reached have no entity, and what they
        // are is asked of the backing, off every owner lock.
        let joined = match &set {
            None => None,
            Some(parts) => Some(self.joined_set(entity, &domain, parts).await),
        };
        // Asked again, off L2 (rule 2) and with no await between it and the
        // record: the head read above can park, and a viewer who moved to
        // another file meanwhile -- or from the file to a member of it --
        // would have this draw recorded and advertised for a file no
        // session plays, or sized for bytes nobody plays.
        if !self.backing.plays(&entity.key)
            || self.backing.played_member(&entity.key) != member
            || self.backing.played_set(&entity.key) != set
        {
            return;
        }
        let budget = self.budget.get();
        // A set's volumes decide one at a time (S, before L2), and a volume
        // whose sibling has drawn adopts that draw: copied out of the
        // sibling's state under its own L2, released before this one's is
        // taken.
        let (draw, refused) = {
            let _set_turn = set.as_ref().map(|_| self.set_draws.lock());
            let drawn_by_a_sibling = set
                .as_ref()
                .and_then(|parts| self.sibling_draw(&entity.key, parts));
            let mut state = entity.state.lock();
            if state.draw_settled(&made_for) || !state.player_opened {
                return;
            }
            if !content {
                // An archive: decided, and nothing in it.
                state.adopt_draw(&mut claim.guard, BTreeSet::new(), made_for);
                drop(state);
                tracing::debug!(
                    key = ?entity.key,
                    "the play session is on an archive; it shares nothing"
                );
                return;
            }
            if let Some(draw) = drawn_by_a_sibling {
                // The set's draw, made and advertised by the volume that
                // was due first: this volume keeps its pieces of it, and
                // nothing is drawn or advertised again.
                state.adopt_draw(&mut claim.guard, draw, made_for);
                return;
            }
            // What the draw is made over: the member's pieces, with its
            // length, when the session plays a member -- a season in one
            // container shares the episode being watched, not the
            // container -- the pieces of every volume's bytes of it when it
            // plays a member across a set, and the entity otherwise.
            let (over, map) = match (&member, joined) {
                (_, Some(Some((over, map)))) => (over, Some(map)),
                (_, Some(None)) => {
                    state.adopt_draw(&mut claim.guard, BTreeSet::new(), made_for);
                    drop(state);
                    tracing::warn!(
                        key = ?entity.key,
                        ?set,
                        "the played set names no bytes of its volumes; it shares nothing"
                    );
                    return;
                }
                (None, None) => (state.domain.clone(), None),
                (Some(bytes), None) => match B::narrowed(&state.domain, bytes.clone()) {
                    Some(over) => (over, None),
                    None => {
                        state.adopt_draw(&mut claim.guard, BTreeSet::new(), made_for);
                        drop(state);
                        tracing::warn!(
                            key = ?entity.key,
                            ?bytes,
                            "the played member names no bytes of its file; it shares nothing"
                        );
                        return;
                    }
                },
            };
            // A member's duration, of one container or across a set, is
            // told for the draw alone ([`State::set_duration`]).
            let buffering = state.draw_buffering(budget, &over, member.is_some() || map.is_some());
            let (sized, refused) = match budget {
                CacheBudget::Bytes(bytes) => match B::policy(&over, bytes, buffering) {
                    Ok(policy) => (Some(policy), None),
                    Err(error) => (None, Some(error)),
                },
                CacheBudget::Unknown | CacheBudget::Unbounded => (None, None),
            };
            let due = match &sized {
                None => true,
                Some(policy) => {
                    policy.shape() == Shape::Whole
                        || buffering.bytes_per_second.is_some()
                        || state.fallback_ran_out(now)
                }
            };
            if !due {
                return;
            }
            let draw: BTreeSet<u32> = sized
                .map(|policy| policy.draw().clone())
                .unwrap_or_default();
            // A set's draw is made in the joined index space: each index
            // there is the piece the list says.
            let draw = match &map {
                None => draw,
                Some(map) => draw
                    .into_iter()
                    .filter_map(|index| map.get(index as usize).copied())
                    .collect(),
            };
            state.adopt_draw(&mut claim.guard, draw.clone(), made_for);
            (draw, refused)
        };
        if let Some(error) = refused {
            tracing::warn!(
                key = ?entity.key,
                error = %format!("{error:#}"),
                "could not size the play session's shared set; it shares nothing"
            );
        }
        tracing::debug!(
            key = ?entity.key,
            pieces = draw.len(),
            set = set.is_some(),
            "the play session's shared set is drawn"
        );
        for run in runs(&draw.iter().copied().collect::<Vec<_>>()) {
            if let Err(error) = self.backing.advertise(run.clone()).await {
                tracing::warn!(
                    key = ?entity.key,
                    first = run.start,
                    end = run.end,
                    error = %format!("{error:#}"),
                    "could not advertise the play session's shared set; its pieces stay ours and unshared"
                );
                break;
            }
        }
    }

    /// **The domain a set's draw is made over** ([`Backing::joined`]): each
    /// volume's domain -- `entity`'s own (`domain`), a sibling's from its
    /// entity, or, for a volume no stream has opened yet, the backing's
    /// ([`Backing::domain_of`]) -- narrowed to the member's bytes in it,
    /// and the lot joined. `None` when a volume cannot be named or its
    /// bytes lie outside it: the set then shares nothing. No owner lock is
    /// held across the backing's await (rule 2).
    async fn joined_set(
        &self,
        entity: &Entity<B>,
        domain: &B::Domain,
        parts: &[(B::Key, Range<u64>)],
    ) -> Option<(B::Domain, Vec<u32>)> {
        let mut narrowed = Vec::with_capacity(parts.len());
        for (key, bytes) in parts {
            let volume = if *key == entity.key {
                domain.clone()
            } else {
                let known = self
                    .lookup(key)
                    .map(|sibling| sibling.state.lock().domain.clone());
                match known {
                    Some(volume) => volume,
                    None => self.backing.domain_of(key).await?,
                }
            };
            narrowed.push(B::narrowed(&volume, bytes.clone())?);
        }
        B::joined(&narrowed)
    }

    /// The draw a sibling volume of `key`'s set has made **for this set**,
    /// if one has: read under S, the sibling's entity copied out of L1 and
    /// its L2 taken alone, one at a time. A sibling's draw made for anything
    /// else -- the volume played by its URL, whose sniff found an archive
    /// and drew nothing, or another set -- is not the set's, and adopting
    /// it would have the set share that instead.
    fn sibling_draw(&self, key: &B::Key, parts: &[(B::Key, Range<u64>)]) -> Option<BTreeSet<u32>> {
        parts
            .iter()
            .filter(|(sibling, _)| sibling != key)
            .filter_map(|(sibling, _)| self.lookup(sibling))
            .find_map(|sibling| {
                let state = sibling.state.lock();
                let for_this_set = state
                    .drawn_for
                    .as_ref()
                    .and_then(|made_for| made_for.set.as_deref())
                    == Some(parts);
                for_this_set.then(|| state.draw.clone()).flatten()
            })
    }

    /// Put `next` in place of the standing policy, which it has already
    /// been carried onto ([`RetentionPolicy::carry_into`]). Under the turn.
    ///
    /// **Nothing is taken back here, however much smaller the new budget
    /// is.** The draw has been advertised, and there is no un-have in
    /// BitTorrent: `carry_into` adopts the whole draw and the whole
    /// committed set for that reason, so there is nothing over capacity to
    /// give back.
    async fn resize_under(
        &self,
        entity: &Entity<B>,
        claim: &mut Claim,
        budget: CacheBudget,
        next: RetentionPolicy,
    ) -> InstallOutcome {
        tracing::debug!(
            key = ?entity.key,
            shape = ?next.shape(),
            "resizing an entity's policy to a budget that moved"
        );
        entity
            .state
            .lock()
            .resize_policy(&mut claim.guard, budget, next);
        InstallOutcome::Resized
    }

    /// Forget the policy for `key`. Under the turn.
    #[cfg(test)]
    pub async fn clear(&self, key: &B::Key) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let mut claim = Claim {
            guard: entity.turn.clone().lock_owned().await,
            about: None,
        };
        self.clear_under(&entity, &mut claim);
    }

    /// `Self::clear` (test builds only) with the turn already held: forget
    /// the policy, and say whether there was one to forget. Nothing is
    /// advertised or withdrawn -- what the entity shares is its draw, which
    /// outlives every policy of the play session.
    ///
    /// What the policy stopped wanting is not wanted again here: that is
    /// [`Self::want_whole`], asked by the callers that leave the entity with
    /// nothing installed, and not by the ones that replace the policy or
    /// retire a sibling.
    fn clear_under(&self, entity: &Entity<B>, claim: &mut Claim) -> bool {
        let mut state = entity.state.lock();
        if state.installed.is_none() {
            return false;
        }
        state.forget_policy(&mut claim.guard);
        true
    }

    /// Forget the entity for `key` once its slack pass has taken the last
    /// of it off the disk and no read is open on it.
    ///
    /// The replacement for the proxy's old 90-second grace, and it prunes on
    /// a fact rather than on a clock: an entity that holds nothing and that
    /// nobody is reading has no window to keep, no head worth remembering
    /// and nothing for a later pass to do. A [`Reader`]'s drop never calls
    /// it -- a read that ends is not a stream that has been replaced, and
    /// the entity it read is kept until something else is played.
    ///
    /// L1 → L2, as [`Self::holdings`] is (rule 1), and never from inside
    /// a pass: the caller holds no [`Claim`] and no `Arc` of the entity by
    /// the time it asks, so an entity a later reader has opened in the gap
    /// keeps itself.
    pub fn forget_empty(&self, key: &B::Key) {
        let forgotten = {
            let mut entities = self.entities.lock();
            let Some(entity) = entities.get(key) else {
                return;
            };
            Arc::strong_count(entity) == 1
                && entity.state.lock().readers.is_empty()
                && entities.remove(key).is_some()
        };
        if forgotten {
            self.backing.forgotten(key);
        }
    }

    /// Nothing bounds `entity` now: every piece of it wanted again
    /// ([`Backing::want_all`]). The policy's passes trimmed the backend's
    /// want-set to the window ([`Backing::want`]), and a piece they left
    /// unwanted stays unwanted until something wants it: nothing on the pin
    /// path recomputes a selection that did not change, so a pinned
    /// single-file torrent would download to the window's edge and stop
    /// there, short of the file the user asked to keep. Under the turn, after
    /// a [`Self::clear_under`] that succeeded.
    async fn want_whole(&self, entity: &Entity<B>) {
        let domain = entity.state.lock().domain.clone();
        self.backing.want_all(&domain).await;
    }

    /// A pin stands on `entity`: once nothing bounds it every piece of it
    /// is wanted again. The pin exit of both passes, under the turn. What it
    /// shares is the pin's to advertise, which it does whole.
    ///
    /// Wanted again only by the pass that forgot a policy, or the first one
    /// after a slack pass took pieces ([`State::slacked`]). This exit is
    /// taken every tick for as long as the pin stands, and a pinned entity
    /// has nothing installed after the first of them; asking for the whole
    /// extent on every one of those is a reselect of every piece of the
    /// file under librqbit's torrent lock every two seconds, for nothing --
    /// a piece the first exit wanted is wanted still. The one that cleared
    /// is the one after which something could be unwanted.
    async fn release_to_pin(&self, entity: &Entity<B>, claim: &mut Claim) {
        let slacked = std::mem::take(&mut entity.state.lock().slacked);
        if self.clear_under(entity, claim) || slacked {
            self.want_whole(entity).await;
        }
    }

    /// The entity's turn, awaited: the torrent's tick and the proxy's slack
    /// sweep queue here, with no owner lock held (rule 3). `None` is a key
    /// with no entity, so nothing to serialise against.
    pub async fn turn(&self, key: &B::Key) -> Option<Claim> {
        let entity = self.lookup(key)?;
        Some(Claim {
            guard: entity.turn.clone().lock_owned().await,
            about: None,
        })
    }

    /// The entity's turn if nobody holds it; never waits. `None` is a pass
    /// in flight, or no such entity. Looks the entity up under L1, so it is
    /// for callers holding nothing; the `try_lock` under L2 that rule 3
    /// allows is [`Reader::note`]'s, on an entity it already holds.
    pub fn try_turn(&self, key: &B::Key) -> Option<Claim> {
        let entity = self.lookup(key)?;
        entity
            .turn
            .clone()
            .try_lock_owned()
            .ok()
            .map(|guard| Claim { guard, about: None })
    }

    /// **A piece the backing has just accepted is ours: commit it if the
    /// draw chose it.** Says whether it joined the committed set.
    ///
    /// The draw was advertised when it was made, so the backend announced
    /// the piece as it completed; what is left here is the bookkeeping. A
    /// drawn piece is committed the moment we are found to hold it -- at
    /// the completion itself, not a pass's next listing -- so that what the
    /// cache counts as committed does not depend on when a pass happens to
    /// run.
    ///
    /// **Off the path that told us.** The caller is librqbit's completion
    /// and may not block, so it hands this to a task; here the entity's
    /// turn is taken like any other writer of a policy, which is what makes
    /// a commit and a pass one order instead of two. A pass that committed
    /// it already, or a policy that was replaced under it, leaves nothing to
    /// commit ([`RetentionPolicy::commit_drawn`]).
    pub async fn commit_completed(&self, piece: u32) -> bool {
        if B::SHARE == Share::Nothing {
            return false;
        }
        let Some(key) = self.drawn_in(piece) else {
            return false;
        };
        let Some(entity) = self.lookup(&key) else {
            return false;
        };
        // No owner lock held across the turn (rule 3), and the policy is
        // asked again under it: the key above is a reading the wait for the
        // turn may have made stale.
        let Some(mut claim) = self.turn(&key).await else {
            return false;
        };
        entity.state.lock().commit_drawn(&mut claim.guard, piece)
    }

    /// Every entity's draw, as the play session made it, with whether a
    /// read or the liveness cell still holds that session open is the
    /// caller's question. L1 → L2, no I/O: the shape [`Self::holdings`]
    /// has. An entity no open has decided yet is not listed.
    pub fn draws(&self) -> Vec<(B::Key, BTreeSet<u32>)> {
        let entities = self.entities.lock();
        entities
            .iter()
            .filter_map(|(key, entity)| {
                let draw = entity.state.lock().draw.clone()?;
                Some((key.clone(), draw))
            })
            .collect()
    }

    /// [`Self::draws`], each with what it was made for ([`DrawnFor`]): what
    /// the driver compares with what is played now, since a draw made for
    /// one member is nothing another member of the same entity shares. L1 →
    /// L2, no I/O, as [`Self::draws`].
    pub fn draws_for(&self) -> Vec<DrawMade<B::Key>> {
        let entities = self.entities.lock();
        entities
            .iter()
            .filter_map(|(key, entity)| {
                let state = entity.state.lock();
                let draw = state.draw.clone()?;
                let made_for = state.drawn_for.clone()?;
                Some((key.clone(), draw, made_for))
            })
            .collect()
    }

    /// **`key`'s draw made for `made_for` has been ended**: the driver
    /// found it shared no more -- the session moved to another member of
    /// the entity, or off a set onto one volume -- and has taken it out of
    /// what the backend advertises with the backend out of the swarm
    /// (`EndShares`). Forget it under the entity's turn, so the session's
    /// next open or pass draws what is played now, as [`Self::end_play_session`]
    /// lets the next open of a deleted file draw afresh -- the session moved
    /// back meanwhile included: the rebuild left that draw out all the
    /// same. A draw recorded since for something else is kept.
    pub async fn forget_draw_made_for(&self, key: &B::Key, made_for: &DrawnFor<B::Key>) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let Some(mut claim) = self.turn(key).await else {
            return;
        };
        let mut state = entity.state.lock();
        if state.drawn_for.as_ref() == Some(made_for) {
            state.forget_draw(&mut claim.guard);
        }
    }

    /// **`pieces` leave every draw made for `made_for`**: one volume of a
    /// played set is being deleted, and the set's union, recorded on every
    /// volume that has drawn or adopted it, shares that volume's pieces no
    /// more -- as [`Self::end_play_session`] ends what a deleted film
    /// shared. Each volume's record is narrowed under its own turn (one at
    /// a time, rule 4) and S, so the driver's next reading of what is
    /// shared ([`Self::draws_for`]) leaves them out and its `EndShares`
    /// takes them out of the announcement. A volume adopting the union
    /// meanwhile copies it from a sibling not narrowed yet; the sweep runs
    /// again until no record made for the set holds any of `pieces`.
    pub async fn withdraw_from_draws_made_for(
        &self,
        made_for: &DrawnFor<B::Key>,
        pieces: &BTreeSet<u32>,
    ) {
        loop {
            let holding: Vec<Arc<Entity<B>>> = {
                let entities = self.entities.lock();
                entities
                    .values()
                    .filter(|entity| {
                        let state = entity.state.lock();
                        state.drawn_for.as_ref() == Some(made_for)
                            && state
                                .draw
                                .as_ref()
                                .is_some_and(|draw| !draw.is_disjoint(pieces))
                    })
                    .cloned()
                    .collect()
            };
            if holding.is_empty() {
                return;
            }
            for entity in holding {
                let mut claim = Claim {
                    guard: entity.turn.clone().lock_owned().await,
                    about: None,
                };
                let _set_turn = self.set_draws.lock();
                let mut state = entity.state.lock();
                if state.drawn_for.as_ref() != Some(made_for) {
                    continue;
                }
                let Some(draw) = state.draw.as_ref() else {
                    continue;
                };
                let narrowed: BTreeSet<u32> = draw.difference(pieces).copied().collect();
                state.adopt_draw(&mut claim.guard, narrowed, made_for.clone());
            }
        }
    }

    /// Decide `key`'s draw now if it is due, as an open would: what a test
    /// that states the film's length after the open asks, rather than
    /// opening the file a second time or running a pass.
    #[cfg(test)]
    pub(crate) async fn settle_draw(&self, key: &B::Key) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let mut claim = Claim {
            guard: entity.turn.clone().lock_owned().await,
            about: None,
        };
        self.decide_draw_if_due(&entity, &mut claim, None).await;
    }

    /// **`key`'s play session ends with its file**: the file is being
    /// deleted, and what its session drew is shared no more. The entity
    /// goes slack under its turn -- no policy, no draw, no player's open --
    /// so the next open of the file is a new session that draws afresh.
    /// For a caller that has made sure no read of the file is open.
    pub async fn end_play_session(&self, key: &B::Key) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let Some(mut claim) = self.turn(key).await else {
            return;
        };
        entity.state.lock().go_slack(&mut claim.guard);
    }

    /// `key`'s draw, as its play session made it, or `None` for a key with
    /// no entity or a session that has not decided one. L1 to look up, then
    /// L2.
    pub fn draw_of(&self, key: &B::Key) -> Option<BTreeSet<u32>> {
        self.lookup(key)?.state.lock().draw.clone()
    }

    /// The key of the entity whose standing policy has drawn `piece` and
    /// not committed it yet, or `None` -- which is every piece of every
    /// file nothing is bounding, and all but a fraction of a percent of
    /// what a bounded one completes.
    ///
    /// **Asked before the turn**, so that the completions nothing can be
    /// done about do not queue behind a retention pass to find that out. It
    /// is a reading and not a decision: the turn is taken after it and the
    /// policy is asked again under it
    /// ([`RetentionPolicy::commit_drawn`]).
    ///
    /// L1 and one L2 read per entity, no I/O: the shape [`Self::holdings`]
    /// has, without copying a committed set out per piece. A torrent has
    /// one policy standing at a time, so this is a walk of one or two
    /// entities.
    fn drawn_in(&self, piece: u32) -> Option<B::Key> {
        let entities = self.entities.lock();
        entities
            .iter()
            .find(|(_, entity)| {
                entity
                    .state
                    .lock()
                    .installed
                    .as_ref()
                    .is_some_and(|installed| installed.policy.draws(piece))
            })
            .map(|(key, _)| key.clone())
    }

    /// One pass over `key`, with its turn in hand and the [`Mode`] its
    /// driver decided.
    ///
    /// [`Mode::Slack`] is the short one: the entity is not being played and
    /// nothing is reading it, so -- once nothing of it is announced in the
    /// swarm -- every byte of it is taken off the disk. It keeps
    /// nothing, so it measures nothing -- no window, no commit, no want-set
    /// -- and it leaves nothing installed. [`Mode::Live`] is the pass the
    /// rest of this file describes.
    pub async fn pass(&self, key: &B::Key, store: &B::Store, claim: Claim, mode: Mode) -> Outcome {
        self.pass_at(key, store, claim, mode, std::time::Instant::now())
            .await
    }

    /// [`Self::pass`] with the clock handed in: `now` is what the pass
    /// measures against on the far side of its listing ([`Asking::now`]),
    /// so a harness that stamps its reads from its own clock can run the
    /// pass on that clock too. A pass that read the wall clock beside reads
    /// stamped `t0 + elapsed` saw no time pass at all: no stream ever
    /// idled, no report was ever due.
    pub async fn pass_at(
        &self,
        key: &B::Key,
        store: &B::Store,
        claim: Claim,
        mode: Mode,
        now: std::time::Instant,
    ) -> Outcome {
        match mode {
            Mode::Live => self.live_pass(key, store, claim, now).await,
            Mode::Slack { opens } => self.slack_pass(key, store, claim, opens).await,
        }
    }

    /// The entity is slack: its play session is over, and everything it
    /// holds goes.
    ///
    /// **Never while the backend is in the swarm and advertises a piece of
    /// it** ([`Backing::announced_in_swarm`]). Such a piece is one a peer may
    /// have been told we have, there is no un-Have, and an announcement ends
    /// only with the torrent leaving the swarm -- which is the driver's to
    /// do first (the torrent's `EndShares`). So the pass takes nothing then,
    /// and runs once the backend is out of the swarm, or for an entity that
    /// shares nothing.
    ///
    /// Then the policy, the windows, the decision and the draw are dropped
    /// under L2 -- an entity nobody is playing has no window, and leaving
    /// one standing is what let the cleaner's gate call a left file's
    /// pieces protected. Then every held run this file alone owns goes
    /// through the same [`Backing::reclaim`] the live pass uses, asking a
    /// [`Door`] that answers "take everything" -- except under a pin, and
    /// except once the entity has become live again, either of which stops
    /// the run where it stands.
    ///
    /// An entity left holding nothing, with no read open on it, is
    /// forgotten ([`Self::forget_empty`]). Pieces a delete refused -- a
    /// hash check running -- stay held, the entity stays, and the next tick
    /// offers them again.
    ///
    /// **Slack is re-established under the turn before anything is
    /// destroyed, and the whole of the pass hangs on that.** The driver
    /// decided this mode from a reading of the liveness cell taken before
    /// its first entity, and between that reading and this turn a viewer
    /// can have started exactly this file: the cell moved, `install` put a
    /// policy in, a reader opened on it. Carried through, the mode would
    /// have the pass hold the window they are inside back from the swarm,
    /// throw the policy away and unlink the bytes they are reading -- and
    /// the door below, which does ask again, gates only the unlinks and by
    /// then the policy is already gone. So the three facts the driver read
    /// are read again here, under the turn that every open must take:
    /// [`Backing::is_live`], the open reads, and [`State::opens`] against
    /// the count the mode carries. `opens` is the one that closes the gap
    /// rather than narrowing it -- the other two are instants, and an
    /// install that has completed but whose reader is not open yet is
    /// neither.
    async fn slack_pass(
        &self,
        key: &B::Key,
        store: &B::Store,
        mut claim: Claim,
        opens: u64,
    ) -> Outcome {
        let Some(entity) = self.lookup(key) else {
            drop(claim);
            return Outcome {
                concluded: None,
                again: None,
            };
        };
        let about = claim.about;
        // A pin is a retention property and outranks the slack: the user
        // asked for those bytes. Nothing is taken -- and what stopped being
        // wanted is wanted again, exactly as the live pass does under a pin.
        // This exit is not the rare one: the driver reads a pinned file
        // nobody is playing as slack, so it is the exit every tick takes
        // over a pinned download that is not being watched. "The pin's own
        // install clears the policy" was the assumption here, and the
        // install runs only when the file is opened; a pin on a file a slack
        // pass had already dropped pieces of left the pieces unwanted, with
        // nothing due to run over it but this.
        //
        // Asked before L2, like every copy-out of a lock outside the owner
        // (rule 1), and so is the liveness cell beside it.
        if self.backing.keeps_everything(key) {
            self.release_to_pin(&entity, &mut claim).await;
            return Self::slack_nothing(claim);
        }
        if self.backing.is_live(key) {
            return Self::slack_nothing(claim);
        }
        // Read under L2 and logged after it (rule 2: no tracing under the
        // lock).
        let opened_since = {
            let state = entity.state.lock();
            state.opens != opens || state.observed_readers() > 0
        };
        if opened_since {
            tracing::debug!(
                key = ?key,
                "a stream opened on this entity since its mode was decided; \
                 the slack pass takes nothing"
            );
            return Self::slack_nothing(claim);
        }
        let domain = entity.state.lock().domain.clone();
        // Where a test puts what playback does while the pass runs.
        self.run_hook();
        let Some(held) = self.backing.held(store, &domain).await else {
            tracing::warn!(
                key = ?key,
                "the slack pass has no reading of the disk; this pass takes nothing"
            );
            return Self::slack_nothing(claim);
        };
        let candidates: Vec<u32> = held.iter().copied().collect();
        let alone = self.backing.alone(&domain, &candidates).await;
        // What a peer may have been told stays until the swarm has been
        // left: asked of exactly the pieces this pass would take, with the
        // turn held. That holds this entity's own draw back -- advertised
        // only under its install, which takes this turn -- but not a set's:
        // the union is advertised by whichever volume drew it, under that
        // volume's turn, so pieces of this entity can become advertised
        // behind this answer. What keeps those is the door's reading of the
        // recorded draws at every unlink ([`Door::drawn`]): a draw is
        // recorded before it is advertised. A boundary piece a pinned
        // neighbour shares is not among them and does not hold the rest up.
        if B::SHARE == Share::Half && self.backing.announced_in_swarm(&alone).await {
            tracing::debug!(
                key = ?key,
                "a slack entity holds pieces a peer may have been told of; \
                 nothing of it goes until the torrent has left the swarm"
            );
            return Self::slack_nothing(claim);
        }
        entity.state.lock().go_slack(&mut claim.guard);
        let door = Door {
            state: entity.state.clone(),
            backing: self.backing.clone(),
            key: key.clone(),
            entities: self.entities.clone(),
            mode: Mode::Slack { opens },
            // A slack entity keeps nothing it is not still handing out, and
            // what it is still handing out is a promise, which this door
            // asks the readers for at the unlink itself.
            exempt: std::sync::Arc::new(crate::retention::exempt::Exempt::for_pieces(0)),
        };
        // And what playback does while the unlinks run.
        self.run_hook();
        let reclaimed = self
            .backing
            .reclaim(store, &domain, runs(&alone), door)
            .await;
        let empty = {
            let state = entity.state.lock();
            // Against what this pass could take, not against everything
            // held: a boundary piece another file shares is never among
            // `alone`, so a file that has one was never "empty" by the
            // held count, and its entity outlived every slack pass.
            let empty = reclaimed >= alone.len() && state.readers.is_empty();
            // Nothing is installed after `go_slack`, so no exit of a slack
            // pass ever owes another one; the claim goes here like any
            // other pass's (rule 3).
            let again = Self::release(&state, claim, about, None);
            debug_assert!(again.is_none(), "a slack pass armed another");
            drop(again);
            empty
        };
        // The entity's own `Arc` goes before the map is asked, so
        // `forget_empty`'s "nothing but the map holds it" reading is about
        // everybody else and not about this pass.
        drop(entity);
        if empty {
            self.forget_empty(key);
        }
        Outcome {
            concluded: Some(Conclusion {
                reclaimed,
                ..Conclusion::default()
            }),
            again: None,
        }
    }

    /// One pass over `key`, with its turn in hand. See the module docs for
    /// the steps. A pass that concluded nothing -- a pin, nothing installed,
    /// no head in this domain before or after the listing, a listing we do
    /// not have, or a decide that ran under us -- answers with no
    /// [`Conclusion`], and every one of them leaves the policy exactly where
    /// it was; a pass that ran and found nothing to do answers with zeroed
    /// counts.
    ///
    /// **Every exit decides `again`**, under L2, with the claim dropped or
    /// handed on inside the same block (rule 3). A byte delivered while the
    /// pass held the turn started nothing; if it was the last of its body,
    /// this decision is the only thing that remembers it wanted a pass, and
    /// that is as true of a pass that refused at its re-read as of one that
    /// concluded. The rule is one: a pass that measured something owes
    /// another if the head moved a stride from what it measured, and a pass
    /// that measured nothing because the policy was replaced under it owes
    /// one outright, because the byte that replaced it was due and could not
    /// start one.
    async fn live_pass(
        &self,
        key: &B::Key,
        store: &B::Store,
        mut claim: Claim,
        now: std::time::Instant,
    ) -> Outcome {
        let Some(entity) = self.lookup(key) else {
            // No entity: no L2 to decide under, and no note that could race
            // the release, because a note is made on an entity.
            drop(claim);
            return Outcome {
                concluded: None,
                again: None,
            };
        };
        let about = claim.about;
        // 1. A pin taken while the entity was already playing leaves the
        // policy installed: `install` is the only other place that asks,
        // and it ran before the pin existed. Without this a pinned file is
        // reclaimed under its own reader -- measured, half a 32 MiB file
        // deleted with the pin set throughout -- while librqbit re-fetches
        // it in a loop.
        if self.backing.keeps_everything(key) {
            self.release_to_pin(&entity, &mut claim).await;
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, None);
        }
        // 1a. The play session's draw, if it waited for the film's rate and
        // the rate has come -- or the wait has run out. Before the listing,
        // so this pass already commits and keeps it.
        self.decide_draw_if_due(&entity, &mut claim, Some(now))
            .await;
        // 1b. And whether it is still waiting on the file's first bytes: a
        // play session on the file, asked off L2 (rule 2). See the head
        // held below.
        let session_plays = B::SHARE != Share::Nothing && self.backing.plays(key);
        // 2. Only which domain and which budget, and the head as a filter:
        // an entity whose reader has moved on must not pay a listing per
        // tick to discover it has nothing to say.
        let begin = {
            let state = entity.state.lock();
            match state.begin(about) {
                Some(begin) => begin,
                None => return Self::nothing(&state, claim, about, None),
            }
        };
        // 3. Where a test puts what playback does while the listing runs.
        self.run_hook();
        // 4. What the disk holds, from the backing: a memory read for the
        // torrent, a listing off the reactor for the proxy. Where it
        // suspends, it is the long suspension of the pass, and the reason
        // the deciding reading below is taken on the far side of it.
        let Some(held) = self.backing.held(store, &begin.domain).await else {
            tracing::warn!(
                key = ?key,
                "the retention pass has no reading of the disk; this pass concludes nothing"
            );
            // Nothing measured, so nothing to write -- but a byte that moved
            // the head a stride while the listing failed is owed its pass,
            // and asking that of the head this pass started from is what
            // keeps a disk that will not list from being asked once per
            // byte rather than once per stride.
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, Some(begin.at));
        };
        // The one reading of the clock this pass measures against, handed
        // in by `pass_at`; see `Asking::now`.
        // **What this entity's consumers are asking of the disk**, answered
        // against the listing above: what to fetch ahead of them, what no
        // unlink may touch, and what to give back if something must go.
        //
        // Every number handed over is a published one. The file's own
        // arithmetic -- size over duration -- is the ceiling every stream
        // on it is fetched at, and the only honest absolute number there
        // is: a starving player's reads measure our delivery and not its
        // consumption (`retention::streams::Stream::sample`). The seconds
        // are the viewer's buffer profile, which is the viewer's decision
        // and not the disk's. The cap is over the whole cache and the
        // headroom is what the volume will still give; what turns those two
        // into this entity's allowance is what this entity holds -- the
        // listing above -- and that is the backing's to price.
        let (held_by_readers, readers_margin);
        let asking = {
            let state = entity.state.lock();
            let (buffering, stride, domain) =
                (state.buffering(), state.stride, state.domain.clone());
            let committed = state
                .installed
                .as_ref()
                .map(|installed| {
                    crate::retention::sorted_runs(installed.policy.advertised().iter().copied())
                })
                .unwrap_or_default();
            let mut holding: Vec<Range<u32>> = state
                .readers
                .values()
                .map(|reader| reader.promised.clone())
                .filter(|range| !range.is_empty())
                .collect();
            let asking: Vec<Range<u32>> = state
                .readers
                .values()
                .map(|reader| reader.asking.clone())
                .filter(|range| !range.is_empty())
                .collect();
            let lookaheads: Vec<Range<u32>> = state
                .readers
                .values()
                .filter_map(|reader| {
                    let head = B::index_of(&state.domain, reader.head()?)?;
                    let ahead = u32::try_from(
                        reader
                            .buffering
                            .lookahead_bytes
                            .div_ceil(B::piece_length(&state.domain)?.max(1)),
                    )
                    .unwrap_or(u32::MAX);
                    Some(head..head.saturating_add(ahead).saturating_add(1))
                })
                .collect();
            holding.extend(lookaheads.iter().cloned());
            // What the readers hold, before the askers' reach joins it:
            // what an asker's room is measured against.
            held_by_readers = holding.clone();
            // **An asker's reach is a lookahead for these two purposes and
            // no other**: the backend refuses to forget a piece a stream is
            // asking for, so offering one to the reclaim frees nothing; and
            // what of it has not arrived is about to, so the margin has to
            // leave it room. It is in no want-set and no promise, and the
            // moment the asker moves on it is in neither of these.
            holding.extend(asking.iter().cloned());
            drop(state);
            // What the open streams will still fetch: their lookaheads, in
            // this entity's extent, less what the listing already found.
            // Only that is still to arrive -- a lookahead piece on the disk
            // is in `held` and priced there -- and counting the whole
            // lookahead counted every fetched piece of it twice. Under
            // `Maximum` a first open's lookahead is the whole cap, so it
            // was an allowance of nothing for the life of the stream.
            let extent = B::extent(&domain);
            let still_to_come = |runs: &[Range<u32>]| {
                runs.iter()
                    .flat_map(|run| run.start.max(extent.start)..run.end.min(extent.end))
                    .filter(|piece| !held.contains(piece))
                    .collect::<BTreeSet<u32>>()
            };
            let to_readers = still_to_come(&lookaheads);
            let unfetched = to_readers.union(&still_to_come(&asking)).count() as u64;
            let piece_length = B::piece_length(&domain).unwrap_or(0);
            // The margin the readers' own fill needs, without the askers':
            // what their room is measured under.
            readers_margin = (to_readers.len() as u64)
                .max(u64::from(stride))
                .saturating_mul(piece_length);
            Asking {
                budget: begin.budget,
                headroom: self.budget.headroom(),
                ceiling: buffering.bytes_per_second,
                // No reader has stated a profile: the whole file, bounded
                // by the allowance, as under `Maximum`.
                seconds: buffering
                    .window_seconds
                    .unwrap_or(crate::backend::priorities::MAXIMUM_WINDOW_SECONDS),
                now,
                holding,
                committed,
                // **What arrives between this pass and the next one.**
                //
                // Two readings of the same thing and the larger wins. What
                // the open readers' streams are still pulling -- their
                // lookaheads less what is already held, above -- is exact
                // when there is a reader: one that has neither promised nor
                // delivered is not in the entity's map, and a torrent fills
                // for a file nobody has read a byte of yet. The stride is
                // what is left then: it is defined as how far the head may
                // move before another pass, which is the same interval
                // measured in pieces.
                //
                // Without it the allowance spends the whole budget, and the
                // disk sits a stride *over* the line the budget exists to
                // keep it under.
                margin: unfetched
                    .max(u64::from(stride))
                    .saturating_mul(piece_length),
            }
        };
        // What an asker's room is measured from, kept beside the asking
        // the backing is about to take.
        let measure = asking.clone();
        let consumers = self.backing.reading(&begin.domain, &held, asking);
        // **How much an asker may ask for in all** ([`Room`]): what the
        // entity may hold, less every piece its readers hold that no
        // reclaim can give back -- what they are fetched for, what they
        // promised, what their streams reach, and the committed set.
        let room = {
            let extent = B::extent(&begin.domain);
            let piece = B::piece_length(&begin.domain).unwrap_or(0).max(1);
            let kept = consumers
                .want
                .iter()
                .chain(&held_by_readers)
                .chain(&measure.committed)
                .flat_map(|run| run.start.max(extent.start)..run.end.min(extent.end))
                .collect::<BTreeSet<u32>>()
                .len() as u64;
            (measure.within(piece, held.len(), readers_margin) / piece).saturating_sub(kept)
        };
        // 5. **The deciding reading, taken after the listing.** Read before
        // the walk the head is the older half of the pair: the window is
        // drawn round where playback *was*, everything the fill wrote ahead
        // of it is outside that window, on the disk, and reclaimed --
        // measured on the proxy before it was reordered, a 16 MB read left
        // an empty directory under an 8 MB budget after two passes. Read
        // after it, the decision may name pieces the listing did not find,
        // which unlinks nothing, because the listing is the candidate set.
        //
        // And the refusal: a policy decided under us -- `install_now` from a
        // delivered byte, which is legal under L2 alone -- is a different
        // shape, and this listing was measured for the old one. It concludes
        // nothing rather than advance the new policy over a reading it never
        // asked for, or the old one that no longer exists. The byte that
        // decided it is owed the pass it could not start: `nothing` with no
        // measurement hands the claim on while something is installed.
        // Asked once per pass: whether the trace target is on at all
        // (`crate::retention::trace`). A disabled target is refused by
        // `tracing` at each call site anyway; this is what keeps the work
        // that only feeds those lines from being done for nothing.
        let tracing_on =
            tracing::enabled!(target: "enginefs::retention::trace", tracing::Level::INFO);
        let (decision, want_windows, door_policy, at, consumed_at, traced) = {
            let mut state = entity.state.lock();
            if !state.still(&begin) {
                return Self::nothing(&state, claim, about, None);
            }
            // **The cadence's head, which is the readers'.** A pass is
            // owed another one when a byte moved this head a stride while
            // the pass held the turn, and that chain terminates only
            // because nothing a pass does moves it; measured from one
            // source and compared against another it would never
            // terminate, and the proxy's cache never settled.
            let Some(at) = state
                .head(about)
                .and_then(|head| B::index_of(&state.domain, head))
            else {
                return Self::nothing(&state, claim, about, None);
            };
            // **Where the entity is being consumed, which is the
            // detector's.** Which of several readers of one file is the
            // viewer is a question about behaviour, and the readers cannot
            // answer it: mpv keeps an index crawler open beside the viewer
            // for the life of a film. What this is for is saying where the
            // entity is -- the trace's head, and the split of what is held
            // behind it and ahead -- and not what a pass keeps. See
            // [`Consumers::at`].
            let consumed_at = consumers.at.unwrap_or(at);
            let promised: Vec<Range<u32>> = state
                .readers
                .values()
                .map(|reader| reader.promised.clone())
                .filter(|range| !range.is_empty())
                .collect();
            // For [`crate::retention::trace`]: the heads and the lookaheads
            // of the reads that are open, and what the read that owns the
            // head this pass measures from is for. Gathered only while the
            // trace is on: a pass runs every two seconds per file, and the
            // `diagnosticsTrace` setting is off on every device that is not
            // being diagnosed.
            let traced = tracing_on.then(|| {
                let readers: Vec<(u32, u64)> = state
                    .readers
                    .values()
                    .filter_map(|reader| {
                        Some((
                            B::index_of(&state.domain, reader.head()?)?,
                            reader.buffering.lookahead_bytes,
                        ))
                    })
                    .collect();
                (readers, state.buffering())
            });
            let Some((decision, policy)) =
                state.advance(&mut claim.guard, &consumers.reclaim, &held)
            else {
                return Self::nothing(&state, claim, about, None);
            };
            // **One list per entity, not one window per playhead.** What is
            // kept and fetched is the run of the file each detected
            // consumer is moving through; the policy sizes that rather than
            // placing it. A pass is one decision about what to give back
            // for the whole entity.
            //
            // **What to keep and what to fetch are one list, and it is the
            // detector's.** A window is a promise not to delete and an
            // order to the swarm to fill it, and both are about the same
            // thing: the run of disk a consumer is moving through. A probe
            // -- a read of the container index -- is a consumer like any
            // other, with its own short run and its own share of the
            // seconds, not a separate classification owed a promise but no
            // fetch.
            //
            // The promise is kept at the door through the published set
            // rather than through this list ([`crate::retention::exempt`]),
            // so an unlink asks a bit rather than this lock.
            let want = consumers.want.clone();
            // **While the stream is still assembling itself, want what is
            // parked on rather than the window.**
            //
            // What a player needs before it can show a frame is a handful
            // of pieces -- the header, the container's index, the seek
            // target -- and they have to be *in* the want-set or nothing
            // orders them. What they must not be is drowned: a window is
            // hundreds of pieces, and a swarm spread over hundreds takes
            // its time over the three.
            //
            // The answer is not to want only those three, which was tried
            // and was worse. librqbit reserves a piece to exactly one peer
            // (`PieceTracker::inflight`), and a second peer holding it can
            // only take over by stealing, which wants a 3x or 10x speed
            // advantage it cannot demonstrate with nothing else to
            // download. A want-set of one piece is therefore a download
            // from one peer: 117 kB/s in the field, against 3.1 MB/s over
            // the same swarm a minute later.
            //
            // So: everything still missing, each with a short run after it.
            // The full window returns the moment nothing is outstanding,
            // which needs no decision about when start-up ended -- a
            // promise is cleared by the byte that unparks the read that
            // made it.
            let outstanding: Vec<Range<u32>> = promised
                .iter()
                .filter(|range| (range.start..range.end).any(|piece| !held.contains(&piece)))
                .map(|range| range.start..range.end.saturating_add(STARTUP_RUN))
                .collect();
            let mut want = if outstanding.is_empty() {
                want
            } else {
                outstanding
            };
            // **The file's first bytes, while the draw waits on them.** A
            // resumed film is read at its head only for the container's
            // header, and then at the resume point: trimmed as outside the
            // window, a head piece still in flight would never arrive, what
            // the file is would never be known, and the play session would
            // share nothing for good. So until it is known the head stays
            // wanted and out of every unlink -- a piece or two, fetched for
            // this server alone: nothing is shared before the draw is made.
            let head = (session_plays
                && state.player_opened
                && state.draw.is_none()
                && state.content.is_none())
            .then(|| Self::head_of(&state.domain))
            .flatten();
            if let Some(head) = head.clone() {
                want.push(head);
            }
            // **The published set is the backing's**, which is where the
            // want set is decided and where what may not be unlinked has to
            // be decided with it. What is kept here is the handle, so a
            // read that parks between passes can write its promise into the
            // same set under this same lock (`Reader::promises`).
            state.exempt = Some(consumers.exempt.clone());
            // Where the detector found the entity being consumed, for the
            // head every reading splits at ([`State::consumed_at`]).
            state.consumed_at = consumers.at;
            // And every promise live *now*, re-held under this lock. The
            // publication above was computed from the promises the pass
            // read before it ran; one made in between wrote into the set
            // and was then overwritten by it, and what that costs is a
            // chunk deleted out of a body a player has already been
            // promised the length of.
            for reader in state.readers.values() {
                if !reader.promised.is_empty() {
                    consumers.exempt.hold(reader.promised.clone());
                }
            }
            // And the play session's draw, held or not: advertised, so a
            // piece of it that completes is announced at once, and so no
            // unlink of this pass may take it -- the reclaim's or the
            // want-set's late arrivals ([`Backing::want`]). The reclaim
            // never offers a committed piece anyway; this is what covers a
            // drawn piece that completed after the listing.
            if let Some(draw) = state.draw.as_ref() {
                for run in runs(&draw.iter().copied().collect::<Vec<_>>()) {
                    consumers.exempt.hold(run);
                }
            }
            if let Some(head) = head {
                consumers.exempt.hold(head);
            }
            (decision, want, policy, at, consumed_at, traced)
        };
        // 6. Nothing to announce here: the draw was advertised when it was
        // made, and the backend announces each piece of it as it completes.
        // Nothing to withdraw either -- a committed piece the disk lost is
        // one the backend no longer has, and it announces only what it has.
        let conclusion = Conclusion {
            // What a pass concluded is what its consumers were asking for:
            // the holdings panel and the proxy's "is anything live inside
            // this chunk" read the same list.
            windows: consumers.want.clone(),
            committed: decision.committed.len(),
            lost: decision.lost.len(),
            ..Conclusion::default()
        };
        debug_assert!(
            B::SHARE == Share::Half || (decision.committed.is_empty() && decision.lost.is_empty()),
            "a Share::Nothing policy committed or lost pieces"
        );
        // The door every unlink of this pass asks, the want-set's included.
        // This entity's own draw is advertised only under its turn, which
        // the pass holds throughout, and it is held above; a set's draw
        // advertised by a sibling volume under the sibling's turn is the
        // door's to refuse, from the record made before that advertise
        // ([`Door::drawn`]).
        let door = Door {
            state: entity.state.clone(),
            backing: self.backing.clone(),
            key: key.clone(),
            entities: self.entities.clone(),
            mode: Mode::Live,
            exempt: consumers.exempt.clone(),
        };
        // And the want-set, trimmed to what this pass keeps: the windows
        // wanted, everything of the entity outside them and not on the disk
        // not wanted. What is on the disk and outside them is the reclaim's,
        // below.
        self.backing
            .want(store, &begin.domain, &want_windows, &held, &door)
            .await;
        // 7. And what playback does while the unlinks run.
        self.run_hook();
        // 8. The reclaim, asking the door at every unlink.
        let alone = self.backing.alone(&begin.domain, &decision.reclaim).await;
        let mut conclusion = conclusion;
        conclusion.reclaimed = self
            .backing
            .reclaim(store, &begin.domain, runs(&alone), door)
            .await;
        // The pass's trace lines; see [`crate::retention::trace`] for what
        // turns them on. Everything here is for them alone, so none of it is
        // done while they are off.
        if let Some((readers, buffering)) = traced {
            let backing = self.backing.trace(store, &begin.domain);
            let name = trace::name(key, backing.as_ref());
            if let Some(piece_length) = backing.as_ref().map(|backing| backing.piece_length) {
                for (head, lookahead) in &readers {
                    let ahead = head.saturating_add(
                        u32::try_from(lookahead.div_ceil(piece_length.max(1))).unwrap_or(u32::MAX),
                    );
                    let inside: Vec<u32> = decision
                        .reclaim
                        .iter()
                        .copied()
                        .filter(|piece| (*head..ahead).contains(piece))
                        .collect();
                    if !inside.is_empty() {
                        trace::planned_to_reclaim_inside_a_lookahead(
                            &name,
                            &inside,
                            *head..ahead,
                            *lookahead,
                        );
                    }
                }
            }
            let (behind, ahead) = held
                .iter()
                .filter(|piece| B::extent(&begin.domain).contains(piece))
                .partition::<Vec<u32>, _>(|piece| **piece < consumed_at);
            trace::pass(
                &name,
                now,
                trace::Pass {
                    playhead: consumed_at,
                    wanted: want_windows
                        .iter()
                        .map(|window| window.end - window.start)
                        .sum(),
                    held_behind: behind.len(),
                    held_ahead: ahead.len(),
                    committed: door_policy.advertised().len(),
                    budget: begin.budget,
                    lookahead_bytes: buffering.lookahead_bytes,
                    bytes_per_second: buffering.bytes_per_second,
                    dropped: B::extent(&begin.domain)
                        .filter(|piece| {
                            !want_windows.iter().any(|window| window.contains(piece))
                                && !held.contains(piece)
                        })
                        .count(),
                    unlinked: conclusion.reclaimed,
                    backing: backing.as_ref(),
                },
            );
        }
        // 9. Conclude, under L2, with the claim released or handed on inside
        // the same block (rule 3).
        let mut state = entity.state.lock();
        // A budget published while this pass ran has already rebuilt the
        // policy for the shape it makes and made every reader due under it;
        // this pass measured the old one, and its conclusion is not the
        // current one. Neither the windows nor `passed_at` are written then:
        // the windows would be a measurement against a cap nobody holds, and
        // `passed_at` would keep a reader the new shape made due from being
        // due until it had travelled a whole *new* stride. The policy needs
        // no guard of its own: it was never out.
        let current = state.still(&begin);
        if current {
            if let Some(reader) = about.and_then(|about| state.readers.get_mut(&about)) {
                reader.passed_at = Some(at);
            }
            state.conclude(&mut claim.guard, conclusion.windows.clone());
            state.room = Some((begin.budget, room));
        }
        // Whether this pass swallowed the trigger for the next one. A
        // delivered byte while a pass is running starts none and does not
        // remember that it wanted one -- right for every byte but the last
        // of a body, which brings nothing after it. So the pass asks the
        // same `head` it measured from: the distance is zero unless a byte
        // really went out in between, and each arming is paid for by one
        // such byte, so the chain terminates. A pass whose measurement was
        // refused measured nothing this question can be asked of, and the
        // byte that refused it is owed a pass outright.
        let again = Self::release(&state, claim, about, current.then_some(at));
        Outcome {
            concluded: Some(conclusion),
            again,
        }
    }

    /// The end of a pass that concluded nothing: `again` decided under the
    /// L2 guard the caller holds, and the claim dropped or handed on before
    /// that guard goes (rule 3). `measured` as for [`State::owes_a_pass`].
    /// A slack pass that takes nothing lets its turn go and hands nothing
    /// on. **Every exit before `go_slack`, not only the last**: they are
    /// all the entity turning out to be pinned or live, or a listing that
    /// failed, and the slack driver has nowhere to hand a claim to --
    /// `drop_slack` reads nothing back. Under [`Trigger::OnMove`]
    /// [`Self::release`] would answer "again" here whenever a policy stands
    /// and a head is in the domain, so the claim was handed back and
    /// dropped unread; a driver that looped on the answer would spin. A
    /// byte due on an entity that has become live is the next live pass's,
    /// armed by the byte after it.
    fn slack_nothing(claim: Claim) -> Outcome {
        drop(claim);
        Outcome {
            concluded: None,
            again: None,
        }
    }

    fn nothing(
        state: &State<B>,
        claim: Claim,
        about: Option<ReaderId>,
        measured: Option<u32>,
    ) -> Outcome {
        Outcome {
            concluded: None,
            again: Self::release(state, claim, about, measured),
        }
    }

    /// Drop the claim or hand it on, under the L2 guard the caller holds.
    /// The one place a pass lets go of its turn: a byte that lands after
    /// this and before the guard goes finds the turn free, and one that
    /// landed before it was counted by `owes_a_pass`.
    fn release(
        state: &State<B>,
        claim: Claim,
        about: Option<ReaderId>,
        measured: Option<u32>,
    ) -> Option<Claim> {
        if state.owes_a_pass(about, measured) {
            Some(claim)
        } else {
            drop(claim);
            None
        }
    }

    /// Every entity's holding. L1 → L2, no I/O, and nothing is pruned
    /// here: an entity goes when a [`Mode::Slack`] pass has taken the last
    /// of it off the disk ([`Self::forget_empty`]), which is a fact about
    /// the entity rather than an age.
    pub fn holdings(&self) -> Vec<(B::Key, Holding<B>)> {
        let entities = self.entities.lock();
        entities
            .iter()
            .map(|(key, entity)| (key.clone(), entity.state.lock().holding()))
            .collect()
    }

    /// One entity's holding, or `None` for a key with no entity. No pruning.
    pub fn holding(&self, key: &B::Key) -> Option<Holding<B>> {
        let entity = self.lookup(key)?;
        Some(entity.state.lock().holding())
    }

    /// How far ahead of `at` the window this position is in reaches, with
    /// the domain it is measured in, or `None` when nothing bounds the
    /// entity, `at` is not in its domain, or no window of the last pass
    /// reaches past it. One L2 reading and no I/O: what a reader about to
    /// open is sized by, so the pieces it asks the backend to fetch ahead
    /// are pieces the next pass will keep.
    pub fn reach(&self, key: &B::Key, at: B::Position) -> Option<(B::Domain, Range<u32>)> {
        let entity = self.lookup(key)?;
        let state = entity.state.lock();
        state.installed.as_ref()?;
        let index = B::index_of(&state.domain, at)?;
        // **What the last pass said this entity is asking for**, at the
        // window this position is in. Not a window drawn round the position
        // from a budget: what is being fetched for is what the consumers
        // are being fetched for, and a stream that read past it would have
        // the next pass refuse to keep what it just pulled.
        //
        // The window *in front of* this position, which is where a consumer
        // here is being fetched to: a want set is drawn from where a
        // consumer has got to and reaches forward, so a reader is normally
        // just behind the window rather than inside it.
        let reach = state
            .windows
            .iter()
            .filter(|window| window.end > index)
            .min_by_key(|window| window.start)
            .cloned()?;
        Some((state.domain.clone(), reach))
    }

    /// **The entity's own bitrate**: its size over the duration a player
    /// stated, or `None` where nobody has stated one.
    ///
    /// It is what a second of this film costs, and it is the only absolute
    /// number about playback that arithmetic can give -- exact at the first
    /// report, nothing to converge, and no read pattern can distort it. A
    /// stream's lookahead is this times the seconds the viewer asked to
    /// have buffered, which makes what the swarm is asked for and what the
    /// disk is kept for one answer with one source
    /// ([`crate::retention::streams`] sizes the want set from the same
    /// number).
    ///
    /// Asked of the entity rather than of a reader, because a reader
    /// opening is the first thing that asks and it is not in the map yet.
    pub fn bitrate(&self, key: &B::Key) -> Option<u64> {
        let entity = self.lookup(key)?;
        let buffering = entity.state.lock().buffering();
        buffering.bytes_per_second
    }

    /// **The most a stream may read ahead whatever anything else says**:
    /// what the whole cache may hold.
    ///
    /// Reading further than the cache can keep is the one thing a lookahead
    /// may not do. The pass cannot keep what is past the cap, the backend
    /// refuses to forget a piece a live stream is reading ahead over, and
    /// the two together leave the disk over its budget for the life of the
    /// stream. `None` is a cache nothing bounds.
    pub fn cap(&self) -> Option<u64> {
        match self.budget.get() {
            CacheBudget::Bytes(bytes) => Some(bytes),
            CacheBudget::Unbounded | CacheBudget::Unknown => None,
        }
    }

    /// How many open reads have promised pieces or delivered a byte and have
    /// not ended: what says an entity is still being read when nothing is
    /// playing it.
    pub fn readers(&self) -> usize {
        let entities: Vec<Arc<Entity<B>>> = self.entities.lock().values().cloned().collect();
        entities
            .iter()
            .map(|entity| entity.state.lock().observed_readers())
            .sum()
    }

    /// [`Self::readers`] for one key: how many open reads of that entity
    /// have promised or delivered and not ended. Zero for a key with no
    /// entity. L1 to look up, then L2.
    pub fn readers_of(&self, key: &B::Key) -> usize {
        self.lookup(key)
            .map(|entity| entity.state.lock().observed_readers())
            .unwrap_or(0)
    }

    /// How many streams have been opened on `key` ([`State::opens`]), for a
    /// driver about to decide a [`Mode`]. `0` for a key with no entity,
    /// which is also what its first open will have counted from.
    pub fn opens_of(&self, key: &B::Key) -> u64 {
        self.lookup(key)
            .map(|entity| entity.state.lock().opens)
            .unwrap_or(0)
    }

    /// [`Self::readers_of`] and [`Self::opens_of`] as one reading, for the
    /// driver deciding a [`Mode`]: how many open reads of `key` have
    /// promised or delivered and not ended, and how many streams have ever
    /// been opened on it. `(0, 0)` for a key with no entity.
    ///
    /// One L2 acquisition, and it has to be one. The count travels with
    /// [`Mode::Slack`] so the pass can tell whether a stream opened between
    /// the driver's reading and the pass's turn, and it can only tell that
    /// about the reading it was taken beside. Read as two acquisitions --
    /// "no reader" first, the count second -- an install that lands between
    /// them is inside the count: the pass compares, finds it equal, finds
    /// no reader open yet, because an aside's install completes before its
    /// reader opens and moves no liveness cell, and takes the bytes the
    /// stream is about to read. The gap the count exists to close, reopened
    /// one lock-release wide by the reading of it.
    ///
    /// **It closes one direction only.** An install that *preceded* this
    /// reading is already inside the count, so the comparison is equal and
    /// nothing here says the stream it belongs to is still on its way --
    /// exactly the state an opener is in between its install and its
    /// reader. Nothing in this reading can close that; what closes it is
    /// the opener taking its reader before it awaits anything, so the
    /// entity is held by something the pass cannot forget. See
    /// [`Self::reader_on`].
    pub fn readers_and_opens_of(&self, key: &B::Key) -> (usize, u64) {
        self.lookup(key)
            .map(|entity| {
                let state = entity.state.lock();
                (state.observed_readers(), state.opens)
            })
            .unwrap_or((0, 0))
    }
}

impl<B: Backing> State<B> {
    /// How many of this entity's open reads something has observed -- a
    /// promise or a delivered byte. **Not the size of the reader map**,
    /// which has held an entry per read since the open (for
    /// [`ReaderState::opened_at`]) and so counts a read that has done
    /// neither. Every "is anything reading this" in the owner is this
    /// question, and answering it with the map would have a slack pass
    /// refuse an entity a handed-out-and-abandoned stream still holds.
    fn observed_readers(&self) -> usize {
        self.readers
            .values()
            .filter(|reader| reader.observed())
            .count()
    }

    /// What the entity's open readers have already been promised: the
    /// largest stream lookahead any of them was granted.
    ///
    /// The window is sized never to be smaller than this
    /// ([`Buffering`]), and the max and not the newest because every open
    /// reader fetches its own lookahead and the pass has to hold all of
    /// them. A reader that has ended is out of the map and out of this: its
    /// stream is not fetching anything.
    fn buffering(&self) -> Buffering {
        let mut asked = Buffering {
            seed: self.seed,
            ..Buffering::default()
        };
        for reader in self.readers.values() {
            // The most generous profile in force wins; the `Maximum`
            // profile is a number. See [`Buffering::widest`].
            asked = asked.widest(reader.buffering);
        }
        // **Computed, never measured.** Size over duration is the film's
        // bitrate by arithmetic: exact at the first report, nothing to
        // converge, and nothing a player's read pattern can distort. It is
        // the entity's and not any one reader's, because no reader outlives
        // a reconnect. There is no fallback: a stream whose length nobody
        // has stated has no rate here, the two time caps do not apply, and
        // the byte arithmetic stands on its own -- which is better than the
        // measurement this replaced, whose three bytes a second and
        // seventeen bytes a second both collapsed a window onto its floor
        // and stopped playback.
        asked.bytes_per_second = self.rate_over(&self.domain);
        asked
    }

    /// `domain`'s bytes over the entity's stated duration: its own rate.
    fn rate_over(&self, domain: &B::Domain) -> Option<u64> {
        self.duration
            .filter(|duration| !duration.is_zero())
            .and_then(|duration| Some((B::bytes(domain)? as f64 / duration.as_secs_f64()) as u64))
    }

    /// The head a pass for `about` is about: that reader's own head while
    /// its body is open, and the entity's otherwise.
    ///
    /// **One function, because it is one question.** Where a pass measures
    /// from and whether a pass is still owed are the same question asked at
    /// two moments, and the second only terminates if it is a fixed point of
    /// the first: two spellings of it, one reading the reader and one the
    /// entity, differ by however far apart two players are, no pass moves
    /// either of them, and so every pass arms the next one forever.
    ///
    /// The entity's own head, which is what a tick pass and every stats
    /// reading ask for, is three questions in order and each one answers
    /// only what it knows:
    ///
    /// **The detector outranks all three, and this is the fallback under
    /// it.** Which of several readers of one file is the viewer is a
    /// question about behaviour, and `Consumers::at` is what answers it
    /// from the bytes each stream has eaten; [`Self::holding`] reports
    /// that first ([`Self::consumed_at`]) and only falls here when the
    /// detector is silent. The pass's own cadence -- where it measures
    /// from, whether it is owed another -- is this order throughout, since
    /// it has to be a fixed point of itself. What is below is the order
    /// for an entity the detector is silent about.
    ///
    /// 1. **A read that is playing it now**, at its head -- the last byte
    ///    it delivered, or where it was opened if it is still parked on its
    ///    first piece. The newest of them, because a seek is a second
    ///    response on the file still playing and the one just opened is the
    ///    one the player is using.
    /// 2. **Where playback last got to**, [`Self::last_position`]. A paused
    ///    film holds its body open or has closed it; either way nothing is
    ///    delivering, and a pass still has to measure from somewhere.
    /// 3. **Any live read at all**, playing or not. A file being
    ///    downloaded, or probed, and never played has no playback position
    ///    of either kind -- and an entity with no head at all is an entity
    ///    no pass measures at all.
    ///
    /// **What a player says about itself is not asked, and neither is what
    /// its range header looks like.** Converting where a player claims to
    /// be in the picture to a byte offset drifts on a variable-bitrate
    /// encode; reading a range's offset and length to tell mpv's crawl over
    /// the container index apart from a viewer's seek gets it wrong in both
    /// directions -- a viewer seeking into the last minutes of a film looks
    /// like an index read, and a crawler far enough out looks like a seek.
    /// Both are answered instead by what the reads *do*
    /// (`crate::retention::streams`).
    fn head(&self, about: Option<ReaderId>) -> Option<B::Position> {
        if let Some(head) = about
            .and_then(|id| self.readers.get(&id))
            .and_then(|reader| reader.head())
        {
            return Some(head);
        }
        self.live_head().or(self.last_position)
    }

    /// The newest live read's head. Newest by [`ReaderId`], which is
    /// handed out in open order.
    fn live_head(&self) -> Option<B::Position> {
        self.readers
            .iter()
            .filter_map(|(id, reader)| Some((*id, reader.head()?)))
            .max_by_key(|(id, _)| id.0)
            .map(|(_, head)| head)
    }

    /// What a pass snapshots at its start, or `None` when there is nothing
    /// to pass over: nothing installed, no head yet, or a head the domain
    /// has no index for ([`Backing::index_of`]).
    fn begin(&self, about: Option<ReaderId>) -> Option<Begin<B>> {
        let installed = self.installed.as_ref()?;
        let at = B::index_of(&self.domain, self.head(about)?)?;
        Some(Begin {
            domain: self.domain.clone(),
            budget: installed.budget,
            at,
        })
    }

    /// Whether the policy a pass began over is still the one installed: the
    /// same budget and the same domain. `false` is a decide that ran under
    /// the pass, and the pass's measurement is of a shape that no longer
    /// exists.
    fn still(&self, begin: &Begin<B>) -> bool {
        self.installed
            .as_ref()
            .is_some_and(|installed| installed.budget == begin.budget)
            && self.domain == begin.domain
    }

    /// Whether the head a pass was about owes another pass; asked under L2
    /// at the end of every pass, and the only way one pass starts the next.
    ///
    /// `measured` is the piece the pass measured from, and the head has to
    /// have moved a stride from it since: nothing a pass does moves a
    /// playhead, so the distance is zero unless a byte really went out
    /// while the pass held the turn, each arming is paid for by one such
    /// byte, and the chain terminates. `None` is a pass that measured
    /// nothing because the policy was replaced under it: the byte that
    /// replaced it was due (`install_now` makes every reader due), found the
    /// turn taken and started nothing, and if it was the last of its body
    /// nothing else remembers it -- so a pass is owed as long as something
    /// is installed and the head is in the domain. That terminates too: the
    /// successor measures under the new policy and is back on the stride
    /// rule. Only under [`Trigger::OnMove`]; the tick has its own clock.
    fn owes_a_pass(&self, about: Option<ReaderId>, measured: Option<u32>) -> bool {
        if !matches!(B::TRIGGER, Trigger::OnMove { .. }) || self.installed.is_none() {
            return false;
        }
        let Some(to) = self
            .head(about)
            .and_then(|head| B::index_of(&self.domain, head))
        else {
            return false;
        };
        measured.is_none_or(|at| to.abs_diff(at) >= self.stride)
    }

    /// Decide the entity under `budget`, under L2 alone: **the one write to
    /// the policy cell that does not require the turn** (rule 5). Sound only
    /// because [`Share::Nothing`] held nothing back for the policy this
    /// replaces, so there is no backend to tell; a pass in flight finds the
    /// budget changed at its re-read and concludes nothing.
    ///
    /// Every reader is due again: a budget is a different shape, so the
    /// stride the last one's passes measured against is not this one's, and
    /// `passed_at` left standing would keep a reader that has not travelled
    /// a whole *new* stride from ever being due. The windows are NOT
    /// cleared; see [`State::windows`]. The error is returned, not logged:
    /// this runs under L2.
    fn install_now(&mut self, budget: CacheBudget) -> Result<(), anyhow::Error> {
        self.decided = Some(budget);
        self.installed = None;
        self.stride = 1;
        for reader in self.readers.values_mut() {
            reader.passed_at = None;
        }
        let CacheBudget::Bytes(bytes) = budget else {
            return Ok(());
        };
        let policy = B::policy(&self.domain, bytes, self.buffering())?;
        let Shape::Split { unshared, .. } = policy.shape() else {
            // The budget covers it: nothing here will reclaim anything, and
            // a reader inside it is inside all of it.
            return Ok(());
        };
        self.stride = stride_for::<B>(unshared);
        self.installed = Some(Installed { budget, policy });
        Ok(())
    }

    /// Replace the standing policy with `policy`, the same domain under
    /// `budget`, which has already adopted the draw and the committed set
    /// of the one it replaces. Under the turn.
    fn resize_policy(&mut self, _turn: &mut Turn, budget: CacheBudget, policy: RetentionPolicy) {
        self.stride = match policy.shape() {
            Shape::Split { unshared, .. } => stride_for::<B>(unshared),
            Shape::Whole => 1,
        };
        for reader in self.readers.values_mut() {
            reader.passed_at = None;
        }
        self.installed = Some(Installed { budget, policy });
    }

    /// Put a resolved policy in its cell. Under the turn.
    fn install_policy(
        &mut self,
        _turn: &mut Turn,
        domain: B::Domain,
        budget: CacheBudget,
        policy: RetentionPolicy,
    ) {
        self.domain = domain;
        self.stride = match policy.shape() {
            Shape::Split { unshared, .. } => stride_for::<B>(unshared),
            Shape::Whole => 1,
        };
        for reader in self.readers.values_mut() {
            reader.passed_at = None;
        }
        self.installed = Some(Installed { budget, policy });
    }

    /// Forget the policy, and that the entity was decided at all. Under the
    /// turn: see [`Retention::clear_under`]. The draw stays -- it is the
    /// play session's, not the policy's.
    ///
    /// `decided` goes with the policy. Left standing, the next delivered
    /// byte finds it equal to the published budget, skips `install_now`,
    /// and the entity is unbounded until the budget's *value* changes --
    /// the hole this module exists to close, reopened by its own clear.
    /// Under a pin that makes each delivered byte a decide, a claim and a
    /// pass that clears again at its first step; cheap (no listing, and
    /// under [`Share::Nothing`] no backend call), and on a combination --
    /// pins and [`Install::OnDeliveredByte`] -- no backing has.
    fn forget_policy(&mut self, _turn: &mut Turn) {
        self.installed = None;
        self.decided = None;
        self.consumed_at = None;
    }

    /// Advance the resident policy in place, and hand back a copy of it as
    /// advanced, for the door. Under the turn. `None` is nothing installed,
    /// which the caller has already ruled out under the same lock.
    fn advance(
        &mut self,
        _turn: &mut Turn,
        giving_up: &[u32],
        held: &BTreeSet<u32>,
    ) -> Option<(Decision, RetentionPolicy)> {
        // Under what the readers are doing *now*, before the decision: the
        // bitrate the time caps are sized from needs a duration the player
        // states after the policy is built, and a reader that opened or
        // ended since changes what the window has to hold. The budget is not re-read -- a
        // budget that moves builds a new policy under the turn.
        let buffering = self.buffering();
        let installed = self.installed.as_mut()?;
        if installed.policy.observe(buffering)
            && let Shape::Split { unshared, .. } = installed.policy.shape()
        {
            self.stride = stride_for::<B>(unshared);
        }
        let installed = self.installed.as_mut()?;
        let decision = installed.policy.advance(giving_up, held);
        Some((decision, installed.policy.clone()))
    }

    /// Commit `piece` into the standing policy's committed set, now that the
    /// backing holds it, and say whether it joined the set. Under the turn;
    /// see [`Retention::commit_completed`].
    fn commit_drawn(&mut self, _turn: &mut Turn, piece: u32) -> bool {
        self.installed
            .as_mut()
            .is_some_and(|installed| installed.policy.commit_drawn(piece))
    }

    /// Write what a pass concluded. Under the turn.
    fn conclude(&mut self, _turn: &mut Turn, windows: Vec<Range<u32>>) {
        self.windows = windows;
    }

    /// A stream has been opened on this entity, by `opener`, asking
    /// `opening` of the cache. Under the turn, from
    /// [`Retention::install_opening`] alone; see [`Self::opens`],
    /// [`Self::player_opened`] and [`Self::asked`].
    fn opened(&mut self, _turn: &mut Turn, opening: Buffering, opener: Opener) {
        self.opens += 1;
        self.player_opened |= opener == Opener::Player;
        self.asked = self.asked.widest(opening);
    }

    /// What the play session's draw is sized beside: every read-ahead in
    /// force or stated by an open of the session, and -- once the film's
    /// rate is known -- the read-ahead a later open is granted, the rate
    /// times the widest seconds the viewer asked for, never past the whole
    /// cache (`Engine::try_get_file_with_intent` asks exactly that).
    ///
    /// The rate is `over`'s: the entity's own domain over its stated
    /// duration -- or, for a `member` (the member a play session plays
    /// inside one container, [`Backing::narrowed`], or its bytes in every
    /// volume of a set, [`Backing::joined`]), the member's length over the
    /// duration told for the draw alone ([`State::set_duration`]), which
    /// gives the entity's own stream no rate: a container's length over an
    /// episode's duration is the episode's rate times however many episodes
    /// the container holds.
    fn draw_buffering(&self, budget: CacheBudget, over: &B::Domain, member: bool) -> Buffering {
        let mut buffering = self.buffering().widest(self.asked);
        buffering.bytes_per_second = if member {
            self.set_duration
                .filter(|duration| !duration.is_zero())
                .and_then(|duration| Some((B::bytes(over)? as f64 / duration.as_secs_f64()) as u64))
        } else {
            self.rate_over(over)
        };
        if let (Some(rate), Some(seconds)) = (buffering.bytes_per_second, buffering.window_seconds)
        {
            let granted = match budget {
                CacheBudget::Bytes(cap) => rate.saturating_mul(seconds).min(cap),
                CacheBudget::Unknown | CacheBudget::Unbounded => rate.saturating_mul(seconds),
            };
            buffering.lookahead_bytes = buffering.lookahead_bytes.max(granted);
        }
        buffering
    }

    /// Whether the play session has waited [`DRAW_FALLBACK`] of playback
    /// for the film's rate, on the pass clock `now` (`None` for an open,
    /// which has no clock and never runs the fallback out). Playback is
    /// under way once a byte has been delivered; the first pass to see that
    /// starts the wait.
    fn fallback_ran_out(&mut self, now: Option<std::time::Instant>) -> bool {
        let Some(now) = now else {
            return false;
        };
        let delivered = self.last_position.is_some()
            || self
                .readers
                .values()
                .any(|reader| reader.playhead.is_some());
        if !delivered {
            return false;
        }
        let since = *self.draw_waiting_since.get_or_insert(now);
        now.saturating_duration_since(since) >= DRAW_FALLBACK
    }

    /// Record the play session's draw, and have the standing policy share
    /// it and nothing else. Under the turn.
    fn adopt_draw(&mut self, _turn: &mut Turn, draw: BTreeSet<u32>, made_for: DrawnFor<B::Key>) {
        if let Some(installed) = self.installed.as_mut() {
            installed.policy.adopt_draw(&draw);
        }
        self.draw = Some(draw);
        self.drawn_for = Some(made_for);
    }

    /// Whether the draw is decided for `made_for`, so nothing is drawn
    /// again: a draw is recorded, and it is not an empty one made for
    /// something else. An empty draw advertised nothing, so one made for
    /// another member, another set or the file as itself -- a volume first
    /// played by its URL, whose sniff found an archive -- is replaced by
    /// the draw of what is played now, which withdraws nothing.
    fn draw_settled(&self, made_for: &DrawnFor<B::Key>) -> bool {
        match &self.draw {
            None => false,
            Some(draw) => !draw.is_empty() || self.drawn_for.as_ref() == Some(made_for),
        }
    }

    /// The draw is shared no more and its announcement has been ended:
    /// forget it, so the session's next open or pass draws what is played
    /// now. The standing policy keeps nothing of it any more. Under the
    /// turn. The open, the read-ahead asked and what the content is stay:
    /// the session is still on the entity.
    fn forget_draw(&mut self, _turn: &mut Turn) {
        if let Some(installed) = self.installed.as_mut() {
            installed.policy.adopt_draw(&BTreeSet::new());
        }
        self.draw = None;
        self.drawn_for = None;
        self.draw_waiting_since = None;
    }

    /// Nothing bounds this entity any more, its play session is over, and
    /// everything it holds is on its way off the disk. Under the turn, once
    /// nothing it holds is announced in the swarm.
    ///
    /// The windows go with the policy, deliberately: a window is what a
    /// pass measured round a head somebody was at, and an entity nobody is
    /// playing has none. Left standing they would tell a protection reading
    /// that a left file's pieces are protected, which is how a switch used
    /// to leave the previous film on the disk under two owners' protection
    /// and neither one's deleter. The draw goes too: the next open of this
    /// file is a new play session, and draws afresh.
    fn go_slack(&mut self, _turn: &mut Turn) {
        self.installed = None;
        self.decided = None;
        self.windows = Vec::new();
        self.consumed_at = None;
        self.draw = None;
        self.drawn_for = None;
        self.player_opened = false;
        self.asked = Buffering::default();
        self.draw_waiting_since = None;
        self.content = None;
        self.slacked = true;
    }

    /// [`Self::consumed_at`] as a position, or `None` when the detector
    /// was silent or the backing cannot make one. To the piece: a file
    /// that starts inside a piece is placed at that piece's start, which
    /// is the granularity every reading of this has anyway.
    fn consumed_head(&self) -> Option<B::Position> {
        let piece = self.consumed_at?;
        let extent = B::extent(&self.domain);
        let offset = u64::from(piece.saturating_sub(extent.start)) * B::piece_length(&self.domain)?;
        B::position_at(&self.domain, offset)
    }

    fn holding(&self) -> Holding<B> {
        Holding {
            domain: self.domain.clone(),
            extent: B::extent(&self.domain),
            installed: self.installed.as_ref().map(|installed| InstalledView {
                budget: installed.budget,
                pieces: installed.policy.pieces(),
                committed: installed.policy.advertised().clone(),
                shape: installed.policy.shape(),
            }),
            windows: self.windows.clone(),
            promised: self
                .readers
                .values()
                .map(|reader| reader.promised.clone())
                .filter(|range| !range.is_empty())
                .collect(),
            asking: self
                .readers
                .values()
                .map(|reader| reader.asking.clone())
                .filter(|range| !range.is_empty())
                .collect(),
            live_playhead: self
                .readers
                .values()
                .any(|reader| reader.playhead.is_some()),
            head: self.consumed_head().or_else(|| self.head(None)),
            last_position: self.last_position,
            decided: self.decided,
        }
    }
}

/// A fresh draw for one entity's shared set ([`State::seed`]).
///
/// `RandomState` is seeded from the operating system once per process and
/// is keyed per instance, so two `RandomState`s in one process disagree and
/// two processes disagree -- which is exactly what is wanted here, and it
/// costs no dependency. What is hashed is a counter, so two entities of one
/// process never draw the same set either.
#[cfg(not(test))]
fn share_seed() -> u64 {
    use std::hash::BuildHasher;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::collections::hash_map::RandomState::new().hash_one(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Fixed under test: what the tests are about is which pieces a given draw
/// keeps and that nothing ever takes one back, not that the draw is random
/// -- that it *is* is one call to `RandomState`, above. The value is
/// chosen so that the eight-piece fake files draw their first two pieces,
/// which is where the tests that predate the draw had their windows.
#[cfg(test)]
fn share_seed() -> u64 {
    692
}

/// How far a playhead must move before another pass is worth its listing:
/// the window over the trigger's passes per window, at least one piece.
/// Under [`Trigger::External`] nothing reads it.
fn stride_for<B: Backing>(window: u32) -> u32 {
    match B::TRIGGER {
        Trigger::OnMove { passes_per_window } => {
            u32::try_from(u64::from(window) / passes_per_window.max(1))
                .unwrap_or(u32::MAX)
                .max(1)
        }
        Trigger::External => 1,
    }
}

/// **How many pieces of an entity an asker may have asked for in all**
/// ([`Reader::room`]): the rule that a piece which could not be kept is not
/// asked for.
///
/// What the entity may hold, less everything its readers hold that no
/// reclaim can give back -- their windows, their promises, their streams'
/// lookaheads, the committed set. An asker that stays inside it can always
/// be kept beside them, whatever else has to go to make the room; one that
/// went past it would be fetching pieces for the next pass to take, or
/// pushing out the pieces it fetched first. Measured by each pass, from the
/// budget and what is held then; no fixed share of anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Room {
    /// Nothing bounds the entity: a budget that covers it, or a pin.
    Unbounded,
    /// This many pieces, counting every one the asker has asked for so far.
    Pieces(u64),
    /// A policy bounds the entity and no pass has measured it under that
    /// policy yet: the next one will, and until then nothing is asked for.
    Unmeasured,
}

/// One open read: a handle that holds its promise and carries its playhead.
/// Dropping it is what says the read is over, and it is the only thing that
/// releases a promise.
pub struct Reader<B: Backing> {
    owner: Arc<Retention<B>>,
    entity: Arc<Entity<B>>,
    id: ReaderId,
    /// Where this read was opened; see [`ReaderState::opened_at`].
    opened_at: Option<B::Position>,
    /// What this read asks of the cache; see [`ReaderState::buffering`].
    buffering: Buffering,
}

impl<B: Backing> Reader<B> {
    /// What the entity this read is on covers: for a torrent's file, where
    /// the file lies in the torrent. A copy, taken under L2.
    pub fn domain(&self) -> B::Domain {
        self.entity.state.lock().domain.clone()
    }

    /// The piece at `at` is what this read is waiting for.
    ///
    /// [`Self::promises`] in the entity's own coordinates, so a caller that
    /// has a position and not a piece index -- every reader of a torrent
    /// file, whose cursor counts from the file and not from the torrent --
    /// does not have to do the conversion itself and get it wrong on a file
    /// that does not start at piece zero.
    pub fn promises_at(&self, at: B::Position) {
        let index = {
            let state = self.entity.state.lock();
            B::index_of(&state.domain, at)
        };
        if let Some(index) = index {
            self.promises(index..index.saturating_add(1));
        }
    }

    /// This read will deliver `pieces` off the disk, and until it has,
    /// nothing may unlink them. The range shrinks from the front as
    /// [`Self::note`] reports bytes going out, and is released whole when
    /// this handle is dropped. An empty promise records nothing.
    pub fn promises(&self, pieces: Range<u32>) {
        if pieces.is_empty() {
            return;
        }
        let mut state = self.entity.state.lock();
        // **And held against every unlink, at once.** A promise is made
        // between passes -- a read parks on a piece the last pass knew
        // nothing about -- and the door reads the published set and nothing
        // else. Written here, under the entity's lock, which is the lock
        // the pass publishes under: the two writers are the owner's and
        // they cannot interleave. Nothing clears it; the next pass
        // republishes what is asked for and what is promised then.
        if let Some(exempt) = state.exempt.as_ref() {
            exempt.hold(pieces.clone());
        }
        state
            .readers
            .entry(self.id)
            .or_insert_with(|| ReaderState::opened(self.opened_at, self.buffering))
            .promised = pieces;
    }

    /// **This asker's stream is asking the backend for `pieces` now**, and
    /// for nothing else: what it asked for before and has left is no
    /// longer its own ([`Retention::ask_ahead`]). An empty range is a
    /// stream that asks for nothing.
    pub fn asks(&self, pieces: Range<u32>) {
        self.entity
            .state
            .lock()
            .readers
            .entry(self.id)
            .or_insert_with(|| ReaderState::opened(self.opened_at, self.buffering))
            .asking = pieces;
    }

    /// **How much an asker may ask for**, as the entity stands; see
    /// [`Room`].
    pub fn room(&self) -> Room {
        let state = self.entity.state.lock();
        match (&state.installed, state.room) {
            (None, _) => Room::Unbounded,
            (Some(installed), Some((budget, pieces))) if budget == installed.budget => {
                Room::Pieces(pieces)
            }
            _ => Room::Unmeasured,
        }
    }

    /// A byte at `at` of this entity has reached a player.
    ///
    /// **Every delivered byte moves the entity's `last_position`, and it is
    /// the last resort under the detector.** Where the entity is being
    /// consumed is [`Consumers::at`], from the bytes each detected stream
    /// has eaten; this value is only what a pass falls back to when no
    /// stream has read the entity lately -- a film nobody is watching,
    /// whose crawler has stopped too. Moving it on every delivered byte,
    /// not only a byte some caller declares to be playback, keeps mpv's
    /// read of the container index at the tail from stranding the head
    /// there once that read has closed.
    ///
    /// The budget is read before L2 (a copy-out of a foreign lock, rule 2);
    /// under L2 the entity's `last_position`, an
    /// [`Install::OnDeliveredByte`] decide when the budget moved, this
    /// reader's playhead and the shrink of its promise; and, for
    /// [`Trigger::OnMove`], whether the byte moved a stride since this
    /// reader's last pass -- `abs_diff`, so a seek back is as much a reason
    /// to look as playing on. A due byte tries the turn **under L2** (rule
    /// 3): `Some` is a claim the caller must hand to [`Retention::pass`];
    /// `None` is a pass in flight, and the pass's conclusion asks the same
    /// head again.
    ///
    /// No clock: nothing here is timed. The reads a detector measures are
    /// stamped where they are served ([`crate::retention::streams::Read`]),
    /// and the pass measures against the clock it is handed
    /// ([`Retention::pass_at`]).
    pub fn note(&self, at: B::Position) -> Option<Claim> {
        let budget = self.owner.budget.get();
        let (claim, refused) = {
            let mut state = self.entity.state.lock();
            state.last_position = Some(at);
            let refused = if B::INSTALL == Install::OnDeliveredByte && state.decided != Some(budget)
            {
                state.install_now(budget).err()
            } else {
                None
            };
            let index = B::index_of(&state.domain, at);
            let (bounded, stride) = (state.installed.is_some(), state.stride);
            let reader = state
                .readers
                .entry(self.id)
                .or_insert_with(|| ReaderState::opened(self.opened_at, self.buffering));
            reader.playhead = Some(at);
            let due = match index {
                Some(index) => {
                    // Delivered is no longer promised: the piece went out
                    // whole before this was called, so the promise starts
                    // after it.
                    reader.promised.start = reader
                        .promised
                        .start
                        .max(index.saturating_add(1))
                        .min(reader.promised.end);
                    let moved = !reader
                        .passed_at
                        .is_some_and(|was| index.abs_diff(was) < stride);
                    matches!(B::TRIGGER, Trigger::OnMove { .. }) && bounded && moved
                }
                None => false,
            };
            // A due byte tries the turn, under L2 (rule 3): `None` is a
            // pass in flight, and its conclusion asks the same head again.
            let claim = due
                .then(|| self.entity.turn.clone().try_lock_owned().ok())
                .flatten()
                .map(|guard| Claim {
                    guard,
                    about: Some(self.id),
                });
            (claim, refused)
        };
        if let Some(error) = refused {
            tracing::debug!(
                key = ?self.entity.key,
                error = %format!("{error:#}"),
                "could not size a retention policy for the entity; it is not bounded"
            );
        }
        claim
    }
}

impl<B: Backing> Drop for Reader<B> {
    /// The read is over: its promise is released and its playhead is not a
    /// live reader's any more. The entity and the windows its last pass
    /// chose stay: a read that ends is not a stream that has been replaced,
    /// and what ends one is [`Mode::Slack`].
    fn drop(&mut self) {
        self.entity.state.lock().readers.remove(&self.id);
    }
}

/// The pass's last asking, built by the owner and handed to
/// [`Backing::reclaim`]. Sync and cheap, callable from a blocking thread:
/// [`Self::refuses`] takes L2 briefly and no other lock, and asks
/// [`Backing::keeps_everything`] before L2, never under it;
/// [`Self::drawn`], asked once per run, takes L1 → L2 to copy out.
pub struct Door<B: Backing> {
    state: Arc<parking_lot::Mutex<State<B>>>,
    backing: Arc<B>,
    key: B::Key,
    /// L1, for [`Self::drawn`]: every entity's recorded draw, read at the
    /// unlink.
    entities: Arc<Entities<B>>,
    /// Which pass this door is for. A [`Mode::Slack`] door keeps nothing
    /// but what a live read was promised, and closes the moment the entity
    /// is played again.
    mode: Mode,
    /// **What may not be unlinked, as one bit per piece.** Everything this
    /// entity's consumers are asking for and every promise live when the
    /// pass published it, so the answer at the door is a load and a mask
    /// rather than a lock taken once per candidate piece from a blocking
    /// thread. Empty under [`Mode::Slack`], which keeps nothing but a live
    /// promise.
    exempt: std::sync::Arc<crate::retention::exempt::Exempt>,
}

impl<B: Backing> Door<B> {
    /// **Take nothing more at all**, whatever the run says: the entity is
    /// pinned now, or it is slack and a player has opened it again while
    /// its bytes were going. Neither un-happens mid-loop, so a reclaim that
    /// sees this stops rather than skipping a run.
    ///
    /// Asked here, at the unlink, and not carried in from the mode the
    /// driver decided.
    pub fn shut(&self) -> bool {
        self.backing.keeps_everything(&self.key)
            || (matches!(self.mode, Mode::Slack { .. }) && self.backing.is_live(&self.key))
    }

    /// **Every piece a play session's draw has recorded, of every entity
    /// a session plays**: what the backing refuses to unlink beside what
    /// the backend announces, read once per run at the act
    /// ([`Backing::reclaim`], [`Backing::want`]).
    ///
    /// Because a draw is recorded before it is advertised, and -- under a
    /// set -- it is advertised by whichever volume drew it, under that
    /// volume's turn and not this one's: a reading of what the backend
    /// announces, taken by this pass a moment before a sibling's advertise
    /// of the union lands, says nothing of pieces of this entity that are
    /// about to be announced. The record is there before the advertise, so
    /// this reading, taken after the backend's, sees what that one could
    /// not. A recorded draw of an entity no session plays any more is left
    /// out: its file was left, and what it announced is the backend's
    /// reading to answer for.
    ///
    /// L1 → L2 per entity to copy the draws out, as
    /// [`Retention::draws`] does; the backing's `plays` asked after both
    /// are released (rule 2). Never asked under an owner lock.
    pub fn drawn(&self) -> BTreeSet<u32> {
        let draws: Vec<(B::Key, BTreeSet<u32>)> = {
            let entities = self.entities.lock();
            entities
                .iter()
                .filter_map(|(key, entity)| {
                    let draw = entity.state.lock().draw.clone()?;
                    (!draw.is_empty()).then(|| (key.clone(), draw))
                })
                .collect()
        };
        draws
            .into_iter()
            .filter(|(key, _)| self.backing.plays(key))
            .flat_map(|(_, draw)| draw)
            .collect()
    }

    /// **Whether `index` may not be taken at this instant.**
    ///
    /// [`Self::shut`] first -- the backing's pin read and, on a slack pass,
    /// its liveness read, each a lock outside the owner -- and then, on the
    /// live path, a load and a bit test: the set was published by the pass
    /// under the entity's lock, and every promise made since was written
    /// into it under that same lock. The door is never a writer -- two
    /// unordered writers to one piece of state is the defect this owner
    /// keeps finding -- and it takes no owner lock on the live path, because
    /// it is asked once per candidate piece, per unlink, from a blocking
    /// thread.
    ///
    /// The race it tolerates is a region that grew onto a piece just after
    /// the door was asked, which costs a re-fetch. The race that cannot
    /// happen is a set bit being unlinked: bits are only cleared by the
    /// pass, for a region it has just measured as gone.
    pub fn refuses(&self, index: u32) -> bool {
        if self.shut() {
            return true;
        }
        if self.exempt.holds(index) {
            return true;
        }
        if matches!(self.mode, Mode::Slack { .. }) {
            // Nothing of a slack entity is kept for what somebody might
            // read; what is kept is what an open read was already promised,
            // and it is served every byte of it. A slack pass publishes no
            // set of its own, so this is asked of the readers.
            return self
                .state
                .lock()
                .readers
                .values()
                .any(|reader| reader.promised.contains(&index));
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The fixture every test here is built over lives in
    // `crate::retention::scenario`, one module up: the policy that replaces
    // this one has to be drivable through the same fake, and a fixture
    // buried in this module's own `mod tests` cannot be. See that module
    // for why it is a sibling rather than a child.
    use crate::retention::scenario::*;
    use std::time::{Duration, Instant};

    /// Which of `pieces` a door refuses, for a test that wants the shape of
    /// its answer rather than one piece of it.
    fn refused<B: Backing>(door: &Door<B>, pieces: Range<u32>) -> Vec<u32> {
        pieces.filter(|piece| door.refuses(*piece)).collect()
    }

    use std::sync::atomic::AtomicBool;

    /// **A pass that dies leaves the policy where it was, and the next one
    /// runs.**
    ///
    /// The abandoned-pass hole, closed by construction: a driver that took
    /// the policy for the length of a pass would lose it to a pass future
    /// dropped at its listing -- the runtime shutting down -- and a
    /// `running` flag it set would never be cleared, so no later pass would
    /// run either. Here the policy never moves and the claim is a guard.
    #[tokio::test]
    async fn a_pass_dropped_at_its_listing_leaves_the_policy_installed_and_the_next_pass_runs() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("the first byte is due");
        backing.read_from(0, PIECE);
        let (entered, _release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("the pass to reach its listing");
        pass.abort();
        assert!(pass.await.expect_err("aborted").is_cancelled());
        let holding = owner.holding(&0).expect("the entity");
        assert!(
            holding.installed.is_some(),
            "the policy went down with the pass"
        );
        assert!(
            holding.windows.is_empty(),
            "the dead pass concluded something"
        );
        // Turn free, policy in its cell: the next pass keeps what the one
        // consumer is reading ahead over -- pieces 1 and 2, since its read
        // ended at the start of piece 1 -- and gives back four pieces of
        // the rest, oldest first. Piece 0 is behind the consumer and is
        // scrub-back, which this budget cannot afford.
        backing.read_from(0, PIECE);
        let claim = owner.try_turn(&0).expect("the dead pass released the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(outcome.concluded.expect("a pass that ran").reclaimed, 4);
        assert_eq!(backing.on_disk(), vec![0, 1, 2, 3]);
    }

    /// **A reclaim whose closure dies reports nothing freed and touches
    /// nothing.**
    ///
    /// A pass that took the policy out for its length would lose it with
    /// the task, and the entity would be unbounded until the budget's value
    /// changed. Here there is nothing to lose.
    #[tokio::test]
    async fn a_reclaim_that_panics_leaves_the_policy_the_decision_and_the_windows_untouched() {
        let (backing, owner, _budget) = proxy();
        let second = owner.reader(0, domain(0, 0..8));
        // The second player's first byte is due too; its claim is dropped
        // unused, which is a pass that never ran.
        drop(second.note((0, 6 * PIECE)));
        backing.seek_to(6 * PIECE);
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 3 * PIECE)).expect("due");
        backing.seek_to(3 * PIECE);
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        // One window per live playhead: 7 is inside the second player's.
        assert_eq!(first.windows, vec![3..6]);
        assert_eq!(first.reclaimed, 4);
        let before = owner.holding(&0).expect("the entity");
        assert_eq!(before.windows, vec![3..6]);

        backing.reclaim_panics.store(true, Ordering::SeqCst);
        let claim = owner.try_turn(&0).expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            outcome.concluded.expect("a pass that ran").reclaimed,
            0,
            "a dead closure vouched for a number"
        );
        assert!(outcome.again.is_none());
        let after = owner.holding(&0).expect("the entity");
        assert_eq!(after.installed, before.installed);
        assert_eq!(after.decided, before.decided);
        assert_eq!(after.windows, before.windows);
        assert!(
            owner.try_turn(&0).is_some(),
            "the dead reclaim left the turn taken"
        );
    }

    /// **A byte delivered while a pass runs starts nothing, and the pass
    /// arms exactly one successor if that byte moved the head a stride --
    /// and none if it did not.**
    ///
    /// The arming fixed point: the pass re-asks the same `head` it measured
    /// from, so each arming is paid for by one delivered byte and the chain
    /// terminates. The claim it hands on is the same one, so nothing can
    /// slip a pass in between.
    #[tokio::test]
    async fn a_note_during_a_pass_spawns_nothing_and_the_pass_rearms_once_per_moved_stride() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("the pass to reach its reclaim");
        // A stride is one piece here; the head moves two.
        assert!(
            reader.note((0, 2 * PIECE)).is_none(),
            "a note during a pass took the turn"
        );
        backing.read_from(2 * PIECE, 1);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        assert!(outcome.concluded.is_some(), "a pass that ran");
        let again = outcome.again.expect("the swallowed byte is owed a pass");
        assert!(
            owner.try_turn(&0).is_none(),
            "the claim was handed on and yet the turn is free"
        );
        // The successor measures from where the head is now, and the head
        // has not moved since: nothing further is owed.
        let outcome = owner.pass(&0, &(), again, Mode::Live).await;
        assert!(outcome.concluded.is_some(), "the successor");
        assert!(
            outcome.again.is_none(),
            "a pass armed itself off a head that did not move"
        );
        assert_eq!(owner.holding(&0).unwrap().windows, vec![2..6]);
        assert!(owner.try_turn(&0).is_some());
        // With the turn free, a byte inside the stride of the pass that
        // measured this reader is not due: one reader playing on does not
        // spend a listing per byte.
        assert!(
            reader.note((0, 2 * PIECE + 1)).is_none(),
            "a byte inside the stride was due"
        );
        backing.read_from(2 * PIECE + 1, 1);

        // And a byte that did not move a stride arms nothing.
        let (entered, release) = backing.park_reclaim();
        let claim = owner.try_turn(&0).expect("the turn");
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked");
        assert!(reader.note((0, 2 * PIECE + 1)).is_none());
        backing.read_from(2 * PIECE + 1, 1);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        assert!(outcome.concluded.is_some(), "a pass that ran");
        assert!(
            outcome.again.is_none(),
            "a byte inside the stride armed a pass"
        );
    }

    /// A waker that records whether the entity's state lock was held at
    /// the instant it was woken -- which, for a waiter on the turn, is the
    /// instant the claim was dropped.
    struct WokenUnder<B: Backing> {
        state: Arc<parking_lot::Mutex<State<B>>>,
        woken: AtomicBool,
        under_state: AtomicBool,
    }

    impl<B: Backing> std::task::Wake for WokenUnder<B> {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.woken.store(true, Ordering::SeqCst);
            self.under_state
                .store(self.state.is_locked(), Ordering::SeqCst);
        }
    }

    /// A queued `lock_owned` on an entity's turn.
    type Waiter =
        std::pin::Pin<Box<dyn Future<Output = tokio::sync::OwnedMutexGuard<Turn>> + Send>>;

    /// A waiter queued on the entity's turn, polled once so it is in the
    /// queue, whose waker records whether L2 was held at the instant the
    /// claim dropped. Every exit of the pass is held to that.
    fn watch_release<B: Backing>(entity: &Entity<B>) -> (Arc<WokenUnder<B>>, Waiter) {
        let probe = Arc::new(WokenUnder {
            state: entity.state.clone(),
            woken: AtomicBool::new(false),
            under_state: AtomicBool::new(false),
        });
        let waker = std::task::Waker::from(probe.clone());
        let mut context = std::task::Context::from_waker(&waker);
        let mut waiter: Waiter = Box::pin(entity.turn.clone().lock_owned());
        assert!(
            waiter.as_mut().poll(&mut context).is_pending(),
            "the turn was free while a claim stood"
        );
        (probe, waiter)
    }

    /// **THE RACE: the conclusion and the release of the turn are one step
    /// under the state lock, so a byte that lands between them cannot find
    /// a pass running and start none.**
    ///
    /// Rule 3. A note reads "is a pass running" (`try_lock` on the turn)
    /// and "is this byte due" under one L2 acquisition; the pass's
    /// conclusion decides `again` from the head and drops or hands on the
    /// claim under the same L2 block. Released after the block instead,
    /// there is a gap in which a delivered byte sees the turn taken, starts
    /// nothing, and the conclusion has already decided nothing is owed --
    /// the swallowed last byte of a body, which the proxy test at L2250
    /// pins from outside.
    ///
    /// No test can put a thread into that gap on purpose, so the instant
    /// of the release is observed instead: a waiter queued on the turn is
    /// woken synchronously when the claim drops (tokio's semaphore hands
    /// the permit over inside the drop), and its waker records whether L2
    /// was held at that instant. Then the behaviour: a byte that moves a
    /// stride after the conclusion gets its pass.
    #[tokio::test]
    async fn a_byte_landing_between_the_conclusion_and_the_release_still_gets_a_pass() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        let (probe, waiter) = watch_release(&owner.lookup(&0).expect("the entity"));
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_some(), "a pass");
        assert!(outcome.again.is_none(), "the head did not move");
        assert!(
            probe.woken.load(Ordering::SeqCst),
            "the claim was never released"
        );
        assert!(
            probe.under_state.load(Ordering::SeqCst),
            "the claim was released outside the conclusion's state lock"
        );
        // The waiter now holds the permit; let it go so the note below can
        // claim.
        drop(waiter);
        let claim = reader
            .note((0, 3 * PIECE))
            .expect("a byte that moved a stride after the conclusion is owed a pass");
        backing.read_from(3 * PIECE, 1);
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            outcome.concluded.expect("a pass that ran").windows,
            // Two consumers: the read at the head, and the one three
            // pieces on with nothing held between them.
            vec![0..3, 3..6]
        );
    }

    /// **A pass that hands its claim on does not release the turn in
    /// between.** The successor is the same claim, so a waiter on the turn
    /// is not woken and a note cannot slip in.
    #[tokio::test]
    async fn a_handed_on_claim_never_releases_the_turn() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        let (probe, _waiter) = watch_release(&owner.lookup(&0).expect("the entity"));
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked");
        assert!(reader.note((0, 2 * PIECE)).is_none());
        backing.read_from(2 * PIECE, 1);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        let again = outcome.again.expect("owed");
        assert!(
            !probe.woken.load(Ordering::SeqCst),
            "the turn was released and re-taken"
        );
        drop(again);
        assert!(probe.woken.load(Ordering::SeqCst));
    }

    /// **The draw is advertised once, before a piece of it exists, and a
    /// drawn piece is committed when it completes, with no pass having to
    /// have seen it.**
    ///
    /// The committed set is the draw's pieces we hold. While "found to hold
    /// it" was a pass reading a listing every couple of seconds, a drawn
    /// piece fetched and given back between two passes was never committed
    /// at all. Here every piece of the file arrives and is gone again before
    /// any pass runs, so a pass would have found nothing whatever, and the
    /// draw's two pieces are committed -- and the backend was told about
    /// them exactly once, at the install, so it could announce each as it
    /// completed: a completion tells the backend nothing new.
    #[tokio::test]
    async fn a_drawn_piece_completing_between_two_passes_is_committed_without_one() {
        let (backing, owner, _budget) = torrent();
        // An empty disk, so nothing here can be a listing's answer.
        backing.held.lock().clear();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        let drawn: Vec<u32> = owner
            .draws()
            .into_iter()
            .find(|(key, _)| *key == 0)
            .map(|(_, draw)| draw.into_iter().collect())
            .expect("the install drew this file's shared set");
        assert_eq!(
            drawn.len(),
            2,
            "this budget's committed half is two pieces: {drawn:?}"
        );
        assert_eq!(
            backing.advertised_pieces(),
            drawn,
            "the draw is advertised at the install, before a piece of it exists"
        );
        let advertised = backing.advertised.lock().clone();

        let mut committed = Vec::new();
        for piece in 0..8u32 {
            backing.holds([piece]);
            if owner.commit_completed(piece).await {
                committed.push(piece);
            }
            // And the window moves on: nothing is left for a pass to find.
            backing.held.lock().remove(&piece);
        }
        assert_eq!(committed, drawn, "what was committed is the draw");
        assert_eq!(
            owner
                .holding(&0)
                .and_then(|holding| holding.installed)
                .expect("the policy the install put in")
                .committed
                .into_iter()
                .collect::<Vec<_>>(),
            drawn
        );
        assert_eq!(
            *backing.advertised.lock(),
            advertised,
            "a completion told the backend nothing: the draw was advertised already"
        );
        // And a piece that completes again -- the same bytes downloaded a
        // second time -- is committed once: a committed piece is committed.
        backing.holds(drawn.iter().copied());
        for piece in &drawn {
            assert!(
                !owner.commit_completed(*piece).await,
                "piece {piece} was committed twice"
            );
        }
    }

    /// **Only the viewer's playback stream draws.** An open the liveness
    /// cell does not name -- a subtitle beside the film, which the aside
    /// rule keeps off the cell -- draws nothing, and neither does an
    /// unshared open of the file being played (a translated source's). The
    /// player's own open of the film draws; and a side file the cell is
    /// later handed to (the next episode, opened before the last one's read
    /// had closed) draws at its next pass.
    #[tokio::test]
    async fn only_the_players_stream_of_the_file_being_played_draws() {
        let (backing, owner, _budget) = torrent();
        *backing.playing.lock() = Some(0);
        for key in [0, 1] {
            owner.entity(key, domain(key, key as u32 * 8..key as u32 * 8 + 8));
            owner.note_duration(&key, FILM_LENGTH);
        }
        assert_eq!(
            owner
                .install_opening(1, 1, Buffering::default(), Opener::Player)
                .await,
            InstallOutcome::Installed
        );
        assert_eq!(
            owner
                .install_opening(0, 0, Buffering::default(), Opener::Unshared)
                .await,
            InstallOutcome::Installed
        );
        assert!(owner.draws().is_empty(), "drawn: {:?}", owner.draws());
        assert!(backing.advertised.lock().is_empty());

        assert_eq!(
            owner
                .install_opening(0, 0, Buffering::default(), Opener::Player)
                .await,
            InstallOutcome::Kept
        );
        assert_eq!(owner.draws().len(), 1);
        assert_eq!(*backing.advertised.lock(), vec![0..2], "the film's draw");

        *backing.playing.lock() = Some(1);
        owner.note_position(&1, (1, 0));
        let claim = owner.turn(&1).await.expect("the turn");
        owner.pass(&1, &(), claim, Mode::Live).await;
        assert!(
            owner.draw_of(&1).is_some_and(|draw| !draw.is_empty()),
            "the file the cell was handed to drew nothing"
        );
    }

    /// **The draw waits for the film's rate, and is sized with the
    /// read-ahead a reader is really granted once it comes.**
    ///
    /// Six pieces of budget, and a first open that states one piece of
    /// read-ahead -- what a stream is granted before the player has said how
    /// long the film is. Drawn then, the set would be three pieces. The
    /// length arrives: 8000 bytes over 32 s is 250 B/s, and the ten seconds
    /// the open asked for come to 2500 bytes, four pieces touched -- beside
    /// which six pieces hold two. The pass that finds the rate draws those
    /// two, and nothing after it changes them.
    #[tokio::test]
    async fn the_draw_waits_for_the_films_rate_and_is_sized_with_the_real_read_ahead() {
        let (backing, owner, budget) = torrent();
        budget.set(Some(6 * PIECE), None);
        *backing.playing.lock() = Some(0);
        let opening = Buffering {
            lookahead_bytes: PIECE,
            window_seconds: Some(10),
            ..Buffering::default()
        };
        assert_eq!(
            owner.install_opening(0, 0, opening, Opener::Player).await,
            InstallOutcome::Installed
        );
        assert!(owner.draws().is_empty(), "drawn before the rate was known");
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(owner.draws().is_empty(), "a pass drew with no rate yet");
        assert!(backing.advertised.lock().is_empty());
        assert!(
            owner
                .holding(&0)
                .and_then(|holding| holding.installed)
                .expect("the policy")
                .committed
                .is_empty(),
            "the pass committed pieces nothing advertised: the whole file is on the disk"
        );

        owner.note_duration(&0, Duration::from_secs(32));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        let draw = owner.draw_of(&0).expect("drawn once the rate was known");
        assert_eq!(draw.len(), 2, "{draw:?}");
        let advertised = backing.advertised_pieces();
        assert_eq!(advertised, draw.iter().copied().collect::<Vec<_>>());
        // The swarm brings the file back -- the pass before the rate had
        // taken what nobody was reading -- and the standing policy commits
        // exactly the draw.
        backing.holds(0..8);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            owner
                .holding(&0)
                .and_then(|holding| holding.installed)
                .expect("the policy")
                .committed,
            draw,
            "the standing policy does not share the draw it was handed"
        );

        // Fixed: another open, a budget that grows, a length stated again.
        budget.set(Some(8 * PIECE - 1), None);
        owner
            .install_opening(0, 0, Buffering::default(), Opener::Player)
            .await;
        owner.note_duration(&0, Duration::from_secs(1000));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(owner.draw_of(&0), Some(draw));
        assert_eq!(backing.advertised_pieces(), advertised);
    }

    /// **A film whose length never comes draws after [`DRAW_FALLBACK`] of
    /// playback**, sized beside what its opens asked for. The wait starts at
    /// the first pass that finds a byte delivered -- not at the open, which
    /// on a cold swarm can be a minute before the first frame -- and is
    /// measured on the passes' own clock.
    #[tokio::test]
    async fn a_film_whose_length_never_comes_draws_after_the_fallback() {
        let (backing, owner, budget) = torrent();
        budget.set(Some(6 * PIECE), None);
        *backing.playing.lock() = Some(0);
        let opening = Buffering {
            lookahead_bytes: PIECE,
            window_seconds: Some(10),
            ..Buffering::default()
        };
        owner.install_opening(0, 0, opening, Opener::Player).await;
        let t0 = Instant::now();
        let pass_at = |at: Duration| {
            let owner = owner.clone();
            async move {
                let claim = owner.turn(&0).await.expect("the turn");
                owner.pass_at(&0, &(), claim, Mode::Live, t0 + at).await;
            }
        };
        // Nothing delivered yet: the wait has not begun, however late.
        pass_at(Duration::from_secs(60)).await;
        assert!(owner.draws().is_empty());
        owner.note_position(&0, (0, 0));
        pass_at(Duration::from_secs(61)).await;
        pass_at(Duration::from_secs(61) + DRAW_FALLBACK - Duration::from_millis(1)).await;
        assert!(
            owner.draws().is_empty(),
            "drawn before the fallback ran out"
        );
        pass_at(Duration::from_secs(61) + DRAW_FALLBACK).await;
        let draw = owner.draw_of(&0).expect("drawn once the fallback ran out");
        assert_eq!(
            draw.len(),
            3,
            "sized beside the one piece the open asked for: {draw:?}"
        );
        assert_eq!(
            backing.advertised_pieces(),
            draw.into_iter().collect::<Vec<_>>()
        );
    }

    /// **A second open in the play session draws nothing new.**
    ///
    /// The draw is made once, before playing, and every policy after it --
    /// the next open, a seek's, one under a budget that moved -- adopts it.
    /// Drawn again, a second open would either share pieces nobody sized the
    /// read-ahead against or leave out pieces a peer has been told of.
    #[tokio::test]
    async fn a_second_open_of_the_play_session_adopts_the_draw_and_advertises_nothing() {
        let (backing, owner, budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        let draws = owner.draws();
        let advertised = backing.advertised.lock().clone();
        assert!(!advertised.is_empty());

        owner.clear(&0).await;
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        // And under a roomier budget, which would draw more on its own.
        budget.set(Some(8 * PIECE - 1), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Resized);
        assert_eq!(owner.draws(), draws, "a later open redrew the set");
        assert_eq!(
            *backing.advertised.lock(),
            advertised,
            "a later open advertised something"
        );
    }

    /// **An advertise the backend refuses still leaves the draw decided**:
    /// the entity is bounded, the draw is out of every reclaim's reach, and
    /// the refusal costs the swarm the pieces -- ours and unshared -- and
    /// nothing else. Nothing is withdrawn anywhere, so there is nothing a
    /// refusal could leave half done.
    #[tokio::test]
    async fn a_refused_advertise_keeps_the_draw_out_of_every_reclaims_reach() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = (0..8).collect();
        backing.fail_advertise.store(true, Ordering::SeqCst);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        assert!(backing.advertised.lock().is_empty());
        let drawn: BTreeSet<u32> = owner
            .draws()
            .into_iter()
            .find(|(key, _)| *key == 0)
            .map(|(_, draw)| draw)
            .expect("the draw is recorded whatever the backend said");
        assert_eq!(drawn.len(), 2);
        owner.note_position(&0, (0, 4 * PIECE));
        backing.read_from(4 * PIECE, 1);
        let claim = owner.turn(&0).await.expect("the turn");
        let concluded = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert!(concluded.reclaimed > 0, "the pass reclaimed nothing");
        assert!(
            drawn.iter().all(|piece| backing.on_disk().contains(piece)),
            "a drawn piece was reclaimed: {:?} of {drawn:?}",
            backing.on_disk()
        );
    }

    /// **No owner lock is held across a backing call**: a backing that
    /// takes the entity's own state lock inside `advertise` gets it. Asked
    /// with `try_lock` so a violation is an assertion and not a hang.
    #[tokio::test]
    async fn a_backing_that_takes_the_state_lock_inside_advertise_does_not_deadlock() {
        let (backing, owner, _budget) = torrent();
        let got_it = Arc::new(parking_lot::Mutex::new(Vec::new()));
        {
            let entity = owner.entity(0, domain(0, 0..8));
            let got_it = got_it.clone();
            *backing.on_advertise.lock() = Some(Box::new(move || {
                got_it.lock().push(entity.state.try_lock().is_some());
            }));
        }
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        owner.clear(&0).await;
        assert_eq!(
            *got_it.lock(),
            vec![true],
            "L2 was held across advertise: the draw's one run, at the install"
        );
    }

    /// **The door refuses what the pass published and what has been
    /// promised since**, at the instant it is asked, and everything once the
    /// entity keeps everything.
    ///
    /// The published set is the pass's own reading, and it does not move
    /// under the door: a reader ending mid-reclaim does not open its region
    /// up, and a playhead moving does not close one. What *does* change is a
    /// promise, because a parked read writes one into the set under the same
    /// lock the pass publishes under -- a read that parks mid-reclaim is
    /// waiting on that piece now, and nothing else can say so in time.
    ///
    /// A playhead that merely moves changes nothing until the next pass
    /// republishes -- the tolerated race, which costs a re-fetch.
    #[tokio::test]
    async fn the_door_refuses_what_was_published_and_what_has_been_promised_since() {
        let (backing, owner, budget) = proxy();
        // A two-piece window, so a second head in the back half of the file
        // does not cover the whole reclaim.
        budget.set(Some(2 * PIECE), None);
        let reader = owner.reader(0, domain(0, 0..8));
        // A second player at piece 7 whose body ends while the unlinks run:
        // its window is in the pass's own windows and nowhere else by then.
        let second = owner.reader(0, domain(0, 0..8));
        drop(second.note((0, 7 * PIECE)));
        backing.seek_to(7 * PIECE);
        let second = parking_lot::Mutex::new(Some(second));
        // A body framed over piece 4 that ends while the unlinks run: the
        // pass snapshotted its promise at the re-read and honours it to the
        // end.
        let ended = owner.reader(0, domain(0, 0..8));
        ended.promises(4..5);
        let ended = parking_lot::Mutex::new(Some(ended));
        // A third player arriving during the unlinks.
        let third = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.seek_to(0);
        let answers = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let hook: Hook<ProxySide> = Box::new({
            let answers = answers.clone();
            let backing = backing.clone();
            move |door: &Door<Proxy>| {
                let mut answers = answers.lock();
                // The pass's windows are 0..2 and 6..8; 2..6 is the reclaim.
                answers.push(("pass window", door.refuses(1)));
                drop(second.lock().take());
                answers.push(("inside an open stream's lookahead", door.refuses(7)));
                drop(ended.lock().take());
                answers.push(("promised at the re-read", door.refuses(4)));
                answers.push(("free before", door.refuses(5)));
                third.promises(5..6);
                answers.push(("promised since", door.refuses(5)));
                answers.push(("free before", door.refuses(3)));
                // A note during a pass starts nothing, but its window is
                // live at the door.
                assert!(third.note((0, 3 * PIECE)).is_none());
                backing.seek_to(3 * PIECE);
                answers.push(("a head that moved since", door.refuses(3)));
                assert!(!door.shut(), "nothing has closed the door");
                backing.keeps_everything.store(true, Ordering::SeqCst);
                answers.push(("pinned", door.refuses(2)));
                assert!(
                    door.shut(),
                    "a pin takes nothing more, whatever the run says"
                );
                backing.keeps_everything.store(false, Ordering::SeqCst);
            }
        });
        *backing.on_reclaim.lock() = Some(hook);
        let _outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            *answers.lock(),
            vec![
                ("pass window", true),
                ("inside an open stream's lookahead", true),
                ("promised at the re-read", true),
                ("free before", false),
                ("promised since", true),
                ("free before", false),
                ("a head that moved since", false),
                ("pinned", true),
            ]
        );
        // What the consumer is reading ahead over (0, 1, 2) stayed, and so
        // did 4 (promised at the re-read) and 5 (promised since). What went
        // is what nothing asked for.
        assert!(
            backing.on_disk().contains(&4) && backing.on_disk().contains(&5),
            "a promise is not the pass's to take: {:?}",
            backing.on_disk()
        );
    }

    /// **A pin taken between two runs of one reclaim stops the second
    /// run**: [`Door::shut`] answers `true` and the run is never asked
    /// about.
    #[tokio::test]
    async fn a_pin_taken_between_the_runs_of_a_reclaim_stops_it_before_the_second() {
        let (backing, owner, _budget) = torrent();
        // The whole file on the disk and one consumer at piece 4, so what
        // it is not reading ahead over is over the allowance and the
        // reclaim has runs on both sides of it.
        *backing.held.lock() = (0..8).collect();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 4 * PIECE));
        backing.read_from(4 * PIECE, 1);
        *backing.between_runs.lock() = Some(Box::new({
            let backing = backing.clone();
            move || backing.keeps_everything.store(true, Ordering::SeqCst)
        }));
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            *backing.reclaims.lock(),
            vec![vec![2..4, 7..8]],
            "the coldest pieces of what nothing is asking for"
        );
        assert_eq!(outcome.reclaimed, 2);
        // The next pass finds the pin first: the policy goes, nothing is
        // listed, and the claim is released under the state lock like any
        // other exit's.
        let claim = owner.turn(&0).await.expect("the turn");
        let (probe, waiter) = watch_release(&owner.lookup(&0).unwrap());
        assert!(
            owner
                .pass(&0, &(), claim, Mode::Live)
                .await
                .concluded
                .is_none()
        );
        assert!(
            probe.under_state.load(Ordering::SeqCst),
            "a pass under a pin released its claim outside the state lock"
        );
        drop(waiter);
        assert!(owner.holding(&0).unwrap().installed.is_none());
        // **And nothing was said to the backend but the draw**, at the
        // install: the reclaim took none of it, and the pin announces the
        // file through its own door, not through the policy it clears.
        assert_eq!(
            *backing.advertised.lock(),
            vec![0..2],
            "the draw's pieces, which no reclaim may take and no pin un-announce"
        );
        assert_eq!(backing.reclaims.lock().len(), 1);
    }

    /// **A slack pass takes nothing a peer may have been told of while the
    /// torrent is in the swarm, and all of it once the torrent has left.**
    ///
    /// The draw is advertised, and the backend announces each piece of it
    /// it holds: there is no un-Have, and an announcement ends only with the
    /// torrent leaving the swarm. So the pass over a file the viewer left
    /// takes nothing then -- not even what nobody was told of, since the
    /// play session ends with the torrent stopped whatever it fetched -- and
    /// leaves the policy standing; out of the swarm, as the torrent's
    /// `EndShares` has it, the same pass takes everything. The fake fails
    /// any unlink of an advertised piece while it is in the swarm.
    #[tokio::test]
    async fn a_slack_pass_takes_nothing_announced_while_the_torrent_is_in_the_swarm() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = (0..8).collect();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        assert_eq!(backing.advertised_pieces(), vec![0, 1]);
        // The viewer leaves it: slack, and only the swarm keeps it.
        *backing.playing.lock() = None;
        let opens = owner.opens_of(&0);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(outcome.concluded.is_none(), "the pass took something");
        assert_eq!(backing.on_disk(), (0..8).collect::<Vec<_>>());
        assert!(
            owner.holding(&0).unwrap().installed.is_some(),
            "and the entity stands as it was, for the pass that will run"
        );

        backing.in_swarm.store(false, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            8
        );
        assert!(backing.on_disk().is_empty());
        assert!(owner.holding(&0).is_none(), "and the entity is forgotten");
    }

    /// **And a play session that shared nothing goes with the torrent in
    /// the swarm**: nothing of it was advertised, so nothing of it was
    /// announced, and there is nothing to wait for.
    #[tokio::test]
    async fn a_slack_pass_takes_a_session_that_shared_nothing_while_the_torrent_is_in_the_swarm() {
        let (backing, owner, budget) = torrent();
        *backing.held.lock() = (0..8).collect();
        // One piece of budget: all of it the window's, none of it shared.
        budget.set(Some(PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert!(backing.advertised.lock().is_empty());
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            8
        );
    }

    /// **A slack pass asks again, under the turn, whether the entity is
    /// still slack -- and a viewer who started this file in the meantime
    /// keeps everything.**
    ///
    /// The mode is decided by the driver from a reading taken before its
    /// first entity, and the pass runs seconds later. What can happen in
    /// between is a viewer starting exactly this file: the liveness cell
    /// moves first, the install follows. A pass that carried its mode
    /// through would hold the window they are inside back from the swarm,
    /// throw the fresh policy away and unlink what they are reading -- and
    /// the door below would gate none of it, because the door is asked at
    /// the unlink and the policy is gone before the first one.
    #[tokio::test]
    async fn a_slack_pass_takes_nothing_from_a_file_being_played_again() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        // What the driver read when it decided, and then the viewer.
        let opens = owner.opens_of(&0);
        backing.is_live.store(true, Ordering::SeqCst);
        backing.advertised.lock().clear();

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;

        assert!(outcome.concluded.is_none(), "nothing was concluded");
        assert_eq!(backing.on_disk(), vec![0, 1, 2], "and nothing was taken");
        assert!(
            backing.advertised.lock().is_empty(),
            "the slack pass said nothing to the backend"
        );
        assert!(
            owner
                .holding(&0)
                .expect("the entity stands")
                .installed
                .is_some(),
            "the policy the open installed stands"
        );
    }

    /// **And so does a viewer who opened it before the cell could say so.**
    ///
    /// The liveness cell moves for the file being *played*; an open on
    /// another file of the same torrent while the film is still being read
    /// is an aside -- a subtitle -- and deliberately moves nothing. There
    /// is no instant to ask about for one of those: the install has
    /// finished and the reader is not open yet, so "is it live?" and "is
    /// anything reading it?" both answer no while a stream is being handed
    /// out on it.
    ///
    /// So the pass compares counts instead of asking again. Every open
    /// takes this entity's turn ([`Retention::install`]), so an open the
    /// driver did not see is either in this number or still waiting behind
    /// the pass -- including one that found the policy already right and
    /// installed nothing.
    #[tokio::test]
    async fn a_slack_pass_takes_nothing_from_an_entity_a_stream_opened_since() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let opens = owner.opens_of(&0);
        // The aside: an open that changes nothing anybody can ask about.
        assert_eq!(
            owner.install(0, 0).await,
            InstallOutcome::Kept,
            "the policy was already right for this file and this budget"
        );
        assert!(!backing.is_live.load(Ordering::SeqCst));
        assert_eq!(owner.readers_of(&0), 0);
        backing.advertised.lock().clear();

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;

        assert!(outcome.concluded.is_none());
        assert_eq!(backing.on_disk(), vec![0, 1, 2]);
        assert!(backing.advertised.lock().is_empty());
        assert!(owner.holding(&0).unwrap().installed.is_some());

        // With the count the driver read now the current one, the same pass
        // empties it -- once the torrent is out of the swarm, since two of
        // those pieces are the draw's.
        backing.in_swarm.store(false, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Slack { opens })
            .await
            .concluded
            .expect("a pass that ran");
        assert_eq!(outcome.reclaimed, 3);
        assert!(backing.on_disk().is_empty());
    }

    /// **And a read that opened on it in the same gap.**
    ///
    /// An open read is not what makes an entity live, but it is a response
    /// being delivered out of these bytes, and [`Mode::Slack`] means nobody
    /// is reading it either. The reader can arrive after the driver counted
    /// the readers and before the pass takes the turn -- the install and the
    /// reader are two steps with a backend call between them -- so the
    /// readers are counted again here.
    #[tokio::test]
    async fn a_slack_pass_takes_nothing_from_an_entity_with_a_read_open() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let opens = owner.opens_of(&0);
        let reader = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        assert!(
            reader.note((0, 0)).is_none(),
            "the tick is the torrent's trigger"
        );
        backing.read_from(0, 1);
        backing.advertised.lock().clear();

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;

        assert!(outcome.concluded.is_none());
        assert_eq!(backing.on_disk(), vec![0, 1, 2]);
        assert!(backing.advertised.lock().is_empty());

        // The body ends, and the file nobody is playing is slack again; out
        // of the swarm, its draw goes with the rest.
        drop(reader);
        backing.in_swarm.store(false, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            3
        );
    }

    /// **A pin outranks the slack: nothing of a pinned entity is taken.**
    ///
    /// A pin is a retention property -- the user asked for those bytes --
    /// and the slack pass is the one delete in this owner that would take a
    /// whole extent, so it is also the one that would empty a pinned
    /// download, which is shared whole for as long as the pin stands.
    #[tokio::test]
    async fn a_pinned_entity_is_never_slack() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let opens = owner.opens_of(&0);
        backing.keeps_everything.store(true, Ordering::SeqCst);
        backing.advertised.lock().clear();

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;

        assert!(outcome.concluded.is_none());
        assert_eq!(backing.on_disk(), vec![0, 1, 2]);
        assert!(
            backing.advertised.lock().is_empty(),
            "the slack pass said nothing to the backend about a pinned file"
        );
        assert!(
            owner.holding(&0).unwrap().installed.is_none(),
            "the policy went with the pin, as it does under the live pass"
        );
        assert_eq!(
            backing.listings.load(Ordering::SeqCst),
            0,
            "and the pin was found before the disk was read"
        );
    }

    /// **A slack pass that could take nothing leaves no window standing.**
    ///
    /// The pass drops the policy under the turn whether or not the unlinks
    /// that follow succeed, and the windows go with it. A window is what
    /// some pass measured round a head somebody was at; left standing on an
    /// entity nobody is playing it tells [`Retention::holdings`] that the
    /// left file's pieces are protected -- which is a piece with protection
    /// and no deleter, the shape this slice exists to remove.
    #[tokio::test]
    async fn a_slack_pass_that_took_nothing_leaves_no_window_standing() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        backing.read_from(0, 1);
        let claim = owner.turn(&0).await.expect("the turn");
        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a live pass");
        assert!(
            !owner.holding(&0).unwrap().windows.is_empty(),
            "the live pass concluded what its consumer was asking for"
        );

        // The unlinks die, so the entity survives its own slack pass and
        // the next tick has to walk it again. Out of the swarm, so the pass
        // runs at all: two of these pieces are the draw's.
        backing.in_swarm.store(false, Ordering::SeqCst);
        backing.reclaim_panics.store(true, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            0
        );
        let holding = owner.holding(&0).expect("the entity still holds its bytes");
        assert!(holding.installed.is_none(), "the policy went");
        assert!(
            holding.windows.is_empty(),
            "and so did the window it had measured"
        );
        assert!(
            owner.draws().is_empty(),
            "and so did the play session's draw: the next open is a new session"
        );
    }

    /// **A slack pass that took everything it could take forgets the
    /// entity, a shared boundary piece notwithstanding.** The piece another
    /// file shares is never this file's to take (`Backing::alone`), so
    /// measured against everything held the file was never empty, and its
    /// entity stood for the rest of the process over nothing of its own.
    #[tokio::test]
    async fn a_slack_pass_forgets_an_entity_whose_last_piece_is_shared() {
        let (backing, owner, _budget) = torrent();
        // File zero's eight pieces (the fake's listing is not narrowed to
        // a file, so only file zero's are on it), the last of which file
        // one shares.
        *backing.held.lock() = (0..8).collect();
        backing.shared.lock().insert(7);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        // Out of the swarm, as the torrent's `EndShares` has it: two of
        // these pieces are the draw's.
        backing.in_swarm.store(false, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        let reclaimed = owner
            .pass(&0, &(), claim, Mode::Slack { opens })
            .await
            .concluded
            .expect("a pass that ran")
            .reclaimed;

        assert_eq!(reclaimed, 7, "everything file zero has alone");
        assert_eq!(backing.on_disk(), vec![7], "the shared piece stays");
        assert!(
            !owner.holdings().iter().any(|(key, _)| *key == 0),
            "and the entity is forgotten: nothing is left that a pass over it may take"
        );
    }

    /// **A file watched again after its slack pass emptied it is a new play
    /// session, with a draw of its own.**
    ///
    /// The slack pass takes the draw with the bytes -- it runs once the
    /// torrent is out of the swarm, whose advertised set the torrent's
    /// `EndShares` then clears -- and forgets the entity. The next open is a
    /// fresh start: under a budget that now covers the file, its draw is the
    /// whole file. And an entity that goes straight to a policy advertises
    /// its draw and nothing else.
    #[tokio::test]
    async fn a_rewatch_after_the_slack_pass_is_a_new_play_session_with_a_draw_of_its_own() {
        let (backing, owner, budget) = torrent();
        *backing.held.lock() = (0..8).collect();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        assert_eq!(*backing.advertised.lock(), vec![0..2], "the first draw");
        // The viewer leaves it, and the torrent leaves the swarm.
        *backing.playing.lock() = None;
        backing.in_swarm.store(false, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            8
        );
        assert!(
            owner.holding(&0).is_none(),
            "the emptied entity is forgotten"
        );
        assert!(owner.draws().is_empty(), "and its draw with it");

        // The rewatch, under a budget that covers the file: nothing to
        // install, and the whole file shared.
        budget.set(Some(8 * PIECE), None);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Unbounded);
        assert_eq!(*backing.advertised.lock(), vec![0..2, 0..8]);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Unbounded);
        assert_eq!(
            *backing.advertised.lock(),
            vec![0..2, 0..8],
            "and a second open of the session advertises nothing again"
        );

        // A fresh entity that goes straight to a policy advertises its draw.
        budget.set(Some(4 * PIECE), None);
        assert_eq!(play(&owner, &backing, 1).await, InstallOutcome::Installed);
        let second: Vec<u32> = owner
            .draws()
            .into_iter()
            .find(|(key, _)| *key == 1)
            .map(|(_, draw)| draw.into_iter().collect())
            .expect("the second file's draw");
        assert_eq!(second.len(), 2);
        assert_eq!(
            backing.advertised.lock()[2..]
                .iter()
                .flat_map(|run| run.clone())
                .collect::<Vec<_>>(),
            second
        );
    }

    /// **A pin on a file nobody plays wants it whole again on the slack
    /// pass, because the slack pass is the one that runs over it.**
    ///
    /// A pinned download nobody is watching is read by the driver as
    /// [`Mode::Slack`] every tick, so the slack pass's pin exit has to want
    /// the pieces back itself. The pin's own install only clears the policy
    /// when the file is opened, and an offline download is pinned to be
    /// fetched *without* being opened -- without this, the pieces the slack
    /// pass dropped would stay unwanted, so a download the user asked to
    /// keep would stand still. What the pin shares is the engine's to
    /// advertise, not the owner's.
    #[tokio::test]
    async fn a_pin_on_a_slack_entity_nobody_plays_wants_it_whole_on_the_slack_pass() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));

        // Out of the swarm, the slack pass drops the policy and then cannot
        // unlink.
        backing.in_swarm.store(false, Ordering::SeqCst);
        backing.reclaim_panics.store(true, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(owner.holding(&0).expect("the entity").installed.is_none());
        assert!(
            backing.wanted_all.lock().is_empty(),
            "a slack pass wants nothing"
        );

        // The user pins the file and does not open it: the next tick reads
        // it as slack, and that pass is the one that has to put it right.
        backing.reclaim_panics.store(false, Ordering::SeqCst);
        backing.keeps_everything.store(true, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(
            outcome.concluded.is_none(),
            "a pinned entity is never slack"
        );
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2],
            "and nothing of it is taken"
        );
        assert_eq!(
            *backing.wanted_all.lock(),
            vec![0..8],
            "and every piece of it is wanted again"
        );
    }

    /// **A pinned entity is wanted whole by the pass that cleared it, and
    /// by no pass after.**
    ///
    /// The pin exit runs every tick for as long as the pin stands, and after
    /// the first there is nothing installed to clear. `want_all` is a
    /// reselect of every piece of the file under the torrent's write lock;
    /// once is what changes anything, and every two seconds was the cost of
    /// asking a question whose answer was already in force.
    #[tokio::test]
    async fn a_pinned_entity_is_wanted_whole_once_and_not_on_every_pass() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        backing.keeps_everything.store(true, Ordering::SeqCst);
        let opens = owner.opens_of(&0);

        for (tick, mode) in [
            Mode::Live,
            Mode::Slack { opens },
            Mode::Live,
            Mode::Slack { opens },
            Mode::Live,
        ]
        .into_iter()
        .enumerate()
        {
            let claim = owner.turn(&0).await.expect("the turn");
            assert!(
                owner.pass(&0, &(), claim, mode).await.concluded.is_none(),
                "tick {tick}: a pinned entity's pass concludes nothing"
            );
            assert!(owner.holding(&0).unwrap().installed.is_none());
            assert_eq!(
                *backing.wanted_all.lock(),
                vec![0..8],
                "tick {tick}: wanted whole by the pass that cleared the policy, and by no other"
            );
        }
    }

    /// **The driver's two counts come as one reading**: no reader and no
    /// open for a key with no entity, and then the reads that have
    /// delivered beside the streams that have opened, off one lock.
    ///
    /// What this pins is the values. That they are read under one
    /// acquisition is the shape of the method -- one guard, two fields --
    /// and no interleaving a test can construct gets between two reads
    /// that are not there.
    #[tokio::test]
    async fn the_readers_and_the_opens_are_one_reading() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.readers_and_opens_of(&0), (0, 0));
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert_eq!(
            owner.readers_and_opens_of(&0),
            (0, 1),
            "opened, nothing read"
        );
        let reader = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity");
        assert!(reader.note((0, 0)).is_none());
        backing.read_from(0, 1);
        assert_eq!(owner.readers_and_opens_of(&0), (1, 1), "a read delivering");
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Kept);
        assert_eq!(
            owner.readers_and_opens_of(&0),
            (1, 2),
            "an aside's open counts before its reader does"
        );
        drop(reader);
        assert_eq!(owner.readers_and_opens_of(&0), (0, 2), "the read ended");
        assert_eq!(
            (owner.readers_of(&0), owner.opens_of(&0)),
            owner.readers_and_opens_of(&0),
            "the same two facts the two readings give"
        );
    }

    /// **An entity a reader is open on is never forgotten, even emptied.**
    ///
    /// [`Retention::forget_empty`] prunes on a fact -- it holds nothing and
    /// nobody is reading it -- and both halves are asked under L1 after the
    /// pass has let go of its own `Arc`. A [`Reader`] that has delivered
    /// nothing yet is a stream handed out a moment ago, whose first byte
    /// has not gone out: nothing has observed it, so it does not stop the
    /// slack pass -- and forgetting the entity under it would leave the
    /// read with no head and so no window for the pass that follows.
    #[tokio::test]
    async fn an_emptied_entity_a_reader_holds_is_not_forgotten() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let opens = owner.opens_of(&0);
        let reader = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        assert_eq!(owner.readers_of(&0), 0, "it has delivered nothing");
        // Out of the swarm, as the torrent's `EndShares` has it: two of
        // these pieces are the draw's.
        backing.in_swarm.store(false, Ordering::SeqCst);

        let claim = owner.turn(&0).await.expect("the turn");
        assert_eq!(
            owner
                .pass(&0, &(), claim, Mode::Slack { opens })
                .await
                .concluded
                .expect("a pass that ran")
                .reclaimed,
            3
        );
        assert!(
            owner.holding(&0).is_some(),
            "the entity a stream is being read out of stays"
        );

        // Once that read is over, the next pass forgets it.
        drop(reader);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(owner.holding(&0).is_none());
    }

    /// **A byte of another key moves nothing here, and a listing we do not
    /// have concludes nothing** and leaves the policy where it is.
    ///
    /// Each entity has its own head, and only its own bytes reach it. A
    /// byte noted for file 1 -- before file 0 has a head, or while file 0's
    /// pass is at its listing -- leaves file 0's head where it was: no head,
    /// so no listing and nothing concluded; then the head at piece 0, so the
    /// pass concludes on it as if nothing had happened elsewhere.
    #[tokio::test]
    async fn a_byte_of_another_key_moves_no_head_here_and_a_failed_listing_concludes_nothing() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        owner.note_position(&1, (1, 0));
        assert_eq!(
            owner.holding(&0).unwrap().last_position,
            None,
            "a byte of file 1 gave file 0 a head"
        );
        let claim = owner.turn(&0).await.expect("the turn");
        assert!(
            owner
                .pass(&0, &(), claim, Mode::Live)
                .await
                .concluded
                .is_none()
        );
        assert!(backing.reclaims.lock().is_empty());
        assert_eq!(
            backing.listings.load(Ordering::SeqCst),
            0,
            "a file with no head paid a listing to learn it"
        );
        // A byte of file 1 lands while file 0's listing runs: file 0's head
        // is still piece 0 at the re-read, and the pass concludes on it.
        owner.note_position(&0, (0, 0));
        let (entered, release) = backing.park_held();
        let claim = owner.turn(&0).await.expect("the turn");
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        owner.note_position(&1, (1, 0));
        release.send(()).expect("the parked pass");
        let concluded = pass
            .await
            .expect("joined")
            .concluded
            .expect("a byte of another file stopped this file's pass");
        assert_eq!(
            concluded.windows,
            Vec::<Range<u32>>::new(),
            "a byte of another key made no consumer of this one"
        );
        assert_eq!(concluded.reclaimed, 6);
        assert_eq!(owner.holding(&0).unwrap().last_position, Some((0, 0)));
        assert_eq!(backing.reclaims.lock().len(), 1);
        // And a listing the disk would not give: nothing concluded, nothing
        // withdrawn, the policy standing.
        backing.holds(0..16);
        backing.fail_held.store(true, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        assert!(
            owner
                .pass(&0, &(), claim, Mode::Live)
                .await
                .concluded
                .is_none()
        );
        assert_eq!(backing.reclaims.lock().len(), 1);
        let holding = owner.holding(&0).unwrap();
        assert!(holding.installed.is_some());
        assert_eq!(
            holding.windows,
            Vec::<Range<u32>>::new(),
            "a byte of another key made no consumer of this one"
        );
        assert_eq!(
            *backing.advertised.lock(),
            vec![0..2],
            "the draw, advertised at the install and never withdrawn"
        );
    }

    /// **A budget that changes under a pass replaces the policy on the
    /// delivered byte, and the pass concludes nothing**: no reclaim call,
    /// windows unchanged, the new policy standing. And under
    /// `Install::OnOpen` the budget is nothing the note looks at.
    #[tokio::test]
    async fn an_install_on_the_delivered_byte_during_a_pass_makes_the_pass_conclude_nothing() {
        let (backing, owner, budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(first.reclaimed, 5);
        let windows_before = owner.holding(&0).unwrap().windows.clone();
        assert_eq!(backing.reclaims.lock().len(), 1);

        let claim = reader.note((0, 2 * PIECE)).expect("moved a stride");
        backing.read_from(2 * PIECE, 1);
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        budget.set(Some(2 * PIECE), None);
        // The byte that observes the new budget decides under it, under L2
        // alone, while the pass holds the turn.
        assert!(reader.note((0, 2 * PIECE)).is_none());
        backing.read_from(2 * PIECE, 1);
        let mid = owner.holding(&0).unwrap();
        assert_eq!(mid.decided, Some(CacheBudget::Bytes(2 * PIECE)));
        assert_eq!(
            mid.installed.as_ref().map(|i| i.budget),
            Some(CacheBudget::Bytes(2 * PIECE))
        );
        release.send(()).expect("the parked pass");
        let overtaken = pass.await.expect("joined");
        assert!(
            overtaken.concluded.is_none(),
            "a pass measured under the old budget concluded something"
        );
        assert_eq!(
            backing.reclaims.lock().len(),
            1,
            "the overtaken pass reclaimed"
        );
        let after = owner.holding(&0).unwrap();
        assert_eq!(after.windows, windows_before);
        assert_eq!(
            after.installed.as_ref().map(|i| i.budget),
            Some(CacheBudget::Bytes(2 * PIECE)),
            "the overtaken pass wrote its policy back over the new one"
        );
        // The byte that decided the new budget was due and found the turn
        // taken: the refused pass hands its claim on, and the successor
        // measures under the new policy from where that byte left the head.
        let again = overtaken
            .again
            .expect("the byte that replaced the policy is owed a pass");
        let successor = owner.pass(&0, &(), again, Mode::Live).await;
        assert_eq!(
            successor.concluded.expect("the successor").windows,
            vec![2..6]
        );
        assert!(successor.again.is_none());
        assert_eq!(owner.holding(&0).unwrap().windows, vec![2..6]);
        assert_eq!(backing.on_disk(), vec![2]);

        // The same publish landing during the unlinks rather than the
        // listing: the re-read has passed, the unlinks stand as refetch cost,
        // and the conclusion is still not written.
        budget.set(Some(4 * PIECE), None);
        // A publish makes every reader due again, whatever the stride: the
        // old `passed_at` described a shape that no longer exists.
        let claim = reader
            .note((0, 2 * PIECE))
            .expect("a budget change makes a paused reader due");
        backing.read_from(2 * PIECE, 1);
        let settled = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        let windows_before = settled.windows;
        assert_eq!(windows_before, vec![2..6]);
        let claim = reader.note((0, 5 * PIECE)).expect("moved a stride");
        backing.read_from(5 * PIECE, 1);
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the reclaim");
        budget.set(Some(2 * PIECE), None);
        assert!(reader.note((0, 5 * PIECE)).is_none());
        backing.read_from(5 * PIECE, 1);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        assert_eq!(
            outcome.concluded.expect("a pass that ran").windows,
            vec![2..6, 5..8]
        );
        assert_eq!(backing.reclaims.lock().len(), 4, "the unlinks were made");
        let after = owner.holding(&0).unwrap();
        assert_eq!(
            after.windows, windows_before,
            "a conclusion measured under a cap nobody holds was written"
        );
        assert_eq!(
            after.installed.as_ref().map(|i| i.budget),
            Some(CacheBudget::Bytes(2 * PIECE))
        );
        // And this one too owes the pass its overtaking byte could not
        // start; the successor writes the first conclusion under the new
        // budget.
        let again = outcome.again.expect("owed");
        let successor = owner.pass(&0, &(), again, Mode::Live).await;
        assert!(successor.again.is_none());
        assert_eq!(owner.holding(&0).unwrap().windows, vec![2..6, 5..8, 5..8]);

        // A proxy entity has held nothing back, so clearing it asks the
        // backing for nothing -- and neither does installing on one.
        owner.clear(&0).await;
        assert!(owner.holding(&0).unwrap().installed.is_none());
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert!(backing.advertised.lock().is_empty());

        // OnOpen: the note carries the byte and nothing else.
        let (_backing, owner, budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        budget.set(Some(2 * PIECE), None);
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(
            reader.note((0, PIECE)).is_none(),
            "the tick is the torrent's trigger"
        );
        backing.read_from(PIECE, 1);
        owner.note_position(&0, (0, 2 * PIECE));
        let holding = owner.holding(&0).unwrap();
        assert_eq!(
            holding.installed.as_ref().map(|i| i.budget),
            Some(CacheBudget::Bytes(4 * PIECE)),
            "a note rebuilt a torrent policy, which only an install may"
        );
        assert_eq!(holding.decided, None);
        assert_eq!(holding.last_position, Some((0, 2 * PIECE)));
    }

    /// **A last byte delivered under a new budget while a pass is at its
    /// listing gets its pass.**
    ///
    /// The interleaving: the reader's byte at piece 2 claims the turn and
    /// its pass parks at the listing; a smaller budget is published; the
    /// body's last byte, piece 6, lands -- it decides the new policy under
    /// L2, is due (every reader is), tries the turn, finds it taken, and
    /// the body ends. The pass resumes and refuses at its
    /// re-read, correctly: it measured for a budget nobody holds. Dropped
    /// there with no `again`, the claim takes the last byte's pass with it,
    /// and the tail the fill wrote stays over budget until the grace prunes
    /// the entity. The owner hands the claim on from the refusal too.
    #[tokio::test]
    async fn the_last_byte_under_a_new_budget_during_a_refused_pass_gets_its_pass() {
        let (backing, owner, budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(first.windows, vec![0..3]);
        let claim = reader.note((0, 2 * PIECE)).expect("moved a stride");
        backing.read_from(2 * PIECE, 1);
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        // The fill writes ahead while the pass waits on the disk.
        backing.holds(4..8);
        budget.set(Some(2 * PIECE), None);
        assert!(
            reader.note((0, 6 * PIECE)).is_none(),
            "a note during a pass took the turn"
        );
        backing.read_from(6 * PIECE, 1);
        release.send(()).expect("the parked pass");
        let refused = pass.await.expect("joined");
        assert!(
            refused.concluded.is_none(),
            "a pass measured under the old budget concluded something"
        );
        assert_eq!(backing.reclaims.lock().len(), 1);
        assert_eq!(owner.holding(&0).unwrap().windows, vec![0..3]);
        let again = refused
            .again
            .expect("the last byte, delivered under the new budget, is owed a pass");
        let successor = owner.pass(&0, &(), again, Mode::Live).await;
        assert_eq!(
            successor.concluded.expect("the successor").windows,
            vec![2..6, 6..8]
        );
        assert!(successor.again.is_none());
        assert_eq!(owner.holding(&0).unwrap().windows, vec![2..6, 6..8]);
        assert!(owner.try_turn(&0).is_some(), "the turn was not released");
    }

    /// **A listing that fails owes a pass only for a byte that moved a
    /// stride while it failed**, and leaves the reader due where it stood.
    ///
    /// Nothing was measured, so nothing is written -- not the windows and
    /// not `passed_at`. But a byte that moved the head a stride during the
    /// failure is the same swallowed byte as during any pass, and is owed
    /// the same. Asked of the head the pass started from, not of nothing:
    /// a disk that will not list is asked once per stride, not once per
    /// byte, and never in a loop.
    #[tokio::test]
    async fn a_failed_listing_owes_a_pass_only_for_a_byte_that_moved_a_stride() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        backing.fail_held.store(true, Ordering::SeqCst);
        let claim = reader.note((0, 0)).expect("due");
        backing.seek_to(0);
        let (probe, waiter) = watch_release(&owner.lookup(&0).unwrap());
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_none());
        assert!(
            outcome.again.is_none(),
            "a failed listing armed a pass off a head that did not move"
        );
        assert!(
            probe.under_state.load(Ordering::SeqCst),
            "a failed listing released its claim outside the state lock"
        );
        drop(waiter);
        let claim = reader
            .note((0, 0))
            .expect("a pass that measured nothing left the reader due where it stood");
        backing.seek_to(0);
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        assert!(reader.note((0, 3 * PIECE)).is_none());
        backing.seek_to(3 * PIECE);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        assert!(outcome.concluded.is_none());
        let again = outcome
            .again
            .expect("the byte that moved a stride during the failed listing is owed a pass");
        backing.fail_held.store(false, Ordering::SeqCst);
        let successor = owner.pass(&0, &(), again, Mode::Live).await;
        assert_eq!(
            successor.concluded.expect("the successor").windows,
            vec![3..6]
        );
        assert_eq!(backing.listings.load(Ordering::SeqCst), 3);
    }

    /// **An entity nothing bounds is never due, and a pass over it lists
    /// nothing**: no budget, a budget that covers it, a domain no policy can
    /// be sized for. A key with no entity remembers no byte and has nothing
    /// to pass over, and an entity keeps the domain it was made with.
    #[tokio::test]
    async fn an_unbounded_entity_is_never_due_and_a_pass_over_it_lists_nothing() {
        let (backing, owner, budget) = proxy();
        budget.set(None, None);
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(
            reader.note((0, 0)).is_none(),
            "a byte of an unbounded entity was due"
        );
        backing.read_from(0, 1);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.decided, Some(CacheBudget::Unbounded));
        assert!(holding.installed.is_none());
        let claim = owner.try_turn(&0).expect("the turn");
        let (probe, waiter) = watch_release(&owner.lookup(&0).unwrap());
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_none() && outcome.again.is_none());
        assert!(
            probe.under_state.load(Ordering::SeqCst),
            "a pass over nothing installed released its claim outside the state lock"
        );
        drop(waiter);
        assert_eq!(
            backing.listings.load(Ordering::SeqCst),
            0,
            "a pass over nothing paid a listing"
        );
        owner.note_position(&9, (0, PIECE));
        assert!(
            owner.holding(&9).is_none(),
            "a byte nothing bounds was remembered"
        );
        let claim = owner.try_turn(&0).expect("the turn");
        let outcome = owner.pass(&9, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_none() && outcome.again.is_none());
        assert!(
            owner.try_turn(&0).is_some(),
            "a pass over no entity kept the turn"
        );
        owner.entity(0, domain(0, 0..4));
        assert_eq!(owner.holding(&0).unwrap().domain, domain(0, 0..8));
        // The budget covers the file: decided, and nothing installed.
        budget.set(Some(8 * PIECE), None);
        assert!(reader.note((0, PIECE)).is_none());
        backing.read_from(PIECE, 1);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.decided, Some(CacheBudget::Bytes(8 * PIECE)));
        assert!(holding.installed.is_none());
        // A domain no policy can be sized for: decided, nothing installed,
        // and the error carried out from under the lock.
        let owner = Retention::new(Proxy::new([broken(0, 0..8)]), budget.clone());
        budget.set(Some(4 * PIECE), None);
        let reader = owner.reader(0, broken(0, 0..8));
        assert!(reader.note((0, 0)).is_none());
        backing.read_from(0, 1);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.decided, Some(CacheBudget::Bytes(4 * PIECE)));
        assert!(holding.installed.is_none());
    }

    /// **What `install` answers when it installs nothing**: no budget, a
    /// budget that covers the file, a want that resolves to nothing, a pin;
    /// `Kept` only under the same budget, `Resized` under another over the
    /// same domain; the domain resolved afresh under the turn -- and what the
    /// play session shares decided by its first open, whatever the later
    /// ones find.
    #[tokio::test]
    async fn an_install_that_bounds_nothing_says_so() {
        let (backing, owner, budget) = torrent();
        budget.set(None, None);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Unbounded);
        assert_eq!(
            owner.draws(),
            vec![(0, BTreeSet::new())],
            "no budget to size a shared set against: the session shares nothing"
        );
        budget.set(Some(8 * PIECE), None);
        assert_eq!(
            play(&owner, &backing, 0).await,
            InstallOutcome::Unbounded,
            "a budget that covers the file installed a policy"
        );
        assert!(owner.holding(&0).unwrap().installed.is_none());
        // A budget that covers the file shares all of it -- on a file whose
        // session has not drawn yet.
        assert_eq!(play(&owner, &backing, 1).await, InstallOutcome::Unbounded);
        budget.set(Some(4 * PIECE), None);
        assert_eq!(
            owner.install(2, 2).await,
            InstallOutcome::Unbounded,
            "a want that resolves to nothing was installed"
        );
        assert!(owner.holding(&2).is_none());
        assert_eq!(
            *backing.advertised.lock(),
            vec![8..16],
            "the file whose first open had no budget shares nothing; the one whose \
             first open was covered shares the whole file"
        );
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        // A pin clears, and says nothing to the backend.
        backing.keeps_everything.store(true, Ordering::SeqCst);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Unbounded);
        assert!(owner.holding(&0).unwrap().installed.is_none());
        backing.keeps_everything.store(false, Ordering::SeqCst);
        // The same key under a new budget is resized in place.
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        budget.set(Some(6 * PIECE), None);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Resized);
        assert_eq!(
            owner.holding(&0).unwrap().installed.map(|i| i.budget),
            Some(CacheBudget::Bytes(6 * PIECE))
        );
        // The domain is what the backend says the file is now, and a
        // policy over another domain is replaced.
        backing.domains.lock().insert(0, domain(0, 0..6));
        budget.set(Some(4 * PIECE), None);
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        assert_eq!(owner.holding(&0).unwrap().domain, domain(0, 0..6));
        assert_eq!(
            *backing.advertised.lock(),
            vec![8..16],
            "nothing an open after the first did was said to the backend"
        );
    }

    /// **A pass commits the draw it holds and withdraws nothing the disk
    /// lost.**
    ///
    /// The torrent's share: the draw was advertised at the install, and a
    /// pass that finds a piece of it held commits it; a committed piece the
    /// disk no longer holds leaves the committed set, and nothing is said to
    /// the backend for it -- it announces only what it has, and there is no
    /// un-Have to send. The tick is the torrent's trigger, so a head that
    /// moved during the reclaim arms nothing.
    #[tokio::test]
    async fn a_pass_commits_the_draw_it_holds_and_withdraws_nothing_the_disk_lost() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            (first.committed, first.lost, first.reclaimed),
            (2, 0, 6),
            "the two pieces of this file's draw were held already, so the first pass commits them"
        );
        owner.note_position(&0, (0, PIECE));
        *backing.between_runs.lock() = Some(Box::new({
            let owner = owner.clone();
            move || owner.note_position(&0, (0, 3 * PIECE))
        }));
        let claim = owner.turn(&0).await.expect("the turn");
        let second = owner.pass(&0, &(), claim, Mode::Live).await;
        let conclusion = second.concluded.expect("a pass");
        assert_eq!(
            (conclusion.committed, conclusion.lost),
            (0, 0),
            "the draw was committed by the first pass; this one finds nothing new"
        );
        assert!(second.again.is_none(), "the tick armed a pass");
        assert_eq!(*backing.advertised.lock(), vec![0..2]);
        assert_eq!(
            owner.holding(&0).unwrap().installed.unwrap().committed,
            [0, 1].into_iter().collect()
        );
        // The disk loses the committed piece behind our back.
        backing.held.lock().remove(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        let third = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!((third.committed, third.lost), (0, 1));
        assert_eq!(
            *backing.advertised.lock(),
            vec![0..2],
            "nothing was withdrawn for the piece the disk lost"
        );
        // It coming back commits it again: it is still the draw's.
        backing.holds([0]);
        let claim = owner.turn(&0).await.expect("the turn");
        let fourth = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!((fourth.committed, fourth.lost), (1, 0));
        assert_eq!(*backing.advertised.lock(), vec![0..2]);
    }

    /// **A budget that moved resizes the policy in place, and the draw and
    /// what it committed stay as they were -- whichever way it moved.**
    ///
    /// The budget is republished every minute from the free space, and
    /// every open installs. Rebuilt on each one, the new policy's first pass
    /// would reclaim what the old one had committed and announced.
    ///
    /// **A bigger budget draws nothing more**, because the draw is the play
    /// session's and was sized against its read-ahead before it began; **and
    /// a smaller one takes back nothing**, because there is no un-have. So
    /// the committed set is carried whole, over the new capacity and all,
    /// and those bytes are the price of having said we had them.
    #[tokio::test]
    async fn a_budget_that_moved_resizes_the_policy_and_keeps_what_it_committed() {
        let (backing, owner, budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        for piece in 0..3u64 {
            owner.note_position(&0, (0, piece * PIECE));
            let claim = owner.turn(&0).await.expect("the turn");
            owner.pass(&0, &(), claim, Mode::Live).await;
        }
        let committed = |owner: &Retention<Torrent>| {
            owner
                .holding(&0)
                .unwrap()
                .installed
                .unwrap()
                .committed
                .into_iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(committed(&owner), vec![0, 1]);
        assert_eq!(*backing.advertised.lock(), vec![0..2]);

        // A roomier budget would draw a third piece, 5, of its own: it does
        // not, and piece 5 arriving is a piece like any other.
        budget.set(Some(6 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Resized);
        backing.holds([5]);
        owner.note_position(&0, (0, 3 * PIECE));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            committed(&owner),
            vec![0, 1],
            "a roomier budget grew the draw"
        );
        assert!(
            backing.on_disk().starts_with(&[0, 1]),
            "a committed piece was reclaimed"
        );
        assert_eq!(
            *backing.advertised.lock(),
            vec![0..2],
            "a resize said something to the backend"
        );

        // Two pieces: a window of one and a committed capacity of one. The
        // two pieces already announced are over that and stay announced
        // and on the disk, because nothing here can un-say a Have.
        budget.set(Some(2 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Resized);
        assert_eq!(*backing.advertised.lock(), vec![0..2]);
        assert_eq!(committed(&owner), vec![0, 1]);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(
            backing.on_disk().starts_with(&[0, 1]),
            "{:?}",
            backing.on_disk()
        );
    }

    /// **A reader reports what it holds**: its promise shrinks from the
    /// front as bytes go out and an empty promise records nothing; a seek
    /// back a stride is as due as playing on; the holding says what is
    /// promised, whether a playhead is live, and the extent.
    #[tokio::test]
    async fn a_reader_reports_what_it_holds() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        reader.promises(0..0);
        assert_eq!(owner.readers(), 0, "an empty promise was recorded");
        reader.promises(2..6);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.promised, vec![2..6]);
        assert_eq!(holding.extent, 0..8);
        assert!(!holding.live_playhead, "a promise is not a delivered byte");
        assert_eq!(owner.readers(), 1);
        let claim = reader.note((0, 3 * PIECE)).expect("due");
        backing.read_from(3 * PIECE, 1);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(
            holding.promised,
            vec![4..6],
            "the delivered piece is still promised"
        );
        assert!(holding.live_playhead);
        assert_eq!(holding.last_position, Some((0, 3 * PIECE)));
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_some() && outcome.again.is_none());
        assert!(
            reader.note((0, PIECE)).is_some(),
            "a seek back a stride was not due"
        );
        backing.read_from(PIECE, 1);
        drop(reader);
        let holding = owner.holding(&0).unwrap();
        assert!(holding.promised.is_empty() && !holding.live_playhead);
    }

    /// **A byte in file B between two runs of file A's reclaim leaves A's
    /// pass concluding normally on A's own head**: the byte is B's, A's
    /// head is where it was, A's door still refuses exactly what A's pass
    /// published, and the second run is asked about and taken.
    #[tokio::test]
    async fn a_byte_in_another_file_between_the_runs_of_a_reclaim_leaves_it_running() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2, 6, 7].into_iter().collect();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 4 * PIECE));
        *backing.between_runs.lock() = Some(Box::new({
            let owner = owner.clone();
            move || owner.note_position(&1, (1, 0))
        }));
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            *backing.asked.lock(),
            vec![Vec::<Range<u32>>::new()],
            "nothing is asking for anything of this entity, so the reclaim \
             was handed the whole of what it holds and the door refused none \
             of it"
        );
        assert_eq!(
            outcome.reclaimed, 0,
            "nothing needed the room, so nothing was given back"
        );
        assert_eq!(
            owner.holding(&0).unwrap().last_position,
            Some((0, 4 * PIECE)),
            "a byte of file 1 moved file 0's head"
        );
    }

    /// **Two readers on one entity are both covered by what the door
    /// refuses.** The entity's head is the last byte either delivered, and
    /// what the door refuses is the set the pass published, which covers
    /// what every consumer of the entity is being fetched for; it stands
    /// for the length of the pass, so a reader that ends mid-reclaim does
    /// not open its region up.
    #[tokio::test]
    async fn the_torrents_door_answers_a_window_per_open_reader() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let first = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity install made");
        let second = owner
            .reader_on(&0, (0, 6 * PIECE), Buffering::default())
            .expect("the same entity");
        assert!(
            owner.reader_on(&9, (9, 0), Buffering::default()).is_none(),
            "a reader was opened on a key with no entity"
        );
        assert_eq!(
            owner.readers_of(&0),
            0,
            "a reader that delivered nothing counts"
        );
        assert!(
            first.note((0, 0)).is_none(),
            "the tick is the torrent's trigger"
        );
        backing.read_from(0, 1);
        assert!(second.note((0, 6 * PIECE)).is_none());
        backing.read_from(6 * PIECE, 1);
        assert_eq!(owner.readers_of(&0), 2);
        assert_eq!(owner.readers_of(&1), 0);
        let answers = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let first = parking_lot::Mutex::new(Some(first));
        let hook: Hook<TorrentSide> = Box::new({
            let answers = answers.clone();
            move |door: &Door<Torrent>| {
                let mut answers = answers.lock();
                answers.push(refused(door, 0..8));
                drop(first.lock().take());
                answers.push(refused(door, 0..8));
            }
        });
        *backing.on_reclaim.lock() = Some(hook);
        let claim = owner.turn(&0).await.expect("the turn");
        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        // What the door refuses is what the pass published, and it stands
        // for the length of the pass: a reader ending mid-reclaim does not
        // open its region up, because the promise not to delete was made
        // before the unlinks began.
        let answers = answers.lock();
        assert_eq!(
            answers[0], answers[1],
            "a reader that ended mid-reclaim changed what the door refuses"
        );
        assert!(
            !answers[0].is_empty(),
            "the door refused nothing at all: {:?}",
            answers[0]
        );
        assert_eq!(owner.readers_of(&0), 1);
        drop(second);
        assert_eq!(owner.readers_of(&0), 0);
    }

    /// **The film's length is the bitrate, and nothing has to measure it.**
    ///
    /// Size over duration is arithmetic: exact at the first report, with
    /// nothing to converge and no read pattern to distort it. Three field
    /// rounds went into measuring the same number and produced three bytes
    /// a second and then seventeen.
    #[tokio::test]
    async fn a_stated_duration_is_the_bitrate_without_measuring_anything() {
        let (_backing, owner, budget) = torrent();
        budget.set(Some(6 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let asks = Buffering {
            window_seconds: Some(10),
            committed_seconds: Some(10),
            ..Buffering::default()
        };
        let reader = owner
            .reader_on(&0, (0, 0), asks)
            .expect("the entity the install made");

        // File 0 is eight pieces of a thousand bytes. Eighty seconds of film
        // is a hundred bytes a second, and ten seconds of that is one piece.
        owner.note_duration(&0, std::time::Duration::from_secs(80));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            owner
                .holding(&0)
                .and_then(|holding| holding.installed)
                .map(|installed| installed.shape),
            Some(Shape::Split {
                unshared: 5,
                committed: 1
            }),
            "one report, nothing to measure it against, and the cap binds"
        );
        drop(reader);
    }

    /// **A budget that shrinks under an open reader does not shrink the
    /// window under that reader's lookahead.**
    ///
    /// The stream's lookahead is fixed when the reader opens and the fork
    /// has no setter for it, while the budget is republished every sixty
    /// seconds from the volume's free space. So what a pass asks for and
    /// what it refuses to unlink both have to cover what an open reader was
    /// granted -- the backend will not forget a piece inside a live stream's
    /// lookahead anyway, so a pass that asked would be refused and the disk
    /// would grow by exactly that much, for the life of the stream.
    #[tokio::test]
    async fn a_budget_that_shrinks_under_a_reader_keeps_its_lookahead_inside_the_window() {
        let (backing, owner, budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let _reader = owner
            .reader_on(
                &0,
                (0, 0),
                Buffering {
                    lookahead_bytes: 5 * PIECE,
                    ..Buffering::default()
                },
            )
            .expect("the entity the install made");
        // The volume filled: half the budget, published under the reader.
        budget.set(Some(2 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Resized);

        let claim = owner.turn(&0).await.expect("the turn");
        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        let left = backing.on_disk();
        for piece in 0..5 {
            assert!(
                left.contains(&piece),
                "piece {piece} is inside the lookahead this reader was granted \
                 and the pass took it anyway: {left:?}"
            );
        }
        assert!(
            !left.contains(&6) && !left.contains(&7),
            "and what is outside it still went: {left:?}"
        );
    }

    /// **A lookahead the stream has already fetched is on the disk once.**
    ///
    /// The margin is the room the fill needs before the next pass, and an
    /// open stream's part of that is what its lookahead has still to bring.
    /// Counted as the whole lookahead, every piece of it already fetched
    /// was priced twice -- in what is held, and again as room still owed --
    /// and the disk settled a lookahead short of the cap. Under `Maximum`
    /// a first open's lookahead is the cap itself, so that was an
    /// allowance of nothing: every stream at its floor and all of the
    /// scrub-back reclaimed on every pass.
    #[tokio::test]
    async fn a_fetched_lookahead_is_not_counted_again_as_room_the_fill_needs() {
        // The proxy's shape, so no committed set stands in the way of
        // what the allowance alone decides.
        let backing = Proxy::new([domain(0, 0..16)]);
        backing.holds(0..16);
        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(10 * PIECE), None);
        let owner = Retention::new(backing.clone(), budget.clone());
        let installing = owner.reader(0, domain(0, 0..16));
        let claim = installing
            .note((0, 8 * PIECE))
            .expect("the first byte is due");
        drop(installing);
        backing.seek_to(8 * PIECE);
        // Eight pieces in, reading four ahead, and all of it on the disk.
        let _reader = owner
            .reader_on(
                &0,
                (0, 8 * PIECE),
                Buffering {
                    lookahead_bytes: 4 * PIECE,
                    ..Buffering::default()
                },
            )
            .expect("the entity the first byte installed");
        let now = std::time::Instant::now();
        owner.pass_at(&0, &(), claim, Mode::Live, now).await;
        for _ in 0..3 {
            let claim = owner.turn(&0).await.expect("the turn");
            owner.pass_at(&0, &(), claim, Mode::Live, now).await;
        }
        // The cap less a stride of room: the lookahead the fill still owes
        // is none, so the stride is all the margin there is.
        let left = backing.on_disk();
        assert_eq!(left.len(), 9, "where the disk settled: {left:?}");

        // And what it does still owe is room kept for it: the file back on
        // the disk but for two pieces of the lookahead, so the rest of it
        // makes room for those.
        backing.holds(0..16);
        backing
            .held
            .lock()
            .retain(|piece| *piece != 11 && *piece != 12);
        for _ in 0..3 {
            let claim = owner.turn(&0).await.expect("the turn");
            owner.pass_at(&0, &(), claim, Mode::Live, now).await;
        }
        let left = backing.on_disk();
        assert_eq!(
            left.len(),
            8,
            "the two pieces still to come fit under the cap: {left:?}"
        );
    }

    /// **A probe's window is kept and not fetched.**
    ///
    /// The other half of the same field bug. A live probe never claimed the
    /// entity's head, but it did put a window of its own into the list the
    /// pass hands [`Backing::want`], and that list is an order to the
    /// swarm: a sixteen-megabyte read of the container index at the tail
    /// ordered the whole forward reach of a window round it -- a hundred
    /// and thirty-eight megabytes on the device this was measured on --
    /// which the next pass reclaimed again. So the two lists are split: the
    /// probe is in the one that says "do not delete this" and out of the
    /// one that says "fetch this".
    #[tokio::test]
    async fn a_live_probe_keeps_its_window_without_ordering_it_fetched() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        let playing = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        assert!(playing.note((0, 0)).is_none());
        backing.seek_to(0);
        // mpv's read of the Cues, still open while the tick runs.
        let probe = owner
            .reader_on(&0, (0, 7 * PIECE), Buffering::default())
            .expect("the same entity");
        assert!(probe.note((0, 7 * PIECE)).is_none());
        backing.seek_to(7 * PIECE);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            outcome.windows,
            // One run of disk, so one consumer: what a reader is depends on
            // what is between the reads, and here the whole file is held.
            vec![7..8],
            "the consumer both reads belong to, which is the run they are in"
        );
        assert_eq!(
            *backing.wanted.lock(),
            vec![vec![7..8]],
            "the consumer's own run was ordered fetched, and nothing else"
        );
        assert!(
            backing.on_disk().contains(&7),
            "and the door still refused to unlink the piece the probe is reading"
        );
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            "everything outside what the consumer asked for went, and \
             nothing inside it did"
        );
    }

    /// **A read that has delivered nothing still has a head: where it was
    /// opened.**
    ///
    /// This is the other half of the same tick, and on a cold open it is
    /// the whole of it. The policy is installed before the reader opens
    /// ([`Install::OnOpen`]), so between the open and the first byte -- a
    /// whole piece, tens of seconds on a slow swarm -- the entity has a
    /// policy and no head: without a promise, every pass over it concludes
    /// nothing and nothing trims the want-set, so the swarm fills the disk
    /// with the file in whatever order it likes while the player shows
    /// 0:00 (measured on a television: 180 MB fetched and 54 MB kept
    /// before the first frame).
    ///
    /// What keeps it is the promise, which is what a parked read makes:
    /// `poll_read` returning `Pending` promises the piece it is waiting on,
    /// and a promise is published into the set the door reads. A handle that
    /// has neither promised nor delivered has asked for nothing, and there
    /// is nothing for a pass to keep.
    #[tokio::test]
    async fn a_read_parked_on_its_first_piece_keeps_what_it_promised() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        let reader = owner
            .reader_on(&0, (0, 4 * PIECE), Buffering::default())
            .expect("the entity the install made");
        assert_eq!(
            owner.readers_of(&0),
            0,
            "nothing has been observed of it: it has neither promised nor delivered"
        );
        // And then it parks, which is what a read does when the piece it
        // wants is not there.
        reader.promises(4..6);
        assert_eq!(
            owner.holding(&0).expect("a holding").last_position,
            None,
            "no byte has gone out"
        );

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass over an entity a reader is parked in");
        assert_eq!(
            outcome.windows,
            Vec::<Range<u32>>::new(),
            "a read that has delivered nothing is not a consumer: nothing is \
             being fetched ahead of it"
        );
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15],
            "and the pieces it is waiting for stayed, beside the two this file shares"
        );
        drop(reader);
    }

    /// A sixteen-piece film under a budget of eight, its viewer reading at
    /// the top: the viewer's window is 0..3, the disk may hold seven pieces
    /// -- the budget less a stride of room -- and the play session's draw
    /// is pieces 0, 1, 5 and 9, which the tests over this keep off the
    /// disk past the first two so that what is held by nothing is plain.
    async fn film_with_a_viewer_at_the_top() -> (
        Arc<Torrent>,
        Arc<Retention<Torrent>>,
        Arc<RetentionBudget>,
        Reader<Torrent>,
    ) {
        let backing = Torrent::new([domain(0, 0..16)]);
        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(8 * PIECE), None);
        let owner = Retention::new(backing.clone(), budget.clone());
        assert_eq!(play(&owner, &backing, 0).await, InstallOutcome::Installed);
        let viewer = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the play made");
        assert!(viewer.note((0, 0)).is_none());
        (backing, owner, budget, viewer)
    }

    /// A live pass over the film at `now`, its viewer having just read a
    /// byte at the top.
    async fn pass_with_the_viewer_reading(
        backing: &Torrent,
        owner: &Arc<Retention<Torrent>>,
        now: Instant,
    ) {
        backing.note_read(1, 0, 1, now);
        let claim = owner.turn(&0).await.expect("the turn");
        owner
            .pass_at(&0, &(), claim, Mode::Live, now)
            .await
            .concluded
            .expect("a pass over a film being watched");
    }

    /// **What an asker fetched is cached data like any other once it has
    /// moved on: held by nothing, and given back in the one order
    /// everything is given back in.** A pre-want's pieces, in the scenario
    /// they are for -- a viewer at the top of the film, an asker fetching
    /// further in.
    ///
    /// While the asker is asking for a piece, no reclaim is offered it: the
    /// backend will not forget what a stream is asking for. The moment it
    /// asks for something else, what it left is ranked by when it arrived,
    /// exactly as a piece that was read is ranked by when it was read: not
    /// first to go -- data that arrived before it goes first -- and not
    /// last -- data that arrived after it outlives it. The viewer's window
    /// and the draw are never among what goes, and the disk is never left
    /// over what it may hold.
    #[tokio::test]
    async fn what_an_asker_fetched_is_cached_data_like_any_other_once_it_has_moved_on() {
        let (backing, owner, _budget, _viewer) = film_with_a_viewer_at_the_top().await;
        let t0 = Instant::now();
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        // The viewer's three pieces, and one that has been lying on the
        // disk since before anything here: never read, held by nothing.
        *backing.held.lock() = [0, 1, 2, 6].into_iter().collect();
        pass_with_the_viewer_reading(&backing, &owner, at(0)).await;

        let asker = owner.ask_ahead(&0).expect("the film's entity");
        assert!(owner.ask_ahead(&7).is_none(), "no such entity");
        asker.asks(10..12);
        let holding = owner.holding(&0).expect("a holding");
        assert_eq!(holding.asking, vec![10..12]);
        assert!(holding.promised.is_empty(), "an asker promises nothing");
        assert_eq!(holding.head, Some((0, 0)), "and is nobody's head");
        assert_eq!(owner.readers_of(&0), 1, "nor a reader of the film");

        // Its pieces arrive, and it moves on to the next two.
        backing.holds([10, 11]);
        pass_with_the_viewer_reading(&backing, &owner, at(10)).await;
        asker.asks(12..14);
        backing.holds([12, 13]);
        // Eight on a disk that may hold seven: one has to go, and it is
        // the oldest thing nothing holds -- the piece that was there first,
        // not the asker's, which arrived after it.
        pass_with_the_viewer_reading(&backing, &owner, at(20)).await;
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 10, 11, 12, 13],
            "older cached data went before what the asker fetched"
        );

        // On again -- back, this time, as a walk outward goes -- and
        // something else arrives with its piece. Nine on the disk: two have
        // to go, and now the oldest thing nothing holds is what the asker
        // fetched first.
        asker.asks(7..8);
        backing.holds([7, 14]);
        pass_with_the_viewer_reading(&backing, &owner, at(30)).await;
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 7, 12, 13, 14],
            "the asker's own first pieces went, held by nothing; what arrived \
             after them stayed, and so did everything the viewer holds"
        );

        // And what it is asking for is not offered while it asks. Three
        // more arrive, so three have to go: the asker's second pair, and
        // then, of the two that came together, not the one its stream is
        // on -- the first in the file, which is how a tie goes -- but the
        // other.
        backing.holds([6, 8, 15]);
        pass_with_the_viewer_reading(&backing, &owner, at(40)).await;
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 6, 7, 8, 15],
            "a piece a stream is asking for was offered to the reclaim"
        );
        // Let go, that piece is the oldest of what is left, and the first
        // to go.
        drop(asker);
        assert!(owner.holding(&0).expect("a holding").asking.is_empty());
        backing.holds([10, 11]);
        pass_with_the_viewer_reading(&backing, &owner, at(50)).await;
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 8, 10, 11, 15],
            "the piece it was asking for went once it was not"
        );
    }

    /// **What an asker has yet to receive is room the disk keeps for it,
    /// and it is never a reader's piece that makes that room**: a reach
    /// that has not arrived is fill to come, as a reading stream's
    /// lookahead is, so the pass makes the room before the pieces land and
    /// the disk is not over its line when they do -- and what goes for it
    /// is what nothing holds, never the viewer's window nor what a parked
    /// read was promised, however old.
    #[tokio::test]
    async fn what_an_asker_is_still_to_receive_is_room_kept_for_it() {
        let (backing, owner, _budget, _viewer) = film_with_a_viewer_at_the_top().await;
        let t0 = Instant::now();
        // Seven on the disk, which is what it may hold; a second read is
        // parked on the oldest of them outside the viewer's window.
        *backing.held.lock() = [0, 1, 2, 4, 6, 7, 8].into_iter().collect();
        let parked = owner
            .reader_on(&0, (0, 4 * PIECE), Buffering::default())
            .expect("the same entity");
        parked.promises(4..5);
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert_eq!(backing.on_disk().len(), 7);

        let asker = owner.ask_ahead(&0).expect("the film's entity");
        asker.asks(10..13);
        pass_with_the_viewer_reading(&backing, &owner, t0 + Duration::from_secs(10)).await;
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 4, 8],
            "three pieces are on their way: the disk made room for three, not \
             for a stride, out of what no reader holds"
        );
        backing.holds(10..13);
        assert_eq!(backing.on_disk().len(), 8, "arrived, and inside the budget");
        drop(parked);
    }

    /// **An asker may ask for what could be kept beside everything the
    /// readers hold, and no more** ([`Room`]): what the entity may hold
    /// less the viewer's window, the draw, and every promise and lookahead
    /// of an open read -- measured by each pass, and by nothing else.
    #[tokio::test]
    async fn an_askers_room_is_what_the_readers_do_not_hold() {
        let (backing, owner, budget, viewer) = film_with_a_viewer_at_the_top().await;
        let t0 = Instant::now();
        // The viewer's three, and a piece of the draw further in.
        *backing.held.lock() = [0, 1, 2, 5].into_iter().collect();
        let asker = owner.ask_ahead(&0).expect("the film's entity");
        assert_eq!(
            asker.room(),
            Room::Unmeasured,
            "a policy bounds the film and no pass has measured it"
        );
        // Twice: the first pass is the one that commits what of the draw
        // is on the disk, and the second measures with it committed.
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert_eq!(
            asker.room(),
            Room::Pieces(3),
            "seven the disk may hold; three are the viewer's and one more is committed"
        );
        // What the asker itself is asking for takes nothing from its room:
        // the room is for it.
        asker.asks(12..14);
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert_eq!(asker.room(), Room::Pieces(3));

        // A second read parks further in, promising two pieces: all but
        // one of the rest is spoken for.
        let seeking = owner
            .reader_on(&0, (0, 8 * PIECE), Buffering::default())
            .expect("the same entity");
        seeking.promises(8..10);
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert_eq!(asker.room(), Room::Pieces(1));
        // And a viewer whose own stream reaches further leaves none: what
        // eviction cannot give, an asker may not ask for.
        drop(viewer);
        let _viewer = owner
            .reader_on(
                &0,
                (0, 0),
                Buffering {
                    lookahead_bytes: 4 * PIECE,
                    ..Buffering::default()
                },
            )
            .expect("the same entity");
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert_eq!(asker.room(), Room::Pieces(0));
        drop(seeking);

        // A new budget is a new policy: what was measured under the old one
        // says nothing until a pass has measured this one.
        budget.set(Some(10 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Resized);
        assert_eq!(asker.room(), Room::Unmeasured);
        pass_with_the_viewer_reading(&backing, &owner, t0).await;
        assert!(matches!(asker.room(), Room::Pieces(_)));
        // And a budget that covers the film bounds nothing.
        budget.set(Some(16 * PIECE), None);
        owner.install(0, 0).await;
        assert!(owner.holding(&0).expect("a holding").installed.is_none());
        assert_eq!(asker.room(), Room::Unbounded);
    }

    /// **And a parked read is one of the windows, not only the pass's
    /// own.** The seek: the response the player has just opened is waiting
    /// for its first piece while the one it is abandoning still delivers,
    /// so the entity's head is the old response's and the new one is among
    /// the others. A window per head, and the parked head is a head.
    #[tokio::test]
    async fn a_parked_read_beside_a_delivering_one_gets_a_window_of_its_own() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let seeking = owner
            .reader_on(&0, (0, 6 * PIECE), Buffering::default())
            .expect("the entity the install made");
        let playing = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the same entity");
        assert!(playing.note((0, 0)).is_none());
        backing.read_from(0, 1);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            outcome.windows,
            vec![0..3],
            "the delivering read is the consumer; the parked one has \
             promised what it is waiting for, which is not a window"
        );
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 6, 8, 9, 10, 11, 12, 13, 14, 15],
            "the piece the seek promised was not taken from under it, and \
             what the delivering read is reading ahead over stayed"
        );
        drop(seeking);
    }

    /// **The paused film, which is why a closed playback read leaves its
    /// position behind.** Nothing is open, nothing is delivering, and the
    /// window still belongs where the viewer stopped -- so the entity's
    /// head falls back to the last playback byte and a pass keeps drawing
    /// the same window over it, tick after tick.
    #[tokio::test]
    async fn a_paused_film_with_no_reader_open_keeps_the_window_where_it_stopped() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        {
            let playing = owner
                .reader_on(&0, (0, 0), Buffering::default())
                .expect("the entity the install made");
            assert!(playing.note((0, 5 * PIECE)).is_none());
            backing.seek_to(5 * PIECE);
        }
        assert_eq!(owner.readers_of(&0), 0, "the response closed");
        // And a probe runs beside the paused film, as a player's next
        // container read does. It does not take the window off it.
        {
            let probe = owner
                .reader_on(&0, (0, 0), Buffering::default())
                .expect("the same entity");
            assert!(probe.note((0, 0)).is_none());
            backing.seek_to(0);
        }

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            outcome.windows,
            vec![0..3],
            "a closed playback read outranks a probe that has been and gone, \
             and the header piece is the container's either way"
        );
        assert!(
            !backing.on_disk().contains(&5) && !backing.on_disk().contains(&6),
            "the film was paused at the head and the probe has gone, so \
             nothing is asking for the middle of the file: it is scrub-back, \
             and this budget has no room for it"
        );
    }

    /// **A film's length is not a playhead and does not go stale with one.**
    ///
    /// It is what a cast can state and nothing else: the receiver does the
    /// reading and reports seconds, which do not convert to a byte offset
    /// without a constant bitrate. The length *is* the bitrate, so the
    /// window is sized exactly even though where it sits is left to the
    /// reads -- and it keeps sizing it when the viewer stops reporting a
    /// position, because a film does not get shorter while nobody is
    /// looking.
    #[tokio::test]
    async fn a_films_length_sizes_the_window_with_no_playhead_at_all() {
        let (_backing, owner, budget) = torrent();
        budget.set(Some(6 * PIECE), None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let reader = owner
            .reader_on(
                &0,
                (0, 0),
                Buffering {
                    window_seconds: Some(10),
                    committed_seconds: Some(10),
                    ..Buffering::default()
                },
            )
            .expect("the entity the install made");

        // Eight pieces of a thousand bytes over eighty seconds: a hundred
        // bytes a second, and ten seconds of it is one piece. No position
        // was ever reported.
        owner.note_duration(&0, std::time::Duration::from_secs(80));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            owner
                .holding(&0)
                .and_then(|holding| holding.installed)
                .map(|installed| installed.shape),
            Some(Shape::Split {
                unshared: 5,
                committed: 1
            }),
            "the cap binds on a length alone"
        );
        drop(reader);
    }

    /// **While a read is parked, the want-set is a small window round what
    /// it is waiting for** -- not the configured one, and not one piece.
    ///
    /// Not the configured one, because a swarm spread over two hundred
    /// pieces takes its time over the three a first frame needs. Not one
    /// piece, because librqbit reserves a piece to exactly one peer, so a
    /// want-set of one is a download from one: 117 kB/s in the field where
    /// the same swarm gave 3.1 MB/s a minute later.
    #[tokio::test]
    async fn a_parked_read_gets_a_small_window_round_what_it_waits_for() {
        let (backing, owner, _budget) = torrent();
        // A disk without piece 6, so the promise below is unmet.
        backing.held.lock().remove(&6);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let reader = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        reader.promises(6..7);

        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            *backing.wanted.lock(),
            vec![vec![6..23]],
            "the piece the read is stuck on and a short run after it -- \
             enough peers busy to matter, and not the whole configured \
             window competing with it"
        );
        drop(reader);
    }

    /// **And the configured window is wanted again the moment it arrives.**
    ///
    /// The narrowing ends by itself, which is why it can be as blunt as it
    /// is: a promise is made by a parked read and cleared by the byte that
    /// unparks it, so nothing has to decide when start-up is over.
    #[tokio::test]
    async fn a_promise_the_disk_can_meet_leaves_the_windows_wanted() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let reader = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        // `torrent()` holds every piece, so this promise is already met.
        reader.promises(6..7);

        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            *backing.wanted.lock(),
            vec![Vec::<Range<u32>>::new()],
            "nothing is waiting and nothing has read: there is no consumer \
             to order anything for"
        );
        drop(reader);
    }

    /// **An entity nothing has ever played is still bounded.** A download,
    /// or a probe, on a file no viewer has opened is a live read like any
    /// other -- and an entity with no head is one no pass measures, so
    /// nothing would trim its want-set or take anything off its disk.
    #[tokio::test]
    async fn a_file_only_a_probe_is_reading_is_bounded_round_the_probe() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let probe = owner
            .reader_on(&0, (0, 3 * PIECE), Buffering::default())
            .expect("the entity the install made");
        assert!(probe.note((0, 3 * PIECE)).is_none());
        backing.read_from(3 * PIECE, 1);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(outcome.windows, vec![3..6]);
        assert!(
            backing.on_disk().contains(&3),
            "the piece the probe is reading out of"
        );
        drop(probe);
    }

    /// **A read that opens while the reclaim is running is a window at the
    /// door, from its open and not from its first byte.**
    ///
    /// The door is asked at every unlink because a read can open between
    /// the decision and the unlink -- a pass over a 4 MiB piece on a slow
    /// disk is not instant, and mpv opens its read of the container index
    /// while the film is already streaming. That read has delivered
    /// nothing, so a door that asks for a *playhead* does not see it at
    /// all, cuts no run round it, refuses no index of it, and unlinks the
    /// piece it is parked on waiting for. Asking for its head instead is
    /// the whole of the difference, and it is the reason both readings in
    /// [`Door`] say `head()`.
    ///
    /// A probe deliberately, and not a second player: a parked *playback*
    /// read is the entity's head as well ([`State::playing_head`]), so the
    /// door's first window would cover it however the readers were asked
    /// about. Nothing but the per-reader reading protects a parked probe.
    #[tokio::test]
    async fn a_read_that_opens_under_the_reclaim_is_a_window_at_the_door() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let playing = owner
            .reader_on(&0, (0, 0), Buffering::default())
            .expect("the entity the install made");
        assert!(
            playing.note((0, 0)).is_none(),
            "the tick is the torrent's trigger"
        );
        backing.read_from(0, 1);

        // mpv's read of the Cues, opened after the pass had decided what to
        // take and while it is taking it. It is parked on the last piece of
        // the file: it has promised nothing and delivered nothing.
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let held_open = Arc::new(parking_lot::Mutex::new(None));
        let hook: Hook<TorrentSide> = Box::new({
            let (owner, seen, held_open) = (owner.clone(), seen.clone(), held_open.clone());
            move |door: &Door<Torrent>| {
                *held_open.lock() = Some(
                    owner
                        .reader_on(&0, (0, 6 * PIECE), Buffering::default())
                        .expect("the same entity"),
                );
                seen.lock().push(refused(door, 0..8));
            }
        });
        *backing.on_reclaim.lock() = Some(hook);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert!(
            !outcome.windows.is_empty(),
            "the pass asked for nothing at all: {:?}",
            outcome.windows
        );
        // The read that opened mid-reclaim is not in what the pass
        // published -- it did not exist when the pass published it -- and
        // the pieces it is parked on are refused only once it promises
        // them, which is what a parked read does.
        assert!(
            !seen.lock()[0].is_empty(),
            "the door refused nothing the pass had published"
        );
        assert_eq!(
            backing.on_disk(),
            vec![0, 1, 2, 8, 9, 10, 11, 12, 13, 14, 15],
            "what the consumer is reading ahead over stayed; the read that \
             opened mid-reclaim had promised nothing"
        );
        drop(held_open.lock().take());
    }

    /// **The test hook runs twice per pass, with the turn held and no owner
    /// lock**: a note inside it finds the pass running, and a holding can
    /// be read.
    #[tokio::test]
    async fn the_hook_runs_before_the_listing_and_before_the_reclaim_with_no_owner_lock() {
        let (backing, owner, _budget) = proxy();
        let reader = Arc::new(owner.reader(0, domain(0, 0..8)));
        let fired = Arc::new(AtomicU64::new(0));
        owner.hook({
            let owner = owner.clone();
            let reader = reader.clone();
            let fired = fired.clone();
            let inside = backing.clone();
            move || {
                fired.fetch_add(1, Ordering::SeqCst);
                assert!(
                    reader.note((0, 2 * PIECE)).is_none(),
                    "a note inside the hook found the turn free"
                );
                inside.seek_to(2 * PIECE);
                assert!(owner.holding(&0).is_some());
            }
        });
        let claim = reader.note((0, 0)).expect("due");
        backing.seek_to(0);
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(fired.load(Ordering::SeqCst), 2);
        // The hook's byte landed before the re-read, so this pass measured
        // from it and owes nothing for it.
        assert_eq!(
            outcome.concluded.expect("a pass").windows,
            vec![2..5],
            "a byte delivered before the re-read was not measured"
        );
        assert!(outcome.again.is_none());
    }

    /// **An install parked inside `advertise` under the turn blocks
    /// nothing that must not wait on it**: a note, a note_position, a pin
    /// and a holding all land while the backend is being asked, which is
    /// the module doc's witness for deadlock-freedom, at the owner.
    #[tokio::test]
    async fn a_pass_parked_inside_advertise_blocks_no_note_pin_or_holding() {
        let (backing, owner, _budget) = torrent();
        let (entered, release) = backing.park_advertise();
        // The viewer's open, with the film's length known: its draw is
        // decided and advertised at the open.
        *backing.playing.lock() = Some(0);
        owner.entity(0, domain(0, 0..8));
        owner.note_duration(&0, FILM_LENGTH);
        let install = {
            let owner = owner.clone();
            tokio::spawn(async move {
                owner
                    .install_opening(0, 0, Buffering::default(), Opener::Player)
                    .await
            })
        };
        entered.await.expect("the install to reach its advertise");
        assert!(
            owner.try_turn(&0).is_none(),
            "the turn was free during the advertise"
        );
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(reader.note((0, PIECE)).is_none());
        backing.read_from(PIECE, 1);
        owner.note_position(&0, (0, 2 * PIECE));
        backing.keeps_everything.store(true, Ordering::SeqCst);
        let holding = owner.holding(&0).expect("the entity");
        assert_eq!(holding.last_position, Some((0, 2 * PIECE)));
        assert!(
            holding.installed.is_some(),
            "the policy goes in before the draw it adopts is advertised"
        );
        release.send(()).expect("the parked install");
        assert_eq!(install.await.expect("joined"), InstallOutcome::Installed);
        assert!(owner.holding(&0).unwrap().installed.is_some());
    }

    /// **A cleared entity is decided afresh on its next delivered byte.**
    ///
    /// `clear` forgets the policy; had it left `decided` standing, every
    /// later note would find it equal to the published budget, skip the
    /// decide, and the entity would be unbounded until the budget's *value*
    /// changed -- the hole this module exists to close, reopened by its own
    /// clear.
    #[tokio::test]
    async fn a_cleared_entity_is_decided_afresh_on_the_next_delivered_byte() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        drop(claim);
        owner.clear(&0).await;
        let cleared = owner.holding(&0).unwrap();
        assert!(cleared.installed.is_none());
        assert_eq!(
            cleared.decided, None,
            "a cleared entity still says it was decided"
        );
        let claim = reader
            .note((0, PIECE))
            .expect("the byte after a clear is due under a fresh decision");
        backing.read_from(PIECE, 1);
        assert!(owner.holding(&0).unwrap().installed.is_some());
        drop(claim);
    }

    /// **A pass overtaken at its reclaim leaves the reader due.**
    ///
    /// The byte that replaced the policy under the pass reset `passed_at`
    /// so every reader is due under the new shape; the pass's conclusion
    /// must not write the old measurement back over that, or a reader that
    /// has not travelled a whole *new* stride is never due and the new
    /// budget is never measured.
    #[tokio::test]
    async fn a_pass_overtaken_at_its_reclaim_leaves_the_reader_due() {
        let (backing, owner, budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 5 * PIECE)).expect("due");
        backing.read_from(5 * PIECE, 1);
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the reclaim");
        budget.set(Some(2 * PIECE), None);
        assert!(reader.note((0, 5 * PIECE)).is_none());
        backing.read_from(5 * PIECE, 1);
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        // The pass hands its claim on (the replaced policy is owed a pass);
        // dropped unused here, the reader itself must still be due on the
        // byte it delivered under the new budget.
        drop(outcome);
        assert!(
            reader.note((0, 5 * PIECE)).is_some(),
            "the overtaken pass wrote its measurement over a reader the new budget made due"
        );
        backing.read_from(5 * PIECE, 1);
    }

    /// The pass future and the door can be sent to another thread, which
    /// is what the proxy's driver does with the one and the proxy's
    /// blocking closure with the other.
    /// **One allowance for every backing.** The torrent, the proxy and the
    /// scenario fake each carried their own copy of this arithmetic, and
    /// the scenarios tested the fake's. What this entity may hold is the
    /// smaller of the configured cap and what it holds plus the volume's
    /// headroom, less the margin the fill needs; an unknown budget over an
    /// unread volume is nothing.
    #[test]
    fn the_allowance_is_the_smaller_of_cap_and_volume_less_the_margin() {
        use crate::retention::CacheBudget;
        let asking = |budget, headroom, margin| super::Asking {
            budget,
            headroom,
            ceiling: None,
            seconds: 90,
            holding: Vec::new(),
            committed: Vec::new(),
            margin,
            now: std::time::Instant::now(),
        };
        assert_eq!(
            asking(CacheBudget::Unbounded, None, 5).allowance(1, 100),
            u64::MAX - 5
        );
        assert_eq!(
            asking(CacheBudget::Bytes(1_000), Some(200), 0).allowance(1, 100),
            300,
            "the volume binds"
        );
        assert_eq!(
            asking(CacheBudget::Bytes(250), Some(200), 0).allowance(1, 100),
            250,
            "the cap binds"
        );
        assert_eq!(
            asking(CacheBudget::Unknown, Some(200), 50).allowance(1, 100),
            250,
            "no cap stated: what is held plus the headroom, less the margin"
        );
        assert_eq!(
            asking(CacheBudget::Bytes(250), None, 0).allowance(1, 100),
            250
        );
        assert_eq!(
            asking(CacheBudget::Unknown, None, 0).allowance(1, 100),
            0,
            "nothing stated and nothing read is not a licence"
        );
        // Ten pieces of a thousand bytes held, three of them committed,
        // under a cap of six pieces: the window may be three.
        let mut committed = asking(CacheBudget::Bytes(6_000), None, 0);
        committed.committed = vec![0..2, 5..6];
        assert_eq!(
            committed.allowance(1_000, 10),
            3_000,
            "the committed set is on the disk whatever the consumers ask for"
        );
        assert_eq!(
            committed.overhang(1_000, 10),
            4_000,
            "and it is part of what is held: the disk gives back what it \
             holds over the cap, not over the windows' share of it"
        );
    }

    /// **The head a reading splits at is where the entity is consumed,
    /// not the newest reader's.**
    ///
    /// mpv keeps an index crawler open beside the viewer and reopens it
    /// about once a second, so the newest reader with a head is regularly
    /// one parked at the tail. The detector tells them apart by what they
    /// eat, and the holding reports its answer.
    #[tokio::test]
    async fn the_holding_head_is_where_the_entity_is_consumed_not_the_newest_readers() {
        let (backing, owner, _budget) = torrent();
        // The field's disk: the viewer's run at the head of the file and
        // the tail piece the crawler reads, nothing between. (A fully
        // cached film joins the two reads into one stream by the
        // detector's same-run rule; see "What a stream is" in
        // `docs/design/read-pattern-retention.md`.)
        *backing.held.lock() = [0u32, 1, 2, 3, 7].into_iter().chain(8..16).collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        // The viewer, three pieces into the file and eating.
        let viewer = owner.reader(0, domain(0, 0..8));
        viewer.note((0, 2 * PIECE));
        backing.read_from(0, 3 * PIECE);
        // The crawler, opened after it and parked at the tail, with one
        // small read to its name.
        let crawler = owner.reader(0, domain(0, 0..8));
        crawler.note((0, 7 * PIECE));
        backing.read_from(7 * PIECE, 1);
        // The torrent passes on its tick.
        let claim = owner.turn(&0).await.expect("the turn");

        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        let head = owner.holding(&0).unwrap().head.expect("a head");
        assert_eq!(
            Torrent::index_of(&domain(0, 0..8), head),
            Some(3),
            "the split landed on the crawler at the tail, not on the viewer"
        );
    }

    /// **A promise made while the backing is publishing is held after
    /// it.**
    ///
    /// The backing publishes what may not be unlinked with no owner lock
    /// held, and a read can park and promise a piece between the pass's
    /// reading of the promises and that publication; the publication is a
    /// whole-word store and overwrites the promise's bit. Step 5 re-holds
    /// every live promise under the entity's lock, and that is the only
    /// thing that keeps a chunk out of a body a player has been promised
    /// the length of.
    #[tokio::test]
    async fn a_promise_made_under_the_publication_is_re_held_after_it() {
        let (backing, owner, _budget) = proxy();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        backing.read_from(0, 1);
        // A second read parks on piece 6 while the backing is inside its
        // reading, after the pass took its snapshot of the promises.
        let late = owner.reader(0, domain(0, 0..8));
        *backing.on_reading.lock() = Some(Box::new(move || late.promises(6..7)));

        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        let exempt = owner
            .entity(0, domain(0, 0..8))
            .state
            .lock()
            .exempt
            .clone()
            .expect("the pass published");
        assert!(
            exempt.holds(6),
            "the promise made under the publication was overwritten by it"
        );
    }

    /// **The pass runs on the clock it is handed.**
    ///
    /// The scenario harness stamps its reads from its own clock, `t0`
    /// plus the beat's offset, and ran the pass on the wall clock beside
    /// them: to the detector no time ever passed, so no stream in any
    /// scenario ever idled and no report was ever due. A pass handed
    /// `t0 + 60 s` after a read at `t0` finds that stream gone.
    #[tokio::test]
    async fn the_pass_measures_against_the_clock_it_is_handed() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let t0 = std::time::Instant::now();
        owner.note_position(&0, (0, 0));
        backing.note_read(1, 0, PIECE, t0);

        let claim = owner.turn(&0).await.expect("the turn");
        let concluded = owner
            .pass_at(
                &0,
                &(),
                claim,
                Mode::Live,
                t0 + std::time::Duration::from_secs(60),
            )
            .await
            .concluded
            .expect("a pass");
        assert!(
            concluded.windows.is_empty(),
            "a stream last read a minute ago on the pass's clock was still granted a \
             window: {:?}",
            concluded.windows
        );
    }

    /// **A slack pass that finds the entity live hands no claim on.**
    ///
    /// Under [`Trigger::OnMove`] the ordinary release answers "again"
    /// whenever a policy stands and a head is in the domain, but an early
    /// exit of the slack pass must not: the driver never reads a slack
    /// pass's claim back, so answering `Some` here would hand on a claim
    /// nobody takes, and a driver that looped on it would spin.
    #[tokio::test]
    async fn a_slack_pass_that_finds_the_entity_live_hands_no_claim_on() {
        let (backing, owner, _budget) = proxy();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let opens = owner.opens_of(&0);
        backing.is_live.store(true, Ordering::SeqCst);

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(outcome.concluded.is_none(), "nothing was concluded");
        assert!(
            outcome.again.is_none(),
            "a slack pass that took nothing handed its claim on"
        );
    }

    #[test]
    fn the_pass_and_the_door_are_send() {
        fn pass<'a, B: Backing>(
            owner: &'a Retention<B>,
            key: &'a B::Key,
            store: &'a B::Store,
            claim: Claim,
        ) -> impl Future<Output = Outcome> + Send + 'a {
            owner.pass(key, store, claim, Mode::Live)
        }
        fn send<T: Send + 'static>() {}
        send::<Door<Proxy>>();
        send::<Reader<Proxy>>();
        let _ = pass::<Proxy>;
    }
}
