//! One owner for retention: the policy never leaves its cell, and one party
//! at a time changes the world it describes.
//!
//! Both retention drivers -- `Engine::retain` over a torrent's piece store
//! and `server::proxy_retention` over a proxied body's chunk store -- run the
//! same arithmetic ([`RetentionPolicy`]) and validate a different number of
//! times, and four review rounds found the same defect in each: **a value
//! read at one moment, trusted at another**. The policy itself was the worst
//! case. Both sides `take()` it out of its slot for the length of a pass and
//! put it back afterwards, so a pass that dies at an await -- the runtime
//! shutting down, the blocking pool refusing a task, a panic in the unlink
//! closure -- walks off with the policy, and the entity is unbounded until
//! the budget's *value* changes. Both sides then grew shadows of the policy
//! beside the empty slot (`bounds`, `bounded`, `windows`-as-snapshot) so a
//! panel or the cleaner asking mid-pass would not be told "nothing bounds
//! this stream", and the torrent's clear left a range held back beside an
//! empty slot when the backend refused to re-advertise it -- held back and
//! read as announced, the one combination that is never right.
//!
//! Here the policy is resident in [`State::installed`], behind a lock that
//! is never held across an await, and it is never taken out: a pass advances
//! it in place. What a pass holds instead is the entity's **turn**, a tokio
//! mutex over a zero-sized [`Turn`] token -- `Engine::announce` made per
//! entity and given to the proxy too. Whoever holds the turn is the one party
//! installing, clearing, passing or cleaner-deleting on that entity, and the
//! turn *is* held across that party's I/O, because that is what keeps
//! "nothing becomes announced between the decision and the unlink" true.
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
//!
//! 1. L1 → L2 only, and only inside [`Retention::holdings_at`] and
//!    [`Retention::forget_empty`], which take L1 and read each entity's L2
//!    under it; never L2 → L1. An entity holds its own `Arc` and never
//!    reaches the map. (Every other reader of the map --
//!    [`Retention::readers`], `lookup`, `entity` -- copies the `Arc`s out
//!    and releases L1 before touching any L2.)
//! 2. No L1 or L2 guard is ever live across an await, a tracing macro, a
//!    `&self` method of [`Backing`] or any X. The pure associated functions
//!    of [`Backing`] -- `governs`, `extent`, `policy`, `index_of` -- ARE
//!    called under L2 (`policy` from [`Reader::note`]'s decide, `index_of`
//!    from the [`Door`] and every head reading) and must take no lock and do
//!    no I/O. [`Backing::policy`] returns its `Err` as a value and the
//!    caller logs after unlock (today's one exception, the proxy's `decide`
//!    logging under its map lock, is gone). [`Backing::keeps_everything`]
//!    (`pinned_files.read()`) is asked before L2 is taken, in the [`Door`]
//!    and in the pass.
//! 3. T is awaited (`lock().await`) only with NO owner lock held:
//!    [`Retention::pass`]'s callers, [`Retention::install`],
//!    [`Retention::clear`] and the cleaner's delete take it first. T → L2
//!    briefly is allowed. `try_lock` on T IS allowed under L2 -- deliberately:
//!    "is a pass running" and "is this byte due" must be one reading (today
//!    `running` and `moved` are read under one map lock), and **every exit
//!    of the pass** must decide `again` and either hand the claim on or drop
//!    it INSIDE its L2 block (today `running = false` and
//!    `arms_another_pass` run under one lock). Releasing L2 before the
//!    `try_lock`, or the claim after L2, reopens the swallowed-last-byte
//!    hole: a byte delivered between the conclusion and the release finds a
//!    pass "running", starts none, and nothing remembers that it wanted
//!    one. The one `try_lock` under L2 is [`Reader::note`]'s, on an entity
//!    it already holds; [`Retention::try_turn`] looks its entity up under
//!    L1 and is for callers holding nothing.
//! 4. T is per entity and never nested: every party takes one turn, does
//!    its work and releases it before taking another, and nothing here
//!    touches two entities in one act. [`Retention::install`] used to,
//!    retiring every sibling under an ordering lock of its own; both are
//!    gone with the value that replaced them (see
//!    [`crate::retention::live`]).
//! 5. Writes to `installed`, `windows`, `stride` and the in-place advance of
//!    the policy require `&mut Turn`, so "written only under the turn" is a
//!    type -- with one documented exception: [`State::install_now`]
//!    ([`Install::OnDeliveredByte`]) writes `installed`, `decided` and
//!    `stride` and resets every reader's `passed_at` under L2 alone from
//!    [`Reader::note`] while a pass may hold T (the writes today's `decide`
//!    makes). Legal only because [`Share::Nothing`] holds nothing back (no
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
//! `advertise_gate` tests exercise: a pass parked inside
//! `set_pieces_advertised` under T while a note and a pin land.
//!
//! # The pass
//!
//! Two passes, and which one runs is the driver's, from one reading of what
//! is being played ([`Mode`]). [`Mode::Slack`] is the short one, written
//! out on [`Retention::slack_pass`]: re-establish that it really is slack
//! under the turn, hold the whole extent back, drop the policy and the
//! windows, take every byte off the disk, forget the entity if nothing is
//! left. The re-establishing is the first step and not a formality -- the
//! driver's reading is older than the turn by every unlink it has done
//! since, and this is the pass that deletes an entity whole. What follows
//! is [`Mode::Live`].
//!
//! One body with two entries. [`Retention::turn`] queues on T for the
//! torrent's tick and the cleaner's delete; [`Reader::note`] claims T with a
//! `try_lock` when a delivered byte moved a stride, which is the proxy's
//! trigger. Both hand a [`Claim`] to [`Retention::pass`], whose steps are:
//!
//! 1. [`Backing::keeps_everything`] → clear, conclude nothing.
//! 2. Snapshot heads and promises under L2.
//! 3. Test hook.
//! 4. The listing ([`Backing::held`]), with no owner lock held.
//! 5. RE-READ under L2, and REFUSE if the resident policy's budget or
//!    domain differs from the snapshot -- a decide ran under us. Otherwise
//!    advance the resident policy in place and build the windows.
//! 6. Re-issue the hold-back if [`Backing::epoch`] says the backend threw
//!    away the record that carried it; advertise committed runs, withdraw
//!    lost runs, no L2 held (all three empty by construction under
//!    [`Share::Nothing`]); then
//!    [`Backing::want`], which trims what the backend fetches to the windows
//!    and what is on the disk, asking the same [`Door`] the reclaim asks
//!    before it unlinks anything that arrived under the pass.
//! 7. Test hook.
//! 8. [`Backing::reclaim`] with a [`Door`] that answers both
//!    [`Door::window_now`] and [`Door::refuses`] from one reading of L2.
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
//! # Departures from the design
//!
//! Where this file differs from the approved synthesis, on purpose:
//!
//! * The design's `try_turn` comment says "never waits; legal under L2". It
//!   looks its entity up under L1, so it is NOT legal under L2 (L2 → L1
//!   inverts rule 1); the `try_lock` rule 3 allows under L2 is
//!   [`Reader::note`]'s on an entity it already holds.
//! * `again` is decided on every exit of the pass, not only at step 9, and a
//!   pass refused at its re-read (or overtaken during its reclaim) hands the
//!   claim on outright rather than asking the stride rule of a head it never
//!   measured. The design's step 9 alone reopened the swallowed-last-byte
//!   hole for a last byte delivered under a budget change; see
//!   [`Retention::pass`].
//! * `passed_at` is written only when the windows are (budget and domain
//!   unchanged). Today's `finish` writes it regardless, which undoes the
//!   "every reader is due again" its own `decide` promises.
//! * `clear` forgets `decided` with the policy (design A's step 1; the
//!   synthesis dropped the words). Without it a cleared
//!   [`Install::OnDeliveredByte`] entity is never decided again until the
//!   budget's value changes.
//! * The windows are written only if budget AND domain are unchanged (the
//!   synthesis says budget). Strictly more conservative; the domain check is
//!   dead today because only `install`, under the turn, writes the domain.
//! * Step 2 snapshots `{domain, budget}` and the head's piece, and reads
//!   heads and promises afresh at step 5 with no fallback to a step-2 copy:
//!   an entity cannot vanish under a pass, because the pass holds its `Arc`.
//! * [`Backing`] has a `Sized` bound (RPITIT with `Door<Self>` needs it),
//!   and [`Retention::holdings_at`] is `pub` (the proxy's grace test needs a
//!   clock it supplies).
//! * `install` creates the entity for a fresh key before its checks, so an
//!   install that answers `Unbounded` (a pin, a budget that covers the
//!   file) leaves an entity with nothing installed in the map until a
//!   slack pass finds it holding nothing ([`Retention::forget_empty`]) --
//!   one small struct per file ever installed on, and gate-equivalent to
//!   today's missing slot.
//! * An entity left unbounded wants its whole extent again
//!   ([`Backing::want_all`]), which the design's step 3 does not say. Its
//!   passes trim the backend's want-set to the window ([`Backing::want`] at
//!   step 6), and a dropped piece stays dropped until something wants it; a
//!   pin on a single-file torrent changes no selection, so without this the
//!   download the user asked to keep would stop at the window's edge. It is
//!   asked by the pass under a pin and by an install that ends with nothing
//!   installed -- not by every clear: an install that replaces one policy
//!   with another has a window to trim to at its next pass, and a file the
//!   reader left is [`Mode::Slack`], with its pieces on their way off the
//!   disk rather than something to fetch whole.
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
use std::time::{Duration, Instant};

use crate::piece_store::{Decision, RetentionPolicy, Shape, Share};
use crate::retention::{CacheBudget, RetentionBudget, runs};

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
    /// [`Share::Nothing`]: swapping a policy that holds nothing back needs
    /// no backend call, so it can happen under L2 alone while a pass holds
    /// the turn -- see rule 5 in the module docs.
    OnDeliveredByte,
    /// By [`Retention::install`], before the reader opens, so the hold-back
    /// precedes the pieces: there is no un-Have in BitTorrent, and a piece
    /// announced once is announced to every peer that was connected.
    OnOpen,
}

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
    /// Nothing is playing it and nothing is reading it. The pass holds its
    /// whole extent back from what we announce and then takes every byte of
    /// it off the disk. Nothing is kept and nothing is re-announced: an
    /// entity that has been left is not a small cache, it is disposable.
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

/// How long an entity nobody is reading keeps what its last pass concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    /// Pruned by [`Retention::holdings`] once no reader has been seen for
    /// this long. The proxy's grace between one request of a player and its
    /// next; measured from the last delivered byte, and only once no
    /// [`Reader`] is open.
    Grace(Duration),
    /// Until a sibling is installed over it, it is cleared, or the owner
    /// is dropped. The torrent: a policy lives as long as the active file.
    UntilReplaced,
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
    /// torrent's store root, handed to the pass as `retain` is handed one
    /// today, so no constructor changes.
    type Store: Sync;
    /// How the budget is split. [`Share::Nothing`] says [`Self::advertise`]
    /// is unreachable from the pass: the committed set has capacity zero, so
    /// both lists it would be called for are empty by construction.
    const SHARE: Share;
    const TRIGGER: Trigger;
    const INSTALL: Install;
    const LIVENESS: Liveness;

    /// Resolve what `want` names, or `None` when it names nothing that can
    /// be bounded -- a torrent with no metadata. May do I/O; called with the
    /// turn held and no owner lock.
    fn resolve(&self, want: Self::Want) -> impl Future<Output = Option<Self::Domain>> + Send;
    /// Whether an installed `domain` is the one `want` asks about. Pure.
    fn governs(domain: &Self::Domain, want: Self::Want) -> bool;
    /// The piece index space of the entity. Pure.
    fn extent(domain: &Self::Domain) -> Range<u32>;
    /// The policy for `domain` under `budget` bytes, or why there is none.
    /// Pure, and it returns its error rather than logging it: it is called
    /// under L2 from [`Reader::note`], and the owner logs after unlock. The
    /// owner installs only a [`Shape::Split`] -- a budget that covers the
    /// entity bounds nothing and holds nothing back.
    fn policy(domain: &Self::Domain, budget: u64) -> anyhow::Result<RetentionPolicy>;
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
    /// Asked twice on a [`Mode::Slack`] pass, and by nothing else. Once at
    /// the top, under the turn, before the pass destroys anything: the mode
    /// was decided from a reading taken before the driver's first entity,
    /// and a viewer can have started this file since. Then once per run of
    /// the reclaim, at the [`Door`]: a player can open the file again while
    /// its bytes are going, and the run has to stop at the piece it is on
    /// rather than empty the window the new stream is already reading. A
    /// copy-out read like [`Self::keeps_everything`], with no owner lock
    /// held.
    ///
    /// The default is `false`: a backing with no liveness of its own has
    /// nothing that could interrupt a slack run.
    fn is_live(&self, _key: &Self::Key) -> bool {
        false
    }
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
    /// Hold `pieces` back from what we announce (`false`) or put them back
    /// (`true`). Under [`Share::Nothing`] never called; the proxy's
    /// implementation may `debug_assert!` that.
    fn advertise(
        &self,
        pieces: Range<u32>,
        on: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Which build of the backend's record of what it holds is in force,
    /// moving whenever that record -- and with it every hold-back
    /// [`Self::advertise`] put into it -- was thrown away and made again.
    ///
    /// The torrent: a restart out of an error builds a fresh piece store
    /// and a fresh chunk tracker, and librqbit announces everything the
    /// disk holds in the handshake bitfield as it comes back, so the
    /// pieces the window holds back are announced again with nothing here
    /// having asked for it. It cannot be prevented from this side -- the
    /// seed runs under librqbit's own lock, where the call that holds
    /// pieces back is refused -- so the pass notices instead
    /// ([`Installed::asserted_epoch`]). A backing that rebuilds no such
    /// record answers the default and is never asked to re-issue anything.
    ///
    /// Read at step 6 of the pass with the turn held and no owner lock, so
    /// it may take a lock of its own.
    fn epoch(&self, _store: &Self::Store) -> u64 {
        0
    }
    /// The subset of `pieces` this domain alone owns bytes in: the torrent's
    /// boundary rule (`this_files_alone`); the proxy's identity.
    fn alone(&self, domain: &Self::Domain, pieces: &[u32])
    -> impl Future<Output = Vec<u32>> + Send;
    /// Trim what the backend fetches to what the pass decided to keep: want
    /// every piece of `windows` again, and stop wanting every piece of the
    /// entity that is in no window and not `held`. The committed set needs
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
    /// Take `runs` off the disk, asking `door` at the backing's own
    /// granularity **at the instant of each unlink**, and say how many
    /// pieces went. The torrent asks [`Door::window_now`] before every part
    /// of every run and cuts the run with `outside`; the proxy asks
    /// [`Door::refuses`] per chunk inside one blocking closure, so the
    /// asking and the unlink cannot be separated by a suspension. A closure
    /// that dies reports what it can vouch for, which is nothing: the
    /// policy is untouched either way, because it never left its cell.
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
    /// The cleaner's cap, the one cell both sides read. Read before L2 and
    /// never under it.
    budget: Arc<RetentionBudget>,
    /// L1. The entities by key. Held for lookup, insert, prune and
    /// iterate-for-holdings only.
    entities: parking_lot::Mutex<HashMap<B::Key, Arc<Entity<B>>>>,
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
    /// budget that covers it, a pin, or a hold-back the backend refused.
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
    readers: HashMap<ReaderId, ReaderState<B>>,
    /// The entity's own last delivered byte: the proxy's `last_playhead`,
    /// and a torrent file's head. Only its own bytes ever reach it -- a
    /// [`Reader`] is on one entity, [`Retention::note_position`] names one
    /// key -- so a file whose reader has gone on to another file keeps the
    /// head it last had. What a pass measures from once the reader that
    /// asked has ended, and what [`Door::window_now`] draws the window
    /// round.
    last_position: Option<B::Position>,
    /// When a byte last reached a player, which is what [`Liveness::Grace`]
    /// is measured from once no reader is left.
    last_seen: Instant,
    /// Whether this entity's range is held back from what we announce with
    /// nothing installed to put it back.
    ///
    /// A slack pass holds the whole extent back before it unlinks anything,
    /// and drops the policy in the same breath. Where its unlinks are then
    /// refused -- a hash check running, a backend that will not forget --
    /// the entity stands holding bytes it does not announce, and the next
    /// tick's slack pass re-issues the hold-back and retries. That is the
    /// intended shape, and it ends when the bytes go.
    ///
    /// It ends the other way too: a **pin**, which makes the entity one no
    /// slack pass will ever walk again. [`Retention::clear_under`] is the
    /// only thing that gives a range back, and with no policy to read the
    /// range off it used to return at once -- leaving the pieces the user
    /// had just asked us to keep held back from every peer while
    /// [`crate::engine::Engine::standing`], finding no policy, told the
    /// cleaner the torrent announces them. Held back and protected at once
    /// is the one combination that is never right, and there was no pass
    /// left to undo it. So the fact is recorded, and `clear_under` reads it.
    held_back: bool,
    /// What the pass standing over this entity decided to take off the
    /// disk, and what is therefore never put back into what we announce.
    ///
    /// A belt over the turn's braces. Everything that unlinks holds the
    /// turn and so does everything that advertises, so the ordering already
    /// says a piece cannot become announced between a decision and its
    /// unlink -- but the *next* act on the entity is a different pass, and
    /// the one that re-advertises a whole range is
    /// [`Retention::clear_under`] under a pin taken while the reclaim was
    /// running. Half the range is gone from the disk by then, and putting
    /// it back into what we announce is the advertise-then-serve-a-hole
    /// this owner exists to prevent, wearing a pin as its excuse. So the
    /// runs a pass dooms are recorded and subtracted from every
    /// `advertise(_, true)` the owner makes.
    ///
    /// It lives exactly as long as the policy that made it: written at the
    /// pass's decision, cleared when a policy is installed or forgotten.
    doomed: Vec<Range<u32>>,
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
    /// The [`Backing::epoch`] this policy's hold-back is known to be in
    /// force under, and `None` until a pass has read one.
    ///
    /// [`Retention::install`] held this policy's whole range back before
    /// the reader opened, and nothing here gives it back until the policy
    /// goes -- but the backend can lose it without being asked to, by
    /// rebuilding the record that carries it, and then announces every
    /// piece of the window it holds. Nothing can stop that; a pass can see
    /// it, by comparing the epoch it is under with the one this hold-back
    /// was issued under, and issue it again.
    ///
    /// `None` is the install's own assertion. The install cannot read the
    /// epoch -- the store is the pass's, handed to it by the driver -- so
    /// the first pass records what it sees rather than re-issuing what
    /// went out a moment ago, and every later one compares. The window
    /// that leaves open is a rebuild between the install and the first
    /// pass over the entity.
    asserted_epoch: Option<u64>,
}

/// One open read of one entity.
struct ReaderState<B: Backing> {
    /// The position of the last byte this read delivered, and `None` until
    /// it has delivered one. Only ever written from a byte that really went
    /// out, never from a `Range` header.
    playhead: Option<B::Position>,
    /// The pieces this read has promised off the disk and not yet
    /// delivered. Nothing may unlink one of them.
    promised: Range<u32>,
    /// The piece this reader's last pass ran at, so one reader playing on
    /// does not spend another's throttle.
    passed_at: Option<u32>,
}

impl<B: Backing> Default for ReaderState<B> {
    fn default() -> Self {
        Self {
            playhead: None,
            promised: 0..0,
            passed_at: None,
        }
    }
}

/// Names one [`Reader`]. Equality only.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct ReaderId(u64);

/// What [`Retention::install`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    /// A new policy holds the entity back and bounds it.
    Installed,
    /// The policy already installed describes this entity under this
    /// budget, so nothing was touched and nothing re-held-back.
    Kept,
    /// Nothing bounds the entity: no budget yet, no cap, a budget that
    /// covers it, nothing to resolve, a pin, or a hold-back the backend
    /// refused (logged). Whatever was installed before has been given back.
    Unbounded,
    /// The previous policy could not be given back to what we announce, so
    /// it stands and nothing new was installed. Never the held-back range
    /// beside an empty cell: the next install or clear retries.
    OldStands,
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
    /// Pieces that joined the committed set and are now advertised.
    pub committed: usize,
    /// Pieces we had advertised and no longer hold, so no longer announce.
    pub withdrawn: usize,
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
    /// Some reader has delivered a byte and has not ended.
    pub live_playhead: bool,
    /// The entity's last delivered byte, and `None` until one has gone out.
    pub last_position: Option<B::Position>,
    /// When a byte last reached a player.
    pub last_seen: Instant,
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
    /// An owner with no entities, over `backing`, reading the cleaner's cap
    /// from `budget`. `Arc`, because every [`Reader`] holds its owner.
    pub fn new(backing: Arc<B>, budget: Arc<RetentionBudget>) -> Arc<Self> {
        // `install_now` swaps a policy under L2 alone, with no backend call;
        // that is only sound for a backing that held nothing back for the
        // policy it swaps out. A backing that holds back and installs on the
        // delivered byte would leave a range held back beside a policy that
        // does not know about it.
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
            entities: parking_lot::Mutex::new(HashMap::new()),
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
                        domain,
                        installed: None,
                        decided: None,
                        stride: 1,
                        windows: Vec::new(),
                        readers: HashMap::new(),
                        last_position: None,
                        last_seen: Instant::now(),
                        held_back: false,
                        doomed: Vec::new(),
                        opens: 0,
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
    pub fn reader(self: &Arc<Self>, key: B::Key, domain: B::Domain) -> Reader<B> {
        Reader {
            owner: self.clone(),
            entity: self.entity(key, domain),
            id: ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed)),
        }
    }

    /// Open a reader on the entity `key` already has, or `None` when it has
    /// none. The torrent's opener: its install runs before the reader opens
    /// and made the entity if the file could be bounded at all, and a file
    /// that could not -- no metadata to name its pieces by -- has nothing a
    /// head would be measured against, so its bytes are not remembered. L1
    /// only, no I/O.
    pub fn reader_on(self: &Arc<Self>, key: &B::Key) -> Option<Reader<B>> {
        let entity = self.lookup(key)?;
        Some(Reader {
            owner: self.clone(),
            entity,
            id: ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed)),
        })
    }

    /// Where a reader of `key` last got to, told without a [`Reader`]: the
    /// entity's head moves and no reader's does. L2 only, never claims the
    /// turn, never installs. A key with no entity is a byte nothing is
    /// bounding, and it is not remembered; a byte of another key is that
    /// key's and moves nothing here.
    pub fn note_position(&self, key: &B::Key, at: B::Position) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let mut state = entity.state.lock();
        state.last_seen = Instant::now();
        state.last_position = Some(at);
    }

    /// Install (or keep) the policy for `key` about to be streamed, and hold
    /// its pieces back from what we announce. [`Install::OnOpen`]'s path.
    ///
    /// **It touches no other entity.** It used to clear every sibling first
    /// -- one active file per torrent, and the old range given back before
    /// the new one was held back -- under an install-ordering lock that
    /// existed only to make that sweep atomic. Both are gone with the value
    /// that replaced them: what is live is one cell
    /// ([`crate::retention::live`]), the file a reader left is
    /// [`Mode::Slack`] at the next pass, and a slack pass takes its bytes
    /// off the disk rather than putting its range back into what we
    /// announce. Re-announcing a range we are about to delete was issue
    /// (a): a Have per switch for pieces that went seconds later.
    ///
    /// Under this key's turn alone: a pin clears; a policy that already
    /// describes this domain under this budget is kept untouched (nothing
    /// re-held-back); otherwise the old policy is cleared -- advertised back
    /// first, and only on success forgotten -- the new range held back, and
    /// the policy installed. A hold-back the backend refuses installs
    /// nothing: without it every window piece would be announced and
    /// withdrawn seconds later, which is worse than bounding nothing.
    pub async fn install(&self, key: B::Key, want: B::Want) -> InstallOutcome {
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
        entity.state.lock().opened(&mut claim.guard);
        let budget = self.budget.get();
        if self.backing.keeps_everything(&key) {
            // A pin is a retention property: the user asked for those
            // bytes, and they are shared like any other bytes we keep --
            // and fetched whole, which the policy's passes stopped asking
            // for beyond the window.
            return if self.clear_under(&entity, &mut claim).await {
                self.want_whole(&entity).await;
                InstallOutcome::Unbounded
            } else {
                InstallOutcome::OldStands
            };
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
        // Resolved under the turn, as `policy_for` runs under `announce`
        // today: what the backend says the file is, now. A fresh entity was
        // resolved a moment ago to be made at all, and is not asked twice.
        let resolved = match fresh {
            Some(domain) => Some(domain),
            None => self.backing.resolve(want).await,
        };
        let policy = resolved.as_ref().and_then(|domain| match budget {
            CacheBudget::Bytes(bytes) => match B::policy(domain, bytes) {
                Ok(policy) if policy.shape() != Shape::Whole => Some(policy),
                // The budget covers the file. Keep all of it, share all of
                // it, reclaim none of it -- and install nothing, because an
                // installed policy is what makes a piece unadvertised and a
                // piece reclaimable, and neither is true here.
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(
                        key = ?key,
                        error = %format!("{error:#}"),
                        "could not size a retention policy; the entity is neither bounded nor held back"
                    );
                    None
                }
            },
            CacheBudget::Unknown | CacheBudget::Unbounded => None,
        });
        // Whatever was held back before goes back into what we announce
        // first, whether or not a new policy is going in. Otherwise an
        // entity whose reader moved on would leave the old range announced
        // to nobody for the life of the owner, while the cleaner's gate --
        // which reads "no policy covers this piece" as "we announce it" --
        // called those same pieces protected.
        if !self.clear_under(&entity, &mut claim).await {
            return InstallOutcome::OldStands;
        }
        // Nothing installed after this point leaves the entity unbounded,
        // and an unbounded entity is fetched whole: what the old policy's
        // passes stopped wanting is wanted again. A new policy going in
        // wants nothing here -- its first pass trims to its own window.
        let (Some(domain), Some(policy)) = (resolved, policy) else {
            self.want_whole(&entity).await;
            return InstallOutcome::Unbounded;
        };
        let pieces = policy.pieces();
        if B::SHARE == Share::Half
            && let Err(error) = self.backing.advertise(pieces.clone(), false).await
        {
            tracing::warn!(
                key = ?key,
                error = %format!("{error:#}"),
                "could not hold the playback window back from what we announce; the entity is not bounded"
            );
            self.want_whole(&entity).await;
            return InstallOutcome::Unbounded;
        }
        tracing::debug!(
            key = ?key,
            first = pieces.start,
            end = pieces.end,
            shape = ?policy.shape(),
            "holding an entity's pieces back and bounding it to the cache budget"
        );
        entity
            .state
            .lock()
            .install_policy(&mut claim.guard, domain, budget, policy);
        InstallOutcome::Installed
    }

    /// Forget the policy for `key` and put back what it was holding back.
    /// Under the turn.
    pub async fn clear(&self, key: &B::Key) {
        let Some(entity) = self.lookup(key) else {
            return;
        };
        let mut claim = Claim {
            guard: entity.turn.clone().lock_owned().await,
            about: None,
        };
        self.clear_under(&entity, &mut claim).await;
    }

    /// [`Self::clear`] with the turn already held. `true` when nothing is
    /// installed afterwards.
    ///
    /// **The range is advertised back first, and the policy forgotten only
    /// when that succeeded.** Today's order is the reverse -- slot to
    /// `None`, then re-advertise, and a backend that refuses leaves the
    /// pieces held back beside an empty slot, which the cleaner's gate reads
    /// as announced: held back and protected at once, the one combination
    /// that is never right, logged at debug. Here a refusal keeps the policy
    /// (still bounding, still holding back, still telling the gate the
    /// truth), warns, and the next clear -- the next pass under a pin, the
    /// next install -- retries. Under [`Share::Nothing`] nothing was held
    /// back and there is nothing to put back.
    ///
    /// What the policy stopped wanting is not wanted again here: that is
    /// [`Self::want_whole`], asked by the callers that leave the entity with
    /// nothing installed, and not by the ones that replace the policy or
    /// retire a sibling.
    async fn clear_under(&self, entity: &Entity<B>, claim: &mut Claim) -> bool {
        let (extent, doomed) = {
            let state = entity.state.lock();
            match state.installed.as_ref() {
                Some(installed) => (installed.policy.pieces(), state.doomed.clone()),
                // Nothing installed, but a slack pass held the range back
                // and could not finish taking it: see [`State::held_back`].
                // Nothing is doomed there -- a slack pass records no runs --
                // and a piece its reclaim did take is one the backend has
                // already forgotten, so lifting the mask over it announces
                // nothing.
                None if state.held_back => (B::extent(&state.domain), Vec::new()),
                None => return true,
            }
        };
        if B::SHARE == Share::Half {
            // Everything the standing pass doomed is left out. Its bytes
            // are gone or going, and announcing a piece we do not have is
            // the failure this owner exists to prevent -- a pin taken while
            // a reclaim was running is no excuse for it. See
            // [`State::doomed`].
            for run in without(extent, &doomed) {
                if let Err(error) = self.backing.advertise(run, true).await {
                    tracing::warn!(
                        key = ?entity.key,
                        error = %format!("{error:#}"),
                        "could not put a policy's pieces back into what we announce; the policy stands until a later clear can"
                    );
                    return false;
                }
            }
        }
        let mut state = entity.state.lock();
        // The range is back in what we announce, so nothing is holding it
        // back any more -- whether it was a policy's hold-back or a slack
        // pass's ([`State::held_back`]).
        state.held_back = false;
        state.forget_policy(&mut claim.guard);
        true
    }

    /// Forget the entity for `key` once its slack pass has taken the last
    /// of it off the disk and no read is open on it.
    ///
    /// The replacement for [`Liveness::Grace`]'s pruning, and it prunes on
    /// a fact rather than on a clock: an entity that holds nothing and that
    /// nobody is reading has no window to keep, no head worth remembering
    /// and nothing for a later pass to do. A [`Reader`]'s drop never calls
    /// it -- a read that ends is not a stream that has been replaced, and
    /// the entity it read is kept until something else is played.
    ///
    /// L1 → L2, as [`Self::holdings_at`] is (rule 1), and never from inside
    /// a pass: the caller holds no [`Claim`] and no `Arc` of the entity by
    /// the time it asks, so an entity a later reader has opened in the gap
    /// keeps itself.
    pub fn forget_empty(&self, key: &B::Key) {
        let mut entities = self.entities.lock();
        let Some(entity) = entities.get(key) else {
            return;
        };
        if Arc::strong_count(entity) == 1 && entity.state.lock().readers.is_empty() {
            entities.remove(key);
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

    /// The entity's turn, awaited: the torrent's tick and the cleaner's
    /// delete queue here, with no owner lock held (rule 3). `None` is a key
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

    /// One pass over `key`, with its turn in hand and the [`Mode`] its
    /// driver decided.
    ///
    /// [`Mode::Slack`] is the short one: the entity is not being played and
    /// nothing is reading it, so its whole extent is held back from what we
    /// announce and every byte of it is taken off the disk. It keeps
    /// nothing, so it measures nothing -- no window, no commit, no want-set
    /// -- and it leaves nothing installed. [`Mode::Live`] is the pass the
    /// rest of this file describes.
    pub async fn pass(&self, key: &B::Key, store: &B::Store, claim: Claim, mode: Mode) -> Outcome {
        match mode {
            Mode::Live => self.live_pass(key, store, claim).await,
            Mode::Slack { opens } => self.slack_pass(key, store, claim, opens).await,
        }
    }

    /// The entity is slack: everything it holds goes.
    ///
    /// The order is the one the hold-back rule forces. The extent is
    /// un-advertised **first**, before a single unlink, because there is no
    /// un-Have and a piece we announce and then delete is a peer's request
    /// answered with a read past the end of nothing. Then the policy, the
    /// windows and the decision are dropped under L2 -- an entity nobody is
    /// playing has no window, and leaving one standing is what let the
    /// cleaner's gate call a left file's pieces protected. Then every held
    /// run this file alone owns goes through the same [`Backing::reclaim`]
    /// the live pass uses, asking a [`Door`] that answers "take everything"
    /// -- except under a pin, and except once the entity has become live
    /// again, either of which stops the run where it stands.
    ///
    /// Nothing is put back into what we announce on the way out. That is
    /// the whole of issue (a): the install that used to retire a sibling
    /// re-announced the range it was about to delete, so every switch cost
    /// a Have for pieces that went seconds later.
    ///
    /// An entity left holding nothing, with no read open on it, is
    /// forgotten ([`Self::forget_empty`]). Pieces a delete refused -- a
    /// hash check running -- stay held and unadvertised, the entity stays,
    /// and the next tick offers them again.
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
        // asked for those bytes. Nothing is taken and nothing is given
        // back -- the pin's own install is what clears the policy.
        //
        // Asked before L2, like every copy-out of a lock outside the owner
        // (rule 1), and so is the liveness cell beside it.
        if self.backing.keeps_everything(key) || self.backing.is_live(key) {
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, None);
        }
        {
            let state = entity.state.lock();
            if state.opens != opens || !state.readers.is_empty() {
                tracing::debug!(
                    key = ?key,
                    "a stream opened on this entity since its mode was decided; \
                     the slack pass takes nothing"
                );
                return Self::nothing(&state, claim, about, None);
            }
        }
        let domain = entity.state.lock().domain.clone();
        let extent = B::extent(&domain);
        // Where a test puts what playback does while the pass runs.
        self.run_hook();
        let Some(held) = self.backing.held(store, &domain).await else {
            tracing::warn!(
                key = ?key,
                "the slack pass has no reading of the disk; this pass takes nothing"
            );
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, None);
        };
        // Un-advertisable before anything is unlinked, and re-issued every
        // tick for as long as the entity holds anything: a delete refused
        // under a hash check leaves pieces that must stay unannounced.
        if B::SHARE == Share::Half
            && !extent.is_empty()
            && let Err(error) = self.backing.advertise(extent.clone(), false).await
        {
            tracing::warn!(
                key = ?key,
                error = %format!("{error:#}"),
                "could not hold a slack entity's pieces back from what we announce; not deleting them this pass"
            );
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, None);
        }
        let promised: Vec<Range<u32>> = {
            let mut state = entity.state.lock();
            state.go_slack(&mut claim.guard);
            // The hold-back above went out and the policy has just gone, so
            // from here only `clear_under` can give this range back -- see
            // [`State::held_back`]. Under `Share::Nothing` nothing was held
            // back and there is nothing to give.
            state.held_back = B::SHARE == Share::Half && !extent.is_empty();
            state
                .readers
                .values()
                .map(|reader| reader.promised.clone())
                .filter(|range| !range.is_empty())
                .collect()
        };
        let door = Door {
            state: entity.state.clone(),
            backing: self.backing.clone(),
            key: key.clone(),
            domain: domain.clone(),
            mode: Mode::Slack { opens },
            policy: None,
            windows: Vec::new(),
            promised,
        };
        // And what playback does while the unlinks run.
        self.run_hook();
        let candidates: Vec<u32> = held.iter().copied().collect();
        let alone = self.backing.alone(&domain, &candidates).await;
        let reclaimed = self
            .backing
            .reclaim(store, &domain, runs(&alone), door)
            .await;
        let empty = {
            let state = entity.state.lock();
            let empty = reclaimed >= held.len() && state.readers.is_empty();
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
    /// concluded. Today's proxy re-arms from every finish and from no
    /// abandon; here the rule is one: a pass that measured something owes
    /// another if the head moved a stride from what it measured, and a pass
    /// that measured nothing because the policy was replaced under it owes
    /// one outright, because the byte that replaced it was due and could not
    /// start one.
    async fn live_pass(&self, key: &B::Key, store: &B::Store, mut claim: Claim) -> Outcome {
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
        // deleted with the pin set throughout -- and, because the policy
        // also holds its range back, the file the user asked to keep is
        // announced to nobody while librqbit re-fetches it in a loop.
        if self.backing.keeps_everything(key) {
            if self.clear_under(&entity, &mut claim).await {
                self.want_whole(&entity).await;
            }
            let state = entity.state.lock();
            return Self::nothing(&state, claim, about, None);
        }
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
        let (decision, windows, promised, door_policy, at, doomed, asserted) = {
            let mut state = entity.state.lock();
            if !state.still(&begin) {
                return Self::nothing(&state, claim, about, None);
            }
            let Some(at) = state
                .head(about)
                .and_then(|head| B::index_of(&state.domain, head))
            else {
                return Self::nothing(&state, claim, about, None);
            };
            let others: Vec<u32> = state
                .readers
                .iter()
                .filter(|(id, _)| Some(**id) != about)
                .filter_map(|(_, reader)| reader.playhead)
                .filter_map(|position| B::index_of(&state.domain, position))
                .collect();
            let promised: Vec<Range<u32>> = state
                .readers
                .values()
                .map(|reader| reader.promised.clone())
                .filter(|range| !range.is_empty())
                .collect();
            let Some((decision, policy)) = state.advance(&mut claim.guard, at, &held) else {
                return Self::nothing(&state, claim, about, None);
            };
            // One window per live playhead. The policy answers for one
            // playhead at a time -- that is what a window is about -- and an
            // entity two players are inside has two of them. `window_at` and
            // not a second `advance`: a pass is one decision about what to
            // give back, and the other readers' windows are inputs to it.
            // Two heads in one window are one window: a pass about the
            // entity rather than a reader (the tick, a re-arm from the
            // turn) counts every reader among the others, including the one
            // whose byte was the entity's last.
            let mut windows = vec![decision.window.clone()];
            for other in others {
                let window = policy.window_at(other);
                if !windows.contains(&window) {
                    windows.push(window);
                }
            }
            // What this pass has decided to take is what it will never put
            // back into what we announce; see [`State::doomed`]. Written
            // here, under the decision's own lock, so a clear that runs
            // before the next pass -- a pin taken while the reclaim ran --
            // reads it.
            state.doom(&mut claim.guard, runs(&decision.reclaim));
            (
                decision,
                windows,
                promised,
                policy,
                at,
                state.doomed.clone(),
                state.asserted_epoch(),
            )
        };
        // 6. Advertise what is committed before reclaiming: the two sets are
        // disjoint and the commit is what takes a piece out of reach of the
        // reclaim. No owner lock held. Under `Share::Nothing` the committed
        // set has capacity zero, so both lists are empty and the backing is
        // never asked.
        let mut conclusion = Conclusion {
            windows: windows.clone(),
            ..Conclusion::default()
        };
        if B::SHARE == Share::Nothing {
            debug_assert!(
                decision.committed.is_empty() && decision.withdrawn.is_empty(),
                "a Share::Nothing policy committed or withdrew pieces"
            );
        } else {
            // The hold-back first, if the backend has rebuilt the record
            // that carried it: everything of this policy that is not
            // committed goes back out of what we announce before a piece
            // of it is committed or reclaimed, which is the order the
            // install used and for the same reason -- a piece announced
            // once is announced to every peer already connected. See
            // [`Installed::asserted_epoch`] for what `None` is, and why
            // this is a re-issue and not a repair: the Haves librqbit sent
            // as it came back cannot be recalled.
            let epoch = self.backing.epoch(store);
            if asserted != Some(epoch) {
                let committed: Vec<u32> = door_policy.advertised().iter().copied().collect();
                let mut in_force = true;
                if asserted.is_some() {
                    tracing::debug!(
                        key = ?key,
                        epoch,
                        was = asserted,
                        "the backend rebuilt what it holds; holding this entity's window back again"
                    );
                    for run in without(door_policy.pieces(), &runs(&committed)) {
                        if let Err(error) = self.backing.advertise(run, false).await {
                            tracing::warn!(
                                key = ?key,
                                error = %format!("{error:#}"),
                                "could not hold the window back again after the backend rebuilt what it holds; the next pass retries"
                            );
                            in_force = false;
                            break;
                        }
                    }
                }
                if in_force {
                    entity.state.lock().assert_epoch(&mut claim.guard, epoch);
                }
            }
            // The doomed runs are subtracted here too, though the two
            // sets are disjoint by construction (`advance` never commits a
            // piece it reclaims): a belt is only a belt if it is worn on
            // every announcement.
            for run in without_all(runs(&decision.committed), &doomed) {
                if let Err(error) = self.backing.advertise(run.clone(), true).await {
                    tracing::warn!(
                        key = ?key,
                        error = %format!("{error:#}"),
                        "could not announce the pieces the window released; they stay ours and unshared"
                    );
                    break;
                }
                conclusion.committed += (run.end - run.start) as usize;
            }
            // A committed piece the disk has lost behind our back cannot stay
            // announced: that is the advertise-then-refuse this exists to
            // avoid, wearing the other sign.
            for run in runs(&decision.withdrawn) {
                if self.backing.advertise(run.clone(), false).await.is_ok() {
                    conclusion.withdrawn += (run.end - run.start) as usize;
                }
            }
        }
        // The door every unlink of this pass asks, the want-set's included.
        // What the door answers is not for the owner to know about a piece
        // becoming announced under the pass: every advertise is made under
        // this entity's turn, which the pass holds throughout.
        let door = Door {
            state: entity.state.clone(),
            backing: self.backing.clone(),
            key: key.clone(),
            domain: begin.domain.clone(),
            mode: Mode::Live,
            policy: Some(door_policy),
            windows: windows.clone(),
            promised,
        };
        // And the want-set, trimmed to what this pass keeps: the windows
        // wanted, everything of the entity outside them and not on the disk
        // not wanted. What is on the disk and outside them is the reclaim's,
        // below.
        self.backing
            .want(store, &begin.domain, &windows, &held, &door)
            .await;
        // 7. And what playback does while the unlinks run.
        self.run_hook();
        // 8. The reclaim, asking the door at every unlink.
        let alone = self.backing.alone(&begin.domain, &decision.reclaim).await;
        conclusion.reclaimed = self
            .backing
            .reclaim(store, &begin.domain, runs(&alone), door)
            .await;
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

    /// Every entity's holding, as of `Instant::now()`. Prunes
    /// [`Liveness::Grace`] entities first; L1 → L2, no I/O.
    pub fn holdings(&self) -> Vec<(B::Key, Holding<B>)> {
        self.holdings_at(Instant::now())
    }

    /// [`Self::holdings`] against a clock the caller supplies.
    ///
    /// The pruning is here because this is the call that happens once per
    /// cleaner pass rather than once per delivered chunk, and that cadence
    /// is enough: writing a chunk is a filesystem event, and a filesystem
    /// event under the cache root is what arms the cleaner. An entity goes
    /// once nothing but this map holds it -- no [`Reader`] open on it, no
    /// pass in flight -- and the grace has passed since its last delivered
    /// byte. A reader that is open and has delivered nothing keeps its
    /// entity; it also keeps no window and no playhead, so the gate is
    /// told nothing about it.
    pub fn holdings_at(&self, now: Instant) -> Vec<(B::Key, Holding<B>)> {
        let mut entities = self.entities.lock();
        if let Liveness::Grace(grace) = B::LIVENESS {
            entities.retain(|_, entity| {
                Arc::strong_count(entity) > 1
                    || now.duration_since(entity.state.lock().last_seen) < grace
            });
        }
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

    /// How far ahead of `at` the resident policy's window reaches
    /// ([`RetentionPolicy::ahead_of`]), with the domain it is measured in,
    /// or `None` when nothing bounds the entity or `at` is not in its
    /// domain. One L2 reading and no I/O: what a reader about to open is
    /// sized by, so the pieces it asks the backend to fetch ahead are pieces
    /// the next pass will keep.
    pub fn reach(&self, key: &B::Key, at: B::Position) -> Option<(B::Domain, Range<u32>)> {
        let entity = self.lookup(key)?;
        let state = entity.state.lock();
        let installed = state.installed.as_ref()?;
        let index = B::index_of(&state.domain, at)?;
        Some((state.domain.clone(), installed.policy.ahead_of(index)))
    }

    /// How many open reads have promised pieces or delivered a byte and have
    /// not ended: the gate's own reason for refusing the cleaner, counted.
    pub fn readers(&self) -> usize {
        let entities: Vec<Arc<Entity<B>>> = self.entities.lock().values().cloned().collect();
        entities
            .iter()
            .map(|entity| entity.state.lock().readers.len())
            .sum()
    }

    /// [`Self::readers`] for one key: how many open reads of that entity
    /// have promised or delivered and not ended. Zero for a key with no
    /// entity. L1 to look up, then L2.
    pub fn readers_of(&self, key: &B::Key) -> usize {
        self.lookup(key)
            .map(|entity| entity.state.lock().readers.len())
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
}

impl<B: Backing> State<B> {
    /// The head a pass for `about` is about: that reader's own playhead
    /// while its body is open, and the entity's last delivered byte once it
    /// has ended.
    ///
    /// **One function, because it is one question.** Where a pass measures
    /// from and whether a pass is still owed are the same question asked at
    /// two moments, and the second only terminates if it is a fixed point of
    /// the first: two spellings of it, one reading the reader and one the
    /// entity, differ by however far apart two players are, no pass moves
    /// either of them, and so every pass arms the next one forever.
    fn head(&self, about: Option<ReaderId>) -> Option<B::Position> {
        about
            .and_then(|id| self.readers.get(&id))
            .and_then(|reader| reader.playhead)
            .or(self.last_position)
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
        let policy = B::policy(&self.domain, bytes)?;
        let Shape::Split { window, .. } = policy.shape() else {
            // The budget covers it: nothing here will reclaim anything, and
            // a reader inside it is inside all of it.
            return Ok(());
        };
        self.stride = stride_for::<B>(window);
        self.installed = Some(Installed {
            budget,
            policy,
            asserted_epoch: None,
        });
        Ok(())
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
            Shape::Split { window, .. } => stride_for::<B>(window),
            Shape::Whole => 1,
        };
        for reader in self.readers.values_mut() {
            reader.passed_at = None;
        }
        self.doomed = Vec::new();
        self.installed = Some(Installed {
            budget,
            policy,
            asserted_epoch: None,
        });
    }

    /// Forget the policy, and that the entity was decided at all. Under the
    /// turn, and only after its range has been given back: see
    /// [`Retention::clear_under`].
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
        // The doomed runs were this policy's decision; a later policy makes
        // its own.
        self.doomed = Vec::new();
    }

    /// Advance the resident policy in place, and hand back a copy of it as
    /// advanced, for the door. Under the turn. `None` is nothing installed,
    /// which the caller has already ruled out under the same lock.
    fn advance(
        &mut self,
        _turn: &mut Turn,
        at: u32,
        held: &BTreeSet<u32>,
    ) -> Option<(Decision, RetentionPolicy)> {
        let installed = self.installed.as_mut()?;
        let decision = installed.policy.advance(at, held);
        Some((decision, installed.policy.clone()))
    }

    /// The epoch the standing policy's hold-back was issued under, or
    /// `None` when no pass has read one; asked only where a policy is
    /// known to stand. See [`Installed::asserted_epoch`].
    fn asserted_epoch(&self) -> Option<u64> {
        self.installed
            .as_ref()
            .and_then(|installed| installed.asserted_epoch)
    }

    /// Record that the standing policy's hold-back is in force under
    /// `epoch`. Under the turn.
    fn assert_epoch(&mut self, _turn: &mut Turn, epoch: u64) {
        if let Some(installed) = self.installed.as_mut() {
            installed.asserted_epoch = Some(epoch);
        }
    }

    /// Write what a pass concluded. Under the turn.
    fn conclude(&mut self, _turn: &mut Turn, windows: Vec<Range<u32>>) {
        self.windows = windows;
    }

    /// Record what this pass has decided to take off the disk, so nothing
    /// puts it back into what we announce. Under the turn; see
    /// [`Self::doomed`].
    fn doom(&mut self, _turn: &mut Turn, runs: Vec<Range<u32>>) {
        self.doomed = runs;
    }

    /// A stream has been opened on this entity. Under the turn, from
    /// [`Retention::install`] alone; see [`Self::opens`].
    fn opened(&mut self, _turn: &mut Turn) {
        self.opens += 1;
    }

    /// Nothing bounds this entity any more and everything it holds is on
    /// its way off the disk. Under the turn, after the extent has been held
    /// back from what we announce.
    ///
    /// The windows go with the policy, deliberately: a window is what a
    /// pass measured round a head somebody was at, and an entity nobody is
    /// playing has none. Left standing they would tell the cleaner's gate
    /// that a left file's pieces are protected, which is how a switch used
    /// to leave the previous film on the disk under two owners' protection
    /// and neither one's deleter.
    ///
    /// Nothing is doomed here, though the pass is about to unlink
    /// everything. [`Self::doomed`] exists so that a re-advertise cannot
    /// put back what a reclaim is taking, and the only re-advertise the
    /// owner makes is [`Retention::clear_under`]'s, which returns at once
    /// when nothing is installed -- which is the line above. A slack pass
    /// has already held its whole extent back before it takes a byte, so
    /// there is nothing left for a doomed list to keep from being
    /// announced again.
    fn go_slack(&mut self, _turn: &mut Turn) {
        self.installed = None;
        self.decided = None;
        self.windows = Vec::new();
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
            live_playhead: self
                .readers
                .values()
                .any(|reader| reader.playhead.is_some()),
            last_position: self.last_position,
            last_seen: self.last_seen,
            decided: self.decided,
        }
    }
}

/// `whole` with every range of `holes` taken out of it, in ascending order.
///
/// The pieces a pass has doomed are not put back into what we announce, and
/// what the owner has to announce is a range at a time, so a hold-back that
/// straddles one becomes two calls. Empty holes leave the range as it was.
fn without(whole: Range<u32>, holes: &[Range<u32>]) -> Vec<Range<u32>> {
    without_all(vec![whole], holes)
}

/// [`without`] over several ranges at once.
fn without_all(ranges: Vec<Range<u32>>, holes: &[Range<u32>]) -> Vec<Range<u32>> {
    let mut kept = ranges;
    for hole in holes {
        kept = kept
            .into_iter()
            .flat_map(|part| crate::retention::outside(part, hole))
            .collect();
    }
    kept
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

/// One open read: a handle that holds its promise and carries its playhead.
/// Dropping it is what says the read is over, and it is the only thing that
/// releases a promise.
pub struct Reader<B: Backing> {
    owner: Arc<Retention<B>>,
    entity: Arc<Entity<B>>,
    id: ReaderId,
}

impl<B: Backing> Reader<B> {
    /// This read will deliver `pieces` off the disk, and until it has,
    /// nothing may unlink them. The range shrinks from the front as
    /// [`Self::note`] reports bytes going out, and is released whole when
    /// this handle is dropped. An empty promise records nothing.
    pub fn promises(&self, pieces: Range<u32>) {
        if pieces.is_empty() {
            return;
        }
        let mut state = self.entity.state.lock();
        state.readers.entry(self.id).or_default().promised = pieces;
    }

    /// A byte at `at` of this entity has reached a player.
    ///
    /// The budget is read before L2 (a copy-out of a foreign lock, rule 2);
    /// under L2 the entity's `last_seen` and `last_position`, an
    /// [`Install::OnDeliveredByte`] decide when the budget moved, this
    /// reader's playhead and the shrink of its promise; and, for
    /// [`Trigger::OnMove`], whether the byte moved a stride since this
    /// reader's last pass -- `abs_diff`, so a seek back is as much a reason
    /// to look as playing on. A due byte tries the turn **under L2** (rule
    /// 3): `Some` is a claim the caller must hand to [`Retention::pass`];
    /// `None` is a pass in flight, and the pass's conclusion asks the same
    /// head again.
    pub fn note(&self, at: B::Position) -> Option<Claim> {
        let budget = self.owner.budget.get();
        let (claim, refused) = {
            let mut state = self.entity.state.lock();
            state.last_seen = Instant::now();
            state.last_position = Some(at);
            let refused = if B::INSTALL == Install::OnDeliveredByte && state.decided != Some(budget)
            {
                state.install_now(budget).err()
            } else {
                None
            };
            let index = B::index_of(&state.domain, at);
            let (bounded, stride) = (state.installed.is_some(), state.stride);
            let reader = state.readers.entry(self.id).or_default();
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
    /// chose stay for [`Liveness`] to decide.
    fn drop(&mut self) {
        self.entity.state.lock().readers.remove(&self.id);
    }
}

/// The pass's last asking, built by the owner and handed to
/// [`Backing::reclaim`]. Sync and cheap, callable from a blocking thread:
/// it takes L2 briefly and no other lock, and asks
/// [`Backing::keeps_everything`] before L2, never under it.
pub struct Door<B: Backing> {
    state: Arc<parking_lot::Mutex<State<B>>>,
    backing: Arc<B>,
    key: B::Key,
    domain: B::Domain,
    /// Which pass this door is for. A [`Mode::Slack`] door keeps nothing
    /// but what a live read was promised, and closes the moment the entity
    /// is played again.
    mode: Mode,
    /// The policy as this pass advanced it, so the window at the door is
    /// the shape the decision was made with. `None` under [`Mode::Slack`],
    /// which has no policy and no window: there is no shape to ask about.
    policy: Option<RetentionPolicy>,
    /// What this pass concluded, one window per playhead live at the
    /// re-read. Empty under [`Mode::Slack`].
    windows: Vec<Range<u32>>,
    /// Every promise live at the re-read.
    promised: Vec<Range<u32>>,
}

impl<B: Backing> Door<B> {
    /// The window round the entity's head at this instant, or `None` for
    /// "take nothing more" -- the entity is pinned now, or it has no head
    /// the domain indexes. Neither is a state that has a window for the pass
    /// to keep, so the reclaim stops rather than skipping a run: a pin does
    /// not un-pin mid-loop. The first of [`Self::windows_now`].
    pub fn window_now(&self) -> Option<Range<u32>> {
        self.windows_now().map(|mut windows| windows.swap_remove(0))
    }

    /// The torrent's shape: every window live at this instant, from one
    /// reading of L2 -- the one round the entity's head first, then one
    /// round each open reader's *current* playhead -- or `None` as
    /// [`Self::window_now`] answers it. Two readers on one file (a seek is a
    /// second response on the file still playing) each have a head, and a
    /// run between them is cut round both: the entity's head is the last
    /// byte either delivered, and a window round that alone would have the
    /// pass take the piece the other reader is inside. The pass's own
    /// windows are not here: they were drawn round heads at the re-read,
    /// and where those heads have moved the window at the door is the
    /// current one, as it has always been for the entity's.
    pub fn windows_now(&self) -> Option<Vec<Range<u32>>> {
        if self.backing.keeps_everything(&self.key) {
            return None;
        }
        if matches!(self.mode, Mode::Slack { .. }) {
            // A slack entity keeps nothing, so an empty window cuts nothing
            // out of a run -- unless a player opened it again while its
            // bytes were going, and then the run stops where it is rather
            // than emptying the window the new stream is already reading.
            // Asked here, at the unlink, and not carried in from the mode
            // the driver decided.
            let empty: Range<u32> = 0..0;
            return (!self.backing.is_live(&self.key)).then(|| vec![empty]);
        }
        let Some(policy) = self.policy.as_ref() else {
            debug_assert!(false, "a live door with no policy");
            return None;
        };
        let state = self.state.lock();
        let head = state.last_position?;
        let index = B::index_of(&self.domain, head)?;
        let mut windows = vec![policy.window_at(index)];
        for window in state
            .readers
            .values()
            .filter_map(|reader| reader.playhead)
            .filter_map(|position| B::index_of(&self.domain, position))
            .map(|head| policy.window_at(head))
        {
            if !windows.contains(&window) {
                windows.push(window);
            }
        }
        Some(windows)
    }

    /// The proxy's shape: whether `index` may not be taken at this instant.
    /// Refused when the entity keeps everything; when the index is inside
    /// this pass's own windows or the promises it snapshotted; and, under
    /// L2, when a live reader's promise covers it or the window round a
    /// live reader's *current* playhead does. The entity's last delivered
    /// byte is deliberately not asked about: a pass whose own reader has
    /// ended is already holding the window round that, in the windows the
    /// decision built.
    pub fn refuses(&self, index: u32) -> bool {
        if self.backing.keeps_everything(&self.key) {
            return true;
        }
        if self.windows.iter().any(|window| window.contains(&index))
            || self.promised.iter().any(|range| range.contains(&index))
        {
            return true;
        }
        if matches!(self.mode, Mode::Slack { .. }) {
            // Nothing of a slack entity is kept for what somebody might
            // read; what is kept is what an open read was already promised,
            // and it is served every byte of it. And the entity being
            // played again closes the door outright.
            return self.backing.is_live(&self.key)
                || self
                    .state
                    .lock()
                    .readers
                    .values()
                    .any(|reader| reader.promised.contains(&index));
        }
        let Some(policy) = self.policy.as_ref() else {
            debug_assert!(false, "a live door with no policy");
            return true;
        };
        let state = self.state.lock();
        state
            .readers
            .values()
            .any(|reader| reader.promised.contains(&index))
            || state
                .readers
                .values()
                .filter_map(|reader| reader.playhead)
                .filter_map(|position| B::index_of(&self.domain, position))
                .any(|head| policy.window_at(head).contains(&index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::marker::PhantomData;
    use std::sync::atomic::AtomicBool;

    const PIECE: u64 = 1000;

    /// The two shapes of driver the owner has to carry, as consts a fake
    /// can be built over.
    trait Side: Send + Sync + 'static {
        const SHARE: Share;
        const TRIGGER: Trigger;
        const INSTALL: Install;
        const LIVENESS: Liveness;
    }

    /// The torrent's shape: half the budget shared, the tick as trigger,
    /// installed before the reader opens, kept until replaced.
    struct TorrentSide;
    impl Side for TorrentSide {
        const SHARE: Share = Share::Half;
        const TRIGGER: Trigger = Trigger::External;
        const INSTALL: Install = Install::OnOpen;
        const LIVENESS: Liveness = Liveness::UntilReplaced;
    }

    /// The proxy's shape: nothing shared, the delivered byte as trigger,
    /// installed on that byte, a grace once no reader is left.
    struct ProxySide;
    const GRACE: Duration = Duration::from_secs(90);
    impl Side for ProxySide {
        const SHARE: Share = Share::Nothing;
        const TRIGGER: Trigger = Trigger::OnMove {
            passes_per_window: 20,
        };
        const INSTALL: Install = Install::OnDeliveredByte;
        const LIVENESS: Liveness = Liveness::Grace(GRACE);
    }

    /// One file of full pieces, `PIECE` bytes each.
    #[derive(Clone, PartialEq, Debug)]
    struct FakeDomain {
        file: usize,
        pieces: Range<u32>,
        /// A domain no policy can be sized for: `policy` returns the error
        /// the owner has to carry out from under L2.
        broken: bool,
    }

    fn domain(file: usize, pieces: Range<u32>) -> FakeDomain {
        FakeDomain {
            file,
            pieces,
            broken: false,
        }
    }

    fn broken(file: usize, pieces: Range<u32>) -> FakeDomain {
        FakeDomain {
            broken: true,
            ..domain(file, pieces)
        }
    }

    /// A position in the fake's coordinates: which file, and the byte
    /// offset into it.
    type At = (usize, u64);

    type Hook<S> = Box<dyn Fn(&Door<FakeBacking<S>>) + Send + Sync>;

    /// An in-memory backing with the knobs the tests need: the disk as a
    /// set, recorders for every advertise and reclaim call, a park inside
    /// `held`, `advertise` and `reclaim` (a oneshot the test releases, and
    /// one it is told through when the park begins), a settable
    /// `keeps_everything`, an `advertise` that can fail and a `reclaim`
    /// whose blocking closure can die.
    struct FakeBacking<S: Side> {
        domains: parking_lot::Mutex<HashMap<usize, FakeDomain>>,
        held: parking_lot::Mutex<BTreeSet<u32>>,
        advertised: parking_lot::Mutex<Vec<(Range<u32>, bool)>>,
        fail_advertise: AtomicBool,
        fail_held: AtomicBool,
        /// How many listings were asked for.
        listings: AtomicU64,
        keeps_everything: AtomicBool,
        /// What [`Backing::is_live`] answers: the entity the fake is
        /// playing right now.
        is_live: AtomicBool,
        /// What [`Backing::epoch`] answers: moved by a test to say that the
        /// backend threw away everything it was told about what to hold
        /// back, as a restart out of an error does.
        epoch: AtomicU64,
        /// The runs each `reclaim` call was handed.
        reclaims: parking_lot::Mutex<Vec<Vec<Range<u32>>>>,
        /// The runs each `reclaim` call really asked the door about, which
        /// stops at the first `window_now` of `None`.
        asked: parking_lot::Mutex<Vec<Vec<Range<u32>>>>,
        reclaim_panics: AtomicBool,
        park_held: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        park_advertise: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        park_reclaim: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        entered: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        /// Run inside `advertise`, with whatever the test wants to try
        /// there.
        on_advertise: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
        /// Run inside `reclaim` with the door, before any run is walked.
        on_reclaim: parking_lot::Mutex<Option<Hook<S>>>,
        /// Run between two runs of one reclaim.
        between_runs: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
        _side: PhantomData<S>,
    }

    impl<S: Side> FakeBacking<S> {
        fn new(domains: impl IntoIterator<Item = FakeDomain>) -> Arc<Self> {
            Arc::new(Self {
                domains: parking_lot::Mutex::new(
                    domains.into_iter().map(|d| (d.file, d)).collect(),
                ),
                held: parking_lot::Mutex::new(BTreeSet::new()),
                advertised: parking_lot::Mutex::new(Vec::new()),
                fail_advertise: AtomicBool::new(false),
                fail_held: AtomicBool::new(false),
                listings: AtomicU64::new(0),
                keeps_everything: AtomicBool::new(false),
                is_live: AtomicBool::new(false),
                epoch: AtomicU64::new(1),
                reclaims: parking_lot::Mutex::new(Vec::new()),
                asked: parking_lot::Mutex::new(Vec::new()),
                reclaim_panics: AtomicBool::new(false),
                park_held: parking_lot::Mutex::new(None),
                park_advertise: parking_lot::Mutex::new(None),
                park_reclaim: parking_lot::Mutex::new(None),
                entered: parking_lot::Mutex::new(None),
                on_advertise: parking_lot::Mutex::new(None),
                on_reclaim: parking_lot::Mutex::new(None),
                between_runs: parking_lot::Mutex::new(None),
                _side: PhantomData,
            })
        }

        fn holds(&self, pieces: impl IntoIterator<Item = u32>) {
            self.held.lock().extend(pieces);
        }

        fn on_disk(&self) -> Vec<u32> {
            self.held.lock().iter().copied().collect()
        }

        /// Park the next call of the named kind: the returned receiver
        /// fires when the pass is inside it, and the sender lets it go.
        fn park(
            slot: &parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
            entered: &parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        ) -> (
            tokio::sync::oneshot::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ) {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            *slot.lock() = Some(release_rx);
            *entered.lock() = Some(entered_tx);
            (entered_rx, release_tx)
        }

        fn park_held(
            &self,
        ) -> (
            tokio::sync::oneshot::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ) {
            Self::park(&self.park_held, &self.entered)
        }

        fn park_reclaim(
            &self,
        ) -> (
            tokio::sync::oneshot::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ) {
            Self::park(&self.park_reclaim, &self.entered)
        }

        fn park_advertise(
            &self,
        ) -> (
            tokio::sync::oneshot::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ) {
            Self::park(&self.park_advertise, &self.entered)
        }

        async fn parked(
            slot: &parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
            entered: &parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        ) {
            let release = slot.lock().take();
            if let Some(release) = release {
                if let Some(entered) = entered.lock().take() {
                    let _ = entered.send(());
                }
                let _ = release.await;
            }
        }
    }

    impl<S: Side> Backing for FakeBacking<S> {
        type Key = usize;
        type Position = At;
        type Domain = FakeDomain;
        type Want = usize;
        type Store = ();
        const SHARE: Share = S::SHARE;
        const TRIGGER: Trigger = S::TRIGGER;
        const INSTALL: Install = S::INSTALL;
        const LIVENESS: Liveness = S::LIVENESS;

        async fn resolve(&self, want: usize) -> Option<FakeDomain> {
            self.domains.lock().get(&want).cloned()
        }

        fn governs(domain: &FakeDomain, want: usize) -> bool {
            domain.file == want
        }

        fn extent(domain: &FakeDomain) -> Range<u32> {
            domain.pieces.clone()
        }

        fn policy(domain: &FakeDomain, budget: u64) -> anyhow::Result<RetentionPolicy> {
            if domain.broken {
                anyhow::bail!("a domain nothing can be sized for");
            }
            let count = u64::from(domain.pieces.end - domain.pieces.start);
            RetentionPolicy::new(
                budget,
                PIECE,
                domain.pieces.clone(),
                count * PIECE,
                S::SHARE,
            )
        }

        fn index_of(domain: &FakeDomain, (file, offset): At) -> Option<u32> {
            if file != domain.file {
                return None;
            }
            let last = u64::from(domain.pieces.end - 1);
            Some((u64::from(domain.pieces.start) + offset / PIECE).min(last) as u32)
        }

        fn keeps_everything(&self, _key: &usize) -> bool {
            self.keeps_everything.load(Ordering::SeqCst)
        }

        fn is_live(&self, _key: &usize) -> bool {
            self.is_live.load(Ordering::SeqCst)
        }

        async fn held(&self, _store: &(), _domain: &FakeDomain) -> Option<BTreeSet<u32>> {
            self.listings.fetch_add(1, Ordering::SeqCst);
            Self::parked(&self.park_held, &self.entered).await;
            if self.fail_held.load(Ordering::SeqCst) {
                return None;
            }
            Some(self.held.lock().clone())
        }

        async fn advertise(&self, pieces: Range<u32>, on: bool) -> anyhow::Result<()> {
            if let Some(hook) = self.on_advertise.lock().as_ref() {
                hook();
            }
            Self::parked(&self.park_advertise, &self.entered).await;
            if self.fail_advertise.load(Ordering::SeqCst) {
                anyhow::bail!("the backend would not change what it advertises");
            }
            self.advertised.lock().push((pieces, on));
            Ok(())
        }

        fn epoch(&self, _store: &()) -> u64 {
            self.epoch.load(Ordering::SeqCst)
        }

        async fn alone(&self, _domain: &FakeDomain, pieces: &[u32]) -> Vec<u32> {
            pieces.to_vec()
        }

        /// Both shapes at once: `window_now` gates each run as the torrent
        /// does, `refuses` gates each index as the proxy does, and the disk
        /// loses what neither refused.
        async fn reclaim(
            &self,
            _store: &(),
            _domain: &FakeDomain,
            runs: Vec<Range<u32>>,
            door: Door<Self>,
        ) -> usize {
            if self.reclaim_panics.load(Ordering::SeqCst) {
                // The proxy's shape of failure: the blocking closure dies,
                // and the join error is the whole of what the pass hears.
                return tokio::task::spawn_blocking(|| -> usize {
                    panic!("the unlink closure died")
                })
                .await
                .unwrap_or(0);
            }
            Self::parked(&self.park_reclaim, &self.entered).await;
            self.reclaims.lock().push(runs.clone());
            if let Some(hook) = self.on_reclaim.lock().as_ref() {
                hook(&door);
            }
            let mut asked = Vec::new();
            let mut freed = 0;
            for run in runs {
                if door.window_now().is_none() {
                    break;
                }
                asked.push(run.clone());
                for index in run {
                    if !door.refuses(index) && self.held.lock().remove(&index) {
                        freed += 1;
                    }
                }
                if let Some(hook) = self.between_runs.lock().as_ref() {
                    hook();
                }
            }
            self.asked.lock().push(asked);
            freed
        }
    }

    type Proxy = FakeBacking<ProxySide>;
    type Torrent = FakeBacking<TorrentSide>;

    /// An eight-piece entity under a budget of four pieces: the proxy
    /// shape gives the whole budget to the window, so the window is four
    /// pieces and the stride is one.
    fn proxy() -> (Arc<Proxy>, Arc<Retention<Proxy>>, Arc<RetentionBudget>) {
        let backing = Proxy::new([domain(0, 0..8)]);
        backing.holds(0..8);
        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(4 * PIECE));
        let owner = Retention::new(backing.clone(), budget.clone());
        (backing, owner, budget)
    }

    /// Two eight-piece files under a budget of four pieces: the torrent
    /// shape splits it two and two.
    fn torrent() -> (Arc<Torrent>, Arc<Retention<Torrent>>, Arc<RetentionBudget>) {
        let backing = Torrent::new([domain(0, 0..8), domain(1, 8..16)]);
        backing.holds(0..16);
        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(4 * PIECE));
        let owner = Retention::new(backing.clone(), budget.clone());
        (backing, owner, budget)
    }

    /// Run a pass to its end in a task of its own, so the test can be
    /// inside it while it is parked.
    fn spawn_pass<S: Side>(
        owner: &Arc<Retention<FakeBacking<S>>>,
        key: usize,
        claim: Claim,
    ) -> tokio::task::JoinHandle<Outcome> {
        let owner = owner.clone();
        tokio::spawn(async move { owner.pass(&key, &(), claim, Mode::Live).await })
    }

    /// **A pass that dies leaves the policy where it was, and the next one
    /// runs.**
    ///
    /// The abandoned-pass hole, closed by construction: today both drivers
    /// `take()` the policy for the length of a pass, so a pass future
    /// dropped at its listing -- the runtime shutting down -- walks off with
    /// it, and on the proxy the `running` flag it set is never cleared, so
    /// no later pass runs either. Here the policy never moves and the claim
    /// is a guard.
    #[tokio::test]
    async fn a_pass_dropped_at_its_listing_leaves_the_policy_installed_and_the_next_pass_runs() {
        let (backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("the first byte is due");
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
        // Turn free, policy in its cell: the next pass reclaims round the
        // head at piece 0, which is the window 0..4.
        let claim = owner.try_turn(&0).expect("the dead pass released the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(outcome.concluded.expect("a pass that ran").reclaimed, 4);
        assert_eq!(backing.on_disk(), vec![0, 1, 2, 3]);
    }

    /// **A reclaim whose closure dies reports nothing freed and touches
    /// nothing.**
    ///
    /// Today's proxy `abandon(None)` after a join error: the policy went
    /// down with the task, `bounded` and the windows stand as shadows of
    /// it, and the entity is unbounded until the budget's value changes.
    /// Here there is nothing to lose.
    #[tokio::test]
    async fn a_reclaim_that_panics_leaves_the_policy_the_decision_and_the_windows_untouched() {
        let (backing, owner, _budget) = proxy();
        let second = owner.reader(0, domain(0, 0..8));
        // The second player's first byte is due too; its claim is dropped
        // unused, which is a pass that never ran.
        drop(second.note((0, 6 * PIECE)));
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 3 * PIECE)).expect("due");
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        // One window per live playhead: 7 is inside the second player's.
        assert_eq!(first.windows, vec![3..7, 4..8]);
        assert_eq!(first.reclaimed, 3);
        let before = owner.holding(&0).expect("the entity");
        assert_eq!(before.windows, vec![3..7, 4..8]);

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
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("the pass to reach its reclaim");
        // A stride is one piece here; the head moves two.
        assert!(
            reader.note((0, 2 * PIECE)).is_none(),
            "a note during a pass took the turn"
        );
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

        // And a byte that did not move a stride arms nothing.
        let (entered, release) = backing.park_reclaim();
        let claim = owner.try_turn(&0).expect("the turn");
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked");
        assert!(reader.note((0, 2 * PIECE + 1)).is_none());
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
        let (_backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
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
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            outcome.concluded.expect("a pass that ran").windows,
            vec![3..7]
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
        let (probe, _waiter) = watch_release(&owner.lookup(&0).expect("the entity"));
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked");
        assert!(reader.note((0, 2 * PIECE)).is_none());
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

    /// **A clear the backend refuses keeps the policy**, and a later clear
    /// retries and succeeds.
    ///
    /// The one deliberate change from today. Today's `clear_retention_locked`
    /// empties the slot first and re-advertises second, so a refusal leaves
    /// the range held back beside an empty slot: the gate reads it as
    /// announced, and nothing retries. Here the order is the other way and
    /// the policy stands until the range really is given back.
    #[tokio::test]
    async fn a_clear_the_backend_refuses_keeps_the_policy_and_a_later_clear_retries() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        backing.fail_advertise.store(true, Ordering::SeqCst);
        owner.clear(&0).await;
        assert!(
            owner.holding(&0).unwrap().installed.is_some(),
            "a refused re-advertise dropped the policy: held back and read as announced"
        );
        // An install on a sibling meets the same refusal retiring this one,
        // and then its own hold-back is refused too: nothing new stands and
        // the old policy still does.
        assert_eq!(owner.install(1, 1).await, InstallOutcome::Unbounded);
        assert!(owner.holding(&0).unwrap().installed.is_some());
        assert!(owner.holding(&1).unwrap().installed.is_none());
        backing.fail_advertise.store(false, Ordering::SeqCst);
        owner.clear(&0).await;
        assert!(owner.holding(&0).unwrap().installed.is_none());
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..8, true)]
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
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.clear(&0).await;
        assert_eq!(
            *got_it.lock(),
            vec![true, true],
            "L2 was held across advertise"
        );
    }

    /// **The door refuses what a live reader holds, at the instant it is
    /// asked**: a promise made during the reclaim, the window round a
    /// playhead that moved during it, the pass's own windows, and
    /// everything once the entity keeps everything.
    #[tokio::test]
    async fn the_door_refuses_promises_live_windows_pass_windows_and_everything_under_a_pin() {
        let (backing, owner, budget) = proxy();
        // A two-piece window, so a second head in the back half of the file
        // does not cover the whole reclaim.
        budget.set(Some(2 * PIECE));
        let reader = owner.reader(0, domain(0, 0..8));
        // A second player at piece 7 whose body ends while the unlinks run:
        // its window is in the pass's own windows and nowhere else by then.
        let second = owner.reader(0, domain(0, 0..8));
        drop(second.note((0, 7 * PIECE)));
        let second = parking_lot::Mutex::new(Some(second));
        // A body framed over piece 4 that ends while the unlinks run: the
        // pass snapshotted its promise at the re-read and honours it to the
        // end, as today's does.
        let ended = owner.reader(0, domain(0, 0..8));
        ended.promises(4..5);
        let ended = parking_lot::Mutex::new(Some(ended));
        // A third player arriving during the unlinks.
        let third = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        let answers = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let hook: Hook<ProxySide> = Box::new({
            let answers = answers.clone();
            let backing = backing.clone();
            move |door: &Door<Proxy>| {
                let mut answers = answers.lock();
                // The pass's windows are 0..2 and 6..8; 2..6 is the reclaim.
                answers.push(("pass window", door.refuses(1)));
                drop(second.lock().take());
                answers.push(("pass window of an ended reader", door.refuses(7)));
                drop(ended.lock().take());
                answers.push(("promised at the re-read", door.refuses(4)));
                answers.push(("free before", door.refuses(5)));
                third.promises(5..6);
                answers.push(("promised since", door.refuses(5)));
                answers.push(("free before", door.refuses(3)));
                // A note during a pass starts nothing, but its window is
                // live at the door.
                assert!(third.note((0, 3 * PIECE)).is_none());
                answers.push(("live window since", door.refuses(3)));
                assert_eq!(
                    door.window_now(),
                    Some(3..5),
                    "the window round the entity's head, which the third reader moved"
                );
                backing.keeps_everything.store(true, Ordering::SeqCst);
                answers.push(("pinned", door.refuses(2)));
                assert_eq!(door.window_now(), None);
                backing.keeps_everything.store(false, Ordering::SeqCst);
            }
        });
        *backing.on_reclaim.lock() = Some(hook);
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            *answers.lock(),
            vec![
                ("pass window", true),
                ("pass window of an ended reader", true),
                ("promised at the re-read", true),
                ("free before", false),
                ("promised since", true),
                ("free before", false),
                ("live window since", true),
                ("pinned", true),
            ]
        );
        // Piece 2 went; 3 and 4 (the third player's window, and 4 promised
        // at the re-read), 5 (promised since) and 6, 7 (the pass's own
        // windows) stayed.
        assert_eq!(outcome.reclaimed, 1);
        assert_eq!(backing.on_disk(), vec![0, 1, 3, 4, 5, 6, 7]);
    }

    /// **A pin taken between two runs of one reclaim stops the second
    /// run**: `window_now` answers `None` and the run is never asked
    /// about.
    #[tokio::test]
    async fn a_pin_taken_between_the_runs_of_a_reclaim_stops_it_before_the_second() {
        let (backing, owner, _budget) = torrent();
        // Held 0..3 and 6..8 of file 0; the head at piece 4 draws a
        // two-piece window 4..6, so the reclaim is two runs.
        *backing.held.lock() = [0, 1, 2, 6, 7].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 4 * PIECE));
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
        assert_eq!(*backing.reclaims.lock(), vec![vec![0..3, 6..8]]);
        assert_eq!(
            *backing.asked.lock(),
            vec![vec![0..3]],
            "the second run was asked about under a pin"
        );
        assert_eq!(outcome.reclaimed, 3);
        assert_eq!(backing.on_disk(), vec![6, 7]);
        // The next pass finds the pin first: the policy goes, the range
        // goes back to the swarm, nothing is listed, and the claim is
        // released under the state lock like any other exit's.
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
        // **And what the clear puts back is the extent minus what the
        // stopped pass had doomed.** Pieces 0, 1 and 2 are off the disk; a
        // pin is no reason to tell a peer we have them, and telling one is
        // how a request is answered with a read past the end of nothing.
        // The window the pass kept -- 3..6, still on the disk -- goes back
        // whole. See [`State::doomed`].
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (3..6, true)]
        );
        assert_eq!(backing.reclaims.lock().len(), 1);
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
            "the extent was never held back from the swarm"
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
        // empties it.
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
        let reader = owner.reader_on(&0).expect("the entity the install made");
        assert!(
            reader.note((0, 0)).is_none(),
            "the tick is the torrent's trigger"
        );
        backing.advertised.lock().clear();

        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner.pass(&0, &(), claim, Mode::Slack { opens }).await;

        assert!(outcome.concluded.is_none());
        assert_eq!(backing.on_disk(), vec![0, 1, 2]);
        assert!(backing.advertised.lock().is_empty());

        // The body ends, and the file nobody is playing is slack again.
        drop(reader);
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

    /// **A pin outranks the slack: nothing of a pinned entity is taken and
    /// nothing of it is held back.**
    ///
    /// A pin is a retention property -- the user asked for those bytes --
    /// and the slack pass is the one delete in this owner that would take a
    /// whole extent, so it is also the one that would empty a pinned
    /// download. Asked before the extent is held back, because the hold-back
    /// is what a pinned file must not have: a pinned download is shared
    /// whole.
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
            "a pinned file's pieces are not held back from the swarm"
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
    /// entity nobody is playing it tells the cleaner's gate that the left
    /// file's pieces are protected -- which is a piece with two owners'
    /// protection and neither one's deleter, the shape this slice exists to
    /// remove.
    #[tokio::test]
    async fn a_slack_pass_that_took_nothing_leaves_no_window_standing() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a live pass");
        assert!(
            !owner.holding(&0).unwrap().windows.is_empty(),
            "the live pass concluded a window"
        );

        // The unlinks die, so the entity survives its own slack pass and
        // the next tick has to walk it again.
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
    }

    /// **A pin on an entity whose slack pass could not finish gives its
    /// bytes back to the swarm.**
    ///
    /// A slack pass holds the whole extent back before it unlinks anything,
    /// because a delete refused under a hash check leaves pieces that must
    /// stay unannounced. It then drops the policy. If the unlinks are
    /// refused, what stands is an entity holding bytes with nothing
    /// installed and its range held back -- and the next tick's slack pass
    /// re-issues the hold-back and retries, which is the intended shape.
    ///
    /// But a pin lands on that state, and a pinned entity is never slack:
    /// no further slack pass runs. [`Retention::clear_under`] is what puts a
    /// range back, and it had nothing to put back from -- no policy, so it
    /// returned at once. The pieces the user had just asked us to keep were
    /// then held back from every peer while
    /// [`crate::engine::Engine::standing`], finding no policy, told the
    /// cleaner the torrent announces them: held back and protected at once,
    /// which is the one combination that is never right, and with no pass
    /// left to undo it.
    ///
    /// So the range goes back, exactly as a policy's would. The whole
    /// extent, with nothing subtracted: `set_pieces_advertised(_, true)`
    /// lifts a mask rather than claiming anything, and the fork emits a
    /// Have only for a piece we have *and* were holding back
    /// (`live::set_pieces_advertised`), so a piece the reclaim did take --
    /// forgotten by the backend before it was unlinked -- announces
    /// nothing.
    #[tokio::test]
    async fn a_pin_on_a_slack_entity_that_kept_its_bytes_announces_them_again() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));

        // The slack pass holds the extent back and then cannot unlink.
        backing.reclaim_panics.store(true, Ordering::SeqCst);
        let opens = owner.opens_of(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Slack { opens }).await;
        assert!(
            owner.holding(&0).expect("the entity").installed.is_none(),
            "the slack pass dropped the policy"
        );
        assert!(
            backing.advertised.lock().iter().any(|(_, on)| !on),
            "and held its range back before trying to unlink"
        );

        // The user pins the file. Nothing will run a slack pass over it
        // again, so this is the last chance to give the range back.
        backing.reclaim_panics.store(false, Ordering::SeqCst);
        backing.keeps_everything.store(true, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;

        let covered: BTreeSet<u32> = backing
            .advertised
            .lock()
            .iter()
            .filter(|(_, on)| *on)
            .flat_map(|(range, _)| range.clone())
            .collect();
        assert_eq!(
            covered,
            (0..8).collect::<BTreeSet<u32>>(),
            "the pinned entity's range is back in what we announce, and the \
             pieces it holds with it"
        );
    }

    /// **An entity a reader is open on is never forgotten, even emptied.**
    ///
    /// [`Retention::forget_empty`] prunes on a fact -- it holds nothing and
    /// nobody is reading it -- and both halves are asked under L1 after the
    /// pass has let go of its own `Arc`. A [`Reader`] that has delivered
    /// nothing yet is not in the state's reader map at all: it is a stream
    /// handed out a moment ago, whose first byte has not gone out, and
    /// forgetting the entity under it would leave the read with no head and
    /// so no window for the pass that follows.
    #[tokio::test]
    async fn an_emptied_entity_a_reader_holds_is_not_forgotten() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let opens = owner.opens_of(&0);
        let reader = owner.reader_on(&0).expect("the entity the install made");
        assert_eq!(owner.readers_of(&0), 0, "it has delivered nothing");

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
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
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
        assert_eq!(concluded.windows, vec![0..2]);
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
        assert_eq!(holding.windows, vec![0..2], "the failed listing concluded");
        assert_eq!(*backing.advertised.lock(), vec![(0..8, false)]);
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
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(first.reclaimed, 4);
        let windows_before = owner.holding(&0).unwrap().windows.clone();
        assert_eq!(backing.reclaims.lock().len(), 1);

        let claim = reader.note((0, 2 * PIECE)).expect("moved a stride");
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        budget.set(Some(2 * PIECE));
        // The byte that observes the new budget decides under it, under L2
        // alone, while the pass holds the turn.
        assert!(reader.note((0, 2 * PIECE)).is_none());
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
            vec![2..4]
        );
        assert!(successor.again.is_none());
        assert_eq!(owner.holding(&0).unwrap().windows, vec![2..4]);
        assert_eq!(backing.on_disk(), vec![2, 3]);

        // The same publish landing during the unlinks rather than the
        // listing: the re-read has passed, the unlinks stand as refetch cost
        // (as today's do), and the conclusion is still not written.
        budget.set(Some(4 * PIECE));
        // A publish makes every reader due again, whatever the stride: the
        // old `passed_at` described a shape that no longer exists.
        let claim = reader
            .note((0, 2 * PIECE))
            .expect("a budget change makes a paused reader due");
        let settled = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        let windows_before = settled.windows;
        assert_eq!(windows_before, vec![2..6]);
        let claim = reader.note((0, 5 * PIECE)).expect("moved a stride");
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the reclaim");
        budget.set(Some(2 * PIECE));
        assert!(reader.note((0, 5 * PIECE)).is_none());
        release.send(()).expect("the parked pass");
        let outcome = pass.await.expect("joined");
        assert_eq!(
            outcome.concluded.expect("a pass that ran").windows,
            vec![4..8]
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
        assert_eq!(owner.holding(&0).unwrap().windows, vec![5..7]);

        // A proxy entity has held nothing back, so clearing it asks the
        // backing for nothing -- and neither does installing on one.
        owner.clear(&0).await;
        assert!(owner.holding(&0).unwrap().installed.is_none());
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert!(backing.advertised.lock().is_empty());

        // OnOpen: the note carries the byte and nothing else.
        let (_backing, owner, budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        budget.set(Some(2 * PIECE));
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(
            reader.note((0, PIECE)).is_none(),
            "the tick is the torrent's trigger"
        );
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
    /// its pass parks at the listing; the cleaner publishes a smaller
    /// budget; the body's last byte, piece 6, lands -- it decides the new
    /// policy under L2, is due (every reader is), tries the turn, finds it
    /// taken, and the body ends. The pass resumes and refuses at its
    /// re-read, correctly: it measured for a budget nobody holds. Dropped
    /// there with no `again`, the claim takes the last byte's pass with it,
    /// and the tail the fill wrote stays over budget until the grace prunes
    /// the entity. Today's proxy re-arms from every finish; the owner hands
    /// the claim on from the refusal too.
    #[tokio::test]
    async fn the_last_byte_under_a_new_budget_during_a_refused_pass_gets_its_pass() {
        let (backing, owner, budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(first.windows, vec![0..4]);
        let claim = reader.note((0, 2 * PIECE)).expect("moved a stride");
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        // The fill writes ahead while the pass waits on the disk.
        backing.holds(4..8);
        budget.set(Some(2 * PIECE));
        assert!(
            reader.note((0, 6 * PIECE)).is_none(),
            "a note during a pass took the turn"
        );
        release.send(()).expect("the parked pass");
        let refused = pass.await.expect("joined");
        assert!(
            refused.concluded.is_none(),
            "a pass measured under the old budget concluded something"
        );
        assert_eq!(backing.reclaims.lock().len(), 1);
        assert_eq!(owner.holding(&0).unwrap().windows, vec![0..4]);
        let again = refused
            .again
            .expect("the last byte, delivered under the new budget, is owed a pass");
        let successor = owner.pass(&0, &(), again, Mode::Live).await;
        assert_eq!(
            successor.concluded.expect("the successor").windows,
            vec![6..8]
        );
        assert!(successor.again.is_none());
        assert_eq!(owner.holding(&0).unwrap().windows, vec![6..8]);
        assert_eq!(backing.on_disk(), vec![6, 7]);
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
        let (entered, release) = backing.park_held();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the listing");
        assert!(reader.note((0, 3 * PIECE)).is_none());
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
            vec![3..7]
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
        budget.set(None);
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(
            reader.note((0, 0)).is_none(),
            "a byte of an unbounded entity was due"
        );
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
        budget.set(Some(8 * PIECE));
        assert!(reader.note((0, PIECE)).is_none());
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.decided, Some(CacheBudget::Bytes(8 * PIECE)));
        assert!(holding.installed.is_none());
        // A domain no policy can be sized for: decided, nothing installed,
        // and the error carried out from under the lock.
        let owner = Retention::new(Proxy::new([broken(0, 0..8)]), budget.clone());
        budget.set(Some(4 * PIECE));
        let reader = owner.reader(0, broken(0, 0..8));
        assert!(reader.note((0, 0)).is_none());
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.decided, Some(CacheBudget::Bytes(4 * PIECE)));
        assert!(holding.installed.is_none());
    }

    /// **What `install` answers when it installs nothing, and when the old
    /// policy will not go**: no budget, a budget that covers the file, a
    /// want that resolves to nothing, a pin; `Kept` only under the same
    /// budget; the domain resolved afresh under the turn; and `OldStands`
    /// from both of its sites -- a refused clear under a new budget, and
    /// under a pin.
    #[tokio::test]
    async fn an_install_that_bounds_nothing_says_so_and_a_policy_that_will_not_go_stands() {
        let (backing, owner, budget) = torrent();
        budget.set(None);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Unbounded);
        budget.set(Some(8 * PIECE));
        assert_eq!(
            owner.install(0, 0).await,
            InstallOutcome::Unbounded,
            "a budget that covers the file installed a policy"
        );
        assert!(owner.holding(&0).unwrap().installed.is_none());
        budget.set(Some(4 * PIECE));
        assert_eq!(
            owner.install(2, 2).await,
            InstallOutcome::Unbounded,
            "a want that resolves to nothing was installed"
        );
        assert!(owner.holding(&2).is_none());
        assert!(backing.advertised.lock().is_empty());
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        // A pin clears, and gives the range back.
        backing.keeps_everything.store(true, Ordering::SeqCst);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Unbounded);
        assert!(owner.holding(&0).unwrap().installed.is_none());
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..8, true)]
        );
        backing.keeps_everything.store(false, Ordering::SeqCst);
        // The same key under a new budget is not kept: given back, and
        // held back afresh.
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        budget.set(Some(6 * PIECE));
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert_eq!(
            owner.holding(&0).unwrap().installed.map(|i| i.budget),
            Some(CacheBudget::Bytes(6 * PIECE))
        );
        assert_eq!(
            *backing.advertised.lock(),
            vec![
                (0..8, false),
                (0..8, true),
                (0..8, false),
                (0..8, true),
                (0..8, false)
            ]
        );
        // The domain is what the backend says the file is now.
        backing.domains.lock().insert(0, domain(0, 0..6));
        budget.set(Some(4 * PIECE));
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        assert_eq!(owner.holding(&0).unwrap().domain, domain(0, 0..6));
        // The old policy will not go: nothing new is installed and the old
        // one stands, under a new budget and under a pin alike.
        backing.fail_advertise.store(true, Ordering::SeqCst);
        budget.set(Some(2 * PIECE));
        assert_eq!(owner.install(0, 0).await, InstallOutcome::OldStands);
        assert_eq!(
            owner.holding(&0).unwrap().installed.map(|i| i.budget),
            Some(CacheBudget::Bytes(4 * PIECE))
        );
        backing.keeps_everything.store(true, Ordering::SeqCst);
        assert_eq!(owner.install(0, 0).await, InstallOutcome::OldStands);
        assert!(owner.holding(&0).unwrap().installed.is_some());
    }

    /// **A pass advertises what the window released and withdraws what the
    /// disk lost**, and a refused announce stops the announcing without
    /// stopping the pass.
    ///
    /// The torrent's share: a second pass whose playhead walked past a
    /// piece the first window covered commits it, and it is advertised
    /// before the reclaim; a committed piece the disk no longer holds is
    /// un-advertised. The tick is the torrent's trigger, so a head that
    /// moved during the reclaim arms nothing.
    #[tokio::test]
    async fn a_pass_advertises_the_pieces_it_committed_and_withdraws_the_ones_the_disk_lost() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        let first = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            (first.committed, first.withdrawn, first.reclaimed),
            (0, 0, 6)
        );
        // Playback walks to piece 1: piece 0 is behind it, was covered, and
        // is committed.
        owner.note_position(&0, (0, PIECE));
        *backing.between_runs.lock() = Some(Box::new({
            let owner = owner.clone();
            move || owner.note_position(&0, (0, 3 * PIECE))
        }));
        let claim = owner.turn(&0).await.expect("the turn");
        let second = owner.pass(&0, &(), claim, Mode::Live).await;
        let conclusion = second.concluded.expect("a pass");
        assert_eq!((conclusion.committed, conclusion.withdrawn), (1, 0));
        assert!(second.again.is_none(), "the tick armed a pass");
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..1, true)]
        );
        assert_eq!(
            owner.holding(&0).unwrap().installed.unwrap().committed,
            [0].into_iter().collect()
        );
        // The disk loses the committed piece behind our back.
        backing.held.lock().remove(&0);
        let claim = owner.turn(&0).await.expect("the turn");
        let third = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!((third.committed, third.withdrawn), (0, 1));
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..1, true), (0..1, false)]
        );
        // A refused announce: the pass counts nothing committed and goes
        // on to its reclaim.
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        owner.note_position(&0, (0, PIECE));
        backing.fail_advertise.store(true, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(outcome.committed, 0, "a refused announce was counted");
        assert_eq!(
            backing.reclaims.lock().len(),
            2,
            "a refused announce stopped the pass"
        );
        assert_eq!(*backing.advertised.lock(), vec![(0..8, false)]);
    }

    /// **A backend that threw away what it was holding back is told again,
    /// once.**
    ///
    /// The install held the file back from what we announce before the
    /// reader opened, and nothing here gives it back while the policy
    /// stands -- but a restart out of an error builds librqbit a fresh
    /// chunk tracker, and the hold-back went with the old one: the window's
    /// pieces are announced again, by an event no call of ours ordered and
    /// none can refuse. The only thing left is to notice, which is what the
    /// epoch is for. So the pass that finds it moved holds everything the
    /// policy has not committed back again -- the committed half is what we
    /// announce, and re-issuing over it would withdraw what a peer is
    /// downloading -- and the pass after it, finding the epoch where it
    /// left it, says nothing.
    #[tokio::test]
    async fn a_moved_epoch_holds_the_window_back_again_and_the_pass_after_it_does_not() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        owner.note_position(&0, (0, 0));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        // Playback walks to piece 1, which commits piece 0: the re-issue
        // has to leave a committed piece announced.
        owner.note_position(&0, (0, PIECE));
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..1, true)],
            "the install's hold-back and the commit, and no re-issue: the first \
             pass recorded the epoch the install went out under"
        );

        // The restart.
        backing.epoch.fetch_add(1, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..1, true), (1..8, false)],
            "everything but the committed piece is held back again"
        );
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            backing.advertised.lock().len(),
            3,
            "and the pass after it re-issued nothing"
        );

        // A refused re-issue is not recorded as made: the next pass tries
        // again, and the one after that -- once it went out -- does not.
        backing.epoch.fetch_add(1, Ordering::SeqCst);
        backing.fail_advertise.store(true, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(backing.advertised.lock().len(), 3, "a refusal was recorded");
        backing.fail_advertise.store(false, Ordering::SeqCst);
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(
            *backing.advertised.lock(),
            vec![(0..8, false), (0..1, true), (1..8, false), (1..8, false)],
            "the retry"
        );
        let claim = owner.turn(&0).await.expect("the turn");
        owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(backing.advertised.lock().len(), 4);
    }

    /// **A reader reports what it holds**: its promise shrinks from the
    /// front as bytes go out and an empty promise records nothing; a seek
    /// back a stride is as due as playing on; the holding says what is
    /// promised, whether a playhead is live, the extent, and when a byte
    /// last went out.
    #[tokio::test]
    async fn a_reader_reports_what_it_holds() {
        let (_backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        reader.promises(0..0);
        assert_eq!(owner.readers(), 0, "an empty promise was recorded");
        let made = owner.holding(&0).unwrap().last_seen;
        reader.promises(2..6);
        let holding = owner.holding(&0).unwrap();
        assert_eq!(holding.promised, vec![2..6]);
        assert_eq!(holding.extent, 0..8);
        assert!(!holding.live_playhead, "a promise is not a delivered byte");
        assert_eq!(owner.readers(), 1);
        while Instant::now() == made {}
        let claim = reader.note((0, 3 * PIECE)).expect("due");
        let holding = owner.holding(&0).unwrap();
        assert_eq!(
            holding.promised,
            vec![4..6],
            "the delivered piece is still promised"
        );
        assert!(holding.live_playhead);
        assert!(
            holding.last_seen > made,
            "a delivered byte did not move last_seen"
        );
        assert_eq!(holding.last_position, Some((0, 3 * PIECE)));
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert!(outcome.concluded.is_some() && outcome.again.is_none());
        assert!(
            reader.note((0, PIECE)).is_some(),
            "a seek back a stride was not due"
        );
        drop(reader);
        let holding = owner.holding(&0).unwrap();
        assert!(holding.promised.is_empty() && !holding.live_playhead);
    }

    /// **A byte in file B between two runs of file A's reclaim leaves A's
    /// pass concluding normally on A's own head**: the byte is B's, A's
    /// head is where it was, `window_now` still answers A's window, and the
    /// second run is asked about and taken.
    #[tokio::test]
    async fn a_byte_in_another_file_between_the_runs_of_a_reclaim_leaves_it_running() {
        let (backing, owner, _budget) = torrent();
        *backing.held.lock() = [0, 1, 2, 6, 7].into_iter().collect();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
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
        assert_eq!(*backing.asked.lock(), vec![vec![0..3, 6..8]]);
        assert_eq!(outcome.reclaimed, 5);
        assert!(backing.on_disk().is_empty());
        assert_eq!(
            owner.holding(&0).unwrap().last_position,
            Some((0, 4 * PIECE)),
            "a byte of file 1 moved file 0's head"
        );
    }

    /// **Two readers on one entity each have a window at the door of the
    /// torrent's shape.** The entity's head is the last byte either
    /// delivered, and `windows_now` answers the window round it and the one
    /// round every other open reader's current playhead, from one reading;
    /// a reader that has ended is not among them, and its window goes.
    #[tokio::test]
    async fn the_torrents_door_answers_a_window_per_open_reader() {
        let (backing, owner, _budget) = torrent();
        assert_eq!(owner.install(0, 0).await, InstallOutcome::Installed);
        let first = owner.reader_on(&0).expect("the entity install made");
        let second = owner.reader_on(&0).expect("the same entity");
        assert!(
            owner.reader_on(&9).is_none(),
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
        assert!(second.note((0, 6 * PIECE)).is_none());
        assert_eq!(owner.readers_of(&0), 2);
        assert_eq!(owner.readers_of(&1), 0);
        let answers = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let first = parking_lot::Mutex::new(Some(first));
        let hook: Hook<TorrentSide> = Box::new({
            let answers = answers.clone();
            move |door: &Door<Torrent>| {
                let mut answers = answers.lock();
                answers.push(door.windows_now());
                answers.push(door.window_now().map(|window| vec![window]));
                drop(first.lock().take());
                answers.push(door.windows_now());
            }
        });
        *backing.on_reclaim.lock() = Some(hook);
        let claim = owner.turn(&0).await.expect("the turn");
        let outcome = owner
            .pass(&0, &(), claim, Mode::Live)
            .await
            .concluded
            .expect("a pass");
        assert_eq!(
            outcome.windows,
            vec![6..8, 0..2],
            "one window per head at the re-read"
        );
        assert_eq!(
            *answers.lock(),
            vec![
                Some(vec![6..8, 0..2]),
                Some(std::iter::once(6..8).collect()),
                Some(std::iter::once(6..8).collect()),
            ],
            "the entity's head first, then the other reader's; the first's gone once it ended"
        );
        assert_eq!(owner.readers_of(&0), 1);
        drop(second);
        assert_eq!(owner.readers_of(&0), 0);
    }

    /// **The test hook runs twice per pass, with the turn held and no owner
    /// lock**: a note inside it finds the pass running, and a holding can
    /// be read.
    #[tokio::test]
    async fn the_hook_runs_before_the_listing_and_before_the_reclaim_with_no_owner_lock() {
        let (_backing, owner, _budget) = proxy();
        let reader = Arc::new(owner.reader(0, domain(0, 0..8)));
        let fired = Arc::new(AtomicU64::new(0));
        owner.hook({
            let owner = owner.clone();
            let reader = reader.clone();
            let fired = fired.clone();
            move || {
                fired.fetch_add(1, Ordering::SeqCst);
                assert!(
                    reader.note((0, 2 * PIECE)).is_none(),
                    "a note inside the hook found the turn free"
                );
                assert!(owner.holding(&0).is_some());
            }
        });
        let claim = reader.note((0, 0)).expect("due");
        let outcome = owner.pass(&0, &(), claim, Mode::Live).await;
        assert_eq!(fired.load(Ordering::SeqCst), 2);
        // The hook's byte landed before the re-read, so this pass measured
        // from it and owes nothing for it.
        assert_eq!(
            outcome.concluded.expect("a pass").windows,
            vec![2..6],
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
        let install = {
            let owner = owner.clone();
            tokio::spawn(async move { owner.install(0, 0).await })
        };
        entered.await.expect("the install to reach its hold-back");
        assert!(
            owner.try_turn(&0).is_none(),
            "the turn was free during the hold-back"
        );
        let reader = owner.reader(0, domain(0, 0..8));
        assert!(reader.note((0, PIECE)).is_none());
        owner.note_position(&0, (0, 2 * PIECE));
        backing.keeps_everything.store(true, Ordering::SeqCst);
        let holding = owner.holding(&0).expect("the entity");
        assert_eq!(holding.last_position, Some((0, 2 * PIECE)));
        assert!(
            holding.installed.is_none(),
            "installed before the hold-back was made"
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
        let (_backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        let claim = reader.note((0, 0)).expect("due");
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
        let (entered, release) = backing.park_reclaim();
        let pass = spawn_pass(&owner, 0, claim);
        entered.await.expect("parked at the reclaim");
        budget.set(Some(2 * PIECE));
        assert!(reader.note((0, 5 * PIECE)).is_none());
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
    }

    /// An entity nobody holds and nobody has delivered to for the grace is
    /// pruned; one with a reader open, however quiet, is not; the torrent's
    /// are never pruned.
    #[tokio::test]
    async fn the_grace_prunes_only_what_nothing_holds() {
        let (_backing, owner, _budget) = proxy();
        let reader = owner.reader(0, domain(0, 0..8));
        reader.note((0, 0));
        let later = Instant::now() + GRACE + Duration::from_secs(1);
        assert_eq!(
            owner.holdings_at(later).len(),
            1,
            "an open reader's entity was pruned"
        );
        assert_eq!(owner.readers(), 1);
        drop(reader);
        assert_eq!(owner.readers(), 0, "a dropped reader is still counted");
        assert_eq!(owner.holdings_at(Instant::now()).len(), 1);
        assert_eq!(owner.holdings_at(later).len(), 0);

        let (_backing, owner, _budget) = torrent();
        owner.install(0, 0).await;
        assert_eq!(owner.holdings_at(later).len(), 1);
    }

    /// The pass future and the door can be sent to another thread, which
    /// is what the proxy's driver does with the one and the proxy's
    /// blocking closure with the other.
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
