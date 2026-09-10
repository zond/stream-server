//! Where a proxied stream's playback has got to, what an open read has
//! promised, and the window that follows the one and never takes the other.
//!
//! This is the proxy's adapter over [`enginefs::retention::owner`], the one
//! retention owner both drivers run: the same budget, the same
//! 90%-ahead-10%-behind window, the same rule about what may be reclaimed,
//! and the same pass -- what is here is what makes a chunk directory an
//! entity of it ([`ProxyBacking`]), and the wiring that was missing before
//! any of it existed: **a proxied stream had no playhead at all**. It serves
//! ranges, so the reads were always there and the route always knew the
//! offset, but nothing recorded where playback *was*, so there was nothing
//! for a window to follow and the cache was bounded by the cleaner's walk
//! alone: a minute after the last write at best, while a stream at 20 MB/s
//! writes a gigabyte in that minute.
//!
//! # The playhead is an observation, and its absence is real
//!
//! A reader's playhead exists when a byte of it reaches a player, and never
//! before ([`Reader::note`]). At process start the map is empty, and that is
//! **true** rather than a placeholder: nothing has been played yet by this
//! process, so there is no position it could name. A cache directory left by
//! a previous run says which bytes were fetched and nothing whatever about
//! where anyone had got to in them, and inventing a playhead from it -- the
//! middle, the start, the last chunk written -- would have the window keep a
//! region nobody has ever read and reclaim the region a player is about to
//! ask for. A reader that has been *opened* and has delivered nothing is
//! therefore a reader with no playhead and no window: what it has is a
//! promise, which is the other half of this module.
//!
//! Nothing here is persisted for the same reason. What survives a restart is
//! the chunks; a chunk with no live reader is ordinary cache, which is
//! exactly what the cleaner already treats it as.
//!
//! # An open read holds what it promised, and that is the missing interlock
//!
//! A torrent's reader is protected by librqbit: a reclaim goes through
//! `drop_pieces`, which will not forget a piece a reader is waiting on, so a
//! live read's own bytes cannot be unlinked out from under it. **A proxied
//! read has no backend to refuse**, and it is not only the cleaner it needs
//! refusing -- the retention pass unlinks the same files, and the playhead
//! that drives it is the reading body's own. A response is framed before its
//! first byte goes out (`Content-Length`, `Content-Range`), so the bytes it
//! has yet to deliver are already promised; a window that had moved on from
//! them would delete a body's own tail while the body was reading it, and
//! the player would get a truncated read of a range the cache told it it
//! held.
//!
//! So a [`Reader`] carries two things and they answer two different
//! questions. Its **playhead** says where the window should be, and it is an
//! observation that can be absent. Its **promise** ([`Reader::promises`]) is
//! the chunks an open body still has to deliver off the disk; it shrinks as
//! they go out and it is released when the body ends or the client vanishes,
//! and while it stands neither the pass nor the cleaner may take those
//! chunks. The promise is not a second policy: it decides nothing about what
//! to keep, it only refuses to unlink bytes we have already said we would
//! serve.
//!
//! What that can cost is stated rather than hidden. A body framed around
//! more than the budget holds more than the budget until it has delivered
//! it, and there is no version of this that does not: the alternative is
//! deleting bytes a player has been told it is being sent. It takes a budget
//! that shrank *after* the run was cached to get there at all -- what a
//! lookup can promise is the contiguous run it found, and under a budget
//! that has been in force since the run was written, that run is a window.
//!
//! # Two players in one entity are two readers
//!
//! `p=` is deliberately out of the cache key, so two players reading one
//! stream share its chunks -- and they read it at two different offsets.
//! There is one policy per entity (a budget is a statement about a volume)
//! but a window is a statement about *a playhead*, so the pass asks the
//! policy for one window per live reader and reclaims what none of them
//! covers. Two players in one entity therefore cost two windows, exactly as
//! two players in two entities would; what they do not do is delete each
//! other's read-ahead, which one playhead per entity made them do in
//! alternation.
//!
//! # Two differences from the torrent's wiring, and both are the adapter's
//!
//! * **Nothing is held back and nothing is announced.** A torrent's window
//!   has to be kept out of what we advertise, because there is no un-Have in
//!   BitTorrent and a piece announced once is announced forever. A proxied
//!   response is not seeded: there is no peer, so there is nothing to hold
//!   back from, no committed set to fill and nothing to withdraw. The policy
//!   says that with [`Share::Nothing`] -- the whole budget is window -- and
//!   not with a case of its own.
//! * **The reclaim needs no interlock with a have-set.** A torrent's piece
//!   file may only be unlinked after librqbit has forgotten the piece, under
//!   the claim `enginefs::retention::take_claimed` holds, or the torrent
//!   advertises bytes it no longer has. Nothing believes anything about a
//!   proxy chunk except the directory listing itself -- and the promises
//!   above, which is why they are here.
//!
//! # When a pass runs
//!
//! On the playhead moving, and nowhere else. A torrent's pass rides the
//! reconciler's two-second tick because a torrent fills its cache whether or
//! not anyone is reading (the swarm sends what the want-set asks for); a
//! proxied entity grows only as its own body is relayed, so the byte that
//! moves the window is the same byte that grew the cache, and the pass
//! belongs there. It is throttled to [`PASSES_PER_WINDOW`] passes per
//! window of playback, which bounds the overshoot to a twentieth of the
//! budget and keeps the directory listing off the hot path. The owner does
//! the throttling ([`Trigger::OnMove`]); what a delivered byte hands this
//! module is a [`Claim`] on the entity's turn, and [`ProxyRetention::spawn_pass`]
//! is the task that runs the pass under it, and every pass the first one
//! says it owes.
//!
//! # What is protected when nothing needs bounding
//!
//! A policy is installed only when the budget really splits the entity. For
//! a budget that covers it (`Shape::Whole` -- the phone with 379 GB free
//! and every desktop), for a volume with no cap, and before any pass has
//! published a budget at all (`CacheBudget::Unknown`, which is an absence
//! and not a zero), there is nothing to reclaim -- **but there is still a
//! player inside these bytes**, and the cleaner's cap is a per-volume number
//! that the rest of the cache can push past on its own. So the window in
//! that case is the whole entity, which is what the policy itself answers
//! for `Shape::Whole`, and it is the same answer the torrent half gives:
//! an engine with no policy is `TorrentGate::Announced`, and the cleaner may
//! take none of it. What is *not* protected in either case is a stream
//! nobody is reading, which is the first thing that should go.

use std::collections::BTreeSet;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use anyhow::Context as _;
use enginefs::chunk_store::ChunkDir;
use enginefs::piece_store::{RetentionPolicy, Share};
use enginefs::retention::owner::{Backing, Claim, Door, Install, Liveness, Retention, Trigger};
use enginefs::retention::{ReclaimGate, RetentionBudget};

use crate::proxy_cache::CHUNK_BYTES;

/// How long after its last delivered byte an entity nothing is reading any
/// more keeps its entry, and with it the windows the last pass over it
/// chose.
///
/// While a reader is open its promise and its window stand whatever the
/// clock says -- a paused player is still going to read the body it has --
/// so this is the grace *between* reads: the gap between one request of a
/// player and its next, and a demuxer that is reconnecting. Once it passes
/// with no reader open, every byte of the entity is ordinary cache again --
/// which is what the proxy cache has always been, and what a proxied stream
/// nobody is reading should be.
///
/// **What it does not do is outlast the next pass.** What stands during the
/// grace is the windows the last pass concluded, and a pass writes those
/// wholesale from the readers that were live when it ran; a read that has
/// ended is not one of those, and its playhead is nowhere in the protection
/// path. So this is a grace for a player between two of its own requests,
/// where no other read of the entity runs a pass in the gap, and not against
/// one that does: a seek is two readers, and when the body the seek left
/// behind ends, the first pass for the new position drops the region that
/// was just played rather than holding it for the time here. Scrubbing back
/// into it then refetches from the origin.
///
/// That is a deliberate trade and not an oversight. Holding it would mean
/// keeping a window round every playhead a read left behind until this
/// elapsed -- a whole extra window's worth of chunks per ended read, on the
/// device whose disk is the reason any of this exists -- to protect a
/// position the player has just deliberately left. The bound is the point.
const IDLE: Duration = Duration::from_secs(90);

/// How many passes one window's worth of playback gets: the pass runs when
/// the playhead has moved a window over this.
///
/// Tying the throttle to the window rather than to a fixed number of chunks
/// makes the overshoot a fraction of the budget instead of a constant, and
/// makes the listing rarer exactly when it is dearer -- a big window is many
/// bucket directories.
///
/// The number itself is the overshoot the bound tolerates, and nothing
/// subtler than that: what is on the disk when a pass measures it is the
/// window plus whatever the fill wrote since the last pass, which is a
/// stride. A twentieth of the budget is a small enough overhang to be
/// invisible against the cleaner's own margin, and twenty directory
/// listings per window of playback is a cheap way to buy it.
const PASSES_PER_WINDOW: u64 = 20;

/// A proxied entity, as the owner sees it: a chunk directory, its length,
/// and the URL it was relayed for.
///
/// Fixed for the entity's life: a response of a different length is a
/// different entity in a different directory, so none of this can go stale
/// under the key. The `target` is **the one thing here that is not
/// derivable from the store**: a cache key is a hash of the URL *and* the
/// player headers that reach the origin, so an entity's directory name
/// cannot be worked back to the URL a client is holding, and a panel asking
/// "what do you hold for the stream I am playing" has only that URL to ask
/// with. It is written when the entity is made and dies with it; at process
/// start there are no entities, so there is no URL to read back -- which is
/// true, this process has relayed nothing yet.
#[derive(Clone, PartialEq)]
struct ProxyDomain {
    dir: ChunkDir,
    total: u64,
    target: Arc<str>,
}

impl ProxyDomain {
    /// The chunk a byte offset of this entity is in, clamped to the last one
    /// it has. `max(1)` for the entity of no bytes, which cannot be here at
    /// all -- [`Reader::note`] and [`Reader::promises`] both return before
    /// they would create it.
    fn chunk(&self, at: u64) -> u32 {
        index((at / CHUNK_BYTES).min((self.total.max(1) - 1) / CHUNK_BYTES))
    }

    /// How many chunks the entity is, in the gate's `u64` index space.
    fn chunks(&self) -> u64 {
        self.total.div_ceil(CHUNK_BYTES)
    }
}

/// The chunk store, for the owner.
///
/// Nothing shared, so `advertise` is unreachable; installed on the delivered
/// byte, because that is the only moment a proxied entity is known to be
/// read; a grace once no reader is left. The reclaim is the proxy adapter's
/// own `remove_file` of its own chunk -- the chunk store's delete is
/// `pub(crate)` to `enginefs`, deliberately, so nothing outside it can unlink
/// a torrent piece behind librqbit's back, and there is no have-set here to
/// disagree with.
struct ProxyBacking {
    /// The threads the blocking halves of a pass really ran on.
    ///
    /// A `#[tokio::test]` drives its runtime on the test's own thread, so
    /// "the reactor" is a thread identity there and this is what a test can
    /// read it off. Nothing else can: a `read_dir` and an `unlink` leave no
    /// trace of where they were made, and a test that instead watched for
    /// the pass yielding would be reading a race -- a blocking task that
    /// finishes before its handle is first polled yields nothing. Shared
    /// with [`ProxyRetention::disk_threads`], where the test reads it.
    ///
    /// The shipped build has neither this nor the two calls to it.
    #[cfg(test)]
    disk_threads: Arc<Mutex<Vec<std::thread::ThreadId>>>,
}

impl ProxyBacking {
    /// Record that this is a thread a pass did disk work on.
    #[cfg(test)]
    fn note_disk_thread(&self) {
        if let Ok(mut threads) = self.disk_threads.lock() {
            threads.push(std::thread::current().id());
        }
    }

    #[cfg(not(test))]
    fn note_disk_thread(&self) {}
}

impl Backing for ProxyBacking {
    /// The entity's own directory -- which is what says *which* chunk index
    /// space the window is over, since two entities under one cache key
    /// index their chunks the same way and hold different bytes.
    type Key = PathBuf;
    /// The absolute byte offset of a delivered byte.
    type Position = u64;
    type Domain = ProxyDomain;
    /// Nothing installs from outside: the delivered byte does it.
    type Want = ();
    /// The pass needs nothing handed to it that the domain does not carry.
    type Store = ();
    /// A proxied response is not seeded: the whole budget is window.
    const SHARE: Share = Share::Nothing;
    /// The byte that grows the entity is the byte that measures it.
    const TRIGGER: Trigger = Trigger::OnMove {
        passes_per_window: PASSES_PER_WINDOW,
    };
    const INSTALL: Install = Install::OnDeliveredByte;
    const LIVENESS: Liveness = Liveness::Grace(IDLE);

    /// Nothing calls [`Retention::install`] on the proxy, and `()` names no
    /// directory to resolve.
    async fn resolve(&self, (): ()) -> Option<ProxyDomain> {
        debug_assert!(
            false,
            "the proxy installs on the delivered byte, never by install()"
        );
        None
    }

    fn governs(_domain: &ProxyDomain, (): ()) -> bool {
        true
    }

    fn extent(domain: &ProxyDomain) -> Range<u32> {
        0..index(domain.chunks())
    }

    /// The policy for the entity under `budget` bytes. An entity of more
    /// chunks than a `u32` can index is an error and not a clamp: a policy
    /// over a truncated index space would draw its window over the wrong
    /// chunks, so it is left to the cleaner, which counts bytes.
    fn policy(domain: &ProxyDomain, budget: u64) -> anyhow::Result<RetentionPolicy> {
        let chunks = u32::try_from(domain.chunks())
            .context("an entity of more chunks than a retention policy can index")?;
        RetentionPolicy::new(budget, CHUNK_BYTES, 0..chunks, domain.total, Share::Nothing)
    }

    fn index_of(domain: &ProxyDomain, at: u64) -> Option<u32> {
        Some(domain.chunk(at))
    }

    /// The proxy has no pins. The nearest thing is the promise, and that is
    /// per reader and the owner's own.
    fn keeps_everything(&self, _key: &PathBuf) -> bool {
        false
    }

    /// The listing, on the blocking pool: one `read_dir` of the entity's
    /// directory and one more per thousand chunks, which on the flash of a
    /// television is whatever the device says it is. The same listing goes
    /// there from [`ProxyRetention::window`] when a panel asks what a stream
    /// holds; this is the other caller of it.
    ///
    /// A chunk index too big for the policy's index space is one the policy
    /// was never built over -- see [`Self::policy`], which refuses to build
    /// one at all in that case -- so the filter cannot narrow a window that
    /// exists. A directory that would not list is a listing we do not have,
    /// and the pass concludes nothing rather than advance over an empty
    /// reading of a directory that is not empty -- which is what a listing
    /// error used to arrive as, and what had the policy withdraw every
    /// committed chunk on one tick and reclaim them on the next (see
    /// `ChunkDir::held_in_bucket`).
    async fn held(&self, _store: &(), domain: &ProxyDomain) -> Option<BTreeSet<u32>> {
        let dir = domain.dir.clone();
        let backing = self.probe();
        let listing = tokio::task::spawn_blocking(move || {
            backing.note_disk_thread();
            dir.held().map(|held| {
                held.into_iter()
                    .filter_map(|index| u32::try_from(index).ok())
                    .collect::<BTreeSet<u32>>()
            })
        });
        match listing.await {
            Ok(Ok(held)) => Some(held),
            Ok(Err(error)) => {
                tracing::warn!(
                    dir = %domain.dir.path().display(),
                    error = %error,
                    "the chunk directory could not be listed"
                );
                None
            }
            Err(_) => None,
        }
    }

    async fn advertise(&self, _pieces: Range<u32>, _on: bool) -> anyhow::Result<()> {
        debug_assert!(false, "a Share::Nothing policy has nothing to advertise");
        Ok(())
    }

    /// Every chunk of a proxied entity is that entity's alone.
    async fn alone(&self, _domain: &ProxyDomain, pieces: &[u32]) -> Vec<u32> {
        pieces.to_vec()
    }

    /// The unlinks, on the blocking pool, **and the door with them**. The
    /// door has to be asked at the instant of each unlink and not a moment
    /// before it -- the unlinks take as long as they take, one `unlink` per
    /// chunk of a window on the flash of a television, while playback goes
    /// on delivering bytes and a seek can frame a whole new body over the
    /// run this pass is walking -- so the loop is one thing and goes to the
    /// pool whole. Splitting the asking from the taking is the one
    /// rearrangement of this that would change what a pass deletes. It is
    /// the same refusal the cleaner's own delete makes
    /// ([`ProxyRetention::still_free`], the sibling of this one), for the
    /// same reason: a chunk somebody is inside costs the player a broken
    /// read and the origin the same fetch again, while a chunk left standing
    /// costs a few bytes until the next pass.
    ///
    /// A staged copy is not looked for: proxy staging is anonymous, lives
    /// only inside one `write_whole`, and what a kill leaves is the launch
    /// sweep's. A blocking task already started is not cancelled, so
    /// whatever this has taken off the disk is really gone whether or not
    /// the await returns; a closure that dies reports nothing, which is what
    /// it can vouch for.
    async fn reclaim(
        &self,
        _store: &(),
        domain: &ProxyDomain,
        runs: Vec<Range<u32>>,
        door: Door<Self>,
    ) -> usize {
        let dir = domain.dir.clone();
        let backing = self.probe();
        tokio::task::spawn_blocking(move || {
            backing.note_disk_thread();
            let mut freed = 0usize;
            for index in runs.into_iter().flatten() {
                if door.refuses(index) {
                    continue;
                }
                if std::fs::remove_file(dir.chunk_path(u64::from(index))).is_ok() {
                    freed += 1;
                }
            }
            freed
        })
        .await
        .unwrap_or(0)
    }
}

impl ProxyBacking {
    /// What a blocking closure needs of this to say which thread it ran on:
    /// the probe, and in the shipped build nothing.
    fn probe(&self) -> ProxyBacking {
        ProxyBacking {
            #[cfg(test)]
            disk_threads: self.disk_threads.clone(),
        }
    }
}

/// A chunk index in the policy's `u32` index space. An index a `u32` cannot
/// hold belongs to an entity [`ProxyBacking::policy`] refused to bound, so
/// the clamp never names a chunk a window exists over.
fn index(chunk: u64) -> u32 {
    u32::try_from(chunk).unwrap_or(u32::MAX)
}

/// A chunk range in the `u64` index space the gate and the cleaner speak.
fn span(chunks: Range<u32>) -> Range<u64> {
    u64::from(chunks.start)..u64::from(chunks.end)
}

/// The cell a test writes its interleaving into: see
/// [`ProxyRetention::interleave`].
#[cfg(test)]
type Interleave = Mutex<Option<Arc<dyn Fn() + Send + Sync>>>;

/// The readers of every proxied stream a player is inside, and the policy
/// over each entity.
///
/// A thin driver over the owner: what is here is the spawn of the pass, the
/// disk-work ticket that covers it, and the gate and the panel derived from
/// what the owner holds. The state -- the readers, the policy, the windows,
/// the turn -- is the owner's, and it is the same owner the torrent's
/// `Engine` delegates to.
pub struct ProxyRetention {
    owner: Arc<Retention<ProxyBacking>>,
    /// The passes below while they are running, counted beside the cache's
    /// chunk writes: `crate::proxy_cache::DiskWork`. A pass is spawned and
    /// never joined, so it is the other half of what makes a listing of the
    /// cache root a moving picture.
    ///
    /// **A whole pass, and not the blocking halves of one.** The ticket is
    /// taken in [`ProxyRetention::spawn_pass`] and lives in the task, so it
    /// covers the reading, the listing, the second reading, the unlinks and
    /// the windows going back into the map. Held only round the
    /// `spawn_blocking` calls inside a pass it would say the cache had
    /// stopped moving at the very moments it is deciding what to move.
    work: Arc<crate::proxy_cache::DiskWork>,
    /// The one place a test can be *inside* a pass.
    ///
    /// A pass reads the playheads, lists the entity's directories and
    /// unlinks what no window covers, and what this exists to pin happens
    /// between those: playback moving on while the listing runs, a seek
    /// framing a body over the run being walked. The owner calls it twice
    /// per pass -- after the snapshot and before the listing, and after the
    /// decision and before the unlinks -- with the turn held and no lock of
    /// the owner's held, so the closure may deliver a byte, promise, or
    /// publish a budget. What it guarantees is a position, not an
    /// exclusion: a pass is a task with suspension points between its
    /// steps, and anything the runtime is carrying may run there too.
    ///
    /// The shipped build has neither this nor the runner that reads it.
    #[cfg(test)]
    interleave: Arc<Interleave>,
    /// How many passes have run here, for the tests that bound them.
    ///
    /// A pass can owe another (the owner's `again`), so the passes of one
    /// entity are a chain, and what says a chain terminates is a count
    /// rather than a clock: one that arms itself off a term no pass moves
    /// runs tens of thousands of times a second, and is over any honest
    /// bound long before a timeout would notice.
    #[cfg(test)]
    passes: AtomicU64,
    /// See [`ProxyBacking::disk_threads`].
    #[cfg(test)]
    disk_threads: Arc<Mutex<Vec<std::thread::ThreadId>>>,
}

/// One open read: a handle that holds its promise and carries its playhead.
///
/// Dropping it is what says the read is over -- the body ended, or the
/// client went away -- and it is the only thing that does.
pub struct Reader {
    retention: Arc<ProxyRetention>,
    inner: enginefs::retention::owner::Reader<ProxyBacking>,
    key: PathBuf,
    total: u64,
}

impl ProxyRetention {
    pub fn new(budget: Arc<RetentionBudget>, work: Arc<crate::proxy_cache::DiskWork>) -> Self {
        #[cfg(test)]
        let disk_threads: Arc<Mutex<Vec<std::thread::ThreadId>>> = Arc::default();
        let owner = Retention::new(
            Arc::new(ProxyBacking {
                #[cfg(test)]
                disk_threads: disk_threads.clone(),
            }),
            budget,
        );
        #[cfg(test)]
        let interleave: Arc<Interleave> = Arc::default();
        #[cfg(test)]
        owner.hook({
            let interleave = interleave.clone();
            // Cloned out and released before the call, so the closure may
            // take any lock this module or the owner has.
            move || {
                let hook = interleave
                    .lock()
                    .ok()
                    .and_then(|hook| hook.as_ref().cloned());
                if let Some(hook) = hook {
                    hook();
                }
            }
        });
        Self {
            owner,
            work,
            #[cfg(test)]
            interleave,
            #[cfg(test)]
            passes: AtomicU64::new(0),
            #[cfg(test)]
            disk_threads,
        }
    }

    /// Open a reader on the entity in `dir`, whose length is `total`.
    ///
    /// It records nothing by itself: a reader that never promises and never
    /// delivers a byte is a reader nothing has observed, and it holds
    /// nothing.
    pub fn reader(self: &Arc<Self>, dir: &ChunkDir, total: u64, target: Arc<str>) -> Reader {
        let key = dir.path().to_path_buf();
        Reader {
            retention: self.clone(),
            inner: self.owner.reader(
                key.clone(),
                ProxyDomain {
                    dir: dir.clone(),
                    total,
                    target,
                },
            ),
            key,
            total,
        }
    }

    /// Run the pass `claim` is for, and every pass it says it owes, as one
    /// task.
    ///
    /// **The ticket is taken here and lives in the task**, so it stands for
    /// the whole chain and not for the `spawn_blocking` calls inside one
    /// pass. That is what `ServerHandle::proxy_cache_settled` means by the
    /// cache having stopped moving: a pass that has listed the directory and
    /// not yet unlinked anything is a cache that is about to move, and a
    /// wait that returned there would answer a test with a directory the
    /// pass is still deciding about.
    ///
    /// The chain is the owner's `again`: a byte delivered while a pass held
    /// the turn started nothing, and if it was the last of its body nothing
    /// else remembers that it wanted a pass, so the pass that swallowed it
    /// hands its claim back and this loop runs the next one. It terminates
    /// because each arming is paid for by one such byte -- see
    /// `owes_a_pass` in the owner.
    fn spawn_pass(self: &Arc<Self>, key: PathBuf, claim: Claim) {
        let retention = self.clone();
        let ticket = self.work.start();
        tokio::spawn(async move {
            let _ticket = ticket;
            let mut next = Some(claim);
            while let Some(claim) = next {
                #[cfg(test)]
                retention.passes.fetch_add(1, Ordering::Relaxed);
                let outcome = retention.owner.pass(&key, &(), claim).await;
                if let Some(conclusion) = &outcome.concluded
                    && conclusion.reclaimed > 0
                {
                    tracing::debug!(
                        dir = %key.display(),
                        freed = conclusion.reclaimed,
                        windows = ?conclusion.windows,
                        "proxy retention pass"
                    );
                }
                next = outcome.again;
            }
        });
    }

    /// What this cache holds of the stream `target` names, split at the
    /// playhead -- the proxy half of `crate::stream_numbers`.
    ///
    /// `None` is "nothing here is about that URL", and it covers three
    /// different truths that a client shows the same way, by drawing no
    /// row: no reader of this process has ever been opened on that target,
    /// no byte of it has reached a player yet so there is no playhead to
    /// split at, and -- the case that is a policy statement rather than an
    /// absence -- nothing is *bounding* this entity, because the budget
    /// covers it or no budget has been published. What is on the disk then
    /// is not a window, it is whatever the cleaner has not yet aged out,
    /// and putting that under the same label would give one row two
    /// meanings.
    ///
    /// Two entities can carry one target -- the key covers the player
    /// headers that reach the origin too -- so the most recently read of
    /// them answers: that is the one a player is inside now.
    ///
    /// Blocking: it lists the entity's bucket directories, one `getdents`
    /// per thousand chunks. Call it off the reactor. No lock is held across
    /// the listing.
    pub fn window(&self, target: &str) -> Option<enginefs::retention::CacheWindow> {
        let holding = self
            .owner
            .holdings()
            .into_iter()
            .map(|(_, holding)| holding)
            .filter(|holding| &*holding.domain.target == target && holding.installed.is_some())
            .max_by_key(|holding| holding.last_seen)?;
        let at = holding.last_position? / CHUNK_BYTES;
        let dir = holding.domain.dir;
        let mut window = enginefs::retention::CacheWindow::default();
        // No listing, no window: a panel shown an empty window would be
        // shown a measurement nobody made.
        for index in dir.held().ok()? {
            // The chunk the playhead is in counts as ahead: it is the one a
            // player is reading out of, not one it has passed.
            let half = if index < at {
                &mut window.behind_bytes
            } else {
                &mut window.ahead_bytes
            };
            *half = half.saturating_add(CHUNK_BYTES);
        }
        Some(window)
    }

    /// How many open bodies this cache is answering: reads that have
    /// promised chunks or delivered a byte and have not ended.
    ///
    /// The gate's own reason for refusing the cleaner a byte, counted. It
    /// is *not* `crate::proxy_streams::ProxyStreams::live`, which counts
    /// what a client can close by token and lets its registration go the
    /// instant the body ends -- while the read itself, and the window and
    /// promise it holds, live on until hyper drops the response. Anything
    /// asking "is anybody inside these bytes" has to ask this one.
    pub fn reads(&self) -> usize {
        self.owner.readers()
    }

    /// Whether this path is still outside every live window, asked at the
    /// instant of the unlink rather than read from a gate.
    ///
    /// **The second asking, and the sibling of the torrent side's.** The
    /// gate the cleaner carries was filled before a walkdir over the whole
    /// root -- sixteen thousand files on the television that prompted the
    /// debounce -- and before every delete ahead of this one. A reader that
    /// seeks in that time promises chunks the snapshot says are free, and
    /// unlinking one costs the player a broken read and the origin the same
    /// fetch again, which are the two things a cache is for. So the promise
    /// is a refusal at the door and not only a reading taken at the start.
    ///
    /// `true` for anything this has no opinion about: a path that is not a
    /// chunk name, an entity nothing is reading. The cleaner's own rules
    /// decide those, as they did before.
    pub fn still_free(&self, path: &std::path::Path) -> bool {
        let mut gate = ReclaimGate::default();
        self.fill_gate(&mut gate);
        gate.releases_file(path)
    }

    /// Tell the cache cleaner's gate what live readers are holding, and
    /// forget the entities nothing is reading any more.
    ///
    /// Two things are inserted and they are different claims. A **window**
    /// is where a playhead is and what playback is about to want; a
    /// **promise** is bytes an open body has already been framed to
    /// deliver, which is not a policy question at all. The cleaner asks one
    /// gate about everything it walks, so both are put where it can read
    /// them.
    ///
    /// The pruning is the owner's, on this call, because this is the one
    /// call that happens once per cleaner pass rather than once per
    /// delivered chunk, and that cadence is enough on its own: writing a
    /// chunk is a filesystem event, and a filesystem event under the cache
    /// root is what arms the cleaner's debounce, so the case where entries
    /// are being added is exactly the case where this runs often. A
    /// reader's entry goes with the body it belongs to; an entity's goes
    /// once no reader is open on it and [`IDLE`] has passed since its last
    /// delivered byte.
    pub fn fill_gate(&self, gate: &mut ReclaimGate) {
        self.fill_holdings(gate, self.owner.holdings());
    }

    /// [`Self::fill_gate`] against a clock the test supplies: the grace is a
    /// minute and a half, and a test that slept through it would be a test
    /// of nothing but the runner's patience.
    #[cfg(test)]
    fn fill_gate_at(&self, gate: &mut ReclaimGate, now: Instant) {
        self.fill_holdings(gate, self.owner.holdings_at(now));
    }

    fn fill_holdings(
        &self,
        gate: &mut ReclaimGate,
        holdings: Vec<(PathBuf, enginefs::retention::owner::Holding<ProxyBacking>)>,
    ) {
        for (dir, holding) in holdings {
            for window in &holding.windows {
                gate.insert_window(dir.clone(), span(window.clone()));
            }
            // A reader that is inside these bytes and has no window round it
            // yet is inside all of them: either nothing here reclaims
            // anything (the budget covers the entity, the volume has no cap,
            // no pass has published one) or no pass has run since the byte
            // that made this a playhead. Both are "we have measured nothing
            // to give up", and what a live reader may not lose is the chunk
            // under its head. Once the last reader has gone, what stands is
            // the windows a pass really chose, for the grace above -- a
            // stream nobody is reading is the first thing that should go.
            if (holding.installed.is_none() || holding.windows.is_empty()) && holding.live_playhead
            {
                gate.insert_window(dir.clone(), 0..holding.domain.chunks());
            }
            for promised in holding.promised {
                gate.insert_window(dir.clone(), span(promised));
            }
        }
    }
}

impl Reader {
    /// This read will deliver chunks `chunks` off the disk, and until it
    /// has, nothing may unlink them.
    ///
    /// Called with what a lookup found and a response is being framed
    /// around; the range shrinks from the front as [`Self::note`] reports
    /// the bytes going out, and is released whole when this handle is
    /// dropped.
    pub fn promises(&self, chunks: Range<u64>) {
        if self.total == 0 || chunks.is_empty() {
            return;
        }
        self.inner.promises(index(chunks.start)..index(chunks.end));
    }

    /// A byte at `delivered_to` of this entity has reached a player.
    ///
    /// This is the whole of what makes a proxied stream's playhead exist.
    /// Cheap enough for the body path -- a lock, a hash lookup and a
    /// comparison -- and it starts a pass only when the playhead has moved a
    /// stride and no pass holds the entity's turn; a byte that finds the
    /// turn taken is remembered by the pass that holds it, which asks the
    /// same head again when it concludes.
    pub fn note(&self, delivered_to: u64) {
        if self.total == 0 {
            return;
        }
        let delivered_to = delivered_to.min(self.total - 1);
        if let Some(claim) = self.inner.note(delivered_to) {
            self.retention.spawn_pass(self.key.clone(), claim);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A promise made after the cleaner took its reading still refuses the
    /// unlink.
    ///
    /// The gate a pass carries is filled once, before a walkdir over the
    /// whole root -- sixteen thousand files on the television that prompted
    /// the debounce -- and before every delete ahead of this one. A reader
    /// that seeks in that time promises chunks the reading calls free, and
    /// unlinking one costs the player a broken read and the origin the same
    /// fetch again, which are the two things a cache is for.
    ///
    /// The torrent half has always re-asked: its delete goes to the engine,
    /// which consults the live policy under the lock. This is the sibling,
    /// and it was missing -- the promise was a snapshot rather than a
    /// refusal at the door.
    #[tokio::test]
    async fn a_promise_made_since_the_reading_still_refuses_the_unlink() {
        let tmp = tempfile::tempdir().expect("a scratch root");
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, [4]);
        let path = dir.chunk_path(4);
        let retention = retention(Some(CHUNK_BYTES));

        // The reading the cleaner would take: nothing is promised, so the
        // chunk is free.
        let mut reading = ReclaimGate::default();
        retention.fill_gate(&mut reading);
        assert!(
            reading.releases_file(&path),
            "with nothing open, the chunk is ordinary cache"
        );
        assert!(retention.still_free(&path));

        // A reader opens and promises that very chunk, which is what a seek
        // does while the walk is still running.
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.promises(4..5);

        assert!(
            reading.releases_file(&path),
            "the reading is a snapshot and cannot know: this is the defect"
        );
        assert!(
            !retention.still_free(&path),
            "but asked now, the promise refuses the unlink"
        );
    }

    /// A 4 MiB entity: sixteen chunks.
    const TOTAL: u64 = 16 * CHUNK_BYTES;

    /// The origin URL these readers are of. Only [`ProxyRetention::window`]
    /// reads it back; everything else here is keyed by the entity's own
    /// directory.
    const TARGET: &str = "https://origin.example/film.mkv";

    fn retention(limit: Option<u64>) -> Arc<ProxyRetention> {
        let budget = Arc::new(RetentionBudget::default());
        if let Some(limit) = limit {
            budget.set(Some(limit));
        }
        Arc::new(ProxyRetention::new(budget, Arc::default()))
    }

    fn write_chunks(dir: &ChunkDir, indices: impl IntoIterator<Item = u64>) {
        let bytes = vec![0u8; CHUNK_BYTES as usize];
        for index in indices {
            dir.write_whole(index, &bytes, Some(CHUNK_BYTES))
                .expect("a chunk");
        }
    }

    /// Wait for the passes a `note` started to be over and to have got
    /// somewhere -- `what` says where, and is what a failure is reported as.
    /// Bounded so a regression fails instead of hanging, and generously,
    /// because the bound is not the assertion.
    ///
    /// **Both halves, and the first one is not decoration.** What a pass
    /// does to the disk it does from inside itself, and what it concludes
    /// -- the windows back in the map, the throttle rearmed, the next pass
    /// armed -- it does after that, past a suspension point, since the
    /// unlinks are awaited on the blocking pool. So a file that has gone is
    /// not a pass that has finished, and a test that read the gate on the
    /// strength of one would be reading the *previous* pass's windows
    /// beside this pass's disk. `DiskWork` counts a whole pass, which is
    /// what `ServerHandle::proxy_cache_settled` is for, so a count of
    /// nothing beside the condition is a cache that has stopped moving and
    /// has said where it stopped.
    async fn settled(
        retention: &Arc<ProxyRetention>,
        what: &str,
        until: impl Fn(&ReclaimGate) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let mut gate = ReclaimGate::default();
            retention.fill_gate(&mut gate);
            if retention.work.idle() && until(&gate) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the retention passes never got there: {what}");
    }

    /// Wait for the chain of passes over one entity to stop, and say how
    /// many of them ran.
    ///
    /// **The assertion is the count.** `at_most` is what the shape under
    /// test can honestly need, and it is checked while the passes are
    /// running rather than after they have stopped, because a chain that
    /// arms itself off a term no pass moves has no "after": it runs tens of
    /// thousands of times a second for as long as the process lives. So a
    /// regression here fails on the number of passes it ran, and the sleep
    /// below is only what lets them run at all.
    async fn passes_stop(retention: &Arc<ProxyRetention>, at_most: u64, what: &str) -> u64 {
        loop {
            let ran = retention.passes.load(Ordering::Relaxed);
            assert!(
                ran <= at_most,
                "{what}: {ran} passes have run where {at_most} is the most \
                 this shape can need, so the arming chain does not terminate"
            );
            if retention.work.idle() {
                return ran;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Deliver a byte from inside the next pass, once, and say whether the
    /// reader that delivered it ends there.
    ///
    /// This is the case [`Reader::note`] swallows: a pass is in flight, so
    /// the byte starts no pass of its own. What it does do is move the
    /// entity's last delivered byte away from the playhead the running pass
    /// is measuring, and that is the whole of what the two tests below are
    /// about.
    fn deliver_during_the_next_pass(
        retention: &Arc<ProxyRetention>,
        reader: Reader,
        at: u64,
        and_end: bool,
    ) -> Arc<Mutex<Option<Reader>>> {
        let held = Arc::new(Mutex::new(Some(reader)));
        let once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook = {
            let held = held.clone();
            move || {
                if once.swap(true, Ordering::Relaxed) {
                    return;
                }
                let mut slot = held.lock().expect("the reader to deliver from");
                if and_end {
                    // The range request to the end of the file closes on its
                    // last byte, which is what leaves the entity's last
                    // delivered byte at the end of the film with nothing
                    // open there.
                    if let Some(reader) = slot.take() {
                        reader.note(at);
                    }
                } else if let Some(reader) = slot.as_ref() {
                    reader.note(at);
                }
            }
        };
        *retention.interleave.lock().expect("the interleave slot") = Some(Arc::new(hook));
        held
    }

    /// **The playhead is an observation, and there is none at process
    /// start.**
    ///
    /// The failure this rules out is the one the freshness invariant is
    /// about: a position invented from what happens to be on disk, read back
    /// as though a player had been there. A cache directory says which bytes
    /// were fetched, by this process or a previous one, and nothing at all
    /// about where anybody had got to in them.
    #[test]
    fn a_stream_nobody_has_read_a_byte_of_has_no_window() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(4 * CHUNK_BYTES));
        // A reader is *open* on it -- the lookup found bytes -- and has
        // delivered nothing and promised nothing. That is not a playhead,
        // and it is not a window either.
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        for index in 0..16u64 {
            assert!(
                gate.releases_file(&dir.chunk_path(index)),
                "chunk {index} is cache: nothing has been played out of this entity"
            );
        }
        drop(reader);
    }

    /// **What a panel is shown is what is on the disk, split where a byte
    /// really reached a player.**
    ///
    /// Not the extent the policy intends to fill: the ahead half is
    /// read-ahead that has *arrived*, and a proxied entity only ever has
    /// what the origin has relayed so far.
    #[tokio::test]
    async fn the_window_a_panel_shows_is_the_disk_split_at_the_playhead() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        // Chunks three to nine: everything a player has fetched of a
        // sixteen-chunk entity so far.
        write_chunks(&dir, 3..10);

        // Twelve chunks of budget over sixteen, so a policy is installed --
        // and a playhead whose window covers every chunk on the disk, so
        // this measures the reading rather than a race with the pass.
        let retention = retention(Some(12 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(4 * CHUNK_BYTES + 5);

        assert_eq!(
            retention.window(TARGET),
            Some(enginefs::retention::CacheWindow {
                behind_bytes: CHUNK_BYTES,
                ahead_bytes: 6 * CHUNK_BYTES,
            }),
            "chunk three is behind the head; the chunk under it and the five \
             after it are what playback has in hand"
        );
        assert_eq!(
            retention.window("https://origin.example/other-film.mkv"),
            None,
            "and it is the stream that was asked about, not whatever is open"
        );
        drop(reader);
    }

    /// The two absences a panel draws no row for, told apart from a zero.
    ///
    /// A reader that has delivered nothing has no playhead to split at --
    /// the freshness rule this whole module is built on -- and a stream
    /// nothing is *bounding* has no window at all: what is on its disk is
    /// then whatever the cleaner has not yet aged out, which is a different
    /// quantity, and one row cannot honestly carry both.
    #[tokio::test]
    async fn a_stream_with_no_playhead_or_no_policy_has_no_window_to_show() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let bounded = retention(Some(8 * CHUNK_BYTES));
        let opened = bounded.reader(&dir, TOTAL, TARGET.into());
        opened.promises(0..16);
        assert_eq!(
            bounded.window(TARGET),
            None,
            "a body has been framed, but no byte of it has reached a player"
        );
        drop(opened);

        // A budget that covers the entity: nothing here is bounded.
        let covered = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(covered.path().join("entity"));
        write_chunks(&dir, 0..16);
        let unbounded = retention(Some(32 * CHUNK_BYTES));
        let reader = unbounded.reader(&dir, TOTAL, TARGET.into());
        reader.note(4 * CHUNK_BYTES);
        assert_eq!(unbounded.window(TARGET), None);
        drop(reader);
    }

    /// And once a byte has gone out, the window round it is the cleaner's to
    /// leave alone -- while the chunks the playhead has left behind are not.
    #[tokio::test]
    async fn a_window_a_player_is_inside_is_not_the_cleaners_to_take() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Eight chunks of budget over a sixteen-chunk entity: a split, so a
        // window exists at all.
        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(12 * CHUNK_BYTES);
        // Until the first pass has run, the window is the whole entity:
        // nothing has been measured yet and nothing is given up on a guess.
        settled(&retention, "the first pass narrowed the window", |gate| {
            gate.releases_file(&dir.chunk_path(0))
        })
        .await;

        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(12)),
            "the chunk under the player's head"
        );
        assert!(
            gate.releases_file(&dir.chunk_path(0)),
            "and the start of the film, which it played past long ago"
        );
    }

    /// **A budget that covers the entity is not a player nobody protects.**
    ///
    /// No policy is installed when there is nothing to reclaim, and the hole
    /// that left was in the *other* answer: with no policy there was no
    /// window, so the gate released every chunk of a stream a player was
    /// inside. The cleaner's cap is a per-volume number -- a 500 MB episode
    /// under a 10 GB cap with 12 GB of other cache beside it is the ordinary
    /// case -- so "the budget covers this entity" says nothing whatever
    /// about whether the cleaner is about to take it.
    #[tokio::test]
    async fn a_budget_that_covers_the_entity_still_holds_the_chunk_under_the_head() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Thirty-two chunks of budget over a sixteen-chunk entity.
        let retention = retention(Some(32 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(12 * CHUNK_BYTES);

        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(12)),
            "the chunk under the player's head is not the cleaner's to take"
        );
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "and neither is the read-ahead: all of it fits, so all of it is the window"
        );
        assert_eq!(dir.held().unwrap().len(), 16, "and nothing was reclaimed");
    }

    /// A budget nobody has published is not a budget of nothing.
    ///
    /// Before the first cache pass this process has not been told what the
    /// volume may hold. Reading that as zero would have the first proxied
    /// stream of every boot reclaim every chunk behind its playhead before
    /// anything had measured the disk -- and reading it as "no protection
    /// either" would have the cleaner's first pass, which is the very pass
    /// that publishes the budget, free to take the chunk under the head.
    #[tokio::test]
    async fn nothing_is_reclaimed_before_a_budget_has_been_published() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(None);
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(15 * CHUNK_BYTES);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(dir.held().unwrap().len(), 16, "every chunk is still here");
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "and a player is inside it, so the cleaner is not offered it either"
        );
    }

    /// **An open read holds what it has promised, and the window does not
    /// overrule it.**
    ///
    /// A response is framed before its first byte goes out, so the chunks it
    /// has yet to deliver are already promised to the player. The window is
    /// 90% ahead of the playhead and a body can be longer than that -- a
    /// read of a whole window's worth from its first chunk has its own tail
    /// outside the window by the tenth that sits behind it, and the run the
    /// window itself kept and a player then asks for whole is exactly that
    /// shape. Unlink one of those and the player gets a body that ends early
    /// under a `Content-Length` that said otherwise.
    ///
    /// The pass here really is reclaiming -- the two chunks nothing promised
    /// go -- which is what says the promise is being honoured rather than
    /// the pass having quietly done nothing.
    #[tokio::test]
    async fn a_chunk_an_open_read_has_promised_is_not_reclaimed_under_it() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Ten chunks of budget, and a body framed around fourteen of the
        // sixteen: four of what it promised are outside the window at its
        // first chunk, and two chunks are promised by nobody.
        let retention = retention(Some(10 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.promises(0..14);
        reader.note(0);
        settled(&retention, "a pass reclaimed what nobody promised", |_| {
            !dir.chunk_path(15).exists()
        })
        .await;

        for index in 0..14u64 {
            assert!(
                dir.chunk_path(index).is_file(),
                "chunk {index} was promised to an open body and the pass took it"
            );
        }
        assert!(
            !dir.chunk_path(14).exists(),
            "while the chunks nothing promised, outside the window, went"
        );
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        for index in 0..14u64 {
            assert!(
                !gate.releases_file(&dir.chunk_path(index)),
                "chunk {index} is promised to an open body, so it is not the cleaner's either"
            );
        }

        // The promise shrinks behind the bytes as they go out, and what it
        // lets go of is the window's to reclaim like anything else.
        for chunk in 0..13u64 {
            reader.note(chunk * CHUNK_BYTES + CHUNK_BYTES - 1);
        }
        settled(
            &retention,
            "the head of the film left the promise and the window",
            |_| !dir.chunk_path(0).exists(),
        )
        .await;
        assert!(
            !dir.chunk_path(0).exists(),
            "the head of the film is neither promised nor in the window any more"
        );
        assert!(
            dir.chunk_path(13).is_file(),
            "and the last chunk the body still owes its player is here"
        );
    }

    /// **Two players in one entity are two windows, not one that alternates.**
    ///
    /// `p=` is out of the cache key on purpose, so two players share one
    /// entity's chunks and read it at two offsets. One playhead per entity
    /// made the second player's read-ahead whatever the first player's
    /// window had just left behind, in alternation; a window is a statement
    /// about a playhead, and there are two of them here.
    #[tokio::test]
    async fn two_players_in_one_entity_each_get_a_window() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Four chunks of budget: two windows of four cannot both be the
        // whole entity, so this really is two windows and not one big one.
        let retention = retention(Some(4 * CHUNK_BYTES));
        let one = retention.reader(&dir, TOTAL, TARGET.into());
        let two = retention.reader(&dir, TOTAL, TARGET.into());
        one.note(0);
        settled(&retention, "the first player's pass ran", |gate| {
            gate.releases_file(&dir.chunk_path(15))
        })
        .await;
        two.note(12 * CHUNK_BYTES);
        settled(&retention, "the second player's pass ran", |gate| {
            !gate.releases_file(&dir.chunk_path(12))
        })
        .await;

        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(0)),
            "the first player's head, which the second player's pass ran over"
        );
        assert!(
            !gate.releases_file(&dir.chunk_path(12)),
            "and the second player's own"
        );
        assert!(
            dir.chunk_path(0).is_file(),
            "and the first player's chunk is still on the disk"
        );
    }

    /// **A pass arms another pass off its own reader having moved, and a
    /// byte some other player delivered is not that.**
    ///
    /// [`LiveStream::heads`] asks where the reader this pass is *about* has
    /// got to, and falls back to the entity's last delivered byte only when
    /// that read has ended. The arming question has to be the same question,
    /// because a pass moves neither term of it: asked of the entity's last
    /// delivered byte while the pass measured a live reader's own playhead,
    /// the distance between them is whatever the other player is doing, no
    /// pass changes it, and every pass arms the next. Two players a stride
    /// apart is an ordinary shape -- `p=` is out of the cache key so that
    /// they share these chunks -- and it ran the listing and the unlinks of
    /// a whole entity tens of thousands of times a second, for as long as
    /// both bodies were open.
    #[tokio::test]
    async fn a_pass_does_not_arm_itself_off_a_byte_another_player_delivered() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Four chunks of budget over sixteen: a policy is installed and a
        // stride is one chunk, so twelve chunks apart is many strides.
        let retention = retention(Some(4 * CHUNK_BYTES));
        let one = retention.reader(&dir, TOTAL, TARGET.into());
        let two = deliver_during_the_next_pass(
            &retention,
            retention.reader(&dir, TOTAL, TARGET.into()),
            12 * CHUNK_BYTES,
            false,
        );

        one.note(0);
        let ran = passes_stop(
            &retention,
            4,
            "the first player's pass, with the second player's byte behind it",
        )
        .await;
        assert!(ran >= 1, "the note really did start a pass");

        assert!(
            dir.chunk_path(0).is_file(),
            "the first player's own chunk is where the pass left it"
        );
        assert!(
            dir.chunk_path(12).is_file(),
            "and the second player's, which the pass was told about"
        );
        drop(two);
    }

    /// **A player that paused after a read of the end of the file is not a
    /// player that is moving.**
    ///
    /// The shape needs no second live reader at all. A progressive MP4 has
    /// its `moov` atom at the end, so a player asks for a few kilobytes
    /// there before it plays anything, and that request closes -- leaving
    /// the entity's last delivered byte at the end of the film with nothing
    /// open there, while the body that is actually playing sits where
    /// playback is. Then the person pauses, and nothing delivers another
    /// byte of this entity ever again.
    ///
    /// Asked of the entity's last delivered byte, the pass round the paused
    /// body found the end of the film a hundred chunks away, armed itself,
    /// measured the same two numbers and armed itself again -- a pegged core
    /// and a listing plus an `unlink` attempt per chunk against the flash of
    /// a television, on an idle stream. It is also what
    /// `ServerHandle::proxy_cache_settled` waits for, so nothing that waits
    /// on the proxy cache to stop moving could ever return.
    #[tokio::test]
    async fn a_paused_player_does_not_arm_passes_off_a_tail_read_that_ended() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(4 * CHUNK_BYTES));
        let playing = retention.reader(&dir, TOTAL, TARGET.into());
        let tail = deliver_during_the_next_pass(
            &retention,
            retention.reader(&dir, TOTAL, TARGET.into()),
            TOTAL - 1,
            true,
        );

        playing.note(3 * CHUNK_BYTES);
        let ran = passes_stop(
            &retention,
            4,
            "the paused player's pass, with a tail read that ended behind it",
        )
        .await;
        assert!(ran >= 1, "the note really did start a pass");
        assert!(
            tail.lock().unwrap().is_none(),
            "and the read of the end of the film really did end inside it"
        );
        assert!(
            dir.chunk_path(3).is_file(),
            "the chunk the paused player is inside is where the pass left it"
        );
    }

    /// **A pass that lands after its read has ended still puts the window
    /// where playback stopped.**
    ///
    /// A request ends the moment its last chunk goes out, and the pass that
    /// byte started is still in flight -- so a pass running with no
    /// reader left is the ordinary case and not a corner. Where playback got
    /// to is still the last thing that happened to this entity, and the
    /// window belongs round there for the [`IDLE`] grace, because the
    /// player's next request is a moment away and it will ask for the bytes
    /// either side of that point. A pass that shrugged at a reader it could
    /// not find would leave the window wherever the *previous* one put it,
    /// which is where playback was and not where it stopped.
    ///
    /// The read ends from inside the pass its last byte started -- the hook
    /// drops the reader before the listing -- so the pass measures with no
    /// reader left, which is the shape a request's last chunk always makes.
    #[tokio::test]
    async fn a_pass_that_lands_after_its_read_has_ended_puts_the_window_where_playback_stopped() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(0);
        settled(
            &retention,
            "the window settled at the head of the film",
            |_| !dir.chunk_path(15).exists(),
        )
        .await;
        // Playback goes on to the end of the film, filling as it goes.
        write_chunks(&dir, 8..16);

        // The request ends with its last byte: the pass that byte started
        // finds the reader gone before it has listed anything.
        let ended = Arc::new(Mutex::new(Some(reader)));
        let into_hook = ended.clone();
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            drop(into_hook.lock().unwrap().take());
        }));
        ended
            .lock()
            .unwrap()
            .as_ref()
            .expect("the reader is still open")
            .note(15 * CHUNK_BYTES);
        settled(
            &retention,
            "the pass the last byte started put the window where playback stopped",
            |_| !dir.chunk_path(0).exists(),
        )
        .await;
        assert!(
            ended.lock().unwrap().is_none(),
            "and the read really did end inside that pass"
        );

        assert!(
            dir.chunk_path(15).is_file(),
            "the chunk playback stopped on is still here"
        );
        assert!(
            !dir.chunk_path(0).exists(),
            "and the head of the film, a window behind it, is not"
        );
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "the cleaner is refused the bytes the player's next request will ask for"
        );
        assert!(gate.releases_file(&dir.chunk_path(0)));
    }

    /// **A budget published while a pass was running is the one that
    /// holds.**
    ///
    /// A pass has the policy out of its slot for the length of it, and the
    /// cleaner publishes a budget after every walk -- `min(configured,
    /// occupied + available - floor)`, which moves whenever the volume does,
    /// and which is an absence again the moment the volume cannot be read.
    /// Putting the finishing pass's policy back over that would go on
    /// reclaiming to a cap nobody has published, which is the one thing
    /// [`CacheBudget::Unbounded`] says not to do -- and nothing would ever
    /// rebuild it, because `decided` already says the new budget.
    ///
    /// The change lands from inside the pass -- the hook publishes it while
    /// the pass is between its snapshot and its listing -- because that is
    /// the only way to be inside the window at all.
    #[tokio::test]
    async fn a_budget_published_while_a_pass_was_running_is_the_one_that_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(12 * CHUNK_BYTES));
        let retention = Arc::new(ProxyRetention::new(budget.clone(), Arc::default()));
        let reader = Arc::new(retention.reader(&dir, TOTAL, TARGET.into()));
        reader.note(0);
        settled(&retention, "the published cap was applied", |_| {
            dir.held().unwrap().len() <= 12
        })
        .await;
        // Playback fills what it passes over, as it does.
        write_chunks(&dir, 0..16);

        // While the next pass is working: the cleaner's walk finds no cap at
        // all -- no `cacheSize` set and a volume whose free space it could
        // not read -- and a byte goes out under it.
        let into_hook = reader.clone();
        let published = budget.clone();
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            published.set(None);
            into_hook.note(0);
        }));
        // Playing on a chunk is what starts the pass over the old cap.
        reader.note(CHUNK_BYTES);
        settled(
            &retention,
            "the pass that measured the old cap is over",
            |_| true,
        )
        .await;

        // Nothing bounds this entity now, so playing on reclaims none of it.
        reader.note(2 * CHUNK_BYTES);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            dir.held().unwrap().len(),
            16,
            "a cap nobody has published is not a cap to evict against: {:?}",
            dir.held().unwrap()
        );
    }

    /// **A pass reclaims against where playback is, not where it was when
    /// the pass began.**
    ///
    /// A pass reads the playheads, lists the entity's directories and
    /// unlinks; the listing is the slow half, and a fill relaying at twenty
    /// megabytes a second writes a whole window of chunks while it runs. A
    /// window read before the listing therefore sat a window *behind* what
    /// the listing found, so the pass reclaimed the read-ahead the fill had
    /// just written -- and the next pass, at the playhead that had by then
    /// caught up with it, reclaimed everything left behind. Two passes and
    /// an empty cache: measured on a loaded machine, a sixteen-megabyte
    /// read left nothing at all on the disk under an eight-megabyte budget,
    /// and the panel then showed a window of zero bytes for a film that had
    /// just been played.
    ///
    /// So the playheads are read after the listing, and asked about again
    /// at each unlink -- the same second asking the cleaner's own delete
    /// makes. Playback moves here at both of those moments, which is what
    /// says both are load-bearing: with either reading taken early, the
    /// chunks it did not see are reclaimed, and the pass that the movement
    /// arms then takes the rest.
    #[tokio::test]
    async fn a_pass_reclaims_round_where_playback_has_got_to_while_it_ran() {
        const CHUNKS: u64 = 32;
        const WHOLE: u64 = CHUNKS * CHUNK_BYTES;
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));

        // Eight chunks of budget over thirty-two: the window is a quarter of
        // the entity, so where it sits is the whole of what is kept.
        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = Arc::new(retention.reader(&dir, WHOLE, TARGET.into()));
        write_chunks(&dir, 0..CHUNKS);

        // Playback while the pass below runs: at the tenth chunk when it
        // lists the directory, at the twentieth by the time it unlinks. The
        // bytes are delivered the way any byte is, and no pass starts for
        // them -- a byte that finds the turn taken starts none, which is
        // exactly the case this is about.
        let moved = std::sync::atomic::AtomicU64::new(0);
        let into_hook = reader.clone();
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            let at = match moved.fetch_add(1, Ordering::Relaxed) {
                0 => 10,
                1 => 20,
                // The pass this movement arms runs after those two, and
                // playback has stopped by then.
                _ => return,
            };
            into_hook.note(at * CHUNK_BYTES);
        }));

        // The pass this starts measures the head of the film -- and playback
        // has left it by the time the pass has listed the directory.
        reader.note(0);
        settled(
            &retention,
            "the pass the movement armed reclaimed round the twentieth chunk",
            |_| !dir.chunk_path(10).exists(),
        )
        .await;

        let held = dir.held().unwrap();
        assert_eq!(
            held,
            (20..28).collect::<BTreeSet<u64>>(),
            "what is on the disk is the window round where playback got to"
        );
        drop(reader);
    }

    /// **Neither half of a pass is the reactor's to run.**
    ///
    /// A pass lists the entity's bucket directories -- one `read_dir`, and
    /// one more per thousand chunks -- and then unlinks a chunk at a time.
    /// On the flash of a television, with nothing in the dentry cache, both
    /// are syscall loops of no bounded length, and the reactor they would
    /// run on is carrying every other request this server is answering.
    /// This is the rule `enginefs::retention`'s `unlink` states, asked of
    /// the other adapter over the same chunks.
    ///
    /// A `#[tokio::test]` drives its runtime on the test's own thread, so
    /// "the reactor" here is a thread identity and not a timing. Read that
    /// way there is no race in it: a `spawn_blocking` closure never runs on
    /// the thread that spawned it, whereas a test that watched for the pass
    /// yielding would be reading one -- a blocking task that finishes
    /// before its handle is first polled yields nothing.
    #[tokio::test]
    async fn neither_the_listing_nor_the_unlinks_of_a_pass_run_on_the_reactor() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Eight chunks of budget over sixteen, so the pass has a directory
        // worth listing and chunks to give back: both halves really run.
        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(0);
        settled(
            &retention,
            "the pass reclaimed the far end of the film",
            |_| !dir.chunk_path(15).exists(),
        )
        .await;

        let reactor = std::thread::current().id();
        let threads = retention.disk_threads.lock().unwrap().clone();
        assert!(
            threads.len() >= 2,
            "a pass takes the listing and the unlinks to the pool as two \
             pieces of work, and {} of them got there: {threads:?}",
            threads.len()
        );
        assert!(
            threads.iter().all(|thread| *thread != reactor),
            "no part of a pass walks the disk on the reactor: {threads:?} \
             against {reactor:?}"
        );
        drop(reader);
    }

    /// **A pass in flight is the cache moving, at every point of it.**
    ///
    /// `ServerHandle::proxy_cache_settled` is how anything outside this
    /// process's own paths asks whether the cache has stopped, and it is a
    /// wait on `proxy_cache::DiskWork` having nothing in it. A pass takes a
    /// ticket for the whole of itself, so a pass that has read the
    /// playheads, or listed the directory, or decided what to reclaim and
    /// not yet unlinked it, all count as work in flight.
    ///
    /// Counted only round the `spawn_blocking` calls inside a pass, the
    /// count would fall to nothing at exactly the moments a pass is deciding
    /// what to delete -- and a test that waited for it would then look at a
    /// directory that is about to lose files, which is the flakiness the
    /// seam was added to end rather than a wait on a condition. The hook is
    /// the one place a test can stand between two steps of a pass, so it is
    /// where this is asked from.
    #[tokio::test]
    async fn a_pass_is_still_disk_work_between_the_listing_and_the_unlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());

        // What the count said at each point of a pass that is not one of
        // its two calls to the disk.
        let seen: Arc<Mutex<Vec<bool>>> = Arc::default();
        let watcher = Arc::downgrade(&retention);
        let into_hook = seen.clone();
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            let Some(retention) = watcher.upgrade() else {
                return;
            };
            into_hook
                .lock()
                .expect("what the count said")
                .push(retention.work.idle());
        }));

        reader.note(0);
        settled(
            &retention,
            "the pass reclaimed the far end of the film",
            |_| !dir.chunk_path(15).exists(),
        )
        .await;

        let seen = seen.lock().unwrap().clone();
        assert!(
            !seen.is_empty(),
            "the note really did start a pass to be inside"
        );
        assert!(
            seen.iter().all(|idle| !idle),
            "a pass between two of its steps is still work the cache has not \
             finished, and the count said it was idle at {} of {} of them",
            seen.iter().filter(|idle| **idle).count(),
            seen.len()
        );
        drop(reader);
    }

    /// **The last byte of a body gets its pass, even though one was already
    /// running.**
    ///
    /// A pass is throttled and it is exclusive: while one is in flight
    /// [`Reader::note`] starts no other, and it does not remember that it
    /// wanted one. For every byte but the last that is right -- the next one
    /// is along in a moment and it brings the trigger with it. The last byte
    /// of a body brings nothing after it, and a body ends while a pass is
    /// running as often as the blocking pool is busy.
    ///
    /// What that left behind was not a fraction of a window. Every chunk
    /// written since the running pass took its listing stayed on the disk,
    /// over the budget, until the cleaner's hourly walk got to it -- and
    /// [`ProxyRetention::fill_gate`] went on answering the cleaner with the
    /// window that pass had measured, which names where the player *was*
    /// rather than where it stopped. So the cleaner was offered the bytes
    /// round the playhead and refused the ones the player had left behind,
    /// which is the grace exactly inside out.
    ///
    /// The last byte lands from inside the pass -- the hook delivers it once
    /// the pass has measured the head of the film and is about to unlink --
    /// because that is the only way to be inside the window at all.
    #[tokio::test]
    async fn the_last_byte_of_a_body_gets_a_pass_even_though_one_was_running() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Eight chunks of budget over sixteen, so a window really is smaller
        // than the entity and a pass really has something to give back.
        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = Arc::new(retention.reader(&dir, TOTAL, TARGET.into()));

        // The player reads the film to its end while the pass its first byte
        // started is working: at the hook's second firing the pass has
        // measured where the player was and not yet unlinked. Its last byte
        // finds the turn taken, so it starts nothing -- and there is no byte
        // after it to try again.
        let fired = std::sync::atomic::AtomicU64::new(0);
        let into_hook = reader.clone();
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            if fired.fetch_add(1, Ordering::Relaxed) == 1 {
                into_hook.note(TOTAL - 1);
            }
        }));
        reader.note(0);

        settled(
            &retention,
            "the pass the swallowed trigger armed reclaimed the head of the film",
            |_| !dir.chunk_path(0).exists(),
        )
        .await;
        assert_eq!(
            retention.passes.load(Ordering::Relaxed),
            2,
            "the pass that swallowed the last byte, and the one it owed for it"
        );
        assert!(
            dir.chunk_path(15).is_file(),
            "the chunk the player stopped inside is still here"
        );
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "and the cleaner is refused it, because that is where the player \
             stopped and where its next request will start"
        );
        assert!(
            gate.releases_file(&dir.chunk_path(0)),
            "while the head of the film, which the window left behind long \
             ago, is the cleaner's for the asking"
        );
        drop(reader);
    }

    /// **The grace is for a stream nobody is reading, and an open reader is
    /// somebody reading.**
    ///
    /// A player that pauses keeps its body open and stops reading it, for as
    /// long as the person is away; nothing about that makes the bytes it has
    /// already been promised anybody else's. So the age of the last
    /// delivered byte decides only when an entity *no reader is open on* is
    /// forgotten.
    ///
    /// The clock is moved by hand rather than waited on -- the grace is a
    /// minute and a half, and a test that slept through it would be a test
    /// of nothing but the runner's patience.
    #[tokio::test]
    async fn a_reader_quiet_longer_than_the_grace_still_holds_what_it_promised() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.promises(0..16);
        reader.note(0);

        // Longer than the grace: an entity nothing was reading would be
        // forgotten here, every byte of it ordinary cache again.
        let mut gate = ReclaimGate::default();
        retention.fill_gate_at(&mut gate, Instant::now() + IDLE + Duration::from_secs(1));
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "the paused player is still owed the tail of the body it has open"
        );
    }

    /// **A pass that dies before its unlinks leaves the policy where it was,
    /// and the next delivered byte's pass reclaims.**
    ///
    /// The pass used to take the policy out of its slot for the length of
    /// itself, so a pass that died at an await -- the blocking pool refusing
    /// a task, a panic in the unlink closure, the runtime shutting down --
    /// walked off with it: the entity was unbounded until the budget's
    /// *value* changed, `running` stayed set so no later pass could start,
    /// and the gate went on protecting the windows of a pass that would
    /// never be superseded. That was documented as shutdown-only, with
    /// nothing to write a test against. Now the policy never leaves its cell
    /// and what a pass holds is a guard on the entity's turn, so this is the
    /// test: a pass aborted while its listing is on the pool leaves a
    /// bounded entity whose turn is free, and the next byte's pass runs.
    ///
    /// The runtime has one blocking thread and the test occupies it, so the
    /// listing cannot start until the test lets it: the pass is parked at
    /// its listing by construction, not by winning a race against the pool.
    #[test]
    fn a_pass_that_dies_before_its_unlinks_leaves_the_policy_where_it_was() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let dir = ChunkDir::new(tmp.path().join("entity"));
            write_chunks(&dir, 0..16);
            let key = dir.path().to_path_buf();

            let retention = retention(Some(8 * CHUNK_BYTES));
            let reader = retention.reader(&dir, TOTAL, TARGET.into());

            // The one blocking thread, busy until the test says otherwise.
            let (release, held_up) = std::sync::mpsc::channel::<()>();
            let occupied = tokio::task::spawn_blocking(move || {
                let _ = held_up.recv();
            });

            // The test holds the turn, so the byte installs the policy and
            // starts nothing; the pass the test runs is the one it would
            // have started.
            let claim = retention
                .owner
                .try_turn(&key)
                .expect("nobody holds the turn yet");
            reader.note(0);
            assert!(
                retention.owner.holding(&key).unwrap().installed.is_some(),
                "the byte installed a policy under the cap"
            );
            let fired = Arc::new(AtomicU64::new(0));
            let into_hook = fired.clone();
            *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
                into_hook.fetch_add(1, Ordering::Relaxed);
            }));
            let owner = retention.owner.clone();
            let pass = tokio::spawn({
                let key = key.clone();
                async move { owner.pass(&key, &(), claim).await }
            });
            // The pass runs to its listing, which queues behind the occupied
            // thread, and parks there. Bounded so a pass that never gets
            // there fails instead of hanging.
            let deadline = Instant::now() + Duration::from_secs(10);
            while fired.load(Ordering::Relaxed) == 0 {
                assert!(
                    Instant::now() < deadline,
                    "the pass never reached its listing"
                );
                tokio::task::yield_now().await;
            }
            pass.abort();
            assert!(
                pass.await.unwrap_err().is_cancelled(),
                "the pass died between its listing and its unlinks"
            );
            release.send(()).unwrap();
            occupied.await.unwrap();

            assert!(
                retention.owner.holding(&key).unwrap().installed.is_some(),
                "the policy is where it was: nothing took it out"
            );
            assert_eq!(
                dir.held().unwrap().len(),
                16,
                "and the dead pass unlinked nothing"
            );
            drop(
                retention
                    .owner
                    .try_turn(&key)
                    .expect("the dead pass let go of the entity's turn"),
            );

            // The next delivered byte starts a pass of its own, and that
            // pass does what the dead one never got to.
            reader.note(CHUNK_BYTES);
            settled(
                &retention,
                "the next byte's pass reclaimed the far end",
                |_| !dir.chunk_path(15).exists(),
            )
            .await;
            assert!(
                dir.chunk_path(1).is_file(),
                "round the chunk the player is inside"
            );
        });
    }

    /// A reader that is gone stops holding anything, and the entity it was
    /// reading is ordinary cache again once the grace has passed.
    #[tokio::test]
    async fn a_read_that_ended_holds_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.promises(0..16);
        reader.note(0);
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(!gate.releases_file(&dir.chunk_path(15)), "promised");

        drop(reader);
        settled(
            &retention,
            "the read that ended stopped holding chunk 15",
            |gate| gate.releases_file(&dir.chunk_path(15)),
        )
        .await;
    }

    /// **The grace holds against the clock and not against the next pass.**
    ///
    /// What a read that ended leaves behind is the windows the last pass
    /// chose, and a pass writes those whole from the readers live when it
    /// ran. So a second read of the same entity -- which an ordinary seek
    /// makes, the new range request opening while the old body drains --
    /// replaces them the first time its own playhead moves a stride, well
    /// inside [`IDLE`], and the region that was just played is ordinary
    /// cache again. Scrubbing back into it refetches from the origin.
    ///
    /// This is the trade [`IDLE`] states, pinned so that it is a decision
    /// rather than a surprise: holding that region would mean a window per
    /// playhead every ended read left behind, kept for a minute and a half,
    /// on the device whose disk is the reason there is a budget at all.
    #[tokio::test]
    async fn a_second_players_pass_replaces_what_a_read_that_ended_was_holding() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Four chunks of budget over sixteen: two windows apart in this
        // entity cannot both be held.
        let retention = retention(Some(4 * CHUNK_BYTES));
        let played = retention.reader(&dir, TOTAL, TARGET.into());
        played.note(15 * CHUNK_BYTES);
        // The head of the film going is what says a pass really ran: a
        // reader with no window yet is inside all of the entity, so the gate
        // alone would answer before anything had been measured.
        settled(&retention, "the first player's pass ran", |_| {
            !dir.chunk_path(0).exists()
        })
        .await;
        drop(played);

        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "with nothing else reading the entity, the window the read that \
             ended left behind is still standing"
        );

        // The seek: a new body at the head of the film, and the old one is
        // gone. Nothing here waits on a clock -- the grace is a minute and a
        // half away, and this is what happens instead of it.
        let seeked = retention.reader(&dir, TOTAL, TARGET.into());
        seeked.note(0);
        settled(
            &retention,
            "the pass for the position the player seeked to ran",
            |gate| gate.releases_file(&dir.chunk_path(15)) && !dir.chunk_path(15).exists(),
        )
        .await;

        let held = retention.owner.holding(&dir.path().to_path_buf());
        let stream = held.as_ref().expect("the entity is being read");
        assert!(
            stream.last_seen.elapsed() < IDLE,
            "and it went well inside the grace, which is the point of this \
             test: the clock never came into it"
        );
        assert!(
            !dir.chunk_path(15).exists(),
            "the chunk the player was inside a moment ago is gone from the \
             disk, so scrubbing back to it costs the origin fetch again"
        );
        drop(held);
        drop(seeked);
    }

    /// **Two entities can carry one target, and the panel is told about the
    /// one a player is inside now.**
    ///
    /// A cache key covers the player headers that reach the origin -- an
    /// origin may answer two of them with two entities -- so one `d=` URL can
    /// have more than one directory under it, and an entry outlives the read
    /// that made it by the [`IDLE`] grace. A session that has *ended*
    /// therefore sits in the map beside the one being played now, both
    /// bounded and both naming that target, and only one of them has a
    /// playhead anybody is at. Answering from the other draws the panel a
    /// window round a position playback left behind, and round the wrong
    /// directory's chunks as well.
    ///
    /// The situation is built again and again because the order the map
    /// yields its entries in is not ours to choose and is not the same twice:
    /// one store answering rightly is a coin that landed face up, and what is
    /// claimed here is that the answer comes from which entity was read last
    /// rather than from where the two of them happen to sit. Nothing waits on
    /// a clock for that -- the later `last_seen` is the later `note`, which is
    /// the order the bytes really went out in.
    #[tokio::test]
    async fn a_panel_is_told_about_the_entity_being_read_and_not_a_session_that_ended() {
        for _ in 0..24 {
            let tmp = tempfile::tempdir().unwrap();
            // Twelve chunks of budget over a sixteen-chunk entity: a policy
            // is installed, so both of these are bounded and have a window to
            // show at all -- and both playheads below have a window covering
            // every chunk on their own disk, so what is asserted is the
            // reading and not a race with a pass.
            let retention = retention(Some(12 * CHUNK_BYTES));

            // The session that is over: one player played out the end of the
            // film and its body finished. Its entry stays for the grace,
            // because a request ending is the ordinary gap between two
            // requests of a player.
            let ended = ChunkDir::new(tmp.path().join("ended"));
            write_chunks(&ended, 12..16);
            let finished = retention.reader(&ended, TOTAL, TARGET.into());
            finished.note(15 * CHUNK_BYTES);
            drop(finished);

            // Then the same stream is opened again under other player headers
            // -- a second entity of the one target -- and this is the read a
            // player is inside. Its byte reaches a player after the other's,
            // which is the whole of what makes it the more recent.
            let live = ChunkDir::new(tmp.path().join("live"));
            write_chunks(&live, 3..10);
            let reader = retention.reader(&live, TOTAL, TARGET.into());
            reader.note(4 * CHUNK_BYTES + 5);

            assert_eq!(
                retention.window(TARGET),
                Some(enginefs::retention::CacheWindow {
                    behind_bytes: CHUNK_BYTES,
                    ahead_bytes: 6 * CHUNK_BYTES,
                }),
                "the seven chunks of the entity being read, split at its \
                 playhead -- and not the four the finished session left round \
                 the end of the film, which would be three behind and one \
                 ahead of a playhead nobody is at"
            );
            drop(reader);
        }
    }
}
