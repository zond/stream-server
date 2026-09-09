//! Where a proxied stream's playback has got to, what an open read has
//! promised, and the window that follows the one and never takes the other.
//!
//! This is `enginefs::retention` for the *other* adapter over the chunk
//! store. The arithmetic is not written twice: it is
//! `enginefs::piece_store::policy::RetentionPolicy`, the same budget, the
//! same 90%-ahead-10%-behind window, the same rule about what may be
//! reclaimed. What is here is the wiring, and it is the wiring that was
//! missing -- **a proxied stream had no playhead at all**. It serves ranges,
//! so the reads were always there and the route always knew the offset, but
//! nothing recorded where playback *was*, so there was nothing for a window
//! to follow and the cache was bounded by the cleaner's walk alone: a minute
//! after the last write at best, while a stream at 20 MB/s writes a
//! gigabyte in that minute.
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
//! budget and keeps the directory listing off the hot path.
//!
//! # What is protected when nothing needs bounding
//!
//! A policy is installed only when the budget really splits the entity. For
//! a budget that covers it ([`Shape::Whole`] -- the phone with 379 GB free
//! and every desktop), for a volume with no cap, and before any pass has
//! published a budget at all ([`CacheBudget::Unknown`], which is an absence
//! and not a zero), there is nothing to reclaim -- **but there is still a
//! player inside these bytes**, and the cleaner's cap is a per-volume number
//! that the rest of the cache can push past on its own. So the window in
//! that case is the whole entity, which is what the policy itself answers
//! for [`Shape::Whole`], and it is the same answer the torrent half gives:
//! an engine with no policy is `TorrentGate::Announced`, and the cleaner may
//! take none of it. What is *not* protected in either case is a stream
//! nobody is reading, which is the first thing that should go.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use enginefs::chunk_store::ChunkDir;
use enginefs::piece_store::{RetentionPolicy, Shape, Share};
use enginefs::retention::{CacheBudget, ReclaimGate, RetentionBudget};

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
/// grace is [`LiveStream::windows`], and a pass writes those wholesale from
/// the readers that were live when it ran ([`ProxyRetention::finish`]); a
/// read that has ended is not one of those, and its playhead is nowhere in
/// the protection path. So this is a grace for a player between two of its
/// own requests, where no other read of the entity runs a pass in the gap,
/// and not against one that does: a seek is two readers, and when the body
/// the seek left behind ends, the first pass for the new position drops the
/// region that was just played rather than holding it for the time here.
/// Scrubbing back into it then refetches from the origin.
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

/// The readers of every proxied stream a player is inside, and the policy
/// over each entity.
pub struct ProxyRetention {
    /// The cache cleaner's cap, shared with the torrent half rather than
    /// copied from it: `EngineFS::cache_budget`. One volume, one number, one
    /// place it is written.
    budget: Arc<RetentionBudget>,
    /// One entry per entity a reader has been opened on, keyed by the
    /// entity's own directory -- which is what says *which* chunk index
    /// space the window is over, since two entities under one cache key
    /// index their chunks the same way and hold different bytes.
    ///
    /// A `std::sync::Mutex` and not an async one: every critical section
    /// here is a map lookup and a few integers, and the two expensive parts
    /// of a pass -- the listing and the unlinks -- are done outside it on
    /// the blocking pool.
    streams: Mutex<HashMap<PathBuf, LiveStream>>,
    /// Names the next reader. Only ever compared for equality, so it may
    /// wrap without meaning anything: a process would have to open
    /// eighteen quintillion bodies for two live ones to collide.
    next_reader: AtomicU64,
    /// The one place a test can be *inside* a pass.
    ///
    /// A pass reads the playheads, lists the entity's directories and
    /// unlinks what no window covers, and what this exists to pin happens
    /// between those: playback moving on while the listing runs, a seek
    /// framing a body over the run being walked. A pass is one call on the
    /// blocking pool, so nothing outside can get in there --
    /// `a_budget_published_while_a_pass_was_running_is_the_one_that_holds`
    /// drives the two halves of a pass by hand for the same reason, which
    /// is not open to a test about the middle of one.
    ///
    /// The shipped build has neither this nor the two calls to it.
    #[cfg(test)]
    interleave: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// The passes below while they are on the blocking pool, counted beside
    /// the cache's chunk writes: `crate::proxy_cache::DiskWork`. A pass is
    /// spawned and never joined, so it is the other half of what makes a
    /// listing of the cache root a moving picture.
    work: Arc<crate::proxy_cache::DiskWork>,
    /// How many passes have run here, for the tests that bound them.
    ///
    /// A pass can arm another ([`LiveStream::arms_another_pass`]), so the
    /// passes of one entity are a chain, and what says a chain terminates
    /// is a count rather than a clock: one that arms itself off a term no
    /// pass moves runs tens of thousands of times a second, and is over any
    /// honest bound long before a timeout would notice.
    ///
    /// The shipped build has neither this nor the increment in
    /// [`ProxyRetention::pass`].
    #[cfg(test)]
    passes: AtomicU64,
}

/// One entity one or more readers are open on.
struct LiveStream {
    dir: ChunkDir,
    /// The origin URL the reader that created this entry was opened for --
    /// `/proxy`'s `d=`, after the request's own query has been folded into
    /// it, which is exactly the URL the cache key was built from.
    ///
    /// **The one thing here that is not derivable from the store**, and it
    /// is here because nothing else can be: a cache key is a hash of the
    /// target *and* the player headers that reach the origin, so an entity's
    /// directory name cannot be worked back to the URL a client is holding.
    /// A panel asking "what do you hold for the stream I am playing" has
    /// only that URL to ask with.
    ///
    /// An observation like every other field of this map: it exists because
    /// a reader was opened for that URL in this process, it is written once
    /// when the entry is created and never revised, and it dies with the
    /// entry. At process start there are no entries, so there is no URL to
    /// read back -- which is true, this process has relayed nothing yet.
    target: Arc<str>,
    /// The entity's length, as the origin stated it. A response of a
    /// different length is a different entity in a different directory, so
    /// this cannot go stale under the key.
    total: u64,
    /// The readers open on it, by [`Reader::id`]. An entry appears when a
    /// read promises or delivers something and goes when the body it
    /// belongs to is dropped, so this is what "somebody is inside these
    /// bytes right now" means.
    readers: HashMap<u64, ReaderState>,
    /// When a byte of this entity last reached a player, which is what
    /// [`IDLE`] is measured from once no reader is left.
    last_seen: Instant,
    /// The offset of that byte, and `None` until one has gone out.
    ///
    /// The entity's own reading of where playback was, as distinct from a
    /// reader's, and it answers exactly two questions: what a pass measures
    /// from when the read that started it has ended ([`LiveStream::head`]),
    /// and where [`ProxyRetention::window`] splits the disk for a panel.
    ///
    /// **It protects nothing by itself.** It is not in `decision.window`,
    /// not in [`Self::windows`] and not in
    /// [`ProxyRetention::is_inside_now`]; a chunk is held because a live
    /// reader's window or promise covers it, or because the last pass put a
    /// window there. Nor could it be read as "where the player that just
    /// stopped was" -- [`Reader::note`] writes it for whichever reader
    /// delivered last, so a second read of the same entity moves it to its
    /// own head at once.
    ///
    /// An observation like any other here -- absent until a byte really goes
    /// out, and never invented from what is on the disk.
    last_playhead: Option<u64>,
    /// The budget the policy below was decided under, and `None` before any
    /// decision has been taken. A different budget is a different shape, so
    /// the policy is rebuilt rather than nudged -- the same rule
    /// `enginefs::retention::still_current` states for a torrent.
    decided: Option<CacheBudget>,
    /// The policy, or `None` when there is nothing to reclaim: no budget has
    /// been published yet, there is no cap at all, or the budget covers the
    /// whole entity. Taken out of the slot for the length of a pass.
    policy: Option<RetentionPolicy>,
    /// The windows as of the last pass -- one per reader that had a playhead
    /// -- which is what the cache cleaner is answered from. Read rather than
    /// recomputed so that a pass in flight, which has the policy out of its
    /// slot, does not make the gate answer "nothing is being read here".
    ///
    /// It outlives the readers it was computed for, which is what makes the
    /// gap between one request of a player and its next survivable: that gap
    /// has no reader open in it, and the bytes the player is about to ask
    /// for again are the ones the last pass kept. It outlives them only
    /// until the next pass, though -- this is written whole from the readers
    /// live at that pass, so a second read of the entity replaces what a
    /// read that ended was holding, rather than the [`IDLE`] clock doing it.
    /// See [`IDLE`] for why that is the trade and not an accident.
    windows: Vec<Range<u64>>,
    /// Whether a policy is reclaiming here at all -- a [`Shape::Split`] was
    /// installed for the budget in [`Self::decided`].
    ///
    /// `false` is the answer for a budget that covers the entity, for a
    /// volume with no cap, and for a budget nobody has published yet, and it
    /// is what says a live reader is inside *all* of these bytes rather than
    /// inside a window of them: see [`Self::decide`] and
    /// [`ProxyRetention::fill_gate`]. Kept beside the policy rather than
    /// read off it, because a pass in flight has the policy out of its slot.
    bounded: bool,
    /// How far a playhead must move before another pass is worth its
    /// listing.
    stride: u64,
    /// A pass is on the blocking pool for this stream right now, so a second
    /// one would only list the same directory again.
    running: bool,
}

/// One open read of one entity.
struct ReaderState {
    /// The absolute offset of the last byte this read delivered to a
    /// player, and `None` until it has delivered one.
    ///
    /// **Only ever written from a byte that really went out** -- the cached
    /// body's ([`crate::proxy_cache::Cached::body`]) or the origin body's on
    /// its way past ([`crate::proxy_cache::Filler`]). Not from a `Range`
    /// header, which is where a player says what it *wants*: a player that
    /// asks for `bytes=0-` and reads a megabyte has a playhead a megabyte
    /// in, not at the end of the film.
    playhead: Option<u64>,
    /// The chunks this read has promised off the disk and not yet
    /// delivered. Nothing may unlink one of them; see the module docs.
    promised: Range<u64>,
    /// The chunk this reader's last pass ran at, so one reader playing on
    /// does not spend the other's throttle.
    passed_at: Option<u64>,
}

/// One open read: a handle that holds its promise and carries its playhead.
///
/// Dropping it is what says the read is over -- the body ended, or the
/// client went away -- and it is the only thing that does.
pub struct Reader {
    retention: Arc<ProxyRetention>,
    key: PathBuf,
    dir: ChunkDir,
    /// The origin URL this read is of; see [`LiveStream::target`].
    target: Arc<str>,
    total: u64,
    id: u64,
}

/// What one pass was handed under the lock.
struct Begin {
    dir: ChunkDir,
    policy: RetentionPolicy,
    budget: Option<CacheBudget>,
    total: u64,
    /// The playhead of the reader whose movement started this pass.
    at: u64,
    /// Every other live reader's playhead: each gets a window of its own.
    others: Vec<u64>,
    /// What open reads have promised and not yet delivered.
    promised: Vec<Range<u64>>,
}

impl ProxyRetention {
    pub fn new(budget: Arc<RetentionBudget>, work: Arc<crate::proxy_cache::DiskWork>) -> Self {
        Self {
            budget,
            streams: Mutex::new(HashMap::new()),
            next_reader: AtomicU64::new(0),
            #[cfg(test)]
            interleave: Mutex::new(None),
            work,
            #[cfg(test)]
            passes: AtomicU64::new(0),
        }
    }

    /// Open a reader on the entity in `dir`, whose length is `total`.
    ///
    /// It records nothing by itself: a reader that never promises and never
    /// delivers a byte is a reader nothing has observed, and it leaves no
    /// entry behind.
    pub fn reader(self: &Arc<Self>, dir: &ChunkDir, total: u64, target: Arc<str>) -> Reader {
        Reader {
            retention: self.clone(),
            key: dir.path().to_path_buf(),
            dir: dir.clone(),
            target,
            total,
            id: self.next_reader.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// One pass over one entity: what the policy makes of where the
    /// playheads are now, and the unlinks that make it so.
    ///
    /// Blocking -- it lists the entity's bucket directories and unlinks what
    /// no window covers -- and the policy is taken out of its slot for the
    /// length of it, so a second pass that overlaps this one does nothing
    /// rather than queueing behind it. Exactly the shape
    /// `enginefs::engine::Engine::retain` uses, and for the same reason.
    fn pass(self: &Arc<Self>, key: &Path, id: u64) {
        #[cfg(test)]
        self.passes.fetch_add(1, Ordering::Relaxed);
        let Some(begin) = self.begin(key, id) else {
            return;
        };
        let Begin {
            dir,
            mut policy,
            budget,
            total,
            at,
            others,
            promised,
        } = begin;
        // `max(1)` as in `LiveStream::chunk`, which is the other reading of
        // this: an entity of no bytes never reaches either of them.
        let last = (total.max(1) - 1) / CHUNK_BYTES;
        // Where a test puts what playback does while the listing below runs.
        #[cfg(test)]
        self.interleave();
        // A chunk index too big for the policy's index space is one the
        // policy was never built over -- see `LiveStream::decide`, which
        // refuses to build one at all in that case -- so this cannot narrow
        // a window that exists.
        let held: BTreeSet<u32> = dir
            .held()
            .into_iter()
            .filter_map(|index| u32::try_from(index).ok())
            .collect();
        // **The playheads again, now that the disk has been listed.**
        //
        // The listing and the playheads are not one reading of one moment
        // and cannot be: the listing is the slow half, and a fill relaying
        // at twenty megabytes a second writes a whole window's worth of
        // chunks while it runs. What the order buys is the direction of the
        // skew. Read after the listing, the playheads are the newer half, so
        // a window may name chunks the listing did not find -- which unlinks
        // nothing, because only what the listing found is a candidate. Read
        // before it, the window sat a window *behind* the chunks the listing
        // found, so the pass reclaimed the read-ahead the fill had just
        // written, and the next pass, at the playhead that had by then caught
        // up with it, reclaimed everything left behind it. Two passes, and
        // between them a cache with nothing in it: measured here, a
        // sixteen-megabyte read left an empty directory under an
        // eight-megabyte budget.
        //
        // So the invariant this pass really keeps, and the reason nobody
        // should put the two halves back the other way round, is about each
        // chunk it unlinks rather than about a moment. Every one of them was
        // on the disk when the listing ran; was outside every window and
        // every promise of the readers live *after* the listing, which is
        // what the reading below and the windows built from it are; and was
        // outside every live reader's window and promise again at the
        // instant of the unlink, which is what `is_inside_now` asks at the
        // door. A chunk that was written, or read, or promised after any one
        // of those is simply not taken.
        let (at, others, promised) = self.heads(key, id).unwrap_or((at, others, promised));
        let at = (at / CHUNK_BYTES).min(last);
        let decision = policy.advance(at as u32, &held);
        // One window per live playhead. The policy answers for one playhead
        // at a time -- that is what a window is about -- and an entity two
        // players are inside has two of them. `window_at` and not a second
        // `advance`: a pass is one decision about what to give back, and the
        // other readers' windows are inputs to that decision rather than
        // decisions of their own.
        let mut windows = vec![span(decision.window.clone())];
        for other in others {
            let other = (other / CHUNK_BYTES).min(last);
            windows.push(span(policy.window_at(other as u32)));
        }
        let mut freed = 0usize;
        // And what it does while the unlinks below run.
        #[cfg(test)]
        self.interleave();
        for index in &decision.reclaim {
            let index = u64::from(*index);
            if windows.iter().any(|window| window.contains(&index)) {
                continue;
            }
            // A chunk an open body has already been told it will get is not
            // ours to take, whatever the window says: this is the refusal
            // librqbit makes for a torrent's reader, in the one place a
            // proxied read can make it for itself.
            if promised.iter().any(|range| range.contains(&index)) {
                continue;
            }
            // And the third asking, at the door. Everything above is a
            // reading, and the unlinks below take as long as they take --
            // one `unlink` per chunk of a window, on the flash of a
            // television -- while playback goes on delivering bytes and a
            // seek can frame a whole new body over the run this pass is
            // walking. This is the same refusal the cleaner's own delete
            // makes (`still_free`, the sibling of this one) and it is made
            // for the same reason: a chunk somebody is inside costs the
            // player a broken read and the origin the same fetch again,
            // while a chunk left standing costs a few bytes until the next
            // pass.
            if self.is_inside_now(key, index, &policy, last) {
                continue;
            }
            // The chunk store's own delete is `pub(crate)` to `enginefs` --
            // deliberately, so nothing outside it can unlink a torrent piece
            // behind librqbit's back. This is the proxy adapter's own
            // reclaim of its own chunk, the same `remove_file` the cache
            // cleaner has always done to these files, and there is no
            // have-set for it to disagree with. A staged copy is not looked
            // for: proxy staging is anonymous, lives only inside one
            // `write_whole`, and what a kill leaves is the launch sweep's.
            if std::fs::remove_file(dir.chunk_path(index)).is_ok() {
                freed += 1;
            }
        }
        if freed > 0 {
            tracing::debug!(
                dir = %dir.path().display(),
                freed,
                windows = ?windows,
                "proxy retention pass"
            );
        }
        self.finish(key, id, policy, budget, windows, at);
    }

    /// Run the interleaving a test installed, outside the lock that holds
    /// it, so the closure may take any lock this module has.
    #[cfg(test)]
    fn interleave(&self) {
        let hook = self
            .interleave
            .lock()
            .ok()
            .and_then(|hook| hook.as_ref().cloned());
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Put a pass for `id` on the blocking pool.
    ///
    /// The caller has already claimed the stream's `running` slot under the
    /// lock, so this is the spawn and the counting of it and nothing else.
    /// It is counted because a pass unlinks files nobody joins the task for:
    /// see `crate::proxy_cache::DiskWork`.
    fn spawn_pass(self: &Arc<Self>, key: PathBuf, id: u64) {
        let retention = self.clone();
        let ticket = self.work.start();
        tokio::task::spawn_blocking(move || {
            let _ticket = ticket;
            retention.pass(&key, id);
        });
    }

    /// The locked half before a pass: take the policy out, and say what the
    /// pass is about. `None` when there is nothing to do.
    fn begin(&self, key: &Path, id: u64) -> Option<Begin> {
        let mut streams = self.streams.lock().ok()?;
        let stream = streams.get_mut(key)?;
        let Some(policy) = stream.policy.take() else {
            stream.running = false;
            return None;
        };
        let Some((at, others, promised)) = stream.heads(id) else {
            stream.policy = Some(policy);
            stream.running = false;
            return None;
        };
        Some(Begin {
            dir: stream.dir.clone(),
            policy,
            budget: stream.decided,
            total: stream.total,
            at,
            others,
            promised,
        })
    }

    /// Where every reader of this entity is now, and what open bodies have
    /// been framed to deliver -- the same reading [`Self::begin`] takes,
    /// taken again after the listing. `None` when the entity is gone or
    /// nothing has a playhead in it.
    fn heads(&self, key: &Path, id: u64) -> Option<(u64, Vec<u64>, Vec<Range<u64>>)> {
        self.streams.lock().ok()?.get(key)?.heads(id)
    }

    /// Whether some live reader's window or promise covers `index` at this
    /// instant, asked with the policy this pass has out of its slot.
    ///
    /// Every live reader's playhead gets its window and every open body its
    /// promise. What is *not* asked about again is the entity's last
    /// delivered byte: a pass whose own reader has ended is already holding
    /// the window round that, in the `windows` the decision built, and this
    /// is only ever asked about a chunk those have already released.
    fn is_inside_now(&self, key: &Path, index: u64, policy: &RetentionPolicy, last: u64) -> bool {
        let Ok(streams) = self.streams.lock() else {
            // Nothing can be said about what is being read, so nothing is
            // taken: the direction this leans in everywhere.
            return true;
        };
        let Some(stream) = streams.get(key) else {
            return false;
        };
        if stream
            .readers
            .values()
            .any(|reader| reader.promised.contains(&index))
        {
            return true;
        }
        stream
            .readers
            .values()
            .filter_map(|reader| reader.playhead)
            .any(|head| {
                let head = (head / CHUNK_BYTES).min(last);
                span(policy.window_at(head as u32)).contains(&index)
            })
    }

    /// The locked half after one: the policy back in its slot, the windows
    /// it chose where the cache cleaner can read them, and the throttle
    /// rearmed.
    fn finish(
        self: &Arc<Self>,
        key: &Path,
        id: u64,
        policy: RetentionPolicy,
        budget: Option<CacheBudget>,
        windows: Vec<Range<u64>>,
        at: u64,
    ) {
        let again = {
            let Ok(mut streams) = self.streams.lock() else {
                return;
            };
            let Some(stream) = streams.get_mut(key) else {
                return;
            };
            stream.running = false;
            if let Some(reader) = stream.readers.get_mut(&id) {
                reader.passed_at = Some(at);
            }
            // A budget published while this pass was running has already
            // rebuilt the policy and the windows for the shape it makes; this
            // pass measured the old one, and neither its answer nor its policy
            // is the current one.
            if stream.decided == budget {
                stream.windows = windows;
                if stream.policy.is_none() {
                    stream.policy = Some(policy);
                }
            }
            stream.arms_another_pass(id, at)
        };
        if again {
            self.spawn_pass(key.to_path_buf(), id);
        }
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
    /// The pruning is here because this is the one call that happens once
    /// per cleaner pass rather than once per delivered chunk, and that
    /// cadence is enough on its own: writing a chunk is a filesystem event,
    /// and a filesystem event under the cache root is what arms the
    /// cleaner's debounce, so the case where entries are being added is
    /// exactly the case where this runs often. A reader's entry goes with
    /// the body it belongs to; an entity's goes once no reader is open on it
    /// and [`IDLE`] has passed since its last delivered byte -- so what a
    /// map that was never pruned could grow is one entry per entity played
    /// since the last pass, and not one per URL ever played.
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
    /// per thousand chunks. Call it off the reactor. The lock is not held
    /// across the listing.
    pub fn window(&self, target: &str) -> Option<enginefs::retention::CacheWindow> {
        let (dir, playhead) = {
            let streams = self.streams.lock().ok()?;
            let stream = streams
                .values()
                .filter(|stream| &*stream.target == target && stream.bounded)
                .max_by_key(|stream| stream.last_seen)?;
            (stream.dir.clone(), stream.last_playhead?)
        };
        let at = playhead / CHUNK_BYTES;
        let mut window = enginefs::retention::CacheWindow::default();
        for index in dir.held() {
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
        let Ok(streams) = self.streams.lock() else {
            return 0;
        };
        streams.values().map(|stream| stream.readers.len()).sum()
    }

    pub fn still_free(&self, path: &std::path::Path) -> bool {
        let mut gate = ReclaimGate::default();
        self.fill_gate(&mut gate);
        gate.releases_file(path)
    }

    pub fn fill_gate(&self, gate: &mut ReclaimGate) {
        let Ok(mut streams) = self.streams.lock() else {
            return;
        };
        let now = Instant::now();
        streams.retain(|_, stream| {
            !stream.readers.is_empty() || now.duration_since(stream.last_seen) < IDLE
        });
        for (dir, stream) in streams.iter() {
            for window in &stream.windows {
                gate.insert_window(dir.clone(), window.clone());
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
            if (!stream.bounded || stream.windows.is_empty())
                && stream
                    .readers
                    .values()
                    .any(|reader| reader.playhead.is_some())
            {
                gate.insert_window(dir.clone(), 0..stream.total.div_ceil(CHUNK_BYTES));
            }
            for reader in stream.readers.values() {
                if !reader.promised.is_empty() {
                    gate.insert_window(dir.clone(), reader.promised.clone());
                }
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
        let Ok(mut streams) = self.retention.streams.lock() else {
            return;
        };
        streams
            .entry(self.key.clone())
            .or_insert_with(|| LiveStream::new(self.dir.clone(), self.total, self.target.clone()))
            .readers
            .entry(self.id)
            .or_default()
            .promised = chunks;
    }

    /// A byte at `delivered_to` of this entity has reached a player.
    ///
    /// This is the whole of what makes a proxied stream's playhead exist.
    /// Cheap enough for the body path -- a lock, a hash lookup and a
    /// comparison -- and it starts a pass only when the playhead has moved a
    /// stride.
    pub fn note(&self, delivered_to: u64) {
        if self.total == 0 {
            return;
        }
        let budget = self.retention.budget.get();
        let delivered_to = delivered_to.min(self.total - 1);
        let at = delivered_to / CHUNK_BYTES;
        let due = {
            let Ok(mut streams) = self.retention.streams.lock() else {
                return;
            };
            let stream = streams.entry(self.key.clone()).or_insert_with(|| {
                LiveStream::new(self.dir.clone(), self.total, self.target.clone())
            });
            stream.last_seen = Instant::now();
            stream.last_playhead = Some(delivered_to);
            if stream.decided != Some(budget) {
                stream.decide(budget, self.total);
            }
            let (running, bounded, stride) =
                (stream.running, stream.policy.is_some(), stream.stride);
            let reader = stream.readers.entry(self.id).or_default();
            reader.playhead = Some(delivered_to);
            // Delivered is no longer promised: the chunk went out whole
            // before this was called, so the promise starts after it.
            reader.promised.start = reader
                .promised
                .start
                .max(at.saturating_add(1))
                .min(reader.promised.end);
            // `abs_diff`, so a seek backwards is as much a reason to look as
            // playing on is: the window has moved either way.
            let moved = !reader
                .passed_at
                .is_some_and(|was| at.abs_diff(was) < stride);
            let due = bounded && !running && moved;
            if due {
                stream.running = true;
            }
            due
        };
        if !due {
            return;
        }
        self.retention.spawn_pass(self.key.clone(), self.id);
    }
}

impl Drop for Reader {
    /// The read is over, so its promise is released and its playhead is not
    /// a live reader's any more. What it leaves behind is the entity's
    /// entry, holding the windows the last pass chose until [`IDLE`] passes
    /// or another read of this entity runs a pass, so a player's next
    /// request usually finds the bytes it is about to ask for.
    fn drop(&mut self) {
        let Ok(mut streams) = self.retention.streams.lock() else {
            return;
        };
        if let Some(stream) = streams.get_mut(&self.key) {
            stream.readers.remove(&self.id);
        }
    }
}

impl Default for ReaderState {
    fn default() -> Self {
        Self {
            playhead: None,
            promised: 0..0,
            passed_at: None,
        }
    }
}

/// A chunk range in the `u64` index space the gate and the cleaner speak.
fn span(chunks: Range<u32>) -> Range<u64> {
    u64::from(chunks.start)..u64::from(chunks.end)
}

impl LiveStream {
    fn new(dir: ChunkDir, total: u64, target: Arc<str>) -> Self {
        Self {
            dir,
            target,
            total,
            readers: HashMap::new(),
            last_seen: Instant::now(),
            last_playhead: None,
            decided: None,
            policy: None,
            windows: Vec::new(),
            bounded: false,
            stride: 1,
            running: false,
        }
    }

    /// Where every reader of this entity is, and what their open bodies
    /// have been framed to deliver.
    ///
    /// The playhead the pass is *about* is `id`'s, and it falls back to the
    /// entity's last delivered byte: the read that asked for the pass can
    /// have ended in the meantime, and where it got to is still the last
    /// thing that happened here -- the window belongs round there for the
    /// grace, because a player between two requests is the ordinary way for
    /// a reader to be gone. `None` when nothing has ever delivered a byte
    /// of this entity, which is a stream with no playhead and so no window.
    fn heads(&self, id: u64) -> Option<(u64, Vec<u64>, Vec<Range<u64>>)> {
        let at = self.head(id)?;
        let others = self
            .readers
            .iter()
            .filter(|(other, _)| **other != id)
            .filter_map(|(_, reader)| reader.playhead)
            .collect();
        let promised = self
            .readers
            .values()
            .map(|reader| reader.promised.clone())
            .filter(|range| !range.is_empty())
            .collect();
        Some((at, others, promised))
    }

    /// The playhead a pass for `id` is about, in bytes: that reader's own
    /// while its body is open, and the entity's last delivered byte once it
    /// has ended.
    ///
    /// **One function, because it is one question.** Where a pass measures
    /// from and whether a pass is still owed are the same question asked at
    /// two moments ([`Self::heads`] and [`Self::arms_another_pass`]), and
    /// the second only terminates if it is a fixed point of the first: two
    /// spellings of it, one reading the reader and one the entity, differ by
    /// however far apart two players are, no pass moves either of them, and
    /// so every pass arms the next one forever.
    fn head(&self, id: u64) -> Option<u64> {
        self.readers
            .get(&id)
            .and_then(|reader| reader.playhead)
            .or(self.last_playhead)
    }

    /// The chunk a byte offset of this entity is in, clamped to the last
    /// one it has.
    ///
    /// `max(1)` for the entity of no bytes, which cannot be here at all --
    /// [`Reader::note`] and [`Reader::promises`] both return before they
    /// would create it -- and is written the same way in
    /// [`ProxyRetention::pass`] so that neither reading of "the last chunk"
    /// can drift from the other.
    fn chunk(&self, at: u64) -> u64 {
        (at / CHUNK_BYTES).min((self.total.max(1) - 1) / CHUNK_BYTES)
    }

    /// Whether the pass that just measured chunk `at` swallowed the trigger
    /// for the next one -- and if it did, claim the slot for it.
    ///
    /// [`Reader::note`] starts no pass while one is in flight, because a
    /// second listing of the same directory at the same moment measures the
    /// same disk, **and it does not remember that it wanted one**. That is
    /// the right answer for every byte but the last: playback delivers
    /// another one along in a moment and it brings the trigger with it. The
    /// last byte of a body brings nothing, and a body ends while a pass is
    /// running as often as the blocking pool is busy -- so the pass that
    /// would have run round where the player *stopped* never ran at all.
    ///
    /// Two things followed from that and neither is a small one. Every chunk
    /// written since the running pass took its listing stayed on the disk,
    /// over the budget, until the cleaner's own walk an hour later got to it
    /// -- the overshoot the throttle bounds to a twentieth of a window was
    /// in fact the whole tail of the stream. And [`ProxyRetention::fill_gate`]
    /// answers the cleaner from `windows`, which then named where the player
    /// *had been* when the last pass ran rather than where it stopped: the
    /// cleaner was offered the bytes round the playhead and refused the ones
    /// behind them, which is the grace exactly inside out.
    ///
    /// So the pass that swallowed the trigger arms the next one itself, and
    /// it asks [`Self::head`] -- the reader this pass was about, falling
    /// back to the entity, exactly as the pass's own measurement did.
    ///
    /// **That is what makes the chain terminate.** `at` is what `head(id)`
    /// answered a moment ago, so the pass this arms measures `head(id)`
    /// again and asks this question of a distance that is zero unless a byte
    /// really has gone out in between. Nothing a pass does moves a playhead
    /// -- only [`Reader::note`] does, from a byte that reached a player --
    /// so each arming is paid for by a delivered byte that arrived while a
    /// pass held the slot, and the chain is at most one pass long per such
    /// byte. Asked instead of `last_playhead` while `at` came from a live
    /// reader, the two terms are a distance between two players, or between
    /// a body and a range request that has ended, and no pass moves either.
    fn arms_another_pass(&mut self, id: u64, at: u64) -> bool {
        let moved = self
            .head(id)
            .is_some_and(|to| self.chunk(to).abs_diff(at) >= self.stride);
        let due = moved && self.policy.is_some() && !self.running;
        if due {
            self.running = true;
        }
        due
    }

    /// Build the policy for this entity under `budget`, or say why there is
    /// none -- and in either case say what a reader inside it holds.
    ///
    /// `None` is not a failure. It is the answer for a budget nobody has
    /// published yet ([`CacheBudget::Unknown`], which is an absence and not
    /// a zero), for a volume with no cap at all, and for a budget that
    /// covers the whole entity -- the phone with 379 GB free and every
    /// desktop, where there is nothing to bound and the cleaner's ordinary
    /// aging is the whole of the policy. **The window is not `None` with
    /// it**: the cleaner's cap is a per-volume number and the rest of the
    /// cache can push past it on its own, so an entity nothing reclaims is
    /// still an entity a player is inside. All of it is the window then,
    /// which is what the policy answers for [`Shape::Whole`] and what the
    /// torrent half answers with `TorrentGate::Announced`.
    fn decide(&mut self, budget: CacheBudget, total: u64) {
        self.decided = Some(budget);
        self.policy = None;
        self.bounded = false;
        self.stride = 1;
        // Every reader is due again. A budget is a different shape, so the
        // stride the last one's passes measured against is not this one's,
        // and `passed_at` is where a reader stood for a pass that no longer
        // describes anything: left standing, a reader that has not travelled
        // a whole *new* stride is never due, so no pass runs and the windows
        // below are never rebuilt. A paused player never moves at all.
        for reader in self.readers.values_mut() {
            reader.passed_at = None;
        }
        // The windows are NOT cleared. They are the last measurement a pass
        // really made, and `fill_gate` reads empty windows beside a live
        // reader as "protect the whole entity" -- the honest answer when
        // nothing has ever been measured, and a wrong one the moment it
        // means "measured, but against a budget one byte different". The
        // budget is `CacheLimit::effective` minus headroom, which moves with
        // the volume's free space, so it differs on most passes: clearing
        // here made the fallback the normal case and the proxy cache
        // effectively unreclaimable. A window measured against a slightly
        // different budget is superseded by the next pass, which the reset
        // above makes due on the next delivered byte.
        let CacheBudget::Bytes(bytes) = budget else {
            return;
        };
        let Ok(chunks) = u32::try_from(total.div_ceil(CHUNK_BYTES)) else {
            // An entity of a petabyte. Nothing to do but leave it to the
            // cleaner, which counts bytes rather than indices.
            return;
        };
        let policy =
            match RetentionPolicy::new(bytes, CHUNK_BYTES, 0..chunks, total, Share::Nothing) {
                Ok(policy) => policy,
                Err(error) => {
                    tracing::debug!(
                        dir = %self.dir.path().display(),
                        error = %format!("{error:#}"),
                        "could not size a retention policy for a proxied entity; it is not bounded"
                    );
                    return;
                }
            };
        let Shape::Split { window, .. } = policy.shape() else {
            // The budget covers it: nothing here will reclaim anything, and
            // a reader inside it is inside all of it.
            return;
        };
        self.stride = (u64::from(window) / PASSES_PER_WINDOW).max(1);
        self.bounded = true;
        self.policy = Some(policy);
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

    /// Wait for the passes a `note` started to have got somewhere -- `what`
    /// says where, and is what a failure is reported as. Bounded so a
    /// regression fails instead of hanging, and generously, because the
    /// bound is not the assertion.
    async fn settled(
        retention: &Arc<ProxyRetention>,
        what: &str,
        until: impl Fn(&ReclaimGate) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let mut gate = ReclaimGate::default();
            retention.fill_gate(&mut gate);
            if until(&gate) {
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
        assert_eq!(dir.held().len(), 16, "and nothing was reclaimed");
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

        assert_eq!(dir.held().len(), 16, "every chunk is still here");
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
    /// byte started is on the blocking pool -- so a pass running with no
    /// reader left is the ordinary case and not a corner. Where playback got
    /// to is still the last thing that happened to this entity, and the
    /// window belongs round there for the [`IDLE`] grace, because the
    /// player's next request is a moment away and it will ask for the bytes
    /// either side of that point. A pass that shrugged at a reader it could
    /// not find would leave the window wherever the *previous* one put it,
    /// which is where playback was and not where it stopped.
    ///
    /// The interleaving is driven by hand: `running` is what a pass already
    /// on the blocking pool looks like from `note`, so the note below starts
    /// no pass of its own and this test owns which pass runs when.
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
        retention
            .streams
            .lock()
            .unwrap()
            .get_mut(dir.path())
            .expect("the entity is being read")
            .running = true;
        reader.note(15 * CHUNK_BYTES);

        // The request ends with that byte, and only then does the pass it
        // started run.
        let id = reader.id;
        drop(reader);
        retention.pass(dir.path(), id);

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
    /// The interleaving is driven by hand -- the two halves of a pass either
    /// side of the change -- because that is the only way to be inside the
    /// window at all.
    #[tokio::test]
    async fn a_budget_published_while_a_pass_was_running_is_the_one_that_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let budget = Arc::new(RetentionBudget::default());
        budget.set(Some(12 * CHUNK_BYTES));
        let retention = Arc::new(ProxyRetention::new(budget.clone(), Arc::default()));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(0);
        settled(&retention, "the published cap was applied", |_| {
            dir.held().len() <= 12
        })
        .await;
        // Playback fills what it passes over, as it does.
        write_chunks(&dir, 0..16);

        // A pass takes the policy out and is still working.
        let begun = retention
            .begin(dir.path(), reader.id)
            .expect("a pass over a policy that is installed");
        // The cleaner's next walk finds no cap at all -- no `cacheSize` set
        // and a volume whose free space it could not read.
        budget.set(None);
        reader.note(0);
        // Only now does the pass that measured the old cap finish, with the
        // window it chose, which is as stale as its policy.
        let stale: Vec<Range<u64>> = std::iter::once(0..12).collect();
        retention.finish(dir.path(), reader.id, begun.policy, begun.budget, stale, 0);

        // Nothing bounds this entity now, so playing on reclaims none of it.
        reader.note(2 * CHUNK_BYTES);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            dir.held().len(),
            16,
            "a cap nobody has published is not a cap to evict against: {:?}",
            dir.held()
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
        let reader = retention.reader(&dir, WHOLE, TARGET.into());
        write_chunks(&dir, 0..CHUNKS);
        reader.note(0);
        settled(
            &retention,
            "the first pass ran at the head of the film",
            |_| !dir.chunk_path(31).exists(),
        )
        .await;
        // Playback fills what it passes over, as it does.
        write_chunks(&dir, 0..CHUNKS);

        // Playback while the pass below runs: at the tenth chunk when it
        // lists the directory, at the twentieth by the time it unlinks. The
        // playhead is moved the way a delivered byte moves it, and no pass
        // is started for it -- `note` starts none while the policy is out of
        // its slot, which is exactly the case this is about.
        let moved = std::sync::atomic::AtomicU64::new(0);
        let key = dir.path().to_path_buf();
        let streams = Arc::downgrade(&retention);
        *retention.interleave.lock().unwrap() = Some(Arc::new(move || {
            let at = match moved.fetch_add(1, Ordering::Relaxed) {
                0 => 10,
                1 => 20,
                // The pass this movement arms runs after those two, and
                // playback has stopped by then.
                _ => return,
            };
            let Some(retention) = streams.upgrade() else {
                return;
            };
            let mut streams = retention.streams.lock().unwrap();
            let stream = streams.get_mut(&key).expect("the entity is being read");
            stream.last_playhead = Some(at * CHUNK_BYTES);
            for reader in stream.readers.values_mut() {
                reader.playhead = Some(at * CHUNK_BYTES);
            }
        }));

        retention.pass(dir.path(), reader.id);
        settled(
            &retention,
            "the pass the movement armed reclaimed round the twentieth chunk",
            |_| !dir.chunk_path(10).exists(),
        )
        .await;

        let held = dir.held();
        assert_eq!(
            held,
            (20..28).collect::<BTreeSet<u64>>(),
            "what is on the disk is the window round where playback got to"
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
    /// The interleaving is driven by hand -- the two halves of a pass either
    /// side of the last byte -- because that is the only way to be inside
    /// the window at all.
    #[tokio::test]
    async fn the_last_byte_of_a_body_gets_a_pass_even_though_one_was_running() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        // Eight chunks of budget over sixteen, so a window really is smaller
        // than the entity and a pass really has something to give back.
        let retention = retention(Some(8 * CHUNK_BYTES));
        let reader = retention.reader(&dir, TOTAL, TARGET.into());
        reader.note(0);
        settled(
            &retention,
            "the first pass ran at the head of the film",
            |_| !dir.chunk_path(15).exists(),
        )
        .await;
        // Playback fills what it passes over, as it does.
        write_chunks(&dir, 0..16);

        // A pass takes the policy out of its slot at the head of the film,
        // and is still working.
        let begun = retention
            .begin(dir.path(), reader.id)
            .expect("a pass over a policy that is installed");
        assert_eq!(begun.at, 0, "it measured where the player was");

        // The player reads the film to its end while that pass runs. Its
        // last byte finds no policy in the slot, so it starts nothing -- and
        // there is no byte after it to try again.
        reader.note(TOTAL - 1);

        // Only now does the pass that swallowed the trigger finish, with the
        // window it chose round the head of the film.
        let measured: Vec<Range<u64>> = std::iter::once(0..8).collect();
        retention.finish(
            dir.path(),
            reader.id,
            begun.policy,
            begun.budget,
            measured,
            0,
        );

        settled(
            &retention,
            "the pass the swallowed trigger armed reclaimed the head of the film",
            |_| !dir.chunk_path(0).exists(),
        )
        .await;
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

        // Longer ago than the grace: an entity nothing was reading would be
        // forgotten here, every byte of it ordinary cache again.
        retention
            .streams
            .lock()
            .unwrap()
            .get_mut(dir.path())
            .expect("the entity is being read")
            .last_seen = Instant::now() - IDLE - Duration::from_secs(1);

        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            !gate.releases_file(&dir.chunk_path(15)),
            "the paused player is still owed the tail of the body it has open"
        );
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

        let held = retention.streams.lock().unwrap();
        let stream = held.get(dir.path()).expect("the entity is being read");
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
