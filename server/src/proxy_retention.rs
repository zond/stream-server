//! Where a proxied stream's playback has got to, and the window that
//! follows it.
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
//! A stream appears in [`ProxyRetention::streams`] when a byte of it reaches
//! a player, and never before. At process start the map is empty, and that
//! is **true** rather than a placeholder: nothing has been played yet by
//! this process, so there is no position it could name. A cache directory
//! left by a previous run says which bytes were fetched and nothing whatever
//! about where anyone had got to in them, and inventing a playhead from it
//! -- the middle, the start, the last chunk written -- would have the window
//! keep a region nobody has ever read and reclaim the region a player is
//! about to ask for.
//!
//! Nothing here is persisted for the same reason. What survives a restart is
//! the chunks; a chunk with no live reader is ordinary cache, which is
//! exactly what the cleaner already treats it as.
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
//! * **The reclaim needs no interlock.** A torrent's piece file may only be
//!   unlinked after librqbit has forgotten the piece, under the claim
//!   `enginefs::retention::take_claimed` holds, or the torrent advertises
//!   bytes it no longer has. Nothing believes anything about a proxy chunk
//!   except the directory listing itself, so the reclaim is the unlink and
//!   there is nothing to keep atomic with it.
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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use enginefs::chunk_store::ChunkDir;
use enginefs::piece_store::{RetentionPolicy, Shape, Share};
use enginefs::retention::{CacheBudget, ReclaimGate, RetentionBudget};

use crate::proxy_cache::CHUNK_BYTES;

/// How long after its last delivered byte a stream stops counting as live.
///
/// While it is live the cleaner will not take the chunks under its window
/// (see [`ProxyRetention::fill_gate`]); once it is not, every byte of it is
/// ordinary cache again -- which is what the proxy cache has always been,
/// and what a proxied stream nobody is reading should be.
///
/// Long enough to cover a player that is paused and a demuxer that is
/// reconnecting, short enough that a closed player stops holding a window
/// within one cleaner interval.
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

/// The playheads of every proxied stream a player is reading, and the
/// policy over each.
pub struct ProxyRetention {
    /// The cache cleaner's cap, shared with the torrent half rather than
    /// copied from it: `EngineFS::cache_budget`. One volume, one number, one
    /// place it is written.
    budget: Arc<RetentionBudget>,
    /// One entry per entity a byte has been delivered from, keyed by the
    /// entity's own directory -- which is what says *which* chunk index
    /// space the window is over, since two entities under one cache key
    /// index their chunks the same way and hold different bytes.
    ///
    /// A `std::sync::Mutex` and not an async one: every critical section
    /// here is a map lookup and a few integers, and the two expensive parts
    /// of a pass -- the listing and the unlinks -- are done outside it on
    /// the blocking pool.
    streams: Mutex<HashMap<PathBuf, LiveStream>>,
}

/// One entity a player is reading.
struct LiveStream {
    dir: ChunkDir,
    /// The entity's length, as the origin stated it. A response of a
    /// different length is a different entity in a different directory, so
    /// this cannot go stale under the key.
    total: u64,
    /// The absolute offset of the last byte delivered to a player.
    ///
    /// **Only ever written from a byte that really went out** -- the cached
    /// body's ([`crate::proxy_cache::Cached::body`]) or the origin body's on
    /// its way past ([`crate::proxy_cache::Filler`]). Not from a `Range`
    /// header, which is where a player says what it *wants*: a player that
    /// asks for `bytes=0-` and reads a megabyte has a playhead a megabyte
    /// in, not at the end of the film.
    playhead: u64,
    /// When that byte went out, so a stream nobody is reading any more stops
    /// holding a window (see [`IDLE`]).
    last_seen: Instant,
    /// The budget the policy below was decided under, and `None` before any
    /// decision has been taken. A different budget is a different shape, so
    /// the policy is rebuilt rather than nudged -- the same rule
    /// `enginefs::retention::still_current` states for a torrent.
    decided: Option<CacheBudget>,
    /// The policy, or `None` when there is nothing to bound: no budget has
    /// been published yet, there is no cap at all, or the budget covers the
    /// whole entity. Taken out of the slot for the length of a pass.
    policy: Option<RetentionPolicy>,
    /// The window as of the last pass, which is what the cache cleaner is
    /// answered from. Read rather than recomputed so that a pass in flight
    /// -- which has the policy out of its slot -- does not make the gate
    /// answer "nothing is being read here".
    window: Option<Range<u64>>,
    /// How far the playhead must move before another pass is worth its
    /// listing.
    stride: u64,
    /// The chunk the last pass ran at.
    passed_at: Option<u64>,
    /// A pass is on the blocking pool for this stream right now, so a second
    /// one would only list the same directory again.
    running: bool,
}

impl ProxyRetention {
    pub fn new(budget: Arc<RetentionBudget>) -> Self {
        Self {
            budget,
            streams: Mutex::new(HashMap::new()),
        }
    }

    /// A byte at `delivered_to` of the entity in `dir` has reached a player.
    ///
    /// This is the whole of what makes a proxied stream's playhead exist.
    /// Cheap enough for the body path -- a lock, a hash lookup and a
    /// comparison -- and it starts a pass only when the playhead has moved a
    /// stride.
    pub fn note(self: &Arc<Self>, dir: &ChunkDir, total: u64, delivered_to: u64) {
        if total == 0 {
            return;
        }
        let budget = self.budget.get();
        let due = {
            let Ok(mut streams) = self.streams.lock() else {
                return;
            };
            let stream = streams
                .entry(dir.path().to_path_buf())
                .or_insert_with(|| LiveStream::new(dir.clone(), total));
            stream.playhead = delivered_to.min(total - 1);
            stream.last_seen = Instant::now();
            if stream.decided != Some(budget) {
                stream.decide(budget, total);
            }
            stream.due()
        };
        if !due {
            return;
        }
        let this = self.clone();
        let key = dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || this.pass(&key));
    }

    /// One pass over one entity: what the policy makes of where the playhead
    /// is now, and the unlinks that make it so.
    ///
    /// Blocking -- it lists the entity's bucket directories and unlinks what
    /// the window has left behind -- and the policy is taken out of its slot
    /// for the length of it, so a second pass that overlaps this one does
    /// nothing rather than queueing behind it. Exactly the shape
    /// `enginefs::engine::Engine::retain` uses, and for the same reason.
    fn pass(&self, key: &Path) {
        let Some((dir, mut policy, playhead, total)) = self.begin(key) else {
            return;
        };
        let last = (total - 1) / CHUNK_BYTES;
        let at = (playhead / CHUNK_BYTES).min(last);
        // A chunk index too big for the policy's index space is one the
        // policy was never built over -- see `LiveStream::decide`, which
        // refuses to build one at all in that case -- so this cannot narrow
        // a window that exists.
        let held: BTreeSet<u32> = dir
            .held()
            .into_iter()
            .filter_map(|index| u32::try_from(index).ok())
            .collect();
        let decision = policy.advance(at as u32, &held);
        let mut freed = 0usize;
        for index in &decision.reclaim {
            // The chunk store's own delete is `pub(crate)` to `enginefs` --
            // deliberately, so nothing outside it can unlink a torrent piece
            // behind librqbit's back. This is the proxy adapter's own
            // reclaim of its own chunk, the same `remove_file` the cache
            // cleaner has always done to these files, and there is no
            // have-set for it to disagree with. A staged copy is not looked
            // for: proxy staging is anonymous, lives only inside one
            // `write_whole`, and what a kill leaves is the launch sweep's.
            if std::fs::remove_file(dir.chunk_path(u64::from(*index))).is_ok() {
                freed += 1;
            }
        }
        if freed > 0 {
            tracing::debug!(
                dir = %dir.path().display(),
                freed,
                window = ?decision.window,
                "proxy retention pass"
            );
        }
        self.finish(key, policy, decision.window, at);
    }

    /// The locked half before a pass: take the policy out, and say what the
    /// pass is about. `None` when there is nothing to do.
    #[allow(clippy::type_complexity)]
    fn begin(&self, key: &Path) -> Option<(ChunkDir, RetentionPolicy, u64, u64)> {
        let mut streams = self.streams.lock().ok()?;
        let stream = streams.get_mut(key)?;
        let Some(policy) = stream.policy.take() else {
            stream.running = false;
            return None;
        };
        Some((stream.dir.clone(), policy, stream.playhead, stream.total))
    }

    /// The locked half after one: the policy back in its slot, the window it
    /// chose where the cache cleaner can read it, and the throttle rearmed.
    fn finish(&self, key: &Path, policy: RetentionPolicy, window: Range<u32>, at: u64) {
        let Ok(mut streams) = self.streams.lock() else {
            return;
        };
        let Some(stream) = streams.get_mut(key) else {
            return;
        };
        stream.running = false;
        stream.passed_at = Some(at);
        stream.window = Some(u64::from(window.start)..u64::from(window.end));
        if stream.policy.is_none() {
            stream.policy = Some(policy);
        }
    }

    /// Tell the cache cleaner's gate what live readers are holding, and
    /// forget the streams that have stopped being read.
    ///
    /// The pruning is here because this is the one call that happens once
    /// per cleaner pass rather than once per delivered chunk: a map that
    /// grew a permanent entry per URL ever played would be a leak measured
    /// in playbacks. That cadence is enough on its own -- writing a chunk
    /// is a filesystem event, and a filesystem event under the cache root
    /// is what arms the cleaner's debounce, so the case where entries are
    /// being added is exactly the case where this runs often.
    pub fn fill_gate(&self, gate: &mut ReclaimGate) {
        let Ok(mut streams) = self.streams.lock() else {
            return;
        };
        let now = Instant::now();
        streams.retain(|_, stream| now.duration_since(stream.last_seen) < IDLE);
        for (dir, stream) in streams.iter() {
            if let Some(window) = stream.window.clone() {
                gate.insert_window(dir.clone(), window);
            }
        }
    }
}

impl LiveStream {
    fn new(dir: ChunkDir, total: u64) -> Self {
        Self {
            dir,
            total,
            playhead: 0,
            last_seen: Instant::now(),
            decided: None,
            policy: None,
            window: None,
            stride: 1,
            passed_at: None,
            running: false,
        }
    }

    /// Build the policy for this entity under `budget`, or say why there is
    /// none.
    ///
    /// `None` is not a failure. It is the answer for a budget nobody has
    /// published yet ([`CacheBudget::Unknown`], which is an absence and not
    /// a zero), for a volume with no cap at all, and for a budget that
    /// covers the whole entity -- the phone with 379 GB free and every
    /// desktop, where there is nothing to bound and the cleaner's ordinary
    /// aging is the whole of the policy.
    fn decide(&mut self, budget: CacheBudget, total: u64) {
        self.decided = Some(budget);
        self.policy = None;
        self.window = None;
        self.passed_at = None;
        self.stride = 1;
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
            return;
        };
        self.stride = (u64::from(window) / PASSES_PER_WINDOW).max(1);
        self.policy = Some(policy);
    }

    /// Whether a pass is worth running now, and claim it if so.
    fn due(&mut self) -> bool {
        if self.running || self.policy.is_none() {
            return false;
        }
        let at = self.playhead / CHUNK_BYTES;
        // `abs_diff`, so a seek backwards is as much a reason to look as
        // playing on is: the window has moved either way.
        if self
            .passed_at
            .is_some_and(|was| at.abs_diff(was) < self.stride)
        {
            return false;
        }
        self.running = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4 MiB entity: sixteen chunks.
    const TOTAL: u64 = 16 * CHUNK_BYTES;

    fn retention(limit: Option<u64>) -> Arc<ProxyRetention> {
        let budget = Arc::new(RetentionBudget::default());
        if let Some(limit) = limit {
            budget.set(Some(limit));
        }
        Arc::new(ProxyRetention::new(budget))
    }

    fn write_chunks(dir: &ChunkDir, indices: impl IntoIterator<Item = u64>) {
        let bytes = vec![0u8; CHUNK_BYTES as usize];
        for index in indices {
            dir.write_whole(index, &bytes, Some(CHUNK_BYTES))
                .expect("a chunk");
        }
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
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        for index in 0..16u64 {
            assert!(
                gate.releases_file(&dir.chunk_path(index)),
                "chunk {index} is cache: nothing has been played out of this entity"
            );
        }
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
        retention.note(&dir, TOTAL, 12 * CHUNK_BYTES);
        // The pass runs on the blocking pool; wait for it to put the window
        // where the gate can read it.
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut gate = ReclaimGate::default();
            retention.fill_gate(&mut gate);
            if !gate.releases_file(&dir.chunk_path(12)) {
                break;
            }
        }

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

    /// A budget nobody has published is not a budget of nothing.
    ///
    /// Before the first cache pass this process has not been told what the
    /// volume may hold. Reading that as zero would have the first proxied
    /// stream of every boot reclaim every chunk behind its playhead before
    /// anything had measured the disk.
    #[tokio::test]
    async fn nothing_is_reclaimed_before_a_budget_has_been_published() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ChunkDir::new(tmp.path().join("entity"));
        write_chunks(&dir, 0..16);

        let retention = retention(None);
        retention.note(&dir, TOTAL, 15 * CHUNK_BYTES);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(dir.held().len(), 16, "every chunk is still here");
        let mut gate = ReclaimGate::default();
        retention.fill_gate(&mut gate);
        assert!(
            gate.releases_file(&dir.chunk_path(15)),
            "and none of it is held against the cleaner either"
        );
    }
}
