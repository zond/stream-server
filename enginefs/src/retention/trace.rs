//! **The retention trace: what a pass decided and why, in the log, under
//! a setting.**
//!
//! A phone opened a 4K film and fetched 1.6 GB to play about a hundred
//! megabytes. Four causes were fixed -- a tail probe claiming the playhead,
//! a probe's window ordering a forward reach of fetch, a window sized under
//! an open stream's lookahead, and a committed half sized from the disk
//! rather than from the time it buys -- and every one of them was worked
//! out afterwards, from piece counts, because nothing in the log said what
//! a pass was deciding or why. This is what says it: one line per entity
//! per ten seconds, a line per file on what its reads look like, the
//! budget in force when it is published, and an immediate line for the one
//! event that should never happen. Five field logs on the claim rules were
//! read by these lines.
//!
//! It was written as temporary, to be deleted once a field log showed the
//! fixes holding. That log exists (2026-09-15), and the lines stayed
//! useful every time something else went wrong; so instead of going they
//! are gated. Everything here logs at INFO on the `enginefs::retention::trace`
//! target, and the server's log filter carries that target at `off` unless
//! the `diagnosticsTrace` setting turns it on
//! (`stream_server::diagnostics::logging::set_diagnostics_trace`), at which
//! point it reaches the app's Diagnostics log without a debug build or an
//! environment variable. Nothing here checks the setting: a disabled target
//! is refused at the call site by `tracing` itself, and the tests that read
//! these lines install their own subscriber.
//!
//! The two numbers that are always on are [`crate::retention::TorrentStreamNumbers`]'s
//! `refused_reclaims` and the unverified figure the app draws beside it.

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use crate::retention::CacheBudget;

/// How often one entity may say anything. A pass runs every two seconds
/// per file, and five of those per line is enough to see a trend without
/// making the log the reason the phone is busy.
const INTERVAL: Duration = Duration::from_secs(10);

/// How long an entity nobody has passed over stays in [`SEEN`]. The map is
/// keyed by entity and only ever grew: on the proxy that is one entry per
/// URL ever proxied, for the life of the process. An entity quiet for this
/// long is forgotten; if it is passed over again its first line is a
/// fresh one with no deltas, which is what a new entity gets.
const STALE: Duration = Duration::from_secs(60);

/// Forget every entity not seen for [`STALE`] as of `now`.
fn sweep(seen: &mut HashMap<String, Was>, now: Instant) {
    seen.retain(|_, was| now.duration_since(was.at) < STALE);
}

/// What the backing can say that the owner cannot; `None` for a backing
/// that counts none of it (the proxy).
#[derive(Debug, Clone, Copy)]
pub struct Backing {
    /// Bytes this torrent has received from peers, ever.
    pub fetched: u64,
    /// Of those, the bytes that verified and were kept.
    pub verified: u64,
    /// Pieces this engine's passes asked the backend to forget and did not
    /// get back.
    pub refused: usize,
    /// Bytes per piece, for turning a lookahead into a piece range.
    pub piece_length: u64,
    /// How many times a staged copy has been opened over a piece the store
    /// holds -- a backend writing into a piece it was told was finished.
    ///
    /// Zero on a healthy run, and it stays in the pass line because the
    /// failure it names has a silent half: a read past the end of the fresh
    /// staged file shows up as `reading N bytes at X of piece P`, but a read
    /// inside a sparse hole in it returns zeros and looks like quiet media.
    /// This number is the only thing that distinguishes them.
    pub staged_over_held: u64,
}

/// What one pass decided, as the owner saw it.
pub struct Pass<'a> {
    /// **Where the entity is being consumed**, from the read-pattern
    /// detector: the head of the stream that has eaten the most of it. The
    /// readers' own answer only where the detector has none.
    pub playhead: u32,
    /// How many pieces this entity's consumers are asking for between
    /// them -- what `streams_seen` prints as `want`, totalled.
    pub wanted: u32,
    pub held_behind: usize,
    pub held_ahead: usize,
    pub committed: usize,
    pub budget: CacheBudget,
    /// The largest stream lookahead granted to a reader still open.
    pub lookahead_bytes: u64,
    /// The bitrate the time caps are sized from -- the entity's size over
    /// the duration a player stated -- or `None` until one is stated.
    pub bytes_per_second: Option<u64>,
    /// Pieces of the entity this pass stopped wanting.
    pub dropped: usize,
    /// Pieces this pass took off the disk.
    pub unlinked: usize,
    pub backing: Option<&'a Backing>,
}

/// The reading kept between lines, so the counters can be reported as
/// deltas over the interval rather than as totals nobody can subtract in
/// their head.
struct Was {
    at: Instant,
    fetched: u64,
    verified: u64,
    held: usize,
}

static SEEN: LazyLock<parking_lot::Mutex<HashMap<String, Was>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// One line for one pass over `key`, at most one per [`INTERVAL`].
pub fn pass<K: Debug>(key: &K, sample: Pass<'_>) {
    let name = format!("{key:?}");
    let held = sample.held_behind + sample.held_ahead;
    let now = Instant::now();
    let (fetched, verified) = sample
        .backing
        .map_or((0, 0), |backing| (backing.fetched, backing.verified));
    let over = {
        let mut seen = SEEN.lock();
        match seen.get(&name) {
            Some(was) if now.duration_since(was.at) < INTERVAL => return,
            was => {
                let over = was.map(|was| {
                    (
                        fetched.saturating_sub(was.fetched),
                        verified.saturating_sub(was.verified),
                        held as i64 - was.held as i64,
                    )
                });
                sweep(&mut seen, now);
                seen.insert(
                    name.clone(),
                    Was {
                        at: now,
                        fetched,
                        verified,
                        held,
                    },
                );
                over
            }
        }
    };
    let (fetched_since, verified_since, held_since) = over.unwrap_or((0, 0, 0));
    tracing::info!(
        target: "enginefs::retention::trace",
        key = %name,
        head = sample.playhead,
        wanted = sample.wanted,
        held_behind = sample.held_behind,
        held_ahead = sample.held_ahead,
        committed = sample.committed,
        budget = ?sample.budget,
        lookahead_bytes = sample.lookahead_bytes,
        bytes_per_second = sample.bytes_per_second,
        dropped = sample.dropped,
        unlinked = sample.unlinked,
        refused_reclaims = sample.backing.map(|backing| backing.refused),
        staged_over_held = sample.backing.map(|backing| backing.staged_over_held),
        fetched_since,
        verified_since,
        // Fetched over this interval and not vouched for by a piece hash:
        // chunks still in flight, the second copy of a chunk asked of a
        // fast peer on purpose, and what was really thrown away, with
        // nothing here telling the three apart. A level, not a verdict.
        unverified_since = fetched_since.saturating_sub(verified_since),
        held_since,
        "retention pass"
    );
}

/// A pass that has *decided* to reclaim a piece an open stream is reading
/// ahead over.
///
/// **This is the plan and not the outcome**, and the difference has cost
/// two wrong diagnoses. The decision is taken against the entity's own
/// window; the door is asked again at every unlink ([`Door::refuses`]),
/// against the set the pass published -- one bit per piece, covering
/// everything this entity's consumers are being fetched for and every
/// promise an open read has been made -- and a piece in it is cut out of
/// the run there. So a line here is a piece the pass would have taken had
/// nothing been reading it, which is the ordinary case, and may well have
/// deleted nothing.
///
/// What says whether anything went is `unlinked` on the pass line that
/// follows. Read the two together or not at all.
///
/// [`Door::refuses`]: super::owner::Door::refuses
pub fn planned_to_reclaim_inside_a_lookahead<K: Debug>(
    key: &K,
    pieces: &[u32],
    reader: Range<u32>,
    lookahead_bytes: u64,
) {
    tracing::info!(
        target: "enginefs::retention::trace",
        key = ?key,
        pieces = ?pieces,
        reader_start = reader.start,
        reader_end = reader.end,
        lookahead_bytes,
        "a retention pass planned to reclaim inside an open stream's lookahead; \
         `unlinked` on the next pass line says whether any of it went"
    );
}

/// Where the published budget came from, said once per publication. The
/// pass reports the number in force; this is the only place that knows
/// whether it is the operator's `cacheSize` or what the volume had left.
pub fn budget_published(bytes: Option<u64>, configured: Option<u64>, from_disk: Option<u64>) {
    let source = match (configured, from_disk) {
        (Some(configured), Some(disk)) if configured <= disk => "cacheSize",
        (Some(_), Some(_)) => "free space",
        (Some(_), None) => "cacheSize",
        (None, Some(_)) => "free space",
        (None, None) => "nothing",
    };
    tracing::info!(
        target: "enginefs::retention::trace",
        bytes,
        configured,
        from_disk,
        source,
        "the cache budget in force"
    );
}

/// What the read-pattern detector has found on one file.
pub struct StreamsSeen<'a> {
    pub info_hash: &'a str,
    pub file_idx: usize,
    /// How many streams on each file of the entity.
    pub counts: &'a [(usize, usize)],
    /// Where each stream on `file_idx` has reached, and how many reads took
    /// it there.
    pub heads: &'a [(u64, u32)],
    /// The two halves of the last rate sample, raw.
    pub sample: Option<(u64, Duration)>,
    /// What each stream has measured its consumer to be eating.
    pub rates: &'a [Option<u64>],
    /// What the replacement policy would order, which nothing obeys.
    pub want: &'a [std::ops::Range<u32>],
    /// What this entity was allowed to hold when that was sized: what it
    /// holds now plus the volume's headroom. Without it a narrow want set
    /// cannot be told apart -- a slow start, a share squeezed by another
    /// stream, and a disk with nothing left all look the same.
    pub allowed: u64,
    /// How many pieces of this file its streams are holding -- the size of
    /// the answer the door would get from the published set, beside the
    /// windows it was computed from.
    pub exempt: u32,
    /// How many pieces the LRU is watching.
    pub tracked: usize,
    /// The pieces it would give up first, oldest by effective age, with
    /// everything inside a wanted window already excluded.
    pub coldest: &'a [u32],
    /// Why the most recent read had to start a stream, if it did.
    pub why: Option<super::streams::Rejected>,
}

pub fn streams_seen(seen: StreamsSeen<'_>) {
    tracing::info!(
        target: "enginefs::retention::trace",
        info_hash = %seen.info_hash,
        file_idx = seen.file_idx,
        streams = ?seen.counts,
        heads = ?seen.heads,
        rates = ?seen.rates,
        want = ?seen.want,
        allowed = seen.allowed,
        exempt = seen.exempt,
        tracked = seen.tracked,
        coldest = ?seen.coldest,
        consumed = seen.sample.map(|(bytes, _)| bytes),
        gap_ms = seen.sample.map(|(_, gap)| gap.as_millis() as u64),
        why = ?seen.why,
        stage = "streams_seen",
        "what the reads of this file look like"
    );
}

/// The pass handed the backend a new split depth; see
/// [`super::deadline`]. Logged on change only, so a stream that has settled
/// says nothing.
pub fn deadline_depth(
    info_hash: &str,
    depth: usize,
    asked: usize,
    median: Option<Duration>,
    stalls: usize,
) {
    tracing::info!(
        target: "enginefs::retention::trace",
        info_hash = %info_hash,
        depth,
        asked,
        median_ms = median.map(|median| median.as_millis() as u64),
        stalls,
        stage = "deadline_depth",
        "how many pieces at the head of the lookahead are split"
    );
}

/// The player reported buffering while playing; `counted` says whether it
/// went into the stalls that deepen the split, or was read as the viewer's
/// window still filling after an open or a seek. See
/// [`crate::engine::Engine::player_stalled`].
pub fn player_stalled(info_hash: &str, counted: bool, stalls: usize) {
    tracing::info!(
        target: "enginefs::retention::trace",
        info_hash = %info_hash,
        counted,
        stalls,
        stage = "player_stalled",
        "the player said it was buffering"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **An entity nobody passes over any more is forgotten.** The map
    /// grew by one entry per proxied URL for the life of the process.
    #[test]
    fn an_entity_not_seen_for_a_minute_leaves_the_map() {
        let now = Instant::now();
        let was = |at: Instant| Was {
            at,
            fetched: 0,
            verified: 0,
            held: 0,
        };
        let mut seen = HashMap::new();
        seen.insert("stale".to_string(), was(now - 2 * STALE));
        seen.insert("fresh".to_string(), was(now - INTERVAL));
        sweep(&mut seen, now);
        assert_eq!(seen.len(), 1, "the stale entity stayed: {:?}", seen.keys());
        assert!(seen.contains_key("fresh"));
    }
}
