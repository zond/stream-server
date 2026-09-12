//! **Temporary instrumentation for the retention bug, and nothing else.**
//!
//! A phone opened a 4K film and fetched 1.6 GB to play about a hundred
//! megabytes. Four causes are fixed -- a tail probe claiming the playhead,
//! a probe's window ordering a forward reach of fetch, a window sized under
//! an open stream's lookahead, and a committed half sized from the disk
//! rather than from the time it buys -- and every one of them was worked
//! out afterwards, from piece counts, because nothing in the log said what
//! a pass was deciding or why.
//!
//! **What closes this**: a field log from the device that opened the film,
//! showing `ahead` holding at roughly the profile's seconds of the stream,
//! `dropped`/`unlinked` settling to a piece or two per pass, and `waste`
//! staying near zero over a viewing. When that log exists, delete this
//! module and its call sites -- `git rm` and the compiler names the rest.
//! The two numbers that stay are [`crate::retention::TorrentStreamNumbers`]'s
//! `refused_reclaims` and the waste figure the app draws beside it; those
//! are the permanent version of this and are already wired.
//!
//! It logs at INFO on the `enginefs` target, which `DEFAULT_LOG_FILTER`
//! carries in a release build, so it reaches the app's Diagnostics log
//! without a debug build or an environment variable. One line per entity
//! per ten seconds, plus an immediate line for the one event that should
//! never happen.

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
}

/// What one pass decided, as the owner saw it.
pub struct Pass<'a> {
    pub playhead: u32,
    /// What the read that owns the head is for, or `None` when the head is
    /// the entity's own remembered one and no read is live.
    pub reading: Option<&'static str>,
    pub window: Range<u32>,
    /// The pieces from the playhead to the window's forward edge.
    pub reach: u32,
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
        playhead = sample.playhead,
        reading = sample.reading.unwrap_or("none"),
        window_start = sample.window.start,
        window_end = sample.window.end,
        reach = sample.reach,
        held_behind = sample.held_behind,
        held_ahead = sample.held_ahead,
        committed = sample.committed,
        budget = ?sample.budget,
        lookahead_bytes = sample.lookahead_bytes,
        bytes_per_second = sample.bytes_per_second,
        dropped = sample.dropped,
        unlinked = sample.unlinked,
        refused_reclaims = sample.backing.map(|backing| backing.refused),
        fetched_since,
        verified_since,
        // What the swarm was paid for and nothing kept, over this interval.
        wasted_since = fetched_since.saturating_sub(verified_since),
        held_since,
        "retention pass"
    );
}

/// A pass that has *decided* to reclaim a piece an open stream is reading
/// ahead over.
///
/// **This is the plan and not the outcome**, and the difference has cost
/// two wrong diagnoses. The decision is taken against the entity's own
/// window; the door is asked again at every unlink, against every live
/// reader's *current* window ([`Door::windows_now`]), and cuts these pieces
/// out of the runs there. So a line here is a piece the pass would have
/// taken had nothing been reading it, which is the ordinary case, and may
/// well have deleted nothing.
///
/// What says whether anything went is `unlinked` on the pass line that
/// follows. Read the two together or not at all.
///
/// [`Door::windows_now`]: super::owner::Door::windows_now
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
