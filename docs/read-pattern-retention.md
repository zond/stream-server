# Read-pattern retention

**Status: designed, not built.** Nothing in `enginefs::retention` works this
way yet. This is the agreed shape, written down so the reasoning survives the
session that produced it; the constants named here are placeholders and the
staging at the end is the order to build it in.

## Why the current design has to go

The retention owner decides what to keep and what to fetch from a *reading*:
`Reading::{Playback, Probe}`, derived from a `PlaybackIntent`, derived in turn
by `playback_intent_for_request` from a priority header, two download flags and
the geometry of a byte range. A player states none of that. It sends a range.

Every field failure of the last week is that derivation being wrong, and each
one was diagnosed as something else first:

- `is_container_metadata_request` recognises "the container index" from two
  invented constants -- a window of `max(16 MiB, file_size / 512)` and a start
  offset scaled the same way. A reader that sat at `file_size - 25,961,713`
  once a second for a whole session was read as an index crawl. It is 15.2 MB
  *before* that file's `moov`, so it is inside `mdat`: media data. The
  `/512`, `container_metadata_window` and `STRUCTURAL_PIECES` were all built
  on that misreading.
- That same reader is a second live track -- subtitles or a second audio
  stream, muxed near the end of `mdat`. Labelled `ContainerMetadata`, it never
  got a playback want-set, and a read blocked on its next piece for twenty
  seconds while seventeen seeders were connected. Every retry served exactly
  up to the missing piece's boundary and gave up: 40,898 bytes, then 40,851,
  then 40,804, each one the distance from where it resumed to piece 5560.
- Placing the window from reads put it at the end of a 23 GB file while the
  viewer was sixteen minutes in, which is what the told playhead was added to
  fix -- and the told playhead then needed `read_near` to decide which read to
  believe, because a reader's position cannot say whether it is the viewer.

The common shape is that we ask what a read *means* and answer from its
geometry. We never have to: `files.rs::poll_read` hands us a file offset for
every read, so we can measure what a read *does* instead.

## 1. Detection

Two types. A `Read` is one served read; a `Stream` is a consumer moving
through a file.

```rust
struct Read {
    begin: u64,
    end: u64,
    arrived: Instant,     // the consumer asked
    returned: Instant,    // we finished serving it
}

struct Stream {
    file: FileIdx,
    reader: ReaderId,     // which response is feeding it now
    begin: u64,           // where this connection began serving
    end: u64,             // how far we have served
    last: Read,
    rate: Ema,            // bytes consumed per second
    reads: u32,
    seen: Instant,
}
```

A read `R` joins a stream `S` when it is the same file and

- `S.begin <= R.begin <= S.end` -- it starts inside what we have served, and
- `R.end > S.end` -- it extends past it.

Ties go to the stream whose `end` is nearest `R.begin`. No match makes a new
stream, and a stream unseen for `STREAM_IDLE` expires.

### Why the span, and not a tolerance

A reopened connection does not resume where we stopped sending. It resumes
where the *consumer* stopped consuming, which is behind: the difference is
what we had written into the socket that mpv never read. Measured over one
session, every reopen landed 1.15 MB to 9.0 MB behind our send position, and
never once ahead.

An earlier draft matched with a `MAX_UNCONSUMED` constant for that overhang.
The stream's own span replaces it, which is better: the tolerance stops being
a number to guess and becomes "anywhere inside what we have already served
you", which is what it always meant.

### Why "extends past it" is load-bearing

Three cases all begin inside the served span, and only the first continues
the stream:

| | begins inside | ends past | outcome |
|---|---|---|---|
| resume after a reopen | yes | yes | extends the stream |
| backward seek into served territory | yes | no | new stream |
| a second track, once the first stream's span is long enough to contain it | yes | no | its own stream |

The third row is the one that matters. On a connection that runs without a
seek, the video stream's span reaches the end of the file, so the second
track's reads *do* start inside it. Their ends are inside it too, so they do
not match, and the two playheads stay apart. Without this clause the design
silently merges them, which is the exact failure it exists to prevent.

`S.begin` resets to `R.begin` whenever `R.reader != S.reader`, so the span
stays bounded by the seek interval instead of growing to the whole file.

### Rate

```
overlap  = min(S.last.end - R.begin, S.last.size)
consumed = S.last.size - overlap
gap      = R.arrived - S.last.returned
sample   = consumed / gap
```

`consumed` is what the consumer ate, not what we sent. In one measured case
we served 21,757,952 bytes ending at 3,478,455,795 and mpv resumed at
3,475,071,898, so 3,383,897 of that was still in the socket and it had truly
consumed 18,374,055. Crediting the full read overstates the rate by 18%.

`gap` runs from **our return** to **its next arrival**, and the pairing is not
arbitrary. Any other one books our own fetch latency as consumer think-time:
a stream blocked four seconds on a missing piece then measures as slow, gets a
smaller window, and stays blocked. Arrival-of-new minus return-of-previous is
pure consumer time by construction, because our stall happens after the read
has arrived.

An earlier attempt to measure a rate -- `DeliveryRate`, deleted -- sampled
bytes leaving the server over short windows and produced 3 B/s and then
17 B/s on successive runs, each of which collapsed the window onto its floor.
Sampling the consumer's own appetite, over gaps it chooses, has no such
failure mode.

### The cap, which is also the cold start

The EMA starts at `file_size / duration` and is capped there.

It has to start somewhere, and a stream that has served one read has measured
nothing. It also has to be capped, because a player filling its own cache
drains as fast as the socket allows: the early samples are not a consumption
rate at all, and uncapped they would size the window off how fast we can
deliver rather than how fast the film plays. The film's average bitrate
answers both -- it is the rate a video stream converges on anyway, so the
cap only ever binds while the measurement is meaningless.

This is the one number the server cannot work out for itself, and it is why
`note_duration` survives when `note_playhead` does not: a cap derived from
measurement is not a cap. The API shrinks from a position every second to a
length once per film.

Two things it is not. It is the *whole file's* rate -- every track together --
so capping each stream at it individually is loose, since it is the sum of
all streams that cannot exceed the bitrate; only the video stream ever
approaches it, so per-stream is the simpler and safe choice. And it is an
*average*: through a dense scene the true local rate is higher and the window
is undersized by the difference. That is the same variable-bitrate error that
has come up twice before, in a place where it costs a little lookahead rather
than a misplaced window.

## 2. The want set

Per stream, `S.end .. S.end + lookahead_seconds * rate`, with two additions
that are not tunables.

**The demand floor.** A piece a reader is *parked on* is wanted
unconditionally at top priority, whatever the rate says. Without it the rate
is a starvation loop in the other direction: a blocked stream consumes
nothing, so a rate-sized window is empty, so it stays blocked. This set is
also the natural producer for the deadline pieces librqbit now splits across
peers (`rqbit` `3b387e11`, `7892816e`).

**One piece past the current one.** A sequential reader that is given only
the piece it is sitting in reads to the boundary and blocks -- literally the
5560 pattern, every read ending exactly at `23,320,330,240`. A second track
at ~20 kB/s over a 90 s profile wants 1.8 MB, less than half a 4 MB piece, so
without this guarantee it blocks at every boundary however generous the
profile is. The demand floor covers the piece it is in; this covers the next.

### Sharing the lookahead between streams

When disk cannot cover every stream's profile, solve for a single `t` such
that `sum(rate_s * t) <= lookahead_budget` and give every stream `t` seconds.

Equal *time*, not equal *bytes*. Playback is gated by the worst track, so a
90-second subtitle runway buys nothing while video has two seconds; the only
allocation that makes sense maximises the minimum. Scaling proportionally by
bytes happens to produce equal time for free, since bytes are rate times time.
Splitting the budget into equal shares does not, and is the trap: 10 MB split
two ways gives video 1.4 s and subtitles 250 s.

Round each stream up to the next-piece guarantee afterwards, rather than
subtracting floors before the solve. Two pieces on two streams is 16 MB; a
device with less free space than that is not playing video, so this is not a
case the allocation has to be shaped around.

## 3. Retention

One number per piece: `last_useful = max(fetched_at, last_read_at)`. Evict
the lowest first. `last_read_at` is new bookkeeping; nothing tracks it today.

Two tiers sit above the LRU and are not evicted while they hold:

1. **Committed for seeding**, and **inside a live stream's want set**.
2. **Scrub-back** -- everything else that has been read. It takes whatever
   disk is left and gives it back the moment tier 1 grows.

Tier 1's second half is "never-read data counts as most recently read", with
a sharper predicate. The point of that rule is not to throw away the
lookahead we just fetched, and "is it wanted right now" says exactly that --
while also handling what the blanket rule cannot. Lookahead abandoned by a
seek is also never-read, so under the blanket rule it outranks everything
until all read data is gone; here it has an old fetch time, is not wanted, and
is the first thing evicted.

**Re-read promotion.** Effective age divided by `2^(reads - 1)`, to a ceiling.
A piece read repeatedly survives longer, which keeps the `moov` -- re-read on
every seek -- with nobody knowing where it is. This is what retires the
container-parsing branch for good: we do not need to find the metadata if the
reads point at it.

## 4. The disk budget

There is no configured cache ceiling. Data is dropped when a stream closes or
the viewer starts a different one (pins excepted), so the cache is already
bounded by the session; a `cacheSize` setting under that is a second ceiling
below a real one.

```
available = our_usage + (free_disk - margin)
```

**`our_usage` has to be in there.** Computed as `free_disk - margin` alone,
the budget shrinks as we fill it and never converges -- we would stop well
short of the disk and nothing would explain why. Today `cacheSize` binds, so
this never shows.

**The margin is load-bearing in a way it was not.** Filling to the edge makes
ENOSPC the steady state rather than an accident, and the current recovery for
a full volume is a torrent error plus a reconciler restart -- there are tests
named for it (`an_errored_torrent_is_restarted_only_when_a_full_volume_killed_it_and_has_cleared`).
That path is fine as a rare accident and bad as normal operation, so the
margin wants to keep us off it entirely and eviction should trigger on
approaching it, not on hitting it. On Android, sitting at the edge also
reports low storage to the user and to every other app.

The proxy backing works the same way over the chunks it downloads, with tier 1
being lookahead alone since it has nothing to seed.

## 5. What this deletes

`PlaybackIntent` and `playback_intent_for_request`; `is_container_metadata_request`,
`container_metadata_window`, `CONTAINER_METADATA_FRACTION`; `STRUCTURAL_PIECES`;
the `Reading::{Playback, Probe}` split; keep-windows, want-windows,
`Door::windows_now`, `window_at`.

**And the told playhead entirely**: `note_playhead` on `ServerHandle` and the
FRB surface under it, `Told`, `TOLD_FRESH`, `read_near`, `READ_VICINITY`, the
playhead fallback tiers, and `Pass::drift`. Its last job was placing a window
round where the viewer is so that scrub-back stayed cheap, and scrub-back is
now tier 2: it exists when there is disk for it and does not when there is
not, which needs no position. `note_duration` stays, for the cap in §1.

That deletion crosses the repo boundary -- xtremio reports the playhead once a
second (`_reportPlayhead`, `PlayheadReport`, `PlayheadReporter`) and its pin
would stop compiling. The two changes land together, in stage D.

## 6. Open

Nothing about the shape. What is left is the ordering of the stage-D
deletion against xtremio's pin, and whether the lookahead cap wants to apply
to the sum of the streams rather than to each of them, which the Phase A
trace will say more about than an argument will.

## 7. What a stream is

**A stream is a contiguous run of bytes on disk.** A fetch outside what we
hold starts a new one.

Not "what a stream asked for", which is easier to answer and was proposed
and rejected. The reason is not purity. Membership from disk makes the
back-scrub tolerance scale with the disk by itself: a large disk keeps more
history, so a scrub back lands inside a run and joins; a small one keeps
less, so the same scrub lands outside and is a new stream. It both allows
and limits back-scrub membership, and it is why none of the rules below
needs a tuned distance.

Three rules were tried against the field before this one, and each was a
guess about byte geometry:

- *Inside the stream's whole span, and extending past it.* Right at the
  granularity the design was drawn at, a whole HTTP response; wrong at the
  granularity reads arrive. One read is at most 256 KiB and a reopen resumes
  1.15-9.0 MB behind, so the first dozen reads of a resumed connection reach
  past nothing. 43 streams on a film with two tracks.
- *Within 16 MB of the last read.* Removed the span's failure and brought a
  constant that is in bytes while the thing it separates is in time: 4.5
  seconds on this film, 32 on a low-bitrate one, under a second on a high one.
- *Inside what this connection has served.* Bounded by a measurement rather
  than a guess, and still wrong: a long-lived connection widens the span
  until a second track twenty gigabytes away falls inside it.

## 8. Staging

The old policy cannot be left running alongside the new one -- it would have
to be the one deciding, which is the thing being replaced -- and is useless
if it is not. So: build the new one standalone, prove it against scaffolded
scenarios, and swap it in as one commit.

The harness comes first and proves itself before the policy exists, by
reproducing the field failure against the *old* policy:
`planned to reclaim inside an open stream's lookahead pieces=[5560,5561,5562]
reader_start=5559`. A harness that cannot reproduce the bug cannot prove the
fix. It also settles something two surveys disagreed about -- whether that
starvation was the keep set or the want set -- before anything is written.

Order:

0. **An in-memory held set for the proxy, maintained by the owner.** The
   torrent already has one (`StoreRegistry` -> `HeldBits`, and
   `Engine::held` never touches the filesystem); the proxy never got one and
   `read_dir`s its buckets on every pass. Membership from disk has to be
   answerable on the read path, where a listing is out of the question. This
   is also the invariant the common owner was built for -- it owns every
   filesystem mutation, so the mirror is derivable and authoritative, and
   the one honest `read_dir` left is the one that seeds it at startup.
   Chunk_store.rs:510 records what the listing costs when it lies: a
   transient directory error read as "empty" withdrew every committed piece
   of a file from what we announce, after peers had been told, and there is
   no un-Have.
1. The scenario harness, proved against the old policy.
2. Membership from disk: runs, births, and the one-past-the-edge clause.
3. Rate from the consumer, with **slow start** -- seeded at zero, not at the
   film bitrate. Seeded at the bitrate a new stream is handed 315 MB on a
   90 s profile; one-shot that is ~12 MB actually fetched, which does not
   re-buy the 1.6 GB probe regression, but 46 reopens in 70 seconds that
   miss is 0.5-4.6 GB, which does. Slow start makes a spurious stream cost
   8 MiB, and the film reaches its full window in under a second.
4. The want set: equal seconds, the demand floor, the next piece.
5. The LRU, the per-piece ledger, and the tiers.
6. The exempt bitmap: one `Arc<[AtomicU64]>` bit per piece, written only by
   the owner under L2, so the Door does one load and a bit test and takes no
   lock at all. The Door must never become a writer.
7. The disk budget of section 4.
8. The swap, as one commit -- which also deletes the told playhead, and so
   lands with xtremio.
