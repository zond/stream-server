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

This is also where the bitrate comes from. Bytes consumed per second of real
time *is* the bitrate, so `note_duration` and the size-over-duration
arithmetic stop being needed for anything. An earlier attempt to measure a
rate -- `DeliveryRate`, deleted -- sampled bytes leaving the server over short
windows and produced 3 B/s and then 17 B/s on successive runs, each of which
collapsed the window onto its floor. Sampling the consumer's own appetite,
over gaps it chooses, has no such failure mode.

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
`Door::windows_now`, `window_at`; `note_duration` and bitrate-from-duration.

## 6. Open

**Does the told playhead survive?** Its remaining job is scrub-back. The LRU
ages by *read* time and mpv reads minutes ahead of the viewer, so "keep 30
seconds behind" measured from reads is not measured from the viewer. If
instant scrub-back matters it stays, for biasing age and nothing else;
`read_near`, the fallback tiers and `TOLD_FRESH` go either way, and if it does
not matter the app stops reporting at all.

**The buffer-fill phase.** While mpv is filling its cache it drains as fast as
the socket allows, so early rate samples read as near-infinite. A huge opening
window is deliberately fine -- download speed and disk bound it in practice,
and `iter_next_pieces` walks a stream's queue in playback order, so an
oversized want set still fetches the useful end first. Whether it needs a cap
at all is a question for the Phase A trace rather than a constant to pick now.

## 7. Staging

**A. The detector, running alongside, trace only.** One field session says
whether it reports exactly two streams on that film, with sensible rates, and
nothing else. Everything downstream is wrong if this is wrong, and it is one
APK away from being known.

**B. The want set from streams.** Should fix the 5560 starvation on its own,
and is worth having whatever happens to the rest.

**C. The LRU replaces keep-windows.** The larger and riskier half.

**D. Delete the classification code.**
