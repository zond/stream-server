# Known issues, redundancy and stale docs

Verified against the tree at `fbb8d79` on 2026-09-14. Every entry here was
checked against the code on that date, not carried over from notes: two
items on the previous list (a standalone binary binding `0.0.0.0`, and a
JNI embed that never reclaims) turned out to describe crates this workspace
no longer has, which is why this file exists and the notes did not.

The workspace is two library crates, `enginefs` and `server`. There is no
binary and no JNI crate; the only embedder is xtremio, which links `server`
through flutter_rust_bridge.

## Redundant

### Dead modules -- deleted

The four modules this list carried -- 1,332 lines, each reachable only
through its own `pub mod` line, and `pub`, which is why `-D warnings` never
caught them -- are gone, with the 13 tests that exercised code nothing ran.
Two of them were the last of the design dropped in `fbb8d79`: inspect the
container, guess where its index is, treat those pieces specially. The
read-pattern detector answers that from behaviour instead.

### Smaller

* `Consumers::at`, `Streams::busiest`, `Stream::eaten` live only for
  `trace::pass` -- the entity head it prints and the `held_behind`/
  `held_ahead` split. Nothing keeps, fetches or reclaims because of them.
  They die with the trace module.
* `Shape::Split`'s `window` is no longer a window. One production read
  (`stride_for`, and only under `Trigger::OnMove`, so the proxy; the torrent
  is `Trigger::External` and never reads it), plus it is the subtrahend that
  sizes `committed` in `shape_for`. Misnamed rather than unused: it is "the
  part of the budget the sharing draw may not have".
* `Shape::piece_budget` is called only from tests.

## Stale docs

**Fixed on 2026-09-14.** The five entries this list carried -- the intra-doc
links to `Door::windows_now`, `RetentionPolicy::ahead_of` and
`Retention::note_playhead_at`; `README.md`'s buffer-profile table;
`AGENTS.md:23`'s `note_playhead`; this design document's "what to delete"
and "what is left of the old model"; and `pin_record.rs`'s standalone
binary -- were all rewritten against the code as it stands.

Found while fixing them, and fixed with them: the doc block of a deleted
method left sitting over the method after it
(`ServerHandle::note_duration`, `Retention::note_duration` -- both were
carrying the told playhead's documentation, including a link to a deleted
`EngineFS::on_playhead`); the broken links `Door::window_now`, `Told` and
`streams::Reading`; and the "observed, never obeyed" headers on
`retention::streams`, `retention::exempt` and `retention::ledger`, each of
which a pass now obeys.

Still stale:

* `AGENTS.md` describes the retention policy as a rolling window "90%
  ahead, 10% behind" (three places on the `enginefs`/proxy rows) and the
  driver as "fed the playhead a reader actually reached". What is kept is
  now the forward run each detected consumer is being fetched into, plus
  what an open read was promised; what is behind a consumer survives as the
  coldest thing on the disk rather than as a tenth of a window. The README's
  two statements of the same thing were corrected; `AGENTS.md`'s were left
  for a pass over that file.
* The **Phase A** label on the detector's plumbing -- `owner.rs`,
  `engine.rs`, `files.rs`, `proxy_retention.rs` and `proxy_cache.rs`, about
  a dozen places -- reads as "carried for a trace line, obeyed by nothing".
  It is load-bearing now. One sweep, when the trace module goes.
* `retention::streams::REPORTED_SECONDS` is unused: its own doc says it goes
  when the want set is wired to the policy with the real number, which has
  happened. A deletion rather than a doc fix.
* `enginefs/src/retention/scenario.rs`'s `CONTAINER_METADATA_LOOKAHEAD` and
  `PLAYBACK_LOOKAHEAD` keep the field's numbers under the names of
  constants that no longer exist. Deliberate -- a scenario should stay the
  measurement it was taken from -- but it reads as drift.

## Open issues

### In the field, unexplained

Both from the 2026-09-14 04:20 log, on `xtremio 836394c` /
`stream-server fb85c49` / `librqbit 9897615c` (the fork with piece
splitting; confirmed in `Cargo.lock`).

1. **A read blocked 29.6 s on piece 872 and 21.3 s on piece 889** while the
   swarm delivered 5-7 MB/s from 20+ connected seeders and the piece was at
   the front of the want set. Splitting mitigates slow peers; it does not
   promise a deadline, so this is a thing to explain rather than a verdict
   on the split. Nothing currently logged says which peers held that piece
   or what they were doing with it.
2. **About 210 MB of 592 MB fetched never passed a hash check** over the
   logged window -- `fetched_bytes` against `downloaded_and_checked_bytes`,
   both monotonic counters, summed over the pass lines' deltas. `unlinked`
   was 0 throughout, so it is not deletion. Candidates: chunks in flight
   (bounded, should not accumulate), duplicate chunks, or pieces cancelled
   mid-flight. Neighbouring symptom: six
   `we already requested ChunkInfo { piece_index: 895, chunk_index: 186..191 }`
   warnings, all to one peer.

These two may be one thing. A claim that overlaps another, or is re-issued
while its chunks are still outstanding, would waste bandwidth re-fetching
what we have *and* leave the blocked chunk unfetched.

### Open in this repo

3. `begin_retention` (which counts the open) and `reader_on` are separated
   by the `get_file_reader` await -- `engine.rs:2189`, `2227`, `2242`. A
   pass that takes both its readings in that gap empties and forgets the
   entity, and the stream handed out ends up with no owner reader, so the
   file is never bounded.
4. A zip **inside a torrent** inflates on a reactor worker, and that is a
   deliberate trade rather than an oversight -- `archives/zip.rs:153`. A zip
   on disk is already inflated on a thread of its own driving the reader
   through `runtime.block_on`; the torrent form stays a `tokio::spawn`
   because its reads park waiting for pieces and a plain thread parked in
   one is never woken when the runtime goes down, so it would hang
   shutdown. `INFLATE_CHUNK_BYTES` (256 KiB) is what keeps it to bursts
   between yields.

   The trade may no longer be necessary. `engine.rs:1166` keeps a
   `read_wakers` map and `files.rs:318` has parked reads check
   `reads_refused()` and fail -- machinery that exists for "stopped for
   space", and that already proves the engine can reach into a parked read
   and end it. A shutdown that used the same path would let the torrent
   form move to a thread like the disk form. Not a bug; an improvement with
   a prerequisite.
5. `enginefs/src/retention/trace.rs` is still compiled in. Its own header
   says to delete it once a field log shows the pass settling; that is the
   call to make after the next viewing.
6. `server/tests/embed.rs` has 19 sleep-based waits; three of them are
   negatives that need an enginefs tick hook to be assertions rather than
   guesses.
7. The narrow boundary-piece race around a pin landing mid-drop. The next
   tick heals it in almost every case.
8. `unrar-rs`'s licence asks that binary redistributions reproduce its
   licence file; packages ship the GPL text but not that file.

### Other repos, not verified on this date

* xtremio's Windows and macOS builds have only ever run in CI; iOS does not
  build at all (`librqbit-dualstack-sockets` `bind_device`).
* xtremio's remove/add race when a title is re-added while being removed,
  and `remove()` never releasing a row's replaces debt.
