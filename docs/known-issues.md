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

### Dead modules

1,332 lines across four modules, each referenced only by its own `pub mod`
line. They are `pub`, which is why `-D warnings` never caught them. They
carry 13 tests between them, all exercising code nothing runs.

| Module | Lines | What it was |
| --- | --- | --- |
| `enginefs/src/backend/metadata.rs` | 987 | `MetadataInspector`, `ContainerType`, `KeyframeInfo` |
| `enginefs/src/piece_waiter.rs` | 126 | superseded by `files.rs`'s blocked-read path |
| `enginefs/src/metadata_cache.rs` | 117 | |
| `enginefs/src/metadata_pins.rs` | 102 | pinned the Cues/`moov` pieces a tail-seek request located |

The last two are the design deleted in `fbb8d79`: inspect the container,
guess where its index is, treat those pieces specially. The read-pattern
detector answers that from behaviour instead.

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

* Intra-doc links to deleted items: `Door::windows_now`
  (`retention/trace.rs`, `engine.rs` x2), `RetentionPolicy::ahead_of`
  (`piece_store/policy.rs`, `engine.rs`), `Retention::note_playhead_at`
  (`retention/scenarios.rs`).
* `README.md:313` gives a playing stream's lookahead as
  `128 MiB (MAX_SEEK_HOT_WINDOW_BYTES)`. It is the film's bitrate times the
  profile's seconds, with a 4 MiB fallback before a duration is stated.
* `AGENTS.md:23` lists `note_playhead` among `ServerHandle`'s methods. It
  was deleted with the told playhead.
* `docs/read-pattern-retention.md` section "what to delete" still lists
  `PlaybackIntent`, `playback_intent_for_request`,
  `is_container_metadata_request`, `Door::windows_now` and `window_at` as
  pending; they are gone. "What is left of the old model: `PlaybackIntent`
  survives as ..." is false.
* `enginefs/src/piece_store/pin_record.rs:20` names "the standalone binary"
  as a caller that gets `PinsUnknown`. There is no standalone binary. The
  mechanism itself is live and correct, and xtremio does hand in a pin set
  (`pins_applied applied=0` in the field log), so the never-reclaims
  condition does not arise in the only embedder there is.
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
4. A zip inside a torrent still inflates on the reactor:
   `server/src/archives/zip.rs` has no `spawn_blocking`. RAR, 7z, tar and
   tgz are on the blocking pool.
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
