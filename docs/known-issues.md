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
* `README.md` around line 374 documents the `/stream-numbers.json`
  `transfer` object as `downloadedBytes`/`uploadedBytes`/`ratio`. The
  server also sends `unverifiedBytes` and, beside it, `refusedReclaims` --
  the two numbers the retention work added and the ones a client most needs
  to draw. The documented wire shape is two fields behind the real one.
* `enginefs/src/retention/scenario.rs`'s `CONTAINER_METADATA_LOOKAHEAD` and
  `PLAYBACK_LOOKAHEAD` keep the field's numbers under the names of
  constants that no longer exist. Deliberate -- a scenario should stay the
  measurement it was taken from -- but it reads as drift.

## Open issues

### In the field

**Settled 2026-09-14 (`73f77b0`, rqbit `bb6bad9b`).** The unverified figure
was the duplication the fork itself was doing, and the numbers close:

| log | fetched | verified | unverified |
| --- | --- | --- | --- |
| 04:20 | 592 MB | 382 MB | 35.5% |
| 06:52 | 416 MB | 281 MB | 32.4% |
| 15:05 | 708 MB | 668 MB | **5.7%** |

Three changes, in the order they mattered: a request is filtered against
`chunk_status` before it goes out; splitting and second copies are confined
to the first two pieces of the lookahead, so every piece past them is
fetched by one peer and stolen if that peer is ten times slower; and a
second copy is paid for by a delivery of the same piece, against a claim
that has delivered nothing over at least as long. 5.7% is about the
in-flight floor -- chunks of pieces not yet complete, which leave the
number when the piece checks.

1. **Head-of-line blocking**, which is no longer a bandwidth problem: in
   the 15:05 log a read blocked 13.4 s on piece 965 and 7.6 s on piece 966
   while the swarm delivered 12-16 MB/s from 16-17 connected seeders, with
   every other blocked read at 1.2-2.6 s.

   **A fix is in and unmeasured** (rqbit `f6753192`): `unclaimed
   .pop_front()` never asked how many claims of a piece the asking peer
   already held, and a peer comes back for another as soon as it has *sent*
   the last one's requests -- so with a 128-chunk request window against a
   16-chunk claim, two peers took all sixteen claims of a 4 MiB piece and
   it was fetched at the speed of the slower of them. A peer now takes
   `CLAIMS_PER_PEER` and moves to the next piece the stream needs; the
   shares it passed over are what it comes back to if the lookahead has
   nothing else, so a piece with few peers cannot stall for want of more.

   What says whether it worked: the long blocked reads, against an
   unverified figure that must stay near its 5.7% floor. If the reads
   shorten and the figure climbs, spreading is costing a round trip per
   peer and `CLAIMS_PER_PEER` is too low.

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

### Standing hazards

* **Pin by full sha, everywhere.** Cargo keys a git source on the literal
  `rev` string, so a short rev in one crate and a full sha in another are
  two sources -- and the whole torrent engine is compiled twice into one
  binary, which `cargo check` is perfectly happy with. It has happened
  twice: `server` and `enginefs` each carry their own `librqbit`
  dependency, and xtremio carries a third for test fixtures. The check is
  `grep -c 'name = "librqbit"$' Cargo.lock`, which must answer 1.
* **Never `git checkout <file>` to undo an experiment.** It restores from
  HEAD, not from the working tree, so it discards everything uncommitted in
  that file. Copy the file to the scratchpad and copy it back instead.

### Other repos, not verified on this date

* xtremio's Windows and macOS builds have only ever run in CI; iOS does not
  build at all (`librqbit-dualstack-sockets` `bind_device`).
* xtremio's remove/add race when a title is re-added while being removed,
  and `remove()` never releasing a row's replaces debt.
