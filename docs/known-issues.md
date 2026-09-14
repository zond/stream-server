# Known issues, redundancy and stale docs

Verified against the tree at `5c7923c`, last checked 2026-09-14. Every
entry here was checked against the code on that date, not carried over
from notes: two
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

2. **A stalling link shrinks the window that would have ridden it out.**
   Field log 2026-09-14 18:21, a wifi-to-mobile switch: `rates=[Some(367710)]`
   against a 3,568,061 B/s film, and a want set of eight pieces where the
   film's own arithmetic allows seventy-seven -- about nine seconds of
   buffer on a link that repeatedly dropped to 32 kB/s for tens of seconds.

   Why the measurement reads low there: a starving player asks again the
   instant it is answered, so its reads are refused by `Stream::sample`'s
   admission rule; what survives admission is the moments the player had
   some buffer and idled, and a long idle over a small read is a *low*
   sample. On a link that alternates stalling and bursting the admitted
   samples are systematically the low ones.

   **Half-addressed** (`77309fa`): the rate is seeded from the film's
   arithmetic rather than set outright by the first admitted sample, so one
   sample can no longer collapse the window. What is *not* addressed is a
   sustained bad patch, where the decay still arrives after a dozen-odd
   samples. Widening the memory was tried and put back -- it only helps a
   stream that is seeded, and a proxied stream states no duration, so the
   widening gave it the cost without the benefit (Windows caught it).

   If this needs more, the lever is the admission rule telling a slow
   consumer from a slow link, not a constant: those two produce identical
   evidence in delivered bytes, and the film's own size over its duration
   is the only number immune to it.

### Open in this repo

Every item below was verified against the tree on 2026-09-14 by a reading
whose job was partly to say "not real" or "leave it". Three did.

2. **The retention trace module stays, for now** -- `enginefs/src/retention/trace.rs`.
   Its exit condition is met (the pass has settled over three logs), but it
   is the *only* place `fetched`/`verified`/`unverified` reach a log at
   all: the permanent replacement is a lifetime total on
   `/stream-numbers.json` drawn in the app, not a line in the diagnostics
   ring a tester sends back. Item 1 above is unmeasured and its named check
   is the unverified figure. Deleting the instrument before reading the
   measurement it exists for is the wrong order. **Revisit after the next
   viewing on `f6753192`.**

   When it goes, the cascade is larger than it looks, and two things are
   decisions rather than consequences: `server/src/routes/stream.rs:1179`
   is a fifth call site the compiler will *not* name, and
   `staged_over_held` (`piece_store/registry.rs:111`, `store.rs:163,480`)
   loses its only reader -- its own doc argues it is the only signal
   separating a read inside a sparse hole from quiet media, so it wants a
   permanent home beside `refused_reclaims` rather than deletion.

3. **The zip-inside-a-torrent reactor trade stays** -- and the prerequisite
   I recorded for it does not exist. The torrent-form reader is not a
   `FileHandle` at all: `server/src/routes/archive.rs:791` takes a raw
   librqbit `FileStream` through `TorrentHandle::get_file_reader` and wraps
   it in the route's own `BackendStreamWrapper`, so `read_wakers` /
   `reads_refused` cannot reach into it. The hang risk is also worse than
   recorded: `ABANDONED_AFTER` fires on the next *write*, which a thread
   parked in a torrent read never reaches, so it would leak forever holding
   the engine `Arc`, the cache handle and the stream. Leave it.

4. **The boundary-piece race is real but not where this file said.**
   `TorrentStorage::remove_file` has no per-file production caller --
   librqbit reaches it only on a whole-torrent delete-with-files -- so its
   boundary rule never races a pin. The race is in `Engine::reclaim`
   (`engine.rs:961`) and `Engine::want` (`:821`), where the pin set is read
   and then acted on with no lock spanning the two. The tick does heal it,
   though not by the mechanism the old note implied. Left alone; what would
   make it matter is a pin arriving faster than a tick.

   Also noted while looking: `store.rs`'s `removed_files` set is
   insert-only and never cleared on a re-pin. Unreachable today, and
   exactly the read-once-trust-later shape that keeps recurring here.

5. `server/tests/embed.rs`'s other fourteen sleeps stay. Each is the
   backoff tail of a bounded poll on a positive observable, guarded by an
   assertion against `CHECK_WAIT_BOUND`, so a regression fails rather than
   hangs. They decide how often a condition is asked, not whether it holds.

6. **The install-to-reader race is narrowed, not closed** (`e2ded80`). A
   reader that has opened and not yet delivered is not an *observed*
   reader, so a pass landing between `Opening::reader_on` and the first
   `poll_read` still reclaims. It can no longer forget the entity, which
   was the unrecoverable half.

7. `unrar-rs`: **not an issue.** xtremio has shipped `LICENSE-unrar-rs`
   inside the build since `25be0ac` (2026-09-12), and this workspace builds
   no binary to ship anything from. The obligation is narrower than
   recorded, too: the crate's LICENSE line 14 asks a binary redistribution
   reproduce "related license information", whose substantive payload is
   the unRAR restriction paragraph at its lines 26-34.

### Standing hazards

* **Pin by full sha, everywhere.** Cargo keys a git source on the literal
  `rev` string, so a short rev in one crate and a full sha in another are
  two sources -- and the whole torrent engine is compiled twice into one
  binary, which `cargo check` is perfectly happy with. It has happened
  twice: `server` and `enginefs` each carry their own `librqbit`
  dependency, and xtremio carries a third for test fixtures. The check is
  `grep -c 'name = "librqbit"$' Cargo.lock`, which must answer 1.
* **A green local test run is not a green gate.** xtremio's CI failed on
  `cargo fmt --check` in its `rust/` crate for nine consecutive pushes
  before anyone looked: `flutter test` passing locally says nothing about
  it, and the fmt gate runs before clippy and the tests, so the whole Rust
  job never ran. Check `gh run list -R zond/<repo>` after pushing -- and
  note that in the forks `gh` targets upstream unless `-R` names the fork.
* **Never `git checkout <file>` to undo an experiment.** It restores from
  HEAD, not from the working tree, so it discards everything uncommitted in
  that file. Copy the file to the scratchpad and copy it back instead.

### Other repos, not verified on this date

* xtremio's Windows and macOS builds have only ever run in CI; iOS does not
  build at all (`librqbit-dualstack-sockets` `bind_device`).
* xtremio's remove/add race when a title is re-added while being removed,
  and `remove()` never releasing a row's replaces debt.
