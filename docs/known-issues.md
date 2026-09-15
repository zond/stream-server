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
* `Shape::Split`'s `window` was no longer a window -- one production read
  (`stride_for`, and only under `Trigger::OnMove`, so the proxy; the torrent
  is `Trigger::External` and never reads it), plus it is the subtrahend that
  sizes `committed` in `shape_for`; misnamed rather than unused, it is "the
  part of the budget the sharing draw may not have". **Renamed `unshared`
  in `ff1605b`**, which is the name to grep for now.
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
`EngineFS::on_playhead`; a third, `EngineFS::on_duration`, carrying the
same block with a link to the deleted `Retention::note_playhead`, was
missed here and fixed by the review of the 14th); the broken links
`Door::window_now`, `Told` and
`streams::Reading`; and the "observed, never obeyed" headers on
`retention::streams`, `retention::exempt` and `retention::ledger`, each of
which a pass now obeys.

Still stale:

* `enginefs/src/retention/scenario.rs`'s `CONTAINER_METADATA_LOOKAHEAD` and
  `PLAYBACK_LOOKAHEAD` keep the field's numbers under the names of
  constants that no longer exist. Deliberate -- a scenario should stay the
  measurement it was taken from -- but it reads as drift.

## Open issues

### Open in this repo

Each entry carries its plan, agreed with zond on 2026-09-15. Order is the
order to do them in.

- **Nothing.** Every item that was open here on 2026-09-15 is under
  "Closed" below with what was done about it.

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

## Closed 2026-09-15

Closed by zond after the fifth field log. Kept here in one line each so
nobody reopens them without knowing why they were shut.

- **Head-of-line blocking.** Verified fixed: the fifth log (xtremio
  `7de2dea`, rqbit `cc969c7b`) had no read wait over 1.5 s. The design is
  rqbit's `crates/librqbit/src/CLAIMS.md`.
- **The pin set read at one moment and acted on at another.** Fixed
  2026-09-16, and narrower than the plan said. The plan proposed moving the
  pinned set into the retention owner's state. Read against the code, the
  owner is per torrent already and the pin set is one shared set with one
  writer path; what the design (`proxy-retention-owner-design`: decide
  inside, act outside, report back) requires is that the reading that
  decides is taken immediately before each act, and that the act's own
  window -- foreign, awaited code that no lock may span -- is reconciled
  after it. The reclaim step already did both. The want step read the pins
  once, in `alone`, and then awaited one drop per run: a pin landing under
  the first run's drop reached the last run as a plan made without it, and
  the neighbour's boundary piece was dropped and wanted back. Now each run
  is read against the pins as they stand when it is about to go
  (`TorrentBacking::want`, `drop_run`), and `rewant_pins_crossed_by` stays
  as the reconciliation of the act itself. Test:
  `a_pin_taken_under_the_want_steps_first_run_keeps_the_piece_out_of_the_second`,
  which fails when the plan's reading is trusted again. The residual cost
  of a pin landing *inside* an act is unchanged and inherent: one boundary
  piece fetched twice.
- **`removed_files` insert-only.** Fixed 2026-09-16 by making a write take
  the file back out: `pwrite_all` for a file removes it from the memo, so a
  file removed and then written again -- re-added, re-pinned -- counts as
  present when its neighbour is removed and the piece they share stays.
  The memo itself stays, because it is what lets a whole-torrent delete
  take a boundary piece once every owner is gone; the store has no view of
  the selection to decide from instead. Test:
  `a_file_written_again_after_its_removal_keeps_the_piece_it_shares`,
  which fails without the write-path removal.
- **The two clock-bound tests.** Fixed 2026-09-16. The swarm tests no
  longer ask how much may land across one 50 ms pass; "at rest" is a pass
  across which nothing came off the swarm with the reader parked and the
  disk unchanged, counted to ten in a row, and the wait is bounded only by
  the test's deadline. The plateau is read during the pause, where the
  torrent is live, not after playback, where the finished torrent is paused
  and has no counter to read (which is why the old count was never reset).
  The precondition test parks the player and lets the fill stop moving
  before it asks whether the cache filled, and waits for the swarm to stay
  quiet rather than for a rate. The LAN test's probe thread reports its
  first served request before the toggling starts. Three runs each under
  a full CPU load passed; a churn mutation (the whole file re-wanted every
  pass) still fails the test, on the total-fetched bound.
- **The retention trace module's keep-until-verified hold**, and then
  the trace and mpv's verbose log themselves: **built as a setting**
  (stream-server `7acaa6e`, xtremio the same day). "Verbose logging" in
  Settings → Developer is a device preference; the app pushes it to the
  server as `diagnosticsTrace`, which flips the `enginefs::retention::trace`
  directive in the running process's log filter and is applied at every
  start, and the next player opened reads it for mpv's log level, the
  `msg-level` override and the engine-log filter. Off by default. The
  trace module is a feature now, `staged_over_held` keeps its reader, and
  the `stream_request` lines stay always on.
- **A stalling link shrinks the window that would have ridden it out.**
  Accepted as a cost of streaming a torrent. The seeded rate stops one
  sample collapsing the window; a sustained bad patch still shrinks it.
- **A seek to unheld ground ramps its window from the two-piece floor.**
  Accepted as a cost of streaming a torrent: audio, subtitle and video
  streams cannot be told apart, so a new stream has no window to inherit.
- **The zip-inside-a-torrent reactor trade.** Not going to be worked on.
  The archive route's torrent reader is a raw `FileStream`, not a
  `FileHandle`, and a thread parked in it would never reach
  `ABANDONED_AFTER`.
- **xtremio's desktop builds only run in CI, and iOS does not build**
  (`librqbit-dualstack-sockets` `bind_device`). Not going to be worked on.
- **The detector dragging a viewer's stream to mpv's tail crawl.** Fixed
  in `78663e6` (the byte-weighted position); it was still listed under
  "not yet fixed".
- **xtremio's remove/add race.** Read against the code on 2026-09-15: it
  is guarded. `remove()` marks the row `pending_removal` before it unpins
  and forgets it only if it still names the same file; a re-add overwrites
  the mark, and a pin landing for a removed row is released
  (`rust/src/downloads.rs:1579-1688`, tests from
  `a_removal_forgets_and_unmarks_only_the_row_it_marked` on).
- **`remove()` never releasing a row's `replaces` debt.** By design: the
  old pin is carried until the new one is confirmed and paid at the next
  boot by `release_replaced_in` (`downloads.rs:1738`), so a kill
  mid-removal cannot lose it; `replacing_a_row_that_wants_no_pin_owes_no_release`
  is the test. What has no test is a debt that is never paid because that
  boot reconciliation never runs; noted, not an issue.

## First field log on the latency claim rules (2026-09-15 06:09, xtremio a58f5f0)

Two head-piece reads still blocked long with a fast swarm: piece 5 at
start for 24.6 s (15 seeders, 5-20 MB/s going to pieces 6-27) and piece
5560 after a seek for 30.1 s (22 seeders; the whole swarm at 160-350 KB/s
and nothing completing for 24 s). Doubling was gated on the claim having
delivered nothing, so a holder trickling a chunk a second was never
rescued; rqbit `d4020903` widens doubling to the one rule (any outpaced
holder). The idle-swarm shape of the 30-second case is not explained by
that alone. stream-server `71f555aa` adds `blocked_read_claims`: two and
ten seconds into a wait, the piece's holders with chunks missing, wait since
last delivery and own latency. **Read the next log for those lines.** Also
fixed: eighty "we already requested" warnings in ten milliseconds after a
head cut (now debug).

## Second field log on the latency claim rules (2026-09-15 06:54, xtremio ef6ac8c)

The `blocked_read_claims` lines answered: piece 0 blocked 15 s with **ten of
sixteen shares untaken** while three peers each held their two and fetched
whole pieces deeper in the window (the "delivered since the last handout"
gate never let them back in); whole pieces held by single peers with 3-6 s
of latency and 128 requests in flight, delivering every few milliseconds
and so never "quiet", took seconds each. Both are invisible to a clock set
by the holder's last delivery. rqbit `a27fa9cd` measures every takeover --
cut, double, share beyond one's own -- against the piece's age in flight
instead (CLAIMS.md, "The one measurement"). Also: one `staged_over_held`
on piece 3181, the write-into-a-finished-piece race, seen once; the probe
fired for reads whose response had closed (fixed, `0fc8b1f7`); the
startup bar counts landed chunks (same commit).

## Third field log on the latency claim rules (2026-09-15 08:56, xtremio ad69105)

The piece's age as the clock did it: piece 0 in 1.0 s (was 15.1 s), first
frame 2.8 s after open (was 22.6 s), no read waited over 2.1 s across four
seeks (was 24 and 30 s two logs ago). What a seek still costs is 4-5 s,
made of three or four sequential 1.1-1.9 s piece waits: a seek to unheld
ground is a new stream, its window starts at the two-piece floor and
doubles once a pass, and mpv's post-seek burst outruns that ramp for ten
seconds. Inheriting the last live stream's window was proposed and
declined by zond: audio, subtitle and video streams cannot be told apart,
so a new stream cannot know whose window to inherit. Left as it is. The
probe's stale `holders=0` lines (a task from an earlier park firing
against the next) are fixed in `735b3ca3`.


## Fourth field log on the latency claim rules (2026-09-15 20:31, xtremio 2272a51)

A slower swarm than the third log's (connected peers' last latencies
0.5-9 s, 12-30 seeders connected, 8-18 MB/s) and a worse run: stalls of
7.9 s (open), 3.7 s, 3.7 s and 7.7 s, with reads waiting 5.4 s on piece
2143 and 6.0 s on piece 1144. The probe lines say why, and it is not the
comparison -- both are cases where the comparison was never asked.

**Piece 2143 (5.4 s): ten of sixteen shares in the pool for two seconds
with nobody asking for them.** At the probe the piece was 2.1 s old; two
peers had finished two shares each (last latencies 162 ms and 1.2 s, both
outpacing the piece many times over), one peer with 5.5 s latency held
two shares undelivered, and 96..256 was unclaimed. The fast peers were not
refused -- they never came back. A peer that finishes its two free shares
asks again at once, finds the piece a few milliseconds old, which nobody
outpaces, is turned away (`Crowded`) and the walk goes on to hand it a
**whole** piece deeper in the window. The request loop then sends every
one of that piece's 256 chunks before it asks for anything again (two
128-chunk windows), so the peer is gone for seconds. By the time the head
piece is old enough to be shared out, everyone who could share it is
committed elsewhere. Pieces 5559 and 1123 show the same shape from the
doubling side: the doublers (0.5-0.7 s latency) arrived at 1.5-1.9 s of
piece age, one whole piece later than the rule would have let them in.

**Piece 1144 (6.0 s): a claim held 14.6 s by a peer that never delivered
a chunk, doubled once by a 5 s peer that then sat on it for 8.4 s, and
`MAX_HOLDERS_PER_CLAIM` (2) refusing everyone else.** The piece was handed
whole to a fresh peer during the window ramp, cut when the head reached
it, and everything but the holder's own claim was fetched by others; the
one claim left had two holders, both dead weight, and the count cap kept
the fast peers off it. (A dead peer's claims go back to the pool in
`release_pieces_owned_by`, so this holder was alive and silent.)

Two changes proposed to zond and approved; built as rqbit `cc969c7b`:

1. **Ask the head at every request slot, not at every piece boundary.**
   A slot frees when a chunk lands, which is the moment the peer's latency
   is re-measured and the event the rule is about. Before each chunk of a
   whole piece the peer would offer itself to the two head pieces (pool
   share or double, the same rules), take what it is given first, and
   resume the whole piece after. Cost: one short walk under the state
   lock per chunk landing, a lock the write path takes per chunk anyway.
2. **Replace the holder count cap with the same comparison at the claim:
   another holder may join a claim iff its latency is shorter than the
   time since the claim's newest holder was handed it.** A healthy holder
   finishes a 16-chunk claim within one of its round trips, so a claim
   still open that long is only joined by someone faster than its newest
   holder; a silent one is joined by anyone live. No count and no
   constant, and it implies outpacing the piece.

Also seen, not to fix: the first tail read after one seek started 12 KB
before the tail stream's earliest piece (5559 rather than 5560) and paid
2.4 s for 4 MiB nobody else wanted -- once, and the piece is held after.
Piece 0 took 3.6 s of which 1.7 s was before any peer had connected to
reserve it. The startup bar reported 15/16 chunks; the probe reported no
stale lines.

## Fifth field log on the latency claim rules (2026-09-15 21:11, xtremio 7de2dea)

The two changes of rqbit `cc969c7b` in the field, on the same torrent as
the fourth log and a comparable swarm (6-17 peers, 5-16 seeders,
5-10 MB/s): **no read waited two seconds, so the probe never fired.** The
longest piece wait was 1.49 s (piece 0 at open); the other ten waits over
a second were 1.0-1.5 s, one round trip of this swarm's typical peer.
Stalls: 3.8 s at open (piece 0 plus the tail read), then 3.0, 3.8, 1.5 and
1.6 s across four seeks -- against 7.9, 3.7, 3.7 and 7.7 s an hour
earlier. The 3.0 s seek had no piece wait over a second at all: what is
left of a seek's cost is the new stream's window ramp, which zond has
chosen to leave as it is. Nothing to fix from this log.
