# Known issues, redundancy and stale docs

Last checked 2026-09-16, against the tree at `bd48aac`. What was checked
on that date: the **open list**, which is empty; the **standing hazards**,
each re-run rather than re-read (`librqbit` resolves to one source in this
repo's, rqbit's and xtremio's lock files; CI is green on every pushed
head); and the **dependency pins**, which is how the three that were a
major behind were found and bumped. The **Redundant** and **Stale docs**
sections below still carry their 2026-09-14 reading and say so where it
matters; the **Closed** entries carry the date each was closed.

Every entry here was checked against the code rather than carried over
from notes: two items on the previous list (a standalone binary binding
`0.0.0.0`, and a JNI embed that never reclaims) turned out to describe
crates this workspace no longer has, which is why this file exists and the
notes did not.

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

Three more were still here when the 2026-09-19 review looked (row #50), and
this entry said otherwise: `cache.rs` (`CachedStream`, with an `unsafe impl
Sync` on a future it wrapped), `piece_cache.rs` (`PieceCacheManager`) and
`disk_cache.rs` (`DiskCacheManager`, reached only by a `BackendEngineFS`
field no constructor filled), together about 650 lines. They went with
`Engine::data_cache` -- a 64 MB moka cache built per engine that nothing
ever read or wrote -- and the `moka` dependency that existed for them.
`BackendMemoryDiagnostics::rust_piece_cache_{entries,bytes}` outlives them
as two fields that are always zero; they are part of a serialized
diagnostics shape, so they wait for whoever next changes it.

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
* **`cargo update` cannot tell you a dependency is out of date.** It only
  ever moves within the requirement it is given, so a workspace pinned a
  whole major behind reports as fully up to date -- `--dry-run` printing
  nothing means "nothing to do under these requirements", not "these are
  the newest crates". On 2026-09-16 that hid `dirs` at 6 (7.0 out),
  `unrar-rs` at 0.7 (0.10.5 out) and `rcgen` at 0.13 (0.14.10 out), two of
  them runtime dependencies. The check is to read each declared
  requirement against what crates.io lists as newest, which is a loop over
  `https://crates.io/api/v1/crates/<name>` and the `max_stable_version`
  field, and to do it for every workspace -- this one, xtremio's `rust/`,
  and rqbit's.

* **An engine with no reader downloads the whole torrent.** A new engine
  wants every file, and the want set only narrows when a stream arrives.
  Anything that creates engines early -- `/{hash}/create`, `/create`, a
  stats request that lands before the stream request -- fetches everything
  until a reader shows up (measured 2026-09-19: Tears of Steel, all 546 MB
  at ~50 MB/s). It is not hit today because the player's stream request
  follows its open by ~160 ms. This is why "start the engine when a source
  is picked" was not done for the cold start: it needs a discover-only
  state first, and rqbit drops peers when neither side is interested, so
  only addresses and metadata would survive it. The cold start itself
  (field 2026-09-17 16:36, on the phone) was a slow peer ramp -- peers found
  in 2 s, 2 connected for 10 s, 32 after 50 s -- not discovery; the
  `stream_progress` line now carries `queued`, `unique` and
  `connection_tries` to tell "nothing found" from "nothing answering".

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

## Sixth field log: the same film on a phone (2026-09-17, xtremio e861525)

The 20 GB film that juddered on the television ran on a phone on the same
wifi, verbose logging on: **same stalls, same shape.** The film wants
3.57 MB/s; the swarm delivered 4.4-7.1 MB/s fetched and 2.4-6.6 MB/s
verified, so the margin was thin and every straggler showed. The stalls
were last-range stragglers: piece 652 took 11.8 s for one 256 KiB range
while its other fifteen claims were long done; joins arrived, in the order
the outpacing rule admits them, each at its joiner's own latency -- and
splitting had begun about a second before the reader reached the piece,
on pieces that needed six. The two split pieces at the head were the right
mechanism at the wrong depth. Not a display or decode problem: mpv's own
cache sat at 0.0 s while the OSD showed 20 s ahead, because the OSD's
"ahead" was the bytes on disk in front of the video head and mpv reads
several streams of the container (the subtitle track was the one behind).

Shipped from it, in three layers:

- **rqbit `3cc8eef7`** -- the split depth is a runtime setting
  (`ManagedTorrent::set_deadline_pieces`) and the tracker reports the
  median completion of its pieces, first claim to last chunk, over the
  last sixty-four completed (sixteen until 2026-09-17 12:22, when one
  seek's burst of fast split pieces was seen swinging it) -- whole and
  split alike, because a piece is one
  peer's whole reservation until the split reaches it, and a median of
  split pieces alone (`8de7eacc`, superseded the same day) measured the
  mode it was sizing. The crate holds no opinion about the depth;
  `CLAIMS.md` says why ("How deep the splitting goes is the embedder's").
- **enginefs `retention::deadline`** -- every pass sizes the depth: start at
  `ceil(median / seconds-a-piece-plays-for)`, never under two, plus one per
  stall the player has reported for this video, never past the stream's
  lookahead in pieces; applied to the backend when it changes, traced as
  `stage=deadline_depth`. `EngineFS::on_player_opened` resets the stalls,
  `on_player_stalled` counts one; `ServerHandle::note_player_opened` and
  `note_player_stalled` are the embedder's calls.
- **xtremio** reports both: opened when a torrent source starts playing,
  stalled when the buffering popup shows after the first frame.

Untested in the field as of this entry. What to look for in the next log:
`deadline_depth` lines with a `median_ms` and the depth they set; whether
the reader still walks into pieces in flight (`piece_claims_at` probes on
pieces inside the split depth); and whether the stall count climbs past
one or two on a film the swarm can carry at all. The ceiling is a guard,
not a target: a depth at the lookahead is every piece of the window split,
which is the thing the head-only split was introduced to stop.

The `hevc_mediacodec: Both surface and native_window are NULL` line in
mpv's log is *not* the copy-path marker an earlier note took it for: it
appears with `hwdec=mediacodec` too. The OSD's hwdec row is the check.

## Seventh field log: the stall reports in the field (2026-09-17 06:41, xtremio 3695218)

The first two logs of the adaptive depth. In the first (06:26, xtremio
3a6ac82) the open's own wait was counted as stall one -- the player's rule
was "the position has changed", and mpv reports zero and then the resume
point on load. Fixed in xtremio 5aa7ee2 (a tick, forward and under two
seconds) and 3695218 (a seek resets it too; zond does not want a seek to
deepen the split). In the second (06:41) the open reported `stalls=0`, the
two seeks changed nothing, and the depth sat at 3 on a 1.3 s median from a
faster swarm. One miss: a popup 1.4 s after the first frame, at the open's
position, counted. It was the tail of the open's wait -- the viewer's
window was 8 MB at that moment and 321 MB ten seconds later -- and the
player cannot tell that from a stall.

This side can: `Stream::filling` records whether the last grant was cut
short by the doubling, `Streams::viewer_filling` answers it for the live
stream that has asked for the most bytes, and `Engine::player_stalled`
does not count a report while it is `Some(true)`. Traced as
`stage=player_stalled counted=<bool>`. A seek is a new stream from the
floor, so the same rule covers seeks server-side as well; the player's
reset is belt and braces. That needed one more rule (review 2026-09-19
#14): the stream a seek left stays live for thirty seconds with far more
bytes to its name, so it answered for the viewer and a stall in the new
stream's ramp was counted. A stream another live stream began after its
last read is now passed over (`FileStreams::current_viewer`).

## Eighth field log: the depth flapped on the arithmetic alone (2026-09-17 12:22, xtremio b0e7659)

Stall counting was right end to end: `stalls=0` throughout, the open and
two seeks reported nothing, no popup during steady playback. The depth
ran 2-8 on the median alone -- and flapped 7-3-4-5-6-8-5-4-3 within forty
seconds after a seek, because a sixteen-sample ring is one seek's burst of
fast split pieces, then the slow whole pieces from deep in the window.
Two changes: rqbit's ring is sixty-four completions, and
`DeadlineDepth::settle` lets the depth rise at once but fall one piece a
pass (`deadline_depth` now logs `asked` beside `depth`). Splitting is
one-way per piece, so the flap never hurt playback; it was noise and a
moving target for the swarm.

## Ninth field log: a refused peer slept through its moment (2026-09-17 14:55, xtremio 42dcb0f)

Smoothing held: one `deadline_depth` line after the open (`depth=3
asked=3`), none after; `stalls=0`, the open and the seek reported nothing.
The open cost 22.9 s: 12.5 s on piece 0 with no peers connected for ten of
them (cold start; discovery, not scheduling), then 6.5 s on tail piece
5565 whose last claim sat with a holder of 6.9 s round trip while a 489 ms
peer, nine claims of that piece already delivered, was refused at ~400 ms
of the claim's age, had nothing in flight, and slept the request loop's
flat five seconds. rqbit `4110d894` replaces the five seconds: every
time-based refusal names the instant it would be lifted (`retry_at`), the
loop also wakes on `new_pieces_notify`, on the peer's own Have, and on
shares put in a pool (a split reservation or a cut at the head, which
pulsed nothing before), and a thirty-second backstop logs at info when it
fires -- a line that in a field log means a wake-up is missing. CLAIMS.md,
"When a refused peer asks again". The ten seconds of zero peers at open
remain: peer discovery latency at cold start, a different problem.

## Tenth field log: a finished piece shadowed by late chunks (2026-09-19 17:31, xtremio 717b0de)

The TV on 5 GHz now (866 Mbps link; the 2.4 GHz link's ceiling had
measured 4.6 MB/s against the film's 3.57), playing the 23 GB film after a
jump to 5098 s. Piece 4342 completed, was verified, committed and read
whole -- and 180 ms later `staged_over_held piece=4342`: a peer's chunks
for it were written after it was done. `open_for_read` prefers a staged
copy, so every later read was served that one: 33 times `reading 262144
bytes at N of piece 4342` with N creeping up as more late chunks landed
(EOF past them), zeros between them (mpv's `Invalid NAL unit size`), two
`failBuffer` reopens that found the same shadow, and an OSD reading 20 s
ahead off the complete copy nobody was served. Only a restart's
`seed_from_disk` clears such a shadow. The third sighting of this shape,
after piece 834 (rqbit `fcde3058`) and piece 3181 (line 228 above).

Not a stale reclaim, as first guessed: `unlinked=0` throughout, and the
warning itself needs the held bit still set. The log does not show which
route left the peer holding a share of a finished piece -- every
reservation path is guarded -- but one route is proven in code: a
reselect (or asking a file back) during a piece's hash check wiped it and
queued it for a second peer.

Fixed from three sides, so none alone has to be right:

- **rqbit `5acbd7d3`**: `write_to_disk` refuses a chunk for a piece that
  is have or fully written, before the storage write, and logs it at info
  with the piece and the peer; `reselect_pieces` and `update_only_files`
  leave a piece being hash-checked alone; `reserve_piece` warns when handed
  a finished piece, naming the route next time.
- **stream-server `b287382b`**: a staged copy shorter than its piece, while
  the store holds the piece, is a shadow; reads get the complete copy
  (`stage="staged_shadow_bypassed"`, once per piece). A legitimate
  re-download is full length when its own hash check reads it.
- xtremio `00d6dc2` pins both.

Also from this session, not code: the RD source that "failed to recognize
file format" was a stored RAR of a cinema DCP (JPEG 2000 reels, separate
PCM sound, teasers of other films). Comet sends a plain URL for it; the
official client does no content sniffing either, and Torrentio answers
such links with a "failed RAR" video. See the xtremio notes for what
archive playback would take.

## Readable before durable (2026-09-19)

A field log had head pieces waiting 20 ms to over a second between their
last chunk and the reader's wake-up. librqbit wakes a reader when it sets
the have-bit, and it sets it only after `on_piece_completed` returns --
which is where the piece store flushed the 4 MiB staged copy
(`fdatasync`) and renamed it into place. On the Chromecast's eMMC under
concurrent writes the flush is the slow part.

**What changed.** `PieceStore::complete_piece` now accepts the piece --
sets its held bit, leaves it readable from the staged copy, which is the
copy the hash check just read whole -- and queues the flush and the
rename for the store's committer thread (`piece_store::commit`,
`Inner::run_commit`). The queue holds eight pieces; a completion that
finds it full waits for room, which is the old synchronous cost and no
worse. The durable record is unchanged in meaning: the final name only
ever stands over flushed bytes, `PieceStore::has_piece` answers from it,
librqbit's `has_piece` and `init`'s walk wait for a queued rename rather
than call the piece absent (the walk waits for any store's commits under
its directory, which covers a restart out of error building a fresh store
while the errored one is still committing), and a crash before the rename
leaves a staged-only piece the next `init` does not claim. A delete of a
piece whose rename is queued cancels it; one that meets the rename in
progress waits for it and deletes the result.

**What is logged.** `stage="piece_commit_slow"` (info) when a flush plus
rename takes 100 ms or more, with `sync_ms`, `rename_ms` and `queued_ms`;
`stage="piece_commit_backpressure"` (info) when a completion waited 100 ms
or more for room in the queue -- that one is a reader waiting again, and
a device that cannot keep up with the download. `stage=
"piece_commit_failed"` (error) is the case below. Not measured on the
device yet; the next field log should show whether head pieces still
wait, and how long the flushes really are. On a desktop NVMe, 4 MiB
pieces completed every 400 ms beside a writer dirtying pages flat out, a
completion held its caller p50 11-17 ms, p90 143-323 ms, max 426-768 ms
before, and under 0.12 ms after. Completed back to back, faster than the
flushes, the queue fills and the two are the same: the backpressure is
the old cost, as intended.

**The case that is not clean: a commit that fails after the completion
returned.** librqbit has set its have-bit by then, and may have announced
the piece -- directly, or through retention's committed set, which counts
held pieces -- and there is no un-have. What the store does
(`Inner::fail_commit`):

- clears the held bit, so retention withdraws the piece from what new
  peers are told and never reclaims or commits it again;
- after a failed **flush**, removes the staged copy: Linux marks the pages
  clean after a failed `fsync`, so a read after eviction would be served
  whatever the device holds, and a `MissingPiece` is better than zeros
  that pass for media. After a failed **rename** the flushed bytes stay
  and keep being read;
- keeps the error and fails every later `pwrite_all` and
  `on_piece_completed` with it, kind intact. librqbit treats either as
  fatal ("FATAL: error writing chunk to disk"), so the torrent goes to
  Error exactly as it did when the commit was synchronous -- one write
  later -- and `ENOSPC` recovery still recognises a full disk. The
  restart's fresh store does not hold the piece and it is downloaded
  again.

Left open, both needing a device that refuses a flush: a torrent that
writes nothing more after the failure -- every piece it wants is here --
does not fail until something restarts it, and until then a read of a
piece whose flush failed fails, and a peer that was told about it and
asks is hung up on. Peers told about a piece the process then crashed
before committing are not a case: their connections end with the
process.
