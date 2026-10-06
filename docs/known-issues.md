# Known issues

What is open in this repository, and the hazards of working in it. What was
closed is in git history (`git log -p -- docs/known-issues.md`, and the two
whole-project review logs, `docs/review-2026-09-14.md` and
`docs/review-2026-09-19.md`, deleted on 2026-09-27 with every row worked);
the one-line list at the end is only there so nobody reopens something
without knowing why it was shut.

Last checked 2026-09-16, against the tree at `bd48aac`: the open list, the
standing hazards (each re-run rather than re-read) and the dependency pins.
Entries dated after that were written when they were found or closed. On
2026-09-27 the file was **trimmed, not re-checked**: the "Redundant",
"Stale docs" and "Closed" sections and the field-log history were cut to one
line each. Of the review logs' last open rows, the two ASKs of 2026-09-14
were found resolved in the code (#28: rqbit logs the resume-data clearing at
`debug` under piece reclaim; #78: xtremio's TEMPORARY mpv log instrumentation
is gone), and the one question left open in a 2026-09-19 status (#100) is
carried in below. On 2026-10-07 every name, test, log field and code path
here was checked against `1e2554e` (after the explicit holds of
2026-10-04/05) and corrected where it had moved; the measurements were not
re-run.

## Open

- **The piece-commit failure paths are unmeasured on a device** -- see
  *Readable before durable* below.
- **A set's draw advertised by a sibling can still land between the
  door's reading and an unlink** (2026-10-01, what is left of review H1).
  The door now refuses every piece of a recorded draw of a played entity
  (`Door::drawn`, read after the advertised set), and a draw is recorded
  before it is advertised, so a pass that reads after the record keeps the
  piece; a sibling that records and advertises in the instant between that
  reading and the backend's release is the window a neighbour's boundary
  piece has always had at `announced_now`.
- **A session that keeps a file but stops sharing it leaves nothing**
  (`PlaySessions::play`, "only `shares` changed"): a member played through
  an id and then the same container by its URL (`shares: false`) keeps the
  member's draw advertised until no session is on the torrent. Only a
  client on the HTTP routes does that; the app plays members by id.

### Readable before durable (2026-09-19)

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
"piece_commit_failed"` (error) is the case below, and
`stage="piece_commit_abandoned"` (error) a queued commit whose committer
died before running it: nothing on the disk is touched, and the store fails
its next write the same way. Not measured on the
device yet; the next field log should show whether head pieces still
wait, and how long the flushes really are. On a desktop NVMe, 4 MiB
pieces completed every 400 ms beside a writer dirtying pages flat out, a
completion held its caller p50 11-17 ms, p90 143-323 ms, max 426-768 ms
before, and under 0.12 ms after. Completed back to back, faster than the
flushes, the queue fills and the two are the same: the backpressure is
the old cost, as intended.

**The case that is not clean: a commit that fails after the completion
returned.** librqbit has set its have-bit by then, and has announced the
piece if it is in a play session's draw or a download -- and there is no
un-have. What the store does (`Inner::fail_commit`):

- clears the held bit, so retention counts the piece lost from its
  committed set and never reclaims it; nothing withdraws the
  announcement, which ends when the torrent errors (below) and so leaves
  the swarm;
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

## Standing hazards

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

* **An engine nothing holds fetches for one reconcile interval and is then
  stopped -- unless nobody told the server what is pinned, in which case it
  fetches the lot.** A new engine wants the file its create named
  (`fileMustInclude`/`guessFileIdx`, since `52f42d7`), or every file when
  it named none, so anything that creates one before a player holds it --
  `/{hash}/create`, `/create`, a stats request that lands before the stream
  request -- fetches at whatever rate its peers give it until something
  stops it. What stops it is the reconciler's timer, and which of the two
  readings below you get is decided by one config field.

  **Under the shipping configuration it stops.**
  `ServerConfig::pins: Some(Default::default())` is what xtremio publishes
  for a user who has pinned nothing (`rust/src/server.rs` ->
  `downloads::pins()`), and under it the ladder's last arm
  (`reconcile::desired`: `if conditions.held || conditions.pinned`)
  answers `Stop` for a torrent nothing holds, nothing is being delivered
  off, and nothing pins. Measured
  2026-09-21 against a real local seeder: the stop lands **2.0 s after the
  add** -- one `reconcile::RECONCILE_INTERVAL`, with no dwell, since the
  dwell holds back only the timer's starts (`start_if_stopped`,
  `restart_if_the_dwell_allows`), never a stop -- and nothing arrives after it. In
  those 2.0 s it took 5.7 to 6.5 MiB (fourteen runs) from a seeder held to
  2 MiB/s, and 186 MiB of a 256 MiB torrent with the limiter off. **The bound is the window, not the
  byte count**: on a fast link one interval is a hundred megabytes and more.

  **Under `pins: None` it never stops.** `None` is "nobody said", which sets
  `PinsUnknown`, under which `Engine::is_pinned` answers true for every
  torrent there is (`enginefs/src/engine.rs`) -- so that same arm reads
  `pinned` and answers `Run` for ever -- and `Engine::reclaim_rest` breaks
  before it takes a piece, so what arrives is also kept. Measured the same
  day: the whole 256 MiB in 2.6 s, with nothing reading it.
  **`ServerConfig::default()` sets `pins: None`**, so a harness that spreads
  the default measures this and not what the app does.

  **What the 2026-09-19 reading was.** Tears of Steel, all 546 MB at
  ~50 MB/s with no reader: real, and it is the second case. 546 MB at
  50 MB/s is eleven seconds of running, five intervals over; a configuration
  whose reconciler stops reader-less torrents cannot produce it. The entry
  it was written into read the number as unbounded fetching in general, and
  that is true of `PinsUnknown` alone.

  **What is left.** One reconcile interval at line rate, per engine created
  and never read -- ~100 MB on the field's 50 MB/s link, and the pieces are
  then reclaimed, so it is spent bandwidth rather than spent disk. It is not
  hit hard today because the player's stream request follows its open by
  ~160 ms, so the player's hold is taken long before the tick and the want set
  narrows; what pays it in full is a `/create` nobody follows with a play.
  Making it smaller means adding a discover-only state, which is why "start
  the engine when a source is picked" was not done for the cold start:
  rqbit drops peers when neither side is interested, so only addresses and
  metadata would survive it.

  Both readings are pinned by `server/tests/reader_less_fetch.rs`, which is
  the one test here with a real seeder in it:
  `a_torrent_nobody_reads_stops_fetching_within_a_few_reconcile_intervals`
  bounds the first, and
  `an_unknown_pin_set_never_stops_a_torrent_nobody_reads` records the second
  as the documented exception.

  The cold start itself (field 2026-09-17 16:36, on the phone) was a slow
  peer ramp -- peers found in 2 s, 2 connected for 10 s, 32 after 50 s --
  not discovery; the `stream_progress` line carries `queued`, `connecting`,
  `unique` and `known` to tell "nothing found" from "nothing answering".

* **A torrent no open names keeps nothing, one reconcile interval after its
  initial check -- so a test that seeds its own torrent data must run with
  the pin set unknown.** Found 2026-09-20 writing the RAR set tests (review
  #103): under
  `ServerConfig::pins: Some(Default::default())` -- "an embedder that keeps a
  pin record and has named nothing in it" -- a freshly added forty-piece
  fixture held forty pieces and then none, one
  `reconcile::RECONCILE_INTERVAL` after its initial check.

  What takes them is `Engine::reclaim_rest` (`enginefs/src/engine.rs`),
  reached from `EngineFS::reconcile_tick` -> `retain_engine` ->
  `Engine::retain` whenever `!live.is_torrent(hash)`: it takes **every held
  piece outside every holding extent** that the torrent does not advertise,
  and a torrent with no reader has no extent (it takes nothing under
  `PinsUnknown`, or while the liveness cell names the torrent). Proved causally by returning 0 from `reclaim_rest` alone, which
  leaves the fixture whole. It asks no volume and no cap, which is why
  `pretend_volume_space(root, u64::MAX)` does not save anything -- what it
  acts on is "nobody wants this", not "the disk is short".

  **This is the design, not a leak**: on a real torrent the bytes come back
  from the swarm. It is worth knowing anyway, because the empty record is the
  ordinary state of the ordinary install -- xtremio's `downloads::pins_in`
  over a registry with no downloads in it answers exactly
  `Some(<empty>)` -- so a viewer who has pinned nothing loses their buffer
  the moment they open something else; while they only stop, their idle
  share keeps its window. `embed.rs`'s
  `an_embedder_that_has_pinned_nothing_keeps_no_torrent_nobody_plays` states
  it.

  **Nothing seeds a test fixture**, so for the tests it is fatal: a read
  after the pass parks for ever. `pins: None` ("nobody said", which sets
  `PinsUnknown` and reads as every file pinned) is the only cure, and
  `server/tests/support/fixture_pins.rs` is the one place that says so;
  every test that reads seeded bytes wraps its config in
  `fixture_pins::keep_what_the_fixture_seeded`. A test *about*
  retention, idle pausing, the reconciler or the pin routes keeps the empty
  record and controls the timer itself -- under `None` a torrent is reported
  as pinned, so it is also exempt from idle removal and the reconciler keeps
  it running.

* **Never `git checkout <file>` to undo an experiment.** It restores from
  HEAD, not from the working tree, so it discards everything uncommitted in
  that file. Copy the file to the scratchpad and copy it back instead.


## Closed, one line each

- 2026-10-03 -- **Deleting one volume while its set played kept its
  pieces advertised until the set was left** (review of `1d0d109`, H3):
  the set's union is recorded on every volume, and the delete ended only
  the deleted volume's record, so the union on its siblings still counted
  as shared and the deleted pieces stayed announced -- and, announced,
  stayed on the disk. The delete now takes the volume's pieces out of
  every record made for the set (`Engine::withdraw_deleted_volume`,
  `Retention::withdraw_from_draws_made_for`), all but a boundary piece
  another volume of the set lies in, before its `EndShares`. Tests
  `deleting_a_volume_while_its_set_plays_ends_what_that_volume_shared`,
  `deleting_a_volume_keeps_the_piece_it_shares_with_a_sibling_shared`.

- 2026-10-01 -- **Review of the member-sharing commits (`39a9105`,
  `1d0d109`)**, fixed in `b194495` (B1, H4), `32990ae` (B2), `42cc28b`
  (H1) and `77a7920` (H2). B1: a move to another member of
  the same container kept the first member's draw (a draw now records what
  it was made for, `DrawnFor`; a new member on the same file is a move);
  test `a_move_to_another_member_of_the_same_container_ends_the_first_members_draw`.
  H4, which fell out with it: a set -> own-volume move kept the union; test
  `a_move_off_a_set_onto_one_of_its_volumes_ends_the_sets_draw`. B2: a
  single-file member's duration gave the container a container-length
  stream rate; test `a_members_duration_gives_the_container_no_stream_rate`.
  H1: a sibling's advertise of the union could race a volume's unlink; test
  `a_volume_unlinks_nothing_of_a_sets_draw_a_sibling_has_yet_to_advertise`.
  H2: a volume adopted any sibling's draw, a stale empty one included; test
  `a_volume_adopts_only_a_draw_made_for_its_set`. H3 is closed above.

- 2026-09-29 -- **Offline tests reached the DHT**: `resolve_dht_bootstrap_names`
  off stopped only this server's own resolution, and librqbit resolved the
  bootstrap names itself and joined, so strangers looked a test's info hash
  up and dialled in (xtremio's downloads test, and this repository's own
  offline configs). `ServerConfig::enable_dht` is the default for
  `btEnableDht`, off in every offline test config. Test:
  `a_test_does_not_announce_itself_on_the_local_network`.
- 2026-09-29 -- **A delete could say it freed nothing when it did**: the
  pieces held were counted after the pin came off, and a tick in between
  (the torrent stopped, the unpinned file's pieces taken as slack) left
  nothing to count as leaving. Counted before the pin comes off now. Test:
  `a_delete_counts_the_pieces_a_tick_took_after_the_pin_came_off`.
- 2026-09-29 -- **An archive read across the volumes of a torrent deleted
  the volumes it had read**: every volume open moved the live entity and
  each move's slack pass took the volumes around it, so offline even the
  first range parked. An archive session now holds its volumes as one set
  (`Live::hold_set`): an open inside the set is no move, and every volume
  of the live set is live to the passes. Test:
  `a_rar_set_read_across_its_volumes_keeps_the_volumes_it_read`.
- 2026-09-28 -- **Sharing did not follow the rule "publish once, never
  withdraw"** (zond): a play session shares a set drawn once, a download is
  shared whole, nothing is withdrawn from a live torrent, and a session's
  announced pieces go only after its torrent has left the swarm. The rule
  is [Sharing](storage.md#sharing).
- 2026-09 -- **`scenario.rs`'s `CONTAINER_METADATA_LOOKAHEAD` and
  `PLAYBACK_LOOKAHEAD`** keep the field's numbers under the names of
  retired constants on purpose: a scenario stays the measurement it was
  taken from (the comments there say so).
- 2026-09-28 -- **Whether a tight budget should leave the sharing draw
  room**: no. zond: if it is impossible to share and play, stop sharing --
  the lookahead floor wins and the committed set yields to nothing.
- 2026-09-28 -- **`an_unknown_pin_set_never_stops_a_torrent_nobody_reads`
  flaked** (server/tests/reader_less_fetch.rs): the test's own reading, not
  a pass. The seeder counts a block when its socket takes it, the server
  keeps librqbit's 128-block request window outstanding, and every piece is
  synced before the store holds it, so under disk pressure the store trailed
  the count by up to eight pieces against a slack of four. The
  pre-`8085194` "stopped" failure was not reproduced; that version read a
  stop from zero bytes in a 4 s tail, and the same pressure slowed the tail
  to 2.3 MB, so it is most likely the same stall. Reproduced 37 of 72 runs under 40 busy loops and 8 fsync
  writers; the store never lost a piece. The test now waits (bounded) for
  the store to catch up with the count and checks the pieces that had
  landed are all still there: 0 of 72, and 0 of 36 under 16 writers.
- 2026-09-20 -- **A proxied play-through counted as dozens of consumers,
  settling the cache over its cap** (review 2026-09-19 #100): a read that
  begins where its own reader's last read ended is that reader carrying on,
  and a drawn piece is committed on its completion event (`b8f5027`). The
  rule is in [design/read-pattern-retention.md](design/read-pattern-retention.md#7-what-a-stream-is).
- 2026-09-20 -- **The extracting archive layer deleted**; nothing writes
  `<cacheRoot>/.archives` and a stored member of every format is served by
  range ([design/translated-sources.md](design/translated-sources.md)). A
  stored RAR set in a torrent is served too (step 3; review 2026-09-19 #102).
- 2026-09-20 -- **Dead code deleted**: `cache.rs`, `piece_cache.rs`,
  `disk_cache.rs`, `Engine::data_cache` and `moka` (review 2026-09-19 #50,
  `22d5b1e`); `BackendMemoryDiagnostics` and `diagnostics_snapshot`, which
  answered zero and had no caller (#94, #98).
- 2026-09-19 -- **A finished piece shadowed by late chunks** (tenth field
  log): rqbit `5acbd7d3` refuses a chunk for a finished piece,
  stream-server `b287382b` reads the complete copy over a short staged one.
- 2026-09-17 -- **Stalls at the head of a stream on a thin margin** (field
  logs six to nine): an adaptive split depth (`retention::deadline`, rqbit
  `3cc8eef7`), stall reports that ignore the open's own wait, a depth that
  falls one piece a pass, and a refused peer that wakes when it may ask
  again (rqbit `4110d894`). The design is rqbit's `CLAIMS.md`.
- 2026-09-15 -- **Head-of-line blocking** on a fast swarm (field logs one to
  five): the claim rules measured against the piece's age (rqbit
  `a27fa9cd`, `cc969c7b`).
- 2026-09-15 -- **Accepted as costs of streaming a torrent**: a stalling link
  shrinks the window that would have ridden it out; a seek to unheld ground
  ramps its window from the two-piece floor (audio, subtitle and video
  streams cannot be told apart, so a new stream has no window to inherit).
- 2026-09-16 -- **The pin set read at one moment and acted on at another**:
  each want run is read against the pins as they stand
  (`a_pin_taken_under_the_want_steps_first_run_keeps_the_piece_out_of_the_second`).
- 2026-09-16 -- **`removed_files` insert-only**: a write takes the file back
  out (`a_file_written_again_after_its_removal_keeps_the_piece_it_shares`).
- 2026-09-15 -- **The retention trace and mpv's verbose log** became the
  `diagnosticsTrace` setting (`7acaa6e`).
- 2026-09-15 -- **xtremio's desktop builds only run in CI, and iOS does not
  build** (`librqbit-dualstack-sockets` `bind_device`): not going to be
  worked on.
