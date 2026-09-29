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
carried in below.

## Open

- **The piece-commit failure paths are unmeasured on a device** -- see
  *Readable before durable* below.
- **An archive read across the volumes of a torrent deletes the volumes it
  has read** (found 2026-09-28, not fixed). Each volume's translated source
  registers a stream on its own file (`sources::torrent`), and a volume
  whose reader has not delivered a byte yet is not an aside: the liveness
  cell moves from volume to volume, and the switch's slack drop takes the
  bytes of the volume the body just left -- and of the index reads done
  across all of them. Online they are fetched again from the swarm; with no
  peer the read parks (a known pin set, seeded, offline: 17 pieces held
  before the first range, 1 after). The RAR tests run under an unknown pin
  set, which keeps everything, so none of them sees it. Archive playback
  shares nothing, so none of this can stop the torrent.
  **Verified 2026-09-29, and worse than written:** the pieces go before the
  body has delivered a byte, so offline even the *first* range parks, and
  every volume goes, the next one included. The test is
  `a_rar_set_read_across_its_volumes_keeps_the_volumes_it_read`
  (`server/tests/embed.rs`, ignored until fixed): three volumes, pieces
  0..=16 seeded under a known pin set; after one read across the 1/2
  boundary only piece 6 (shared with the live volume) is left, and both
  reads time out. Under an unknown pin set it passes. The path:
  `routes/archive.rs` `session_for` opens a `TorrentFileSource` per volume
  for the index and drops them, then `sources_for` opens every volume again;
  each open moves the liveness cell (`EngineFS::switch_to` holds the cell
  only for a file with a reader, and a fresh source has none until its first
  read), and each move's slack pass (`drop_slack_on_switch_and_bell` ->
  `Engine::drop_slack`, `mode_of`) releases every volume that is neither the
  cell's nor read. A fix makes a volume set one live entity while a session
  reads it (the cell names the set, or later volumes register as asides of
  the first), leaves every volume of a live session out of the slack pass
  whether or not a reader is open yet, stops the index-then-body reopen
  churn, and keeps the index reads inside the session's window.
- **A stale-looking name, kept on purpose**:
  `enginefs/src/retention/scenario.rs`'s `CONTAINER_METADATA_LOOKAHEAD` and
  `PLAYBACK_LOOKAHEAD` keep the field's numbers under the names of constants
  that no longer exist -- a scenario stays the measurement it was taken from.

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
"piece_commit_failed"` (error) is the case below. Not measured on the
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

* **An engine with no reader fetches for one reconcile interval and is then
  stopped -- unless nobody told the server what is pinned, in which case it
  fetches the lot.** A new engine wants every file, and the want set only
  narrows when a stream arrives, so anything that creates an engine early --
  `/{hash}/create`, `/create`, a stats request that lands before the stream
  request -- fetches at whatever rate its peers give it until something
  stops it. What stops it is the reconciler's timer, and which of the two
  readings below you get is decided by one config field.

  **Under the shipping configuration it stops.**
  `ServerConfig::pins: Some(Default::default())` is what xtremio publishes
  for a user who has pinned nothing (`rust/src/server.rs` ->
  `downloads::pins()`), and under it the ladder's last arm
  (`reconcile::desired`: `if conditions.playing || conditions.pinned`)
  answers `Stop` for a torrent with no reader and no pin. Measured
  2026-09-21 against a real local seeder: the stop lands **2.0 s after the
  add** -- one `reconcile::RECONCILE_INTERVAL`, with no dwell, since the
  dwell guards only `start_if_stopped` -- and nothing arrives after it. In
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
  ~160 ms, so a reader is registered long before the tick and the want set
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
  not discovery; the `stream_progress` line now carries `queued`, `unique`
  and `connection_tries` to tell "nothing found" from "nothing answering".

* **A torrent nobody is playing keeps nothing, about two seconds after the
  last reader leaves -- so a test that seeds its own torrent data must run
  with the pin set unknown.** Measured 2026-09-20 writing the RAR set tests,
  established 2026-09-20 (review #103): under
  `ServerConfig::pins: Some(Default::default())` -- "an embedder that keeps a
  pin record and has named nothing in it" -- a freshly added forty-piece
  fixture held forty pieces and then none, one
  `reconcile::RECONCILE_INTERVAL` after its initial check.

  What takes them is `Engine::reclaim_rest` (`enginefs/src/engine.rs`),
  reached from `EngineFS::reconcile_tick` -> `retain_engine` ->
  `Engine::retain` whenever `!live.is_torrent(hash)`: it takes **every held
  piece outside every holding extent**, and a torrent with no reader has no
  extent. Proved causally by returning 0 from `reclaim_rest` alone, which
  leaves the fixture whole. It asks no volume and no cap, which is why
  `pretend_volume_space(root, u64::MAX)` does not save anything -- what it
  acts on is "nobody wants this", not "the disk is short".

  **This is the design, not a leak**: on a real torrent the bytes come back
  from the swarm. It is worth knowing anyway, because the empty record is the
  ordinary state of the ordinary install -- xtremio's `downloads::pins_in`
  over a registry with no downloads in it answers exactly
  `Some(<empty>)` -- so every viewer who has pinned nothing re-fetches their
  buffer a couple of seconds after they stop. `embed.rs`'s
  `an_embedder_that_has_pinned_nothing_keeps_no_torrent_nobody_plays` states
  it.

  **Nothing seeds a test fixture**, so for the tests it is fatal: a read
  after the pass parks for ever. `pins: None` ("nobody said", which sets
  `PinsUnknown` and reads as every file pinned) is the only cure, and
  `server/tests/support/fixture_pins.rs` is the one place that says so;
  `embed.rs::seeded_fixture_config` and `iso.rs::offline_config` are built
  from it, and the tests that read seeded bytes use them. A test *about*
  retention, idle pausing, the reconciler or the pin routes keeps the empty
  record and controls the timer itself -- under `None` a torrent is reported
  as pinned, so it is also exempt from idle removal and the reconciler keeps
  it running.

* **Never `git checkout <file>` to undo an experiment.** It restores from
  HEAD, not from the working tree, so it discards everything uncommitted in
  that file. Copy the file to the scratchpad and copy it back instead.


## Closed, one line each

- 2026-09-28 -- **Sharing did not follow the rule "publish once, never
  withdraw"** (zond, 2026-09-28): a play session -- one per player token,
  started and moved only by a request carrying it (`p=`), never by an
  aside or an archive's translated source -- now shares a set drawn once,
  against the stream's real read-ahead (it waits for the player to state
  the film's length), a download is
  shared whole, nothing is announced that nothing chose (rqbit's
  `explicit_piece_advertising`, which keeps the set across a restart from
  error), nothing is ever withdrawn from a live torrent, and a session's
  announced pieces are deleted only after its torrent is stopped
  (`Decision::EndShares`: stop, rebuild, start, then delete) -- and, while
  the torrent is played, only the one player on it moving does that; an
  unpin without a delete, or another player's move, waits. A delete does
  not: it is an explicit request and happens at once, whatever the sharing
  setting and whoever's session is on the torrent, waiting only for a read
  of that very file (a cast) to end.
  See [Sharing](storage.md#sharing).
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
