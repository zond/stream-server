# AGENTS.md

How to change this repository without breaking what it promises. The
[README](README.md) says what the server does and how an embedder uses it;
this file says where things are and which rules a change has to keep. When
the two disagree, the code wins and both are wrong -- fix the doc that lied.

## What this repo is

A Rust library, not a program: [xtremio](https://github.com/zond/xtremio)
links the `server` crate and starts it in-process over flutter_rust_bridge,
and nothing else runs it. It speaks HTTP to exactly two callers -- players,
which fetch media by URL, and stremio-core's `StreamingServer` model, which
speaks Stremio's streaming-server protocol through the app's `Env::fetch` --
and everything the app itself asks is a `ServerHandle` method over FFI
(README, [API](README.md#api) and [Library API](README.md#library-api)). It
transcodes nothing, needs no system library, and spawns no process.

## Licensing

The **source** is MIT and `LICENSE` must stay MIT: it contains no GPL code.
RAR is on by default through `unrar-rs`, which is GPL-3.0-or-later, so a
**program that links this library with `rar` on is GPL-3.0-or-later** -- the
obligation is the embedder's (today xtremio ships `LICENSE-GPL-3.0` and
`unrar-rs`'s own licence file), and this repo's job is to keep the fact
visible: `server/Cargo.toml`'s `license` reads `MIT AND GPL-3.0-or-later`,
the dependency and the feature both carry the explanation, and
`LICENSE-GPL-3.0` is kept verbatim so an embedder has the text. Keep those in
step; do not change `LICENSE` to GPL. `--no-default-features` links no GPL
code (RAR requests then answer `501`).

## Workspace map

Two crates, neither builds a binary; `members` and `default-members` in the
root `Cargo.toml` are identical and the comment there says why.

| Where | What |
|---|---|
| `server/src/lib.rs` | `ServerConfig` (its `Default` is the contract: loopback, generated token, ephemeral torrent port), `start`/`run`, `ServerHandle` (the embed API -- the README's Library API table is its index), `build_router()` = `media_router()` (open) + `control_router()` (bearer), `lan_media_routes()` (the LAN listener's allow-list). The `control_router()` doc comment is the statement of the route rule below. |
| `server/src/auth.rs` | `ServerAuth` and the `require_bearer` middleware. The token reaches nothing but `ServerHandle::auth_token`: never stdout, never a `tracing` macro (each start keeps the last ten launches' logs). |
| `server/src/routes/` | One file per API area. `stream.rs` (torrent media, both listeners), `archive.rs` (`/{fmt}/create` and `/{fmt}/stream`), `proxy.rs` (`/proxy`, `cache_assisted_range`, the redirect and credential rules), `drive.rs` (the Drive open and `/drive/stream`), `downloads.rs` (the pin functions behind the handle, and `/downloads/{key}/stream`), `system.rs` (settings, the stats functions, `background_traffic`), `engine.rs` (`/create`), `casting.rs`, `local_addon.rs` (the stub), `compat.rs` (tracker/file-index normalisation shared by every creation path). |
| `server/src/proxy_cache.rs`, `proxy_retention.rs`, `proxy_downloads.rs`, `proxy_streams.rs` | The proxy cache as an adapter over the chunk store; its retention owner, read-ahead (`Prefetcher`) and pins; the download filler; the registry of player-token streams a close ends. |
| `server/src/cache_budget.rs`, `cache_cleaner.rs` | The one publisher of the cache cap, and the wire types plus the two functions behind `cache_usage`/`clean_cache_now`. |
| `server/src/stream_numbers.rs` | What a playback panel is told about a stream, dispatched on the URL's shape. |
| `server/src/sources/`, `translators/`, `images/` | A file somebody else fetched, read by range (`ByteSource`: torrent file, proxied entity, Drive file, member views); what a container says about the bytes inside it (ZIP, TAR, RAR, 7Z); ISO 9660/UDF. A member is `Direct` extents or a `Refusal` -- there is no extraction and no third state. `docs/translated-sources.md` is the design. |
| `server/src/lan_media.rs`, `diagnostics/`, `state.rs` | The LAN listener's start/stop; logging, the crash handler, `dht_health`; `AppState`. |
| `enginefs/src/backend/librqbit.rs` | The sole torrent backend and the one place `bt*` settings reach librqbit (`SessionTuning`, `bt_settings_support()` -- a truth table a test keeps complete). `dht_bootstrap.rs` resolves bootstrap names itself. |
| `enginefs/src/lib.rs`, `engine.rs`, `reconcile.rs`, `retention.rs`, `retention/` | `EngineFS` (the engine and magnet-add registries) and one `Engine`; the reconciler that makes every start and stop; the retention owner and the read-pattern detector (`retention/streams.rs`), shared with the proxy. `docs/read-pattern-retention.md` is the design. |
| `enginefs/src/piece_store/`, `chunk_store.rs` | The session's storage, one file per piece, and the store both adapters sit on. |
| `docs/` | Designs written against a rev and measured: retention, generic downloads, thin-swarm redial, translated sources, BitTorrent settings, known issues, two whole-project reviews. |

## Toolchain, build, CI

`rust-toolchain.toml` pins the toolchain; CI installs the same version
explicitly in every job of `ci.yml`, so a bump is two edits. Before
committing, all of these, gated on their exit codes (never through a
`grep` or `tail`):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings   # the workspace lints already deny warnings and clippy::all
cargo test
cargo test -p server --no-default-features               # the MIT build; the only place cfg(not(feature = "rar")) compiles
cargo ndk -t armeabi-v7a -t arm64-v8a check -p server --all-targets   # needs an NDK; CI runs it
```

CI (`.github/workflows/ci.yml`, the only workflow) runs exactly those plus a
Windows build and test, with no `apt install` step -- it proves the
no-system-library default build. There is no release workflow: whoever links
the library ships it. Runtime dependencies: none.

## Conventions

- **Errors**: `anyhow` in `server`, `thiserror` in `enginefs`. **Logging**: `tracing` only. **Async**: tokio; `spawn_blocking` for anything that touches the disk; `async-trait` on the backend traits. **State**: `AppState` in `server/src/state.rs`. Edition 2024, one version across both crates, conventional commit prefixes.
- **Routes, and why there are so few.** Two HTTP callers: players and stremio-core's fork (which is changed as little as possible, so its routes stay exactly as it calls them). The app is not a third: every app-facing capability is a `ServerHandle` method with no route. **A new capability is a method, never a control route**; a new byte-serving URL for a player goes in `media_router()`; `control_router()` changes only when the core fork starts calling a new path, which is a change to the core fork first. Never a token in a query string. Keep the paths and camelCase shapes the core parses; do not resurrect removed `server.js` routes without a consumer (README, [Removed routes](README.md#removed-routes)).
- **The LAN listener is an allow-list, not `media_router()` minus the hazards.** A route in `media_router()` is not on the LAN until its group is named in `lan_media_routes()`, and the test of belonging is that a stranger on the network cannot make this device *do* anything through it: `/proxy` and `/ftp` fetch a caller-named URL, the archive `/create`s fetch a caller-named index, the loopback stream route creates a torrent with the caller's trackers on the first request (the LAN mounts `EngineAccess::ExistingOnly`). Loopback answers no CORS; only the LAN router does, for the Cast receiver (README, [LAN media listener](README.md#lan-media-listener)).
- **Library parity runs one way.** Where a core route and a method answer the same question (`/settings`; the per-file stats), they share one function and `embed.rs` compares them; normalisation lives in the shared function, never in the handler. A method needs no route. Everything crossing FFI stays `serde`-serializable.
- **Native code in the default build is two crates, each with its reason written here**: `aws-lc-sys` (rustls's provider and librqbit's SHA-1) and `network-interface`'s one-file shim on macOS/BSD. `cargo tree -p server -e build,normal -i cc` lists what compiles C. No new native dependency in the default set without a reason written here; otherwise behind a feature. `rar` is the only feature.
- **Tests**: unit tests beside the code, integration tests in `server/tests/` (`embed.rs`, `proxy.rs`, `drive.rs`, `proxy_downloads.rs`, ...), every server on ephemeral ports so any number run in parallel. Five ways a test passes here and fails on a runner, each learned the hard way -- check a new test against all of them:
  1. *Torrent file order.* `create_torrent` walks a directory in readdir order; never hardcode a file index -- look it up (`torrent_file_index`, `file_index`) -- and size fixture files as whole pieces.
  2. *Path spellings.* Expectations built from a `TempDir` go through `stream_server::resolved_path`, as the server's `cacheRoot` does (8.3 names on Windows, `/private/var` on macOS).
  3. *Sleeping instead of synchronising.* A bounded poll on observable state, or `start_paused`; a fixed `sleep` is a timing assertion nobody meant.
  4. *`{:?}` on a path in a needle.* `Debug` escapes the separator; match the formatting the value used.
  5. *Waiting for a real peer to connect.* A bet on the runner's network stack; peer-dependent claims belong in the enginefs tests that dial out, everything else is provable with data already on disk.
  Fixtures get a scratch root of their own, never a fixed path under the temp dir.
- **Agent model economy**: the cheapest model adequate for the task -- small models for mechanical edits, renames and docs; strong ones for architecture, concurrency and adversarial review.

## Rules the code depends on

Each is a decision a test pins; the README section named beside it explains the behaviour to an embedder, and this list only says what a change must not undo.

- **Nothing reaches stdout, no unwinding in release** (`panic = "abort"`), **no single-instance lock** (several servers in one process is the tests' ordinary case), **no HTTPS listener** and no daemon: a library decides none of that for its host. What must stay stable is the Rust API: `ServerConfig` and its `Default`, `start`/`run`, `ServerHandle`.
- **Magnet adds block inside librqbit** (metadata first, no timeout). enginefs runs every add detached, bounded by `METADATA_RESOLVE_TIMEOUT`, in a registry keyed by info hash: routes that must answer now use `EngineFS::get_or_begin_add_magnet` and report `resolvingMetadata`/`magnet_add_failed`; routes that need the file list use `get_or_add_magnet` / `compat::get_or_create_engine` (the only path that retries a failure) and map `MagnetAddError` with `compat::engine_creation_failure`. Never call `EngineFS::add_torrent` for a bare hash or a magnet from a route, and every creation path passes the request's `tr=` trackers -- a magnet's trackers can only be set by the request that creates the engine (`magnet_with_trackers` folds them into the link; `add_trackers` is a documented no-op).
- **Every removal is one step, under the `RemovalGate`**: registry and session go together, so an add in the gap cannot be handed a torrent that is leaving.
- **The pin set is not optional state.** Every want-set plan unions `pinned_files` in; everything that drops or pauses skips `is_pinned()` except the free-space arm. The server keeps no pin record: the embedder hands the set in at boot (`ServerConfig::pins`) and `None` means **unknown** -- sweep nothing, treat every torrent as pinned, never lift -- not "no pins" (README, [Offline downloads](README.md#offline-downloads), *No pin set*). A per-file delete asks `drop_file_pieces` before it unlinks, holds the claim across the unlink, and `deleted_files` means bytes really left the disk, never "the path is absent". `piece_reclaim` is on for every add because the piece store can release a piece; the price is that librqbit restores every torrent paused and only the reconciler starts one, after `apply_pins` -- do not reorder that.
- **A pin is a retention property; `output_folder` decides nothing.** One torrent-data root (`settings.cacheRoot`), placement is root + info hash, nothing above `backend/librqbit.rs` reads librqbit's folder for placement, protection or accounting, and there is no relocation. A dormant pin's directory is deleted by hand only for a hash the session has never heard of; anything the session holds is deleted through it.
- **The piece store is the session's default storage, and the only place it can be** (`SessionOptions::default_storage_factory`; the persisted record names no storage). Data is one file per piece under `.pieces/<hash>/`, `DownloadInfo::path` is a name and not a file, every free-space question is asked of that one root (`reconcile::Volumes`), and there is no migration: a whole-file download an earlier version wrote is swept by the first launch handed a pin set.
- **One chunk store, two adapters** (`chunk_store::ChunkDir` under `piece_store` and `proxy_cache`). Exactly two things are parameters: staging identity (addressable for librqbit's incremental writes, anonymous for uncoalesced proxy fillers) and the commit criterion (`expected_len`, which must stay optional: librqbit commits a padded piece short). The store is told an index, never computes one. `ChunkDir::remove` is `pub(crate)` to `enginefs` on purpose: `server` cannot name the unlink.
- **Presence means complete**, in both adapters. A piece is accepted on `on_piece_completed`, flushed by the committer thread and only then renamed into place; a read prefers the staged copy; `init` deletes only a staged copy that shadows a complete one. A proxy chunk is held in memory until whole, committed with its byte count, and a chunk whose length disagrees with its entity is refused and deleted at the read. Do not "optimise" either into a direct write.
- **Occupancy is counted, never `metadata().len()`** (`chunk_store::occupied_bytes`: `st_blocks` on Unix). The free-space floor is `enginefs::CACHE_FREE_SPACE_FLOOR`, one number with three readers (the stream route's gate, the published cap, the reconciler's free-space arm); do not move it back into `server`.
- **The cache budget has one publisher** (`cache_budget::publish`, through `publish_in_turn`): the minute timer (published before the router serves), `update_settings` and `clean_cache_now`. `occupied` is what the owners count, never a walk. Do not add a second writer, and do not read the inputs outside the turn. `CacheBudget::Unknown` is an absence, not a zero: no policy is installed until something has published.
- **There is no cache cleaner and nothing walks the cache.** Every byte under the root has one owner that knows whether anybody wants it: pinned; inside a live entity's want set, promise or committed set; slack (taken at the tick, the switch, the running-low bell, `clean_cache_now`, boot); or a stray the next launch sweep takes. A translated member owns nothing. Adding a category without a deleter is how the disk becomes unbounded again; adding a walk or a second door into the piece unlink (`StoreRoot::delete_pieces` is `#[cfg(test)]`) is how the have-set desyncs. Every piece unlink is `StoreRegistry::delete` under a `drop_pieces` claim, or the proxy owner's own.
- **The retention policy is wired, and it is arithmetic.** One `RetentionPolicy` per file, resident in its cell; the detector (`retention/streams.rs`) says who is consuming a file from the runs of disk their reads caused, not from a playhead; the file's extent is held back from what we announce *before* the reader opens, a drawn piece is committed and announced the moment it completes, and what is over the allowance goes coldest first, backend forgetting it before the store unlinks it. No policy for a pinned file, for a budget that covers the file (`Shape::Whole`), or before a budget is published. The committed set is a uniform draw, never fetched for the swarm; **do not build an availability map** (rarity was measured at <=0.15% over random). `AfterRelease::LeaveDropped` stays, or reclaimed pieces are re-downloaded at once.
- **The same policy bounds `/proxy`**, and its playhead is a byte that really went out (`Cached::body`, `Filler::take`) -- never a `Range` header, never persisted. A window is per detected consumer; `Reader::promises` is the proxy's interlock (a framed response's undelivered chunks are nobody's to take); an entity with no policy still keeps its whole extent while live. The proxy cache's key is URL + `h=` + the forwarded negotiation headers; `r=` and `p=` are out of it, a credential in `h=` is refused rather than keyed, every hit -- whole or partial -- goes through the same playlist classification before it answers or is narrowed against, and the "what it does not do" list (no revalidation, no `Vary`, no coalescing) must stay honest (README, [Proxied remote streams](README.md#proxied-remote-streams)).
- **Read-ahead is a player's and only a player's.** `ProxyBacking::want` fills the pass's want-windows through a quiet source registered by the `/proxy` route for a request carrying `p=` and by the Drive open (`ProxyRetention::note_source`); a whole-budget entity is driven from the player's delivered bytes instead, since the owner runs no pass for it. A source must carry the player's negotiation headers or it fills a sibling key. A rate is not a gate (one 256 KiB read measures one). Registering is what turns it on -- nothing that is not a player's stream calls `note_source`.
- **The reconciler makes every start and stop, and why a torrent is stopped is recomputed, never stored** (`reconcile::desired()`, a pure function of conditions readable now). Decide from `TorrentHandle::run_state`, never `is_paused()` (the flag and the state are two writes; four defects got past tests that asserted the flag). "Playing" is the liveness cell (`retention::live`), no clock attached; the hysteresis stores no bit; whatever needs a torrent running now awaits `reconcile_hash(.., PlaybackStart)` after registering its activity; every route that opens a torrent reader registers a stream first (grep `get_file_reader` before adding a third). `seedingEnabled` is the session's upload switch, `seeding_enabled || playback_is_live()`, recomputed on every tick and never stored.
- **The activity light reads the connection, never the disk** (`TorrentHandle::transfer_totals`, librqbit's peer counters -- not storage reads, not `downloaded`, which grows through a hash check), judged in Rust over a window with playback absent over the same window. It is a peek: it touches no idle clock and creates nothing. `playback_is_live` is the live fields only and must never grow to include the last-chosen file.
- **What a panel is told** (`stream_numbers.rs`) dispatches on the URL's shape through the same parsers the routes use, keeps no state, and every absence is `null`, never a zero; transfer totals are this session's and are not persisted.
- **HTTPS trusts Mozilla's compiled-in roots** through `enginefs::http_client_builder`; never build a bare `reqwest::Client` (Android's platform verifier downloads CRLs in Java per handshake -- measured as the GC storm behind an ANR).
- **The DHT is a peer source, not a requirement, and a dead one stays quiet**: librqbit's per-attempt DHT and UPnP warnings are pinned to `error`, `diagnostics::dht_health` says the conclusion once, the bootstrap list is the two hosts measured to answer (a host is added only with a `ping` behind it), and `backend/dht_bootstrap.rs` resolves the names itself -- hermetic callers use `BootstrapResolvers::offline()`. UPnP is requested only for a `Fixed` listen port.
- **`btMaxConnections` is 160 (40 peers per torrent) and applies live**; **lean mode shrinks, it does not stop**: `set_background(true)` caps peers at `LEAN_PEER_LIMIT` and prunes the table, keeps every torrent live and seeding, and a repeated `Lean` is a no-op -- prune before lowering, never after, or `Full` has nobody to re-dial.
- **The `server` crate is a plain `rlib`**: no `cdylib`, no JNI; xtremio owns the FFI and the Android initialisation.
