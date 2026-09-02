# AGENTS.md

Guidance for AI coding agents working on this repository.

## What this repo is

Stream Server is a hard fork of an open-source Rust replacement for Stremio's closed-source `server.js`: a local HTTP torrent-streaming engine with archive streaming (RAR/ZIP/7Z/TAR/NZB) and a deliberately small control API — only what stremio-core's `StreamingServer` model calls (`/settings`, `/network-info`, `/device-info`, `/casting`, `/create`, `/{infoHash}/create`, the `stats.json` routes, `/get-https`) plus `/heartbeat` and `/stats.json`, and a three-route **stub of the `/local-addon`** Stremio's default profile calls (`routes/local_addon.rs`: empty manifest, `{"streams": []}`, `meta` → quiet 404 — kept so stock profiles show no error group and log no ERROR per details page; it serves nothing). Media routes and the local-addon stub are open; every control route requires a per-launch bearer token (`ServerAuth`, `server/src/auth.rs`), and everything a control route does is also a library method on `ServerHandle` so embedders need no HTTP client. The README's API section lists every surviving route with its OPEN/TOKEN status and the removed ones. Serves HTTP on **11470** and HTTPS on **12470** by default. **Source is MIT; default binaries are GPL-3.0-or-later** — see [Licensing](#licensing). Consumed by stremio-native (desktop) and stremio-android (JNI) today, and targeted at a new in-development Flutter + libmpv/media_kit client going forward.

This fork has deliberately dropped Stremio server.js API compatibility for transcoding: there is **no HLS transcoding and no FFmpeg/FFprobe integration** (no `hlsv2` route, no probe/hwaccel endpoints, no embedded-subtitle-track extraction) — that logic, and the `ffmpeg`/`ffprobe` runtime dependency it needed, has been removed. Clients are expected to direct-play and handle codecs/subtitles themselves.

**The entire build is pure Rust, with no opt-in native path at all**: `librqbit` is the sole, unconditional torrent backend, 7z is pure-Rust `sevenz-rust2`, and RAR is pure-Rust `unrar-rs` behind a default-on feature. `cargo build` / `cargo test` — with or without `--no-default-features` — always succeeds on a machine with **no** libclang, no fontconfig/gtk headers, no FFmpeg, and no libtorrent. The C++ `libtorrent` backend and its vcpkg build apparatus (the `libtorrent-sys` binding crate, `triplets/`, `vcpkg-overlays/`, `vcpkg.json`) have been removed entirely from this repo; there is nothing left here that pulls in a C or C++ toolchain for torrenting. The server is headless by design — no tray, no desktop GUI; graphical shells live in wrapper binaries in other repos.

## Licensing

The repo **source** is MIT (the `LICENSE` file is unchanged and must **stay MIT** — it contains no GPL code). But RAR support is **on by default** and uses the `unrar-rs` crate, which is **GPL-3.0-or-later**. That crate is only fetched at build time, yet linking it means a **default-built, distributed binary is GPL-3.0-or-later**. This is intentional: the owner wants RAR on by default and is releasing openly (iOS App Store is not a concern). MIT is GPL-compatible, so the MIT source shipping alongside GPL default binaries is fine. To build an **MIT binary without RAR**, use `--no-default-features` (RAR requests then return 501; the librqbit backend is not a feature, so it's not named here). Do **not** change the `LICENSE` file to GPL; document the situation instead.

## Workspace map

| Crate | Role |
|---|---|
| `server/` | axum HTTP API. `src/lib.rs` holds `ServerConfig` (incl. `auth: ServerAuth`), `build_router()` — which merges `media_router()` (open) and `control_router()` (wrapped in the bearer middleware) — and the embeddable `ServerHandle` with its library API (`settings`, `update_settings`, `engine_stats`, `file_stats`, `auth_token`, `base_url`; also a cdylib for Android JNI — see `src/jni.rs`). `src/auth.rs` is `ServerAuth` + the `require_bearer` middleware. `src/main.rs` is the headless daemon binary (`--tui` ratatui mode, `--no-auth` / `--token <t>` / `STREAM_SERVER_TOKEN` parsed by `parse_cli`, single-instance fslock). A generated token is printed to stdout only (`run()` in lib.rs, when `print_startup`) — it must never be passed to a `tracing` macro, since the log files (the append-only archive included) would keep it. `src/jni.rs` builds its config with `jni_config`, which sets `ServerAuth::Disabled`: the JNI call returns only a URL, so the Kotlin side could never receive a token, and the listener is loopback-only. `src/routes/` has one file per API area. Also: `archives/` (RAR/ZIP/7Z/TAR/NZB sessions — 7z is pure-Rust `sevenz-rust2` in `archives/sevenz.rs`, always on; RAR is pure-Rust `unrar-rs` in `archives/rar.rs`, behind the default-on `rar` feature), `diagnostics/` (logging setup, crash handler, memory sampler — no HTTP routes), `ssdp.rs`, `state.rs` (`AppState`). |
| `enginefs/` | Torrent engine abstraction. `src/backend/mod.rs` defines async traits `TorrentBackend`/`TorrentHandle`, with a single implementation: `backend/librqbit.rs` (pure Rust, unconditional — there is no other backend to select). Plus `engine.rs`, piece/disk/metadata caches, tracker management/probing. |
| `stremio-runtime-stub/` | Zero-dep `stremio-runtime` shim binary: spawns the server with `--no-auth` (legacy clients speak plain HTTP and cannot send the bearer header; `REAL_SERVER_ARGS`), monitors it, waits for port 11470, prints the exact ready line legacy clients expect. |

There is no `bindings/` directory of any kind: the vendored 7-Zip and UnRAR C++ bindings were replaced long ago by the pure-Rust `sevenz-rust2` and `unrar-rs` crates (direct `server` dependencies), and the C++ `libtorrent-sys` FFI binding crate — the last native crate in the workspace — has since been removed too, along with the vcpkg overlays/triplets/baseline it needed. RAR is behind the default-on `rar` feature; 7z is always on.

`members` and `default-members` in the root `Cargo.toml` are now identical (`enginefs`, `server`, `stremio-runtime-stub`) — there is no native crate left to exclude from bare cargo invocations, so `--workspace` and the default set build the same thing.

## Toolchain

`rust-toolchain.toml` pins **1.98.0** (needed for the committed `Cargo.lock` and edition 2024); rustup resolves it automatically. CI installs the same version explicitly (`dtolnay/rust-toolchain@1.98.0`) for the `fmt`/`test`/`mit-build` jobs — if you ever bump the pin, bump those CI `Setup Rust` steps too (the Windows CI job and `release.yml` use `@stable` instead and are unaffected by the pin).

## Build, test, lint

### Always verify these before committing — the whole build is pure Rust, no system deps for any feature combination

```bash
cargo build                 # debug; add --release for the optimized binary
cargo test
cargo fmt --all             # run before every commit; CI enforces --check
cargo clippy --all-targets -- -D clippy::correctness -D clippy::suspicious -W clippy::all
cargo run -p server         # http://localhost:11470
```

RAR is on by default (`unrar-rs`, `crypto-rust`) and is covered by the plain `cargo build`/`cargo test` above — it needs no system deps. Its escape hatch, `cargo build --no-default-features`, drops RAR (and links no GPL code) and must also keep building; RAR requests then return a 501 JSON error. There is no other feature combination to check: `librqbit` is unconditional, not a feature.

### Runtime deps

None. There is no runtime external-binary dependency at all.

## CI

- `.github/workflows/ci.yml` (PR checks): `fmt` (cargo fmt --all --check), `test` (clippy gated on correctness+suspicious, then `cargo test`, deliberately **no apt installs** — it proves the zero-system-deps default build, which includes pure-Rust RAR), `mit-build` (checks the `--no-default-features` MIT escape hatch still builds without `unrar-rs`), plus a Windows default build+test. There is no libtorrent job anywhere — the backend no longer exists.
- `.github/workflows/release.yml` (release matrix, gated on `v*` tags or manual dispatch): builds the plain default (`cargo build --release`, RAR on, librqbit the only backend) per platform — Windows (+ MSI via cargo-wix), Linux (+ .deb via cargo-deb, + AppImage), and an Arch package. No release has been tagged from this fork yet, so no binaries have actually been published.

## Conventions

- **Errors**: `anyhow` (`Result` + `.context()`) in application crates; `thiserror` for typed errors in `enginefs`.
- **Logging**: `tracing` macros only (no `println!` for diagnostics). Subscriber setup lives in `server`; HTTP tracing via tower-http `TraceLayer`. The one deliberate `println!` besides the startup lines is the generated auth token: secrets never go through `tracing` (the log files would keep them).
- **Async**: tokio multithreaded; `async-trait` for the backend traits; keep hot-path methods cheap (see `TorrentHandle::is_finished` doc comment — no full stats walks on stream-start). Blocking work (e.g. 7z decompression) goes through `spawn_blocking`.
- **Feature gating**: the only cargo feature left in this workspace is `rar` on `server` (`#[cfg(feature = "rar")]`, default-on, returning 501 when disabled). New *native/C* deps must be optional behind a feature, never in the default set; pure-Rust deps like `unrar-rs` (`crypto-rust`) may ship by default, subject to their license (see [Licensing](#licensing)). There is no backend feature to gate — `librqbit` is a plain unconditional dependency of `enginefs`.
- **Routes**: one file per API area in `server/src/routes/`, registered centrally in `build_router()` in `server/src/lib.rs`, which merges two sub-routers. **A new route goes in `control_router()` — token-protected — unless it serves media bytes to a player** (then `media_router()`, open, because players cannot send headers). The `/local-addon` stub is the one deliberate non-media open route (default profiles, legacy clients included, call it as an addon and it exposes nothing) — do not grow it into a real addon. Never add a token-in-query-string path; the bearer header is the only credential. Preserve exact endpoint paths/response shapes for the surface that remains — it mirrors the subset of Stremio's server.js API (camelCase serde) that stremio-core's `StreamingServer` model calls; do not resurrect removed server.js routes without a consumer (see the README's "Removed routes").
- **Library parity**: whatever a control route does must be reachable as a `ServerHandle` method sharing the same function (`routes::system::{engine_stats, file_stats, update_settings}` are the pattern); the embed test compares the two. Input normalisation belongs *inside* the shared function, not in the HTTP handler — the stats functions normalise their tracker list themselves (`compat::normalize_tracker_sources`) and the handlers hand them the raw `tr=` values, so a library caller cannot see different behaviour. Types crossing that boundary stay `serde`-serializable for FFI.
- **State**: `AppState` (Clone, Arc fields) in `server/src/state.rs`; DashMap for concurrent maps.
- **Platform code**: behind `cfg(target_os = ...)` with no-op stubs. Edition 2024 everywhere.
- **Tests**: inline `#[cfg(test)]` modules beside the code; integration test at `server/tests/embed.rs`, where requests to control routes go through `bearer_client(&handle)` (the default config generates a token). Each embedded server takes one of only ten librqbit listen ports, so keep the number of server-spawning tests in that file at nine or fewer (fold assertions into an existing server rather than starting another). Bare `cargo test` runs everything — there's no reduced default set to worry about.
- **Versions**: all crates share one version (bump in lockstep across every Cargo.toml).
- **Commits**: conventional style — `feat(scope):`, `fix(scope):`, `build:`, `ci:`, `docs:`, `chore:`.

### Agent model economy

Use the cheapest model adequate for the task at hand. Small/fast models are fine for mechanical edits, docs updates, renames, and enumeration; reserve strong models for architecture decisions, concurrency-sensitive code, and adversarial review. This is a standing instruction from the repo owner to conserve tokens/quota.

## Gotchas & do-not-touch

- **Do not casually edit**: the `.github/workflows/release.yml` build matrix and artifact/asset names (`scripts/generate_release_notes.py` and the release-notes step depend on the `release/*` filenames it produces); `server/Cargo.toml`'s `[package.metadata.deb]` and `[package.metadata.wix]` blocks (release-critical, and the WiX GUIDs must never be regenerated).
- **Contract strings**: `stremio-runtime-stub` depends on the literal ready line `EngineFS server started at http://127.0.0.1:11470` and port 11470 — changing startup output or default ports breaks legacy clients.
- **Release profile** (`panic = "abort"`, lto, opt-level=z, in the root `Cargo.toml`) means no unwinding in release — don't rely on `catch_unwind`.
- **Single instance**: the server takes a lockfile in the temp dir (`stream-server.lock`); a second desktop instance exits silently.
- **Magnet adds block inside librqbit**: `Session::add_torrent` for a magnet resolves metadata before returning, with no timeout of its own, and no torrent exists until then. enginefs runs every such add detached and bounded by `METADATA_RESOLVE_TIMEOUT` (90 s, const-asserted below `INACTIVE_TORRENT_REMOVE_TIMEOUT`) in a registry keyed by info hash. Routes that must answer promptly (stats/status) go through `EngineFS::get_or_begin_add_magnet` (non-blocking: `EngineLookup::Ready`/`Adding`/`Failed`) and report `EngineStats::resolving_metadata` or `EngineStats::magnet_add_failed`; routes that need the file list use `get_or_add_magnet`/`routes::compat::get_or_create_engine` (waits, joins an existing add, and is the only path that retries a failed one) and map the typed `MagnetAddError` with `compat::engine_creation_failure` (timeout → 504). A supervisor task settles the registry entry however the add ends (success removes it; error/timeout/abort — and a panic in debug builds; under the release profile's `panic = "abort"` a panic kills the process instead — turn it into a `FailedMagnetAdd` record), and the engine eviction loop sweeps entries nobody has looked up for the inactivity window. Never call `EngineFS::add_torrent` for a bare info hash from a route — `/{infoHash}/create` included. Trackers can only be supplied by the request that creates the engine (`LibrqbitHandle::add_trackers` is a documented no-op), so every creation path must pass the request's `tr=` trackers. For a magnet librqbit ignores `AddTorrentOptions::trackers` and reads only the link's own `tr=` params, so `LibrqbitBackend::add_torrent` folds the merged list into the magnet URL (`magnet_with_trackers`) — keep that when touching the add path.
- The `server` crate is also a **cdylib consumed over JNI** by stremio-android — keep `src/jni.rs` symbol names (`Java_com_stremio_mobile_server_...`) and `ServerConfig::embedded()` semantics stable.
- The `default-members`/`members` comment block in the root `Cargo.toml` documents why they're now identical — keep it in sync with any future membership change.
