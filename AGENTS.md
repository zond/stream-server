# AGENTS.md

Guidance for AI coding agents working on this repository.

## What this repo is

Stream Server is an open-source Rust replacement for Stremio's closed-source `server.js`: a local HTTP torrent-streaming engine with HLS transcoding (FFmpeg), archive streaming (RAR/ZIP/7Z/TAR/NZB), subtitle extraction, and Stremio-compatible API endpoints (`/stats.json`, `/heartbeat`, `/network-info`, `/settings`, `/hlsv2/*`, ...). Serves HTTP on **11470** and HTTPS on **12470** by default. MIT licensed. Consumed by stremio-native (desktop) and stremio-android (JNI).

**The default build is pure Rust** (librqbit backend, pure-Rust 7z): `cargo build` / `cargo test` must always succeed on a machine with **no** libclang, no fontconfig/gtk headers, and no libtorrent. Native-heavy paths (libtorrent, RAR) are opt-in. Do not break this property. The server is headless by design — no tray, no desktop GUI; graphical shells live in wrapper binaries in other repos.

## Workspace map

| Crate | Role | In default build? |
|---|---|---|
| `server/` | axum HTTP API. `src/lib.rs` holds `ServerConfig`, `build_router()`, and the embeddable `ServerHandle` (also a cdylib for Android JNI — see `src/jni.rs`). `src/main.rs` is the headless daemon binary (`--tui` ratatui mode, single-instance fslock). `src/routes/` has one file per API area. Also: `updater/` (self-update), `archives/` (RAR/ZIP/7Z/TAR/NZB sessions — 7z is pure-Rust `sevenz-rust2` in `archives/sevenz.rs`), `local_addon/`, `ffmpeg_setup.rs`, `ssdp.rs`, `state.rs` (`AppState`). | yes |
| `enginefs/` | Torrent engine abstraction. `src/backend/mod.rs` defines async traits `TorrentBackend`/`TorrentHandle`; implementations: `backend/librqbit.rs` (pure Rust, **default**) and `backend/libtorrent/` (C++ FFI; piece deadlines, playback leases). Plus `engine.rs`, `hls.rs` (FFmpeg transcode pipeline), `hwaccel.rs`, piece/disk/metadata caches, tracker management. | yes |
| `stremio-runtime-stub/` | Zero-dep `stremio-runtime` shim binary: spawns/monitors the server, waits for port 11470, prints the exact ready line legacy clients expect. | yes |
| `updater-helper/` | `stream-server-updater` binary that applies staged updates (SHA-256 verify, binary swap, restart). | yes |
| `bindings/libtorrent-sys/` | cxx FFI to libtorrent-rasterbar 2.1.1+. `build.rs` probes pkg-config first, then vcpkg. | no — `libtorrent` feature |
| `bindings/async-rar/` | Async RAR extraction over **vendored** UnRAR C++ sources (`vendor/unrar`, autocxx — needs libclang). | no — `rar` feature |

There is no `bindings/async-sevenz` anymore: the vendored 7-Zip C++ binding was replaced by the pure-Rust `sevenz-rust2` crate (a direct `server` dependency, always on).

The workspace `default-members` in the root `Cargo.toml` are exactly the pure-Rust set (`enginefs`, `server`, `stremio-runtime-stub`, `updater-helper`). `bindings/async-rar` and `bindings/libtorrent-sys` are members but excluded from bare cargo invocations — keep it that way.

## Toolchain

`rust-toolchain.toml` pins **1.98.0** (needed for the committed `Cargo.lock` and edition 2024); rustup resolves it automatically. CI installs the same version explicitly (`dtolnay/rust-toolchain@1.98.0`) — if you ever bump the pin, bump all three CI `Setup Rust` steps too.

## Build, test, lint

### Default (pure Rust, no system deps — always verify these before committing)

```bash
cargo build                 # debug; add --release for the optimized binary
cargo test
cargo fmt --all             # run before every commit; CI enforces --check
cargo clippy --all-targets -- -D clippy::correctness -D clippy::suspicious -W clippy::all
cargo run -p server         # http://localhost:11470
```

Do NOT pass `--workspace` to bare check/test/clippy — that pulls in the excluded native crates and fails on a headless machine.

### Opt-in features (each needs system deps; not buildable on a minimal machine)

| Command | System deps |
|---|---|
| `cargo build -p server --no-default-features --features libtorrent` | libtorrent-rasterbar **2.1.1+**, boost headers, pkg-config, C++17 compiler (Debian: `libtorrent-rasterbar-dev libboost-all-dev`). `LIBTORRENT_STATIC=1` forces static pkg-config linking. On Windows, use vcpkg with this repo's overlays: `vcpkg install --triplet=x64-windows-v3-static-md-release --overlay-triplets=./triplets --overlay-ports=./vcpkg-overlays`, then set `VCPKG_ROOT`, `VCPKG_INSTALLED_DIR`, `VCPKGRS_TRIPLET=x64-windows-v3-static-md-release`. |
| `cargo check -p server --features rar` | libclang (`libclang-dev`) + C++ toolchain, for autocxx over vendored UnRAR. Without the feature, RAR requests return a 501 JSON error. |

`libtorrent` and `librqbit` are mutually exclusive backends selected by features on `server`/`enginefs`; when both are enabled, libtorrent wins (see gating below), so build libtorrent with `--no-default-features`. Release binaries are built as `--features libtorrent --no-default-features`.

### Runtime deps

`ffmpeg` and `ffprobe` on PATH for HLS/probing (`server/src/ffmpeg_setup.rs` searches PATH and known tool dirs; Windows auto-downloads Jellyfin FFmpeg). Missing FFmpeg is a hard startup error in binary mode.

## CI

- `.github/workflows/ci.yml` (PR checks): `fmt` (cargo fmt --all --check), `test` (clippy gated on correctness+suspicious, then `cargo test`, deliberately **no apt installs** — it proves the zero-system-deps default build), `features` (installs libclang, then checks `rar`). The libtorrent feature is not checked in ci.yml; release.yml covers it.
- `.github/workflows/release.yml` (release matrix): builds `--features libtorrent --no-default-features` plus the aux binaries per platform.

## Conventions

- **Errors**: `anyhow` (`Result` + `.context()`) in application crates; `thiserror` for typed errors in bindings crates.
- **Logging**: `tracing` macros only (no `println!` for diagnostics). Subscriber setup lives in `server`; HTTP tracing via tower-http `TraceLayer`.
- **Async**: tokio multithreaded; `async-trait` for the backend traits; keep hot-path methods cheap (see `TorrentHandle::is_finished` doc comment — no full stats walks on stream-start). Blocking work (e.g. 7z decompression) goes through `spawn_blocking`.
- **Backend gating**: `#[cfg(feature = "libtorrent")]` vs `#[cfg(all(feature = "librqbit", not(feature = "libtorrent")))]` — libtorrent wins when both are on. Any change to `TorrentBackend`/`TorrentHandle` must compile under **both** feature sets; default-method fallbacks on `TorrentHandle` keep librqbit compiling when a capability is libtorrent-only. Verify libtorrent-gated code at least with `cargo check -p enginefs --no-default-features --features libtorrent` when you have the deps; otherwise say so.
- **Feature gating in server**: RAR behind `#[cfg(feature = "rar")]` returning 501 when disabled. New native/heavy deps must be optional behind a feature, never in the default set.
- **Routes**: one file per API area in `server/src/routes/`, registered centrally in `build_router()` in `server/src/lib.rs`. Preserve exact endpoint paths/response shapes — they mirror Stremio's server.js API (camelCase serde).
- **State**: `AppState` (Clone, Arc fields) in `server/src/state.rs`; DashMap for concurrent maps; `stream_engine()` selects disk-backed vs memory engine — keep stream/HLS routes consistent with it.
- **Platform code**: behind `cfg(target_os = ...)` with no-op stubs. Edition 2024 everywhere.
- **Tests**: inline `#[cfg(test)]` modules beside the code; integration test at `server/tests/embed.rs`. Bare `cargo test` runs the default (librqbit) set.
- **Versions**: all crates share one version (bump in lockstep across every Cargo.toml).
- **Commits**: conventional style — `feat(scope):`, `fix(scope):`, `build:`, `ci:`, `docs:`, `chore:`.

### Agent model economy

Use the cheapest model adequate for the task at hand. Small/fast models are fine for mechanical edits, docs updates, renames, and enumeration; reserve strong models for architecture decisions, concurrency-sensitive code, and adversarial review. This is a standing instruction from the repo owner to conserve tokens/quota.

## Gotchas & do-not-touch

- **Do not casually edit**: the `.github/workflows/release.yml` matrix (vcpkg caching keys hash `vcpkg.json`, `triplets/**`, `vcpkg-overlays/**`); `triplets/*.cmake` (the x86-64-v3 `/arch:AVX2` static-md triplet must match released binaries); `vcpkg-overlays/libtorrent/` (pinned libtorrent 2.1.1 port + patches); `vcpkg.json` baseline; vendored C++ in `bindings/async-rar/vendor/unrar`.
- **Contract strings**: `stremio-runtime-stub` depends on the literal ready line `EngineFS server started at http://127.0.0.1:11470` and port 11470 — changing startup output or default ports breaks legacy clients.
- **Release profile** (`panic = "abort"`, lto, opt-level=z) means no unwinding in release — don't rely on `catch_unwind`.
- **Single instance**: the server takes a lockfile in the temp dir (`stream-server.lock`); a second desktop instance exits silently.
- **Packaging metadata** in `server/Cargo.toml` (`[package.metadata.deb]`, `[package.metadata.wix]` GUIDs) is release-critical; never regenerate the WiX GUIDs.
- The `server` crate is also a **cdylib consumed over JNI** by stremio-android — keep `src/jni.rs` symbol names (`Java_com_stremio_mobile_server_...`) and `ServerConfig::embedded()` semantics stable.
- The `default-members` comment block in the root `Cargo.toml` documents why the native crates are excluded — keep it in sync with any membership change.
