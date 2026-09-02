# Stream Server

<div align="center">

**🚀 Pure-Rust Torrent Streaming Engine**

*A headless, zero-system-dependency streaming backend, forked from Stremio's `server.js` replacement*

[![Release Build](https://github.com/zond/stream-server/actions/workflows/release.yml/badge.svg)](https://github.com/zond/stream-server/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-MIT%20src%20%2F%20GPL--3.0%20binary-blue?style=flat-square)](#-license)
[![Open Source](https://img.shields.io/badge/Open%20Source-✓-brightgreen?style=flat-square)](https://github.com/zond/stream-server)

</div>

---

## 💡 About

Stream Server is a hard fork of [perpetus/stream-server](https://github.com/perpetus/stream-server) (itself an open-source alternative to Stremio's closed-source `server.js`). This fork has a narrower, sharper goal: a **pure-Rust, headless torrent-streaming server with zero external binary or system-library requirements**. `cargo build` on a bare machine — no libtorrent, no libclang, no FFmpeg, no GUI toolkits — is enough to produce a working server, whether you build the default binary or the `--no-default-features` one.

To get there, this fork **deliberately drops Stremio server.js API compatibility**: there is no HLS transcoding, no FFmpeg/FFprobe integration, and no video-probing endpoints. Those existed to reformat video for Stremio's web-based player. This server instead sits behind a **new native client app** (Flutter, with `libmpv`/`media_kit` for playback, currently in development) that does direct play and handles codecs and subtitles itself — so the server's only job is getting torrent and archive bytes onto an HTTP connection efficiently, not transcoding them.

The torrent engine is [`librqbit`](https://github.com/ikatson/rqbit) — the **sole** torrent backend, pure Rust, no system libraries — consumed via a fork ([`zond/rqbit`](https://github.com/zond/rqbit)) that adds a configurable per-stream lookahead window, so the engine can prioritize the bytes a player is about to read differently for sequential playback, seeks, and background downloads. There used to be an optional C++ `libtorrent` backend; it has been removed entirely, along with its vcpkg build apparatus, so there is nothing left in this repo that pulls in a C or C++ toolchain for torrenting.

---

## 🌟 Why this fork?

| | Stream Server (this fork) | Upstream `server.js` / stream-server |
|---|---|---|
| **Build deps** | ✅ None — just the Rust toolchain | FFmpeg/FFprobe required at runtime; Node.js for `server.js` |
| **Transcoding** | ❌ Not the server's job — client plays containers/codecs directly | ✅ HLS transcoding via FFmpeg |
| **Torrent backend** | Pure-Rust `librqbit`, the only backend | Native libtorrent (or Node bindings) |
| **Open Source** | ✅ Source is MIT; default binary is GPL-3.0 (see [License](#-license)) | Upstream `server.js` is closed source |
| **Seekable Streams** | ✅ Instant, via HTTP range requests | ⚠️ Variable |
| **Archive Streaming** | ✅ ZIP/7Z/TAR/RAR built in (pure Rust) | ✅ |
| **Headless** | ✅ No tray, no desktop GUI in this repo | Varies |

This is not a drop-in replacement for `server.js` — the API surface it exposes is intentionally smaller. It's built to be the backend of one specific client, not a generic Stremio-compatible service.

---

## ✨ Features

### Core Streaming
- **🚀 Pure Rust, always**: the entire build — every feature combination — has no system-library or external-binary dependencies, only the pinned Rust toolchain
- **🔧 Single backend**: `librqbit` (pure Rust, via the `zond/rqbit` fork with configurable per-stream lookahead) is the only torrent engine — there is no C++ alternative to opt into
- **📡 HTTP Range Requests**: torrent pieces are streamed straight to HTTP range requests for instant seeking — direct play, no transcoding step in between

### Media & Archives
- **📦 Archive Streaming**: direct playback from ZIP, 7Z, TAR, NZB, and RAR archives out of the box (all pure Rust). RAR is **on by default** via `unrar-rs`, which is GPL-3.0-or-later, so the default binary is GPL-3.0-or-later — see [License](#-license); build `--no-default-features` for an MIT binary without RAR
- **📝 Subtitles**: external subtitle-file detection with pure-Rust SRT/ASS→VTT conversion, plus OpenSubtitles hash calculation (embedded-track extraction, which used FFmpeg, has been removed — the client handles embedded tracks itself)

### Addon & Status API
- **🔌 Local Stremio addon**: serves a Stremio-protocol addon (`manifest.json`, `catalog`, `meta`, `stream`) over scanned local/torrent content
- **📊 Stats API**: `/stats.json` for server status and torrent info
- **🌐 Network Info**: `/network-info` for interface discovery
- **💓 Heartbeat**: `/heartbeat` for health checks
- **⚙️ Settings**: runtime-configurable via `/settings`
- **🔒 BitTorrent Privacy Controls**: DHT, PeX, LSD, encryption, interface binding, ports, and proxy settings. See [BitTorrent Settings](docs/bittorrent-settings.md).

---

## 📦 Installation

### Pre-built Binaries

No releases have been published from this fork yet — build from source (see below). The [`release.yml`](.github/workflows/release.yml) workflow is wired up to publish Windows/Linux/Arch binaries from a `v*` tag when that happens, but no tag has been pushed so far.

### Build from Source

**The whole build is pure Rust and needs zero system libraries** — no libtorrent, no libclang, no FFmpeg, no GUI toolkits, for any feature combination this repo has. The pinned toolchain in `rust-toolchain.toml` (Rust 1.98.0) is picked up automatically by rustup, and this is exactly what CI verifies with no `apt install` step at all:

```bash
# Default build: pure-Rust librqbit backend (the only backend) + pure-Rust RAR.
# NOTE: this links unrar-rs (GPL-3.0-or-later), so this binary is
# GPL-3.0-or-later — see the License section below.
cargo build --release
```

To get an MIT-licensed binary instead, drop RAR:

```bash
# MIT binary: same librqbit backend, no RAR (no unrar-rs, no GPL)
cargo build --release --no-default-features
```

| Feature | What it adds | Extra system deps |
|---|---|---|
| *(default)* | `rar` (pure-Rust RAR via `unrar-rs`) on top of the always-on `librqbit` backend | None |
| `rar` | RAR archive streaming via pure-Rust `unrar-rs` (**on by default**) | None |

RAR streaming is **on by default** and pure Rust — no libclang or C++ toolchain. ZIP, 7Z, TAR, and NZB streaming are always built in too, and are not gated by any feature. Because `unrar-rs` is GPL-3.0-or-later, the default binary is GPL-3.0-or-later; drop the `rar` feature (`--no-default-features`) for an MIT binary, where RAR requests then return a 501 JSON error.

---

## 🚀 Quick Start

```bash
# Run the server
./stream-server

# Or with cargo
cargo run --release -p server
```

The server starts on `http://localhost:11470` by default (compatible with standard streaming server port).

### Startup phases in `stats.json`

`/{infoHash}/stats.json` and `/{infoHash}/{fileIdx}/stats.json` keep the server.js-compatible shape stremio-core parses and add these camelCase fields so a client can show honest pre-playback progress:

| Field | Meaning |
|---|---|
| `phase` | `resolvingMetadata` (no metadata yet), `checking` (hash-checking data already on disk), `buffering` (live, but the stream file's initial priority window is not fully on disk), `ready` (initial window on disk — playback can start), `error` |
| `checkedBytes`, `checkTotalBytes` | Hash-check progress; non-null only while `checking` |
| `initialWindowReadyBytes`, `initialWindowBytes` | Bytes of the stream file's head window (`min(4 MiB, file length)`) already verified on disk; non-null only in `buffering`/`ready`. Also present per entry in `files[]` |
| `peerDiscovery` | `{ seen, queued, connecting, live }` peer counters (`peers`/`unique`/`queued` remain as before) |

The top-level window/phase describe the guessed stream file for `/{infoHash}/stats.json` and the requested file for `/{infoHash}/{fileIdx}/stats.json`.

Both stats routes accept the same query parameters as `/{infoHash}/{fileIdx}` and behave like it when they are the first request for a torrent:

- **`tr=`** (repeatable, `tracker:`-prefixed values accepted, `dht:` ignored) — trackers merged into the engine when the stats request is the one that creates it. Poll stats before the first stream request freely: the engine is created exactly as the stream route would create it, so the addon's trackers are kept for the session. Trackers can only be set by the request that creates the engine — librqbit has no API to add trackers to a torrent later (`add_trackers` is a documented no-op), so a later request carrying extra trackers does not extend the set.
- **`f=`** (per-file route, repeatable) — file filters for resolving `fileIdx=-1`, as on the stream route.
- **`sources`** lists the trackers the torrent was added with (`url` only; librqbit exposes no per-tracker announce counters, so `numRequests`/`numFound`/`lastStarted` are `0`/empty).

**During metadata resolution** (a magnet whose info dictionary has not arrived yet) both routes answer immediately with `200` and `phase: "resolvingMetadata"`, `hasMetadata: false`, an empty `files` array, `streamLen: 0` and `sources` listing the trackers in use — the per-file route included, since there is no file list to index into yet. Requests never block on metadata, and concurrent requests for one magnet share a single resolution. Once metadata is known, a `fileIdx` that does not exist returns `404` as before.

---

## 🔧 Build Instructions

All you need on any platform is Rust via [rustup](https://rustup.rs) — `rust-toolchain.toml` pins the exact toolchain (1.98.0) and rustup installs it automatically on first build. No platform has any extra system packages to install; the steps below are the same everywhere:

```bash
cargo build --release
```

<details>
<summary><b>🐧 Arch Linux</b></summary>

```bash
sudo pacman -S rustup
rustup default stable
cargo build --release
```

</details>

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🐧 Fedora / RHEL</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🍎 macOS</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
cargo build --release
```

</details>

<details>
<summary><b>🪟 Windows</b></summary>

```powershell
# Install Rust from https://rustup.rs — that's it, no other tooling needed.
cargo build --release
```

</details>

---

## 📁 Project Structure

```
stream-server/
├── server/           # HTTP server, API routes, local Stremio-addon plumbing
│   └── src/archives/ # ZIP/7Z/TAR/NZB (always on) + RAR (default-on "rar" feature), all pure Rust
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       └── librqbit.rs   # The sole torrent backend (pure Rust)
├── stremio-runtime-stub/ # Legacy-compatible launcher shim
└── updater-helper/       # Self-update installer helper
```

There is no `bindings/` directory and no vcpkg apparatus: the optional C++ `libtorrent` backend and everything it needed to build (the `libtorrent-sys` FFI crate, `triplets/`, `vcpkg-overlays/`, `vcpkg.json`) have been removed. RAR is handled by the pure-Rust `unrar-rs` crate — a direct `server` dependency behind the default-on `rar` feature — so there is no separate RAR binding crate either.

---

## Upgrade notes

- Existing desktop installs: the librqbit DHT routing table is now stored at `<cache dir>/dht.json` (it previously lived under the XDG/`directories` project dir, which does not exist on Android). The old file is simply ignored and the DHT re-bootstraps once on the first start after upgrading — a one-time, self-healing cost.

---

## 📄 License

**The source in this repository is MIT** — see [LICENSE](LICENSE). It contains no GPL code; the `LICENSE` file is unchanged and stays MIT.

**Compiled default binaries are GPL-3.0-or-later, however.** RAR streaming is on by default and is powered by the [`unrar-rs`](https://crates.io/crates/unrar-rs) crate, which is licensed **GPL-3.0-or-later**. That crate is fetched and linked only at build time, but linking it means a **default-built, distributed binary of stream-server is covered by GPL-3.0-or-later**. This is a deliberate choice: RAR support is wanted on by default, and the project is released openly.

To produce an **MIT-licensed binary with no GPL code**, build without the `rar` feature (RAR requests then return a 501 JSON error):

```bash
cargo build --release --no-default-features
```

MIT is GPL-compatible, so shipping the MIT source alongside GPL default binaries is fine; the GPL obligation attaches to the compiled/distributed default binary, not to this repository's source.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>

## Keywords

`pure rust torrent streaming` `headless torrent server` `librqbit` `rust torrent` `video streaming server` `http range streaming` `torrent to http` `archive streaming` `stremio addon` `enginefs` `no ffmpeg`
