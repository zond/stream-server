# Stream Server

<div align="center">

**🚀 Pure-Rust Torrent Streaming Engine**

*A headless, zero-system-dependency streaming backend, forked from Stremio's `server.js` replacement*

[![Release Build](https://github.com/perpetus/stream-server/actions/workflows/release.yml/badge.svg)](https://github.com/perpetus/stream-server/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![Open Source](https://img.shields.io/badge/Open%20Source-✓-brightgreen?style=flat-square)](https://github.com/perpetus/stream-server)

</div>

---

## 💡 About

Stream Server is a hard fork of [perpetus/stream-server](https://github.com/perpetus/stream-server) (itself an open-source alternative to Stremio's closed-source `server.js`). This fork has a narrower, sharper goal: a **pure-Rust, headless torrent-streaming server with zero external binary or system-library requirements in its default build**. `cargo build` on a bare machine — no libtorrent, no libclang, no FFmpeg, no GUI toolkits — is enough to produce a working server.

To get there, this fork **deliberately drops Stremio server.js API compatibility**: there is no HLS transcoding, no FFmpeg/FFprobe integration, and no video-probing endpoints. Those existed to reformat video for Stremio's web-based player. This server instead sits behind a **new native client app** (Flutter, with `libmpv`/`media_kit` for playback) that does direct play and handles codecs and subtitles itself — so the server's only job is getting torrent and archive bytes onto an HTTP connection efficiently, not transcoding them.

The torrent engine itself is [`librqbit`](https://github.com/rqbit-torrent/rqbit), consumed via a fork ([`zond/rqbit`](https://github.com/zond/rqbit)) that adds a configurable per-stream lookahead window, so the engine can prioritize the bytes a player is about to read differently for sequential playback, seeks, and background downloads.

### Projects using Stream Server

* **[stremio-native](https://github.com/perpetus/stremio-native)**: The recommended way to use Stream Server as a complete desktop application. It integrates the server directly with a native Stremio client and player. **Stremio Native is still in the early stages of development, so expect incomplete features and breaking changes.**
* **[stremio-android](https://github.com/perpetus/stremio-android)**: A native Kotlin/Compose Stremio client app for Android that integrates `stream-server` as a local JNI library/service for high-performance torrent streaming.

---

## 🌟 Why this fork?

| | Stream Server (this fork) | Upstream `server.js` / stream-server |
|---|---|---|
| **Default build deps** | ✅ None — just the Rust toolchain | FFmpeg/FFprobe required at runtime; Node.js for `server.js` |
| **Transcoding** | ❌ Not the server's job — client plays containers/codecs directly | ✅ HLS transcoding via FFmpeg |
| **Torrent backend** | Pure-Rust `librqbit` by default | Native libtorrent (or Node bindings) |
| **Open Source** | ✅ Yes (MIT License) | Upstream `server.js` is closed source |
| **Seekable Streams** | ✅ Instant, via HTTP range requests | ⚠️ Variable |
| **Archive Streaming** | ✅ ZIP/7Z/TAR/RAR built in (pure Rust) | ✅ |
| **Headless** | ✅ No tray, no desktop GUI in this repo | Varies |

This is not a drop-in replacement for `server.js` — the API surface it exposes is intentionally smaller. It's built to be the backend of one specific client, not a generic Stremio-compatible service.

---

## ✨ Features

### Core Streaming
- **🚀 Pure Rust by default**: the default build has no system-library or external-binary dependencies — only the pinned Rust toolchain
- **🔧 Multiple Backends**: `librqbit` (pure Rust, default, via the `zond/rqbit` fork with configurable per-stream lookahead) or `libtorrent` (battle-tested C++, opt-in)
- **📡 HTTP Range Requests**: torrent pieces are streamed straight to HTTP range requests for instant seeking — direct play, no transcoding step in between

### Media & Archives
- **📦 Archive Streaming**: direct playback from ZIP, 7Z, TAR, NZB, and RAR archives out of the box (all pure Rust). RAR uses `unrar-rs`, which is GPL-3.0-or-later, so the default binary is GPL-3.0-or-later — see [License](#-license); build `--no-default-features --features librqbit` for an MIT binary without RAR
- **📝 Subtitles**: external subtitle-file detection with pure-Rust SRT/ASS→VTT conversion, plus OpenSubtitles hash calculation (embedded-track extraction, which used FFmpeg, has been removed — the client handles embedded tracks itself)
- **🎬 YouTube resolution**: resolves YouTube URLs to a direct playable stream via a managed `yt-dlp` (auto-downloaded and refreshed at runtime, not a build dependency)

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

Download from [Releases](https://github.com/perpetus/stream-server/releases):

| Platform | Download |
|----------|----------|
| Windows (portable) | [Download EXE](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.exe) |
| Windows installer | [Download MSI](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.msi) |
| Debian / Ubuntu | [Download DEB](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.deb) |
| Linux (portable) | [Download binary](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64) |
| Linux (AppImage) | [Download AppImage](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.AppImage) |
| Arch Linux | [Download package](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-arch-x86_64.pkg.tar.zst) |
| Checksums and all assets | [View latest release](https://github.com/perpetus/stream-server/releases/latest) |

Note: release binaries are built with the opt-in `libtorrent` backend (`--features libtorrent --no-default-features`), not the default `librqbit` build — see [Backend Comparison](#-backend-comparison).

### Build from Source

The **default build is pure Rust and needs zero system libraries** — no libtorrent, no libclang, no FFmpeg, no GUI toolkits. The pinned toolchain in `rust-toolchain.toml` (Rust 1.98.0) is picked up automatically by rustup, and this is exactly what CI verifies with no `apt install` step at all:

```bash
# Default build: pure-Rust librqbit backend + pure-Rust RAR, zero system deps.
# NOTE: this links unrar-rs (GPL-3.0-or-later), so this binary is
# GPL-3.0-or-later — see the License section below.
cargo build --release
```

The `libtorrent` backend is opt-in and pulls in native dependencies only when you ask for it. The `librqbit`-only build is also the way to get an MIT-licensed binary without RAR:

```bash
# libtorrent backend (advanced; needs libtorrent-rasterbar + boost)
cargo build --release --no-default-features --features libtorrent

# MIT binary: pure-Rust librqbit backend, no RAR (no unrar-rs, no GPL)
cargo build --release --no-default-features --features librqbit
```

| Feature | What it adds | Extra system deps |
|---|---|---|
| *(default)* | `librqbit` torrent backend + `rar` (pure-Rust RAR via `unrar-rs`) | None |
| `rar` | RAR archive streaming via pure-Rust `unrar-rs` (**on by default**) | None |
| `libtorrent` | Alternative torrent backend, C++ libtorrent-rasterbar via FFI | `libtorrent-rasterbar` 2.1.1+, Boost headers, pkg-config, a C++17 compiler |

RAR streaming is **on by default** and pure Rust — no libclang or C++ toolchain. ZIP, 7Z, TAR, and NZB streaming are always built in too. Because `unrar-rs` is GPL-3.0-or-later, the default binary is GPL-3.0-or-later; drop the `rar` feature (`--no-default-features --features librqbit`) for an MIT binary, where RAR requests then return a 501 JSON error. `libtorrent` and `librqbit` are mutually exclusive — build libtorrent with `--no-default-features` so it isn't compiled alongside the default backend.

---

## 🚀 Quick Start

```bash
# Run the server
./stream-server

# Or with cargo
cargo run --release -p server
```

The server starts on `http://localhost:11470` by default (compatible with standard streaming server port).

---

## 🔧 Build Instructions

For the **default build**, all you need on any platform is Rust via [rustup](https://rustup.rs) — `rust-toolchain.toml` pins the exact toolchain (1.98.0) and rustup installs it automatically on first build:

```bash
cargo build --release
```

The platform notes below are only needed for the **opt-in `libtorrent` backend**. RAR streaming is pure Rust and on by default — it needs no extra system packages.

<details>
<summary><b>🐧 Arch Linux</b></summary>

```bash
sudo pacman -S rustup

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo pacman -S libtorrent-rasterbar boost pkg-config
```

</details>

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo apt install build-essential pkg-config libtorrent-rasterbar-dev libboost-all-dev
```

</details>

<details>
<summary><b>🐧 Fedora / RHEL</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo dnf install gcc gcc-c++ pkg-config rb_libtorrent-devel boost-devel
```

</details>

<details>
<summary><b>🍎 macOS</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
brew install libtorrent-rasterbar boost pkg-config
```

</details>

<details>
<summary><b>🪟 Windows</b></summary>

```powershell
# Default build: install Rust from https://rustup.rs — that's it.

# For the libtorrent feature: install Visual Studio Build Tools
# with "Desktop development with C++". (RAR is pure Rust — nothing extra.)

# For libtorrent, use vcpkg with the repository's v3 triplet:
git clone https://github.com/microsoft/vcpkg
.\vcpkg\bootstrap-vcpkg.bat
.\vcpkg\vcpkg install `
  --triplet=x64-windows-v3-static-md-release `
  --overlay-triplets="$PWD\triplets" `
  --overlay-ports="$PWD\vcpkg-overlays"
```

The v3 `static-md` triplet keeps libtorrent static, compiles native release
objects with `/arch:AVX2`, and uses the dynamic MSVC runtime (`/MD`). This
matches the repository's x86-64-v3 Rust binaries and applications using
prebuilt Skia binaries.

</details>

---

## 📊 Backend Comparison

`librqbit` is the **default** backend: pure Rust, no system libraries, builds anywhere the pinned toolchain does. It's consumed from the [`zond/rqbit`](https://github.com/zond/rqbit) fork, which adds a configurable per-stream lookahead window so piece priority can be tuned per playback intent (sequential play, seek, background download, etc). `libtorrent` is **opt-in** (`--no-default-features --features libtorrent`) for those who want the battle-tested C++ engine and are willing to install its native dependencies; it's also what the published release binaries ship with.

| Feature | librqbit (default) | libtorrent (opt-in) |
|---------|----------|------------|
| **Language** | Pure Rust | C++ via FFI |
| **System Dependencies** | ✅ None | libtorrent-rasterbar + boost |
| **Binary Size** | Smaller | Larger |
| **Maturity** | Newer | Battle-tested |
| **DHT** | ✅ | ✅ |
| **uTP** | ✅ | ✅ |
| **Piece Deadline** | ❌ | ✅ |
| **Configurable stream lookahead** | ✅ (via `zond/rqbit` fork) | N/A |
| **Windows Setup** | ✅ Easy | ⚠️ Complex (vcpkg) |

---

## 📁 Project Structure

```
stream-server/
├── server/           # HTTP server, API routes, local Stremio-addon plumbing
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       ├── librqbit.rs   # Pure Rust backend (default)
│       └── libtorrent/   # Native C++ backend (opt-in)
└── bindings/
    └── libtorrent-sys/   # FFI bindings to libtorrent-rasterbar (libtorrent feature)
```

(RAR is handled by the pure-Rust `unrar-rs` crate — a direct `server` dependency behind the default-on `rar` feature — so there is no separate RAR binding crate.)

---

## 🐛 Troubleshooting

The default build has no system-library dependencies — the issues below only apply to the opt-in `libtorrent` feature.

<details>
<summary><b>libtorrent not found</b></summary>

```bash
pkg-config --exists libtorrent-rasterbar && echo "Found" || echo "Not found"
pkg-config --cflags --libs libtorrent-rasterbar
```

</details>

<details>
<summary><b>Boost not found</b></summary>

Install boost development headers:
- **Arch**: `boost`
- **Ubuntu**: `libboost-all-dev`
- **Fedora**: `boost-devel`
- **macOS**: `brew install boost`

</details>

---

## 📄 License

**The source in this repository is MIT** — see [LICENSE](LICENSE). It contains no GPL code; the `LICENSE` file is unchanged and stays MIT.

**Compiled default binaries are GPL-3.0-or-later, however.** RAR streaming is on by default and is powered by the [`unrar-rs`](https://crates.io/crates/unrar-rs) crate, which is licensed **GPL-3.0-or-later**. That crate is fetched and linked only at build time, but linking it means a **default-built, distributed binary of stream-server is covered by GPL-3.0-or-later**. This is a deliberate choice: RAR support is wanted on by default, and the project is released openly.

To produce an **MIT-licensed binary with no GPL code**, build without the `rar` feature (RAR requests then return a 501 JSON error):

```bash
cargo build --release --no-default-features --features librqbit
```

MIT is GPL-compatible, so shipping the MIT source alongside GPL default binaries is fine; the GPL obligation attaches to the compiled/distributed default binary, not to this repository's source.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>

## Keywords

`pure rust torrent streaming` `headless torrent server` `librqbit` `rust torrent` `libtorrent` `video streaming server` `http range streaming` `torrent to http` `archive streaming` `stremio addon` `enginefs` `no ffmpeg`
