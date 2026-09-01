# Stream Server

<div align="center">

**🚀 Open Source Torrent Streaming Engine**

*A modern, high-performance alternative to Stremio's closed-source `server.js`*

[![Release Build](https://github.com/perpetus/stream-server/actions/workflows/release.yml/badge.svg)](https://github.com/perpetus/stream-server/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![Open Source](https://img.shields.io/badge/Open%20Source-✓-brightgreen?style=flat-square)](https://github.com/perpetus/stream-server)

</div>

---

## 💡 About

Stream Server is a **fully open-source** replacement for Stremio's proprietary `server.js`. While Stremio's streaming backend remains closed-source, this project provides complete transparency, community-driven development, and the freedom to run your streaming engine locally on your own terms.

**Built in Rust** for maximum performance, minimal memory footprint, and rock-solid reliability.

### Projects using Stream Server

* **[stremio-native](https://github.com/perpetus/stremio-native)**: The recommended way to use Stream Server as a complete desktop application. It integrates the server directly with a native Stremio client and player. **Stremio Native is still in the early stages of development, so expect incomplete features and breaking changes.**
* **[stremio-android](https://github.com/perpetus/stremio-android)**: A native Kotlin/Compose Stremio client app for Android that integrates `stream-server` as a local JNI library/service for high-performance torrent streaming.

---

## 🌟 Why Stream Server?

| Feature | Stream Server | Stremio server.js |
|---------|--------------|-------------------|
| **Open Source** | ✅ **Yes** (MIT License) | ❌ Closed source |
| **Performance** | ⚡ Native Rust | Node.js overhead |
| **Memory Usage** | ~50MB | ~200MB+ |
| **Control** | ✅ Full local control | ⚠️ Limited |
| **Customizable** | ✅ Fork & modify | ❌ No access |
| **HLS Transcoding** | ✅ Built-in | ✅ |
| **Seekable Streams** | ✅ Instant | ⚠️ Variable |
| **Archive Streaming** | ✅ ZIP/7Z/TAR (RAR opt-in) | ✅ |

> **Seamless Migration**: Drop-in compatible with existing Stremio setups. Same API endpoints, same functionality — just faster and open source.

---

## ✨ Features

### Core Streaming
- **🚀 High Performance**: Pure Rust by default, with an optional C++ libtorrent backend
- **📺 HLS Transcoding**: Real-time video transcoding via FFmpeg (master.m3u8, stream.m3u8)
- **🔧 Multiple Backends**: `librqbit` (pure Rust, default) or `libtorrent` (battle-tested C++, opt-in)
- **📡 HTTP Range Requests**: Full support for instant seeking

### Media Support
- **📝 Subtitle Extraction**: Automatic detection, OpenSubtitles hash calculation
- **🎬 Video Probing**: FFprobe integration for track analysis
- **📦 Archive Streaming**: Direct playback from ZIP, 7Z, and TAR archives out of the box (pure Rust); RAR via the opt-in `rar` feature

### API Compatibility
- **🔌 Stats API**: `/stats.json` for server status and torrent info
- **🌐 Network Info**: `/network-info` endpoint for interface discovery
- **💓 Heartbeat**: `/heartbeat` for health checks
- **⚙️ Settings**: Runtime-configurable via `/settings`
- **🔒 BitTorrent Privacy Controls**: DHT, PeX, LSD, encryption, interface binding, ports, and proxy settings. See [BitTorrent Settings](docs/bittorrent-settings.md).

---

## 📦 Installation

### Pre-built Binaries

Download from [Releases](https://github.com/perpetus/stream-server/releases):

| Platform | Download |
|----------|----------|
| Windows (portable) | [Download EXE](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.exe) |
| Windows settings GUI | [Download EXE](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-settings-windows-amd64.exe) |
| Windows installer | [Download MSI](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.msi) |
| Debian / Ubuntu | [Download DEB](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.deb) |
| Linux (portable) | [Download binary](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64) |
| Linux settings GUI | [Download binary](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-settings-linux-amd64) |
| Linux (AppImage) | [Download AppImage](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.AppImage) |
| Arch Linux | [Download package](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-arch-x86_64.pkg.tar.zst) |
| Checksums and all assets | [View latest release](https://github.com/perpetus/stream-server/releases/latest) |

### Build from Source

The default build is **pure Rust** and needs **zero system libraries** — no libtorrent, no libclang, no GUI toolkits. The pinned toolchain in `rust-toolchain.toml` (Rust 1.98.0) is picked up automatically by rustup:

```bash
# Default build (pure Rust librqbit backend - recommended)
cargo build --release
```

Optional features pull in native dependencies:

```bash
# libtorrent backend (advanced; needs libtorrent-rasterbar + boost)
cargo build --release --no-default-features --features libtorrent

# Desktop tray + settings GUI (needs fontconfig/gtk on Linux)
cargo build --release --features gui

# RAR archive streaming (needs libclang + a C++ toolchain)
cargo build --release --features rar
```

Without the `rar` feature, RAR requests return a 501 JSON error; ZIP, 7Z, and TAR streaming are always built in.

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

The platform notes below are only needed for the **opt-in features** (`libtorrent`, `gui`, `rar`).

<details>
<summary><b>🐧 Arch Linux</b></summary>

```bash
sudo pacman -S rustup

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo pacman -S libtorrent-rasterbar boost pkg-config

# For the tray + settings GUI (--features gui)
sudo pacman -S fontconfig gtk3

# For RAR streaming (--features rar)
sudo pacman -S clang
```

</details>

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo apt install build-essential pkg-config libtorrent-rasterbar-dev libboost-all-dev

# For the tray + settings GUI (--features gui)
sudo apt install libfontconfig1-dev libgtk-3-dev

# For RAR streaming (--features rar)
sudo apt install build-essential libclang-dev
```

</details>

<details>
<summary><b>🐧 Fedora / RHEL</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
sudo dnf install gcc gcc-c++ pkg-config rb_libtorrent-devel boost-devel

# For the tray + settings GUI (--features gui)
sudo dnf install fontconfig-devel gtk3-devel

# For RAR streaming (--features rar)
sudo dnf install gcc-c++ clang-devel
```

</details>

<details>
<summary><b>🍎 macOS</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For the libtorrent backend (--no-default-features --features libtorrent)
brew install libtorrent-rasterbar boost pkg-config

# For RAR streaming (--features rar), Xcode Command Line Tools provide
# the C++ toolchain and libclang:
xcode-select --install
```

</details>

<details>
<summary><b>🪟 Windows</b></summary>

```powershell
# Default build: install Rust from https://rustup.rs — that's it.

# For the rar/libtorrent features: install Visual Studio Build Tools
# with "Desktop development with C++".

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

`librqbit` is the **default** backend: pure Rust, no system libraries, builds anywhere the pinned toolchain does. `libtorrent` is **opt-in** (`--no-default-features --features libtorrent`) for those who want the battle-tested C++ engine and are willing to install its native dependencies.

| Feature | librqbit (default) | libtorrent (opt-in) |
|---------|----------|------------|
| **Language** | Pure Rust | C++ via FFI |
| **System Dependencies** | ✅ None | libtorrent-rasterbar + boost |
| **Binary Size** | Smaller | Larger |
| **Maturity** | Newer | Battle-tested |
| **DHT** | ✅ | ✅ |
| **uTP** | ✅ | ✅ |
| **Piece Deadline** | ❌ | ✅ |
| **Windows Setup** | ✅ Easy | ⚠️ Complex (vcpkg) |

---

## 📁 Project Structure

```
stream-server/
├── server/           # HTTP server and API routes
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       ├── librqbit.rs   # Pure Rust backend (default)
│       └── libtorrent.rs # Native C++ backend (opt-in)
└── bindings/
    ├── libtorrent-sys/   # FFI bindings to libtorrent-rasterbar (libtorrent feature)
    └── async-rar/        # Vendored UnRAR bindings (rar feature)
```

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

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>

## Keywords

`stremio server.js alternative` `open source streaming engine` `torrent streaming` `local streaming` `desktop streaming` `hls transcoding` `rust torrent` `libtorrent` `video streaming server` `media engine` `torrent player` `stream torrents` `stremio alternative` `enginefs` `stremio open source`
