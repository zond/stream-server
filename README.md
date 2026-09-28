# Stream Server

A headless, pure-Rust torrent-streaming library with no system dependencies, forked from an open-source replacement for Stremio's `server.js`.

[![CI](https://github.com/zond/stream-server/actions/workflows/ci.yml/badge.svg)](https://github.com/zond/stream-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20src%20%2F%20GPL--3.0%20binary-blue?style=flat-square)](#license)

---

## About

Stream Server is the torrent-streaming half of [xtremio](https://github.com/zond/xtremio), a Flutter Stremio client, and it is a **Rust library** that the app links and starts in-process (`stream_server::start`, see [Library API](docs/api.md#library-api)). It began as a hard fork of [stremio-native/stream-server](https://github.com/stremio-native/stream-server), an open-source alternative to Stremio's closed-source `server.js`, and has no ambition to merge back: the API, the engine and the licensing have all diverged, and it is shaped by that one client. There is no standalone daemon, so no other Stremio client can run it.

Its goal is narrower than a `server.js` replacement's: a **headless torrent-streaming server with no system-library requirements**. A Rust toolchain and a C compiler build it (the C is what `aws-lc-sys` bundles, under rustls and librqbit's SHA-1); at run time it spawns no external program. It deliberately does not transcode -- the client plays containers and codecs directly through libmpv -- so the server's only job is getting torrent, archive, remote and Drive bytes onto an HTTP connection efficiently, and keeping what it fetched inside a bounded cache.

The torrent engine is [`librqbit`](https://github.com/ikatson/rqbit), the sole backend, through a fork ([`zond/rqbit`](https://github.com/zond/rqbit), pinned to one git rev in both crates) that follows upstream and adds what a bounded streaming cache needs from the engine: a per-stream lookahead window, piece reclaim (the engine forgets a piece so its storage may delete it), announcing only the pieces something chose to share and never taking an announcement back, a runtime per-torrent peer cap, a session-wide upload switch, per-piece chunk progress and a count of connected seeders, a flat re-dial schedule for a thin swarm's proven peers, and Mozilla's compiled-in TLS roots.

---

## What it does that `server.js` does not

Stremio's own server, and the upstream fork, exist to serve a web-based player: transcoding, subtitle conversion and a wide HTTP API. This one serves a native player it shares a process with, and the differences are the point.

| | This server | Stremio `server.js` / upstream fork |
|---|---|---|
| **Build and run** | Rust toolchain + C compiler; no system libraries; nothing spawned at run time | FFmpeg/FFprobe at run time; upstream: `libtorrent` (C++) or `librqbit` |
| **Transcoding** | None -- direct play; codecs and subtitles are the client's | HLS transcoding, probing, hwaccel profiles |
| **Control surface** | The embed API (`ServerHandle`, over FFI); HTTP only for players and the handful of routes stremio-core calls, behind a per-launch bearer token -- see [The HTTP surface](#the-http-surface) | Dozens of HTTP routes, open |
| **Cache** | One torrent-data root, bounded by `cacheSize` and the volume's free space; one retention owner per entity, windowed round the playhead; nothing is ever pre-allocated -- see [What bounds the cache](docs/storage.md#what-bounds-the-cache) | Files written whole; a periodic cache cleaner |
| **Offline downloads** | A pin on the same cache, for a torrent file, an addon link or a Google Drive file; one file per piece, played back through the media routes -- see [Offline downloads](docs/storage.md#offline-downloads) | -- |
| **Remote streams** | `/proxy` caches in 256 KiB chunks, is bounded by the same retention, and reads ahead of a player exactly as a torrent stream is -- see [Proxied remote streams](docs/proxy.md#proxied-remote-streams) | Relayed, nothing kept |
| **Google Drive** | A paired account's files as byte sources, the grant spent inside the server, downloadable and playable offline -- see [Google Drive files](docs/proxy.md#google-drive-files) | -- |
| **Archives** | ZIP, 7Z, TAR, RAR and ISO 9660/UDF read as byte ranges of wherever the archive lives -- nothing downloaded, nothing extracted; a member that would have to be decoded is refused with a sentence -- see [Archive members](docs/api.md#archive-members) | Extracted through native readers |
| **Casting** | A second, media-only listener a cast session turns on and off -- see [LAN media listener](docs/lan-media.md) | SSDP discovery and a casting API |
| **Startup honesty** | `phase`, the in-flight piece, tracker scrapes of the swarm, DHT health -- see [Startup phases](docs/stats.md#startup-phases-in-statsjson) | Progress percentages |
| **Thin swarms** | A proven peer of a starving torrent is re-dialled on a flat 60 s -- see [thin-swarm redial](docs/design/thin-swarm-redial.md) | -- |

**Deliberately not here:** HLS/FFmpeg transcoding and probing, subtitle conversion and OpenSubtitles hashing (the client fetches addon subtitles and picks tracks), an HTTPS listener, a daemon or command line, SSDP casting, NZB and YouTube routes. Every one of them existed upstream and was removed for lack of a consumer; [Removed routes](docs/api.md#removed-routes) lists them.

---

## Quick Start

There is no command to run: this crate builds no program. A host process links it and starts a server inside itself.

```toml
# In the embedder's Cargo.toml
[dependencies]
stream_server = { package = "server", path = "../stream-server/server" }
```

```rust
let handle = stream_server::start(stream_server::ServerConfig {
    // Where settings.json and logs/ go. An embedder must set this: the
    // default reads the platform config dir, which needs HOME/XDG_* to be
    // set.
    config_dir: Some(config_dir),
    // Torrent data and the proxy cache. Defaults to `config_dir/cache`.
    cache_dir: Some(cache_dir),
    ..Default::default()
})?;
let base_url = handle.base_url().to_string();   // http://127.0.0.1:<port>
let token = handle.auth_token().map(str::to_string); // control-route bearer
```

`ServerConfig::default()` is the only configuration: **loopback only** -- `127.0.0.1:11470`, no logging, a freshly generated per-launch bearer token the embedder reads off the handle, and an ephemeral BitTorrent listen port (`TorrentListenPort::Ephemeral`) so several servers (and the tests) coexist. `TorrentListenPort::Fixed(42000..42010)` -- the first free port of that range, stable and forwardable, and the only shape a UPnP mapping is worth taking for -- is there for an embedder that sets the field. There is no HTTPS listener: a server embedded in a host process binds loopback, and the remote address a certificate would be for belongs to the host. An embedder that wants the open media routes reachable from the local network (`/proxy` and `/ftp` among them, which fetch whatever URL they are handed) has to bind them there field by field, which is the point; the [LAN media listener](docs/lan-media.md) is the supported way to give a cast receiver what it needs and nothing else.

**Prefer `http_addr`'s port `0`** and read the one the OS picked from `ServerHandle::bound_http_addr()`. 11470 is the default because it is what stremio-core's default profile points `streaming_server_url` at, so a client that has never been told otherwise looks there -- but an embedder that retargets core at the address it read back (xtremio does, and so does every test) gains nothing from the number and can only lose the bind to a desktop Stremio, to a second instance of itself, or to whatever else holds the port.

**Hand in the pin sets.** `ServerConfig::pins` (torrent files) and `proxy_pins` (addon URLs and Drive files) are the offline downloads the embedder keeps; the server keeps no record of its own. `None` means *unknown* -- nothing is swept and every torrent is treated as pinned -- while an empty set means *nothing is pinned*, and the launch sweep takes everything unclaimed. See [docs/storage.md](docs/storage.md#offline-downloads).

---

## What a client can read

`ServerHandle::engine_stats` / `file_stats` (and the core's `stats.json`) report a startup `phase`, the progress of the one piece a starting stream waits on, tracker-scraped swarm counts and an error sentence when something failed; `stream_numbers` answers a playback panel, `background_traffic` a "working in the background" light, and `dht_status` whether the DHT ever came up. Each number is measured or absent, never a zero standing for "unknown" -- [docs/stats.md](docs/stats.md) says what each means and how to draw it.

---

## The HTTP surface

**The app does not speak HTTP to this server.** It calls `ServerHandle` methods, in-process, over FFI. HTTP exists for exactly two callers, and `build_router()` (`server/src/lib.rs`) is split along that line:

- **Players fetch media by URL** -- mpv, or a Chromecast receiver through the [LAN media listener](docs/lan-media.md). They cannot attach a header, so the **media routes are open**: torrent streams, archive members, `/proxy`, `/drive/stream`, `/ftp`, a finished download's `/downloads/{key}/stream`, and the `/local-addon` stub stremio-core's default profile asks for.
- **stremio-core's `StreamingServer` model speaks Stremio's streaming-server protocol** through the app's `Env::fetch`, which attaches the bearer. Those paths -- `/settings`, `/create`, `/{infoHash}/create`, `/{infoHash}/{fileIdx}/stats.json`, `/network-info`, `/device-info`, `/get-https`, `/casting`, `/casting/{devID}/player` -- are the whole of the **control routes**, and every one requires `Authorization: Bearer <token>`, in the header only.

**There is no third caller, and no control route is added.** A new capability is a `ServerHandle` method; a new byte-serving URL for a player is a media route; a path joins the control routes only when the stremio-core fork starts calling it, which is a change to that fork first. The control routes that mirrored the embed API are gone ([Removed routes](docs/api.md#removed-routes)).

The token is 32 random bytes, fresh per launch, read off `ServerHandle::auth_token()`; there is no way to start without one or to choose one, and the token never passes through `tracing`, so it is in no log. The loopback listener answers no CORS at all.

The full route table, the `ServerHandle` method reference, archive behaviour and the removed routes are in [docs/api.md](docs/api.md).

---

## Building

There is nothing to install: this crate builds no program of its own. An embedder adds it as a path or git dependency and calls `stream_server::start` (see [Quick Start](#quick-start)); building here checks that it compiles and runs its tests.

It needs Rust through [rustup](https://rustup.rs) -- `rust-toolchain.toml` pins the exact toolchain (1.98.0), and rustup installs it on the first build -- and a C compiler, for the C that `aws-lc-sys` bundles and builds from source (and, on macOS, a one-file shim `network-interface` compiles). No system libraries, for any feature combination. The C compiler is `build-essential` on Debian and Ubuntu, `base-devel` on Arch, `gcc` on Fedora, the Xcode command-line tools on macOS, and the MSVC C++ build tools rustup asks for on Windows.

```bash
cargo test                                  # default features: RAR on, through unrar-rs
cargo test -p server --no-default-features  # no RAR, no GPL code linked
```

| Feature | What it adds | Extra system deps |
|---|---|---|
| `rar` (**on by default**) | RAR archives read as byte ranges -- the volume layout of the pure-Rust `unrar-rs`, which decodes nothing here | None |

ZIP, 7Z, TAR and ISO streaming are always built in and not gated by any feature (the `tgz` prefix still exists and answers `415`: gzip has no way in at the middle). Because `unrar-rs` is GPL-3.0-or-later, a program that links this library with `rar` on is GPL-3.0-or-later; without it, RAR requests return a 501 JSON error. See [License](#license).

### CI

[`ci.yml`](.github/workflows/ci.yml) runs on every push and pull request to `master`/`main`, on the pinned 1.98.0 toolchain, with no `apt install` step:

| Job | What it runs |
|---|---|
| Format | `cargo fmt --all --check` |
| Clippy and Tests | `cargo clippy --all-targets --all-features`, `cargo doc --no-deps --all-features` (public and `--document-private-items`) and `cargo test`; the workspace lints in `Cargo.toml` deny every warning and all of `clippy::all` |
| MIT build (no RAR) | `cargo test -p server --no-default-features` -- the only place the `cfg(not(feature = "rar"))` paths compile |
| Android check (armv7, aarch64) | `cargo ndk -t armeabi-v7a -t arm64-v8a check -p server --all-targets --locked`, with the runner's NDK: a check, not a build -- nothing links and no test runs |
| Windows Build and Test | `cargo build` and `cargo test` -- the only job that compiles the `cfg(windows)` half of `diagnostics` |

Nothing builds or tests macOS, and `ci.yml` is the only workflow there is: this crate publishes nothing, so there is no release build, no packaging and no tag matrix.

---

## Layout

```
stream-server/
├── server/           # The library: ServerConfig, start/run, ServerHandle (src/lib.rs),
│                     # the media and control routers, the proxy cache, sources/ translators/ images/
├── enginefs/         # The torrent engine: the librqbit backend, the piece store,
│                     # the retention owner, the reconciler
└── docs/             # The reference behind this README, and design notes
```

Two crates, and neither builds a binary: `server` is the library an embedder links, `enginefs` is what it is built on. [AGENTS.md](AGENTS.md) maps the modules and lists the rules a change has to keep.

| Doc | What it is for |
|---|---|
| [docs/api.md](docs/api.md) | Every HTTP route, who calls it and what it answers; the `ServerHandle` methods; archive members; removed routes |
| [docs/stats.md](docs/stats.md) | What a client is told: `stats.json` fields, the playback panel, the background light, DHT health |
| [docs/settings.md](docs/settings.md) | Every settings key, the buffer profiles, the `bt*` torrent settings |
| [docs/storage.md](docs/storage.md) | Offline downloads, what bounds the cache, cache usage and cleaning |
| [docs/proxy.md](docs/proxy.md) | `/proxy` (redirects, playlists, credentials, caching, read-ahead), ending a proxied stream, Google Drive |
| [docs/lan-media.md](docs/lan-media.md) | The media-only listener a cast session turns on |
| [docs/known-issues.md](docs/known-issues.md) | What is open, and the standing hazards of working in this repo |
| [docs/design/](docs/design/) | Design notes for built features: [read-pattern retention](docs/design/read-pattern-retention.md), [translated sources](docs/design/translated-sources.md), [generic downloads](docs/design/generic-downloads.md), [thin-swarm redial](docs/design/thin-swarm-redial.md) |

---

## Upgrade notes

- **2026-09-28. A torrent download on its way always uploads**, whatever `seedingEnabled` says: downloading is activity, not idling. The setting now governs what was played or downloaded before, and `ServerHandle::set_idle_sharing_held` holds it off for the run without writing the setting -- call it with `true` while the app is in the background on a device where that should stop idle sharing, and `false` on the way back. The activity light's down half now also lights for addon-link and Drive downloads.
- **2026-09-26. Proxied and Drive streams are read ahead of.** A stream a player reads through `/proxy` (with its `p=` token) or `/drive/stream` now fetches the retention window ahead of the player -- or the rest of the file, where the budget covers it whole. Origin traffic per stream goes up by that much, on Drive against the file's quota; requests without a player token fetch exactly what they ask for.
- **2026-09-26. The app-facing control routes are gone** ([docs/api.md](docs/api.md#removed-routes) lists them). An embedder that called one over HTTP calls the matching `ServerHandle` method instead; the core-protocol routes and every media route are unchanged.
- **2026-09-08. Torrent data is stored one file per piece**, under `<cacheRoot>/rqbit-downloads/.pieces/<infoHash>/`, for the streaming cache and offline downloads alike. **There is no migration**: a download from before this is re-fetched as pieces, and any leftover whole file is removed by the first launch handed a pin set (`piece_store::sweep_legacy_downloads`) -- see [What bounds the cache](docs/storage.md#what-bounds-the-cache). `DownloadInfo::path` keeps its shape but names a file that will not appear; play a download through the media routes.
- **2026-09-04.** `<cacheRoot>/rqbit-downloads/dht-bootstrap.json` caches the addresses the DHT bootstrap names last resolved to. It is a fallback for a network whose DNS is broken and safe to delete.
- **2026-09-03.** Offline downloads add one `<infoHash>.bitv` per torrent beside the session state (fastresume bitfields; the first start after upgrading still hash-checks each torrent once). The pin set is the embedder's, handed in at startup. `settings.downloadsDir` is gone -- the one torrent-data root is `settings.cacheRoot` -- and a client that still sends it gets what any unknown key gets: nothing. A stray `pinned-downloads.json` is kept where it lies.

---

## License

**The source in this repository is MIT** -- see [LICENSE](LICENSE). It contains no GPL code. The `license` field in `server/Cargo.toml` says `MIT AND GPL-3.0-or-later` only because a manifest cannot state a licence per feature: `rar`, on by default, links GPL code.

**This repository distributes nothing.** It builds no binary and publishes no packages, so there is no download here that anyone receives under any licence. What it publishes is source, and that source is MIT.

**The GPL obligation passes to whoever links this library and distributes a program.** RAR streaming is on by default and is powered by the [`unrar-rs`](https://crates.io/crates/unrar-rs) crate, which is licensed **GPL-3.0-or-later**. The crate is fetched and linked only at build time, but linking it means the **program built from it** is distributed under GPL-3.0-or-later. Today that program is [xtremio](https://github.com/zond/xtremio), which links this library with `rar` on and ships [LICENSE-GPL-3.0](LICENSE-GPL-3.0) and `unrar-rs`'s own licence file inside the app. `LICENSE-GPL-3.0` is kept here (verbatim from gnu.org) so an embedder has the text to ship; it says nothing about this repository's own source. Any other embedder inherits the same duty. This is a deliberate choice: RAR support is wanted on by default, and the project is released openly.

To link **no GPL code at all**, build without the `rar` feature (see [Building](#building)).

MIT is GPL-compatible, so MIT source under a GPL program is fine; the obligation attaches to the distributed program, not to this repository's source.
