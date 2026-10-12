# Stream Server

A headless, pure-Rust torrent-streaming library with no system dependencies, forked from an open-source replacement for Stremio's `server.js`.

[![CI](https://github.com/zond/stream-server/actions/workflows/ci.yml/badge.svg)](https://github.com/zond/stream-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20src%20%2F%20GPL--3.0%20binary-blue?style=flat-square)](#license)

---

## About

Stream Server is the torrent-streaming half of [xtremio](https://github.com/zond/xtremio), a Flutter Stremio client. It is a **Rust library** that the app links and starts in-process (`stream_server::start`, see [Library API](docs/api.md#library-api)). It began as a hard fork of [stremio-native/stream-server](https://github.com/stremio-native/stream-server), an open-source alternative to Stremio's closed-source `server.js`. It has no ambition to merge back: the API, the engine and the licensing have all diverged, and that one client shapes it. There is no standalone daemon, so no other Stremio client can run it.

Its goal is narrower than a `server.js` replacement's: a **headless torrent-streaming server with no system-library requirements**. A Rust toolchain and a C compiler build it (the C is what `aws-lc-sys` bundles, under rustls and librqbit's SHA-1). At run time it spawns no external program. It deliberately does not transcode: the client plays containers and codecs directly through libmpv. So the server's only job is getting torrent, archive, remote and Drive bytes onto an HTTP connection efficiently, and keeping what it fetched inside a bounded cache.

The torrent engine is [`librqbit`](https://github.com/ikatson/rqbit), the sole backend. It runs through a fork ([`zond/rqbit`](https://github.com/zond/rqbit), pinned to one git rev in both crates) that follows upstream. The fork adds what a bounded streaming cache needs from the engine: a per-stream lookahead window, a background stream (one that fetches ahead without being anybody's position, for the resume pre-want), piece reclaim (the engine forgets a piece so its storage may delete it), announcing only the pieces something chose to share and never taking an announcement back, a runtime per-torrent peer cap, a session-wide upload switch, per-piece chunk progress and a count of connected seeders, a flat re-dial schedule for a thin swarm's proven peers, and Mozilla's compiled-in TLS roots.

---

## What it does that `server.js` does not

Stremio's own server, and the upstream fork, exist to serve a web-based player: transcoding, subtitle conversion and a wide HTTP API. This one serves a native player it shares a process with. The differences are the point.

| | This server | Stremio `server.js` / upstream fork |
|---|---|---|
| **Build and run** | Rust toolchain + C compiler; no system libraries; nothing spawned at run time | FFmpeg/FFprobe at run time; upstream: `libtorrent` (C++) or `librqbit` |
| **Transcoding** | None -- direct play; codecs and subtitles are the client's | HLS transcoding, probing, hwaccel profiles |
| **Control surface** | The embed API (`ServerHandle`, over FFI); HTTP only for players and the handful of routes stremio-core calls, behind a per-launch bearer token -- see [The HTTP surface](#the-http-surface) | Dozens of HTTP routes, open |
| **Playback** | By opaque media id (`register`, `open_reader`): a blocking reader over a torrent file, a link, a Drive file, a download, an archive member or a file on the device, with no HTTP in between -- see [Library API](docs/api.md#library-api) | URLs only |
| **Cache** | One torrent-data root, bounded by `cacheSize` and the volume's free space; one retention owner per entity, windowed round the playhead; nothing is ever pre-allocated -- see [What bounds the cache](docs/storage.md#what-bounds-the-cache) | Files written whole; a periodic cache cleaner |
| **Offline downloads** | A pin on the same cache, for a torrent file, an addon link or a Google Drive file; one file per piece, played back through the media routes -- see [Offline downloads](docs/storage.md#offline-downloads) | -- |
| **Remote streams** | `/proxy` caches in 256 KiB chunks, is bounded by the same retention, and reads ahead of a player exactly as a torrent stream is -- see [Proxied remote streams](docs/proxy.md#proxied-remote-streams) | Relayed, nothing kept |
| **Google Drive** | A paired account's files as byte sources, the grant spent inside the server, downloadable and playable offline -- see [Google Drive files](docs/proxy.md#google-drive-files) | -- |
| **Archives** | ZIP, 7Z, TAR, RAR and ISO 9660/UDF read as byte ranges of wherever the archive lives -- nothing downloaded, nothing extracted; a member that would have to be decoded is refused with a sentence -- see [Archive members](docs/api.md#archive-members) | Extracted through native readers |
| **Casting** | A second listener a cast session turns on and off, serving only the ids the app published for it, each under a random token that unpublishing (or the listener's stop) cuts mid-body; for a receiver that cannot decode the file, a rendition -- one fragmented MP4 the server muxes in memory from what the embedder's `Producer` makes -- see [LAN media listener](docs/lan-media.md) | SSDP discovery and a casting API |
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
    // Where settings.json, read-positions.json and logs/ go. An embedder must set this: the
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

`ServerConfig::default()` is the only configuration, and it is **loopback only**: `127.0.0.1:11470`, no logging, a freshly generated per-launch bearer token the embedder reads off the handle, and an ephemeral BitTorrent listen port (`TorrentListenPort::Ephemeral`) so several servers (and the tests) coexist. An embedder that sets the field can use `TorrentListenPort::Fixed(42000..42010)`. It takes the first free port of that range, which is stable and forwardable, and it is the only shape a UPnP mapping is worth taking for. There is no HTTPS listener. A server embedded in a host process binds loopback, and the remote address a certificate would be for belongs to the host. The open media routes include `/proxy` and `/ftp`, which fetch whatever URL they are handed. An embedder that wants them reachable from the local network has to bind them there field by field, and that is the point. The [LAN media listener](docs/lan-media.md) is the supported way to give a cast receiver what it needs and nothing else.

**Prefer `http_addr`'s port `0`** and read the port the OS picked from `ServerHandle::bound_http_addr()`. The default is 11470 because stremio-core's default profile points `streaming_server_url` there, so a client that was never told otherwise looks there. An embedder that points core at the address it read back gains nothing from the fixed number. xtremio does that, and so does every test. A fixed port can only lose the bind: to a desktop Stremio, to a second instance of the app, or to whatever else holds the port.

**Hand in the pin set.** `ServerConfig::pins` is the offline downloads the embedder keeps, one `PinKey` each: `Torrent {info_hash, file_idx}`, `Url {target, headers}` or `Drive {file_id}`. The server keeps no record of its own. `None` means *unknown*: nothing is swept and every torrent is treated as pinned. An empty list means *nothing is pinned*, and the launch sweeps take everything unclaimed. See [docs/storage.md](docs/storage.md#offline-downloads).

---

## What a client can read

`ServerHandle::engine_stats` / `file_stats` (and the core's `stats.json`) report a startup `phase`, the progress of the one piece a starting stream waits on, tracker-scraped swarm counts and an error sentence when something failed. `stream_numbers` (or `media_stream_numbers` for an id) answers a playback panel, `media_read_wait` whether a player is stuck in a read, `cast_numbers` what a cast publication has served, `background_traffic` a "working in the background" light, and `dht_status` whether the DHT ever came up. Each number is measured or absent, never a zero standing for "unknown". [docs/stats.md](docs/stats.md) says what each means and how to draw it.

---

## The HTTP surface

**The app does not speak HTTP to this server.** It calls `ServerHandle` methods, in-process, over FFI. HTTP exists for exactly two callers, and `build_router()` (`server/src/lib.rs`) is split along that line:

- **Players fetch media by URL.** A Chromecast receiver does, through the [LAN media listener](docs/lan-media.md), which serves `/cast/{token}` (and a rendition's `/cast/{token}/stream.mp4`) for the ids the app published and nothing else. So does mpv, for what xtremio still plays by URL: `/ftp`, and a link whose host will not serve ranges, which `resolve` answers with a `/proxy` URL. Everything else xtremio plays by media id, through `ServerHandle::open_reader`, with no HTTP at all. Players cannot attach a header, so the **media routes are open**: torrent streams, archive members, `/proxy`, `/drive/stream`, `/ftp`, a finished download's `/downloads/{key}/stream`, and the `/local-addon` stub stremio-core's default profile asks for.
- **stremio-core's `StreamingServer` model speaks Stremio's streaming-server protocol** through the app's `Env::fetch`, which attaches the bearer. Those paths are the whole of the **control routes**: `/settings`, `/create`, `/{infoHash}/create`, `/{infoHash}/{fileIdx}/stats.json`, `/network-info`, `/device-info`, `/get-https`, `/casting`, `/casting/{devID}/player`. Every one requires `Authorization: Bearer <token>`, in the header only.

**There is no third caller, and no control route is added.** A new capability is a `ServerHandle` method. A new byte-serving URL for a player is a media route. A path joins the control routes only when the stremio-core fork starts calling it, and that is a change to that fork first. The control routes that mirrored the embed API are gone ([Removed routes](docs/api.md#removed-routes)).

The token is 32 random bytes, fresh per launch, read off `ServerHandle::auth_token()`. There is no way to start without one or to choose one. The token never passes through `tracing`, so it is in no log. The loopback listener answers no CORS at all.

The full route table, the `ServerHandle` method reference, archive behaviour and the removed routes are in [docs/api.md](docs/api.md).

---

## Building

There is nothing to install: this crate builds no program of its own. An embedder adds it as a path or git dependency and calls `stream_server::start` (see [Quick Start](#quick-start)). Building here checks that it compiles and runs its tests.

It needs Rust through [rustup](https://rustup.rs) and a C compiler. `rust-toolchain.toml` pins the exact toolchain (1.98.0), and rustup installs it on the first build. The C is what `aws-lc-sys` bundles and builds from source (and, on macOS, a one-file shim `network-interface` compiles). No feature combination needs a system library. The C compiler is `build-essential` on Debian and Ubuntu, `base-devel` on Arch, `gcc` on Fedora, the Xcode command-line tools on macOS, and the MSVC C++ build tools rustup asks for on Windows.

```bash
cargo test                                  # default features: RAR on, through unrar-rs
cargo test -p server --no-default-features  # no RAR, no GPL code linked
```

| Feature | What it adds | Extra system deps |
|---|---|---|
| `rar` (**on by default**) | RAR archives read as byte ranges -- the volume layout of the pure-Rust `unrar-rs`, which decodes nothing here | None |

ZIP, 7Z, TAR and ISO streaming are always built in, and no feature gates them. The `tgz` prefix still exists and answers `415`, because gzip has no way in at the middle. A program that links this library with `rar` on is GPL-3.0-or-later, because `unrar-rs` is. Without `rar`, RAR requests return a 501 JSON error. See [License](#license).

### CI

[`ci.yml`](.github/workflows/ci.yml) runs on every push and pull request to `master`/`main`, on the pinned 1.98.0 toolchain, with no `apt install` step:

| Job | What it runs |
|---|---|
| Format | `cargo fmt --all --check` |
| Clippy and Tests | `cargo clippy --all-targets --all-features`, `cargo doc --no-deps --all-features` (public and `--document-private-items`), `cargo test`, and `cargo run -p server --example embed_thread` (the embedding example, run, not only compiled); the workspace lints in `Cargo.toml` deny every warning and all of `clippy::all` |
| MIT build (no RAR) | `cargo test -p server --no-default-features` -- the only place the `cfg(not(feature = "rar"))` paths compile |
| Android check (armv7, aarch64) | `cargo ndk -t armeabi-v7a -t arm64-v8a check -p server --all-targets --locked`, with the runner's NDK: a check, not a build -- nothing links and no test runs |
| Windows Build and Test | `cargo build` and `cargo test` -- the only job that compiles any `cfg(windows)` path: the crash filter in `diagnostics::logging`, `resolved_path`, and the piece store's and local source's file handling |

Nothing builds or tests macOS. `ci.yml` is the only workflow there is: this crate publishes nothing, so there is no release build, no packaging and no tag matrix.

---

## Layout

```
stream-server/
├── server/           # The library: ServerConfig, start/run, ServerHandle (src/lib.rs),
│                     # the media and control routers, the proxy cache, media/ (ids and the
│                     # reader), cast and rendition/, sources/ translators/ images/
├── enginefs/         # The torrent engine: the librqbit backend, the piece store,
│                     # the retention owner, the holds, the reconciler
├── tools/lavf-harness/ # Not built by cargo: reads a rendition as a Cast receiver's
│                     # libavformat does
└── docs/             # The reference behind this README, and design notes
```

At run time everything the server keeps is under one directory, `<cacheRoot>/media-cache` (`enginefs::CACHE_DIR_NAME`): the piece store (`.pieces`), the proxy cache (`.proxy`) and the torrent session's records.

There are two crates, and neither builds a binary. `server` is the library an embedder links, and `enginefs` is what it is built on. [AGENTS.md](AGENTS.md) maps the modules and lists the rules a change has to keep.

| Doc | What it is for |
|---|---|
| [docs/api.md](docs/api.md) | Every HTTP route, who calls it and what it answers; the `ServerHandle` methods; archive members; removed routes |
| [docs/stats.md](docs/stats.md) | What a client is told: `stats.json` fields, the playback panel, the background light, DHT health |
| [docs/settings.md](docs/settings.md) | Every settings key, the buffer profiles, the `bt*` torrent settings |
| [docs/storage.md](docs/storage.md) | Offline downloads, what bounds the cache, who keeps a torrent running, sharing, cache usage and cleaning |
| [docs/proxy.md](docs/proxy.md) | `/proxy` (redirects, playlists, credentials, caching, read-ahead), ending a proxied stream, Google Drive |
| [docs/lan-media.md](docs/lan-media.md) | The listener a cast session turns on: published tokens and renditions, and nothing else; what a publication has served |
| [docs/known-issues.md](docs/known-issues.md) | What is open, and the standing hazards of working in this repo |
| [docs/design/read-pattern-retention.md](docs/design/read-pattern-retention.md) | Design, built: what the cache keeps and fetches, from what the reads are doing |
| [docs/design/translated-sources.md](docs/design/translated-sources.md) | Design, built: archives and disc images as byte ranges of a fetched file, nothing on disk |
| [docs/design/generic-downloads.md](docs/design/generic-downloads.md) | Design, built: downloads of links and Drive files, a pin on the proxy cache and a filler |
| [docs/design/thin-swarm-redial.md](docs/design/thin-swarm-redial.md) | Design, built in the rqbit fork: why a download goes quiet for minutes, and the re-dial |
| [docs/design/media-pipeline.md](docs/design/media-pipeline.md) | Design, built (steps A-E): media ids, the in-process reader, casting by published token, pins by id, the sniff, the resume pre-want |
| [docs/design/renditions.md](docs/design/renditions.md) | Design, partly built: a cast the receiver can decode, one fragmented MP4 made on demand (the server side and the copy and sound paths built; video transcode not) |
| [tools/lavf-harness/README.md](tools/lavf-harness/README.md) | How a rendition is measured the way a Chromecast's libavformat reads it |

---

## Upgrade notes

- **2026-10-05. What runs a torrent is declared.** A player screen's `p=` requests (or an `open_reader` with its `PlayToken`) hold its torrent until `ServerHandle::release_player(token)`. A cast holds from `publish` to `unpublish`. The last thing a viewer watched stays their idle share while idle sharing is allowed. A stream response holds only while it is delivered. Nothing else runs a torrent, in particular not the last thing a stream opened. Call `release_player` when a player screen closes. See [docs/storage.md](docs/storage.md), *Who keeps a torrent running*.
- **2026-10-05. A resumed film asks ahead of its reads.** `ServerHandle::set_resume` before `open_reader` makes the reader ask the swarm for the resume point when it opens. `note_media_position` remembers where a session on a torrent file ended, in `read-positions.json` beside `settings.json`.
- **2026-10-01. The cache directory is `<cacheRoot>/media-cache`**, renamed from `rqbit-downloads` with **no migration**. The first boot that finds `rqbit-downloads` deletes it whole, offline downloads included: it is renamed aside before the session opens and deleted after. A torrent pin the embedder hands in then reports held-nothing (`complete: false`, no `error`), and it is downloaded again when pinned again. `ServerConfig::pins` is now one `Option<Vec<PinKey>>` for both stores. It replaces `pins` (a torrent `PinSet`) and `proxy_pins`, and `None` keeps its meaning. `ServerHandle::pin(&MediaId)` / `unpin(&MediaId, delete_files)` pin by media id. See [docs/storage.md](docs/storage.md#offline-downloads).
- **2026-09-28. A torrent download on its way always uploads**, whatever `seedingEnabled` says: downloading is activity, not idling. The setting now governs what was played or downloaded before. `ServerHandle::set_idle_sharing_held` holds it off for the run without writing the setting. Call it with `true` while the app is in the background on a device where that should stop idle sharing, and with `false` on the way back. The activity light's down half now also lights for addon-link and Drive downloads.
- **2026-09-26. Proxied and Drive streams are read ahead of.** A stream a player reads through `/proxy` (with its `p=` token) or `/drive/stream` now fetches the retention window ahead of the player. It fetches the rest of the file instead where the budget covers the file whole. Origin traffic per stream goes up by that much, and on Drive it counts against the file's quota. Requests without a player token fetch exactly what they ask for.
- **2026-09-26. The app-facing control routes are gone** ([docs/api.md](docs/api.md#removed-routes) lists them). An embedder that called one over HTTP calls the matching `ServerHandle` method instead. The core-protocol routes and every media route are unchanged.
- **2026-09-08. Torrent data is stored one file per piece**, under `<cacheRoot>/rqbit-downloads/.pieces/<infoHash>/` (`media-cache` since 2026-10-01), for the streaming cache and offline downloads alike. **There is no migration**: a download from before this is re-fetched as pieces. The first launch handed a pin set removes any leftover whole file (`piece_store::sweep::sweep_legacy_downloads`). See [What bounds the cache](docs/storage.md#what-bounds-the-cache). `DownloadInfo::path` keeps its shape but names a file that will not appear. Play a download through the media routes.
- **2026-09-04.** `<cacheRoot>/rqbit-downloads/dht-bootstrap.json` (`media-cache` since 2026-10-01) caches the addresses the DHT bootstrap names last resolved to. It is a fallback for a network whose DNS is broken, and it is safe to delete.
- **2026-09-03.** Offline downloads add one `<infoHash>.bitv` per torrent beside the session state (fastresume bitfields). The first start after upgrading still hash-checks each torrent once. The pin set is the embedder's, handed in at startup. `settings.downloadsDir` is gone. The one torrent-data root is `settings.cacheRoot`, and a client that still sends `downloadsDir` gets what any unknown key gets: nothing. A stray `pinned-downloads.json` is kept where it lies.

---

## License

**The source in this repository is MIT** -- see [LICENSE](LICENSE). It contains no GPL code. The `license` field in `server/Cargo.toml` says `MIT AND GPL-3.0-or-later` only because a manifest cannot state a licence per feature: `rar`, on by default, links GPL code.

**This repository distributes nothing.** It builds no binary and publishes no packages, so there is no download here that anyone receives under any licence. What it publishes is source, and that source is MIT.

**The GPL obligation passes to whoever links this library and distributes a program.** RAR streaming is on by default and is powered by the [`unrar-rs`](https://crates.io/crates/unrar-rs) crate, which is licensed **GPL-3.0-or-later**. The crate is fetched and linked only at build time, but linking it means the **program built from it** is distributed under GPL-3.0-or-later. Today that program is [xtremio](https://github.com/zond/xtremio), which links this library with `rar` on and ships [LICENSE-GPL-3.0](LICENSE-GPL-3.0) and `unrar-rs`'s own licence file inside the app. `LICENSE-GPL-3.0` is kept here (verbatim from gnu.org) so an embedder has the text to ship. It says nothing about this repository's own source. Any other embedder inherits the same duty. This is a deliberate choice: RAR support is wanted on by default, and the project is released openly.

To link **no GPL code at all**, build without the `rar` feature (see [Building](#building)).

MIT is GPL-compatible, so MIT source under a GPL program is fine. The obligation attaches to the distributed program, not to this repository's source.
