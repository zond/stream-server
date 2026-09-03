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
- Subtitles are the client's job: there is no subtitle conversion, track discovery or OpenSubtitles hashing in the server (see [Removed routes](#removed-routes))

### Control API
- **🔐 Per-launch bearer token** on every non-media route; media routes stay open for players. See [API](#-api)
- **📚 Library API**: an embedder calls `ServerHandle::{settings, update_settings, engine_stats, file_stats, pin_download, unpin_download, downloads, download_path}` directly — the same code the HTTP routes run, no HTTP client needed
- **📊 Stats**: `/stats.json`, `/{infoHash}/stats.json`, `/{infoHash}/{fileIdx}/stats.json` for server status and torrent progress
- **⚙️ Settings**: runtime-configurable via `/settings`, with the stremio-core-compatible shape
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

The server starts on `http://localhost:11470` by default (compatible with standard streaming server port). Every control route requires a bearer token for this launch; the binary chooses it from its command line:

| Flag / variable | Effect |
|---|---|
| *(nothing)* | A fresh random token is generated and printed **to stdout** as `control API token: <token>` — it is never written to the log files. (With `--tui` the alternate screen hides that line; use one of the options below instead.) |
| `--token <t>` / `--token=<t>` | Use exactly this token (headless use: the operator already knows it, nothing is printed) |
| `STREAM_SERVER_TOKEN=<t>` | Same as `--token`, from the environment; `--token` wins if both are given, a blank value counts as unset |
| `--no-auth` | Run the control API open (every route answers without a token). Wins over `STREAM_SERVER_TOKEN`; contradicts an explicit `--token` and is rejected together with it |

The `stremio-runtime` stub spawns the server with `--no-auth`: it is the compatibility shim for legacy clients that speak plain HTTP and cannot send the header. See [API](#-api).

### Startup phases in `stats.json`

`/{infoHash}/stats.json` and `/{infoHash}/{fileIdx}/stats.json` keep the server.js-compatible shape stremio-core parses and add these camelCase fields so a client can show honest pre-playback progress:

| Field | Meaning |
|---|---|
| `phase` | `resolvingMetadata` (no metadata yet), `checking` (hash-checking data already on disk), `buffering` (live, but the stream file's initial priority window is not fully on disk), `ready` (initial window on disk — playback can start), `error` |
| `error` | Present only with `phase: "error"` when anything knows why: the message of a failed magnet add (metadata timeout, backend error), see below, or a fixed message for a torrent the backend put in an error state (broken download folder, full disk) — the backend's own text names server paths and stays in the log |
| `checkedBytes`, `checkTotalBytes` | Hash-check progress; non-null only while `checking` |
| `initialWindowReadyBytes`, `initialWindowBytes` | Bytes of the stream file's head window (`min(4 MiB, file length)`) already verified on disk; non-null only in `buffering`/`ready`. Also present per entry in `files[]` |
| `peerDiscovery` | `{ seen, queued, connecting, live }` peer counters (`peers`/`unique`/`queued` remain as before) |

The top-level window/phase describe the guessed stream file for `/{infoHash}/stats.json` and the requested file for `/{infoHash}/{fileIdx}/stats.json`.

Both stats routes accept the same query parameters as `/{infoHash}/{fileIdx}` and behave like it when they are the first request for a torrent:

- **`tr=`** (repeatable, `tracker:`-prefixed values accepted, `dht:` ignored) — trackers merged into the engine when the stats request is the one that creates it. Poll stats before the first stream request freely: the engine is created exactly as the stream route would create it, so the addon's trackers are kept for the session (the engine passes them to librqbit as `tr=` params of the magnet link it adds — librqbit reads a magnet's trackers from the link alone, so `sources` lists them once metadata arrives). Trackers can only be set by the request that creates the engine — librqbit has no API to add trackers to a torrent later (`add_trackers` is a documented no-op), so a later request carrying extra trackers does not extend the set.
- **`f=`** (per-file route, repeatable) — file filters for resolving `fileIdx=-1`, as on the stream route.
- **`sources`** lists the trackers the torrent was added with (`url` only; librqbit exposes no per-tracker announce counters, so `numRequests`/`numFound`/`lastStarted` are `0`/empty).

**During metadata resolution** (a magnet whose info dictionary has not arrived yet) both routes answer immediately with `200` and `phase: "resolvingMetadata"`, `hasMetadata: false`, an empty `files` array, `streamLen: 0` and `sources` listing the trackers in use — the per-file route included, since there is no file list to index into yet. Requests never block on metadata, and concurrent requests for one magnet share a single resolution — the stream routes, both stats routes and stremio-core's `/{infoHash}/create` all join the same in-flight add. Once metadata is known, a `fileIdx` that does not exist returns `404` as before.

**Metadata resolution is bounded**: an add that has not produced metadata after **90 s** (`enginefs::METADATA_RESOLVE_TIMEOUT`) is given up on. Requests that were waiting for it (`/{infoHash}/{fileIdx}`, `HEAD`, `/{infoHash}/create`) get `504 Gateway Timeout` (`502` if librqbit itself refused the add, `500` otherwise; bodies are fixed strings, details go to the log). The failure is remembered: until something retries it, both stats routes answer `200` with `phase: "error"` and an `error` message for that hash, so a poller can stop waiting. Only a request that needs the file list (stream, `HEAD`, `/create`) retries — a fresh play attempt gets a fresh 90 s — while stats polls never restart an add. A failure record nobody has asked about for 5 minutes is dropped by the same inactivity sweep that removes idle torrents; the next request then starts over.

---

## 🔌 API

The HTTP surface is deliberately small and split in two by `build_router()` (`server/src/lib.rs`):

- **Media routes are OPEN.** They hand bytes to a player. Players (mpv, a future Chromecast receiver) fetch the URLs stremio-core builds for them (`types/resource/stream.rs`) and cannot attach headers, so these routes take no token.
- **Everything else is control API and requires a bearer token**: `Authorization: Bearer <token>`, header only — a token in the query string is never accepted, so it does not end up in access logs or in URLs handed to third parties. A missing or wrong token gets `401` with the fixed body `unauthorized` and `WWW-Authenticate: Bearer`; the compare is constant-time. Control routes are what stremio-core's `StreamingServer` model calls through `Env::fetch` (so the embedding client attaches the header there), plus the app/test status probes.

### Authentication

`ServerConfig.auth: ServerAuth` decides how the token is chosen:

| Variant | Meaning |
|---|---|
| `Generated` (**default** for both `ServerConfig::embedded()` and `ServerConfig::binary_default()`) | 32 random bytes, hex-encoded, fresh per launch. The standalone binary prints it once to stdout at startup (`control API token: <token>`) and never passes it to `tracing`, so it is in no log file; an embedder reads `ServerHandle::auth_token()` |
| `Token(String)` | Use exactly this token (must not be empty). The binary's `--token <t>` flag and `STREAM_SERVER_TOKEN` variable select this |
| `Disabled` | No authentication; every route is open. The binary's `--no-auth` flag selects this, and the `stremio-runtime` stub always passes it. The Android JNI entry point (`server/src/jni.rs`) also runs this way: it can return only a URL to the Kotlin side, and its listener is loopback-only |

### Routes

| Method | Path | Access | Consumer |
|---|---|---|---|
| GET, HEAD | `/{infoHash}/{fileIdx}` | OPEN | players — the stream URL stremio-core builds (`?tr=…`, `?f=…` as documented under [Startup phases](#startup-phases-in-statsjson)) |
| GET, HEAD | `/stream/{infoHash}/{fileIdx}` | OPEN | players (alias of the above) |
| GET, POST | `/{rar\|zip\|7zip\|tar\|tgz}/create`, `/{…}/create/{key}` | OPEN | players — archive session creation via `?lz=` (stremio-core builds these URLs) |
| GET | `/{rar\|zip\|7zip\|tar\|tgz}/stream`, `/{…}/stream/{key}`, `/{…}/stream/{key}/{*file}` | OPEN | players — archive member bytes |
| GET, POST | `/nzb/create`, `/nzb/create/{key}` | OPEN | players |
| GET | `/nzb/stream`, `/nzb/stream/{key}/{*file}` | OPEN | players |
| GET | `/ftp/{filename}?lz=…` | OPEN | players (HTTP/FTP passthrough) |
| any | `/proxy/{*rest}` | OPEN | players — proxied HTTP streams with injected headers |
| GET | `/local-addon/manifest.json` | OPEN | stremio-core default profile — **stub**: a valid manifest (`org.stremio.local`, "Local Files") declaring no types, resources or catalogs |
| GET | `/local-addon/stream/{type}/{id}`, `/local-addon/stream/{type}/{id}.json` | OPEN | stremio-core default profile — **stub**: always `{"streams": []}` |
| GET | `/local-addon/meta/{type}/{id}` | OPEN | stremio-core default profile — **stub**: `404`, logged at debug level only |
| GET | `/heartbeat` | TOKEN | app / tests |
| GET | `/stats.json` (`?sys=1` adds `loadavg`/`cpus`) | TOKEN | app |
| GET | `/{infoHash}/stats.json`, `/{infoHash}/{fileIdx}/stats.json` | TOKEN | stremio-core `Statistics`; accept `tr=`/`f=` like the stream route |
| POST | `/create` | TOKEN | stremio-core `CreateTorrent` (torrent blob / URL) |
| POST | `/{infoHash}/create` | TOKEN | stremio-core `CreateTorrent` (magnet) |
| GET, POST | `/settings` | TOKEN | stremio-core `StreamingServer` (`{ baseUrl, options, values }` / `{ success }`) |
| GET | `/network-info`, `/device-info` | TOKEN | stremio-core `StreamingServer` |
| GET | `/casting` | TOKEN | stremio-core playback devices (always `[]` — no casting). No trailing slash: `/casting/` is an unknown path (`404`) |
| POST | `/casting/{devID}/player` | TOKEN | stremio-core `play_on_device`; answers `501` because casting is not implemented |
| GET | `/get-https?authKey=…&ipAddress=…` | TOKEN | stremio-core remote-HTTPS certificate fetch |
| POST | `/{infoHash}/{fileIdx}/download` | TOKEN | offline downloads — pin the file; optional body `{"trackers":[…]}` (`sources`/`announce` accepted too), answer is a `DownloadInfo`. See [Offline downloads](#offline-downloads) |
| DELETE | `/{infoHash}/{fileIdx}/download?deleteFiles=1` | TOKEN | offline downloads — drop the pin, and with `deleteFiles` the data too |
| GET | `/downloads.json` | TOKEN | offline downloads — every pinned file |

Unknown paths get `404`, a wrong method on a known path `405` (or `401` first, on a control route).

RAR routes return a `501` JSON error in a `--no-default-features` build.

### Library API

An embedder holds a `ServerHandle` (from `stream_server::start`) and never needs an HTTP client for control calls. Every method runs on the server's own runtime and blocks the calling thread until done; all returned types are `serde`-serializable, so they can be passed as JSON over FFI:

| Method | Same as |
|---|---|
| `auth_token() -> Option<&str>` | the token control routes require (`None` with `ServerAuth::Disabled`) |
| `base_url() -> &str` | `settings.baseUrl` |
| `settings() -> Result<ServerSettings>` | `GET /settings` → `values` |
| `update_settings(patch: serde_json::Value) -> Result<ServerSettings>` | `POST /settings` (same keys, validation, engine update and persistence); returns the settings afterwards |
| `engine_stats(info_hash, trackers: &[String]) -> Result<EngineStats>` | `GET /{infoHash}/stats.json?tr=…` — including creating the engine with `trackers` when it is the first request for the hash and answering `resolvingMetadata` at once. `trackers` are normalised inside the shared function exactly like `tr=` (`tracker:` prefix stripped, `dht:` dropped, trimmed), so a stream's `sources` array can be passed as is |
| `file_stats(info_hash, file_idx: usize, trackers) -> Result<EngineStats>` | `GET /{infoHash}/{fileIdx}/stats.json?tr=…`; the route's `404` is a `FileNotFound` error |
| `pin_download(info_hash, file_idx: usize, trackers) -> Result<DownloadInfo>` | `POST /{infoHash}/{fileIdx}/download` — pin the file as an offline download (see [Offline downloads](#offline-downloads)); `trackers` are normalised as for `engine_stats` |
| `unpin_download(info_hash, file_idx: usize, delete_files: bool) -> Result<bool>` | `DELETE /{infoHash}/{fileIdx}/download?deleteFiles=1`; `false` when nothing was pinned |
| `downloads() -> Result<Vec<DownloadInfo>>` | `GET /downloads.json` |
| `download_path(info_hash, file_idx: usize) -> Result<Option<String>>` | the `path` of that file's `downloads()` entry on its own — where to hand a finished download to a local player. Never creates an engine |

The HTTP handlers and these methods call the same functions (`routes::system::{engine_stats, file_stats, update_settings}`, `routes::downloads::{pin_download, unpin_download, downloads, download_path}`), so they cannot drift; `server/tests/embed.rs` compares them.

### Offline downloads

A **pinned** file stays wanted no matter which file of the torrent is being played, and its torrent is exempt from idle removal and the seeding-disabled pause for as long as it has a pinned file — which also keeps it out of the cache cleaner's reach: the cleaner protects the files of every live engine at their on-disk paths, and a pinned engine stays live. `stats.json` reports `pinnedFiles` and, per file, `pinned` and `complete` (`downloaded == length`).

- **Pin**: `POST /{infoHash}/{fileIdx}/download` (`ServerHandle::pin_download`). The body is optional; `{"trackers":["udp://…"]}` — or the same array under `sources`/`announce`, so a stream's own field can be posted as is — supplies the trackers, which, as everywhere else, only matter when this request is the one that creates the engine. Idempotent. The answer is one `DownloadInfo`: `{infoHash, fileIdx, path, name, length, downloaded, complete, phase, error}`. A file the torrent does not have (or a `{fileIdx}` that is not a number) is a `404`, a disk without room a `507`, a magnet that never resolved a `504` — bodies carry `PinDownloadError::client_message` only, which names no local path.
- **List**: `GET /downloads.json` (`ServerHandle::downloads`) — the same `DownloadInfo` shape, one entry per pinned file, ordered by info hash then file index. A dormant pin (see *Restarts* below) is listed last with `phase: "error"` and an `error` explaining that its torrent is not managed right now; `path`/`length`/`downloaded` are then unknown (`null`/`0`). `ServerHandle::download_path(info_hash, file_idx)` returns just the `path` — what a client hands to a local player once `complete` — without creating anything.
- **Unpin**: `DELETE /{infoHash}/{fileIdx}/download` (`ServerHandle::unpin_download`) answers `{infoHash, fileIdx, unpinned, deletedFiles}`; `unpinned: false` means nothing was pinned. Without `?deleteFiles=1` only the pin goes: the bytes stay where they are and the torrent becomes an ordinary, evictable one again (with nothing playing, the file simply keeps downloading until the idle sweep or the next file selection). With `?deleteFiles=1` the data goes too — the whole torrent (files, session record and its now-empty `<downloadsDir>/<infoHash>` folder) when this was its last pin, only the one file while other files of it stay pinned, since the torrent must keep running for them. Deleting does not require the file to have been pinned. An unpin that keeps the files leaves the folder in place, of course; and a pin whose torrent the backend does not have has nothing to delete.
- **Where**: `settings.downloadsDir` (`POST /settings`, `null` by default). Unset, pinned torrents live in the cache root like everything else (`<cacheRoot>/rqbit-downloads`). Set to an absolute path (created if missing; the update fails if it cannot be created or written), every pin from then on places its torrent in `<downloadsDir>/<infoHash>/` wanting only the pinned file — and a torrent already managed in the cache root (streamed first, then pinned) is **relocated** by the pin: dropped from the session keeping its files, files moved (rename, copy + remove across devices), re-added in place, which re-checks the moved data (`phase: checking` for a moment). While the files move the torrent has no live handle, so the hash is looked up as an in-flight add: `stats.json` reports `resolvingMetadata` and a stream request waits for the relocated engine (a cross-device copy of a large file can take minutes) instead of failing on the dropped one. A persisted `downloadsDir` that is unusable at startup is cleared with a warning — in the settings file too, so `GET /settings`, the file and the next boot agree.
- **Free space**: a pin is refused (`PinDownloadError::InsufficientSpace`) when the destination volume has less than what the pin will write there plus a **500 MiB margin** (`PIN_FREE_SPACE_MARGIN`): the file's missing bytes — or, when the pin relocates the torrent onto a *different* volume, the full length of every file the move copies (the pinned file plus any other file with verified data; empty placeholders are dropped, not moved). A relocation within one volume is a rename and costs only the missing bytes; re-pinning a complete file in place needs nothing, and a torrent still `checking` data that may already be in place (right after a restart, or a fresh pin into a `<downloadsDir>/<infoHash>` folder that already existed — the cache dir purged, the downloads dir intact) is not measured at all — its want-set is already librqbit's; a relocation of a torrent that is still checking is sized from what its files have allocated on disk (`downloaded` reads 0 until the check ends). A refused pin drops the torrent only when it placed it under `downloadsDir` itself — a torrent in the cache root (added without a downloads dir, or by the stream request whose in-flight add the pin joined) stays for the idle sweeper — and the folder's files go with it only when the pin created the folder.
- **Cache cleaner**: the 30-day and `cacheSize` rules apply to the cache root only. Nothing under `downloadsDir` is walked at all — not evicted, not counted towards `cacheSize` — even when it sits inside the cache root, and the session's own records there (`session.json`, `<infoHash>.torrent`, the `.bitv` bitfields, `pinned-downloads.json` and their temp files) are never cache either.
- **Restarts**: librqbit persists each torrent's place and want-set and, with fastresume, its verified pieces (`<cacheRoot>/rqbit-downloads/<infoHash>.bitv`), so a pinned download resumes where it was without a full re-hash; the pin set itself is persisted in `<cacheRoot>/rqbit-downloads/pinned-downloads.json` and restored at startup. A pin whose torrent the session did not bring back (librqbit skips, but keeps the record of, a torrent whose output folder is on a volume that is not mounted at boot) stays in that file, dormant, until the torrent returns — on a later boot, or with the next pin of the same torrent, which applies the dormant pins alongside its own — or until it is unpinned.

`ServerConfig::embedded()` (the `Default`) is tuned for a host process: loopback HTTP on 11470, no logging/TUI/SSDP, a generated token, and `torrent_listen_port: TorrentListenPort::Ephemeral` — librqbit's incoming BitTorrent listener takes an OS-assigned port, so any number of embedded servers (and the tests) coexist with a desktop instance. `ServerConfig::binary_default()` keeps `TorrentListenPort::Fixed(42000..42010)`: the first free port of the range, stable and forwardable. Set the field explicitly if an embedder needs a fixed port.

### Removed routes

Everything below existed for server.js compatibility and had no consumer in stremio-core, the Flutter client or the tests; it was removed to shrink the attack surface to what is actually used:

- `/` (redirect to web.stremio.com), `/favicon.ico`, `/thumb.jpg`, `/samples/{filename}` — desktop/web-UI leftovers.
- `/list`, `/removeAll`, `/{infoHash}/remove`, `/{infoHash}/peers`, the `GET` variants of `/create` and `/{infoHash}/create` — engine management nothing called (stremio-core POSTs).
- `/diagnostics/*` — local debugging endpoints; the memory sampler still logs its snapshot.
- All subtitles routes (`/subtitles.vtt`, `/subtitles.{ext}`, `/{infoHash}/{fileIdx}/subtitles.vtt`, `/opensubHash`, `/opensubHash/{infoHash}/{fileIdx}`, `/subtitlesTracks`) and the engine code behind them — the client fetches addon subtitles and selects tracks itself.
- `/update/*` and the self-update manager plus the `stream-server-updater` helper binary — desktop baggage.
- `/{ipc_key}/downloader/*` — stubs for an HTTP downloader that was never implemented.
- `/local-addon/*` — the local-files Stremio addon (scanned `localFiles/` directory, catalogs, `bt:`/`local:` metas). **Three stub routes remain** (see the table above), because stremio-core's `OFFICIAL_ADDONS` carries a *protected* descriptor for `http://127.0.0.1:11470/local-addon/manifest.json` with a `stream` resource for `tt` movies/series: a stock profile requests `/local-addon/stream/{type}/{id}.json` on every details page, and a `404` there shows up as an error group in the client and an ERROR-level unhandled-request log line each time. The stub answers an empty manifest and `{"streams": []}` instead and serves no local files; `meta` (only ever asked for `local:`/`bt:` ids) is a quiet `404`.
- `/casting/transcode`, `/casting/convert`, `GET /casting/{devID}` and the `501` stubs for `/ftp/create*` and `/ftp/stream*`.

**YouTube**: stremio-core builds `/yt/{id}` URLs for `StreamSource::YouTube` when a streaming server is configured; this server has no `/yt` route (that needed yt-dlp/ffmpeg upstream). YouTube-via-server is unsupported — the client opens YouTube streams itself (`404` from the server signals it).

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
├── server/           # HTTP server (media + token-protected control routers), embeddable library
│   ├── src/auth.rs   # ServerAuth + the bearer middleware
│   └── src/archives/ # ZIP/7Z/TAR/NZB (always on) + RAR (default-on "rar" feature), all pure Rust
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       └── librqbit.rs   # The sole torrent backend (pure Rust)
└── stremio-runtime-stub/ # Legacy-compatible launcher shim
```

There is no `bindings/` directory and no vcpkg apparatus: the optional C++ `libtorrent` backend and everything it needed to build (the `libtorrent-sys` FFI crate, `triplets/`, `vcpkg-overlays/`, `vcpkg.json`) have been removed. RAR is handled by the pure-Rust `unrar-rs` crate — a direct `server` dependency behind the default-on `rar` feature — so there is no separate RAR binding crate either.

---

## Upgrade notes

- Offline downloads add files next to the session state in `<cache dir>/rqbit-downloads`: `pinned-downloads.json` (the pin set) and one `<infoHash>.bitv` per torrent (fastresume bitfields, new — the first start after upgrading still hash-checks each torrent once, after which restarts skip it). The cache cleaner leaves all of them alone. Nothing moves on upgrade: `settings.downloadsDir` defaults to `null`, which keeps every torrent in the cache root as before, and setting it only affects torrents pinned from then on (an already-pinned torrent relocates on its next pin).
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

`pure rust torrent streaming` `headless torrent server` `librqbit` `rust torrent` `video streaming server` `http range streaming` `torrent to http` `archive streaming` `enginefs` `no ffmpeg`
