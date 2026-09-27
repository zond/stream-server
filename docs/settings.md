# Settings

What `GET`/`POST /settings` and `ServerHandle::settings`/`update_settings` read and write. Both take the same keys and go through one function, and the result is saved to `settings.json` in the config directory.

## The keys

`GET /settings` answers `{ baseUrl, options: [], values }`; `POST /settings` takes any subset of the keys below, merges it, applies it and saves `settings.json` in the config dir, answering `{ success, btSettings }`. A value of the wrong type leaves that setting as it was, and a key the server does not know is ignored; `cacheRoot` is the one validated key, and an unusable one fails the whole update.

| Key | Default | Effect |
|---|---|---|
| `cacheRoot` | the configured cache directory | The one torrent-data root. A change applies at the next start -- see [Offline downloads](storage.md#offline-downloads) |
| `cacheSize` | `10737418240` (10 GiB) | Bytes the cache may hold; `null` is unlimited, and `0` is "no caching" -- a cap of zero, the tightest there is, never read as unlimited (a pin and what a live stream is inside are still kept, as under any cap). The cap actually enforced is the smaller of this and what the volume can give -- see [What bounds the cache](storage.md#what-bounds-the-cache) |
| `bufferProfile` | `"normal"` | See [Buffer profiles](#buffer-profiles) |
| `seedingEnabled` | `true` | Sharing. `false` chokes every upload while no player is reading from this server, and lets uploads run while one is (a paused player still holds its response open, so it counts as reading). It is a session-wide upload switch: downloads are not affected, no torrent is paused and no peer dropped. See [Background activity](stats.md#background-activity) |
| `lanMediaEnabled` | `false` | Whether the [LAN media listener](lan-media.md) may run at all; setting it to `false` also stops a running one |
| `diagnosticsTrace` | `false` | Whether the retention trace is in the log: what each retention pass decided and why (`enginefs::retention::trace`). Applied to the running process's log filter at once, and at every start |
| `dhtBootstrapNodes` | `null` (built-in list) | DHT bootstrap `host:port` entries; read when the session opens, so a change applies at the next start. See [BitTorrent settings](#bittorrent-settings) |
| `bt*` (27 keys) | see [BitTorrent settings](#bittorrent-settings) | Torrent session knobs; the response's `btSettings` says which applied live, which wait for a restart and which librqbit has no knob for |
| `trackersSourceUrl`, `cachedTrackers`, `trackersLastUpdated` | [ngosang `trackers_best.txt`](https://github.com/ngosang/trackerslist), `[]`, `0` | The public tracker list: fetched from `trackersSourceUrl` when the cached one is more than a day old (checked hourly), the 20 fastest by RTT cached in `cachedTrackers`, and added to every torrent the server adds. `POST /settings` does not change these; they live in `settings.json` |
| `proxyStreamsEnabled`, `remoteHttps` | `false`, `null` | Accepted and persisted for stremio-core's settings shape; nothing in the server reads them |
| `appPath`, `serverVersion` | the executable's path, the crate version | Reported, not settable |

## Buffer profiles

How far ahead playback reads is a choice, not a constant. A spotty connection -- or a receiver whose own buffer is shallower than mpv's -- wants more of the file fetched before it is needed; a fast link on a metered phone wants less. The choice is one of three profiles, and it is offered twice:

- **`settings.bufferProfile`** (`GET`/`POST /settings`, `ServerHandle::settings`/`update_settings`) -- the default for every stream request that does not say otherwise. `"normal"` unless set.
- **`?buffer=` on the stream route** -- `GET`/`HEAD /{infoHash}/{fileIdx}` and its `/stream/…` alias, alongside the existing `tr=`, `f=` and `download=`. It overrides the setting for that request only, so a client can keep a global preference and still change the buffer for one playback.

| Profile | Seconds of film held and read ahead | Before a duration is stated |
|---|---|---|
| `normal` (default) | 90 s (`BufferProfile::window_seconds`) | 32 MiB |
| `large` | 4 min | 32 MiB |
| `maximum` | a day (`MAXIMUM_WINDOW_SECONDS`) -- in effect the whole file, as far as the cache budget reaches | 32 MiB |

**The unit is seconds of film, not bytes of disk.** How far a playing stream reads ahead is the film's own bitrate -- its size over the duration a player stated (`ServerHandle::note_duration`) -- times the profile's seconds, and the same number sizes the retention window that keeps those bytes, so what the swarm is asked for and what the disk is kept for are one answer with one source. Sizing it as a fraction of `cacheSize` instead would tie mobile-data spend to a number picked for unrelated reasons, so a viewer who gave the app a bigger cache would silently buy a bigger data bill.

That number is librqbit's per-stream lookahead (`FileStreamOptions::lookahead_bytes`). A reader is opened with the smaller of it, how far the retention window reaches ahead of the reader (`Engine::fetch_bound`) and what the whole cache may hold, so a stream never asks the swarm for a piece the next retention pass would reclaim. It is a byte budget the engine tries to have on disk ahead of the read head, not a promise: a swarm that cannot fill it simply does not.

**Before a duration has been stated there is nothing to convert**, which is the first open of a session and any file a player can put no length on. A fixed 32 MiB stands in there (`priorities::STREAMING_LOOKAHEAD_BYTES`, eight of the field's 4 MiB pieces, about nine seconds of film), and the profile deliberately does not scale it: multiplying a number that exists because nothing is known yet would be scaling a guess. An offline download, which no player ever states a duration for, reads ahead at 256 MiB (`DOWNLOAD_LOOKAHEAD_BYTES`) for its whole life -- sized to keep the swarm busy rather than to sit ahead of a playhead that does not exist.

**The startup window is the same under every profile, deliberately.** It is that same 32 MiB: the narrow first-frame want-set is what makes playback start quickly, and widening it would spend that latency to buy read-ahead the very next request -- by which time a duration has usually been stated -- already asks for. Choosing a bigger profile never slows a play down; it changes what happens after the first frame. (`maximum` converts the same way, from a day of film, which for any film is the whole file: what bounds it is the cache budget.)

**What it costs.** A larger window downloads further ahead of what is being watched, which means more of the file on disk at once and more **bandwidth** spent on bytes the viewer may seek past or never reach -- worth saying out loud on mobile data. `maximum` asks for the whole film where the cache budget covers it. Where the budget is smaller than the file, the retention window caps the read-ahead, so a bigger profile buys nothing past the window there. If the connection is bad enough that even `maximum` stutters, the honest answer is not a bigger window but an offline download: pin the file (`ServerHandle::pin_download`, see [Offline downloads](storage.md#offline-downloads)) and watch it once it is there.

**Validation is lenient by design.** The value is matched case-insensitively with surrounding whitespace ignored. Anything else -- a profile a future build added, a typo, an empty value -- is *not* an error: on `?buffer=` it falls back to `settings.bufferProfile`, and on `POST /settings` it leaves the setting as it was, like every other unrecognised value in that payload. A player must never lose a playback because it guessed a name wrong. The wire is additive throughout: a client that sends neither gets exactly today's behaviour.

Only the torrent stream route reads the profile: archive members and offline downloads are not affected.

## BitTorrent settings

The `bt*` names and their meanings are `libtorrent`'s; `librqbit` is the only backend. Every key is accepted, echoed back and persisted, but not every one reaches librqbit, and which is which is a fixed, per-key fact: the `enginefs::backend::bt_settings_support()` truth table (a test keeps it complete) says whether a key is `Live` (applied to the running session: `btDownloadSpeedHardLimit`, `btMaxConnections`), `NextStart` (read once when the session opens: `btEnableDht`, `btEnableLsd`, `btOutgoingInterfaces`, the SOCKS5 proxy keys, `dhtBootstrapNodes`) or `NotHonoured` (no librqbit knob, with the reason in the row). An update's answer carries a `btSettings` report -- `appliedLive`, `pendingRestart`, `notHonoured` -- so a client never has to guess whether a setting took effect. Existing `settings.json` files are read with defaults for missing keys.

Over HTTP the update is a control route, so it needs the bearer token:

```bash
curl -X POST "$BASE/settings" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"btDownloadSpeedHardLimit":5000000,"btMaxConnections":400}'
```

### Privacy and Peer Discovery

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btEnableDht` | boolean | `true` | DHT peer discovery. The DHT is created with the session, so `false` means no DHT at all, and a change takes effect on the **next server start**. |
| `btEnablePex` | boolean | `true` | Not honoured: librqbit has no PeX switch (`ut_pex` is always on for public torrents), so neither value changes anything. |
| `btEnableLsd` | boolean | `true` | Local Service Discovery multicast on the LAN, on or off for the whole session; takes effect on the **next server start**. |
| `btEncryptionMode` | string or number | `"allow"` | Not honoured: librqbit speaks plain BitTorrent only (no MSE/PE), so `"require"` cannot be met and `"disable"` is what always stands. Accepts `"allow"`/`0`, `"require"`/`1`, or `"disable"`/`2`. |
| `btAnonymousMode` | boolean | `false` | Not honoured: no equivalent; the client name and peer id are librqbit's own. |
| `btAllowMultipleConnectionsPerIp` | boolean | `false` | Not honoured: librqbit has no per-IP connection rule. |
| `btValidateHttpsTrackers` | boolean | `true` | Not honoured: HTTPS tracker certificates are always validated, and that cannot be turned off. |
| `btSsrfMitigation` | boolean | `true` | Not honoured: no equivalent. |
| `dhtBootstrapNodes` | array of strings, or `null` | `null` (the built-in list) | DHT bootstrap nodes (`"host:port"`) used to seed the routing table on a cold start. A non-empty list *replaces* the default entirely; `null` or `[]` uses it. Invalid entries (no `host:port` split, or an unparseable/zero port) are dropped with a warning instead of failing the request. Like `btEnableDht` and `btEnableLsd`, this is read once when librqbit's session opens, so a change here takes effect on the **next server start**, not the running session. |

The built-in bootstrap list, why it is those two hosts, and how the server resolves the names before librqbit sees them (so an address literal configured here skips DNS altogether) are under [DHT health](stats.md#dht-health-serverhandledht_status). Once a session has run, librqbit persists its routing table to `dht.json` and loads it on every later start, so `dhtBootstrapNodes` normally matters only on a first run or after that file is lost.

### Speed and Connections

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btDownloadSpeedHardLimit` | number | `0` | Session-wide download limit in bytes per second; `0` is unlimited. Applied to the **running** session. |
| `btMaxConnections` | number | `160` | Peer budget. librqbit takes a *per-torrent* live cap from it (`/4`, clamped to 40-200), so the default lands on 40 peers per torrent -- the figure a 2 GB television wants, a peer costing about 75 KiB. Applied to every torrent at once, live; while the embedding app is in the background the lean cap stands instead, and this is what a return to the foreground restores. |
| `btOutgoingInterfaces` | string | `""` | One interface **name** for outgoing traffic, bound with `SO_BINDTODEVICE`. An address, or a list, is not applied (a warning says so), and a name the OS rejects starts the session unbound. Read when the session opens, so it takes effect on the **next server start**. |
| `btDownloadSpeedSoftLimit` | number | `0` | Accepted and persisted, never applied: librqbit has one download limit, not a soft and a hard one. |
| `btHandshakeTimeout` | number | `20000` | Accepted and persisted, never applied: librqbit has a connect timeout (10 s) and a read/write timeout (30 s), and neither is a handshake timeout. |
| `btRequestTimeout` | number | `10000` | Accepted and persisted, never applied: librqbit's request timing is its own. |
| `btMinPeersForStable` | number | `5` | Accepted and persisted, never applied: nothing reads it, and the stats echo librqbit's own peer-search figures. |

**There is no listen-port setting.** `btListenInterfaces`, `btOutgoingPort`
and `btNumOutgoingPorts` are accepted, echoed back and persisted like every
other key, and none of them reaches librqbit: the incoming listener is the
launch configuration's `TorrentListenPort` -- `Ephemeral` by default, so the
OS picks the port, or `Fixed(42000..42010)` for an embedder that wants one it
can forward (the only case UPnP is asked for). Outgoing ports are the OS's
to pick; librqbit has no knob for a fixed range.

### Tracker and Peer Proxy

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btProxyType` | string or number | `"none"` | Proxy type; takes effect on the **next server start**. Accepts `"none"`/`0`, `"socks4"`/`1`, `"socks5"`/`2`, `"socks5Password"`/`3`, `"http"`/`4`, or `"httpPassword"`/`5`, but only `socks5` and `socks5Password` are applied: librqbit has no SOCKS4 or HTTP proxy, and those values run the session unproxied. |
| `btProxyHost` | string | `""` | With `btProxyType` and `btProxyPort`, the one SOCKS5 proxy peer connections and HTTP(S) tracker requests go through; next server start. |
| `btProxyPort` | number | `0` | See `btProxyHost`. |
| `btProxyUsername` | string | `""` | Sent for `socks5Password` only; next server start. |
| `btProxyPassword` | string | `""` | Sent for `socks5Password` only; next server start. Never logged. |
| `btProxyHostnames` | boolean | `true` | Not honoured: peers are addresses, and tracker hostnames are resolved locally (SOCKS5, not SOCKS5h). |
| `btProxyPeerConnections` | boolean | `false` | Not honoured: peer connections always go through a configured proxy. |
| `btProxyTrackerConnections` | boolean | `true` | Not honoured: HTTP(S) tracker requests always go through a configured proxy; UDP trackers and the DHT never can. |
| `btProxySendHostInConnect` | boolean | `false` | Not honoured: an HTTP `CONNECT` option, and librqbit has no HTTP proxy. |




