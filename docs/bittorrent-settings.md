# BitTorrent Settings

Stream Server exposes BitTorrent privacy and network controls through the
existing `/settings` API and persists them in `settings.json`. The setting
names and semantics were originally modeled on a native `libtorrent` backend
this fork no longer has: `librqbit` is the sole torrent backend today and is
always built (there is no backend feature flag). Every setting is accepted,
echoed back and persisted, but not every one reaches librqbit -- some have no
equivalent knob and some are read only when the session opens. This is not
"best-effort": which is which is a fixed, per-setting fact, listed in the
`enginefs::backend::bt_settings_support()` truth table (a test keeps that
table complete) and summarised in the tables below. A `POST /settings`
response also carries a `btSettings` report of what your specific update did
-- `appliedLive`, `pendingRestart`, `notHonoured` -- so a client never has to
guess whether a setting took effect.

`/settings` is a **control route**, so every request below needs the
per-launch bearer token or it answers `401` and changes nothing. The token is
generated at start-up and written down nowhere: the embedder reads it from
`ServerHandle::auth_token` (there is no binary, and nothing prints it), so the
examples assume it is in `$TOKEN`. The port is the embedder's too --
`ServerConfig::http_addr` defaults to `127.0.0.1:11470`, and an embedder that
asks for port 0, as xtremio does, gets whatever the OS gave it
(`ServerHandle::base_url`).

The setting names mirror the JSON keys returned by:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:11470/settings
```

Update only the keys you want to change:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"btEnableDht":false,"btEncryptionMode":"require"}'
```

The server saves accepted changes back to `settings.json` in the server config
directory. On desktop builds this is usually the OS config directory plus
`stremio-server`, for example `%APPDATA%\stremio-server\settings.json` on
Windows or `~/.config/stremio-server/settings.json` on Linux.

## Privacy And Peer Discovery

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btEnableDht` | boolean | `true` | Enables DHT peer discovery. Disable to avoid announcing through the decentralized DHT network. |
| `btEnablePex` | boolean | `true` | Enables Peer Exchange. Disable to avoid learning and sharing peers through connected peers. |
| `btEnableLsd` | boolean | `true` | Enables Local Service Discovery on the LAN. Disable to avoid local-network peer discovery. |
| `btEncryptionMode` | string or number | `"allow"` | Encryption policy. Accepts `"allow"`/`0`, `"require"`/`1`, or `"disable"`/`2`. |
| `btAnonymousMode` | boolean | `false` | Enables anonymous mode, which reduces identifying client metadata where supported. |
| `btAllowMultipleConnectionsPerIp` | boolean | `false` | Allows more than one peer connection per IP address. Keep disabled unless you explicitly need it. |
| `btValidateHttpsTrackers` | boolean | `true` | Validates HTTPS tracker certificates. Disabling this weakens tracker TLS checks. |
| `btSsrfMitigation` | boolean | `true` | Keeps SSRF mitigations enabled for tracker and web seed access. |
| `dhtBootstrapNodes` | array of strings, or `null` | `null` (uses the built-in list below) | DHT bootstrap nodes (`"host:port"`) used to seed the routing table on a cold start. A non-empty list *replaces* the default entirely; `null` or `[]` uses it. Invalid entries (no `host:port` split, or an unparseable/zero port) are dropped with a warning instead of failing the request. Unlike the other `bt*` settings above, this one is read once when librqbit's session opens, so a change here takes effect on the **next server start**, not the running session. |

The built-in default is `dht.libtorrent.org:25401`,
`dht.transmissionbt.com:6881` — librqbit's own default list, fastest first.

They are the only two of the conventional public bootstrap names that were
measured to actually answer a mainline DHT `ping` (3/3 attempts each, ~11 ms
and ~31 ms). An earlier revision also shipped `router.utorrent.com:6881` and
`dht.aelitis.com:6881`; both resolve but answered 0/3, so they were retry
noise rather than resilience and were removed. `router.bittorrent.com:6881`
was kept a while longer on reputation — it is the most widely deployed
bootstrap name in the ecosystem — but a 2026-09 re-probe from two networks,
twice each, with both `ping` and `find_node`, had it resolving to
`67.215.246.10` and answering nothing on either, so it went the same way.
**Do not add a host here without pinging it first.**

Names in this list — the default *and* a configured one — are resolved by the
server before librqbit sees them: the system resolver first, then DNS over
HTTPS (`dns.google`, then `cloudflare-dns.com`) if the system resolver returns
no address, then a `dht-bootstrap.json` cache kept next to the routing table.
Anything still unresolved is handed to librqbit as a name so its own retries
can still succeed. Address literals you configure here are passed through
untouched, with no DNS at all — which is the useful thing to configure if you
already know the addresses because DNS on your network does not work. **This
fixes DNS only: if the network drops the DHT's UDP outright, correct
addresses do not help.**

Once a session has run once, librqbit persists its routing
table to `dht.json` next to the downloads; on every later start it loads
that table *and* still queries the bootstrap nodes in the background, but
with a warm table already available the bootstrap hosts' reachability
matters far less — in practice `dhtBootstrapNodes` normally only matters on
first run, or after that persisted table is lost.

Privacy-focused example:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "btEnableDht": false,
    "btEnablePex": false,
    "btEnableLsd": false,
    "btEncryptionMode": "require",
    "btAnonymousMode": true,
    "btAllowMultipleConnectionsPerIp": false,
    "btValidateHttpsTrackers": true,
    "btSsrfMitigation": true
  }'
```

## Speed And Connections

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btDownloadSpeedHardLimit` | number | `0` | Session-wide download limit in bytes per second; `0` is unlimited. The one setting that applies to the **running** session. |
| `btMaxConnections` | number | `160` | Peer budget. librqbit takes a *per-torrent* live cap from it (`/4`, clamped to 40-200), so the default lands on 40 peers per torrent -- the figure a 2 GB television wants, a peer costing about 75 KiB. Applied to every torrent at once, live; while the embedding app is in the background the lean cap stands instead, and this is what a return to the foreground restores. |
| `btOutgoingInterfaces` | string | `""` | One interface **name** for outgoing traffic, bound with `SO_BINDTODEVICE`. An address, or a list, is not applied (a warning says so), and a name the OS rejects starts the session unbound. Read when the session opens, so it takes effect on the **next server start**. |
| `btDownloadSpeedSoftLimit` | number | `0` | Accepted and persisted, never applied: librqbit has one download limit, not a soft and a hard one. |
| `btHandshakeTimeout` | number | `20000` | Accepted and persisted, never applied: librqbit has a connect timeout (10 s) and a read/write timeout (30 s), and neither is a handshake timeout. |
| `btRequestTimeout` | number | `10000` | Accepted and persisted, never applied: librqbit's request timing is its own. |
| `btMinPeersForStable` | number | `5` | Accepted and persisted, never applied: nothing reads it, and the stats echo librqbit's own peer-search figures. |

Cap the download rate and widen the peer budget, both without a restart:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "btDownloadSpeedHardLimit": 5000000,
    "btMaxConnections": 400
  }'
```

**There is no listen-port setting.** `btListenInterfaces`, `btOutgoingPort`
and `btNumOutgoingPorts` are accepted, echoed back and persisted like every
other key, and none of them reaches librqbit: the incoming listener is the
launch configuration's `TorrentListenPort`, which is `Ephemeral` for an
embedded server -- the only way this library runs -- so the OS picks the port
and nothing needs forwarding or a firewall rule. Outgoing ports are the OS's
to pick; librqbit has no knob for a fixed range.

## Tracker And Peer Proxy

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btProxyType` | string or number | `"none"` | Proxy type. Accepts `"none"`/`0`, `"socks4"`/`1`, `"socks5"`/`2`, `"socks5Password"`/`3`, `"http"`/`4`, or `"httpPassword"`/`5`. |
| `btProxyHost` | string | `""` | Proxy host or IP address. |
| `btProxyPort` | number | `0` | Proxy port. |
| `btProxyUsername` | string | `""` | Proxy username for authenticated proxy types. |
| `btProxyPassword` | string | `""` | Proxy password for authenticated proxy types. |
| `btProxyHostnames` | boolean | `true` | Resolves hostnames through the proxy where supported. |
| `btProxyPeerConnections` | boolean | `false` | Routes peer connections through the proxy. |
| `btProxyTrackerConnections` | boolean | `true` | Routes tracker connections through the proxy. |
| `btProxySendHostInConnect` | boolean | `false` | Sends the hostname in HTTP `CONNECT` requests where supported. |

Proxy only tracker traffic:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "btProxyType": "socks5",
    "btProxyHost": "127.0.0.1",
    "btProxyPort": 1080,
    "btProxyHostnames": true,
    "btProxyTrackerConnections": true,
    "btProxyPeerConnections": false
  }'
```

Proxy tracker and peer traffic through an authenticated SOCKS5 proxy:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "btProxyType": "socks5Password",
    "btProxyHost": "127.0.0.1",
    "btProxyPort": 1080,
    "btProxyUsername": "user",
    "btProxyPassword": "password",
    "btProxyHostnames": true,
    "btProxyTrackerConnections": true,
    "btProxyPeerConnections": true
  }'
```

Disable the proxy:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "btProxyType": "none",
    "btProxyHost": "",
    "btProxyPort": 0,
    "btProxyUsername": "",
    "btProxyPassword": ""
  }'
```

## Notes

- Existing `settings.json` files are read with defaults for missing keys. The new
  keys may not appear on disk until `/settings` is saved.
- What each `bt*` setting actually does against `librqbit` is fixed per setting,
  not best-effort. The authority is the `enginefs::backend::bt_settings_support()`
  truth table, one row per setting: `Live` (applied to the running session now --
  `btDownloadSpeedHardLimit` and `btMaxConnections`), `NextStart` (read once when the session opens,
  so a change takes effect on the next server start -- `btEnableDht`, `btEnableLsd`,
  `btOutgoingInterfaces`, and the SOCKS5 proxy settings), or `NotHonoured` (no
  librqbit knob at all, with the reason in the row -- `btEnablePex`,
  `btEncryptionMode`, `btAnonymousMode`, `btValidateHttpsTrackers`,
  `btSsrfMitigation`, `btListenInterfaces`, the outgoing-port settings, and the
  rest). A `POST /settings`
  response reports which of these buckets each setting you sent fell into, as
  `btSettings.appliedLive` / `pendingRestart` / `notHonoured`.
- `btEnablePex` is `NotHonoured`: librqbit has no PeX switch (`ut_pex` is always on
  for public torrents), so neither enabling nor disabling it changes anything.
- `dhtBootstrapNodes` is not applied to the running session either -- it is read
  once, at session construction, so a change takes effect on the next server start.
