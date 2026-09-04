# BitTorrent Settings

Stream Server exposes BitTorrent privacy and network controls through the
existing `/settings` API and persists them in `settings.json`. The setting
names and semantics were originally modeled on a native `libtorrent` backend
this fork no longer has: `librqbit` is the sole torrent backend today, is
always built (there is no backend feature flag), and honors these settings
on a best-effort basis.

The setting names mirror the JSON keys returned by:

```bash
curl http://127.0.0.1:11470/settings
```

Update only the keys you want to change:

```bash
curl -X POST http://127.0.0.1:11470/settings \
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

## Interface And Port Binding

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `btListenInterfaces` | string | `"0.0.0.0:42000-42010,[::]:42000-42010"` | Incoming BitTorrent listen interfaces and ports, as `host:start-end` pairs. |
| `btOutgoingInterfaces` | string | `""` | Network interface names or IPs used for outgoing BitTorrent traffic. Empty means system default routing. |
| `btOutgoingPort` | number | `0` | First local outgoing TCP port. `0` lets the OS choose. |
| `btNumOutgoingPorts` | number | `0` | Number of outgoing ports starting at `btOutgoingPort`. `0` means no fixed outgoing range. |

Bind incoming traffic to a specific IP and port range:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Content-Type: application/json" \
  -d '{
    "btListenInterfaces": "192.168.1.25:42000-42010",
    "btOutgoingInterfaces": "192.168.1.25"
  }'
```

Bind to a VPN interface by name:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Content-Type: application/json" \
  -d '{
    "btListenInterfaces": "tun0:42000-42010",
    "btOutgoingInterfaces": "tun0"
  }'
```

Use fixed outgoing ports:

```bash
curl -X POST http://127.0.0.1:11470/settings \
  -H "Content-Type: application/json" \
  -d '{
    "btOutgoingPort": 42100,
    "btNumOutgoingPorts": 20
  }'
```

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
- `btEnablePex` can enable PeX dynamically, but disabling PeX for an already
  running session may require a restart to take full effect.
- `btListenInterfaces`, `btOutgoingInterfaces`, proxy options, and SSRF/TLS
  settings are applied by the `librqbit` backend on a best-effort basis; not
  every knob has an equivalent in every backend.
- `dhtBootstrapNodes` is the one setting on this page that is not applied to
  the running session at all -- it is read once, at session construction, so
  a change takes effect on the next server start.
