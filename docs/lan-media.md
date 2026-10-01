# LAN media listener

The second listener a cast session turns on so a receiver on the local network can fetch what the app published for it -- by token, and nothing else.

A cast receiver is not on loopback. `ServerConfig::default()` binds
`127.0.0.1` only, so a Chromecast cannot fetch a byte from it -- casting is
blocked before the media is even prepared. Widening that bind is not the fix:
it would put `/settings`, the stats route and `/create` on the local
network behind nothing but a bearer token.

So there is a **second listener** instead, and it serves **published cast
tokens and nothing else**.

| | |
|---|---|
| **What it exposes** | `GET`/`HEAD` `/cast/{token}` ([`server/src/cast.rs`](../server/src/cast.rs)), and for a rendition its HLS stream under `/cast/{token}/hls/` ([Renditions](#renditions)). The app publishes a media id for a cast (`ServerHandle::publish(&MediaId, Option<PlayToken>) -> CastToken`) and hands the receiver `<lan_media_base_url>/cast/<token>`; the route serves what the id resolves to -- a torrent file, a link through `/proxy`'s cache, a Google Drive file, a finished download, a member of an archive -- with the range framing every media route shares (`200`/`206`/`416`, `Content-Range`, `HEAD`, the DLNA headers). An unknown token is a `404`. A link whose origin will not serve ranges is refused (`501`, `{"refused":"noRanges"}`): nothing here can seek it for a receiver |
| **What it does not** | Every other path is a `404`, every method: the control router is **not mounted at all** (a control path answers `404`, never the `401` that would confirm the route exists and only a bearer token is missing), and neither is any loopback media route -- not the torrent routes, the archive routes, `/proxy`, `/ftp`, `/drive/stream`, `/downloads/{key}/stream` or `/local-addon` |
| **Where it binds** | `ServerConfig::lan_media_addr: Option<SocketAddr>` -- `None` by default, so nothing changes unless an embedder asks for it. `Some(0.0.0.0:0)` lets the OS pick the port |
| **When it runs** | `ServerHandle::set_lan_media(true)` starts it, `set_lan_media(false)` stops it -- meant to bracket a cast session, so the LAN surface exists only while something is casting. Nothing is bound at startup, whatever the configuration: a port already in use fails the cast that asked for the listener, never the server |
| **How it is switched off entirely** | The `lanMediaEnabled` setting (`POST /settings`, **`false` by default**). While it is false, `set_lan_media(true)` is refused; setting it back to false also stops a listener that is already running, which unpublishes every token |

## Tokens

A cast token is **128 random bits, hex, never derived from the id**: a
receiver that saw one learns nothing about another, or about the id. Tokens
live in memory only.

* **Publishing needs the listener running** (`set_lan_media(true)` first),
  and an id this server holds (`register` issued it and it has not been let
  go). The publication **holds the id** as an open reader does, so the id
  is not evicted while it is cast.
* **With a play token**, the receiver's reads are the viewer's playback --
  the same rules `open_reader` applies: the play session moves to the file,
  and it shares as the app's own player's would. Each `GET` opens the id's
  source as a reader does; a torrent's stream registers at the open and
  ends when the body is dropped. Without a play token the reads are an
  aside, which moves no session and shares nothing.
* **`unpublish(&CastToken)` cuts the bytes.** Nothing more is served under
  the token (`404`), and every body being served under it ends at once:
  each cast body polls its token's cut before every chunk, so even a body
  parked on a piece nobody has is woken, and it ends with an **error** --
  the connection is dropped, and the receiver reads a broken source rather
  than a file that ended early.
* **Stopping the listener unpublishes every token** -- `set_lan_media(false)`,
  the `lanMediaEnabled` veto revoked, the server's own stop -- so it stops
  the bytes too, not only new fetches.
* **A token is never logged.** `/cast/<token>` is written as `/cast` in
  every request line and span (`routes::util::log_path`, beside `/proxy`
  and `/ftp`), and `CastToken`'s `Debug` prints no token: a token in a log
  file is a URL into this device for as long as it is published.

## Renditions

A cast the receiver cannot decode as it is -- an MKV, surround sound over
Bluetooth, HEVC to a receiver without it -- is cast as a **rendition**: one
progressive fragmented MP4 produced on demand by the embedder's producer
and muxed here, nothing on disk ([design/renditions.md](design/renditions.md)).
`ServerHandle::publish_rendition(&MediaId, RenditionSpec, Option<PlayToken>)`
publishes one under a token with every rule above (random, memory only, a
lease on the id, cut by `unpublish` and the listener's stop, never logged);
it is refused with `noProducer` until the embedder has called
`install_producer`. The receiver is handed
`<lan_media_base_url>/cast/<token>/stream.mp4`.

| Path | Answers |
|---|---|
| `GET /cast/{token}/stream.mp4[?from=<ms>]` | `200`, `video/mp4`, `Cache-Control: no-store`, **no `Content-Length` and no `Accept-Ranges`**: the init segment (`ftyp` + `moov`), then segment after segment (`styp` + `moof` + `mdat`), from the one `from` falls in (the spec's `startMs` without it) to the film's end, each sent as it is made, the next asked for as the receiver takes the last. A `Range` header is not read. Waits for the first segment and the init segment before it answers, so a rendition that cannot start is `503` `{"refused":"renditionFailed","message":...}` (or `{"refused":"unpublished"}` for one the unpublish woke); after that, a failure or the cut breaks the body with an error, and only the film's end is a clean end. Nothing times it out |
| `HEAD /cast/{token}/stream.mp4` | The same headers, at once; starts nothing |

A plain token has no `stream.mp4` (`404`); a rendition's token serves it
and, like a plain one, the source as it is at `/cast/{token}`. A stream's
`GET` counts as one body (`lan_media_bodies_served`). **A seek is a new
stream** from another `from`: the receiver plays a progressive file it
cannot seek by bytes, and the stream's timestamps are the film's, so
`currentTime` reads the film's position from wherever it starts. Segment
`n` (a unit inside the stream, `T` long) is cut at the first video sync
sample at or after `n x T` and holds the audio whose
time falls in `[n x T, (n+1) x T)`. Production runs at most two segments
past the one the stream last asked for and then waits; a stream from a
time far from where the run is starts a new run there. A run nobody has asked anything of for
a minute is let go (the segments made are kept). A run that makes less than
its own time in film over ten seconds of its own work -- leaving out the
time it waited for the receiver and for the source -- fails the rendition
with a sentence, which `ServerHandle::rendition_state(&CastToken)` reports
(`{"state":"failed","sentence":...}`; otherwise `producing`, `idle`, or
`ended` for a token not published).

`ServerHandle::lan_media_base_url(for_peer)` builds the URL to hand a receiver:
the host is the local interface that shares `for_peer`'s subnet, taken from the
same interface enumeration `GET /network-info` answers from -- on a host with a
VPN or a container bridge the first interface in the list is regularly one the
receiver cannot route back to. A listener bound to one specific address
reports that address as is. It is `None` whenever the listener is not running,
which is also the signal that no cast URL can be built yet.

**When no interface matches**, because the receiver is behind a router or
because the caller has no receiver address to give (not every platform reports
one), the candidates are *ranked* rather than taken in enumeration order: an
ordinary interface before one no receiver on a home network can be behind --
carrier links (`rmnet`, `ccmni`, `pdp_ip`), tunnels (`tun`, `utun`, `tap`,
`wg`), container and VM bridges (`docker`, `br-`, `veth`, `virbr`, `vboxnet`),
the interfaces this device hands out rather than reaches a LAN through (`ap0`,
`p2p`, `rndis`) and `dummy`, all matched by name -- and, within each, an RFC1918
address before anything else. The bridges are why the address shape cannot
decide this on its own: `docker0`'s `172.17.0.1` is as private as the Wi-Fi
address beside it, and only the name tells them apart. An interface the kernel
reported no netmask for is matched against no subnet at all, since a `0.0.0.0`
mask matches every peer and would win outright over the interface that really
shares one. A phone is on Wi-Fi and cellular at once and
`getifaddrs` will happily list the cellular interface first; naming that
address to a Chromecast is a cast that hangs forever, because a TCP connect to
an unroutable host does not fail, it waits. The demotion is only ever a
tie-break: a matching subnet still wins outright, and a host whose one
routable address is cellular is still offered it rather than nothing.

**A cast that never starts leaves no other trace**, which is why this
listener reports on itself. Every answer `lan_media_base_url` gives is logged
at INFO -- the peer, the interface picked and the URL -- as is each of the ways
it can answer `None`, and so is every request that reaches the listener
(method, path with the token elided, peer). Two counts read the same
session from the outside, both reset by every start (whether or not a
listener was already running, since starting a cast to a second receiver
mid-session asks about that cast and not the one before it) and by every
stop, and never carried over:

* `ServerHandle::lan_media_requests_served()` -- every request the listener
  receives, a `404` included: the receiver got here.
* `ServerHandle::lan_media_bodies_served()` -- every `/cast` `GET` that began
  sending bytes; never a `HEAD`, an unknown token's `404` or a refusal.

A receiver told an address it cannot route to reports no error at all,
because a TCP connect to an unroutable host hangs rather than failing; from
the sofa that is indistinguishable from buffering. So the app's check, some
seconds after the receiver was told to load, has **three readings**:

| Requests | Bodies | Reading | What to do |
|---|---|---|---|
| 0 | 0 | Nothing reached this device: the address is wrong (another interface, a client-isolated Wi-Fi, a firewall). | End the session and say the receiver cannot reach this device. |
| > 0 | 0 | The receiver reached this device and was served nothing: a token it was not given, or an id that would not open (the refusal is in the log). | End the session and say so -- the network is fine. |
| > 0 | > 0 | The network and the server did their part. | Leave it to the media. |

The addresses in those log lines are private ones on the user's own LAN;
the bearer token is never on this listener, and a cast token is never in a
log line.

**Stopping closes the door and stops the bytes.**
`set_lan_media(false)` unpublishes every token -- which cuts every cast body
in flight -- then aborts the serving task and awaits it, so by the time the
call returns the listener socket is closed and the port is free (it rebinds
immediately), nothing new is accepted, and a connection idling on
keep-alive is closed without serving another request. The call is not a
drain: it returns at once rather than waiting for a receiver, so ending a
cast session or revoking `lanMediaEnabled` never blocks. The loopback
listener owns a different socket and a different serve future; it and every
request in flight on it are untouched.

**The trade-off, stated plainly.** While a token is published, *anyone* on
the same network who learns the URL can fetch that one file: a receiver
cannot attach a header, so there is no authentication on that port beyond
the token itself. What they cannot do is anything else -- guess another
token, walk a Drive account or the piece cache by info hash, or make this
device fetch, add or open anything: there is no route on this listener that
names a torrent, a URL or a file, so a stranger can only read what the app
chose to cast, for as long as it chose to. That is why the listener is off
by default, why it is meant to be held open only for the length of a cast
session, and why `lanMediaEnabled` exists as an operator veto that no
embedder call can override. (A host that binds the *main* listener to
`0.0.0.0` exposes its routes anyway -- see the
[README](../README.md#quick-start).)

CORS is set up for what a receiver needs (`lan_cors_layer` in
`server/src/lib.rs`): any origin and method; `Accept`, `Accept-Encoding`,
`Content-Type` and `Range` named as allowed request headers (Google's Web
Receiver CORS requirements ask for the last three, and even a plain MP4 needs
CORS once tracks are involved); and `Accept-Ranges`, `Content-Disposition`,
`Content-Encoding`, `Content-Length`, `Content-Range` and `Content-Type`
exposed to script, so a player can seek. A preflight is cached for a day. This is the one
listener that answers CORS: the receiver is a browser media element, and the
loopback listener's readers are not. Byte-range
requests and `HEAD` work on this listener exactly as they do on loopback.
