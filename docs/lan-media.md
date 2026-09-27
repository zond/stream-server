# LAN media listener

The second, media-only listener a cast session turns on so a receiver on the local network can fetch what this device already has.

A cast receiver is not on loopback. `ServerConfig::default()` binds
`127.0.0.1` only, so a Chromecast cannot fetch a byte from it -- casting is
blocked before the media is even prepared. Widening that bind is not the fix:
it would put `/settings`, the stats route and `/create` on the local
network behind nothing but a bearer token.

So there is a **second listener** instead, and it serves media routes only.

| | |
|---|---|
| **What it exposes** | An explicit allow-list (`lan_media_routes()` in [`server/src/lib.rs`](../server/src/lib.rs)), not `media_router()` itself: exactly what a cast receiver needs, which is the bytes of something this device already has. `/{infoHash}/{fileIdx}` and `/stream/…` over torrents that exist -- an unknown hash is a `404` and `tr=` is ignored, where on loopback the same request would create the torrent with the caller's trackers -- and the archive `/{fmt}/stream/…` routes over sessions loopback already created. `/proxy`, `/ftp`, every `/create` and the `/local-addon` stub are deliberately absent -- see below |
| **What it does not** | The control router is **not mounted on it at all**, not even behind the bearer middleware. A control path there is a path this listener does not serve, and never the `401` that would confirm the route exists and only a token is missing: `404` for a path nothing matches, and, where a two-segment one (`/{infoHash}/create`) collides with the `/{infoHash}/{fileIdx}` pattern, the `405` that route answers a method it does not take with, or on a `GET`/`HEAD` its own `404` for a hash this server does not hold. There is no token on that listener to guess, leak or brute-force. `/proxy` and `/ftp` are likewise unmounted and answer the same way |
| **Where it binds** | `ServerConfig::lan_media_addr: Option<SocketAddr>` -- `None` by default, so nothing changes unless an embedder asks for it. `Some(0.0.0.0:0)` lets the OS pick the port |
| **When it runs** | `ServerHandle::set_lan_media(true)` starts it, `set_lan_media(false)` stops it -- meant to bracket a cast session, so the LAN surface exists only while something is casting. Nothing is bound at startup, whatever the configuration: a port already in use fails the cast that asked for the listener, never the server |
| **How it is switched off entirely** | The `lanMediaEnabled` setting (`POST /settings`, **`false` by default**). While it is false, `set_lan_media(true)` is refused; setting it back to false also stops a listener that is already running |

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
(method, path, peer). `ServerHandle::lan_media_requests_served()` is the same
arrival count as a number: it starts at zero on every start -- whether or not
a listener was already running, since starting a cast to a second receiver
mid-session asks about that cast and not the one before it -- counts each
request the listener receives (a `404` from the fallbacks included -- the
receiver still got here), never carries over from the previous session, and
is back to zero once the listener has been stopped.

A receiver told an address it cannot route to reports no error at all, because
a TCP connect to an unroutable host hangs rather than failing; from the sofa
that is indistinguishable from buffering. A count still at zero well after a
load is what tells the two apart, and it is worth different words: nothing
reached this device, so the address was wrong -- as opposed to a non-zero count,
where the receiver fetched the stream and the problem is the media. The
addresses in those log lines are private ones on the user's own LAN, and the
one thing that is secret, the bearer token, is never on this listener at all.

**Stopping closes the door, not the connections already through it.**
`set_lan_media(false)` aborts the serving task and awaits it, so by the time
the call returns the listener socket is closed and the port is free (it
rebinds immediately): nothing new is accepted, and a connection idling on
keep-alive is closed without serving another request. A response that is
*already* streaming is **not** cut -- axum spawns each accepted connection into
its own task, and dropping the serve future asks those to shut down
gracefully, which finishes the response in flight -- so a receiver mid-file
keeps being fed until it has the whole thing or hangs up. The call is still
not a drain, which is the point: it returns at once rather than waiting out a
movie-length response, so ending a cast session or revoking `lanMediaEnabled`
never blocks. But it is not a kill switch for bytes already on the wire, and
the server has none; stopping the LAN listener stops new fetches. The loopback
listener owns a different socket and a different serve future; it and every
request in flight on it are untouched.

**The trade-off, stated plainly.** While the listener is up, *anyone* on the
same network can fetch media from this server: the media routes are open by
design (players cannot attach headers), so there is no authentication on that
port at all. Anyone who can guess or observe an info hash can pull that file
out of the piece cache. That is why it is off by default, why it is meant to
be held open only for the length of a cast session, and why `lanMediaEnabled`
exists as an operator veto that no embedder call can override.

**Nothing a stranger could make this device *do* is on it.** The test of a route belonging on the LAN is that it serves bytes the loopback side has already arranged and cannot be made to arrange anything. `/proxy` and `/ftp` fail it outright -- each fetches a caller-named remote URL, which makes it an open proxy for whoever can reach it -- and so do the archive `/create` routes, which fetch an index from a caller-named URL, and the loopback stream route's first request for an info hash, which starts a torrent with the caller's trackers on this device's disk and connection (the LAN's stream route only looks a hash up, `EngineAccess::ExistingOnly`). That is tolerable on loopback -- which is not "only this app": every app on the device reaches it, and so does every page a browser on it has open, which is why loopback answers no CORS -- but not on a listener the whole LAN can reach. (A host that binds the main listener to `0.0.0.0` makes them reachable anyway -- see the [README](../README.md#quick-start).) The consequence is deliberate: a stream stremio-core plays *through* `/proxy` (an addon stream that needs request headers a player cannot attach) cannot be cast from this listener. Casting it needs another path -- the client resolving it itself, or an addon that hands out a header-free URL -- and the server does not paper over the gap by widening the LAN surface.

CORS is set up for what a receiver needs: `Content-Type`, `Accept-Encoding`
and `Range` are named allowed request headers (Google's Web Receiver CORS
requirements ask for exactly those, and even a plain MP4 needs CORS once
tracks are involved), and `Accept-Ranges`, `Content-Range` and
`Content-Length` are exposed to script so a player can seek. This is the one
listener that answers CORS: the receiver is a browser media element, and the
loopback listener's readers are not. Byte-range
requests and `HEAD` work on this listener exactly as they do on loopback.
