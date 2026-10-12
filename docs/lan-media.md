# LAN media listener

The second listener a cast session turns on so a receiver on the local network can fetch what the app published for it. It serves by token, and nothing else.

A cast receiver is not on loopback. `ServerConfig::default()` binds
`127.0.0.1` only, so a Chromecast cannot fetch a byte from it, and casting
is blocked before the media is even prepared. Widening that bind is not the
fix: it would put `/settings`, the stats route and `/create` on the local
network behind nothing but a bearer token.

So there is a **second listener** instead, and it serves **published cast
tokens and nothing else**.

| | |
|---|---|
| **What it exposes** | `GET`/`HEAD` `/cast/{token}` ([`server/src/cast.rs`](../server/src/cast.rs)), and for a rendition its file at `/cast/{token}/stream.mp4` ([Renditions](#renditions)). The app publishes a media id for a cast (`ServerHandle::publish(&MediaId, Option<PlayToken>) -> CastToken`) and hands the receiver `<lan_media_base_url>/cast/<token>`. The route serves what the id resolves to (a torrent file, a link through `/proxy`'s cache, a Google Drive file, a finished download, a file on this device, a member of an archive), with the range framing every media route shares (`200`/`206`/`416`, `Content-Range`, `HEAD`, the DLNA headers). An unknown token is a `404`. A link whose origin will not serve ranges is refused (`501`, `{"refused":"noRanges"}`): nothing here can seek it for a receiver. Every refusal is the archive routes' `{"refused","message"}`, and is logged as `cast refused` with its kind. The statuses are `404` for `unknownId`, `noSuchFile` or `unrecognisedUrl`, `422` for a malformed container and `415` for another translator refusal, `501` for `noRanges`, `noReader` or `notYet`, `507` for `insufficientDiskSpace`, `503` for `serverStopped`, and `502` for the rest |
| **What it does not** | Another method on a cast path is a `405`. Every other path is a `404`, for every method. The control router is **not mounted at all** (a control path answers `404`, never the `401` that would confirm the route exists and only a bearer token is missing). Neither is any loopback media route: not the torrent routes, the archive routes, `/proxy`, `/ftp`, `/drive/stream`, `/downloads/{key}/stream` or `/local-addon` |
| **Where it binds** | `ServerConfig::lan_media_addr: Option<SocketAddr>` -- `None` by default, so nothing changes unless an embedder asks for it. `Some(0.0.0.0:0)` lets the OS pick the port |
| **When it runs** | `ServerHandle::set_lan_media(true)` starts it, `set_lan_media(false)` stops it. The pair is meant to bracket a cast session, so the LAN surface exists only while something is casting. Nothing is bound at startup, whatever the configuration: a port already in use fails the cast that asked for the listener, never the server |
| **How it is switched off entirely** | The `lanMediaEnabled` setting (`POST /settings`, **`false` by default**). While it is false, `set_lan_media(true)` is refused. Setting it back to false also stops a listener that is already running, which unpublishes every token |

## Tokens

A cast token is **128 random bits, hex, never derived from the id**: a
receiver that saw one learns nothing about another, or about the id. Tokens
live in memory only.

* **Publishing needs the listener running** (`set_lan_media(true)` first),
  and an id this server holds (`register` issued it and it has not been let
  go). The publication **holds the id** as an open reader does, so the id
  is not evicted while it is cast. It also **holds its torrent** (an id a
  torrent is behind): from publish to unpublish the torrent runs and keeps
  its windows, whatever the receiver is reading and whatever else opens
  ([Storage](storage.md), *Who keeps a torrent running*).
* **With a play token**, the receiver's reads are the viewer's playback,
  by the same rules `open_reader` applies. The play session moves to the
  file, and it shares as the app's own player's would. Each `GET` opens the
  id's source as a reader does. A torrent's stream registers at the open
  and ends when the body is dropped. Without a play token the reads are an
  aside, which moves no session and shares nothing.
* **`unpublish(&CastToken)` cuts the bytes.** Nothing more is served under
  the token (`404`), and every body being served under it ends at once.
  Each cast body polls its token's cut before every chunk, so even a body
  parked on a piece nobody has is woken. It ends with an **error**: the
  connection is dropped, and the receiver reads a broken source rather
  than a file that ended early.
* **Stopping the listener unpublishes every token** -- `set_lan_media(false)`,
  the `lanMediaEnabled` veto revoked, the server's own stop -- so it stops
  the bytes too, not only new fetches.
* **A token is never logged.** `/cast/<token>` is written as `/cast` in
  every request line and span (`routes::util::log_path`, beside `/proxy`
  and `/ftp`), and `CastToken`'s `Debug` prints no token. A token in a log
  file is a URL into this device for as long as it is published.

## Renditions

A cast the receiver cannot decode as it is -- an MKV, surround sound over
Bluetooth, HEVC to a receiver without it -- is cast as a **rendition**: one
progressive fragmented MP4 produced on demand by the embedder's producer
and muxed here, nothing on disk ([design/renditions.md](design/renditions.md)).
`ServerHandle::publish_rendition(&MediaId, RenditionSpec, Option<PlayToken>)`
publishes one under a token with every rule above (random, memory only, a
lease on the id, cut by `unpublish` and the listener's stop, never logged).
It is refused with `noProducer` until the embedder has called
`install_producer`. The receiver is handed
`<lan_media_base_url>/cast/<token>/stream.mp4`.

| Path | Answers |
|---|---|
| `GET /cast/{token}/stream.mp4` | `video/mp4`, `Cache-Control: no-store`, a `Content-Length` and `Accept-Ranges: bytes`, with the range framing every media route shares (`200`, `206` with `Content-Range`, `416` naming the length). It is **a file whose every byte is fixed before it is made**. First comes the header (`ftyp` + `moov` + `sidx`, and a second `sidx` for the sound in an estimated layout with sound). Then comes one slot per segment. Segments fall per sync sample when the source has an index and its sync samples are a second or more apart (about a second each when they are closer). When the source has no index, they are slots of `segmentMs`, estimated. Each slot holds that segment's fragment: a `styp` unless the slot opens at its `sidx` label, then a `moof` + `mdat` per half second, the picture and then the sound beside it. The slot is padded to its end with `free` boxes of 1 KiB, the last under 2 KiB (`layout::padding`, `PAD_BOX`), so a demuxer reads its way over them instead of seeking. The response waits for the first run's formats and the source's index, which fix the length. A range that begins in a slot waits for that slot's fragment before it answers. So a rendition that cannot make it is `503` `{"refused":"renditionFailed","message":...}` (or `{"refused":"unpublished","message":...}` for one the unpublish woke). A range that begins in the header answers at once. After that, a failure or the cut breaks the body with an error. Nothing times it out |
| `HEAD /cast/{token}/stream.mp4` | The same headers. It waits for the length as a `GET` does (starting the first run) |

**The receiver seeks by bytes.** The init segment carries the film's
length (`mvhd`, `tkhd`, and each track's `mdhd` on its own clock, which is
the one a receiver's demuxer reads). The video's `sidx` carries two
references per slot: the slot's first 8 KiB, labelled at its time, and the
rest, which lasts nothing. So a seek, in every stream, is one `Range`
straight at the slot that holds the time (measured on zond's TV:
[design/renditions.md](design/renditions.md), "Seeking by bytes"). A
mirrored layout's sound finds its slot by the video's times. An estimated
layout with sound has a `sidx` of its own for the sound, labelled at each
cut less the sound's lead.

The layout **mirrors** the source's index when there is one (Matroska cues,
an MP4's sample tables, an AVI's `idx1`). Segment `n` is cut at the first
indexed sync sample at or after `n` seconds (a wider grid for a film over
about nine hours). Its slot is as long as the source's bytes from that sync
sample to the next segment's, plus a little headroom (8 KiB and a 64th),
plus room for a `moof` and `mdat` header every half second and the 8 KiB
first part (`layout::interleave_room`). The slots are **estimated** when
there is no index (a transport stream, an MKV with no cues). They sit on
the `segmentMs` grid, sized in proportion to time over the source's size,
15% larger, with the same 8 KiB and header room on top. Each is labelled in
the `sidx` 10 s after its cut less the picture's half-second decode lead
(its first sync sample may be up to a GOP late). Every slot is then
lengthened to end on a 32 KiB boundary, the block Chrome asks in
(`SLOT_ALIGN`).

A segment holds the video from its sync sample to the next segment's, and
the audio between the two cuts, 564 ms early (half a second for the
picture's decode lead and 64 ms more). So the frame playing at the sync
sample is in its fragment, where a demuxer seeking the sound looks. A
segment that does not fit its slot keeps what fits and **spills** the rest
into the next slot. When the next slot's start is already decided
(something of it made, or a run started at it), it **drops** the rest
instead (`rendition_slot_truncated`). That decision is made once and kept,
so a slot made again is the same bytes. The last 16 bytes of every slot
are zeros and are answered without making anything (a demuxer peeking at
the file's end for an `mfra`).

A plain token has no `stream.mp4` (`404`). A rendition's token serves it
and, like a plain one, the source as it is at `/cast/{token}`. Each `GET`
that begins sending bytes counts as one body (`lan_media_bodies_served`). A
`416` or a `503` does not.

Up to two runs are live at once. Each produces the slots that begin within
12 s of the one last asked of it, two at least, and then waits. A range at
a slot no run will make soon starts a run there, two seconds before the
slot's cut, so a run started there makes exactly what one passing through
makes. The new run goes beside the other, or in place of the least
recently asked one nobody is waiting on, so two readers far apart each keep
one. Only a range's first look at its first slot takes a run somebody waits
on. Only requests make slots. The run that fixes the layout starts at the
film's start, which is what a reader of the header reads on into. The
receiver's opening read into slot 0 is served like any read (Chrome's
demuxer probes it before it seeks). The ring keeps what 96 MiB holds, and
the slots farthest from any run are dropped first. A run nobody has asked
anything of for a minute is let go (the slots made are kept), but never
while a request waits for it to make a slot. A run that makes less than its
own time in film over ten seconds of its own work fails the rendition with
a sentence. Those ten seconds leave out the time it waited for the receiver
and for the source. `ServerHandle::rendition_state(&CastToken)` reports the
sentence (`{"state":"failed","sentence":...}`). Otherwise the state is
`producing`, `idle`, or `ended` for a token not published, or not a
rendition.

**Prepare before the receiver is told to load.** A receiver's first answer
waits for the layout (the first run's formats and the source's index, which
for a Matroska file is its Cues, usually at its end) and for the slot it
starts in. The Chromecast default receiver gives up on a load that stays
silent that long, and behind a thin swarm it took a minute. So
`ServerHandle::prepare_rendition(&CastToken)` asks for what the receiver
will ask first, through the same request path. That is the header, slot 0
(the demuxer's probe), and the slot for `startMs` with the slots of the 6 s
after it, which one run makes in turn. The slot for `startMs` goes by the
`sidx` label. When mirrored it is the slot the time falls in (a slot begins
half a second before its key is shown), and when estimated it is an earlier
one. Nothing before that slot is made: a receiver asks for the file's start
and then for exactly that slot, which begins on the 32 KiB block Chrome
asks in. `rendition_readiness(&CastToken)` says how far the preparation has
got: `{"phase":"index"}`, `"start"`, `"ready"`, `"failed"` with the
`sentence`, or `"ended"`. The app loads the receiver at `ready`, and the
receiver's first requests find those slots in the ring. The run that made
them is moved back to the one asked, not restarted. No timer gives up. A
preparation waits on a stalled source as long as it takes (its run counts
as waited on, so it is never let go), and an unpublish ends it.

`ServerHandle::lan_media_base_url(for_peer)` builds the URL to hand a
receiver. The host is the local interface that shares `for_peer`'s subnet,
taken from the same interface enumeration `GET /network-info` answers from.
The first interface in that list would not do: on a host with a VPN or a
container bridge it is regularly one the receiver cannot route back to. A
listener bound to one specific address reports that address as is. It is
`None` whenever the listener is not running, which is also the signal that
no cast URL can be built yet.

**When no interface matches**, the candidates are *ranked* rather than taken
in enumeration order. That happens when the receiver is behind a router, or
when the caller has no receiver address to give (not every platform reports
one). An ordinary interface comes before one no receiver on a home network
can be behind, and within each group an RFC1918 address comes before
anything else. The demoted interfaces are carrier links (`rmnet`, `ccmni`,
`pdp_ip`), tunnels (`tun`, `utun`, `tap`, `wg`), container and VM bridges
(`docker`, `br-`, `veth`, `virbr`, `vboxnet`), the interfaces this device
hands out rather than reaches a LAN through (`ap0`, `p2p`, `rndis`) and
`dummy`, all matched by name. The bridges are why the address shape cannot
decide this on its own: `docker0`'s `172.17.0.1` is as private as the Wi-Fi
address beside it, and only the name tells them apart. An interface the
kernel reported no netmask for is matched against no subnet at all, since a
`0.0.0.0` mask matches every peer and would win outright over the interface
that really shares one. A phone is on Wi-Fi and cellular at once, and
`getifaddrs` will happily list the cellular interface first. Naming that
address to a Chromecast is a cast that hangs forever, because a TCP connect
to an unroutable host does not fail, it waits. The demotion is only ever a
tie-break: a matching subnet still wins outright, and a host whose one
routable address is cellular is still offered it rather than nothing.

**A cast that never starts leaves no other trace**, which is why this
listener reports on itself. Every answer `lan_media_base_url` gives is
logged at INFO: the peer, the interface picked and the URL. So is each of
the ways it can answer `None` (an authority it cannot parse at WARN), and
so is every request that reaches the listener (method, path with the token
elided, peer). Two counts read the same session from the outside. Both are
reset by every start and by every stop, and never carried over. A start
resets them whether or not a listener was already running, since starting
a cast to a second receiver mid-session asks about that cast and not the
one before it. The two counts are these:

* `ServerHandle::lan_media_requests_served()` -- every request the listener
  receives, a `404` included: the receiver got here.
* `ServerHandle::lan_media_bodies_served()` -- every `/cast` `GET` that began
  sending bytes. It never counts a `HEAD`, an unknown token's `404` or a refusal.

A receiver told an address it cannot route to reports no error at all,
because a TCP connect to an unroutable host hangs rather than failing. From
the sofa that is indistinguishable from buffering. So the app's check, some
seconds after the receiver was told to load, has **three readings**:

| Requests | Bodies | Reading | What to do |
|---|---|---|---|
| 0 | 0 | Nothing reached this device: the address is wrong (another interface, a client-isolated Wi-Fi, a firewall). | End the session and say the receiver cannot reach this device. |
| > 0 | 0 | The receiver reached this device and was served nothing yet: a token it was not given, an id that would not open (the refusal is in the log), or a stream that has not come. | Say so -- the network is fine. xtremio puts a note on the remote and asks again rather than ending a cast whose stream may still come. |
| > 0 | > 0 | The network and the server did their part. | Leave it to the media. |

The addresses in those log lines are private ones on the user's own LAN.
The bearer token is never on this listener, and a cast token is never in a
log line.

**What one publication has served** is a third reading, per token rather
than per session. `ServerHandle::cast_numbers(&CastToken)` answers
`CastNumbers`, which is what the app's cast panel draws, or `None` once the
token is unpublished. It holds counts that only grow and positions as they
are now, with no rate and no clock: two answers and the time between them
make every rate. It is cheap (atomics and one look under the rendition's
lock, no runtime hop) and safe to poll once a second.

```json
{
  "kind": "rendition",
  "contentType": "video/mp4",
  "delivery": {"requests": 7, "bodiesBegun": 6, "bodiesEnded": 5, "bodiesOpen": 1,
               "bytes": 48213004, "lastRequestAt": 47022080, "furthestAt": 48496640},
  "source": {"kind": "torrent", "bytesRead": 51003392, "opens": 2, "seeks": 14},
  "rendition": {
    "video": "copy", "audio": {"aacStereo": {"bitrate": 192000}},
    "videoOut": {"codec": "hevc", "width": 1920, "height": 1040, "channels": null, "sampleRate": null},
    "audioOut": {"codec": "aac", "width": null, "height": null, "channels": 2, "sampleRate": 48000},
    "layout": {"slots": 3363, "slotMs": null, "exact": true, "total": 2471230464},
    "runs": [{"from": 63, "fromMs": 126040, "produced": 67}],
    "runsStarted": 2, "slotsMade": 70, "filmMadeMs": 139800,
    "askedSlot": 64, "askedMs": 128040, "madeTo": 129, "madeToMs": 262000
  }
}
```

* `kind` is `plain` (the source as it is, at `/cast/{token}`) or
  `rendition`. `contentType` is what the receiver is answered with: a plain
  publication's once a request has resolved its source, `null` before.
* `delivery`: every `GET` and `HEAD` under the token, answered with bytes
  or not; the responses that began a body (as `lan_media_bodies_served`
  counts them), those ended -- whole, cut by an unpublish, or let go by
  the receiver -- and the difference; the bytes sent; the byte the latest
  body began at; and one past the furthest byte any body has sent. A
  healthy receiver makes a request per seek, so requests rising while
  nobody seeks is the number to watch.
* `source`: what was read for the receiver -- every body of a plain
  publication (each one open, its bytes the bytes sent), every run of a
  rendition (each one open, and a seek for every reopen at another offset
  its producer makes). `kind` is the first source opened: `torrent`,
  `member` (a file inside an archive), or `http` for every source read
  through the shared reader (a link, a Drive file, a finished download, a
  file on this device). `http` names the reader, not the network. A
  rendition token's bodies at `/cast/{token}` count here too, as a plain
  publication's do.
* `rendition`, for a rendition: the spec's plans and the tracks the first
  run reported making; the layout once it is fixed (`slotMs` when every slot
  but the last holds the same film -- an estimated layout's grid, or a
  mirrored one whose sync samples are evenly spaced -- and `null` with
  fewer than three slots); the live
  runs, each with the slot it began at and how many it has made; runs
  begun since the publish; every slot made and the film they hold (a slot
  dropped from the ring and asked again is made again, and counted again
  -- `filmMadeMs` over the wall time between two answers is the production
  speed); the slot the receiver last asked for (a range's first slot, or
  one it read on into); and `madeTo`/`madeToMs`, the end of the unbroken
  run of slots in the ring from that one, which is how far ahead of the
  receiver the film is made. Every absence is `null`.

A publication's end -- unpublished, or every token at the listener's stop
-- writes one INFO line with its totals (`stage="cast_publication_end"`:
`rendition`, `requests`, `bodies` (begun), `bytes`, `runs` (begun; `0` for
a plain one), `source_read`, `source_opens`, `source_seeks`) and nothing
that names what it was or where. Each plain body writes
`stage="cast_body_start"` (`source`, `start`, `length`, `played`) and
`stage="cast_body_end"` (`source`, `delivered`, `length`, `cut`), `source`
being the kind above. A rendition's body writes no start line, and its end
line says `source="rendition"`.

**Stopping closes the door and stops the bytes.**
`set_lan_media(false)` unpublishes every token, which cuts every cast body
in flight. Then it aborts the serving task and awaits it. By the time the
call returns, the listener socket is closed and the port is free (it
rebinds immediately), nothing new is accepted, and a connection idling on
keep-alive is closed without serving another request. The call is not a
drain: it returns at once rather than waiting for a receiver, so ending a
cast session or revoking `lanMediaEnabled` never blocks. The loopback
listener owns a different socket and a different serve future. It and
every request in flight on it are untouched.

**The trade-off, stated plainly.** While a token is published, *anyone* on
the same network who learns the URL can fetch that one file. A receiver
cannot attach a header, so there is no authentication on that port beyond
the token itself. What they cannot do is anything else: guess another
token, walk a Drive account or the piece cache by info hash, or make this
device fetch, add or open anything. There is no route on this listener that
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
