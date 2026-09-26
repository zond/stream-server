# Thin swarms: why a download goes quiet for minutes

Design, 2026-09-26, against rqbit `d02b73a2` and stream-server `300673e`.
**Built the same day** in the rqbit fork (see the end of §4 for what was built
as designed, what was tightened, and what was dropped).

## 1. What a thin swarm is

A swarm is *thin* when few of the addresses we learn ever accept a
connection. The tracker's count says nothing about that: most addresses a
tracker or the DHT hands out belong to peers behind NAT with no port
forwarded, peers that left hours ago, or peers already at their own
connection limit. In a big swarm that does not matter -- a few percent of
several hundred is still a dozen sources. In a small one it is the whole
story.

The phone on 2026-09-25, home wifi, three torrents:

| torrent | swarm (scrape) | addresses known | connected | outcome |
|---|---|---|---|---|
| Gilmore Girls S05 | 1 seeder, ~80 leechers | 126 | 0-1 | stopped at 54 %, **zero sockets for 10+ min** |
| another episode | 25 seeders | 364 | 1 | finished, slowly, from one seeder; 39 dials sitting in `SYN_SENT` |
| a popular title | large | -- | 17 | 16 MB/s |

Same client, same network, same code. The only thing that varied is how
many addresses answered. So this is not a cap (`effective_connection_limits`
floors at 40 per torrent; the background footprint is 8) and not the piece
picker. It is what happens **after** the few reachable peers hang up.

## 2. What rqbit does today

Read from the fork, so each point is checkable:

1. **An address enters the peer table once.** `Peers::add_if_not_seen`
   (`torrent_state/live/peers/mod.rs`) inserts on `Vacant` and returns
   `None` on `Occupied`. A tracker or DHT reply naming an address that is
   already in the table -- in any state, including `Dead` -- does nothing.
2. **A peer that dies is retried on an exponential schedule.**
   `on_peer_died` marks it `Dead` and, for an outgoing peer, schedules a
   requeue after `stats.backoff.next()`
   (`torrent_state/live/peer/stats/atomic.rs::backoff`): min 10 s, factor 6,
   jitter, max 1 h, give up after 24 h. So roughly **10 s, 1 min, 6 min,
   36 min, 1 h, 1 h ...**. An incoming peer is never retried (it has no
   address we can dial). When the schedule is exhausted the peer is dropped
   from the table.
3. **The schedule resets only on a verified piece.** `reset_peer_backoff`
   has one caller: the hash-check success path. A peer that delivered is
   back at 10 s; one that connected, sent a bitfield and choked us, or one
   that never answered, keeps climbing.
4. **New addresses arrive slowly once the first wave is in.** The tracker
   loop sleeps for the tracker's `min interval` (else `interval`, commonly
   30 min; 60 s only after an error). The DHT re-queries each 60 s once it
   has found peers (every 1 s only while it has found none). And by point
   1, what either of them names again is ignored.
5. **Going lean forgets the dead.** `forget_disconnected_peers` (called when
   the app is backgrounded) drops every `Dead` and `NotNeeded` entry --
   which does let a later sighting re-add them, but also loses their
   backoff and "was useful" history.

## 3. How those combine into a ten-minute stall

Take the 54 % torrent. The first minute: 126 addresses, most never answer;
one leecher that holds 54 % of our file connects and we take everything it
has. Then it hangs up -- leechers rotate their unchoke slots, and they have
nothing more to gain from us.

* It goes `Dead`. Retries at ~10 s and ~1 min; if it is busy or has
  choked us, no verified piece arrives, so the backoff is not reset and
  the next wait is ~6 min, then ~36 min.
* Every other address is already `Dead` from the first wave, climbing the
  same schedule without ever having delivered.
* The DHT keeps finding the same addresses; `add_if_not_seen` ignores them.
  The tracker has told us to come back in 30 min.

So the table fills with addresses that are each waiting minutes to hours,
the queue is empty (`queued=1`, then `0`), nothing is dialled, and
`/proc/net/tcp` shows no sockets at all. That is exactly the field reading:
`is_paused: false`, the session alive, DHT traffic ongoing, the torrent
silent.

Streaming the same torrent "worked" for the same reason it later stopped:
the one peer was there at the time, and it was the one peer both modes had.

## 4. What should change

The principle: **the retry schedule should depend on what we know about a
peer and on how badly the torrent needs peers**, not only on how many times
the peer has failed. A torrent with 30 live peers can afford to forget a
flaky one; a torrent with zero cannot.

### 4.1 Separate "proven" from "never answered"

Add to a peer's stats whether it has ever delivered a verified piece
(the reset site in point 3 already knows). Then:

* **Proven peer, torrent starving** (live peers below a floor, say 4, and
  pieces still wanted): retry on a flat 60 s -- libtorrent's own
  `min_reconnect_time`, which no tracker or peer treats as aggressive -- for
  as long as that holds. These are the only addresses we know are real
  sources; a leecher that choked us now may unchoke us on its next
  rotation, which is tens of seconds away, not an hour.
* **Proven peer, torrent healthy:** the existing schedule.
* **Never answered:** the existing schedule. Those are the NAT'd and the
  departed, and hammering them buys nothing.

Cost: at most a handful of extra SYNs per minute per starving torrent, to
addresses that have previously served us. Bounded by the number of proven
peers, which in a thin swarm is small by definition.

### 4.2 A fresh sighting brings a dead peer forward

When a tracker or DHT reply names an address that is `Dead` and waiting,
**proven**, and the torrent is starving, move its requeue to now instead of
ignoring it -- at most four per torrent per minute, so a reply that names
every dead peer at once is a few dials and not a burst. A peer that has just announced itself to a tracker or answered a DHT
query is more likely to be alive than one we last heard from twenty minutes
ago. This is a change to the `Occupied` arm of `add_if_not_seen` (or a new
`add_or_refresh`) and needs the state check under the entry lock that
`drop_peer_if` already uses.

Not for `Live`/`Connecting`/`Queued` peers (nothing to do) and not for
`NotNeeded` (we decided we don't need it).

### 4.3 Starvation is a signal the torrent can act on -- mostly not

The first draft here had the torrent re-announce to its trackers when it
starved. Reading `tracker_comms` closed that: the announce loop already
sleeps the tracker's `min interval` when it states one and `interval`
otherwise, which is the earliest a client may ask again. There is nothing
to gain inside the rules, and asking outside them is what gets a client
throttled or banned -- so no re-announce. The DHT already re-queries every
60 s; with 4.2 in place what it finds is no longer discarded, which is most
of the value.

What starvation *does* drive is the report: the progress line says
`starving`, and the count of `dead` peers, how many of them are proven,
and the time until the soonest retry -- so a stall reads as "waiting 40 s
for the only peer that ever served us" rather than as silence.

### 4.4 Keep proven peers across going lean

`forget_disconnected_peers` should keep `Dead` entries that are proven,
for the same reason the requeue path already puts proven peers first when
the cap goes back up (`task_peer_adder`: "peers we have already talked to
go first"). They are a few hundred bytes each and they are the most
valuable addresses the torrent has. Parked (`NotNeeded`) peers still go:
that is what the peer-limit tests pin, and a raise re-dials what it parked
from the queue, not from the table.

### What was built (rqbit fork, 2026-09-26)

* `PeerStats::proven` (a verified piece delivered) and
  `PeerStats::next_wait`: the flat retry while starving, the exponential
  schedule otherwise, the schedule's position untouched by the flat phase.
* `TorrentStateLive::is_starving`: fewer live peers than the floor and
  pieces still wanted. `ManagedTorrent::set_starving_retry(floor, retry)`
  sets both numbers; defaults 4 peers and 60 s.
* `add_peer_if_not_seen` brings a proven dead peer forward while starving,
  four per minute per torrent; a generation counter keeps the stale sleep
  from re-queueing it a second time.
* `forget_disconnected_peers` keeps proven dead peers.
* `TorrentStateLive::retry_summary`: dead, proven dead, seconds to the
  soonest retry.
* Tests: the schedule choice as unit tests on `PeerStats`; the forget
  predicate and the burst cap as unit tests; and two end-to-end tests with
  a real seeder that hangs up (`e2e_thin_swarm.rs`) -- three deaths, each
  followed by a re-dial within the flat retry, and a sighting that brings a
  parked peer forward.

## 5. What this does not fix, stated

* **A swarm whose only copy is offline stays stuck.** If the one seeder is
  gone and the leechers together hold 54 %, no retry policy produces the
  other 46 %. The progress line should make that visible (connected
  peers' combined availability for the file, which the piece tracker can
  answer), so the app can say "no one online has the rest of this file"
  rather than show a bar that never moves.
* **Inbound reachability.** Most of the 39 unanswered dials are peers
  behind NAT; they can only reach *us*. The phone's UPnP / listen-port
  situation decides that, not this design, and it is worth measuring on
  its own (does anything ever dial in on wifi?).

## 6. Where it goes

rqbit, as patches shaped for upstream (the fork follows upstream monthly):
4.1, 4.2 and 4.4 are all inside `torrent_state/live` and are useful to any
rqbit user, not only a streaming client. The "starving" predicate needs a
floor that is policy; make it a torrent option with a default, so
stream-server can set it without rqbit knowing why. 4.3's tracker half is a
`tracker_comms` change -- a "please announce now if allowed" channel
alongside the existing sleep.

stream-server: set the floor, and extend the `download progress` line
(`routes/downloads.rs`) with `starving_secs`, `dead`, and `next_retry_secs`.

## 7. Tests

rqbit has the pieces for all of these without a network (fake peers, the
backoff under `start_paused`):

1. **Proven + starving -> short retry.** One peer delivers a piece and
   dies; no other peers; the requeue lands within 60 s of paused time, not
   ~6 min on the third death.
2. **Unproven + starving -> unchanged schedule.** Same, with a peer that
   never delivered: the waits are the existing ones.
3. **Healthy torrent -> unchanged schedule** even for a proven peer.
4. **Re-sighting brings forward.** A `Dead` peer waiting 6 min is named by
   a (fake) DHT reply while starving -> dialled now; while healthy -> not.
5. **Lean keeps proven.** `forget_disconnected_peers` drops unproven dead
   entries and keeps proven ones.
6. **Tracker re-announce respects `min interval`.** Starving at t=10 s
   after an announce with `min interval = 120` -> the next announce is at
   120 s, not 10 s and not 30 min.

And one field check to call it done: the 54 % torrent (or any thin one) on
the phone, where the progress line shows `peers=0` for no longer than the
short retry schedule while a proven peer is still online.
