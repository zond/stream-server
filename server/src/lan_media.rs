//! The optional second HTTP listener that serves media bytes to the local
//! network.
//!
//! A Chromecast (or any other receiver on the LAN) cannot fetch anything from
//! a loopback-only server, which is what [`ServerConfig::embedded`] binds. It
//! also must not be handed the control API: a cast session needs media bytes
//! and nothing else, while the control surface reaches settings, offline
//! downloads, engine stats and the torrent session.
//!
//! So this is a whole second listener rather than a wider bind on the first
//! one. It serves `crate::lan_media_routes` alone: the byte-serving routes,
//! over torrents and archive sessions the loopback side has already created,
//! and nothing that a stranger on the network could make this device *do* --
//! no route that fetches a caller-named URL, none that starts a torrent, and
//! no control route at any level, not even behind the bearer middleware, so
//! an unknown-path `404` is the strongest answer the LAN can get out of them
//! and there is no token to guess, leak or brute-force. Both listeners share
//! one [`AppState`], so a stream the LAN pulls uses the same engines, piece
//! cache and settings as one the loopback listener serves.
//!
//! It is off unless an embedder configures [`ServerConfig::lan_media_addr`],
//! and [`ServerHandle::set_lan_media`] starts and stops it at runtime so it
//! exists only for as long as a cast session does -- nothing binds it at
//! startup, so a configured address is a place, not a running listener. The
//! `lanMediaEnabled` setting is the operator's veto, and since every start
//! goes through `set_lan_media`, every start is subject to it.
//!
//! [`ServerConfig::embedded`]: crate::ServerConfig::embedded
//! [`ServerConfig::lan_media_addr`]: crate::ServerConfig::lan_media_addr
//! [`ServerHandle::set_lan_media`]: crate::ServerHandle::set_lan_media

use crate::routes::system::LocalIpv4Interface;
use crate::state::AppState;
use anyhow::Context;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use url::Url;

/// The LAN media listener's control block, held by [`AppState`] so
/// [`crate::ServerHandle`] (which starts and stops it per cast session),
/// the settings update (which stops it when the veto is revoked) and
/// [`crate::run`] (which stops it at shutdown) all reach the same one.
pub struct LanMedia {
    /// Where a listener binds when it is started, from
    /// [`crate::ServerConfig::lan_media_addr`]. `None` means the embedder
    /// never configured one and the listener can never run.
    configured_addr: Option<SocketAddr>,
    /// The running listener, if any. A single mutex serialises start and
    /// stop, so two concurrent toggles cannot both bind.
    running: tokio::sync::Mutex<Option<Running>>,
    /// Requests that have reached the listener since the cast session it is
    /// serving began -- see [`LanMedia::requests_served`]. Not behind the
    /// mutex, though both [`LanMedia::start`] and [`LanMedia::stop`] reset
    /// it while holding it: it is written
    /// from the serving task on every request and read from whatever thread
    /// asks, and neither cares to be ordered against anything else.
    requests: AtomicU64,
}

struct Running {
    bound: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl LanMedia {
    pub fn new(configured_addr: Option<SocketAddr>) -> Self {
        Self {
            configured_addr,
            running: tokio::sync::Mutex::new(None),
            requests: AtomicU64::new(0),
        }
    }

    /// The address a listener would bind, whether or not one is running.
    pub fn configured_addr(&self) -> Option<SocketAddr> {
        self.configured_addr
    }

    /// The address the listener is bound to right now, or `None` when it is
    /// not running. With a configured port of 0 this is the OS-assigned port,
    /// which is why the answer comes from the listener and not from the
    /// configuration.
    pub async fn bound_addr(&self) -> Option<SocketAddr> {
        self.running
            .lock()
            .await
            .as_ref()
            .map(|running| running.bound)
    }

    /// Bind the listener and start serving media routes on it. Idempotent:
    /// an already-running listener is left alone and its address returned.
    /// The request count is reset either way -- see below.
    ///
    /// Fails when no address is configured, or when the bind fails.
    pub async fn start(&self, state: &AppState) -> anyhow::Result<SocketAddr> {
        let mut running = self.running.lock().await;
        // The count belongs to this session, not to the process or to the
        // listener: a caller asking "has the receiver fetched anything yet?"
        // is asking about the cast it just started, and a count left over
        // from the previous one would answer yes for a receiver that never
        // connected. That is why this is above the already-running return
        // rather than beside the bind -- casting to a second receiver
        // mid-session starts a cast on a listener that is already up, and
        // that cast is the one being asked about.
        self.requests.store(0, Ordering::Relaxed);
        if let Some(running) = running.as_ref() {
            return Ok(running.bound);
        }
        let addr = self.configured_addr.ok_or_else(|| {
            anyhow::anyhow!(
                "no LAN media address is configured; set ServerConfig::lan_media_addr to the \
                 address the listener should bind"
            )
        })?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind the LAN media listener on {addr}"))?;
        let bound = listener.local_addr()?;
        let app = crate::build_lan_media_router(state.clone());
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                tracing::error!(%error, "LAN media listener failed");
            }
        });
        tracing::info!(
            %bound,
            "LAN media listener started; media routes only, no control API"
        );
        *running = Some(Running { bound, task });
        Ok(bound)
    }

    /// Stop the listener. A no-op when it is not running.
    ///
    /// **This closes the door, not the connections already through it.** The
    /// serving task owns the `TcpListener` and the accept loop, so aborting
    /// and awaiting it closes the socket: by the time this returns the port
    /// is free -- it rebinds immediately -- nothing new is accepted, and a
    /// connection sitting idle on keep-alive is closed without serving
    /// another request.
    ///
    /// A response that is *already* streaming is not cut. axum spawns every
    /// accepted connection into a task of its own, and dropping the serve
    /// future signals those tasks rather than owning them: each answers by
    /// calling hyper's `graceful_shutdown`, which stops the connection taking
    /// further requests and then lets the response in flight run to its end.
    /// So a receiver mid-file keeps being fed, by this process, through an
    /// interface this call has otherwise shut, until it has the whole thing
    /// or hangs up. That is measured behaviour on axum 0.8, not an inference
    /// from the API.
    ///
    /// The call is still not a drain, and that is the point of the abort: it
    /// returns as soon as the accept loop is gone instead of waiting out a
    /// movie-length response, so ending a cast session -- or the operator
    /// revoking `lanMediaEnabled` -- never blocks on a receiver's download.
    /// What it does not do is stop the bytes, and nothing else here does
    /// either: cutting a stream in progress would mean holding each
    /// connection's task and aborting it, which needs the listener built on
    /// `hyper_util`'s connection builder by hand (axum's `serve` hands out no
    /// such handle) or every media body wrapped in a cancellation token.
    /// Neither is a reordering of this function.
    ///
    /// Nothing here touches the loopback listener: it owns a different socket
    /// and a different `axum::serve` future, and requests in flight on it --
    /// including ones sharing the very torrent the LAN was streaming -- run on
    /// untouched. Only the shared [`AppState`] is common, and it is not
    /// modified.
    pub async fn stop(&self) {
        let mut running = self.running.lock().await;
        if let Some(running) = running.take() {
            running.task.abort();
            // Awaiting the aborted task is what makes the stop observable:
            // the task owns the `TcpListener`, so the port is only released
            // once it has been dropped.
            let _ = running.task.await;
            tracing::info!(bound = %running.bound, "LAN media listener stopped");
        }
        // The reset goes below that await, not above the abort. Aborting is
        // not instantaneous: the accept loop can already have taken a
        // connection whose request is dispatched -- and counted, from the
        // serving task -- while this one sits at the await. Reset first and
        // that arrival lands on the fresh zero, handing the next session a
        // request its receiver never made, which is exactly the reading
        // [`LanMedia::requests_served`] exists to be trusted for. Reset last
        // and it cannot: by then the accept loop is gone and the shutdown has
        // reached the connection tasks, which take no further request.
        // Unconditional, running listener or not, so the count reads zero
        // whenever nothing is listening.
        self.requests.store(0, Ordering::Relaxed);
    }

    /// Count one request arriving on the listener. Called by the tracing
    /// layer [`crate::build_lan_media_router`] installs, which is the one
    /// place every request to this listener passes through -- including the
    /// ones the fallbacks answer with a `404`, since a receiver that asks
    /// for the wrong path has still demonstrably reached us.
    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    /// How many requests have reached the listener since the current cast
    /// session began. Every [`LanMedia::start`] resets it, running listener
    /// or not, so a second receiver never inherits the first one's count;
    /// [`LanMedia::stop`] resets it too, so it reads zero whenever nothing
    /// is listening.
    ///
    /// Zero is the diagnosis a caller cannot make any other way. A receiver
    /// that never fetched the stream and one that fetched it and failed to
    /// play it look identical from the sofa -- both are a still picture --
    /// and only the first is this device's fault: it means the address we
    /// handed out was one the receiver could not reach. A non-zero count
    /// says the network is fine and the problem is the media.
    pub fn requests_served(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// The base URL to hand a receiver at `peer`, e.g.
    /// `http://192.168.1.7:11471/`. `None` when the listener is not running.
    ///
    /// The host is the local interface that can actually reach `peer` (see
    /// [`pick_host`]), because a receiver is told a URL it has to connect
    /// back to: on a host with a LAN and a VPN or container bridge, the
    /// first interface in the list is regularly the wrong one.
    ///
    /// Every answer is logged at INFO, each way of returning `None`
    /// distinctly. Naming the wrong address leaves no other trace anywhere:
    /// a receiver told an address it cannot route to hangs on the TCP
    /// connect rather than failing, so the cast simply never starts and
    /// nothing on either side says why. The address is a private one on the
    /// user's own LAN, not a secret.
    pub async fn base_url_for(&self, peer: IpAddr) -> Option<Url> {
        let Some(bound) = self.bound_addr().await else {
            tracing::info!(%peer, "no LAN media URL: the listener is not running");
            return None;
        };
        let Some(host) = host_for_peer(bound, peer) else {
            tracing::info!(
                %peer,
                %bound,
                "no LAN media URL: no local interface for this receiver"
            );
            return None;
        };
        // `SocketAddr`'s Display brackets an IPv6 host, which is the spelling
        // a URL authority needs.
        match Url::parse(&format!("http://{}/", SocketAddr::new(host, bound.port()))) {
            Ok(url) => {
                tracing::info!(%peer, %host, %url, "LAN media URL for the receiver");
                Some(url)
            }
            Err(error) => {
                tracing::warn!(%peer, %host, %error, "no LAN media URL: unparsable authority");
                None
            }
        }
    }
}

/// The host a URL for `peer` should name, given what the listener bound.
fn host_for_peer(bound: SocketAddr, peer: IpAddr) -> Option<IpAddr> {
    // Bound to one specific address: that is the only address the listener
    // answers on, so there is nothing to pick.
    if !bound.ip().is_unspecified() {
        return Some(bound.ip());
    }
    let peer = match peer {
        IpAddr::V4(peer) => peer,
        // A v4-mapped v6 peer (`::ffff:192.168.1.7`) is what a dual-stack
        // socket reports for a plain IPv4 receiver.
        IpAddr::V6(peer) => peer.to_ipv4_mapped()?,
    };
    pick_host(&crate::routes::system::local_ipv4_interfaces(), peer).map(IpAddr::V4)
}

/// The local IPv4 address to advertise to a receiver at `peer`.
///
/// The interface whose subnet contains `peer` wins outright: that is the
/// address `peer` can demonstrably route back to, and everything else here
/// is guesswork beside it. Loopback is a subnet like any other for that
/// match, and an interface whose netmask the kernel never reported is on
/// nobody's subnet (see [`same_subnet`]) and takes its chances in the
/// ranking.
///
/// Failing that -- `peer` is behind a router we cannot see, or there is no
/// real peer at all, which is what every platform that does not report a
/// receiver's address gives us -- the answer is a guess, and it used to be
/// whichever non-loopback address `getifaddrs` listed first. On a phone
/// that is as likely to be the cellular interface as the Wi-Fi one, and a
/// cellular address handed to a Chromecast is a cast that hangs on a TCP
/// connect nobody ever times out. So the candidates are ranked instead (see
/// [`Reachability`]) and the best one wins, ties keeping enumeration order.
///
/// `None` only when the host has nothing but loopback and `peer` is not on
/// it either: a loopback address is never a guess worth making, since a
/// receiver reaching this process over loopback is not a receiver.
fn pick_host(interfaces: &[LocalIpv4Interface], peer: Ipv4Addr) -> Option<Ipv4Addr> {
    if let Some(iface) = interfaces.iter().find(|iface| same_subnet(iface, peer)) {
        return Some(iface.addr.ip);
    }
    interfaces
        .iter()
        .filter(|iface| !iface.addr.ip.is_loopback())
        .min_by_key(|iface| reachability(iface))
        .map(|iface| iface.addr.ip)
}

/// How plausibly a receiver on the same home network could reach one of our
/// addresses, best first -- the `Ord` derive *is* the ranking, and
/// [`pick_host`] takes the minimum.
///
/// The order encodes two judgements, the first outranking the second. An
/// interface the receiver cannot be behind -- a tunnel, a carrier link, a
/// container or VM bridge (see [`OFF_LAN_INTERFACE_PREFIXES`]) -- is no way
/// back to it whatever address it carries, so both of its ranks sit below
/// every ordinary interface. Within a kind, an RFC1918 address is what a
/// device on a home network has and anything else is a worse guess.
///
/// Both are heuristics, and deliberately only tie-breaks: an interface on
/// the receiver's own subnet never reaches this, and a host whose only
/// routable address is a cellular one still offers it rather than nothing.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Reachability {
    /// A private address on an ordinary interface: the Wi-Fi or Ethernet
    /// address of a machine sharing a home network with the receiver, and
    /// the rank this is expected to answer with nearly always.
    PrivateOnOrdinary,
    /// Any other address on an ordinary interface -- a public address, or a
    /// carrier-grade NAT or link-local one. A poor guess, but the link
    /// itself is one the receiver may genuinely share.
    OtherOnOrdinary,
    /// A private address on an interface the receiver is not behind: a
    /// VPN's `10.x` or `docker0`'s `172.17.x`, each of which looks exactly
    /// like a LAN address and is reachable only from inside its own tunnel
    /// or bridge. This is the tie the ranking mostly exists to break --
    /// without it, the address shape alone says these are as good as the
    /// Wi-Fi address and enumeration order decides.
    PrivateOffLan,
    /// Everything else, the case that started this: a cellular address,
    /// which no receiver has ever been able to reach.
    OtherOffLan,
}

fn reachability(iface: &LocalIpv4Interface) -> Reachability {
    match (is_off_lan(&iface.name), iface.addr.ip.is_private()) {
        (false, true) => Reachability::PrivateOnOrdinary,
        (false, false) => Reachability::OtherOnOrdinary,
        (true, true) => Reachability::PrivateOffLan,
        (true, false) => Reachability::OtherOffLan,
    }
}

/// Interface names a receiver on a home network cannot be behind, whatever
/// address they carry, by prefix.
///
/// Cellular: `rmnet` is Android's, `ccmni` MediaTek's, `pdp_ip` iOS's.
/// Tunnels: `tun`, `tap` and `wg`, plus macOS and iOS's `utun`, which the
/// `tun` prefix does not match -- a VPN is typically up for a whole
/// session. Container and VM bridges: `docker`, Docker's user-defined
/// `br-<id>`, the `veth` half of a container's pair, libvirt's `virbr` and
/// VirtualBox's `vboxnet`, every one of which carries an RFC1918 address on
/// a machine that also has a real LAN address. Interfaces this device hands
/// *out* rather than reaches a LAN through: Android's `ap0` hotspot, `p2p`
/// (Wi-Fi Direct) and `rndis` (USB tethering). And `dummy`, the kernel's
/// blackhole device. Loopback needs no prefix here -- it is excluded before
/// the ranking runs.
///
/// `br-` keeps its hyphen deliberately: a bare `br0` is as often a real
/// bridged LAN interface as a virtual one, and demoting the address a
/// bridged host actually answers on would be the mistake this list exists
/// to avoid.
///
/// Matching a name is coarse, which is why it only ever demotes: the cost
/// of being wrong is offering a second-choice address that also works, not
/// refusing one that does.
const OFF_LAN_INTERFACE_PREFIXES: [&str; 16] = [
    "rmnet", "ccmni", "pdp_ip", "tun", "utun", "tap", "wg", "docker", "br-", "veth", "virbr",
    "vboxnet", "ap0", "p2p", "rndis", "dummy",
];

fn is_off_lan(name: &str) -> bool {
    OFF_LAN_INTERFACE_PREFIXES.iter().any(|prefix| {
        name.get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    })
}

fn same_subnet(iface: &LocalIpv4Interface, peer: Ipv4Addr) -> bool {
    let mask = u32::from(iface.addr.netmask);
    // `if-addrs` substitutes `0.0.0.0` when the kernel reports no netmask
    // for an interface, and a zero mask matches *every* peer -- the
    // `0.0.0.0` a caller passes when it has no receiver address included.
    // Such an interface would win the outright match against anything at
    // all, taking the ranking and the loopback exclusion with it. An
    // interface with no netmask cannot be subnet-matched; let the ranking
    // have it.
    //
    // The cost was weighed, not overlooked: this also refuses an interface
    // that genuinely *is* on the peer's subnet and merely lost its netmask,
    // dropping a real match down into the ranking with the guesses. That is
    // the cheaper of the two mistakes by a distance. A demoted real match
    // still usually wins, because an interface that shares a receiver's
    // subnet is an ordinary private one and that is the ranking's top rank;
    // and when it loses, the answer is another address on this host, which
    // the receiver may well reach anyway. A zero mask honoured is the
    // opposite: it beats the interface that really shares the subnet, every
    // call, for every receiver, and hands out an address chosen by nothing
    // at all. Guessing is recoverable; a false match is not.
    if mask == 0 {
        return false;
    }
    u32::from(iface.addr.ip) & mask == u32::from(peer) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(name: &str, ip: [u8; 4], prefixlen: u8) -> LocalIpv4Interface {
        let mask = if prefixlen == 0 {
            0
        } else {
            u32::MAX << (32 - prefixlen)
        };
        LocalIpv4Interface {
            name: name.to_string(),
            addr: if_addrs::Ifv4Addr {
                ip: Ipv4Addr::from(ip),
                netmask: Ipv4Addr::from(mask),
                prefixlen,
                broadcast: None,
            },
        }
    }

    fn loopback() -> LocalIpv4Interface {
        iface("lo", [127, 0, 0, 1], 8)
    }

    fn wifi() -> LocalIpv4Interface {
        iface("wlan0", [192, 168, 1, 20], 24)
    }

    /// A carrier that hands out an RFC1918 address, so this case can only be
    /// decided by the interface name.
    fn cellular() -> LocalIpv4Interface {
        iface("rmnet_data0", [10, 82, 3, 4], 24)
    }

    /// The peer a caller passes when it has no receiver address at all --
    /// which is every platform that does not report one. It matches no
    /// interface's subnet, so it is the ranking's real workload rather than
    /// an edge case.
    const NO_PEER: Ipv4Addr = Ipv4Addr::UNSPECIFIED;

    /// The whole of the host pick, on the shapes of host it runs on.
    ///
    /// Everything here is a pure function over a list, so the awkward cases
    /// -- a phone on Wi-Fi and cellular at once, a machine with a VPN up or
    /// containers running, an interface the kernel gave no netmask, a host
    /// with nothing but loopback -- are built rather than found, and no case
    /// touches the network.
    #[test]
    fn pick_host_ranks_what_a_receiver_could_reach() {
        let cases: [(&str, Vec<LocalIpv4Interface>, Ipv4Addr, Option<Ipv4Addr>); 18] = [
            (
                "the interface on the peer's subnet wins over the ones that merely come first",
                vec![loopback(), iface("docker0", [172, 17, 0, 1], 16), wifi()],
                Ipv4Addr::new(192, 168, 1, 50),
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a peer on the bridge's subnet is answered with the bridge, ranking or not",
                vec![loopback(), iface("docker0", [172, 17, 0, 1], 16), wifi()],
                Ipv4Addr::new(172, 17, 0, 9),
                Some(Ipv4Addr::new(172, 17, 0, 1)),
            ),
            (
                "loopback is a subnet like any other for the match",
                vec![loopback(), wifi()],
                Ipv4Addr::LOCALHOST,
                Some(Ipv4Addr::LOCALHOST),
            ),
            (
                "a phone on Wi-Fi and cellular at once answers Wi-Fi, cellular listed first",
                vec![loopback(), cellular(), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "and answers Wi-Fi with the enumeration the other way round",
                vec![loopback(), wifi(), cellular()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a VPN's address looks like a LAN address and still loses to the LAN",
                vec![loopback(), iface("tun0", [10, 8, 0, 6], 24), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a macOS VPN too, whose name the `tun` prefix does not match",
                vec![loopback(), iface("utun0", [10, 8, 0, 6], 24), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a container bridge with no peer to match is the tie this ranking is for",
                vec![loopback(), iface("docker0", [172, 17, 0, 1], 16), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "and it loses listed last too, so it is the rank deciding and not the order",
                vec![loopback(), wifi(), iface("docker0", [172, 17, 0, 1], 16)],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a libvirt bridge is the same tie under another name",
                vec![loopback(), iface("virbr0", [192, 168, 122, 1], 24), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "the hotspot this device hands out is not a way back to the receiver",
                vec![loopback(), iface("ap0", [192, 168, 43, 1], 24), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "a MediaTek phone's cellular link ranks with every other carrier link",
                vec![loopback(), iface("ccmni0", [10, 50, 1, 2], 24), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "an ordinary interface wins even carrying a public address a tunnel's is private",
                vec![
                    iface("wg0", [10, 9, 0, 2], 24),
                    iface("eth0", [198, 51, 100, 7], 24),
                ],
                NO_PEER,
                Some(Ipv4Addr::new(198, 51, 100, 7)),
            ),
            (
                "a peer behind a router we cannot see still gets the one routable address",
                vec![loopback(), iface("eth0", [10, 1, 2, 3], 24)],
                Ipv4Addr::new(203, 0, 113, 5),
                Some(Ipv4Addr::new(10, 1, 2, 3)),
            ),
            (
                "a cellular address is a last resort, not a disqualification",
                vec![loopback(), cellular()],
                NO_PEER,
                Some(Ipv4Addr::new(10, 82, 3, 4)),
            ),
            (
                "nothing but loopback and a peer that is not on it: no URL to give",
                vec![loopback()],
                Ipv4Addr::new(203, 0, 113, 5),
                None,
            ),
            (
                "an interface the kernel gave no netmask is on nobody's subnet, not everybody's",
                vec![loopback(), iface("rmnet_data0", [10, 82, 3, 4], 0), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            (
                "loopback with no netmask does not swallow the match either",
                vec![iface("lo", [127, 0, 0, 1], 0), wifi()],
                NO_PEER,
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
        ];
        for (why, interfaces, peer, expected) in cases {
            assert_eq!(pick_host(&interfaces, peer), expected, "{why}");
        }
    }

    /// A listener bound to one address answers only there, so that address is
    /// the URL host whatever the peer is -- no interface pick at all.
    #[test]
    fn host_for_peer_uses_a_specific_bind_address_verbatim() {
        let bound = SocketAddr::from(([192, 168, 1, 20], 11471));
        assert_eq!(
            host_for_peer(bound, IpAddr::from([10, 0, 0, 9])),
            Some(IpAddr::from([192, 168, 1, 20]))
        );
    }

    /// A wildcard bind goes through the real interface enumeration. Loopback
    /// is the one interface every machine that runs this test has, so it is
    /// the only peer an assertion can be built on -- and a v4-mapped v6 peer,
    /// which is what a dual-stack socket reports for an IPv4 receiver, has to
    /// resolve to the same interface as the plain v4 one.
    #[test]
    fn host_for_peer_picks_a_local_interface_for_a_wildcard_bind() {
        let bound = SocketAddr::from(([0, 0, 0, 0], 11471));
        assert_eq!(
            host_for_peer(bound, IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
        assert_eq!(
            host_for_peer(bound, IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped())),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
    }
}
