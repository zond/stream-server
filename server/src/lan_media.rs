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
//! one. It serves [`crate::media_router`] alone -- the control routes are not
//! mounted on it at all, not even behind the bearer middleware, so an
//! unknown-path `404` is the strongest answer the LAN can get out of them and
//! there is no token to guess, leak or brute-force. Both listeners share one
//! [`AppState`], so a stream the LAN pulls uses the same engines, piece cache
//! and settings as one the loopback listener serves.
//!
//! It is off unless an embedder configures [`ServerConfig::lan_media_addr`],
//! and [`ServerHandle::set_lan_media`] starts and stops it at runtime so it
//! exists only for as long as a cast session does. The `lanMediaEnabled`
//! setting is the operator's veto over both.
//!
//! [`ServerConfig::embedded`]: crate::ServerConfig::embedded
//! [`ServerConfig::lan_media_addr`]: crate::ServerConfig::lan_media_addr
//! [`ServerHandle::set_lan_media`]: crate::ServerHandle::set_lan_media

use crate::state::AppState;
use anyhow::Context;
use if_addrs::Ifv4Addr;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use url::Url;

/// The LAN media listener's control block, held by [`AppState`] so both
/// [`crate::run`] (which starts it at boot when an address is configured) and
/// [`crate::ServerHandle`] (which toggles it per cast session) reach the same
/// one.
pub struct LanMedia {
    /// Where a listener binds when it is started, from
    /// [`crate::ServerConfig::lan_media_addr`]. `None` means the embedder
    /// never configured one and the listener can never run.
    configured_addr: Option<SocketAddr>,
    /// The running listener, if any. A single mutex serialises start and
    /// stop, so two concurrent toggles cannot both bind.
    running: tokio::sync::Mutex<Option<Running>>,
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
    ///
    /// Fails when no address is configured, or when the bind fails.
    pub async fn start(&self, state: &AppState) -> anyhow::Result<SocketAddr> {
        let mut running = self.running.lock().await;
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
    /// **Shutdown is an abort, not a drain.** The serving task is aborted and
    /// awaited, so by the time this returns the socket is closed, the port is
    /// free and every response that was still streaming over the LAN has been
    /// dropped mid-body. That is the intended behaviour: this is called when
    /// a cast session ends (or when the operator revokes `lanMediaEnabled`),
    /// and a receiver that is still pulling bytes is exactly what should stop.
    /// Draining instead would mean waiting for a movie-length response to
    /// finish before the LAN surface actually closed.
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
    }

    /// The base URL to hand a receiver at `peer`, e.g.
    /// `http://192.168.1.7:11471/`. `None` when the listener is not running.
    ///
    /// The host is the local interface that can actually reach `peer` (see
    /// [`pick_host`]), because a receiver is told a URL it has to connect
    /// back to: on a host with a LAN and a VPN or container bridge, the
    /// first interface in the list is regularly the wrong one.
    pub async fn base_url_for(&self, peer: IpAddr) -> Option<Url> {
        let bound = self.bound_addr().await?;
        let host = host_for_peer(bound, peer)?;
        // `SocketAddr`'s Display brackets an IPv6 host, which is the spelling
        // a URL authority needs.
        Url::parse(&format!("http://{}/", SocketAddr::new(host, bound.port()))).ok()
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
/// The interface whose subnet contains `peer` if there is one -- that is the
/// address `peer` can route back to. Otherwise the first non-loopback
/// address, as a best effort for a peer behind a router we cannot see;
/// `None` only when the host has nothing but loopback and `peer` is not on
/// it either.
fn pick_host(interfaces: &[Ifv4Addr], peer: Ipv4Addr) -> Option<Ipv4Addr> {
    interfaces
        .iter()
        .find(|iface| same_subnet(iface, peer))
        .or_else(|| interfaces.iter().find(|iface| !iface.ip.is_loopback()))
        .map(|iface| iface.ip)
}

fn same_subnet(iface: &Ifv4Addr, peer: Ipv4Addr) -> bool {
    let mask = u32::from(iface.netmask);
    u32::from(iface.ip) & mask == u32::from(peer) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(ip: [u8; 4], prefixlen: u8) -> Ifv4Addr {
        let mask = if prefixlen == 0 {
            0
        } else {
            u32::MAX << (32 - prefixlen)
        };
        Ifv4Addr {
            ip: Ipv4Addr::from(ip),
            netmask: Ipv4Addr::from(mask),
            prefixlen,
            broadcast: None,
        }
    }

    /// The interface a receiver can route back to wins over the ones that
    /// merely come first -- a VPN or container bridge is regularly ahead of
    /// the LAN in the enumeration.
    #[test]
    fn pick_host_prefers_the_interface_on_the_peers_subnet() {
        let interfaces = [
            iface([127, 0, 0, 1], 8),
            iface([172, 17, 0, 1], 16),
            iface([192, 168, 1, 20], 24),
        ];
        assert_eq!(
            pick_host(&interfaces, Ipv4Addr::new(192, 168, 1, 50)),
            Some(Ipv4Addr::new(192, 168, 1, 20))
        );
        assert_eq!(
            pick_host(&interfaces, Ipv4Addr::new(172, 17, 0, 9)),
            Some(Ipv4Addr::new(172, 17, 0, 1))
        );
        assert_eq!(
            pick_host(&interfaces, Ipv4Addr::LOCALHOST),
            Some(Ipv4Addr::LOCALHOST),
            "loopback is a subnet like any other, and the only one a test can rely on"
        );
    }

    /// No interface shares the peer's subnet (it is behind a router): the
    /// first routable address is the best guess, and loopback is never it.
    #[test]
    fn pick_host_falls_back_to_the_first_non_loopback_interface() {
        let interfaces = [iface([127, 0, 0, 1], 8), iface([10, 1, 2, 3], 24)];
        assert_eq!(
            pick_host(&interfaces, Ipv4Addr::new(203, 0, 113, 5)),
            Some(Ipv4Addr::new(10, 1, 2, 3))
        );
        assert_eq!(
            pick_host(&[iface([127, 0, 0, 1], 8)], Ipv4Addr::new(203, 0, 113, 5)),
            None,
            "nothing but loopback and a peer that is not on it: no URL to give"
        );
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
