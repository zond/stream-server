//! The one place that decides what this workspace's HTTPS trusts.
//!
//! Every `reqwest` client either crate builds starts at
//! [`http_client_builder`] -- the tracker-list fetch, the tracker prober, the
//! BEP-48 scrape, the DoH bootstrap resolver, `/proxy`, `/ftp`, the archive
//! and NZB fetches, and the `/get-https` call to Stremio's certificate API.
//! It lives in `enginefs` rather than `server` because `server` depends on
//! `enginefs` and not the reverse; librqbit has its own copy of this policy
//! (`librqbit::http_client_builder`) for the same reason, and the two say the
//! same thing.

/// A [`reqwest::ClientBuilder`] that trusts Mozilla's root program as
/// compiled into this binary, and nothing else.
///
/// The roots come from `webpki-root-certs`, which `rustls-platform-verifier`
/// already pulls into the graph, so this compiles nothing new.
///
/// # Why not the platform store
///
/// reqwest 0.13's rustls path constructs `rustls_platform_verifier::Verifier`
/// for any client that brings no roots of its own (reqwest 0.13.4
/// `src/async_impl/client.rs`, the `!config.tls_certs_only` arm of the
/// verifier `match`). `tls_certs_only` is the one builder state that takes
/// the plain `with_root_certificates` arm instead and never names that
/// verifier: rustls checks the chain itself. `add_root_certificate` is not an
/// escape -- extra roots take `Verifier::new_with_extra_roots`, still the
/// platform verifier.
///
/// On Android that verifier hands every handshake to Java's
/// `CertPathValidator` with revocation checking on and SOFT_FAIL, so for a
/// leaf certificate with no OCSP URL Android downloads the issuer's CRL and
/// parses it in Java. Measured in an app embedding librqbit, on a Chromecast
/// with Google TV: one tracker announce whose issuer's CRL held 116,196
/// entries cost about 400 MB of Java heap -- every announce, no result cache
/// -- which was 91% of the whole app's Java allocation and the GC storm
/// behind an ANR. This server is the same process on that device, and its own
/// HTTPS is on the same paths: the tracker list is refetched, trackers are
/// probed and scraped, and `/proxy` fetches whatever a stream URL points at.
/// On Linux the same verifier re-reads the system store from disk for every
/// client built, and this workspace builds several.
///
/// # The trade
///
/// A CA the user or their organisation installed on the device -- a
/// TLS-inspecting corporate proxy, mitmproxy while debugging -- no longer
/// verifies our HTTPS, and a root Mozilla admits after this build ships is
/// not trusted until the binary is rebuilt. There is no runtime opt-out.
///
/// For this server that is the right way round. Everything it speaks HTTPS to
/// is on the open internet -- public trackers, a tracker list on GitHub, DoH
/// resolvers, Stremio's certificate API, and the remote media URLs `/proxy`
/// and `/ftp` are handed -- and it has no intranet host to reach, so a
/// private CA on this path is far likelier to be interception than a need.
/// The traffic is worth protecting from exactly that: an announce URL carries
/// this peer's identity and the torrents it holds, `/get-https` carries the
/// user's Stremio auth key, and a proxied stream URL often carries a
/// credential of its own.
///
/// Two clients in this workspace deliberately do **not** start here, and
/// neither reaches the platform verifier either: `/proxy`'s unverified retry
/// client sets `danger_accept_invalid_certs`, which takes reqwest's
/// `NoVerifier` arm before any root store is consulted, and it exists so that
/// an origin with a broken chain can still be played after the verified
/// attempt has failed; and one proxy unit test builds a bare client to make a
/// connection that is refused before any TLS.
///
/// The other half of the policy -- that the platform verifier is never
/// constructed -- is `tests/tls_roots.rs`, which needs a test binary of its
/// own.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new().tls_certs_only(mozilla_roots())
}

/// Mozilla's roots as reqwest certificates. Each is a constant DER blob and
/// `from_der` only stores the bytes -- rustls parses them when the client is
/// built -- so this cannot fail.
fn mozilla_roots() -> impl Iterator<Item = reqwest::Certificate> {
    webpki_root_certs::TLS_SERVER_ROOT_CERTS
        .iter()
        .map(|der| reqwest::Certificate::from_der(der).expect("compiled-in root is DER"))
}

#[cfg(test)]
mod tests {
    /// That the compiled-in set is a real root program and not an empty or
    /// stubbed one. With `tls_certs_only` an empty set trusts nothing at all,
    /// so every HTTPS request in the process would fail -- a way for this to
    /// go wrong that no compiler check catches. Mozilla's program has held
    /// between 130 and 160 roots for years.
    #[test]
    fn the_compiled_in_roots_are_a_full_root_program() {
        let count = super::mozilla_roots().count();
        assert!((100..300).contains(&count), "{count} roots");
    }
}
