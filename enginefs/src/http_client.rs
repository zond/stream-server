//! The one place that decides what this workspace's HTTPS trusts.
//!
//! Every `reqwest` client either crate builds starts at
//! [`http_client_builder`] -- the tracker-list fetch, the tracker prober, the
//! BEP-48 scrape, the DoH bootstrap resolver, `/proxy`, `/ftp`, the archive
//! fetches, and the `/get-https` call to Stremio's certificate API.
//! It lives in `enginefs` rather than `server` because `server` depends on
//! `enginefs` and not the reverse; librqbit has its own copy of this policy
//! (`librqbit::http_client_builder`) for the same reason. The two are no
//! longer identical: librqbit's is written to be upstreamable and trusts the
//! compiled-in roots alone, while this one also reads the platform's store.
//! Nothing librqbit fetches leaves the open internet, so the anchors it is
//! missing are ones its traffic never needs.

/// A [`reqwest::ClientBuilder`] that trusts Mozilla's root program as
/// compiled into this binary, plus whatever the platform itself trusts,
/// and nothing else.
///
/// The compiled-in roots come from `webpki-root-certs`, which
/// `rustls-platform-verifier` already pulls into the graph, so they compile
/// nothing new; the platform's own anchors are read by `platform_roots`,
/// once, at the first HTTPS request rather than at every handshake.
///
/// # Why not the platform verifier
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
/// **The cost was never the anchors, it was doing the verification in Java
/// per handshake.** So the anchors come back and the Java call does not:
/// `platform_roots` reads the same store the verifier would have consulted,
/// once, and rustls checks chains against it in Rust. That is why this is a
/// union rather than a replacement, and why the trade below is small.
///
/// # The trade
///
/// Revocation is not checked. rustls verifies a chain against these anchors
/// and consults no CRL and no OCSP responder, where Android's verifier at
/// least asked (and SOFT_FAILed when it could not reach one). Against a CA
/// that has had to revoke, this is weaker.
///
/// The union is also, by construction, more permissive than either set alone:
/// a root that one program has distrusted and the other has not is trusted
/// here. That matters only against an adversary who can get a certificate
/// from a CA in good standing with either program, which is a different
/// threat from the one this path faces.
///
/// What it buys is that nothing silently stops verifying. Everything this
/// server speaks HTTPS to is on the open internet -- public trackers, a
/// tracker list on GitHub, DoH resolvers, Stremio's certificate API, and the
/// remote media URLs `/proxy` and `/ftp` are handed -- and that traffic is
/// worth protecting: an announce URL carries this peer's identity and the
/// torrents it holds, `/get-https` carries the user's Stremio auth key, and a
/// proxied stream URL often carries a credential of its own. A device or
/// organisation that has installed its own CA keeps working, which is what
/// stops a failure here from being routed around.
///
/// One client in this workspace deliberately does **not** start here: a proxy
/// unit test builds a bare client to make a connection that is refused before
/// any TLS. `/proxy` used to have a second one -- an unverified retry that
/// took `danger_accept_invalid_certs` when a chain would not verify, and
/// remembered that origin for the life of the process -- which made every
/// word above advisory. It is gone; see `routes::proxy`.
///
/// The other half of the policy -- that the platform verifier is never
/// constructed -- is `tests/tls_roots.rs`, which needs a test binary of its
/// own.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new().tls_certs_only(
        mozilla_roots()
            .chain(platform_roots().iter().cloned())
            .chain(test_roots()),
    )
}

/// Extra trust anchors for tests that need an HTTPS origin that *verifies*.
///
/// `server`'s proxy tests stand up a real TLS listener, and several of them
/// are about what a chain carries once it is established -- the credential
/// rules for a redirect that steps down to cleartext, and for the lines of a
/// playlist. A chain that will not verify never reaches the behaviour under
/// test, and there is deliberately no other way in: what this module trusts
/// is otherwise decided entirely by what the platform and Mozilla ship.
///
/// Behind a feature the server enables only as a dev-dependency, so it does
/// not exist in a release build. Set once, before the first client is built:
/// every proxy test builds its fixture before it makes a request, and the
/// fixture is where this is called, so the ordering holds without a lock. A
/// second call is ignored rather than racing the first.
#[cfg(feature = "test-roots")]
#[doc(hidden)]
pub fn trust_roots_for_tests(pems: Vec<Vec<u8>>) {
    let _ = TEST_ROOTS.set(pems);
}

#[cfg(feature = "test-roots")]
static TEST_ROOTS: std::sync::OnceLock<Vec<Vec<u8>>> = std::sync::OnceLock::new();

#[cfg(feature = "test-roots")]
fn test_roots() -> impl Iterator<Item = reqwest::Certificate> {
    TEST_ROOTS
        .get()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|pem| reqwest::Certificate::from_pem(pem).ok())
}

#[cfg(not(feature = "test-roots"))]
fn test_roots() -> impl Iterator<Item = reqwest::Certificate> {
    std::iter::empty()
}

/// Mozilla's roots as reqwest certificates. Each is a constant DER blob and
/// `from_der` only stores the bytes -- rustls parses them when the client is
/// built -- so this cannot fail.
fn mozilla_roots() -> impl Iterator<Item = reqwest::Certificate> {
    webpki_root_certs::TLS_SERVER_ROOT_CERTS
        .iter()
        .map(|der| reqwest::Certificate::from_der(der).expect("compiled-in root is DER"))
}

/// The platform's own trust anchors, read once for the life of the process.
///
/// Empty is a valid answer and never an error: it means this build trusts the
/// compiled-in roots alone, which is what every release before this one did.
/// A store that cannot be read is logged and skipped rather than propagated,
/// because the alternative -- refusing to build a client -- would take the
/// whole server down over a directory the compiled-in roots make optional.
fn platform_roots() -> &'static [reqwest::Certificate] {
    static ROOTS: std::sync::LazyLock<Vec<reqwest::Certificate>> =
        std::sync::LazyLock::new(load_platform_roots);
    &ROOTS
}

/// Android keeps its system CAs as one PEM file per hashed subject, world
/// readable, in two places: Conscrypt's APEX module, which is authoritative
/// from Android 14 and is what the platform verifier itself consults, and the
/// older `/system` copy, which is what earlier releases have. Both are read
/// and the result deduplicated, because a device may have either or both and
/// the union is the set the platform would have accepted.
///
/// **User-installed CAs are deliberately not read.** They live in
/// `/data/misc/user/0/cacerts-added`, which an app cannot read, and an app
/// does not trust them anyway unless its manifest opts in with a
/// `networkSecurityConfig` -- Android's default has excluded them since API
/// 24. Reading them here would make this server trust more than the platform
/// it runs on, which is not what "what the platform trusts" should mean.
#[cfg(target_os = "android")]
fn load_platform_roots() -> Vec<reqwest::Certificate> {
    const DIRS: [&str; 2] = [
        "/apex/com.android.conscrypt/cacerts",
        "/system/etc/security/cacerts",
    ];

    let mut seen = std::collections::HashSet::new();
    let mut roots = Vec::new();
    for dir in DIRS {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::debug!("no platform CA store at {dir}: {e}");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(pem) = std::fs::read(&path) else {
                continue;
            };
            // Each file is one PEM certificate followed by the human-readable
            // dump `keytool` writes, so a bundle parse would stop at the same
            // one certificate a single parse takes.
            match reqwest::Certificate::from_pem(&pem) {
                // Deduplicated on the file's bytes rather than the parsed
                // certificate, which is opaque: the two directories hold the
                // same file under the same hashed name where they overlap.
                Ok(cert) if seen.insert(pem) => roots.push(cert),
                Ok(_) => {}
                Err(e) => tracing::debug!("skipping {}: {e}", path.display()),
            }
        }
    }
    tracing::debug!("{} platform CA roots", roots.len());
    roots
}

/// Everywhere else the OS store is whatever `rustls-native-certs` knows how
/// to find -- `/etc/ssl` and the `SSL_CERT_FILE`/`SSL_CERT_DIR` overrides on
/// unix, the system stores on Windows and macOS. It is already in the graph
/// under `rustls-platform-verifier`, so this compiles nothing new either.
#[cfg(not(target_os = "android"))]
fn load_platform_roots() -> Vec<reqwest::Certificate> {
    let result = rustls_native_certs::load_native_certs();
    for error in &result.errors {
        tracing::debug!("reading the platform CA store: {error}");
    }
    result
        .certs
        .iter()
        .filter_map(|der| reqwest::Certificate::from_der(der).ok())
        .collect()
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

    /// That reading the platform store is never fatal and never a panic. It
    /// is allowed to find nothing -- a container with no `/etc/ssl`, an
    /// Android image with neither cacerts directory -- and the compiled-in
    /// roots carry the process in that case.
    #[test]
    fn the_platform_store_is_optional() {
        let _ = super::platform_roots();
    }
}
