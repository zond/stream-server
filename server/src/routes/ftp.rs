use crate::routes::compat;
use crate::state::AppState;
use axum::{
    Router,
    body::Body,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use suppaftp::FtpError;
use suppaftp::tokio::{AsyncFtpStream, AsyncRustlsConnector, AsyncRustlsFtpStream};
use suppaftp::types::FileType;
use tokio::io::AsyncRead;

#[derive(Debug, Deserialize)]
pub struct FtpQuery {
    pub lz: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtpStreamBody {
    pub ftp_url: String,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/{filename}", get(stream_ftp))
}

/// What an FTP URL with no user in it logs in as, which is what an FTP URL
/// in a stream almost always is. `curl` used `anonymous` with an email
/// address for the password; servers that ask for one accept anything.
const ANONYMOUS_USER: &str = "anonymous";
const ANONYMOUS_PASSWORD: &str = "anonymous";

/// The ports the two schemes mean when the URL names none: 21 for FTP, and
/// 990 for the implicit TLS `ftps://` asks for -- the same pair `curl`
/// applied, since this route used to be `curl`.
const DEFAULT_FTP_PORT: u16 = 21;
const DEFAULT_FTPS_PORT: u16 = 990;

async fn stream_ftp(Path(filename): Path<String>, Query(params): Query<FtpQuery>) -> Response {
    let lz_data = match params.lz {
        Some(lz) => lz,
        None => return (StatusCode::BAD_REQUEST, "Missing lz parameter").into_response(),
    };

    // Decompress lz-string (returns Vec<u16>)
    let utf16_data = match lz_str::decompress_from_encoded_uri_component(&lz_data) {
        Some(s) => s,
        None => return (StatusCode::BAD_REQUEST, "Failed to decompress lz data").into_response(),
    };
    let json_str = match String::from_utf16(&utf16_data) {
        Ok(s) => s,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UTF-16 in lz data").into_response(),
    };

    let body: FtpStreamBody = match serde_json::from_str(&json_str) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse FTP body: {}", e),
            )
                .into_response();
        }
    };

    // Only what the route is named for. This route is open to any loopback
    // caller -- on Android, every app on the device -- so the URL decides
    // what this server will go and fetch on a stranger's behalf. Back when
    // the fetch was a spawned `curl`, that meant every scheme curl has:
    // `file:///<data dir>/settings.json` streamed the proxy password to
    // whoever asked. The fetch speaks FTP and nothing else now, and the
    // check stays anyway, because a URL of another scheme reaching an FTP
    // client is a request nobody meant.
    let target = match ftp_target(&body.ftp_url) {
        Ok(target) => target,
        Err(refusal) => return (StatusCode::BAD_REQUEST, refusal).into_response(),
    };

    let data = match open_transfer(&target).await {
        Ok(data) => data,
        // The origin's failure, which is the caller's to hear as one. `curl`
        // could not report it: its exit code arrived long after the response
        // head had gone out, so a dead host, a refused login and a missing
        // file were all a `200` with nothing in it. The transfer is opened
        // here before a single header is written, so the three are a status.
        Err(e) => {
            tracing::debug!(url = %body.ftp_url, error = %e, "FTP transfer failed");
            return (
                StatusCode::BAD_GATEWAY,
                format!("FTP transfer failed: {}", e),
            )
                .into_response();
        }
    };

    let stream = tokio_util::io::ReaderStream::new(data);

    let content_type = mime_guess::from_path(&filename)
        .first_or_octet_stream()
        .to_string();

    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(
            axum::http::header::CONTENT_DISPOSITION,
            compat::content_disposition_inline(&filename),
        )
        .header("transferMode.dlna.org", compat::DLNA_TRANSFER_MODE)
        .header("contentFeatures.dlna.org", compat::DLNA_CONTENT_FEATURES)
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Where a request is going once it has been read: the socket to open,
/// whether it is wrapped in TLS, who to log in as, and what to `RETR`.
#[derive(Debug, PartialEq, Eq)]
struct FtpTarget {
    host: String,
    port: u16,
    secure: bool,
    user: String,
    password: String,
    path: String,
}

/// The FTP fetch `url` describes, or why there is not one.
///
/// **The scheme is matched by allow-list**, as it was when the fetch was a
/// spawned `curl` and the list was the only thing between an unauthenticated
/// caller and every scheme curl knows. It is not merely a leftover: it is
/// what keeps the route to what it is named for, and it refuses HTTP(S) too
/// -- a caller with an HTTP URL has `/proxy`, which is built to be handed
/// one. The `://` is required rather than taken from the parser, so
/// `ftp:host`, which has no host part, is refused here and not later.
///
/// Everything after the scheme is `url`'s parse and not a split of our own:
/// the host (an IPv6 literal keeps its brackets, which is what a socket
/// address wants), the port or the scheme's default, the credentials or
/// anonymous, and the path percent-decoded, because a file called
/// `The Film.mkv` is `The%20Film.mkv` in a URL and `The Film.mkv` to the
/// server.
fn ftp_target(url: &str) -> Result<FtpTarget, &'static str> {
    const REFUSAL: &str = "ftpUrl must be an ftp:// or ftps:// URL";

    let Some((scheme, _)) = url.split_once("://") else {
        return Err(REFUSAL);
    };
    let secure = if scheme.eq_ignore_ascii_case("ftp") {
        false
    } else if scheme.eq_ignore_ascii_case("ftps") {
        true
    } else {
        return Err(REFUSAL);
    };

    let parsed = url::Url::parse(url).map_err(|_| "ftpUrl is not a URL")?;
    let host = parsed.host_str().ok_or("ftpUrl names no host")?;
    // `Url::host_str` unwraps an IPv6 literal's brackets and a socket address
    // needs them back, so they are put back here rather than at the one call
    // site that formats the address.
    let host = match parsed.host() {
        Some(url::Host::Ipv6(addr)) => format!("[{addr}]"),
        _ => host.to_string(),
    };

    let (user, password) = if parsed.username().is_empty() {
        (ANONYMOUS_USER.to_string(), ANONYMOUS_PASSWORD.to_string())
    } else {
        (
            decoded(parsed.username()),
            decoded(parsed.password().unwrap_or_default()),
        )
    };

    Ok(FtpTarget {
        port: parsed.port().unwrap_or(if secure {
            DEFAULT_FTPS_PORT
        } else {
            DEFAULT_FTP_PORT
        }),
        host,
        secure,
        user,
        password,
        path: decoded(parsed.path()),
    })
}

/// A percent-decoded URL component, or the component as it stands when it
/// decodes to no valid UTF-8 -- there is nothing better to send, and an
/// error here would refuse a URL the server may well understand.
fn decoded(component: &str) -> String {
    urlencoding::decode(component)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| component.to_string())
}

/// Open the data connection for `target` and hand back the bytes.
///
/// The control connection is not returned: the transfer stream holds a share
/// of it, so it lives exactly as long as the body being read from it, and
/// dropping the client here sends no `QUIT`. A body the player abandons
/// mid-film therefore closes both sockets when the response is dropped,
/// which is what the killed `curl` did.
async fn open_transfer(target: &FtpTarget) -> Result<Pin<Box<dyn AsyncRead + Send>>, FtpError> {
    open_transfer_with(target, tls_config()).await
}

/// [`open_transfer`], with the anchors `ftps://` verifies against named
/// rather than compiled in -- which is what lets a test stand up an FTPS
/// server of its own and be believed by this client. Production has one
/// caller and it passes [`tls_config`].
async fn open_transfer_with(
    target: &FtpTarget,
    tls: Arc<rustls::ClientConfig>,
) -> Result<Pin<Box<dyn AsyncRead + Send>>, FtpError> {
    let addr = format!("{}:{}", target.host, target.port);
    if target.secure {
        let connector = AsyncRustlsConnector::from(tokio_rustls::TlsConnector::from(tls));
        // Implicit TLS from the first byte, on 990 unless the URL says
        // otherwise, which is what `ftps://` means to every client that has
        // the scheme -- `curl` included, which is what this replaced.
        let mut ftp =
            AsyncRustlsFtpStream::connect_secure_implicit(&addr, connector, &target.host).await?;
        ftp.login(&target.user, &target.password).await?;
        ftp.transfer_type(FileType::Binary).await?;
        Ok(Box::pin(ftp.retr_as_stream(&target.path).await?))
    } else {
        let mut ftp = AsyncFtpStream::connect(&addr).await?;
        ftp.login(&target.user, &target.password).await?;
        // Binary, always: the default is ASCII, which rewrites line endings
        // and would corrupt every byte of a film.
        ftp.transfer_type(FileType::Binary).await?;
        Ok(Box::pin(ftp.retr_as_stream(&target.path).await?))
    }
}

/// What the `ftps://` path verifies a certificate against: Mozilla's root
/// program as compiled into this binary, and nothing else.
///
/// The same anchors `enginefs::http_client_builder` gives every HTTPS client
/// in this workspace, from the same crate, so `/ftp` trusts what `/proxy`
/// trusts. It is not *quite* the same set: that builder adds the platform's
/// own store, and this does not, because those anchors are held there as
/// opaque `reqwest::Certificate`s that a `rustls::RootCertStore` cannot be
/// given. A device-installed CA is therefore trusted for HTTPS and not for
/// FTPS; `curl` trusted the platform store for both, so that is the one
/// thing this rewrite narrows, and it narrows towards the workspace's own
/// stated policy rather than away from it.
///
/// Built once for the life of the process: parsing ~150 roots per request
/// is what the laziness is for, and the config is immutable and shared.
fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        let mut roots = rustls::RootCertStore::empty();
        let (added, ignored) = roots
            .add_parsable_certificates(webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().cloned());
        tracing::debug!(added, ignored, "compiled-in roots for FTPS");
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("the aws-lc-rs provider supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth(),
        )
    });
    CONFIG.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    /// What the route is named for goes through; nothing else does. The
    /// fetch is an FTP client now, so another scheme reaching it is a
    /// request nobody meant -- and it was worse than that while the fetch
    /// was a spawned `curl`: `file://` read the settings file, and the
    /// proxy password in it, to any app on the device.
    #[test]
    fn only_ftp_and_ftps_are_fetched() {
        assert_eq!(
            ftp_target("ftp://host/dir/movie.mkv").unwrap(),
            FtpTarget {
                host: "host".into(),
                port: 21,
                secure: false,
                user: "anonymous".into(),
                password: "anonymous".into(),
                path: "/dir/movie.mkv".into(),
            }
        );
        assert!(
            ftp_target("FTPS://host/movie.mkv").is_ok(),
            "the scheme is case-insensitive"
        );

        assert!(ftp_target("file:///data/data/app/files/settings.json").is_err());
        assert!(ftp_target("http://example.com/movie.mkv").is_err());
        assert!(ftp_target("https://example.com/movie.mkv").is_err());
        assert!(ftp_target("gopher://host/1").is_err());
        assert!(
            ftp_target("host/movie.mkv").is_err(),
            "no scheme is not ftp"
        );
        assert!(
            ftp_target("ftp:host").is_err(),
            "and nor is a scheme without a host part"
        );
    }

    /// A URL that begins with `-` is refused by the scheme check, which is
    /// the whole of what is needed now: nothing is spawned, so there is no
    /// argument vector for a URL to be read as a flag in. (There used to be
    /// a `--` terminator in front of it for exactly that.)
    #[test]
    fn the_url_can_no_longer_be_an_option() {
        assert!(ftp_target("-o/tmp/owned").is_err());
        assert!(ftp_target("--config=/etc/curlrc").is_err());
        assert!(
            ftp_target("ftp://-host/x").is_ok(),
            "a host that starts with a dash is a host, not a flag"
        );
    }

    /// The parts of the URL that decide where the bytes come from, since
    /// this is what the spawned `curl` used to work out for itself.
    #[test]
    fn a_url_says_the_port_the_login_and_the_path() {
        let explicit = ftp_target("ftp://user:pa%20ss@host:2121/dir/The%20Film.mkv").unwrap();
        assert_eq!(explicit.port, 2121);
        assert_eq!(explicit.user, "user");
        assert_eq!(
            explicit.password, "pa ss",
            "credentials reach the server decoded"
        );
        assert_eq!(
            explicit.path, "/dir/The Film.mkv",
            "and so does the path: the server never sees a percent-escape"
        );

        assert_eq!(
            ftp_target("ftps://host/x").unwrap().port,
            990,
            "ftps means implicit TLS on 990 unless the URL says otherwise"
        );
        assert_eq!(ftp_target("ftps://host:21/x").unwrap().port, 21);
        assert_eq!(
            ftp_target("ftp://[::1]/x").unwrap().host,
            "[::1]",
            "an IPv6 literal keeps the brackets a socket address needs"
        );
    }

    fn lz(url: &str) -> Option<String> {
        let json = serde_json::json!({ "ftpUrl": url }).to_string();
        Some(lz_str::compress_to_encoded_uri_component(&json))
    }

    /// The refusal reaches the wire as a 400 before anything is opened: the
    /// handler has no state, so it is called as the router would.
    #[tokio::test]
    async fn a_non_ftp_url_is_refused_at_the_route() {
        for url in [
            "file:///data/data/app/files/settings.json",
            "http://example.com/movie.mkv",
        ] {
            let response = stream_ftp(
                Path("movie.mkv".to_string()),
                Query(FtpQuery { lz: lz(url) }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{url}");
        }
    }

    /// A one-transfer FTP server: enough of the protocol for one `RETR`,
    /// and a switch for the two ways a real one refuses.
    ///
    /// It exists because the transfer path had no test at all while it was
    /// a spawned `curl` -- a test would have had to put a real `curl` and a
    /// real FTP server on the machine running it. With the client in the
    /// process, the server is forty lines of `TcpStream`, and what this
    /// route does with a login it is refused, or a file that is not there,
    /// becomes something a test can state.
    #[derive(Clone, Copy)]
    enum Behaviour {
        Serve(&'static [u8]),
        RefuseLogin,
        NoSuchFile,
    }

    /// A plain FTP server on loopback. Answers the address to point an
    /// [`FtpTarget`] at.
    async fn ftp_server(behaviour: Behaviour) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (control, _) = listener.accept().await.unwrap();
            dialogue(control, behaviour, None).await;
        });
        addr
    }

    /// The same server inside implicit TLS, as `ftps://` means it: the
    /// control channel is encrypted from the first byte and so is the data
    /// connection. Answers the address and the anchor that verifies it.
    async fn ftps_server(
        behaviour: Behaviour,
    ) -> (
        std::net::SocketAddr,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        let (server_config, ca) = tls_fixture();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepting = acceptor.clone();
        tokio::spawn(async move {
            let (control, _) = listener.accept().await.unwrap();
            let control = accepting.accept(control).await.unwrap();
            dialogue(control, behaviour, Some(acceptor)).await;
        });
        (addr, ca)
    }

    /// The FTP conversation itself, over whatever the control channel turned
    /// out to be: a `TcpStream` for `ftp://`, a TLS stream for `ftps://`.
    ///
    /// `TYPE I` is not merely answered but **required**: a `RETR` that
    /// arrives without it is refused, because the default transfer mode is
    /// ASCII and a film fetched in ASCII is a film with every `\r\n` rewritten
    /// in it. A client that forgot to ask for binary should fail a test here
    /// rather than deliver a corrupt file in the field.
    async fn dialogue<S>(control: S, behaviour: Behaviour, tls: Option<tokio_rustls::TlsAcceptor>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        let mut control = BufReader::new(control);
        let mut line = String::new();
        let mut data: Option<TcpListener> = None;
        let mut binary = false;
        control.write_all(b"220 test server\r\n").await.unwrap();
        loop {
            line.clear();
            if control.read_line(&mut line).await.unwrap() == 0 {
                return;
            }
            let reply: &[u8] = if line.starts_with("USER") {
                match behaviour {
                    Behaviour::RefuseLogin => b"530 not logged in\r\n",
                    _ => b"331 password please\r\n",
                }
            } else if line.starts_with("PASS") {
                b"230 logged in\r\n"
            } else if line.starts_with("TYPE") {
                binary = line.contains('I');
                b"200 type set\r\n"
            } else if line.starts_with("PASV") {
                let passive = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = passive.local_addr().unwrap().port();
                data = Some(passive);
                let reply = format!(
                    "227 Entering Passive Mode (127,0,0,1,{},{})\r\n",
                    port / 256,
                    port % 256
                );
                control.write_all(reply.as_bytes()).await.unwrap();
                continue;
            } else if line.starts_with("RETR") {
                match behaviour {
                    Behaviour::NoSuchFile => b"550 no such file\r\n",
                    _ if !binary => b"451 ask for binary first\r\n",
                    Behaviour::Serve(payload) => {
                        control.write_all(b"150 here it comes\r\n").await.unwrap();
                        let (socket, _) = data.take().unwrap().accept().await.unwrap();
                        match &tls {
                            Some(acceptor) => {
                                let mut socket = acceptor.accept(socket).await.unwrap();
                                socket.write_all(payload).await.unwrap();
                                socket.shutdown().await.unwrap();
                            }
                            None => {
                                let mut socket = socket;
                                socket.write_all(payload).await.unwrap();
                                drop(socket);
                            }
                        }
                        control.write_all(b"226 transfer done\r\n").await.unwrap();
                        continue;
                    }
                    Behaviour::RefuseLogin => unreachable!("no RETR follows a refused login"),
                }
            } else if line.starts_with("QUIT") {
                b"221 bye\r\n"
            } else {
                b"502 not implemented\r\n"
            };
            control.write_all(reply).await.unwrap();
            if matches!(behaviour, Behaviour::RefuseLogin) && line.starts_with("USER") {
                return;
            }
        }
    }

    /// A chain the FTPS test can present and a client can verify: a CA that
    /// is a CA, and a leaf it issued for the loopback names. A self-signed
    /// certificate used as its own anchor is refused by rustls whatever it
    /// is trusted as, which is why this is two certificates and not one.
    fn tls_fixture() -> (
        rustls::ServerConfig,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let leaf_params =
            rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();

        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls::pki_types::PrivateKeyDer::try_from(leaf_key.serialize_der()).unwrap(),
        )
        .unwrap();
        (config, ca.der().clone())
    }

    /// A client config that trusts one extra anchor and nothing else, which
    /// is what makes the FTPS test's own server verifiable without giving
    /// the shipped [`tls_config`] a way to trust anything more.
    fn trusting(anchor: rustls::pki_types::CertificateDer<'static>) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(anchor).unwrap();
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        )
    }

    /// Where the tests above point a transfer: anonymous, plain, at the
    /// loopback server just started.
    fn target(addr: std::net::SocketAddr, path: &str) -> FtpTarget {
        FtpTarget {
            host: addr.ip().to_string(),
            port: addr.port(),
            secure: false,
            user: ANONYMOUS_USER.into(),
            password: ANONYMOUS_PASSWORD.into(),
            path: path.to_string(),
        }
    }

    /// The bytes the server sends are the bytes the reader gets, in order
    /// and whole -- the one thing this route exists to do, and the one
    /// thing nothing could assert while it was a subprocess.
    #[tokio::test]
    async fn a_transfer_delivers_the_file() {
        let addr = ftp_server(Behaviour::Serve(b"a film, in bytes")).await;
        assert_eq!(
            fetched(&target(addr, "/dir/film.mkv"), tls_config()).await,
            b"a film, in bytes"
        );
    }

    /// How long a test waits for a transfer before calling it a failure.
    ///
    /// **A bound and not a hedge.** Both ends of every transfer here are in
    /// this process and the payload is a handful of bytes, so any wait at
    /// all is a client and a server that disagree -- a plain client against
    /// a TLS server waits for a greeting that is never sent in cleartext,
    /// which is what this looks like when the secure branch is broken. A
    /// test that hangs is one CI cancels an hour later with nothing to read;
    /// this one says which transfer stopped.
    const TEST_TRANSFER_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

    /// The whole body of one transfer, within [`TEST_TRANSFER_BOUND`].
    async fn fetched(target: &FtpTarget, tls: Arc<rustls::ClientConfig>) -> Vec<u8> {
        let read = tokio::time::timeout(TEST_TRANSFER_BOUND, async {
            let mut data = open_transfer_with(target, tls).await.expect("a transfer");
            let mut read = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut data, &mut read)
                .await
                .expect("the payload");
            read
        })
        .await;
        read.expect("the transfer finished inside the bound")
    }

    /// The `ftps://` half, end to end: implicit TLS on the control channel
    /// from the first byte, the data connection wrapped in TLS as well, and
    /// the same bytes out the other side. It is the one path `curl` used to
    /// own that nothing here could see, and the client verifies the server's
    /// chain -- `trusting` gives it that server's anchor and nothing else,
    /// so a handshake that skipped verification would fail this too.
    #[tokio::test]
    async fn a_secure_transfer_delivers_the_file() {
        let (addr, anchor) = ftps_server(Behaviour::Serve(b"a film, encrypted")).await;
        let mut target = target(addr, "/dir/film.mkv");
        target.secure = true;
        // The name on the certificate, not the address: an FTPS client that
        // checked neither would pass with either.
        target.host = "localhost".into();
        assert_eq!(
            fetched(&target, trusting(anchor)).await,
            b"a film, encrypted"
        );
    }

    /// Both ways an origin refuses are an error before the response head is
    /// built, which is what lets the route answer with a status at all.
    #[tokio::test]
    async fn a_refused_login_and_a_missing_file_are_both_errors() {
        let refused = ftp_server(Behaviour::RefuseLogin).await;
        assert!(open_transfer(&target(refused, "/film.mkv")).await.is_err());

        let missing = ftp_server(Behaviour::NoSuchFile).await;
        assert!(open_transfer(&target(missing, "/film.mkv")).await.is_err());
        // Both are the route's `502`, which
        // `an_origin_that_refuses_is_a_bad_gateway` states for the second.
        // Together they are what `curl` could not tell anybody at all.
    }

    /// What the response head carries, which is what a player reads before
    /// it reads a byte: the type guessed from the name in the path, the
    /// inline disposition, and the two DLNA headers a renderer wants.
    #[tokio::test]
    async fn the_response_carries_the_headers_a_player_expects() {
        let addr = ftp_server(Behaviour::Serve(b"bytes")).await;
        let url = format!("ftp://{}/film.mkv", addr);
        let response = stream_ftp(
            Path("film.mkv".to_string()),
            Query(FtpQuery { lz: lz(&url) }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers["content-type"], "video/x-matroska");
        assert!(
            headers["content-disposition"]
                .to_str()
                .unwrap()
                .starts_with("inline"),
            "a film is played, not downloaded"
        );
        assert_eq!(headers["transferMode.dlna.org"], compat::DLNA_TRANSFER_MODE);
        assert_eq!(
            headers["contentFeatures.dlna.org"],
            compat::DLNA_CONTENT_FEATURES
        );
    }

    /// An origin that refuses reaches the caller as a `502`, where the
    /// spawned `curl` could only ever have sent a `200` with an empty body:
    /// its exit code arrived after the head had gone out.
    #[tokio::test]
    async fn an_origin_that_refuses_is_a_bad_gateway() {
        let addr = ftp_server(Behaviour::NoSuchFile).await;
        let url = format!("ftp://{}/film.mkv", addr);
        let response = stream_ftp(
            Path("film.mkv".to_string()),
            Query(FtpQuery { lz: lz(&url) }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
