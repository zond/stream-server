use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header, response::Builder},
    response::{IntoResponse, Response},
    routing::any,
};
use dashmap::DashSet;
use futures_util::StreamExt;
use reqwest::{Client, Method};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use url::Url;

/// Lazily-built, process-wide reqwest client for the proxy route: the one
/// that **verifies** the origin's certificate, which is every request until
/// one fails.
///
/// `Client::builder().build()` can fail (e.g. if the TLS backend can't be
/// initialized), so building it once at startup-on-first-use and reusing it
/// avoids both a per-request `.unwrap()` panic and the cost of rebuilding a
/// client for every proxied request.
static HTTP_CLIENT: OnceLock<Option<Client>> = OnceLock::new();

/// The same client with verification off, built only if some host actually
/// needs it. See [`UNVERIFIED_HOSTS`] for why it exists at all.
static INSECURE_HTTP_CLIENT: OnceLock<Option<Client>> = OnceLock::new();

/// Origins whose certificate this process could not verify -- `scheme`,
/// host and port, as [`Url::origin`] serializes them. The request that
/// discovered one is retried unverified; a later request for the same
/// origin goes straight to the unverified client, so a stream pays the
/// failed handshake once rather than once per range.
///
/// The whole origin, not the host: verification is a property of a TLS
/// endpoint, and a host serving a broken certificate on `:8443` says
/// nothing about the one it serves on `:443`. Keyed by host alone, a
/// failure at either turned verification off for both -- and for the
/// plain-HTTP `http://host` that is not even the same protocol.
///
/// This route was built with `danger_accept_invalid_certs(true)` from its
/// first commit, commented "Parity with rejectUnauthorized: false" -- it is
/// inherited from the closed-source `server.js` proxy this file was ported
/// from, not a response to any host we ever measured. Nothing in the history
/// names a host that needs it. Meanwhile the client that used to fetch a
/// remote stream was mpv; now it is this, so the flag stopped being about a
/// rarely-used route and became how every remote stream is fetched.
///
/// Verifying everything and letting the broken hosts fail would break
/// streams that play today, and we cannot say which ones. So: verify, and
/// when a certificate is the reason a fetch failed, retry that host once
/// without verification and remember it, at WARN, by name. Be plain about
/// what that is worth -- an on-path attacker can produce a certificate
/// error as easily as a misconfigured CDN can, so this stops nothing it
/// could not also trigger. What it buys is that the downgrade is per host,
/// visible in the log, and enumerable: today's blanket silence cannot tell
/// us which hosts to scope it to, and this can.
static UNVERIFIED_ORIGINS: LazyLock<DashSet<String>> = LazyLock::new(DashSet::new);

/// Every origin this process has downgraded, for a test to assert against.
///
/// [`UNVERIFIED_ORIGINS`] is the only record that a downgrade happened at
/// all: over plain HTTP the unverified client behaves identically to the
/// verified one, so marking the wrong endpoint is invisible from outside --
/// which is how a plain-HTTP host came to be marked at all. Exported
/// doc-hidden so the test that pins it can see what was written down.
#[doc(hidden)]
pub fn unverified_origins() -> Vec<String> {
    UNVERIFIED_ORIGINS
        .iter()
        .map(|origin| origin.clone())
        .collect()
}

/// How [`UNVERIFIED_ORIGINS`] is keyed: `https://host:port`, with a default
/// port left off, which is what [`Url::origin`] serializes.
fn origin_key(url: &Url) -> String {
    url.origin().ascii_serialization()
}

/// Whether a URL's path names a playlist by its extension.
///
/// Case-insensitively, which the reference is not: `path.extname()` against
/// a list of two lowercase literals does not see `/live/master.M3U8` at
/// all, and only its content-type arm catches one. There is no reason to
/// inherit that -- a filename's case is the origin's spelling, not a
/// statement about the format.
fn names_a_playlist(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".m3u8") || path.ends_with(".m3u")
}

tokio::task_local! {
    /// The URL the verifying client is about to connect to, for the
    /// duration of one fetch.
    ///
    /// reqwest attributes a connect failure to the URL the request
    /// *started* at -- `Error::url` is the one we handed it, however many
    /// redirects it followed since -- so the error alone cannot say which
    /// handshake failed. Its redirect policy can: it is asked before every
    /// hop, and it writes the hop down here. A task local because the
    /// policy belongs to the process-wide client while the answer belongs
    /// to one request, and because the policy runs inside the very future
    /// the handler awaits, on this task.
    static ATTEMPTING: Arc<Mutex<Option<Url>>>;
}

fn http_client() -> Option<&'static Client> {
    HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
                .redirect(reqwest::redirect::Policy::custom(|attempt| {
                    // Note where we are going before we go there (see
                    // [`ATTEMPTING`]), then decide it the way the default
                    // policy would -- the hop limit and the loop detection
                    // are not ours to reinvent.
                    let _ = ATTEMPTING.try_with(|attempting| {
                        if let Ok(mut attempting) = attempting.lock() {
                            *attempting = Some(attempt.url().clone());
                        }
                    });
                    reqwest::redirect::Policy::default().redirect(attempt)
                }))
                .build()
                .map_err(|e| tracing::error!("Failed to build proxy HTTP client: {e}"))
                .ok()
        })
        .as_ref()
}

fn insecure_http_client() -> Option<&'static Client> {
    INSECURE_HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| tracing::error!("Failed to build unverified proxy HTTP client: {e}"))
                .ok()
        })
        .as_ref()
}

/// Whether a failed fetch failed *because the peer's certificate would not
/// verify*, which is the only failure the unverified retry can help with --
/// a refused connection or a DNS miss must stay an error.
///
/// **Matched by type, never by prose.** This used to be a case-insensitive
/// search for the word "certificate" over every `Display` in the chain, and
/// reqwest writes the request URL into that text: measured with no TLS
/// anywhere in the picture, one request for
/// `http://127.0.0.1:1/certificate-of-authenticity.mkv` -- a refused
/// connection -- put `127.0.0.1` into [`UNVERIFIED_HOSTS`] for the life of
/// the process. A filename could turn certificate verification off.
///
/// The error that actually says so is a
/// `rustls::Error::InvalidCertificate`, which covers every reason a chain
/// can be rejected (unknown issuer, expired, not valid for the name) and is
/// exactly the set `danger_accept_invalid_certs` waives. Measured, it sits
/// under reqwest's connect error inside **two** nested `io::Error`s -- and
/// `source()` alone walks straight past it, because `io::Error::source`
/// delegates to the inner error's source instead of yielding the inner
/// error itself. So every node is asked for its `get_ref` as well as its
/// `source`.
///
/// The rustls type comes from `tokio_rustls`, which re-exports the same
/// `rustls` reqwest links (one entry in the lockfile, and the whole
/// workspace is on one TLS backend on purpose). If that ever stopped being
/// true the downcast would simply stop matching, and the failure would be
/// a stream that is not retried rather than a host quietly downgraded.
fn is_certificate_error(error: &(dyn std::error::Error + 'static)) -> bool {
    use tokio_rustls::rustls;

    // Both links out of each node are followed, and the walk is bounded
    // rather than trusting an error chain not to loop.
    let mut pending: Vec<&(dyn std::error::Error + 'static)> = vec![error];
    let mut visited = 0;
    while let Some(error) = pending.pop() {
        visited += 1;
        if visited > 32 {
            break;
        }
        if matches!(
            error.downcast_ref::<rustls::Error>(),
            Some(rustls::Error::InvalidCertificate(_))
        ) {
            return true;
        }
        if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(|error| error.get_ref())
        {
            pending.push(inner);
        }
        if let Some(source) = error.source() {
            pending.push(source);
        }
    }
    false
}

/// The header names `r=` may not set, because they describe how *this*
/// response is framed rather than what it contains -- and framing it is
/// hyper's business, decided from the body actually being written.
///
/// They are the same three the origin's own values are never relayed
/// under, for the same reason, and `r=` is if anything the more dangerous
/// source: it is addon metadata, and an addon that says
/// `r=Content-Length:1` in front of a two-gigabyte film panics the
/// connection task in a debug build ("payload claims content-length of ...,
/// custom content-length header claims 1") and, in a release build, leaves
/// the player waiting for bytes that will never come.
const UNFRAMEABLE_RESPONSE_HEADERS: [&str; 3] =
    ["content-length", "transfer-encoding", "connection"];

/// Applies the `r=` custom response headers to a response builder,
/// validating each name/value pair first so that a malicious or malformed
/// header (e.g. containing a newline) can never poison the builder's
/// internal error state. Invalid pairs -- and every name in
/// [`UNFRAMEABLE_RESPONSE_HEADERS`] -- are skipped and logged at debug
/// level rather than propagated.
///
/// They **replace**, which is the whole of what `r=` is for. Appending
/// them, as this did, left the origin's own header in place beside the
/// override and a client reading the first of two `content-type`s got the
/// origin's -- which is exactly the value stremio-core sends `r=` to
/// correct. `Builder::header` appends; `HeaderMap::insert` takes the name
/// over entirely, which is why the map is reached through `headers_mut`
/// rather than the builder's own method. A builder already in an error
/// state has no map to reach, and skipping is right there too: it is about
/// to become a 502 (see [`finalize_response`]).
fn apply_custom_response_headers(
    mut builder: Builder,
    custom_response_headers: &BTreeMap<String, String>,
) -> Builder {
    for (name, value) in custom_response_headers {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(header_name), Ok(header_value)) => {
                if UNFRAMEABLE_RESPONSE_HEADERS.contains(&header_name.as_str()) {
                    tracing::debug!(
                        name = %header_name,
                        value = %value,
                        "Skipping a framing header from r= proxy param: this response is \
                         framed by the body being written, not by the addon"
                    );
                    continue;
                }
                if let Some(headers) = builder.headers_mut() {
                    headers.insert(header_name, header_value);
                }
            }
            _ => {
                tracing::debug!(
                    name = %name,
                    value = %value,
                    "Skipping invalid custom response header from r= proxy param"
                );
            }
        }
    }
    builder
}

/// The `h=` custom request headers as a header map, validated the same way
/// and for the same reason as the response ones.
///
/// A map rather than a series of `RequestBuilder::header` calls because
/// those *append*: an addon's `h=User-Agent:...` used to be sent alongside
/// the player's own, two `user-agent` headers on one request, and which of
/// them the origin honoured was its business. `RequestBuilder::headers`
/// replaces the name outright, which is what an override means.
fn custom_request_headers(overrides: &BTreeMap<String, String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in overrides {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(header_name), Ok(header_value)) => {
                headers.insert(header_name, header_value);
            }
            _ => {
                tracing::debug!(
                    name = %name,
                    value = %value,
                    "Skipping invalid custom request header from h= proxy param"
                );
            }
        }
    }
    headers
}

/// Finishes building a response, turning a builder error (which can no
/// longer happen for headers we control, but is handled defensively for any
/// other builder failure) into a 502 instead of panicking via `.unwrap()`.
fn finalize_response(builder: Builder, body: axum::body::Body) -> Response {
    match builder.body(body) {
        Ok(resp) => resp.into_response(),
        Err(e) => {
            tracing::error!("Failed to build proxy response: {e}");
            (StatusCode::BAD_GATEWAY, "Proxy response error").into_response()
        }
    }
}

/// The proxy's own parameters, in whichever URL shape carried them: the
/// target (`d=`), the request headers to send with it (`h=`), the response
/// headers to send back (`r=`), and the client's name for the player
/// reading the stream (`p=`).
///
/// One parser for both shapes. The Core format spells them in the path
/// segment before the target's path, the query format in the request's own
/// query, and until this struct existed only the Core format could express
/// `h=`/`r=` at all -- which meant [`rewrite_playlist`], which writes the
/// query format, could not carry an authenticated playlist's headers into
/// the segments it rewrote. Measured: the playlist fetched `200`, every
/// segment `403`, the origin logging `auth=[]`.
///
/// `BTreeMap` rather than `HashMap` because the order the headers come back
/// out in is written into every line of every rewritten playlist, and a
/// body that differs between two identical requests is not something to
/// hand a caching player.
#[derive(Debug, Default, PartialEq, Eq)]
struct ProxyParams {
    /// The target URL, as `d=` spelled it. In the Core format this is the
    /// origin, and the request path is appended to it.
    target: String,
    /// `h=Name:Value` -- sent to the origin, replacing whatever we would
    /// otherwise have forwarded under that name.
    request_headers: BTreeMap<String, String>,
    /// `r=Name:Value` -- sent back to the player, replacing whatever the
    /// origin said under that name.
    response_headers: BTreeMap<String, String>,
    /// `p=<token>` -- never sent to the origin. See [`crate::proxy_streams`].
    player_token: Option<String>,
}

impl ProxyParams {
    /// Reads the four parameters out of one `application/x-www-form-
    /// urlencoded` string, whether it came off the path segment or the
    /// query. Anything else in it belongs to the target and is ignored
    /// here.
    fn parse(query: &str) -> Self {
        let mut params = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "d" => params.target = value.into_owned(),
                // Header format "Name:Value", for the request and the
                // response respectively.
                "h" => {
                    if let Some((name, value)) = value.split_once(':') {
                        params
                            .request_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "r" => {
                    if let Some((name, value)) = value.split_once(':') {
                        params
                            .response_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "p" if !value.is_empty() => params.player_token = Some(value.into_owned()),
                _ => {}
            }
        }
        params
    }

    /// Everything a rewritten playlist line has to carry besides its own
    /// target, spelled as query parameters: `h=` and `p=`.
    ///
    /// `h=` because a segment of an authenticated stream needs the same
    /// authorization the playlist needed -- without it the playlist fetched
    /// `200` and every segment `403`. `p=` because the segment read has to
    /// belong to the same player as the playlist that named it: closing an
    /// HLS player has to break the read that is actually in flight, and
    /// that is a segment, never the playlist.
    ///
    /// **`r=` is deliberately not here.** It is a response-header override
    /// for *the resource the caller named*, and the caller named a
    /// playlist: stremio-core sends `r=Content-Type:application/x-mpegurl`
    /// for an HLS stream, so copying it onto every line labelled the
    /// segments and the AES keys as playlists too. mpv was handed MPEG-TS
    /// under `application/x-mpegurl` and a 16-byte key under it as well.
    ///
    /// The reference does copy it, on every same-origin line and every
    /// absolute path, because its virtual root is the caller's whole opts
    /// string -- and there it is worse than here: it computes `isPlaylist`
    /// *after* merging `r=` into the response headers, so a segment fetched
    /// through such a line is itself classified a playlist and run through
    /// the line rewriter. Its own cross-origin branch drops `r=` (`newOpts`
    /// has only `d` and `h`), which is the half worth keeping.
    fn carried(&self) -> String {
        let mut carried = String::new();
        for (name, value) in &self.request_headers {
            carried.push_str(&format!(
                "&h={}",
                urlencoding::encode(&format!("{name}:{value}"))
            ));
        }
        if let Some(token) = &self.player_token {
            carried.push_str(&format!("&p={}", urlencoding::encode(token)));
        }
        carried
    }
}

/// Every shape `/proxy` answers, at absolute paths and merged rather than
/// nested under the prefix.
///
/// `nest("/proxy", ...)` cannot express all three. It registers the prefix
/// itself plus a `{*tail}` wildcard beneath it, and a wildcard matches at
/// least one character -- so `/proxy/` matches neither and is a router-level
/// `404` before any handler runs. That is exactly the URL the query format
/// has, and exactly the URL [`rewrite_playlist`] writes into every line of
/// every playlist this route rewrites: an HLS stream fetched through the
/// proxy handed the player a playlist whose every segment 404ed.
pub fn router() -> Router<AppState> {
    Router::new()
        // The original JS uses /proxy/:opts/:pathname*
        // We can use a wildcard capturing the whole path.
        .route("/proxy/{*rest}", any(proxy_handler))
        // The query format, with or without the trailing slash.
        .route("/proxy", any(proxy_root_handler))
        .route("/proxy/", any(proxy_root_handler))
}

/// `/proxy/?d=<url>`: the whole target in the query, nothing in the path.
///
/// The one format the wildcard above cannot express, and the one this
/// server writes itself -- [`rewrite_playlist`] turns every line of every
/// playlist into it. Its query carries the same proxy parameters the Core
/// format carries in its path segment (`d=`, `h=`, `r=`, `p=`), read by
/// the same parser, because a rewritten segment URL has to be able to say
/// everything the playlist's own URL said.
pub async fn proxy_root_handler(
    State(state): State<AppState>,
    raw_query: axum::extract::RawQuery,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    proxy(state, None, raw_query.0, headers, method).await
}

/// The Core path format, read from the URI rather than from the router's
/// capture, because the capture is percent-*decoded*.
///
/// Both [`Path`] and `RawPathParams` decode what the wildcard matched, and
/// the target's path is not ours to decode: `%2F` became a path separator,
/// `%3F` began a query and everything from a `%23` on was read as a
/// fragment and lost. Measured end to end, `https://host/a%2Fb/film.mkv`
/// reached the origin as `GET /a/b/film.mkv` -- a signed link whose path
/// segment carries a base64 signature gets a 403, and a file named with a
/// `#` gets a 404. The URI's own path is the target as it came off the wire, so
/// what the caller encoded is what the origin is asked for. It also means
/// the `d=`/`h=`/`r=` segment is decoded exactly once, by
/// `form_urlencoded` -- a header value carrying a `%` or a `&` used to be
/// decoded twice and lose its meaning.
pub async fn proxy_handler(
    State(state): State<AppState>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    // The route this handler serves is `/proxy/{*rest}`, so the prefix is
    // always there; an empty rest could only come of a router change, and it
    // answers 400 the way any unparseable target does.
    let rest = uri.path().strip_prefix("/proxy/").unwrap_or_default();
    proxy(state, Some(rest.to_string()), raw_query, headers, method).await
}

/// `rest` is what the path held after `/proxy/`, and *that is what decides
/// the format*: `None` -- nothing in the path -- is the query format
/// (`/proxy/?d=<url>`), anything else is the Core path format
/// (`/proxy/d=<origin>&h=.../<path>`).
///
/// It used to be decided by asking whether the request's query had a `d`
/// parameter, which is a name the target URL may own too. A Core-format
/// request for `/proxy/d=<encoded>/film.mkv?d=1&t=2` took `d="1"` as the
/// whole target, failed to parse it and answered `400 Invalid target URL`;
/// worse, a `d` value that happened to parse as a URL would have been
/// fetched *instead of* the target the caller named. The path shape cannot
/// be spoofed by the target's own query, so the path shape decides.
async fn proxy(
    state: AppState,
    rest: Option<String>,
    raw_query: Option<String>,
    headers: HeaderMap,
    method: Method,
) -> Response {
    // Porting the logic from express_805.js
    // Format 1: ?d=URL (standard)
    // Format 2: /<query_params>/<path> (Core) where query_params contains d=ORIGIN&h=HEADER&r=RESPONSE_HEADER

    let is_path_format = rest.is_some();
    let params = match &rest {
        None => ProxyParams::parse(raw_query.as_deref().unwrap_or_default()),
        Some(rest) => {
            // The Core path format: /proxy/d=...&h=.../path/to/file. The
            // segment before the first slash is the proxy's own parameters,
            // everything after it is the target's path.
            let (query_seg, path_seg) = match rest.split_once('/') {
                Some((q, p)) => (q, p),
                None => (rest.as_str(), ""),
            };
            let mut params = ProxyParams::parse(query_seg);
            if params.target.is_empty() {
                // Fallback: assume whole rest is the URL (legacy/simple proxy)
                params.target = rest.clone();
            } else if !path_seg.is_empty() {
                // `d=` is the origin, the rest of the path is the file on it.
                if !params.target.ends_with('/') {
                    params.target.push('/');
                }
                params.target.push_str(path_seg);
            }
            params
        }
    };
    // The client's name for the player this stream is for, if it minted one
    // (`p=`). It is ours, not the target's: it never travels to the origin,
    // and it is what `POST /proxy-streams/{token}/close` addresses. See
    // [`crate::proxy_streams`].
    let player_token = params.player_token.clone();

    let mut url = match Url::parse(&params.target) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target URL").into_response(),
    };

    if is_path_format && let Some(q) = raw_query {
        url.set_query(Some(&q));
    }

    // A token that has been closed is not given another stream. ffmpeg
    // reconnects through the URL it already has -- token and all -- so
    // without this the close is a stutter rather than an end: measured,
    // three closes on one live reader produced three fresh origin fetches
    // at the offsets the closes interrupted. `410 Gone` because that is
    // exactly what happened: this stream was here and was deliberately
    // ended. A `404` would read as a target that never existed and send a
    // client looking for a typo in its URL. The check comes before the
    // fetch, so a refusal costs the origin nothing -- but it is only the
    // cheap half of the refusal: the fetch below takes as long as the origin
    // takes, and the token can be retired while it does. The registration
    // asks again (see the `attach` below).
    if let Some(token) = player_token.as_deref()
        && state.proxy_streams.is_closed(token)
    {
        tracing::debug!(
            token = %token,
            "refusing a proxied read for a player token that was closed"
        );
        return (
            StatusCode::GONE,
            "This player's stream was closed by its client",
        )
            .into_response();
    }

    // The origin is fetched verified unless a previous request for it
    // failed on its certificate (see [`UNVERIFIED_ORIGINS`]). This asks
    // about the URL we are about to fetch; a downgrade recorded for a
    // redirect *target* is not visible from here, so such a chain pays its
    // one failed handshake again on every request rather than once. That is
    // the honest cost of not guessing where a redirect will go.
    let known_unverified = UNVERIFIED_ORIGINS.contains(&origin_key(&url));
    let client = match if known_unverified {
        insecure_http_client()
    } else {
        http_client()
    } {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Proxy client unavailable",
            )
                .into_response();
        }
    };

    let custom_request_headers = custom_request_headers(&params.request_headers);
    let build_request = |client: &Client| {
        let mut req_builder = client.request(method.clone(), url.clone());

        // What the player asked for, forwarded as it asked for it.
        // `connection` and `transfer-encoding` are deliberately absent: both
        // describe the framing of one hop, and this is a new hop -- reqwest
        // frames its own request, and a `transfer-encoding: chunked` copied
        // from a bodyless player request describes a body that is not there.
        let allowed_req_headers = [
            "accept",
            "accept-language",
            "range",
            "if-range",
            "user-agent",
        ];

        for name in allowed_req_headers {
            if let Some(value) = headers.get(name) {
                req_builder = req_builder.header(name, value);
            }
        }

        // `accept-encoding` is answered here rather than forwarded. This
        // client has no gzip/brotli/deflate feature, so it decodes nothing,
        // and a playlist arrives as bytes we cannot rewrite -- while the
        // player's own `accept-encoding: gzip` invited exactly that. Asking
        // for `identity` says what we can actually take. An origin that
        // compresses anyway is still relayed honestly: `content-encoding`
        // travels back with the body it describes (see the relayed-body
        // headers below).
        req_builder = req_builder.header(header::ACCEPT_ENCODING, "identity");

        // The `h=` overrides last, and replacing rather than adding to what
        // the player sent: an override that leaves the original in place
        // is not one.
        req_builder = req_builder.headers(custom_request_headers.clone());
        req_builder
    };

    // The fetch, with a place for the redirect policy to write down where it
    // went (see [`ATTEMPTING`]); the initial URL is the answer when it wrote
    // nothing, because then there was no redirect to move it.
    let attempting: Arc<Mutex<Option<Url>>> = Arc::new(Mutex::new(None));
    let sent = ATTEMPTING
        .scope(attempting.clone(), build_request(client).send())
        .await;
    let response = match sent {
        Ok(resp) => resp,
        Err(e) if !known_unverified && is_certificate_error(&e) => {
            // The host whose handshake failed, which is not necessarily the
            // one we asked for. This used to mark the URL the request
            // started at, so an `https` -> `https` redirect to a bad
            // certificate permanently downgraded the *good* host and left
            // the bad one verified, and a plain-HTTP host that redirected to
            // one was downgraded for a certificate it never presented. The
            // redirect policy knows better, and says so.
            let failed_at = attempting
                .lock()
                .ok()
                .and_then(|attempting| attempting.clone())
                .unwrap_or_else(|| url.clone());
            // Only a TLS endpoint has a certificate to waive. Nothing else
            // can produce this error, so this is a guard rather than a
            // case: if it ever fires, the note above was not written and
            // the honest answer is to fail rather than downgrade a guess.
            if failed_at.scheme() != "https" {
                tracing::warn!(
                    url = %failed_at,
                    error = %e,
                    "a certificate failed verification, but not at an https URL we can \
                     name; not retrying"
                );
                return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response();
            }
            // One retry, for this endpoint only, and say so once per
            // process.
            let downgraded = origin_key(&failed_at);
            tracing::warn!(
                origin = %downgraded,
                error = %e,
                "certificate verification failed; retrying this origin unverified for \
                 the life of the process"
            );
            UNVERIFIED_ORIGINS.insert(downgraded);
            let Some(client) = insecure_http_client() else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Proxy client unavailable",
                )
                    .into_response();
            };
            match build_request(client).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e))
                        .into_response();
                }
            }
        }
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response(),
    };

    let status = response.status();
    let res_headers = response.headers().clone();
    // Where the body actually came from. reqwest follows redirects, and a
    // playlist's relative lines are relative to the URL it *arrived* at:
    // rewriting against the URL we asked for sends every segment back to
    // the host that redirected us, and to its directory, which is a
    // CDN-to-edge `302` -- the ordinary HLS deployment -- breaking every
    // segment of every stream served that way. It is also the honest name
    // for what we fetched, so the playlist test below asks it too.
    let fetched_url = response.url().clone();

    let content_type = res_headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    // The content type is matched with the case folded away, because the
    // spelling that matters most is not lowercase: Apple writes
    // `application/x-mpegURL`, it is the spelling stremio-core sends in
    // `r=` and the one this repo's README uses, and a case-sensitive
    // `contains("mpegurl")` sees none of it. The reference lowercases here
    // too (`(responseHeaders["content-type"]||"").toLowerCase()
    // .includes("mpegurl")`) -- and that arm is the only reason it copes
    // with a playlist whose URL does not end `.m3u8`.
    // Both URLs are asked, because either one alone has a blind spot. The
    // URL the *caller* named is the one an HLS player knows it asked for,
    // and it is the only evidence left when a redirect lands on an
    // extension-less URL an indifferent origin labels
    // `application/octet-stream` -- testing the fetched path alone stopped
    // rewriting that stream at all. The URL the body *came from* is the one
    // that catches the other direction, a caller naming an extension-less
    // URL that redirects to a `.m3u8`. The reference tests only the
    // pre-redirect path (its `dest` is the router's, untouched by the
    // redirect loop) and leans on its content-type arm for the rest.
    let is_playlist = names_a_playlist(&url)
        || names_a_playlist(&fetched_url)
        || content_type.to_ascii_lowercase().contains("mpegurl");

    // A body under a content coding we cannot decode is a body we must not
    // rewrite: the lines are not text yet. We relay it whole instead --
    // its segment URLs then point straight at the origin, which loses the
    // `h=` request headers, so say so rather than serving the player a
    // rewritten playlist made of compressed bytes.
    let content_encoding = res_headers
        .get(header::CONTENT_ENCODING)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let encoded_body =
        !content_encoding.is_empty() && !content_encoding.eq_ignore_ascii_case("identity");
    // Only a body that is actually a playlist is rewritten as one, and a
    // status code is half of what says so. A 404's error page served at a
    // `.m3u8` URL was being rewritten line by line and handed back as a
    // playlist of fabricated proxy URLs -- an origin's "Not found" became a
    // segment list. And a `HEAD` has no body to rewrite at all: the rewrite
    // measured the empty one and answered `Content-Length: 0`, so a player
    // asking how big the resource is was told nothing is there. Both fall
    // through to the plain relay, which is what they always should have
    // been.
    let rewriting_playlist =
        is_playlist && !encoded_body && status.is_success() && method != Method::HEAD;
    if is_playlist && encoded_body {
        tracing::warn!(
            content_encoding = %content_encoding,
            url = %fetched_url,
            "relaying a compressed playlist unrewritten; its segments will bypass the proxy"
        );
    }

    let mut res_builder = Response::builder().status(status);

    // What the origin said about the *resource*: true of whatever we send
    // back, because none of it describes the bytes on this hop.
    let resource_res_headers = ["content-type", "server", "date"];
    for name in resource_res_headers {
        if let Some(value) = res_headers.get(name) {
            res_builder = res_builder.header(name, value);
        }
    }

    // What the origin said about *its own body*: only true of a body we hand
    // on byte for byte. A rewritten playlist is a different body, and the
    // origin's framing copied onto it is a lie hyper catches -- with
    // `Content-Length` the connection task panics ("payload claims
    // content-length of 180, custom content-length header claims 82"), with
    // `Transfer-Encoding: chunked` it closes having written nothing, and only
    // a close-delimited origin survived by accident. That was every proxied
    // HLS stream failing to play. `Accept-Ranges`, `Content-Range`, `ETag`
    // and `Last-Modified` go with it: they all describe the entity at the
    // origin, and a client that acted on them -- ranging into the rewritten
    // playlist, or caching it under the origin's tag -- would be acting on
    // the wrong bytes. `content-encoding` belongs to the same set and for
    // the same reason -- it names the coding of *these* bytes, and dropping
    // it (as this route used to) hands the player gzip labelled as identity.
    // `connection` and `transfer-encoding` are relayed in neither branch:
    // framing this response is hyper's job, not the origin's.
    let relayed_body_res_headers = [
        "accept-ranges",
        "content-encoding",
        "content-length",
        "content-range",
        "last-modified",
        "etag",
    ];
    if !rewriting_playlist {
        for name in relayed_body_res_headers {
            if let Some(value) = res_headers.get(name) {
                res_builder = res_builder.header(name, value);
            }
        }
    }

    // Apply custom response headers (Core format), validated so a malformed
    // r= param can never poison the response builder.
    res_builder = apply_custom_response_headers(res_builder, &params.response_headers);

    // CORS headers
    res_builder = res_builder
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "*");

    if rewriting_playlist {
        // Every line of the playlist is rewritten to come back through this
        // proxy, so the whole body has to be in hand before any of it is
        // sent. A body we could not read is a playlist we cannot rewrite:
        // saying so beats handing the player an empty one that parses as a
        // stream with no segments.
        //
        // It is read through the registry, exactly as a media body is, and
        // that is the point: a live-HLS player refreshing its playlist
        // against an origin that has stopped answering is a read wedged in
        // here, and this branch used to return before `attach` ever ran --
        // so the one read this feature exists for was the one read it could
        // not reach. Closing the player's token now breaks this read too.
        let Some(mut chunks) = state
            .proxy_streams
            .attach(player_token.clone(), response.bytes_stream())
        else {
            tracing::debug!(
                token = player_token.as_deref().unwrap_or_default(),
                "a player token was closed while its playlist was being fetched"
            );
            return (
                StatusCode::GONE,
                "This player's stream was closed by its client",
            )
                .into_response();
        };
        let mut body = Vec::new();
        while let Some(chunk) = chunks.next().await {
            match chunk {
                Ok(chunk) => body.extend_from_slice(&chunk),
                Err(e) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!("Proxy could not read the playlist: {e}"),
                    )
                        .into_response();
                }
            }
        }
        // A playlist is UTF-8 by specification, and a byte that is not is a
        // byte we cannot rewrite around either way.
        let body = String::from_utf8_lossy(&body);
        let rewritten = rewrite_playlist(&body, &fetched_url, &params.carried());
        // The framing of the body we built, measured on that body.
        res_builder = res_builder.header(header::CONTENT_LENGTH, rewritten.len().to_string());
        return finalize_response(res_builder, axum::body::Body::from(rewritten));
    }

    // Registered under the client's token, so the client can end this exact
    // read rather than waiting out a timeout meant for a slow swarm -- or
    // refused, if the close landed while the origin was still thinking. Same
    // `410` and for the same reason as the check above: the stream was asked
    // for, and its client ended it before a byte of it arrived.
    let Some(stream) = state
        .proxy_streams
        .attach(player_token.clone(), response.bytes_stream())
    else {
        tracing::debug!(
            token = player_token.as_deref().unwrap_or_default(),
            "a player token was closed while its origin was being fetched"
        );
        return (
            StatusCode::GONE,
            "This player's stream was closed by its client",
        )
            .into_response();
    };
    finalize_response(res_builder, axum::body::Body::from_stream(stream))
}

/// `POST /proxy-streams/{token}/close`: end every proxied stream the client
/// marked with `token`, and say how many that was.
///
/// It also retires the token, which is the half that makes it stick: the
/// closed reads break, and any later `/proxy` request bearing the same `p=`
/// is answered `410 Gone` instead of being given a fresh stream. Without
/// that, ffmpeg's `reconnect=1` re-fetches through the URL it already has
/// and playback carries on.
///
/// A **control** route -- bearer token, loopback listener, and never on the
/// LAN media listener, which serves no control route at all: the ability to
/// cut another device's playback is not something to hand the network. The
/// same operation is [`crate::ServerHandle::close_proxy_streams`], through
/// this same function, so an embedder needs no HTTP client for it.
pub async fn close_proxy_streams(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let closed = state.proxy_streams.close(&token);
    tracing::debug!(closed, "closing proxied streams by player token");
    Json(serde_json::json!({ "closed": closed }))
}

/// Rewrites every URL in a playlist to come back through this proxy,
/// carrying `carried` -- [`ProxyParams::carried`], the `h=`/`p=` the
/// playlist's own URL arrived with -- into each one.
///
/// Both of them, because a segment is not a different stream: it needs the
/// same request headers to be allowed at the origin, and the same player
/// token, since closing an HLS player has to close the segment read that is
/// actually in flight rather than the playlist read that is not. What it
/// does *not* need is the caller's `r=`; see [`ProxyParams::carried`].
fn rewrite_playlist(body: &str, base_url: &Url, carried: &str) -> String {
    let mut rewritten = String::new();
    for line in body.lines() {
        if line.is_empty() {
            rewritten.push('\n');
            continue;
        }
        if line.starts_with("#") {
            // Handle URI="url" in tags like #EXT-X-MEDIA
            if let Some(start) = line.find("URI=\"") {
                let rest = &line[start + 5..];
                if let Some(end) = rest.find("\"") {
                    let uri = &rest[..end];
                    let absolute_uri = if uri.contains("://") {
                        uri.to_string()
                    } else {
                        base_url
                            .join(uri)
                            .map(|u: Url| u.to_string())
                            .unwrap_or_else(|_| uri.to_string())
                    };
                    let proxy_uri =
                        format!("/proxy/?d={}{carried}", urlencoding::encode(&absolute_uri));
                    rewritten.push_str(&line[..start + 5]);
                    rewritten.push_str(&proxy_uri);
                    rewritten.push_str(&rest[end..]);
                    rewritten.push('\n');
                    continue;
                }
            }
            rewritten.push_str(line);
            rewritten.push('\n');
        } else {
            // It's a URL
            let absolute_uri = if line.contains("://") {
                line.to_string()
            } else {
                base_url
                    .join(line)
                    .map(|u: Url| u.to_string())
                    .unwrap_or_else(|_| line.to_string())
            };
            let proxy_uri = format!("/proxy/?d={}{carried}", urlencoding::encode(&absolute_uri));
            rewritten.push_str(&proxy_uri);
            rewritten.push('\n');
        }
    }
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("http://example.com/streams/master.m3u8").unwrap()
    }

    fn proxied(target: &str) -> String {
        format!("/proxy/?d={}", urlencoding::encode(target))
    }

    #[test]
    fn relative_segment_is_joined_against_base_and_wrapped() {
        let body = "seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/streams/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_http_line_is_wrapped_without_double_joining() {
        let body = "http://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_https_line_is_wrapped_without_double_joining() {
        let body = "https://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("https://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn ext_x_media_uri_is_rewritten_and_other_attributes_are_preserved() {
        let body = concat!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
            "URI=\"audio/en.m3u8\",DEFAULT=YES,AUTOSELECT=YES\n"
        );
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            concat!(
                "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
                "URI=\"{}\",DEFAULT=YES,AUTOSELECT=YES\n"
            ),
            proxied("http://example.com/streams/audio/en.m3u8")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn ext_x_media_with_absolute_uri_is_wrapped_without_double_joining() {
        let body = concat!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
            "URI=\"https://cdn.example.org/audio/en.m3u8\"\n"
        );
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",URI=\"{}\"\n",
            proxied("https://cdn.example.org/audio/en.m3u8")
        );
        assert_eq!(rewritten, expected);
    }

    /// Pins current behavior: EXT-X-KEY's URI is rewritten through the same
    /// generic `URI="..."` handling as EXT-X-MEDIA, so encryption key
    /// fetches ARE proxied (not left pointing at the origin directly). If
    /// that's ever intentionally changed, update this test alongside it.
    #[test]
    fn ext_x_key_uri_is_proxied_like_other_uri_attributes() {
        let body = "#EXT-X-KEY:METHOD=AES-128,URI=\"key/enc.key\",IV=0x0123456789abcdef\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}\",IV=0x0123456789abcdef\n",
            proxied("http://example.com/streams/key/enc.key")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn root_relative_path_resolves_against_origin() {
        let body = "/videos/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/videos/seg-0.ts"))
        );
    }

    /// The player's token travels into every line the rewrite writes, so a
    /// segment fetch belongs to the same player as the playlist that named
    /// it -- otherwise closing an HLS player would close its playlist read
    /// and leave the segment in flight, which is the read that matters.
    #[test]
    fn the_player_token_is_carried_into_every_rewritten_line() {
        let body = "#EXT-X-KEY:METHOD=AES-128,URI=\"key/enc.key\"\nseg-0.ts\n";
        let params = ProxyParams::parse("d=whatever&p=player+one");
        let rewritten = rewrite_playlist(body, &base(), &params.carried());
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}&p=player%20one\"\n{}&p=player%20one\n",
            proxied("http://example.com/streams/key/enc.key"),
            proxied("http://example.com/streams/seg-0.ts")
        );
        assert_eq!(rewritten, expected);
    }

    /// Both URL shapes carry the same four parameters, and one parser reads
    /// them: the Core format spells them in its path segment, the query
    /// format -- the one the rewrite writes -- in the query.
    #[test]
    fn both_url_shapes_are_read_by_the_same_parser() {
        let expected = ProxyParams {
            target: "http://example.com/film.mkv".to_string(),
            request_headers: BTreeMap::from([(
                "Authorization".to_string(),
                "Bearer x:y".to_string(),
            )]),
            response_headers: BTreeMap::from([(
                "Content-Type".to_string(),
                "video/mp4".to_string(),
            )]),
            player_token: Some("player one".to_string()),
        };
        let query = "d=http%3A%2F%2Fexample.com%2Ffilm.mkv\
                     &h=Authorization%3ABearer%20x%3Ay\
                     &r=Content-Type%3Avideo%2Fmp4\
                     &p=player%20one";
        assert_eq!(ProxyParams::parse(query), expected);
        // A header value's own colons belong to the value, and a `%`-encoded
        // separator is decoded exactly once.
        assert_eq!(
            expected
                .request_headers
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer x:y")
        );
    }

    /// The whole point of [`ProxyParams::carried`]: a segment fetched
    /// through a rewritten line is asked for with the headers the
    /// playlist's own URL carried. Without this an authenticated HLS stream
    /// served its playlist and 403ed every segment.
    ///
    /// And `r=` stays behind. The playlist is what the caller labelled; a
    /// segment carrying that label is MPEG-TS announced as a playlist.
    #[test]
    fn the_headers_are_carried_into_every_rewritten_line_too() {
        let params = ProxyParams::parse(
            "d=whatever&h=Authorization%3ABearer+abc&r=Content-Type%3Avideo%2Fmp4&p=one",
        );
        let rewritten = rewrite_playlist("seg-0.ts\n", &base(), &params.carried());
        assert_eq!(
            rewritten,
            format!(
                "{}&h=Authorization%3ABearer%20abc&p=one\n",
                proxied("http://example.com/streams/seg-0.ts")
            )
        );
    }

    /// Several of them, and the order is the same every time: the rewritten
    /// playlist is a body a player may cache and re-fetch, and two spellings
    /// of the same playlist would be two.
    #[test]
    fn carried_parameters_come_out_in_a_stable_order() {
        let params = ProxyParams::parse("d=whatever&h=B%3A2&h=A%3A1&r=Y%3Ayes&r=X%3Ano&p=t");
        assert_eq!(params.carried(), "&h=A%3A1&h=B%3A2&p=t");
    }

    #[test]
    fn comment_and_blank_lines_are_left_unchanged() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n\n#EXT-X-TARGETDURATION:10\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(rewritten, body);
    }

    /// `r=` is an override: the origin's value for that name goes, rather
    /// than the two of them being sent together for the client to choose
    /// between -- and a client reading the first of two `content-type`s
    /// read the origin's, which is the value `r=` exists to correct.
    #[test]
    fn a_custom_response_header_replaces_the_origin_s_own() {
        let builder = Response::builder()
            .status(200)
            .header("content-type", "application/octet-stream");
        let overrides = BTreeMap::from([("Content-Type".to_string(), "video/mp4".to_string())]);
        let response = finalize_response(
            apply_custom_response_headers(builder, &overrides),
            axum::body::Body::empty(),
        );

        assert_eq!(
            response.headers().get_all("content-type").iter().count(),
            1,
            "one value, not the override queued behind the origin's"
        );
        assert_eq!(response.headers().get("content-type").unwrap(), "video/mp4");
    }

    /// The same for `h=`, where the collision is with what the player sent.
    #[test]
    fn a_custom_request_header_replaces_what_the_player_sent() {
        let overrides = BTreeMap::from([("User-Agent".to_string(), "addon/1".to_string())]);
        let mut request = HeaderMap::new();
        request.insert("user-agent", HeaderValue::from_static("mpv/0.41"));
        // What `RequestBuilder::headers` does with the map this builds.
        for (name, value) in custom_request_headers(&overrides) {
            request.insert(name.expect("a named header"), value);
        }
        assert_eq!(request.get_all("user-agent").iter().count(), 1);
        assert_eq!(request.get("user-agent").unwrap(), "addon/1");
    }

    /// `r=` names a header on the resource, not on the hop. The three that
    /// frame the hop are dropped from it exactly as they are dropped from
    /// the origin's own headers -- an addon that says its film is one byte
    /// long panics hyper in debug and hangs the player in release.
    #[test]
    fn a_custom_response_header_cannot_reframe_the_response() {
        let overrides = BTreeMap::from([
            ("Content-Length".to_string(), "1".to_string()),
            ("Transfer-Encoding".to_string(), "chunked".to_string()),
            ("Connection".to_string(), "close".to_string()),
            ("Content-Type".to_string(), "video/mp4".to_string()),
        ]);
        let response = finalize_response(
            apply_custom_response_headers(Response::builder().status(200), &overrides),
            axum::body::Body::empty(),
        );

        for name in UNFRAMEABLE_RESPONSE_HEADERS {
            assert!(
                !response.headers().contains_key(name),
                "{name} frames the response, and r= does not get to say"
            );
        }
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "video/mp4",
            "and the header r= exists for still arrives"
        );
    }

    #[test]
    fn apply_custom_response_headers_skips_invalid_name_and_value() {
        let mut headers = BTreeMap::new();
        // Valid pair: should be applied.
        headers.insert("X-Proxy-Ok".to_string(), "yes".to_string());
        // Invalid value: embedded CR/LF must never reach the header map.
        headers.insert(
            "X-Evil".to_string(),
            "bad\r\nInjected-Header: true".to_string(),
        );
        // Invalid name: space is not a legal header-name character.
        headers.insert("Bad Name".to_string(), "value".to_string());

        let builder = apply_custom_response_headers(Response::builder().status(200), &headers);
        let response = builder.body(axum::body::Body::empty()).unwrap();

        assert_eq!(response.headers().get("x-proxy-ok").unwrap(), "yes");
        assert!(response.headers().get("x-evil").is_none());
        assert!(!response.headers().contains_key("injected-header"));
    }

    #[test]
    fn malicious_r_header_value_does_not_panic_and_yields_a_response() {
        // Simulates parsing r=X-Evil:bad%0d%0aInjected:1 from the proxy URL:
        // once percent-decoded and split on ':', the value carries a raw
        // newline. Feeding this straight into a response builder (the old
        // `.header(name, value)` + `.unwrap()` code path) would poison the
        // builder and panic at `.body()`. The validated path must not.
        let mut custom_response_headers = BTreeMap::new();
        custom_response_headers.insert("X-Evil".to_string(), "bad\r\nInjected: true".to_string());

        let builder = apply_custom_response_headers(
            Response::builder().status(200),
            &custom_response_headers,
        );
        let response = finalize_response(builder, axum::body::Body::empty());

        // No panic occurred (we got here), and the handler degrades to a
        // clean response rather than crashing the whole process.
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-evil").is_none());
    }

    #[test]
    fn finalize_response_returns_502_on_builder_error_instead_of_panicking() {
        // Bypass our own validation to force the underlying http builder
        // into an error state, the way an unvalidated header ingest used to.
        let builder = Response::builder()
            .status(200)
            .header("Bad Header Name\r\n", "value");

        let response = finalize_response(builder, axum::body::Body::empty());

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn both_clients_build_successfully() {
        assert!(http_client().is_some());
        assert!(insecure_http_client().is_some());
    }

    /// The retry is for a certificate and nothing else: a refused connection
    /// or a DNS miss must not be retried unverified, because verification is
    /// not what stopped it.
    ///
    /// The second URL is the measured reproduction of what reading the
    /// chain's prose cost. There is no TLS anywhere here -- plain HTTP to a
    /// port nothing listens on -- and the old substring search found the
    /// word "certificate" in reqwest's own "error sending request for url
    /// (...)", so the *filename* marked `127.0.0.1` unverified for the life
    /// of the process.
    #[test]
    fn only_a_certificate_failure_is_read_as_one() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let refused = |url: &'static str| {
            rt.block_on(async move {
                reqwest::Client::new()
                    .get(url)
                    .send()
                    .await
                    .expect_err("nothing is listening there")
            })
        };
        assert!(!is_certificate_error(&refused("http://127.0.0.1:1/")));
        assert!(!is_certificate_error(&refused(
            "http://127.0.0.1:1/certificate-of-authenticity.mkv"
        )));
    }

    /// The shape the real failure arrives in, measured against a self-signed
    /// origin: reqwest's connect error, an `io::Error`, a second
    /// `io::Error`, and only then the rustls one. `source()` alone stops at
    /// the first of the two, because `io::Error::source` yields the inner
    /// error's source rather than the inner error.
    #[test]
    fn a_rustls_failure_is_found_through_the_nested_io_errors_it_arrives_in() {
        use tokio_rustls::rustls;

        let rustls_error =
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let nested = std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls_error,
        ));
        assert!(is_certificate_error(&nested));

        // A TLS failure that verification would not have prevented is not
        // one to waive it for.
        let unrelated = std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::DecryptError,
        ));
        assert!(!is_certificate_error(&unrelated));
    }
}
