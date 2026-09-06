use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header, response::Builder},
    response::{IntoResponse, Response},
    routing::any,
};
use dashmap::DashSet;
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::sync::{LazyLock, OnceLock};
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

/// Hosts whose certificate this process could not verify. The request that
/// discovered it is retried unverified; every later request for that host
/// goes straight to the unverified client, so a stream pays the failed
/// handshake once rather than once per segment.
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
static UNVERIFIED_HOSTS: LazyLock<DashSet<String>> = LazyLock::new(DashSet::new);

fn http_client() -> Option<&'static Client> {
    HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
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

/// Whether a failed fetch failed *because of the certificate*, which is the
/// only failure the unverified retry can help with -- a refused connection
/// or a DNS miss must stay an error.
///
/// reqwest exposes no typed predicate for this, and the rustls error that
/// carries the detail is several `source()`s down (`invalid peer
/// certificate: UnknownIssuer`), so the chain is walked and read. A false
/// positive costs one extra request that fails the same way; a false
/// negative costs a stream that a retry would have played.
fn is_certificate_error(error: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = source {
        if error
            .to_string()
            .to_ascii_lowercase()
            .contains("certificate")
        {
            return true;
        }
        source = error.source();
    }
    false
}

/// Applies the `r=` (Core-format) custom response headers to a response
/// builder, validating each name/value pair first so that a malicious or
/// malformed header (e.g. containing a newline) can never poison the
/// builder's internal error state. Invalid pairs are skipped and logged at
/// debug level rather than propagated.
fn apply_custom_response_headers(
    mut builder: Builder,
    custom_response_headers: HashMap<String, String>,
) -> Builder {
    for (name, value) in custom_response_headers {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            (Ok(header_name), Ok(header_value)) => {
                builder = builder.header(header_name, header_value);
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
/// The one format the wildcard above cannot express. It carries no `h=`/`r=`
/// pairs (those live in the path segment of the Core format), so everything
/// it needs is the `d=` the shared handler reads out of `params`.
pub async fn proxy_root_handler(
    State(state): State<AppState>,
    raw_query: axum::extract::RawQuery,
    params: Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    proxy(state, None, raw_query.0, params.0, headers, method).await
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
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    // The route this handler serves is `/proxy/{*rest}`, so the prefix is
    // always there; an empty rest could only come of a router change, and it
    // answers 400 the way any unparseable target does.
    let rest = uri.path().strip_prefix("/proxy/").unwrap_or_default();
    proxy(
        state,
        Some(rest.to_string()),
        raw_query,
        params,
        headers,
        method,
    )
    .await
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
    params: HashMap<String, String>,
    headers: HeaderMap,
    method: Method,
) -> Response {
    // Porting the logic from express_805.js
    // Format 1: ?d=URL (standard)
    // Format 2: /<query_params>/<path> (Core) where query_params contains d=ORIGIN&h=HEADER&r=RESPONSE_HEADER

    let mut target_url = String::new();
    let mut custom_headers = HashMap::new();
    let mut custom_response_headers = HashMap::new();
    // The client's name for the player this stream is for, if it minted one
    // (`p=`). It is ours, not the target's: it never travels to the origin,
    // and it is what `POST /proxy-streams/{token}/close` addresses. See
    // [`crate::proxy_streams`].
    let mut player_token = None;
    let is_path_format = rest.is_some();

    match rest {
        None => {
            if let Some(d) = params.get("d") {
                target_url = d.clone();
            }
            player_token = params.get("p").filter(|t| !t.is_empty()).cloned();
        }
        Some(rest) => {
            // Handle path-based format: /proxy/d=...&h=.../path/to/file
            // Split rest by first slash to get query_segment and path
            let (query_seg, path_seg) = match rest.split_once('/') {
                Some((q, p)) => (q, p),
                None => (rest.as_str(), ""),
            };

            // Parse the query segment
            for (key, val) in url::form_urlencoded::parse(query_seg.as_bytes()) {
                match key.as_ref() {
                    "d" => target_url = val.into_owned(),
                    "h" => {
                        // Header format "Name:Value"
                        if let Some((name, value)) = val.split_once(':') {
                            custom_headers
                                .insert(name.trim().to_string(), value.trim().to_string());
                        }
                    }
                    "r" => {
                        // Response header format "Name:Value"
                        if let Some((name, value)) = val.split_once(':') {
                            custom_response_headers
                                .insert(name.trim().to_string(), value.trim().to_string());
                        }
                    }
                    "p" if !val.is_empty() => player_token = Some(val.into_owned()),
                    _ => {}
                }
            }

            // If we found 'd', construct the full URL
            if !target_url.is_empty() {
                // target_url is the origin (e.g. http://example.com)
                // path_seg is the relative path (e.g. video.mp4)
                // Join them carefully
                if !path_seg.is_empty() {
                    if !target_url.ends_with('/') {
                        target_url.push('/');
                    }
                    target_url.push_str(path_seg);
                }
            } else {
                // Fallback: assume whole rest is the URL (legacy/simple proxy)
                target_url = rest;
            }
        }
    }

    let mut url = match Url::parse(&target_url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target URL").into_response(),
    };

    if is_path_format && let Some(q) = raw_query {
        url.set_query(Some(&q));
    }

    // The host is fetched verified unless a previous request for it failed
    // on its certificate (see [`UNVERIFIED_HOSTS`]).
    let host = url.host_str().unwrap_or_default().to_string();
    let known_unverified = UNVERIFIED_HOSTS.contains(&host);
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

        // Apply custom headers from query params (Core format)
        for (name, value) in &custom_headers {
            req_builder = req_builder.header(name, value);
        }
        req_builder
    };

    let response = match build_request(client).send().await {
        Ok(resp) => resp,
        Err(e) if !known_unverified && is_certificate_error(&e) => {
            // One retry, for this host only, and say so once per process.
            tracing::warn!(
                host = %host,
                error = %e,
                "certificate verification failed; retrying this host unverified for \
                 the life of the process"
            );
            UNVERIFIED_HOSTS.insert(host);
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

    let content_type = res_headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let is_playlist = url.path().ends_with(".m3u8")
        || url.path().ends_with(".m3u")
        || content_type.contains("mpegurl");

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
    let rewriting_playlist = is_playlist && !encoded_body;
    if is_playlist && encoded_body {
        tracing::warn!(
            content_encoding = %content_encoding,
            url = %url,
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
    res_builder = apply_custom_response_headers(res_builder, custom_response_headers);

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
        let body = match response.text().await {
            Ok(body) => body,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("Proxy could not read the playlist: {e}"),
                )
                    .into_response();
            }
        };
        let rewritten = rewrite_playlist(&body, &url, player_token.as_deref());
        // The framing of the body we built, measured on that body.
        res_builder = res_builder.header(header::CONTENT_LENGTH, rewritten.len().to_string());
        return finalize_response(res_builder, axum::body::Body::from(rewritten));
    }

    // Registered under the client's token, so the client can end this exact
    // read rather than waiting out a timeout meant for a slow swarm.
    let stream = state
        .proxy_streams
        .attach(player_token, response.bytes_stream());
    finalize_response(res_builder, axum::body::Body::from_stream(stream))
}

/// Rewrites every URL in a playlist to come back through this proxy, and
/// carries `player_token` into each one: a segment fetched by the same
/// player is part of the same stream, and closing that player has to close
/// the segment read that is actually in flight.
/// `POST /proxy-streams/{token}/close`: end every proxied stream the client
/// marked with `token`, and say how many that was.
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

fn rewrite_playlist(body: &str, base_url: &Url, player_token: Option<&str>) -> String {
    let token_param = player_token
        .map(|token| format!("&p={}", urlencoding::encode(token)))
        .unwrap_or_default();
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
                    let proxy_uri = format!(
                        "/proxy/?d={}{token_param}",
                        urlencoding::encode(&absolute_uri)
                    );
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
            let proxy_uri = format!(
                "/proxy/?d={}{token_param}",
                urlencoding::encode(&absolute_uri)
            );
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
        let rewritten = rewrite_playlist(body, &base(), None);
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/streams/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_http_line_is_wrapped_without_double_joining() {
        let body = "http://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), None);
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_https_line_is_wrapped_without_double_joining() {
        let body = "https://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), None);
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
        let rewritten = rewrite_playlist(body, &base(), None);
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
        let rewritten = rewrite_playlist(body, &base(), None);
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
        let rewritten = rewrite_playlist(body, &base(), None);
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}\",IV=0x0123456789abcdef\n",
            proxied("http://example.com/streams/key/enc.key")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn root_relative_path_resolves_against_origin() {
        let body = "/videos/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), None);
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
        let rewritten = rewrite_playlist(body, &base(), Some("player one"));
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}&p=player%20one\"\n{}&p=player%20one\n",
            proxied("http://example.com/streams/key/enc.key"),
            proxied("http://example.com/streams/seg-0.ts")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn comment_and_blank_lines_are_left_unchanged() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n\n#EXT-X-TARGETDURATION:10\n";
        let rewritten = rewrite_playlist(body, &base(), None);
        assert_eq!(rewritten, body);
    }

    #[test]
    fn apply_custom_response_headers_skips_invalid_name_and_value() {
        let mut headers = HashMap::new();
        // Valid pair: should be applied.
        headers.insert("X-Proxy-Ok".to_string(), "yes".to_string());
        // Invalid value: embedded CR/LF must never reach the header map.
        headers.insert(
            "X-Evil".to_string(),
            "bad\r\nInjected-Header: true".to_string(),
        );
        // Invalid name: space is not a legal header-name character.
        headers.insert("Bad Name".to_string(), "value".to_string());

        let builder = apply_custom_response_headers(Response::builder().status(200), headers);
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
        let mut custom_response_headers = HashMap::new();
        custom_response_headers.insert("X-Evil".to_string(), "bad\r\nInjected: true".to_string());

        let builder =
            apply_custom_response_headers(Response::builder().status(200), custom_response_headers);
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
    #[test]
    fn only_a_certificate_failure_is_read_as_one() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Nothing listens on this port, so the failure is a refused
        // connection with no certificate anywhere in its chain.
        let refused = rt.block_on(async {
            reqwest::Client::new()
                .get("http://127.0.0.1:1/")
                .send()
                .await
                .expect_err("nothing is listening there")
        });
        assert!(!is_certificate_error(&refused));
    }
}
