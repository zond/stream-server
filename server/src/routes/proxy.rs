use crate::state::AppState;
use axum::{
    Router,
    extract::{Path, Query},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header, response::Builder},
    response::{IntoResponse, Response},
    routing::any,
};
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::sync::OnceLock;
use url::Url;

/// Lazily-built, process-wide reqwest client for the proxy route.
///
/// `Client::builder().build()` can fail (e.g. if the TLS backend can't be
/// initialized), so building it once at startup-on-first-use and reusing it
/// avoids both a per-request `.unwrap()` panic and the cost of rebuilding a
/// client for every proxied request.
static HTTP_CLIENT: OnceLock<Option<Client>> = OnceLock::new();

fn http_client() -> Option<&'static Client> {
    HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
                .danger_accept_invalid_certs(true) // Parity with rejectUnauthorized: false
                .build()
                .map_err(|e| tracing::error!("Failed to build proxy HTTP client: {e}"))
                .ok()
        })
        .as_ref()
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

pub fn router() -> Router<AppState> {
    Router::new()
        // The original JS uses /proxy/:opts/:pathname*
        // We can use a wildcard capturing the whole path.
        .route("/{*rest}", any(proxy_handler))
}

pub async fn proxy_handler(
    Path(rest): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    // Porting the logic from express_805.js
    // Format 1: ?d=URL (standard)
    // Format 2: /<query_params>/<path> (Core) where query_params contains d=ORIGIN&h=HEADER&r=RESPONSE_HEADER

    let mut target_url = String::new();
    let mut custom_headers = HashMap::new();
    let mut custom_response_headers = HashMap::new();
    let mut is_path_format = false;

    // Check for standard query param '?d='
    if let Some(d) = params.get("d") {
        target_url = d.clone();
        // Fallback: If rest is not empty and d is just origin, we might need to append rest?
        // But usually ?d=FULL_URL
    } else {
        is_path_format = true;
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
                        custom_headers.insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "r" => {
                    // Response header format "Name:Value"
                    if let Some((name, value)) = val.split_once(':') {
                        custom_response_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
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
            target_url = rest.clone();
        }
    }

    let mut url = match Url::parse(&target_url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target URL").into_response(),
    };

    if is_path_format && let Some(q) = raw_query {
        url.set_query(Some(&q));
    }

    let client = match http_client() {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Proxy client unavailable",
            )
                .into_response();
        }
    };

    let mut req_builder = client.request(method, url.clone());

    // Forward standard headers
    let allowed_req_headers = [
        "accept",
        "accept-encoding",
        "accept-language",
        "connection",
        "transfer-encoding",
        "range",
        "if-range",
        "user-agent",
    ];

    for name in allowed_req_headers {
        if let Some(value) = headers.get(name) {
            req_builder = req_builder.header(name, value);
        }
    }

    // Apply custom headers from query params (Core format)
    for (name, value) in custom_headers {
        req_builder = req_builder.header(name, value);
    }

    let response = match req_builder.send().await {
        Ok(resp) => resp,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response(),
    };

    let status = response.status();
    let mut res_builder = Response::builder().status(status);

    let allowed_res_headers = [
        "accept-ranges",
        "content-type",
        "content-length",
        "content-range",
        "connection",
        "transfer-encoding",
        "last-modified",
        "etag",
        "server",
        "date",
    ];

    let res_headers = response.headers().clone();
    for name in allowed_res_headers {
        if let Some(value) = res_headers.get(name) {
            res_builder = res_builder.header(name, value);
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

    let content_type = res_headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let is_playlist = url.path().ends_with(".m3u8")
        || url.path().ends_with(".m3u")
        || content_type.contains("mpegurl");

    if is_playlist {
        // We need to rewrite the playlist.
        // For now, let's just stream it without rewriting as a first step,
        // then add rewriting if segments fail.
        let body = response.text().await.unwrap_or_default();
        let rewritten = rewrite_playlist(&body, &url);
        return finalize_response(res_builder, axum::body::Body::from(rewritten));
    }

    let stream = response.bytes_stream();
    finalize_response(res_builder, axum::body::Body::from_stream(stream))
}

fn rewrite_playlist(body: &str, base_url: &Url) -> String {
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
                    let proxy_uri = format!("/proxy/?d={}", urlencoding::encode(&absolute_uri));
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
            let proxy_uri = format!("/proxy/?d={}", urlencoding::encode(&absolute_uri));
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
        let rewritten = rewrite_playlist(body, &base());
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/streams/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_http_line_is_wrapped_without_double_joining() {
        let body = "http://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base());
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_https_line_is_wrapped_without_double_joining() {
        let body = "https://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base());
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
        let rewritten = rewrite_playlist(body, &base());
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
        let rewritten = rewrite_playlist(body, &base());
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
        let rewritten = rewrite_playlist(body, &base());
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}\",IV=0x0123456789abcdef\n",
            proxied("http://example.com/streams/key/enc.key")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn root_relative_path_resolves_against_origin() {
        let body = "/videos/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base());
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/videos/seg-0.ts"))
        );
    }

    #[test]
    fn comment_and_blank_lines_are_left_unchanged() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n\n#EXT-X-TARGETDURATION:10\n";
        let rewritten = rewrite_playlist(body, &base());
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
    fn http_client_builds_successfully() {
        assert!(http_client().is_some());
    }
}
