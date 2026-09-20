//! Small helpers shared across `server/src/routes/*` modules.

/// Parse an HTTP `Range` header of the form `bytes=<start>-<end>` (with
/// either side optional, i.e. suffix ranges `bytes=-N` and open-ended
/// ranges `bytes=N-`) against a resource of `size` bytes.
///
/// Returns `Some((start, end))` (inclusive, `end` clamped to `size - 1`)
/// when the header describes a satisfiable byte range, `None` otherwise
/// (including malformed headers, non-`bytes` units, and any range that
/// cannot be satisfied — callers should fall back to a full-body 200 or
/// respond 416 as appropriate).
///
/// `size == 0` always yields `None`: there is no valid inclusive byte
/// range on an empty resource, and computing `size - 1` for a suffix or
/// open range would otherwise underflow `u64`.
pub(crate) fn parse_range(header: &str, size: u64) -> Option<(u64, u64)> {
    let prefix = "bytes=";
    if !header.starts_with(prefix) || size == 0 {
        return None;
    }

    let range_str = &header[prefix.len()..];
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start_str = parts[0];
    let end_str = parts[1];

    if start_str.is_empty() {
        // Suffix byte range: bytes=-500 (last 500 bytes)
        let suffix: u64 = end_str.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        let start = size.saturating_sub(suffix);
        return Some((start, size - 1));
    }

    let start: u64 = start_str.parse().ok()?;

    let end = if end_str.is_empty() {
        size - 1
    } else {
        end_str.parse().ok()?
    };

    if start > end || start >= size {
        return None;
    }

    Some((start, end.min(size - 1)))
}

/// The framing one media response gets from a `Range` header: which bytes
/// go out, under which status, with which headers.
///
/// **One definition, because a member of an archive has to behave exactly
/// like a plain file over HTTP.** The torrent stream route and the archive
/// routes each used to write this out for themselves, and the differences
/// were not decisions: a `416` that named the resource's length in one
/// place and not the other, an empty resource that was a `Content-Length:
/// 1` here and a `0` there. A player reading a film out of a zip cannot be
/// asked to know which of the two it is talking to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MediaRange {
    pub start: u64,
    /// Inclusive, the way `Content-Range` writes it.
    pub end: u64,
    /// Whether the client asked for a range, which is what makes the
    /// answer a `206` rather than a `200`.
    pub partial: bool,
}

impl MediaRange {
    /// The framing for `range_header` over a resource of `size` bytes, or
    /// `None` for a range nothing can satisfy -- which is
    /// [`range_not_satisfiable`] and not a `200` over the whole file. A
    /// request with no `Range` at all is the whole resource.
    pub(crate) fn of(range_header: Option<&str>, size: u64) -> Option<Self> {
        match range_header {
            Some(header) => parse_range(header, size).map(|(start, end)| Self {
                start,
                end,
                partial: true,
            }),
            None => Some(Self {
                start: 0,
                end: size.saturating_sub(1),
                partial: false,
            }),
        }
    }

    /// How many bytes the body carries.
    ///
    /// An empty resource is `(0, 0)` with no range, and an inclusive end
    /// of 0 is a length of one: a `HEAD` used to promise a byte the `GET`
    /// had not got, and a client reads that as a truncated response.
    pub(crate) fn content_length(&self, size: u64) -> u64 {
        if size == 0 {
            0
        } else {
            self.end.saturating_sub(self.start) + 1
        }
    }

    /// `200` or `206`.
    pub(crate) fn status(&self) -> axum::http::StatusCode {
        if self.partial {
            axum::http::StatusCode::PARTIAL_CONTENT
        } else {
            axum::http::StatusCode::OK
        }
    }

    /// The headers the framing owns: the length, the standing offer of
    /// ranges, and -- for a partial answer -- which bytes these are.
    pub(crate) fn write_headers(&self, size: u64, headers: &mut axum::http::HeaderMap) {
        use axum::http::header;
        headers.insert(header::CONTENT_LENGTH, self.content_length(size).into());
        headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
        if self.partial {
            headers.insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", self.start, self.end, size)
                    .parse()
                    .unwrap(),
            );
        }
    }
}

/// What a `Range` nothing can satisfy is answered with: a `416` that names
/// the resource's length, as RFC 9110 asks, so the player can ask again
/// for something that is there instead of guessing.
pub(crate) fn range_not_satisfiable(size: u64) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        [(header::CONTENT_RANGE, format!("bytes */{size}"))],
        "Range Not Satisfiable",
    )
        .into_response()
}

/// What a caller-supplied URL may appear as in a log: its origin,
/// `scheme://host:port`, and never its path or query.
///
/// `/proxy`'s `d=`, `/ftp`'s and an archive `/create`'s URL are the
/// caller's, and a signed CDN link or a debrid URL carries the viewer's
/// credentials in the query -- the log files this process keeps for the
/// last ten launches are the one place those must not turn up. The origin
/// is what a field report is read for anyway: which host answered, and
/// how. A URL that does not parse is `"<unparsed>"` rather than itself:
/// the reason it did not parse may be exactly the interesting part of it,
/// and that is no reason to write it down.
pub(crate) fn log_origin(url: &str) -> String {
    url::Url::parse(url)
        .map(|parsed| parsed.origin().ascii_serialization())
        .unwrap_or_else(|_| "<unparsed>".to_string())
}

/// The path of a request as a log may carry it: everything under `/proxy`
/// and `/ftp` is elided to the route itself.
///
/// `/proxy` takes its target in the *path* as well as the query (the Core
/// format is `/proxy/d=<url>&h=<header>/<name>`), so a request span built
/// from `uri.path()` carried the proxied URL and the caller's `h=` headers
/// -- `Authorization` among them -- into every line written under that
/// span. `/ftp` spells its target the same way.
pub(crate) fn log_path(path: &str) -> &str {
    for route in ["/proxy", "/ftp"] {
        if path == route || path.starts_with(&format!("{route}/")) {
            return route;
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The framing rules every media response is made of, in one place:
    /// the whole resource without a `Range`, the asked-for span with one,
    /// a length that never promises a byte that is not there, and a range
    /// nothing can satisfy answered as such rather than as the whole file.
    #[test]
    fn the_framing_is_the_same_whatever_is_being_framed() {
        let whole = MediaRange::of(None, 100).expect("no range is the whole resource");
        assert_eq!((whole.start, whole.end, whole.partial), (0, 99, false));
        assert_eq!(whole.content_length(100), 100);
        assert_eq!(whole.status(), axum::http::StatusCode::OK);

        let part = MediaRange::of(Some("bytes=10-19"), 100).expect("satisfiable");
        assert_eq!((part.start, part.end, part.partial), (10, 19, true));
        assert_eq!(part.content_length(100), 10);
        assert_eq!(part.status(), axum::http::StatusCode::PARTIAL_CONTENT);
        let mut headers = axum::http::HeaderMap::new();
        part.write_headers(100, &mut headers);
        assert_eq!(headers[axum::http::header::CONTENT_LENGTH], "10");
        assert_eq!(
            headers[axum::http::header::CONTENT_RANGE],
            "bytes 10-19/100"
        );
        assert_eq!(headers[axum::http::header::ACCEPT_RANGES], "bytes");

        // An empty resource: a body of no bytes under a `200`, never a
        // `Content-Length: 1` over a body that ends at once.
        let empty = MediaRange::of(None, 0).expect("an empty resource is not a bad range");
        assert_eq!(empty.content_length(0), 0);
        assert!(MediaRange::of(Some("bytes=0-"), 0).is_none());

        // And a range past the end is refused, naming the length.
        assert!(MediaRange::of(Some("bytes=100-"), 100).is_none());
    }

    #[test]
    fn parses_standard_ranges() {
        assert_eq!(parse_range("bytes=0-0", 10), Some((0, 0)));
        assert_eq!(parse_range("bytes=5-", 10), Some((5, 9)));
        assert_eq!(parse_range("bytes=-4", 10), Some((6, 9)));
    }

    #[test]
    fn end_is_clamped_to_size_minus_one() {
        assert_eq!(parse_range("bytes=0-100", 10), Some((0, 9)));
    }

    #[test]
    fn rejects_invalid_ranges() {
        assert_eq!(parse_range("items=0-1", 10), None);
        assert_eq!(parse_range("bytes=9-1", 10), None);
        assert_eq!(parse_range("bytes=10-11", 10), None);
        assert_eq!(parse_range("bytes=0-0", 0), None);
    }

    #[test]
    fn zero_size_is_always_none() {
        // Regression: these previously underflowed u64 (`size - 1` with
        // size == 0), panicking in debug and wrapping to a huge value in
        // release. A zero-length entry has no satisfiable byte range.
        assert_eq!(parse_range("bytes=-5", 0), None);
        assert_eq!(parse_range("bytes=0-", 0), None);
        assert_eq!(parse_range("bytes=0-0", 0), None);
    }

    #[test]
    fn start_at_or_past_size_is_none() {
        assert_eq!(parse_range("bytes=10-10", 10), None);
        assert_eq!(parse_range("bytes=11-20", 10), None);
    }

    #[test]
    fn start_after_end_is_none() {
        assert_eq!(parse_range("bytes=5-2", 10), None);
    }

    #[test]
    fn non_bytes_unit_is_none() {
        assert_eq!(parse_range("items=0-5", 10), None);
        assert_eq!(parse_range("bits=0-5", 10), None);
    }

    #[test]
    fn malformed_ranges_are_none() {
        assert_eq!(parse_range("bytes=", 10), None);
        assert_eq!(parse_range("bytes=-", 10), None);
        assert_eq!(parse_range("bytes=abc-def", 10), None);
        assert_eq!(parse_range("bytes=0-1-2", 10), None);
        assert_eq!(parse_range("bytes=0", 10), None);
        assert_eq!(parse_range("", 10), None);
    }
}

#[cfg(test)]
mod log_redaction_tests {
    use super::*;

    /// A log may name the host and nothing else of a caller's URL (review
    /// #17).
    #[test]
    fn log_origin_keeps_the_host_and_drops_everything_else() {
        assert_eq!(
            log_origin("https://cdn.example.org:8443/film.mkv?token=secret"),
            "https://cdn.example.org:8443"
        );
        assert_eq!(log_origin("http://a.example/x"), "http://a.example");
        assert_eq!(log_origin("not a url?token=secret"), "<unparsed>");
    }

    /// And a request span may not carry `/proxy`'s target, which lives in
    /// the path as well as the query.
    #[test]
    fn log_path_elides_the_proxy_and_ftp_targets() {
        assert_eq!(
            log_path(
                "/proxy/d=https%3A%2F%2Fcdn.example%2Ffilm.mkv&h=Authorization%3ABearer%20x/film.mkv"
            ),
            "/proxy"
        );
        assert_eq!(log_path("/proxy/"), "/proxy");
        assert_eq!(log_path("/proxy"), "/proxy");
        assert_eq!(
            log_path("/ftp/ftp%3A%2F%2Fuser%3Apass%40host%2Ffilm.mkv"),
            "/ftp"
        );
        assert_eq!(log_path("/heartbeat"), "/heartbeat");
        assert_eq!(log_path("/proxying/x"), "/proxying/x");
    }
}
