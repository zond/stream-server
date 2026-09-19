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
