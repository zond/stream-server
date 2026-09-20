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

/// How a response body finished, for the line a route writes when one
/// ends.
///
/// A capture of four failing streams had nothing in it about why any of
/// them stopped. The distinction that mattered was invisible: four bodies
/// died after about ten seconds having delivered ~4 MiB of a multi-gigabyte
/// range, i.e. the player hung up, not the server.
///
/// **One vocabulary, because two routes are asked the same question.** A
/// torrent file and a proxied URL fail a player in the same ways, and a
/// field report that had to be read with a glossary per route would answer
/// neither: see `routes::stream`'s `http_stream_end` line and
/// `routes::proxy`'s `http_proxy_body_end` one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyOutcome {
    /// The body was dropped before the range was delivered: the player
    /// disconnected, or the request was cancelled.
    ClientDisconnect,
    /// The whole requested range was delivered.
    Complete,
    /// The reader failed part-way (see the `error` field).
    ReaderError,
}

impl BodyOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ClientDisconnect => "client-disconnect",
            Self::Complete => "complete",
            Self::ReaderError => "reader-error",
        }
    }
}

/// What a response body delivered, accumulated as it is polled.
#[derive(Debug, Default)]
pub(crate) struct BodyProgress {
    pub(crate) bytes_sent: u64,
    /// `None` until the body ends by itself; a body dropped before that is
    /// a player that hung up.
    pub(crate) outcome: Option<BodyOutcome>,
    pub(crate) error: Option<String>,
}

impl BodyProgress {
    pub(crate) fn record_chunk(&mut self, len: usize) {
        self.bytes_sent = self.bytes_sent.saturating_add(len as u64);
    }

    /// The reader failed. First error wins: what broke the stream is more
    /// use than whatever the stream said on its way out.
    ///
    /// Anything that can be shown, because the two routes fail with
    /// different errors -- a reader's `io::Error`, an `axum::Error` off a
    /// relayed body -- and what a report needs is the text either way.
    pub(crate) fn record_error(&mut self, error: &dyn std::fmt::Display) {
        if self.outcome.is_none() {
            self.outcome = Some(BodyOutcome::ReaderError);
            self.error = Some(error.to_string());
        }
    }

    /// The reader ran out, which for a `take`-limited body means the whole
    /// requested range was delivered. For a relayed one it is the origin's
    /// body ending, and `bytes_sent` beside the length the response
    /// promised is what says whether that was all of it.
    pub(crate) fn record_end(&mut self) {
        self.outcome.get_or_insert(BodyOutcome::Complete);
    }

    pub(crate) fn outcome(&self) -> BodyOutcome {
        self.outcome_of(0)
    }

    /// The outcome read against the length the response promised.
    ///
    /// **A body that delivered every byte it promised was not hung up on,
    /// whatever it recorded.** hyper stops polling a body of declared
    /// length the moment that length is met -- the message is finished
    /// without a further poll -- so [`Self::record_end`] never runs, and a
    /// body read to its end by a happy client is indistinguishable from
    /// one dropped part-way. Measured on `/proxy`: 65,536 bytes sent of
    /// 65,536 promised, to a client that read all of them, filed as a
    /// disconnect.
    ///
    /// A `promised` of nought is a response that declared no length -- a
    /// rewritten playlist is framed as it is written -- and such a body is
    /// polled to its end, so what it recorded is what happened.
    pub(crate) fn outcome_of(&self, promised: u64) -> BodyOutcome {
        match self.outcome {
            Some(outcome) => outcome,
            None if promised > 0 && self.bytes_sent >= promised => BodyOutcome::Complete,
            None => BodyOutcome::ClientDisconnect,
        }
    }
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

#[cfg(test)]
mod body_progress_tests {
    use super::*;

    /// A capture of four failing streams said nothing about why any of them
    /// stopped. What mattered was the distinction between a body that
    /// delivered its range and one the player hung up on part-way -- so a
    /// body that never ended by itself must read as a disconnect, and the
    /// first error must survive whatever the stream says afterwards.
    #[test]
    fn body_progress_tells_a_hung_up_player_from_a_delivered_range() {
        let mut dropped = BodyProgress::default();
        dropped.record_chunk(4 * 1024 * 1024);
        assert_eq!(dropped.outcome(), BodyOutcome::ClientDisconnect);
        assert_eq!(dropped.bytes_sent, 4 * 1024 * 1024);
        assert_eq!(dropped.error, None);

        let mut delivered = BodyProgress::default();
        delivered.record_chunk(10);
        delivered.record_chunk(20);
        delivered.record_end();
        assert_eq!(delivered.outcome(), BodyOutcome::Complete);
        assert_eq!(delivered.bytes_sent, 30);

        let mut failed = BodyProgress::default();
        failed.record_chunk(7);
        failed.record_error(&std::io::Error::other("piece read failed"));
        // A stream may still report end-of-stream after erroring; the error
        // is what ended it.
        failed.record_end();
        assert_eq!(failed.outcome(), BodyOutcome::ReaderError);
        assert_eq!(failed.bytes_sent, 7);
        assert_eq!(failed.error.as_deref(), Some("piece read failed"));
    }

    /// hyper never polls a body of declared length again once that length
    /// is met, so a body read to its end by a happy client records no end
    /// at all -- and read without the promise beside it, every delivered
    /// response is a disconnect.
    #[test]
    fn a_body_that_delivered_what_it_promised_was_not_hung_up_on() {
        let mut delivered = BodyProgress::default();
        delivered.record_chunk(64 * 1024);
        assert_eq!(delivered.outcome_of(64 * 1024), BodyOutcome::Complete);
        // Short of the promise is the player hanging up, which is the
        // distinction the line exists for.
        assert_eq!(
            delivered.outcome_of(128 * 1024),
            BodyOutcome::ClientDisconnect
        );
        // A response that promised no length says only what it recorded.
        assert_eq!(delivered.outcome_of(0), BodyOutcome::ClientDisconnect);

        // And what a body did report is never overruled by the arithmetic:
        // a reader that failed after delivering the promised bytes failed.
        let mut failed = BodyProgress::default();
        failed.record_chunk(64 * 1024);
        failed.record_error(&std::io::Error::other("piece read failed"));
        assert_eq!(failed.outcome_of(64 * 1024), BodyOutcome::ReaderError);
    }
}
