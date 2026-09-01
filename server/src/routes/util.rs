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
