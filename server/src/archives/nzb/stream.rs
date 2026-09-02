use super::parser::NzbFile;
use super::session::NzbSession;
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pub struct NzbFileStream {
    session: Arc<NzbSession>,
    file: NzbFile,
    current_segment_idx: usize,
    buffer: Bytes, // Decoded data waiting to be read
    fetching: bool,
    // We might need a future to store the pending fetch
    // But since we are in `poll_read`, we need to be careful with async calls.
    // The idiomatic way is to use a State enum or explicit polling of a future.
    // Using `tokio_util::io::ReaderStream` on a stream of chunks might be easier?
    // Let's stick to AsyncRead but maybe loop simpler.
    // actually, let's use a channel or just spawn the fetches ahead?
    // For simplicity: One segment at a time.
    fetch_future: Option<tokio::task::JoinHandle<Result<Vec<u8>>>>,
}

impl NzbFileStream {
    pub fn new(session: Arc<NzbSession>, file: NzbFile) -> Self {
        // Sort segments by number just in case
        let mut file = file;
        file.segments.segments.sort_by_key(|s| s.number);

        Self {
            session,
            file,
            current_segment_idx: 0,
            buffer: Bytes::new(),
            fetching: false,
            fetch_future: None,
        }
    }
}

impl AsyncRead for NzbFileStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        loop {
            // 1. If we have buffered data, return it
            if !self.buffer.is_empty() {
                let len = std::cmp::min(buf.remaining(), self.buffer.len());
                buf.put_slice(&self.buffer[..len]);
                self.buffer = self.buffer.slice(len..);
                return Poll::Ready(Ok(()));
            }

            // 2. If no buffer and no more segments, EOF
            if self.current_segment_idx >= self.file.segments.segments.len()
                && self.fetch_future.is_none()
            {
                return Poll::Ready(Ok(()));
            }

            // 3. If we are fetching, poll the future
            if let Some(fut) = &mut self.fetch_future {
                return match Pin::new(fut).poll(cx) {
                    Poll::Ready(result) => {
                        self.fetch_future = None;
                        self.fetching = false;

                        match result {
                            Ok(Ok(raw_body)) => {
                                // Decode yEnc
                                // We'll process raw_body here.
                                // Simple yEnc decoder:
                                // Skip header (=ybegin), decode chars, handle =yend
                                // Using the `yenc` crate: `yenc::decode_buffer`?
                                // If crate is not straightforward, we do simplistic decode for now.
                                // Note: `yenc` crate on crates.io is minimal.
                                // Let's try `yenc::decode`.

                                // Parse raw_body for yEnc
                                let decoded = match decode_yenc(&raw_body) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        return Poll::Ready(Err(Error::new(
                                            ErrorKind::InvalidData,
                                            e.to_string(),
                                        )));
                                    }
                                };

                                self.buffer = Bytes::from(decoded);
                                self.current_segment_idx += 1;
                                continue; // Loop back to write to buf
                            }
                            Ok(Err(e)) => Poll::Ready(Err(Error::other(e.to_string()))),
                            Err(e) => Poll::Ready(Err(Error::other(e.to_string()))), // JoinError
                        }
                    }
                    Poll::Pending => Poll::Pending,
                };
            }

            // 4. Start fetching next segment
            if self.current_segment_idx < self.file.segments.segments.len() {
                let segment = self.file.segments.segments[self.current_segment_idx].clone();
                let session = self.session.clone();
                let fut = tokio::spawn(async move {
                    session
                        .fetch_segment(&segment.id)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))
                });
                self.fetch_future = Some(fut);
                self.fetching = true;
                continue;
            }
        }
    }
}

// Minimal yEnc decoder to avoid complex crate dependency issues if `yenc` crate is weird.
// Legacy JS uses `yenc` module.
fn decode_yenc(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    // Find =ybegin
    // Find =ypart (optional)
    // Decode data
    // Find =yend

    // Naive implementation for MVP
    let start_marker = b"=ybegin";
    let part_marker = b"=ypart";

    // Find start
    let start_idx = input
        .windows(start_marker.len())
        .position(|w| w == start_marker)
        .unwrap_or(0); // If no header, maybe raw? But NNTP usually has header.

    // Find data start (newline after header(s))
    let mut data_start = start_idx;
    // Skip line
    if let Some(pos) = input[start_idx..].iter().position(|&b| b == b'\n') {
        data_start += pos + 1;
    }

    // Check for =ypart
    if input[data_start..].starts_with(part_marker)
        && let Some(pos) = input[data_start..].iter().position(|&b| b == b'\n')
    {
        data_start += pos + 1;
    }

    let mut output = Vec::with_capacity(input.len());
    let mut i = data_start;

    while i < input.len() {
        let b = input[i];

        // Check for end
        if b == b'=' && input[i..].starts_with(b"=yend") {
            break;
        }

        if b == b'=' {
            // Escape next
            i += 1;
            if i >= input.len() {
                break;
            }
            let escaped = input[i];
            output.push((escaped.wrapping_sub(64)).wrapping_sub(42));
        } else if b == b'\r' || b == b'\n' {
            // Ignore newlines in body? yEnc usually ignores them, but they might mean end of line.
            // "The CR/LF pairs at the end of each line are not part of the data"
        } else {
            output.push(b.wrapping_sub(42));
        }
        i += 1;
    }

    Ok(output)
}

#[cfg(test)]
mod yenc_tests {
    use super::decode_yenc;

    /// Reference yEnc encoder (OSDb/yEnc spec): each output byte is
    /// `(input + 42) mod 256`; the four critical bytes NUL/LF/CR/'=' are
    /// escaped as '=' followed by `(output + 64) mod 256`.
    fn encode_body(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for &b in data {
            let e = b.wrapping_add(42);
            if e == 0x00 || e == 0x0A || e == 0x0D || e == 0x3D {
                out.push(0x3D);
                out.push(e.wrapping_add(64));
            } else {
                out.push(e);
            }
        }
        out
    }

    fn wrap(body: &[u8]) -> Vec<u8> {
        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin line=128 size=999 name=video.mkv\n");
        article.extend_from_slice(body);
        article.extend_from_slice(b"\n=yend size=999 crc32=deadbeef\n");
        article
    }

    #[test]
    fn round_trips_plain_ascii() {
        let plain = b"Hello, World! The quick brown fox.";
        let article = wrap(&encode_body(plain));
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }

    #[test]
    fn round_trips_every_byte_value() {
        // Exercises all escapes and all wrapping_sub boundaries at once: any
        // off-by-one in the escape offset or the raw-byte subtraction shows up
        // as a mismatch somewhere in the 0..=255 sweep.
        let plain: Vec<u8> = (0u16..=255).map(|b| b as u8).collect();
        let article = wrap(&encode_body(&plain));
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }

    #[test]
    fn decodes_escaped_equals_sign_in_middle() {
        // input 0x13 encodes to (0x13+42)=0x3D '=' => must be escaped, and the
        // decoder must apply the -64 -42 offset, not treat it as a raw byte.
        let plain = [0xAAu8, 0x13, 0xBB];
        let body = encode_body(&plain);
        assert!(body.windows(2).any(|w| w[0] == 0x3D), "0x13 must escape");
        let article = wrap(&body);
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }

    #[test]
    fn raw_byte_near_42_boundary_wraps_not_saturates() {
        // output byte 6 decodes to 6.wrapping_sub(42) == 220 (wrapping), not 0
        // (saturating). Input 220 => (220+42) mod 256 == 6, emitted raw.
        let plain = [220u8, 221, 255, 0, 41, 42, 43];
        let article = wrap(&encode_body(&plain));
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }

    #[test]
    fn skips_ypart_second_header_line() {
        let plain = b"payload bytes after two headers";
        let body = encode_body(plain);
        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin part=1 line=128 size=999 name=video.mkv\n");
        article.extend_from_slice(b"=ypart begin=1 end=31\n");
        article.extend_from_slice(&body);
        article.extend_from_slice(b"\n=yend size=31\n");
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }

    #[test]
    fn strips_crlf_line_breaks_inside_body() {
        // yEnc wraps long lines with CR/LF that are NOT data; the decoder must
        // drop them rather than decode them into output bytes.
        let part1 = b"first half of the payload";
        let part2 = b"second half of the payload";
        let mut body = encode_body(part1);
        body.extend_from_slice(b"\r\n"); // line wrap, not data
        body.extend_from_slice(&encode_body(part2));
        let article = wrap(&body);

        let mut expected = part1.to_vec();
        expected.extend_from_slice(part2);
        assert_eq!(decode_yenc(&article).unwrap(), expected);
    }

    #[test]
    fn data_bytes_that_are_newline_values_survive() {
        // Data values 0x0A/0x0D/0x00 encode to non-critical raw bytes
        // (0x34/0x37/0x2A), so they must come through decoding, unlike literal
        // CR/LF in the stream which are stripped.
        let plain = [0x0Au8, 0x0D, 0x00, 0x0A];
        let body = encode_body(&plain);
        // None of these produced a literal newline in the encoded stream.
        assert!(!body.contains(&b'\n') && !body.contains(&b'\r'));
        let article = wrap(&body);
        assert_eq!(decode_yenc(&article).unwrap(), plain);
    }
}
