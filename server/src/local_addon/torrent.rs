use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum BValue {
    Int(i64),
    Str(Vec<u8>),
    List(Vec<BValue>),
    Dict(BTreeMap<Vec<u8>, BValue>),
}

impl BValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            BValue::Str(bytes) => std::str::from_utf8(bytes).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BValue::Str(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn get_dict(&self) -> Option<&BTreeMap<Vec<u8>, BValue>> {
        match self {
            BValue::Dict(map) => Some(map),
            _ => None,
        }
    }

    pub fn get_list(&self) -> Option<&Vec<BValue>> {
        match self {
            BValue::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn get_int(&self) -> Option<i64> {
        match self {
            BValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

pub struct Torrent {
    pub info_hash: String,
    pub files: Vec<TorrentFile>,
    pub _name: Option<String>,
}

pub struct TorrentFile {
    pub path: String,
    pub _length: u64,
}

pub fn parse_torrent(bytes: &[u8]) -> Result<Torrent, String> {
    let (root, _) = parse_bencode(bytes).map_err(|_| "Invalid bencode")?;
    let dict = root.get_dict().ok_or("Root not a dict")?;

    let info = dict.get(b"info".as_slice()).ok_or("Missing info dict")?;

    // Calculate InfoHash
    // We need the RAW bytes of the info dict.
    // Since our parser is simple, we might not have tracked the raw slice.
    // We need to re-encode or extract the slice.
    // Easier: find "4:info" in original bytes and parse just that part to get the end index.

    let info_bytes = extract_info_bytes(bytes).ok_or("Could not extract info bytes")?;
    let info_hash = sha1_str(info_bytes);

    let info_dict = info.get_dict().ok_or("Info not a dict")?;
    let name = match info_dict.get(b"name".as_slice()).and_then(|v| v.as_bytes()) {
        Some(name_bytes) => Some(
            std::str::from_utf8(name_bytes)
                .map_err(|_| "Invalid name encoding (not UTF-8)")?
                .to_string(),
        ),
        None => None,
    };

    let mut files = Vec::new();

    if let Some(files_list) = info_dict
        .get(b"files".as_slice())
        .and_then(|v| v.get_list())
    {
        // Multi-file
        for file_dict in files_list {
            if let Some(f_dict) = file_dict.get_dict() {
                let length = f_dict
                    .get(b"length".as_slice())
                    .and_then(|v| v.get_int())
                    .unwrap_or(0) as u64;
                let path_list = f_dict.get(b"path".as_slice()).and_then(|v| v.get_list());
                if let Some(p_list) = path_list {
                    let mut path_parts = Vec::new();
                    for p in p_list {
                        if let Some(s) = p.as_str() {
                            path_parts.push(s);
                        }
                    }
                    if !path_parts.is_empty() {
                        files.push(TorrentFile {
                            path: path_parts.join("/"),
                            _length: length,
                        });
                    }
                }
            }
        }
    } else {
        // Single-file
        let length = info_dict
            .get(b"length".as_slice())
            .and_then(|v| v.get_int())
            .unwrap_or(0) as u64;
        if let Some(n) = &name {
            files.push(TorrentFile {
                path: n.clone(),
                _length: length,
            });
        }
    }

    Ok(Torrent {
        info_hash,
        files,
        _name: name,
    })
}

// Bounds the recursion depth of nested lists/dicts. Without this, a small
// crafted file (e.g. ~100KB of consecutive `l` bytes) can drive the parser's
// recursion deep enough to overflow the stack and abort the process (the
// release profile uses panic = "abort", so any such abort takes the whole
// server down).
const MAX_BENCODE_DEPTH: usize = 64;

fn parse_bencode(bytes: &[u8]) -> Result<(BValue, usize), ()> {
    parse_bencode_depth(bytes, 0)
}

fn parse_bencode_depth(bytes: &[u8], depth: usize) -> Result<(BValue, usize), ()> {
    if depth > MAX_BENCODE_DEPTH {
        return Err(());
    }
    if bytes.is_empty() {
        return Err(());
    }
    match bytes[0] {
        b'i' => {
            let end = bytes.iter().position(|&b| b == b'e').ok_or(())?;
            let s = std::str::from_utf8(bytes.get(1..end).ok_or(())?).map_err(|_| ())?;
            let i = s.parse::<i64>().map_err(|_| ())?;
            let total = end.checked_add(1).ok_or(())?;
            Ok((BValue::Int(i), total))
        }
        b'l' => {
            let mut list = Vec::new();
            let mut offset = 1;
            while offset < bytes.len() && bytes[offset] != b'e' {
                let (val, len) = parse_bencode_depth(&bytes[offset..], depth + 1)?;
                list.push(val);
                offset = offset.checked_add(len).ok_or(())?;
            }
            if offset >= bytes.len() {
                return Err(());
            }
            Ok((BValue::List(list), offset.checked_add(1).ok_or(())?))
        }
        b'd' => {
            let mut map = BTreeMap::new();
            let mut offset = 1;
            while offset < bytes.len() && bytes[offset] != b'e' {
                let (key_val, k_len) = parse_bencode_depth(&bytes[offset..], depth + 1)?;
                offset = offset.checked_add(k_len).ok_or(())?;
                let key = key_val.as_bytes().ok_or(())?.to_vec();

                let (val, v_len) = parse_bencode_depth(&bytes[offset..], depth + 1)?;
                offset = offset.checked_add(v_len).ok_or(())?;
                map.insert(key, val);
            }
            if offset >= bytes.len() {
                return Err(());
            }
            Ok((BValue::Dict(map), offset.checked_add(1).ok_or(())?))
        }
        c if c.is_ascii_digit() => {
            let colon = bytes.iter().position(|&b| b == b':').ok_or(())?;
            let len_str = std::str::from_utf8(bytes.get(0..colon).ok_or(())?).map_err(|_| ())?;
            let len = len_str.parse::<usize>().map_err(|_| ())?;
            let start = colon.checked_add(1).ok_or(())?;
            let end = start.checked_add(len).ok_or(())?;
            let slice = bytes.get(start..end).ok_or(())?;
            Ok((BValue::Str(slice.to_vec()), end))
        }
        _ => Err(()),
    }
}

// Minimal SHA1 implementation to avoid pulling dependencies
struct Sha1 {
    h: [u32; 5],
}

impl Sha1 {
    fn new() -> Self {
        Self {
            h: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0],
        }
    }

    fn update(&mut self, data: &[u8]) {
        let (chunks, _) = data.as_chunks::<64>();
        for chunk in chunks {
            self.process_chunk(chunk);
        }
        // Handle last chunk is done by caller usually, but here we do simple one-shot style or simple chunking?
        // Actually, for full SHA1 we need padding.
    }

    fn process_chunk(&mut self, chunk: &[u8; 64]) {
        let mut w = [0u32; 80];
        for (i, value) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*value);
        }
        for i in 16..80 {
            let x = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
            w[i] = x.rotate_left(1);
        }

        let mut a = self.h[0];
        let mut b = self.h[1];
        let mut c = self.h[2];
        let mut d = self.h[3];
        let mut e = self.h[4];

        for (i, word) in w.iter().copied().enumerate() {
            let (f, k) = if i < 20 {
                ((b & c) | (!b & d), 0x5A827999)
            } else if i < 40 {
                (b ^ c ^ d, 0x6ED9EBA1)
            } else if i < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDC)
            } else {
                (b ^ c ^ d, 0xCA62C1D6)
            };

            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        self.h[0] = self.h[0].wrapping_add(a);
        self.h[1] = self.h[1].wrapping_add(b);
        self.h[2] = self.h[2].wrapping_add(c);
        self.h[3] = self.h[3].wrapping_add(d);
        self.h[4] = self.h[4].wrapping_add(e);
    }
}

pub fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut sha1 = Sha1::new();
    // Padding
    let mut padded = Vec::with_capacity(data.len() + 64);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() + 8) % 64 != 0 {
        padded.push(0);
    }
    let len_bits = (data.len() as u64) * 8;
    padded.extend_from_slice(&len_bits.to_be_bytes());

    sha1.update(&padded);

    let mut res = [0u8; 20];
    for (i, val) in sha1.h.iter().enumerate() {
        let bytes = val.to_be_bytes();
        res[i * 4..(i + 1) * 4].copy_from_slice(&bytes);
    }
    res
}

fn sha1_str(data: &[u8]) -> String {
    let digest = sha1_digest(data);
    hex::encode(digest)
}

fn extract_info_bytes(bytes: &[u8]) -> Option<&[u8]> {
    // Find "4:info"
    let pattern = b"4:info";
    let match_pos = bytes.windows(pattern.len()).position(|w| w == pattern)?;
    let start = match_pos.checked_add(pattern.len())?;

    // Now decode the bencode object starting at 'start' to find its length
    let (_, len) = parse_bencode(bytes.get(start..)?).ok()?;
    let end = start.checked_add(len)?;
    bytes.get(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- sha1_digest against RFC 3174 test vectors ----

    #[test]
    fn sha1_rfc3174_abc() {
        let digest = sha1_digest(b"abc");
        assert_eq!(
            hex::encode(digest),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn sha1_rfc3174_empty() {
        let digest = sha1_digest(b"");
        assert_eq!(
            hex::encode(digest),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
    }

    // ---- Valid torrents ----

    // The info dict's keys are deliberately out of BTreeMap-sorted order
    // ("name" before "length") so the info_hash test below proves the hash
    // is computed over the exact raw bytes of the info dict, not a
    // re-encoding of the parsed (and therefore re-sorted) map.
    const SINGLE_FILE_TORRENT: &[u8] =
        b"d8:announce20:http://example.com/a4:infod4:name8:test.txt6:lengthi12345eee";
    const SINGLE_FILE_INFO_HASH: &str = "e4a6004e656344eae966cc1ae77a6ac2496513b2";

    #[test]
    fn parses_single_file_torrent() {
        let t = parse_torrent(SINGLE_FILE_TORRENT).expect("should parse");
        assert_eq!(t.info_hash, SINGLE_FILE_INFO_HASH);
        assert_eq!(t._name.as_deref(), Some("test.txt"));
        assert_eq!(t.files.len(), 1);
        assert_eq!(t.files[0].path, "test.txt");
        assert_eq!(t.files[0]._length, 12345);
    }

    const MULTI_FILE_TORRENT: &[u8] = b"d4:infod5:filesld6:lengthi100e4:pathl4:dir19:file1.txteed6:lengthi200e4:pathl4:dir19:file2.txteee4:name7:testdiree";
    const MULTI_FILE_INFO_HASH: &str = "5225bac1392f2932e2c80123fd8c825cf02b3a42";

    #[test]
    fn parses_multi_file_torrent() {
        let t = parse_torrent(MULTI_FILE_TORRENT).expect("should parse");
        assert_eq!(t.info_hash, MULTI_FILE_INFO_HASH);
        assert_eq!(t._name.as_deref(), Some("testdir"));
        assert_eq!(t.files.len(), 2);
        assert_eq!(t.files[0].path, "dir1/file1.txt");
        assert_eq!(t.files[0]._length, 100);
        assert_eq!(t.files[1].path, "dir1/file2.txt");
        assert_eq!(t.files[1]._length, 200);
    }

    #[test]
    fn extract_info_bytes_returns_raw_unsorted_slice() {
        // The raw slice must preserve the original ("name" before "length")
        // key order -- a re-encoding of the parsed BTreeMap would sort keys
        // alphabetically and put "length" first, yielding a different hash.
        let info_bytes = extract_info_bytes(SINGLE_FILE_TORRENT).expect("info bytes");
        assert_eq!(info_bytes, b"d4:name8:test.txt6:lengthi12345ee".as_slice());
        assert_eq!(sha1_str(info_bytes), SINGLE_FILE_INFO_HASH);
    }

    // ---- Adversarial inputs must return Err, never panic ----

    #[test]
    fn rejects_truncated_dict() {
        // Dict with a key but no value and no closing 'e'.
        assert!(parse_torrent(b"d3:fooe").is_err());
        assert!(parse_bencode(b"d3:foo").is_err());
    }

    #[test]
    fn rejects_huge_string_length_without_panicking() {
        // usize::MAX as the declared length: start + len must not silently
        // wrap around (in release mode) and bypass the bounds check.
        let evil: &[u8] = b"18446744073709551615:x";
        assert!(parse_bencode(evil).is_err());

        // Same trap, reachable through a full torrent file.
        let evil_torrent: &[u8] = b"d4:infod4:name18446744073709551615:xee";
        assert!(parse_torrent(evil_torrent).is_err());
    }

    #[test]
    fn rejects_negative_and_huge_ints_without_panicking() {
        assert!(parse_bencode(b"i-99999999999999999999999e").is_err());
        assert!(parse_bencode(b"i99999999999999999999999e").is_err());
        // A well-formed negative int within i64 range still parses fine.
        assert!(parse_bencode(b"i-42e").is_ok());
    }

    #[test]
    fn rejects_deeply_nested_lists_without_panicking() {
        // ~100KB of nested, unterminated lists. Unbounded recursion would
        // blow the stack -- and since the release profile uses
        // panic = "abort", that takes the whole server down.
        let depth = 100_000;
        let evil = vec![b'l'; depth];
        assert!(parse_bencode(&evil).is_err());

        let mut evil_torrent = b"d4:info".to_vec();
        evil_torrent.extend(vec![b'l'; depth]);
        assert!(parse_torrent(&evil_torrent).is_err());
    }

    #[test]
    fn rejects_non_utf8_name() {
        // "name" is present but its bytes are not valid UTF-8.
        let mut torrent = b"d4:infod6:lengthi1e4:name3:".to_vec();
        torrent.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        torrent.extend_from_slice(b"ee");
        assert!(parse_torrent(&torrent).is_err());
    }

    #[test]
    fn rejects_missing_info_key() {
        // Well-formed dict, but with no "info" key at all.
        assert!(parse_torrent(b"d8:announce4:teste").is_err());
    }

    #[test]
    fn rejects_empty_and_garbage_input_without_panicking() {
        assert!(parse_torrent(b"").is_err());
        assert!(parse_torrent(b"not bencode at all").is_err());
        assert!(parse_bencode(b"").is_err());
    }

    #[test]
    fn rejects_string_with_no_colon() {
        assert!(parse_bencode(b"12345").is_err());
    }

    #[test]
    fn rejects_dict_key_that_is_not_a_string() {
        // A dict whose "key" is a list instead of a string.
        assert!(parse_bencode(b"dlei1ee").is_err());
    }
}
