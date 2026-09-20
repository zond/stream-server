//! TAR, read as an index: one 512-byte header per member, and the data
//! skipped by the size the header states.
//!
//! There is no directory at the end of a tar and none is wanted: every
//! entry says how long it is, so the walk steps from header to header
//! without touching a byte of anyone's data. A regular file is always
//! [`Body::Direct`] -- a tar stores what it is given, byte for byte, and
//! that is the whole reason a film ships in one.
//!
//! The two extensions that carry what the 100-byte name field cannot are
//! read as the entries they are: GNU's `L` type, whose data *is* the next
//! entry's name, and POSIX's `x` type, whose data is a run of
//! `len key=value\n` records of which `path` and `size` matter here. Both
//! are index data, so reading them is reading the index.
//!
//! **`tar.gz` is not this**: see [`super::TarGz`]. A gzip stream has no way
//! in at the middle, so it is refused whole rather than indexed.

use super::{Body, Budget, Index, Member, Refusal, Translator, direct_extent, only_source};
use crate::sources::ByteSource;
use async_trait::async_trait;
use std::sync::Arc;

/// Everything in a tar is a multiple of this, headers included.
const BLOCK: u64 = 512;

/// Where the fields this reads are, in a header block.
const NAME: std::ops::Range<usize> = 0..100;
const SIZE: std::ops::Range<usize> = 124..136;
const CHECKSUM: std::ops::Range<usize> = 148..156;
const TYPE_FLAG: usize = 156;
const MAGIC: std::ops::Range<usize> = 257..263;
const PREFIX: std::ops::Range<usize> = 345..500;

pub struct Tar;

#[async_trait]
impl Translator for Tar {
    fn format(&self) -> &'static str {
        "tar"
    }

    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        let source = only_source(sources, "tar")?;
        let mut budget = Budget::new(source.as_ref(), "tar");
        let total = budget.source_len();
        let mut members = Vec::new();
        let mut at = 0u64;
        // What the last `L` or `x` entry said about the entry after it.
        let mut pending_name: Option<String> = None;
        let mut pending_size: Option<u64> = None;
        let mut seen_a_header = false;

        while at + BLOCK <= total {
            let header = budget.read_exact(at, BLOCK as usize).await?;
            if header.iter().all(|byte| *byte == 0) {
                // The two zero blocks that end a tar. Anything after them
                // is padding, which is why the walk stops rather than
                // looking for more.
                break;
            }
            // The one checksum this design does check: it is the header's
            // own, over 512 bytes that have just been read, and it is what
            // tells a tar from a file that happens to have bytes here.
            // A member's CRC would be a read of the member, which is the
            // thing translators do not do.
            if !checksum_matches(&header) {
                return Err(budget.malformed(format!(
                    "the header at {at} does not check out, so this is not a tar"
                )));
            }
            seen_a_header = true;
            let size = octal(&header[SIZE]).ok_or_else(|| {
                budget.malformed(format!("the size at {at} is not an octal number"))
            })?;
            let size = pending_size.take().unwrap_or(size);
            let data_at = at + BLOCK;
            let padded = size.checked_next_multiple_of(BLOCK).ok_or_else(|| {
                budget.malformed(format!("the entry at {at} claims {size} bytes"))
            })?;
            let next = data_at.checked_add(padded).ok_or_else(|| {
                budget.malformed(format!("the entry at {at} claims {size} bytes"))
            })?;
            if next > total {
                return Err(budget.malformed(format!(
                    "the entry at {at} claims {size} bytes, which is past the end of a \
                     {total}-byte tar"
                )));
            }
            let flag = header[TYPE_FLAG];
            match flag {
                // GNU long name: this entry's data is the next entry's
                // name, and the next entry's own name field is a stub.
                b'L' => {
                    let data = budget.read_exact(data_at, size as usize).await?;
                    pending_name = Some(trim_nul(&data));
                }
                // A PAX extended header: `path` overrides the name and
                // `size` the length, which is how a tar carries a file
                // larger than the octal field can say.
                b'x' | b'X' => {
                    let data = budget.read_exact(data_at, size as usize).await?;
                    let (path, pax_size) = pax_fields(&data);
                    if let Some(path) = path {
                        pending_name = Some(path);
                    }
                    pending_size = pax_size;
                }
                // A regular file, which is what this is all for. The old
                // `\0` spelling is as common as `0` in tars written by
                // hand.
                b'0' | b'\0' => {
                    let name = pending_name.take().unwrap_or_else(|| name_of(&header));
                    let extents = match direct_extent(source.as_ref(), "tar", &name, data_at, size)
                    {
                        Ok(extents) => Body::Direct(extents),
                        Err(refusal) => Body::Opaque(refusal),
                    };
                    members.push(Member {
                        name,
                        len: size,
                        body: extents,
                    });
                }
                // Directories, links, devices, the GNU long *link* name:
                // none of them is a file with bytes to serve, and each one
                // consumes whatever name was pending for it.
                _ => {
                    pending_name = None;
                }
            }
            if flag != b'L' && flag != b'x' && flag != b'X' {
                pending_size = None;
            }
            at = next;
        }

        if !seen_a_header {
            return Err(Refusal::Malformed(
                "there is no tar header at the start of this file, so it is not a tar".to_string(),
            ));
        }
        Ok(Index { members })
    }
}

/// The name as the header spells it: the `ustar` prefix field joined to
/// the name field, which is how a path longer than 100 bytes is carried
/// without a GNU extension.
fn name_of(header: &[u8]) -> String {
    let name = trim_nul(&header[NAME]);
    if &header[MAGIC] == b"ustar\0" || &header[MAGIC] == b"ustar " {
        let prefix = trim_nul(&header[PREFIX]);
        if !prefix.is_empty() {
            return format!("{prefix}/{name}");
        }
    }
    name
}

/// A NUL-padded field as a string, lossily: a name that is not UTF-8 still
/// names a member, and one nobody asks for by that name is never selected.
fn trim_nul(field: &[u8]) -> String {
    let end = field
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// A tar's numbers are octal in ASCII, NUL- or space-terminated. The GNU
/// base-256 form (high bit set) is the other spelling, and it is read too:
/// it is what carries a member over 8 GiB.
fn octal(field: &[u8]) -> Option<u64> {
    if field.first().is_some_and(|byte| byte & 0x80 != 0) {
        let mut value: u64 = u64::from(field[0] & 0x7f);
        for byte in &field[1..] {
            value = value.checked_mul(256)?.checked_add(u64::from(*byte))?;
        }
        return Some(value);
    }
    let text = field
        .iter()
        .take_while(|byte| **byte != 0 && **byte != b' ')
        .map(|byte| *byte as char)
        .collect::<String>();
    let text = text.trim();
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(text, 8).ok()
}

/// A header's checksum is the sum of its bytes with the checksum field
/// read as spaces. Both the signed and the unsigned sum are accepted,
/// because both have been written by real archivers.
fn checksum_matches(header: &[u8]) -> bool {
    let Some(stated) = octal(&header[CHECKSUM]) else {
        return false;
    };
    let mut unsigned: u64 = 0;
    let mut signed: i64 = 0;
    for (at, byte) in header.iter().enumerate() {
        let byte = if CHECKSUM.contains(&at) { b' ' } else { *byte };
        unsigned += u64::from(byte);
        signed += i64::from(byte as i8);
    }
    stated == unsigned || i64::try_from(stated).is_ok_and(|stated| stated == signed)
}

/// `path` and `size` out of a PAX extended header's `len key=value\n`
/// records. Everything else in one is metadata this does not serve.
fn pax_fields(data: &[u8]) -> (Option<String>, Option<u64>) {
    let mut path = None;
    let mut size = None;
    let mut rest = data;
    while !rest.is_empty() {
        let space = match rest.iter().position(|byte| *byte == b' ') {
            Some(space) => space,
            None => break,
        };
        let Ok(len) = std::str::from_utf8(&rest[..space])
            .unwrap_or("")
            .parse::<usize>()
        else {
            break;
        };
        if len == 0 || len > rest.len() {
            break;
        }
        let record = &rest[space + 1..len];
        if let Some(equals) = record.iter().position(|byte| *byte == b'=') {
            let key = &record[..equals];
            let value = String::from_utf8_lossy(&record[equals + 1..])
                .trim_end_matches('\n')
                .to_string();
            match key {
                b"path" => path = Some(value),
                b"size" => size = value.parse().ok(),
                _ => {}
            }
        }
        rest = &rest[len..];
    }
    (path, size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::Extent;
    use crate::sources::testing::{CountingSource, MemorySource};

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i.wrapping_mul(37) % 251) as u8).collect()
    }

    /// A tar of `members`, written by the `tar` crate -- the one this
    /// server already depends on, and not the parser under test.
    fn tar_of(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut builder = ::tar::Builder::new(Vec::new());
        for (name, data) in members {
            let mut header = ::tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, data.as_slice())
                .expect("append");
        }
        builder.into_inner().expect("finish")
    }

    fn sources(bytes: Vec<u8>) -> Vec<Arc<dyn ByteSource>> {
        vec![Arc::new(MemorySource::new("fixture.tar", bytes))]
    }

    /// Every member is one range of the archive, at the offset its bytes
    /// really are -- checked against the archive, not against the walk
    /// that produced it.
    #[tokio::test]
    async fn every_member_is_the_range_of_the_archive_its_bytes_are_at() {
        let first = b"hello from the first entry\n".to_vec();
        let second = payload(40 * 1024);
        let archive = tar_of(&[
            ("first.txt", first.clone()),
            ("videos/second.bin", second.clone()),
            ("empty.txt", Vec::new()),
        ]);
        let index = Tar.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(
            index
                .members
                .iter()
                .map(|member| member.name.as_str())
                .collect::<Vec<_>>(),
            ["first.txt", "videos/second.bin", "empty.txt"]
        );
        for (member, data) in index.members.iter().zip([first, second, Vec::new()]) {
            assert_eq!(member.len, data.len() as u64);
            let Body::Direct(extents) = &member.body else {
                panic!("a tar member is direct: {:?}", member.body);
            };
            assert_eq!(extents.len(), 1);
            let Extent { offset, len, .. } = extents[0];
            assert_eq!(len, data.len() as u64);
            assert_eq!(
                &archive[offset as usize..(offset + len) as usize],
                &data[..]
            );
        }
    }

    /// A name too long for the 100-byte field comes from the entry that
    /// carries it, whichever of the two spellings wrote it.
    #[tokio::test]
    async fn a_long_name_is_read_from_the_entry_that_carries_it() {
        let long = format!("videos/{}/movie.mkv", "d".repeat(120));
        let data = payload(1024);
        let archive = tar_of(&[(long.as_str(), data.clone())]);
        let index = Tar.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(index.members.len(), 1);
        assert_eq!(index.members[0].name, long);
        let Body::Direct(extents) = &index.members[0].body else {
            panic!("direct");
        };
        let (offset, len) = (extents[0].offset, extents[0].len);
        assert_eq!(
            &archive[offset as usize..(offset + len) as usize],
            &data[..]
        );
    }

    /// A PAX header names the entry after it, and its data is index data:
    /// read, and not mistaken for a member of its own.
    #[tokio::test]
    async fn a_pax_header_names_the_entry_after_it() {
        let name = "videos/a name the header cannot hold/movie.mkv";
        let data = payload(2048);
        let mut archive = Vec::new();
        let record = pax_record("path", name);
        archive.extend_from_slice(&pax_block(record.as_bytes()));
        archive.extend_from_slice(record.as_bytes());
        archive.resize(archive.len().next_multiple_of(512), 0);
        // The entry itself, under a stub name, as a PAX writer emits it.
        let mut header = ::tar::Header::new_ustar();
        header.set_path("stub").unwrap();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.extend_from_slice(header.as_bytes());
        archive.extend_from_slice(&data);
        archive.resize(archive.len().next_multiple_of(512), 0);
        archive.extend_from_slice(&[0u8; 1024]);

        let index = Tar.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(index.members.len(), 1, "{:?}", index.members);
        assert_eq!(index.members[0].name, name);
        let Body::Direct(extents) = &index.members[0].body else {
            panic!("direct");
        };
        let (offset, len) = (extents[0].offset, extents[0].len);
        assert_eq!(
            &archive[offset as usize..(offset + len) as usize],
            &data[..]
        );
    }

    /// One PAX record, `len key=value\n`, where `len` counts itself --
    /// so it is found by trying, exactly as the writers do.
    fn pax_record(key: &str, value: &str) -> String {
        let mut len = key.len() + value.len() + 3;
        loop {
            let record = format!("{len} {key}={value}\n");
            if record.len() == len {
                return record;
            }
            len = record.len();
        }
    }

    /// The header block of a PAX extended header holding `record`.
    fn pax_block(record: &[u8]) -> [u8; 512] {
        let mut header = ::tar::Header::new_ustar();
        header.set_path("PaxHeaders/stub").unwrap();
        header.set_size(record.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(::tar::EntryType::XHeader);
        header.set_cksum();
        let mut block = [0u8; 512];
        block.copy_from_slice(header.as_bytes());
        block
    }

    /// **The index reads the headers and not the films.** By range, so a
    /// walk that read a member in small pieces fails this too.
    #[tokio::test]
    async fn indexing_reads_one_header_per_member_and_no_data() {
        const LEN: usize = 256 * 1024;
        let data = payload(LEN);
        let archive = tar_of(&[
            ("first.txt", b"small".to_vec()),
            ("videos/movie.mkv", data.clone()),
        ]);
        let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "fixture.tar",
            archive.clone(),
        ))));
        let counts = counting.counts();
        let index = Tar
            .index(&[counting.clone() as Arc<dyn ByteSource>])
            .await
            .expect("indexed");
        let Body::Direct(extents) = &index.members[1].body else {
            panic!("direct");
        };
        let (at, len) = (extents[0].offset, extents[0].len);
        assert!(
            !counts.read_any_of(at, len),
            "the index read the member's own data: {:?}",
            counts.ranges()
        );
        // One 512-byte header per entry, and the two zero blocks that end
        // the archive. Nothing was opened: an index is `read_at`s.
        assert!(
            counts.read_at_bytes() <= 4 * BLOCK,
            "{} bytes read for two headers",
            counts.read_at_bytes()
        );
        assert_eq!(counts.opens(), 0);
    }

    /// Bytes that are not a tar are refused as such, rather than walked
    /// into nonsense: the first header has to check out.
    #[tokio::test]
    async fn bytes_that_are_not_a_tar_are_refused() {
        let refusal = Tar
            .index(&sources(payload(4096)))
            .await
            .expect_err("not a tar");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// An entry claiming more bytes than the archive holds is a truncated
    /// archive, and is said so rather than pointed at.
    #[tokio::test]
    async fn an_entry_past_the_end_is_malformed() {
        let mut archive = tar_of(&[("movie.mkv", payload(1024))]);
        let mut header = ::tar::Header::new_gnu();
        header.set_size(1 << 40);
        header.set_mode(0o644);
        header.set_cksum();
        archive[..512].copy_from_slice(header.as_bytes());
        // The name field went with the header; only the size matters here.
        let refusal = Tar.index(&sources(archive)).await.expect_err("truncated");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    #[test]
    fn numbers_are_read_in_both_spellings() {
        assert_eq!(octal(b"0000002000\0 "), Some(0o2000));
        assert_eq!(octal(b"           \0"), Some(0));
        // GNU base 256, which is what carries a member over 8 GiB.
        let mut big = [0u8; 12];
        big[0] = 0x80;
        big[11] = 9;
        assert_eq!(octal(&big), Some(9));
        assert_eq!(octal(b"not octal   "), None);
    }
}
