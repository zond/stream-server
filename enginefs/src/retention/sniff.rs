//! **Whether a file is an archive, by its content**: what decides that a
//! play session on it shares nothing.
//!
//! The app plays an archive or a disc image in a torrent through a
//! translated source -- the member is served out of it -- so the file
//! itself is never what plays, and archive playback shares nothing. The
//! stream route knows the file's name before a byte is read and answers
//! from it early; a name is a guess (a split `.001`, a disc image named
//! `.img` or `.bin`, a misnamed archive), so a play session's draw also
//! waits until the file's first bytes are held and asks them. Nothing is
//! announced before the draw, so the wait costs nothing.
//!
//! The signatures are the app's own (`xtremio`'s `archive_sniff.dart`,
//! which is what routes a member at all): RAR 1.5 to 5, ZIP, 7-Zip and
//! ISO 9660 -- and beside them the two it cannot sniff but the server can
//! translate, UDF (its volume recognition sequence where ISO 9660's would
//! be) and TAR (a POSIX or GNU `ustar` header whose checksum adds up: the
//! magic alone is six bytes a film could carry by chance, and the checksum
//! is what a tar header always has and a film's bytes do not).
//!
//! And one the app never routed: a Windows executable (`MZ`), which is
//! what a self-extracting ZIP starts with -- its end record is at the tail
//! like any ZIP's, and the stub in front of it is no film. The server's
//! `resolve` sniffs with [`containers`] and asks each named translator in
//! turn, which verifies its own format; a hit nothing indexes is a
//! refusal there, never a film.

/// How many leading bytes [`is_archive`] needs to see all it can: an ISO
/// 9660 or UDF image carries its signature at the start of sector 16,
/// 32769 bytes in; everything else is in the first few hundred.
pub const HEAD_BYTES: u64 = DESCRIPTOR_AT as u64 + 5;

const DESCRIPTOR_AT: usize = 0x8001;
const TAR_MAGIC_AT: usize = 257;
const TAR_CHECKSUM: std::ops::Range<usize> = 148..156;
const TAR_HEADER: usize = 512;

/// Whether `head` -- a file's first bytes, all of them for a file shorter
/// than [`HEAD_BYTES`] -- begins an archive or a disc image.
pub fn is_archive(head: &[u8]) -> bool {
    !containers(head).is_empty()
}

/// A kind of container a head's signature names: which reader to ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    SevenZ,
    /// RAR 1.5 to 5; a later volume of a set carries the same signature.
    Rar,
    /// A ZIP's local header (or its empty-archive or spanned marker), or
    /// a Windows executable, which a self-extracting ZIP is: only its end
    /// record, at the tail, says which.
    Zip,
    Tar,
    /// ISO 9660 (`CD001`) or UDF (`BEA01`).
    DiscImage,
}

/// Every container `head` carries the signature of, in the order a reader
/// is tried: 7z, RAR, ZIP, TAR, disc image. Empty for a film.
pub fn containers(head: &[u8]) -> Vec<Container> {
    let at = |offset: usize, bytes: &[u8]| head.get(offset..offset + bytes.len()) == Some(bytes);
    [
        (
            Container::SevenZ,
            at(0, &[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]),
        ),
        (Container::Rar, at(0, b"Rar!\x1a\x07")),
        (
            Container::Zip,
            (at(0, b"PK")
                && (at(2, &[0x03, 0x04]) || at(2, &[0x05, 0x06]) || at(2, &[0x07, 0x08])))
                || at(0, b"MZ"),
        ),
        (Container::Tar, is_tar(head)),
        (
            Container::DiscImage,
            at(DESCRIPTOR_AT, b"CD001") || at(DESCRIPTOR_AT, b"BEA01"),
        ),
    ]
    .into_iter()
    .filter_map(|(container, carried)| carried.then_some(container))
    .collect()
}

/// Whether `head` begins with a tar header: the POSIX magic (`ustar\0`) or
/// GNU's (`ustar  \0`) at 257, and the octal checksum at 148 equal to the
/// sum of the header's 512 bytes with that field read as spaces -- the
/// unsigned sum, or the signed one some old writers used.
fn is_tar(head: &[u8]) -> bool {
    let Some(header) = head.get(..TAR_HEADER) else {
        return false;
    };
    let magic = &header[TAR_MAGIC_AT..];
    if !magic.starts_with(b"ustar\0") && !magic.starts_with(b"ustar  \0") {
        return false;
    }
    let field = &header[TAR_CHECKSUM];
    let digits: Vec<u8> = field
        .iter()
        .copied()
        .skip_while(|byte| *byte == b' ')
        .take_while(|byte| (b'0'..=b'7').contains(byte))
        .collect();
    let stated = digits
        .iter()
        .fold(0i64, |sum, digit| sum * 8 + i64::from(digit - b'0'));
    let (unsigned, signed) = header
        .iter()
        .enumerate()
        .map(|(at, byte)| match TAR_CHECKSUM.contains(&at) {
            true => (i64::from(b' '), i64::from(b' ')),
            false => (i64::from(*byte), i64::from(*byte as i8)),
        })
        .fold((0, 0), |(u, s), (byte_u, byte_s)| (u + byte_u, s + byte_s));
    stated == unsigned || stated == signed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(at: usize, bytes: &[u8]) -> Vec<u8> {
        let mut head = vec![0u8; HEAD_BYTES as usize];
        head[at..at + bytes.len()].copy_from_slice(bytes);
        head
    }

    /// A tar header for `film.mkv` under `magic`, its checksum written as
    /// tar writes it: six octal digits, a NUL and a space.
    fn tar(magic: &[u8]) -> Vec<u8> {
        tar_named(b"film.mkv", magic, i64::from)
    }

    /// [`tar`] for `name`, summed byte by byte as `as_sum` reads them.
    fn tar_named(name: &[u8], magic: &[u8], as_sum: fn(u8) -> i64) -> Vec<u8> {
        let mut head = with(0, name);
        head[100..108].copy_from_slice(b"0000644\0");
        head[124..136].copy_from_slice(b"00000001000\0");
        head[156] = b'0';
        head[TAR_MAGIC_AT..TAR_MAGIC_AT + magic.len()].copy_from_slice(magic);
        head[TAR_CHECKSUM].copy_from_slice(b"        ");
        let sum: i64 = head[..TAR_HEADER].iter().map(|byte| as_sum(*byte)).sum();
        head[TAR_CHECKSUM].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        head
    }

    /// **Every kind the app routes, and the two it cannot sniff**, found by
    /// its signature wherever the file is named; a film, a short file and
    /// a near miss are not.
    #[test]
    fn an_archive_is_told_by_its_signature() {
        for head in [
            with(0, b"Rar!\x1a\x07\x00"),
            with(0, b"Rar!\x1a\x07\x01\x00"),
            with(0, b"PK\x03\x04"),
            with(0, b"PK\x05\x06"),
            with(0, b"PK\x07\x08"),
            with(0, &[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]),
            with(0, b"MZ"),
            with(DESCRIPTOR_AT, b"CD001"),
            with(DESCRIPTOR_AT, b"BEA01"),
            tar(b"ustar\x0000"),
            tar(b"ustar  \0"),
            // An old writer's signed sum, over a name past ASCII.
            tar_named("filé.mkv".as_bytes(), b"ustar\x0000", |byte| {
                i64::from(byte as i8)
            }),
        ] {
            assert!(is_archive(&head), "{:?}", &head[..8]);
        }
        for head in [
            vec![0u8; HEAD_BYTES as usize],
            with(0, &[0x1A, 0x45, 0xDF, 0xA3]),
            with(0, b"PK\x09\x09"),
            with(0, b"Rar!\x1a"),
            b"Rar".to_vec(),
            Vec::new(),
            // The magic alone, a film's bytes that happen to spell it.
            with(TAR_MAGIC_AT, b"ustar\x0000"),
            // A header whose checksum does not add up.
            {
                let mut head = tar(b"ustar\x0000");
                head[0] = b'g';
                head
            },
            // "ustar" with no terminator: neither magic.
            tar(b"ustarX"),
            // A tar header cut short.
            tar(b"ustar\x0000")[..TAR_HEADER - 1].to_vec(),
        ] {
            assert!(!is_archive(&head), "{:?}", head.get(..8));
        }
    }

    /// **Each signature names its reader**, so `resolve` asks the one
    /// translator that can say whether it is right; an executable is a
    /// self-extracting ZIP until the ZIP reader says otherwise.
    #[test]
    fn a_signature_names_the_container_to_try() {
        for (head, expected) in [
            (
                with(0, &[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]),
                Container::SevenZ,
            ),
            (with(0, b"Rar!\x1a\x07\x01\x00"), Container::Rar),
            (with(0, b"PK\x03\x04"), Container::Zip),
            (with(0, b"MZ"), Container::Zip),
            (tar(b"ustar\x0000"), Container::Tar),
            (with(DESCRIPTOR_AT, b"CD001"), Container::DiscImage),
            (with(DESCRIPTOR_AT, b"BEA01"), Container::DiscImage),
        ] {
            assert_eq!(containers(&head), [expected], "{:?}", &head[..8]);
        }
        assert!(containers(&with(0, &[0x1A, 0x45, 0xDF, 0xA3])).is_empty());
    }
}
