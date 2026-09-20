//! RAR archives built by hand, for tests that have no `rar` binary.
//!
//! `unrar-rs` deliberately exposes no writer and the RARLAB `rar` tool is
//! not on a build machine, so every RAR this suite reads is assembled here
//! from the format's own technote: signature, main header, one file header
//! plus data area per member (per *part* of a member, in a split set), end
//! header. Both generations are built -- RAR5, which is what `rar` has
//! written since 2013, and RAR4, which is what most of the scene archives
//! still in circulation are -- and both single volumes and sets.
//!
//! Shared by `#[path]` between the translator's unit tests and the
//! integration tests, which is why nothing here is `#[cfg(test)]` and why
//! the unused-function lint is off: each includer uses the part of it that
//! its tests are about.
//!
//! Where `unrar` is installed the translator's tests hand these archives to
//! it (`unrar t`), so what is asserted about them is asserted about archives
//! a real reader accepts, checksums and all.

#![allow(dead_code)]

/// Deterministic, mildly incompressible bytes: a member a test can find in
/// the archive by value, and one whose every 251-byte window differs from
/// every other, so a range served from the wrong offset is caught.
pub fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(31) % 251) as u8).collect()
}

/// [`payload`] with the offset stamped into every kibibyte of it.
///
/// For the multi-volume tests: a member spread over three volumes is three
/// extents of three different files, and a byte served out of the wrong
/// one is then a wrong *sentence* -- `[offset 000099328]` where
/// `[offset 000199680]` was asked for -- rather than a subtle byte
/// somebody has to decode a diff to see. Between the stamps the payload's
/// own every-251-bytes-differs property catches an offset that is wrong by
/// less than a kibibyte.
pub fn signposted(len: usize) -> Vec<u8> {
    let mut bytes = payload(len);
    let mut at = 0;
    while at + 20 <= len {
        let stamp = format!("[offset {at:09}]");
        bytes[at..at + stamp.len()].copy_from_slice(stamp.as_bytes());
        at += 1024;
    }
    bytes
}

/// Standard reflected CRC-32 (ISO-HDLC / zlib), as both RAR generations
/// store for headers and data. Table-less so the fixture pulls in nothing.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// How a member's bytes were packed. Only `Store` is served; `Normal` is
/// what `rar` uses by default and what a test about refusal asks for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Store,
    Normal,
}

/// Which checksum a RAR5 file header carries for the member: the CRC32 in
/// the header proper, a BLAKE2sp digest in an extra record (`rar -htb`)
/// and no CRC32 at all, or nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Hash {
    Crc32,
    Blake2,
    None,
}

/// What a RAR5 archive is written with, beyond its members.
#[derive(Clone, Copy)]
pub struct Rar5Options {
    pub method: Method,
    pub hash: Hash,
    /// The archive-level solid flag in the main header, plus the per-member
    /// solid bit on every member after the first -- both, as `rar -s` sets
    /// them.
    pub solid: bool,
    /// The per-member solid bit alone, on every member after the first,
    /// with the archive-level flag clear: not a shape `rar` writes, but a
    /// shape the format allows.
    pub solid_members: bool,
    /// Every member carries a file-encryption record and its data area is
    /// the AES-padded length (`rar -p`): the headers are readable, the
    /// bytes are ciphertext.
    pub encrypted_members: bool,
}

impl Default for Rar5Options {
    fn default() -> Self {
        Self {
            method: Method::Store,
            hash: Hash::Crc32,
            solid: false,
            solid_members: false,
            encrypted_members: false,
        }
    }
}

const RAR5_SIGNATURE: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

/// RAR5 header types.
const RAR5_MAIN: u64 = 1;
const RAR5_FILE: u64 = 2;
const RAR5_ENCRYPTION: u64 = 4;
const RAR5_END: u64 = 5;

/// RAR5 common header flags.
const RAR5_EXTRA_AREA: u64 = 0x0001;
const RAR5_DATA_AREA: u64 = 0x0002;
const RAR5_SPLIT_BEFORE: u64 = 0x0008;
const RAR5_SPLIT_AFTER: u64 = 0x0010;

/// RAR5 main header archive flags.
const RAR5_VOLUME: u64 = 0x0001;
const RAR5_VOLUME_NUMBER: u64 = 0x0002;
const RAR5_SOLID: u64 = 0x0004;

/// RAR5 file header flags.
const RAR5_DIRECTORY: u64 = 0x0001;
const RAR5_CRC32_PRESENT: u64 = 0x0004;

/// RAR5 end header flags.
const RAR5_MORE_VOLUMES: u64 = 0x0001;

/// RAR5 extra record types.
const RAR5_EXTRA_FILE_ENCRYPTION: u64 = 0x01;
const RAR5_EXTRA_FILE_HASH: u64 = 0x02;

/// A RAR5 variable-length integer: seven bits per byte, low first, the top
/// bit saying another follows.
pub fn encode_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

/// Any RAR5 header: `crc32(size_vint || body) || size_vint || body`, where
/// the body is the type, the common flags, the extra-area and data-area
/// sizes their flags announce, the type's own fields, and the extra area.
fn rar5_header(
    header_type: u64,
    mut flags: u64,
    data_size: Option<u64>,
    type_body: &[u8],
    extra: &[u8],
) -> Vec<u8> {
    if !extra.is_empty() {
        flags |= RAR5_EXTRA_AREA;
    }
    if data_size.is_some() {
        flags |= RAR5_DATA_AREA;
    }
    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(header_type));
    body.extend_from_slice(&encode_vint(flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_vint(extra.len() as u64));
    }
    if let Some(size) = data_size {
        body.extend_from_slice(&encode_vint(size));
    }
    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let size = encode_vint(body.len() as u64);
    let mut crc_input = size.clone();
    crc_input.extend_from_slice(&body);
    let mut header = crc32(&crc_input).to_le_bytes().to_vec();
    header.extend_from_slice(&size);
    header.extend_from_slice(&body);
    header
}

/// One extra record: `size || type || data`, size covering the type.
fn rar5_extra_record(record_type: u64, data: &[u8]) -> Vec<u8> {
    let mut record = encode_vint(record_type);
    record.extend_from_slice(data);
    let mut out = encode_vint(record.len() as u64);
    out.extend_from_slice(&record);
    out
}

/// The main header of volume `volume` -- `None` for an archive that is not
/// a set. A set's first volume carries the volume flag and no number, as
/// `rar` writes it; every later one states its number.
fn rar5_main_header(volume: Option<u64>, solid: bool) -> Vec<u8> {
    let mut flags = 0;
    let mut body = Vec::new();
    match volume {
        None => {}
        Some(0) => flags |= RAR5_VOLUME,
        Some(number) => {
            flags |= RAR5_VOLUME | RAR5_VOLUME_NUMBER;
            body.extend_from_slice(&encode_vint(number));
        }
    }
    if solid {
        flags |= RAR5_SOLID;
    }
    let mut type_body = encode_vint(flags);
    type_body.extend_from_slice(&body);
    rar5_header(RAR5_MAIN, 0, None, &type_body, &[])
}

/// The end header of a volume, saying whether another follows. Public so
/// a test can rewrite a volume's end: the header is the last thing in it.
pub fn rar5_end_header(more_volumes: bool) -> Vec<u8> {
    let flags = if more_volumes { RAR5_MORE_VOLUMES } else { 0 };
    rar5_header(RAR5_END, 0, None, &encode_vint(flags), &[])
}

/// One part of a member, as its file header: `part_len` bytes of data
/// follow it, out of a member `total_len` long. `crc` is what a real
/// writer puts there: the CRC of this part's packed bytes while the member
/// continues into the next volume, and of the whole member in its last
/// part.
#[allow(clippy::too_many_arguments)]
fn rar5_file_header(
    name: &str,
    total_len: u64,
    crc: u32,
    part_len: u64,
    split_before: bool,
    split_after: bool,
    solid_member: bool,
    opts: &Rar5Options,
) -> Vec<u8> {
    // A name ending in `/` is a directory entry: the flag says so, the
    // name loses the slash, and there is no data area.
    let directory = name.ends_with('/');
    let name = name.trim_end_matches('/');
    let mut file_flags = 0;
    if opts.hash == Hash::Crc32 {
        file_flags |= RAR5_CRC32_PRESENT;
    }
    if directory {
        file_flags |= RAR5_DIRECTORY;
    }
    let mut type_body = encode_vint(file_flags);
    type_body.extend_from_slice(&encode_vint(total_len));
    type_body.extend_from_slice(&encode_vint(0)); // attributes
    if opts.hash == Hash::Crc32 {
        type_body.extend_from_slice(&crc.to_le_bytes());
    }
    // Compression info: bits 0-5 the version, bit 6 the member's solid bit,
    // bits 7-9 the method, bits 10-13 the dictionary size.
    let method = match opts.method {
        Method::Store => 0u64,
        Method::Normal => 3,
    };
    let compression = (method << 7) | if solid_member { 0x40 } else { 0 };
    type_body.extend_from_slice(&encode_vint(compression));
    type_body.extend_from_slice(&encode_vint(1)); // host OS: Unix
    type_body.extend_from_slice(&encode_vint(name.len() as u64));
    type_body.extend_from_slice(name.as_bytes());

    let mut extra = Vec::new();
    if opts.encrypted_members {
        // Version 0 (AES-256), no password check, KDF count 2^15, a salt
        // and an IV: the record `rar -p` writes, minus the check value.
        let mut record = encode_vint(0);
        record.extend_from_slice(&encode_vint(0));
        record.push(15);
        record.extend_from_slice(&[0x5a; 16]);
        record.extend_from_slice(&[0xa5; 16]);
        extra.extend_from_slice(&rar5_extra_record(RAR5_EXTRA_FILE_ENCRYPTION, &record));
    }
    if opts.hash == Hash::Blake2 {
        // Hash type 0 is BLAKE2sp; the digest is not checked by anything
        // this suite drives, so it is a stand-in.
        let mut record = encode_vint(0);
        record.extend_from_slice(&[0x42; 32]);
        extra.extend_from_slice(&rar5_extra_record(RAR5_EXTRA_FILE_HASH, &record));
    }

    let mut flags = 0;
    if split_before {
        flags |= RAR5_SPLIT_BEFORE;
    }
    if split_after {
        flags |= RAR5_SPLIT_AFTER;
    }
    let data_size = (!directory).then_some(part_len);
    rar5_header(RAR5_FILE, flags, data_size, &type_body, &extra)
}

/// A single-volume RAR5 archive of `members`.
pub fn rar5_archive(members: &[(&str, &[u8])], opts: &Rar5Options) -> Vec<u8> {
    let mut archive = RAR5_SIGNATURE.to_vec();
    archive.extend_from_slice(&rar5_main_header(None, opts.solid));
    for (at, (name, data)) in members.iter().enumerate() {
        let stored_len = if opts.encrypted_members {
            (data.len() as u64).next_multiple_of(16)
        } else {
            data.len() as u64
        };
        archive.extend_from_slice(&rar5_file_header(
            name,
            data.len() as u64,
            crc32(data),
            stored_len,
            false,
            false,
            (opts.solid || opts.solid_members) && at > 0,
            opts,
        ));
        archive.extend_from_slice(data);
        archive.resize(archive.len() + (stored_len as usize - data.len()), 0);
    }
    archive.extend_from_slice(&rar5_end_header(false));
    archive
}

/// A stored, single-volume RAR5 archive: the ordinary case.
pub fn rar5_stored(members: &[(&str, &[u8])]) -> Vec<u8> {
    rar5_archive(members, &Rar5Options::default())
}

/// A RAR5 set: `members`, stored, split into volumes of at most
/// `data_per_volume` bytes of member data each, the way `rar -v` splits a
/// film -- a member that does not fit continues in the next volume under
/// a header of its own, and the volumes say so at both ends of the split.
pub fn rar5_volumes(members: &[(&str, &[u8])], data_per_volume: usize) -> Vec<Vec<u8>> {
    assert!(data_per_volume > 0);
    let opts = Rar5Options::default();
    let mut volumes = Vec::new();
    let mut number = 0u64;
    let mut current = RAR5_SIGNATURE.to_vec();
    current.extend_from_slice(&rar5_main_header(Some(number), false));
    let mut room = data_per_volume;
    for (name, data) in members {
        let mut at = 0usize;
        let mut split_before = false;
        loop {
            if room == 0 {
                current.extend_from_slice(&rar5_end_header(true));
                volumes.push(std::mem::take(&mut current));
                number += 1;
                current = RAR5_SIGNATURE.to_vec();
                current.extend_from_slice(&rar5_main_header(Some(number), false));
                room = data_per_volume;
            }
            let end = (at + room).min(data.len());
            let last = end == data.len();
            let crc = if last {
                crc32(data)
            } else {
                crc32(&data[at..end])
            };
            current.extend_from_slice(&rar5_file_header(
                name,
                data.len() as u64,
                crc,
                (end - at) as u64,
                split_before,
                !last,
                false,
                &opts,
            ));
            current.extend_from_slice(&data[at..end]);
            room -= end - at;
            at = end;
            split_before = true;
            if last {
                break;
            }
        }
    }
    current.extend_from_slice(&rar5_end_header(false));
    volumes.push(current);
    volumes
}

/// A RAR5 archive whose headers are encrypted (`rar -hp`): the encryption
/// header is in the clear and everything after it is ciphertext, so no
/// member can be named without the password.
pub fn rar5_header_encrypted() -> Vec<u8> {
    let mut archive = RAR5_SIGNATURE.to_vec();
    // Version 0, no password check, KDF count 2^15, a salt.
    let mut body = encode_vint(0);
    body.extend_from_slice(&encode_vint(0));
    body.push(15);
    body.extend_from_slice(&[0x33; 16]);
    archive.extend_from_slice(&rar5_header(RAR5_ENCRYPTION, 0, None, &body, &[]));
    // What follows is ciphertext to a reader without the key.
    archive.extend_from_slice(&payload(256));
    archive
}

// ---- RAR4 -----------------------------------------------------------------

const RAR4_SIGNATURE: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];

/// RAR4 block types.
const RAR4_MAIN: u8 = 0x73;
const RAR4_FILE: u8 = 0x74;
const RAR4_END: u8 = 0x7b;

/// RAR4 main header flags.
const RAR4_VOLUME: u16 = 0x0001;
const RAR4_SOLID: u16 = 0x0008;
const RAR4_NEW_NUMBERING: u16 = 0x0010;
const RAR4_ENCRYPTED_HEADERS: u16 = 0x0080;
const RAR4_FIRST_VOLUME: u16 = 0x0100;

/// RAR4 file header flags.
const RAR4_SPLIT_BEFORE: u16 = 0x0001;
const RAR4_SPLIT_AFTER: u16 = 0x0002;
const RAR4_FILE_ENCRYPTED: u16 = 0x0004;
const RAR4_HAS_DATA: u16 = 0x8000;

/// RAR4 end header flags.
const RAR4_NEXT_VOLUME: u16 = 0x0001;
const RAR4_VOLUME_NUMBER: u16 = 0x0004;

/// Any RAR4 block: `crc16 || type || flags || size || body`, the CRC being
/// the low half of a CRC32 over everything after it.
fn rar4_block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
    let size = (7 + body.len()) as u16;
    let mut block = vec![0, 0, kind];
    block.extend_from_slice(&flags.to_le_bytes());
    block.extend_from_slice(&size.to_le_bytes());
    block.extend_from_slice(body);
    let crc = (crc32(&block[2..]) & 0xFFFF) as u16;
    block[..2].copy_from_slice(&crc.to_le_bytes());
    block
}

fn rar4_main_header(flags: u16) -> Vec<u8> {
    // HighPosAV and PosAV, both zero: no authenticity information.
    rar4_block(RAR4_MAIN, flags, &[0u8; 6])
}

/// One part of a member under a RAR4 file header; `crc` as for RAR5.
/// `unpack_version` is 29 for everything `rar` 3.x and later wrote; an
/// older one selects an older cipher for an encrypted member.
#[allow(clippy::too_many_arguments)]
fn rar4_file_header(
    name: &str,
    total_len: u32,
    crc: u32,
    part_len: u32,
    split_before: bool,
    split_after: bool,
    method: Method,
    unpack_version: u8,
    encrypted: bool,
) -> Vec<u8> {
    let mut flags = RAR4_HAS_DATA;
    if split_before {
        flags |= RAR4_SPLIT_BEFORE;
    }
    if split_after {
        flags |= RAR4_SPLIT_AFTER;
    }
    if encrypted {
        flags |= RAR4_FILE_ENCRYPTED;
    }
    let mut body = Vec::new();
    body.extend_from_slice(&part_len.to_le_bytes()); // packed size
    body.extend_from_slice(&total_len.to_le_bytes()); // unpacked size
    body.push(3); // host OS: Unix
    body.extend_from_slice(&crc.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // DOS time
    body.push(unpack_version);
    body.push(match method {
        Method::Store => 0x30,
        Method::Normal => 0x33,
    });
    body.extend_from_slice(&(name.len() as u16).to_le_bytes());
    body.extend_from_slice(&0x20u32.to_le_bytes()); // attributes
    body.extend_from_slice(name.as_bytes());
    rar4_block(RAR4_FILE, flags, &body)
}

fn rar4_end_header(more_volumes: bool, volume: Option<u32>) -> Vec<u8> {
    let mut flags = 0;
    let mut body = Vec::new();
    if more_volumes {
        flags |= RAR4_NEXT_VOLUME;
    }
    if let Some(number) = volume {
        flags |= RAR4_VOLUME_NUMBER;
        body.extend_from_slice(&number.to_le_bytes());
    }
    rar4_block(RAR4_END, flags, &body)
}

/// A single-volume RAR4 archive of `members`, with `main_flags` beyond the
/// defaults (solid, header encryption).
pub fn rar4_archive(members: &[(&str, &[u8])], method: Method, main_flags: u16) -> Vec<u8> {
    let mut archive = RAR4_SIGNATURE.to_vec();
    archive.extend_from_slice(&rar4_main_header(main_flags));
    for (name, data) in members {
        archive.extend_from_slice(&rar4_file_header(
            name,
            data.len() as u32,
            crc32(data),
            data.len() as u32,
            false,
            false,
            method,
            29,
            false,
        ));
        archive.extend_from_slice(data);
    }
    archive.extend_from_slice(&rar4_end_header(false, None));
    archive
}

/// A RAR4 archive whose members are encrypted with the RAR 2.0 cipher
/// (unpack version 20): encryption the format has that is not AES, and
/// so not one whose bytes can be mapped by block.
pub fn rar4_legacy_encrypted(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut archive = RAR4_SIGNATURE.to_vec();
    archive.extend_from_slice(&rar4_main_header(0));
    for (name, data) in members {
        archive.extend_from_slice(&rar4_file_header(
            name,
            data.len() as u32,
            crc32(data),
            data.len() as u32,
            false,
            false,
            Method::Store,
            20,
            true,
        ));
        archive.extend_from_slice(data);
    }
    archive.extend_from_slice(&rar4_end_header(false, None));
    archive
}

/// A stored, single-volume RAR4 archive.
pub fn rar4_stored(members: &[(&str, &[u8])]) -> Vec<u8> {
    rar4_archive(members, Method::Store, 0)
}

/// A solid RAR4 archive (`rar -s`).
pub fn rar4_solid(members: &[(&str, &[u8])]) -> Vec<u8> {
    rar4_archive(members, Method::Normal, RAR4_SOLID)
}

/// A RAR4 archive whose headers are encrypted (`rar -hp`): the main header
/// says so and is the last thing readable.
pub fn rar4_header_encrypted() -> Vec<u8> {
    let mut archive = RAR4_SIGNATURE.to_vec();
    archive.extend_from_slice(&rar4_main_header(RAR4_ENCRYPTED_HEADERS));
    archive.extend_from_slice(&payload(256));
    archive
}

/// A RAR4 set, split as [`rar5_volumes`] splits, under either numbering:
/// `new_numbering` for `.partN.rar` names (the main header says so), off
/// for `.rar`, `.r00`, `.r01`. Every volume's end header states its number,
/// as `rar` 3.x wrote them.
pub fn rar4_volumes(
    members: &[(&str, &[u8])],
    data_per_volume: usize,
    new_numbering: bool,
) -> Vec<Vec<u8>> {
    rar4_volumes_with(members, data_per_volume, new_numbering, true)
}

/// [`rar4_volumes`] without a volume number anywhere, which is what an
/// old `.rar`/`.r00` set states: nothing but the split flags.
pub fn rar4_volumes_unnumbered(members: &[(&str, &[u8])], data_per_volume: usize) -> Vec<Vec<u8>> {
    rar4_volumes_with(members, data_per_volume, false, false)
}

fn rar4_volumes_with(
    members: &[(&str, &[u8])],
    data_per_volume: usize,
    new_numbering: bool,
    numbered: bool,
) -> Vec<Vec<u8>> {
    assert!(data_per_volume > 0);
    let main_flags = |first: bool| {
        let mut flags = RAR4_VOLUME;
        if first {
            flags |= RAR4_FIRST_VOLUME;
        }
        if new_numbering {
            flags |= RAR4_NEW_NUMBERING;
        }
        flags
    };
    let mut volumes = Vec::new();
    let mut number = 0u32;
    let mut current = RAR4_SIGNATURE.to_vec();
    current.extend_from_slice(&rar4_main_header(main_flags(true)));
    let mut room = data_per_volume;
    for (name, data) in members {
        let mut at = 0usize;
        let mut split_before = false;
        loop {
            if room == 0 {
                current.extend_from_slice(&rar4_end_header(true, numbered.then_some(number)));
                volumes.push(std::mem::take(&mut current));
                number += 1;
                current = RAR4_SIGNATURE.to_vec();
                current.extend_from_slice(&rar4_main_header(main_flags(false)));
                room = data_per_volume;
            }
            let end = (at + room).min(data.len());
            let last = end == data.len();
            let crc = if last {
                crc32(data)
            } else {
                crc32(&data[at..end])
            };
            current.extend_from_slice(&rar4_file_header(
                name,
                data.len() as u32,
                crc,
                (end - at) as u32,
                split_before,
                !last,
                Method::Store,
                29,
                false,
            ));
            current.extend_from_slice(&data[at..end]);
            room -= end - at;
            at = end;
            split_before = true;
            if last {
                break;
            }
        }
    }
    current.extend_from_slice(&rar4_end_header(false, numbered.then_some(number)));
    volumes.push(current);
    volumes
}

/// The file names a set of `count` volumes has under each naming rule, so
/// a test writes the volumes where `unrar` -- and this server's own volume
/// discovery -- will look for them.
pub fn part_names(stem: &str, count: usize) -> Vec<String> {
    (1..=count).map(|n| format!("{stem}.part{n}.rar")).collect()
}

pub fn old_names(stem: &str, count: usize) -> Vec<String> {
    std::iter::once(format!("{stem}.rar"))
        .chain((0..count.saturating_sub(1)).map(|n| format!("{stem}.r{n:02}")))
        .collect()
}
