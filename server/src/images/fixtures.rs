//! Images built byte by byte for the tests.
//!
//! Nothing binary is committed: every fixture here is assembled from the
//! field layouts in the standards, which is also what makes the hostile
//! cases possible at all -- a cycle, a record that points past the end, a
//! descriptor whose CRC no longer matches are all one line each, and no
//! tool will write them for you.
//!
//! The readers here are the other half of the bargain the module makes.
//! [`CountingImage`] records every range that was read so a test can assert
//! that the file's own bytes were never among them, and [`SparseImage`]
//! reports a length far larger than the bytes it holds, which is how a
//! multi-gibibyte file is tested in a few kilobytes.

use super::{ImageReader, SECTOR};
use async_trait::async_trait;
use std::io;
use std::sync::Mutex;

/// An image that records what was read from it.
pub struct CountingImage {
    bytes: Vec<u8>,
    reads: Mutex<Vec<(u64, u64)>>,
}

impl CountingImage {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            reads: Mutex::new(Vec::new()),
        }
    }

    pub fn reads(&self) -> Vec<(u64, u64)> {
        self.reads.lock().expect("not poisoned").clone()
    }

    pub fn total(&self) -> u64 {
        self.reads().iter().map(|(_, l)| l).sum()
    }

    /// Whether any read overlapped `[offset, offset + len)`. The assertion
    /// that matters: an index that touched the film's bytes is not an
    /// index.
    pub fn read_any_of(&self, offset: u64, len: u64) -> bool {
        self.reads()
            .iter()
            .any(|(o, l)| *o < offset + len && offset < o + l)
    }
}

#[async_trait]
impl ImageReader for CountingImage {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let start = offset as usize;
        if start >= self.bytes.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.bytes.len() - start);
        buf[..n].copy_from_slice(&self.bytes[start..start + n]);
        self.reads
            .lock()
            .expect("not poisoned")
            .push((offset, n as u64));
        Ok(n)
    }
}

/// An image whose stated length is `len` and whose first bytes are `head`;
/// everything after that reads as zeros. A 8 GiB image in 64 KiB.
pub struct SparseImage {
    pub head: Vec<u8>,
    pub len: u64,
}

#[async_trait]
impl ImageReader for SparseImage {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.len {
            return Ok(0);
        }
        let n = buf.len().min((self.len - offset) as usize);
        buf[..n].fill(0);
        let start = offset as usize;
        if start < self.head.len() {
            let from_head = n.min(self.head.len() - start);
            buf[..from_head].copy_from_slice(&self.head[start..start + from_head]);
        }
        Ok(n)
    }
}

/// An image whose reads always fail, for the difference between "this is
/// not an image" and "these bytes are not available".
pub struct FailingImage {
    pub len: u64,
}

#[async_trait]
impl ImageReader for FailingImage {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::TimedOut, "no bytes today"))
    }
}

/// ISO 9660 images.
pub mod iso {
    use super::*;

    /// Where the file's data goes in the one-file images, and how long it
    /// is. Not a whole sector, so a parser that rounded up would be caught.
    pub const DATA_SECTOR: u64 = 19;
    pub const DATA_LEN: u64 = 1234;

    fn image(sectors: usize) -> Vec<u8> {
        vec![0u8; sectors * SECTOR as usize]
    }

    fn at(image: &mut [u8], sector: u64) -> &mut [u8] {
        let start = (sector * SECTOR) as usize;
        &mut image[start..start + SECTOR as usize]
    }

    /// A both-endian 32-bit field: little first, then big.
    fn both_u32(b: &mut [u8], at: usize, v: u32) {
        b[at..at + 4].copy_from_slice(&v.to_le_bytes());
        b[at + 4..at + 8].copy_from_slice(&v.to_be_bytes());
    }

    fn both_u16(b: &mut [u8], at: usize, v: u16) {
        b[at..at + 2].copy_from_slice(&v.to_le_bytes());
        b[at + 2..at + 4].copy_from_slice(&v.to_be_bytes());
    }

    /// One directory record. `name` is the raw name bytes (`\0` for `.`,
    /// `\x01` for `..`), `system_use` whatever Rock Ridge data follows.
    pub fn record(name: &[u8], lba: u32, len: u32, flags: u8, system_use: &[u8]) -> Vec<u8> {
        let name_len = name.len();
        let su_start = 33 + name_len + usize::from(name_len.is_multiple_of(2));
        let mut total = su_start + system_use.len();
        if total % 2 == 1 {
            total += 1;
        }
        let mut rec = vec![0u8; total];
        rec[0] = u8::try_from(total).expect("a test record fits in a byte");
        both_u32(&mut rec, 2, lba);
        both_u32(&mut rec, 10, len);
        rec[25] = flags;
        both_u16(&mut rec, 28, 1);
        rec[32] = u8::try_from(name_len).expect("a test name fits in a byte");
        rec[33..33 + name_len].copy_from_slice(name);
        rec[su_start..su_start + system_use.len()].copy_from_slice(system_use);
        rec
    }

    /// A volume descriptor: type, `CD001`, version, the logical block size
    /// and a root directory record.
    fn volume_descriptor(kind: u8, root: &[u8], escape: Option<&[u8]>) -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        d[0] = kind;
        d[1..6].copy_from_slice(b"CD001");
        d[6] = 1;
        if let Some(escape) = escape {
            d[88..88 + escape.len()].copy_from_slice(escape);
        }
        both_u32(&mut d, 80, 64);
        both_u16(&mut d, 120, 1);
        both_u16(&mut d, 124, 1);
        both_u16(&mut d, 128, SECTOR as u16);
        d[156..156 + root.len()].copy_from_slice(root);
        d
    }

    fn terminator() -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        d[0] = 255;
        d[1..6].copy_from_slice(b"CD001");
        d[6] = 1;
        d
    }

    fn directory(records: &[Vec<u8>]) -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        let mut pos = 0;
        for r in records {
            d[pos..pos + r.len()].copy_from_slice(r);
            pos += r.len();
        }
        d
    }

    /// The `.` and `..` records every directory starts with.
    fn dots(lba: u32, parent: u32) -> Vec<Vec<u8>> {
        vec![
            record(&[0x00], lba, SECTOR as u32, 0x02, &[]),
            record(&[0x01], parent, SECTOR as u32, 0x02, &[]),
        ]
    }

    /// Primary volume descriptor, terminator, a root directory holding one
    /// file, and that file's data.
    pub fn minimal_iso() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(
            b"HELLO.TXT;1",
            DATA_SECTOR as u32,
            DATA_LEN as u32,
            0,
            &[],
        ));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        let data = (DATA_SECTOR * SECTOR) as usize;
        image[data..data + DATA_LEN as usize].fill(b'A');
        image
    }

    /// Where [`minimal_iso`]'s file data is.
    pub fn data_range(_image: &[u8]) -> (u64, u64) {
        (DATA_SECTOR * SECTOR, DATA_LEN)
    }

    /// A root with one file and one directory, the directory holding one
    /// file of its own.
    pub fn iso_with_subdirectory() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());

        let mut records = dots(18, 18);
        records.push(record(b"HELLO.TXT;1", 19, DATA_LEN as u32, 0, &[]));
        records.push(record(b"VIDEO_TS", 20, SECTOR as u32, 0x02, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));

        let mut sub = dots(20, 18);
        sub.push(record(b"VTS_01_1.VOB;1", 21, 4096, 0, &[]));
        at(&mut image, 20).copy_from_slice(&directory(&sub));
        image
    }

    /// One name, two records, the first flagged multi-extent: a file over
    /// 4 GiB. The image reports eight gibibytes and holds twenty-four
    /// sectors, which is exactly the point -- nothing reads the data.
    pub fn multi_extent_image() -> SparseImage {
        let mut head = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut head, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut head, 17).copy_from_slice(&terminator());

        // 0xFFFF_F800 is the largest length that is a whole number of
        // sectors, which is how a writer splits a big file.
        let first_len: u32 = 0xffff_f800;
        let mut records = dots(18, 18);
        records.push(record(b"BIG.BIN;1", 32, first_len, 0x80, &[]));
        records.push(record(
            b"BIG.BIN;1",
            32 + first_len / SECTOR as u32,
            1_000_000,
            0,
            &[],
        ));
        at(&mut head, 18).copy_from_slice(&directory(&records));
        SparseImage {
            head,
            len: 8 * 1024 * 1024 * 1024,
        }
    }

    /// A primary tree and a Joliet supplementary tree over the same data.
    pub fn iso_with_joliet() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 21, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        let joliet_root = record(&[0x00], 20, SECTOR as u32, 0x02, &[]);
        at(&mut image, 17).copy_from_slice(&volume_descriptor(
            2,
            &joliet_root,
            Some(&[0x25, 0x2f, 0x45]),
        ));
        at(&mut image, 18).copy_from_slice(&terminator());

        // The same file's extent, under two names, in two trees.
        let mut primary = dots(21, 21);
        primary.push(record(b"FALLT_SC.MKV;1", 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 21).copy_from_slice(&directory(&primary));

        let ucs2: Vec<u8> = "Fällt schwer.mkv;1"
            .encode_utf16()
            .flat_map(|u| u.to_be_bytes())
            .collect();
        let mut joliet = dots(20, 20);
        joliet.push(record(&ucs2, 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 20).copy_from_slice(&directory(&joliet));
        image
    }

    /// A primary tree whose records carry Rock Ridge `NM` names, and whose
    /// `.` record carries the `SP` entry.
    pub fn iso_with_rock_ridge() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());

        let sp = [b'S', b'P', 7, 1, 0xbe, 0xef, 0];
        let mut records = vec![
            record(&[0x00], 18, SECTOR as u32, 0x02, &sp),
            record(&[0x01], 18, SECTOR as u32, 0x02, &[]),
        ];
        let name = b"a long unix name.mkv";
        let mut nm = vec![b'N', b'M', (5 + name.len()) as u8, 1, 0];
        nm.extend_from_slice(name);
        records.push(record(b"A_LONG_U.MKV;1", 19, DATA_LEN as u32, 0, &nm));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A file record whose extent is past the end of the image.
    pub fn iso_with_file_past_the_end() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"GONE.BIN;1", 10_000, 4096, 0, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A subdirectory whose extent is the directory that holds it.
    pub fn iso_with_directory_cycle() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"LOOP", 18, SECTOR as u32, 0x02, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A record whose name holds a path separator: a name that would
    /// build a path the image does not describe.
    pub fn iso_with_a_separator_in_a_name() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"../../ETC/PASSWD;1", 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A chain of directories deeper than the walk's limit, each one a
    /// sector of its own so that nothing is a cycle.
    pub fn iso_with_a_depth_bomb() -> Vec<u8> {
        let levels = 100u32;
        let mut image = image(18 + levels as usize + 2);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        for level in 0..levels {
            let here = 18 + level;
            let mut records = dots(here, here.saturating_sub(1).max(18));
            records.push(record(b"D", here + 1, SECTOR as u32, 0x02, &[]));
            at(&mut image, here as u64).copy_from_slice(&directory(&records));
        }
        image
    }

    /// A record claiming more bytes than are left in its directory.
    pub fn iso_with_overlong_record() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, 100, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut rec = record(b"BIG.BIN;1", 19, 4096, 0, &[]);
        rec[0] = 200;
        let dir = at(&mut image, 18);
        dir[..rec.len()].copy_from_slice(&rec);
        image
    }
}
