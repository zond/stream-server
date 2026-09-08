//! The offset arithmetic, with no filesystem and no librqbit types in it.
//!
//! librqbit addresses storage as `(file_id, offset within that file)`. One
//! file per piece addresses it as `(piece index, offset within that piece)`.
//! Everything hard about the backend is the translation between the two, so
//! it lives here on its own where it can be tested exhaustively against
//! hand-built torrents -- including the shapes a real swarm would take an
//! evening to reproduce: a piece spanning three files, a padding file in the
//! middle of one, a zero-length file on a piece boundary, the short last
//! piece.
//!
//! The torrent's global byte space is the concatenation of every file in
//! metadata order, padding files included -- that is how the piece hashes
//! were computed and it is how librqbit's own `offset_in_torrent` is built
//! (a running sum over `iter_file_details_ext`). Pieces tile that space from
//! zero at a fixed length, the last one short whenever the total does not
//! divide evenly.

use std::ops::Range;

/// One file of the torrent, as this module needs to see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSpec {
    pub len: u64,
    /// A BEP-47 padding file: it occupies global offset space and its bytes
    /// are hashed as zeroes, but librqbit never reads or writes one through
    /// storage.
    pub padding: bool,
}

impl FileSpec {
    pub fn payload(len: u64) -> Self {
        Self {
            len,
            padding: false,
        }
    }

    pub fn padding(len: u64) -> Self {
        Self { len, padding: true }
    }
}

/// A file's extent in the torrent's global byte space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileExtent {
    offset: u64,
    len: u64,
    /// Whether any of this file's bytes are payload somebody can want.
    ///
    /// A padding file's are not -- they are zeroes nobody transfers, and
    /// librqbit skips them on every storage path -- and a zero-length file
    /// has no bytes at all. Neither can keep a piece alive once the real
    /// files sharing it are gone, which is what [`PieceLayout::files_overlapping_piece`]
    /// is asked about when a piece is a candidate for deletion.
    owns_bytes: bool,
}

/// Where one contiguous run of a request lands: a byte range inside a single
/// piece file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    pub piece: u32,
    pub offset_in_piece: u64,
    pub len: u64,
}

/// The map between `(file_id, offset)` and `(piece, offset_in_piece)` for one
/// torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceLayout {
    piece_length: u64,
    total_length: u64,
    last_piece: u32,
    files: Vec<FileExtent>,
}

impl PieceLayout {
    /// Build the map from the torrent's piece length, its total length and
    /// its files in metadata order.
    ///
    /// `total_length` must be the sum of the file lengths: that identity is
    /// what makes a file offset resolvable to a piece at all, and both
    /// numbers come from the same metadata, so a disagreement means we are
    /// about to write bytes at addresses the swarm does not share. Refusing
    /// the torrent is the only safe answer -- a mapping built on a wrong
    /// total silently shifts every piece after the discrepancy.
    pub fn new(
        piece_length: u64,
        total_length: u64,
        files: impl IntoIterator<Item = FileSpec>,
    ) -> anyhow::Result<Self> {
        if piece_length == 0 {
            anyhow::bail!("torrent has a zero piece length");
        }
        if total_length == 0 {
            anyhow::bail!("torrent has zero total length");
        }
        let mut offset = 0u64;
        let mut extents = Vec::new();
        for file in files {
            extents.push(FileExtent {
                offset,
                len: file.len,
                owns_bytes: !file.padding && file.len > 0,
            });
            offset = offset
                .checked_add(file.len)
                .ok_or_else(|| anyhow::anyhow!("torrent file lengths overflow a u64"))?;
        }
        if offset != total_length {
            anyhow::bail!(
                "torrent file lengths sum to {offset} but its total length is {total_length}"
            );
        }
        // `Lengths` counts pieces the same way and stores the count in a u32,
        // so the last index fits one by construction.
        let piece_count = total_length.div_ceil(piece_length);
        let last_piece = u32::try_from(piece_count - 1)
            .map_err(|_| anyhow::anyhow!("torrent has more than u32::MAX pieces"))?;
        Ok(Self {
            piece_length,
            total_length,
            last_piece,
            files: extents,
        })
    }

    pub fn piece_count(&self) -> u32 {
        self.last_piece + 1
    }

    pub fn default_piece_length(&self) -> u64 {
        self.piece_length
    }

    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// The length of one piece. Every piece is [`Self::default_piece_length`]
    /// except the last, which is short exactly when the total length does not
    /// divide evenly -- when it does, the last piece is full size, which is
    /// why this is a remainder test and not `total % piece_length`.
    pub fn piece_length_of(&self, piece: u32) -> u64 {
        if piece == self.last_piece {
            let rem = self.total_length % self.piece_length;
            if rem == 0 { self.piece_length } else { rem }
        } else {
            self.piece_length
        }
    }

    /// The byte offset of a piece in the torrent's global space.
    pub fn piece_offset(&self, piece: u32) -> u64 {
        piece as u64 * self.piece_length
    }

    /// Whether this file has payload bytes of its own -- see
    /// `FileExtent::owns_bytes`. `false` for an out-of-range id.
    pub fn owns_bytes(&self, file_id: usize) -> bool {
        self.files.get(file_id).is_some_and(|f| f.owns_bytes)
    }

    pub fn file_len(&self, file_id: usize) -> anyhow::Result<u64> {
        Ok(self.file(file_id)?.len)
    }

    fn file(&self, file_id: usize) -> anyhow::Result<&FileExtent> {
        self.files
            .get(file_id)
            .ok_or_else(|| anyhow::anyhow!("no file with id {file_id} in this torrent"))
    }

    /// Resolve a request against one file into the piece-file runs that
    /// satisfy it, in order.
    ///
    /// A request that starts mid-piece and ends mid-another is the normal
    /// case, not the edge case: librqbit asks in 16 KiB chunks and 64 KiB
    /// hash slices whose alignment to *pieces* is whatever the file's own
    /// offset in the torrent happens to be.
    ///
    /// A zero-length request is legal at any offset up to and including the
    /// file's length, and yields nothing. librqbit really does issue those:
    /// its per-file walk in `file_ops` tests `absolute_offset > file_len`,
    /// not `>=`, so a chunk that ends exactly on a file boundary reaches the
    /// next file with nothing left to do.
    pub fn segments(&self, file_id: usize, offset: u64, len: u64) -> anyhow::Result<Segments<'_>> {
        let file = self.file(file_id)?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| anyhow::anyhow!("request offset {offset} + length {len} overflows"))?;
        if end > file.len {
            anyhow::bail!(
                "request for {len} bytes at offset {offset} runs past the end of file {file_id} ({} bytes)",
                file.len
            );
        }
        Ok(Segments {
            layout: self,
            global: file.offset + offset,
            remaining: len,
        })
    }

    /// Every piece any of this file's bytes land in. Empty for a zero-length
    /// file, which has no bytes and therefore no pieces -- librqbit's own
    /// `piece_range` says the same for one.
    pub fn pieces_overlapping_file(&self, file_id: usize) -> anyhow::Result<Range<u32>> {
        let file = self.file(file_id)?;
        if file.len == 0 {
            return Ok(0..0);
        }
        let first = (file.offset / self.piece_length) as u32;
        let last = ((file.offset + file.len - 1) / self.piece_length) as u32;
        Ok(first..last + 1)
    }

    /// Every file with a byte in this piece, as a range of file ids.
    ///
    /// This is what makes a piece deletable or not: a piece is shared by as
    /// many files as it straddles, and dropping it because one of them was
    /// removed would take the others' data with it.
    pub fn files_overlapping_piece(&self, piece: u32) -> Range<usize> {
        let start = self.piece_offset(piece);
        let end = start + self.piece_length_of(piece);
        // The extents are sorted by offset and contiguous, so both
        // predicates are monotone over the vector.
        let first = self.files.partition_point(|f| f.offset + f.len <= start);
        let past = self.files.partition_point(|f| f.offset < end);
        first..past
    }
}

/// The iterator [`PieceLayout::segments`] returns.
pub struct Segments<'a> {
    layout: &'a PieceLayout,
    global: u64,
    remaining: u64,
}

impl Iterator for Segments<'_> {
    type Item = Segment;

    fn next(&mut self) -> Option<Segment> {
        if self.remaining == 0 {
            return None;
        }
        let piece = (self.global / self.layout.piece_length) as u32;
        let offset_in_piece = self.global % self.layout.piece_length;
        // `segments` bounded the request by the file's length and the file
        // extents sum to the total length, so the whole request lies inside
        // the torrent and this subtraction cannot underflow.
        let to_piece_end = self.layout.piece_length_of(piece) - offset_in_piece;
        let len = self.remaining.min(to_piece_end);
        self.global += len;
        self.remaining -= len;
        Some(Segment {
            piece,
            offset_in_piece,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(piece_length: u64, files: &[FileSpec]) -> PieceLayout {
        let total = files.iter().map(|f| f.len).sum();
        PieceLayout::new(piece_length, total, files.iter().copied()).expect("valid layout")
    }

    fn segments(layout: &PieceLayout, file_id: usize, offset: u64, len: u64) -> Vec<Segment> {
        layout
            .segments(file_id, offset, len)
            .expect("in-range request")
            .collect()
    }

    fn seg(piece: u32, offset_in_piece: u64, len: u64) -> Segment {
        Segment {
            piece,
            offset_in_piece,
            len,
        }
    }

    #[test]
    fn a_read_that_starts_and_ends_mid_piece_splits_at_the_boundary() {
        // One 40-byte file over 16-byte pieces: 0..16, 16..32, 32..40.
        let l = layout(16, &[FileSpec::payload(40)]);
        assert_eq!(l.piece_count(), 3);
        assert_eq!(
            segments(&l, 0, 10, 15),
            vec![seg(0, 10, 6), seg(1, 0, 9)],
            "a read from mid-piece-0 into mid-piece-1"
        );
        assert_eq!(
            segments(&l, 0, 4, 34),
            vec![seg(0, 4, 12), seg(1, 0, 16), seg(2, 0, 6)],
            "and one that covers a whole piece on the way"
        );
        assert_eq!(
            segments(&l, 0, 16, 16),
            vec![seg(1, 0, 16)],
            "a piece-aligned read is one segment"
        );
    }

    #[test]
    fn a_piece_spanning_two_files_is_addressed_from_both() {
        // 10 + 10 bytes over 16-byte pieces: piece 0 holds all of file 0 and
        // the first 6 bytes of file 1.
        let l = layout(16, &[FileSpec::payload(10), FileSpec::payload(10)]);
        assert_eq!(segments(&l, 0, 0, 10), vec![seg(0, 0, 10)]);
        assert_eq!(
            segments(&l, 1, 0, 10),
            vec![seg(0, 10, 6), seg(1, 0, 4)],
            "file 1 begins in the middle of the piece file 0 ends in"
        );
        assert_eq!(l.files_overlapping_piece(0), 0..2, "piece 0 is shared");
        assert_eq!(l.files_overlapping_piece(1), 1..2, "piece 1 is file 1's");
        assert_eq!(l.pieces_overlapping_file(0).unwrap(), 0..1);
        assert_eq!(l.pieces_overlapping_file(1).unwrap(), 0..2);
    }

    #[test]
    fn a_padding_file_takes_offset_space_but_owns_nothing() {
        // BEP-47 padding between two payload files: it shifts everything
        // after it, and it is on piece 0 without being able to keep it.
        let l = layout(
            16,
            &[
                FileSpec::payload(10),
                FileSpec::padding(6),
                FileSpec::payload(10),
            ],
        );
        assert_eq!(l.total_length(), 26);
        assert_eq!(l.piece_count(), 2);
        assert!(l.owns_bytes(0));
        assert!(!l.owns_bytes(1), "padding owns no bytes");
        assert!(l.owns_bytes(2));
        assert_eq!(
            segments(&l, 2, 0, 10),
            vec![seg(1, 0, 10)],
            "the padding pushed file 2 onto a piece boundary"
        );
        assert_eq!(l.files_overlapping_piece(0), 0..2);
        assert_eq!(l.files_overlapping_piece(1), 2..3);
    }

    #[test]
    fn the_last_piece_is_short_only_when_the_total_does_not_divide_evenly() {
        let short = layout(16, &[FileSpec::payload(40)]);
        assert_eq!(short.piece_length_of(0), 16);
        assert_eq!(short.piece_length_of(2), 8, "40 = 2 pieces and a half");
        assert_eq!(
            segments(&short, 0, 32, 8),
            vec![seg(2, 0, 8)],
            "and it is addressable to its real end"
        );

        let exact = layout(16, &[FileSpec::payload(32)]);
        assert_eq!(exact.piece_count(), 2);
        assert_eq!(
            exact.piece_length_of(1),
            16,
            "an even division leaves a full-length last piece, not a zero-length one"
        );
    }

    #[test]
    fn a_zero_length_request_is_legal_at_the_end_of_a_file() {
        // librqbit's per-file walk tests `absolute_offset > file_len`, so a
        // chunk ending exactly on a file boundary arrives here with nothing
        // left to read. Erroring on it would break every such chunk.
        let l = layout(16, &[FileSpec::payload(10), FileSpec::payload(10)]);
        assert!(segments(&l, 0, 10, 0).is_empty());
        assert!(segments(&l, 0, 0, 0).is_empty());
        assert!(
            l.segments(0, 11, 0).is_err(),
            "but past the end is still past the end"
        );
        assert!(l.segments(0, 5, 6).is_err());
        assert!(l.segments(9, 0, 0).is_err(), "and there is no file 9");
    }

    #[test]
    fn a_zero_length_file_has_no_pieces_and_cannot_keep_one() {
        let l = layout(
            16,
            &[
                FileSpec::payload(10),
                FileSpec::payload(0),
                FileSpec::payload(10),
            ],
        );
        assert_eq!(l.pieces_overlapping_file(1).unwrap(), 0..0);
        assert!(segments(&l, 1, 0, 0).is_empty());
        assert!(!l.owns_bytes(1));
        assert_eq!(
            l.files_overlapping_piece(0),
            0..3,
            "it sits on the boundary between two files that do share the piece; \
             owning no bytes itself, it can never be what keeps the piece alive"
        );
    }

    #[test]
    fn a_file_table_that_does_not_sum_to_the_total_is_refused() {
        let files = [FileSpec::payload(10), FileSpec::payload(10)];
        assert!(PieceLayout::new(16, 20, files).is_ok());
        let err = PieceLayout::new(16, 21, files).unwrap_err().to_string();
        assert!(err.contains("sum to 20"), "{err}");
        assert!(PieceLayout::new(0, 20, files).is_err(), "zero piece length");
        assert!(PieceLayout::new(16, 0, []).is_err(), "zero total length");
    }

    /// Every request that resolves at all must resolve to a contiguous run of
    /// the torrent's global bytes, each segment inside one real piece. Checked
    /// over every offset and length of every file in a torrent shaped to hit
    /// all of it at once: files shorter and longer than a piece, a padding
    /// file, a zero-length file, boundaries hit exactly, and a short last
    /// piece.
    #[test]
    fn every_request_maps_to_the_bytes_it_asked_for() {
        let specs = [
            FileSpec::payload(3),
            FileSpec::payload(5),
            FileSpec::padding(2),
            FileSpec::payload(0),
            FileSpec::payload(13),
            FileSpec::payload(1),
        ];
        let l = layout(5, &specs);
        assert_eq!(l.total_length(), 24);
        assert_eq!(l.piece_count(), 5, "24 bytes over 5-byte pieces");
        assert_eq!(l.piece_length_of(4), 4);

        let mut file_offset = 0u64;
        for (file_id, spec) in specs.iter().enumerate() {
            for offset in 0..=spec.len {
                for len in 0..=(spec.len - offset) {
                    let segs = segments(&l, file_id, offset, len);
                    assert_eq!(
                        segs.iter().map(|s| s.len).sum::<u64>(),
                        len,
                        "file {file_id} offset {offset} len {len}"
                    );
                    let mut want = file_offset + offset;
                    for s in &segs {
                        assert!(s.len > 0, "no empty segments");
                        assert_eq!(
                            l.piece_offset(s.piece) + s.offset_in_piece,
                            want,
                            "file {file_id} offset {offset} len {len}: segment {s:?} is at the wrong global offset"
                        );
                        assert!(
                            s.offset_in_piece + s.len <= l.piece_length_of(s.piece),
                            "file {file_id} offset {offset} len {len}: segment {s:?} runs past piece {}",
                            s.piece
                        );
                        want += s.len;
                    }
                }
            }
            file_offset += spec.len;
        }
    }

    /// The two directions have to agree: if a piece overlaps a file then that
    /// file overlaps the piece. A disagreement is how a shared piece gets
    /// deleted out from under its other owner.
    #[test]
    fn the_file_and_piece_overlap_views_agree() {
        let specs = [
            FileSpec::payload(3),
            FileSpec::payload(5),
            FileSpec::padding(2),
            FileSpec::payload(0),
            FileSpec::payload(13),
            FileSpec::payload(1),
        ];
        let l = layout(5, &specs);
        for (file_id, spec) in specs.iter().enumerate() {
            let pieces = l.pieces_overlapping_file(file_id).unwrap();
            for piece in 0..l.piece_count() {
                let files = l.files_overlapping_piece(piece);
                assert_eq!(
                    pieces.contains(&piece),
                    files.contains(&file_id) && spec.len > 0,
                    "file {file_id} vs piece {piece}"
                );
            }
        }
    }
}
