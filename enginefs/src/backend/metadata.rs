use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tracing::{debug, info};

/// EBML Element IDs for Matroska (defined for reference, parsing uses byte patterns)
#[allow(dead_code)]
mod ebml_ids {
    pub const SEGMENT: u32 = 0x18538067;
    pub const SEEK_HEAD: u32 = 0x114D9B74;
    pub const SEEK: u32 = 0x4DBB;
    pub const SEEK_ID: u32 = 0x53AB;
    pub const SEEK_POSITION: u32 = 0x53AC;
    pub const CUES: u32 = 0x1C53BB6B;
    pub const CUE_POINT: u32 = 0xBB;
    pub const CUE_TIME: u32 = 0xB3;
    pub const CUE_TRACK_POSITIONS: u32 = 0xB7;
    pub const CUE_CLUSTER_POSITION: u32 = 0xF1;
    pub const CLUSTER: u32 = 0x1F43B675;
}

pub enum ContainerType {
    Mp4,
    Matroska,
    Unknown,
}

/// Represents a keyframe/cue point with its byte offset
#[derive(Debug, Clone)]
pub struct KeyframeInfo {
    pub timestamp_ms: u64,
    pub byte_offset: u64,
}

pub struct MetadataInspector;

impl MetadataInspector {
    /// Find critical ranges AND keyframe positions for optimal piece prioritization
    pub async fn find_critical_ranges<R>(
        reader: &mut R,
        total_size: u64,
        filename: &str,
    ) -> Vec<(u64, u64)>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let container = Self::guess_container(filename);
        let mut ranges = Vec::new();

        // Always prioritize the first 64KB for initial headers
        ranges.push((0, 65536.min(total_size)));

        match container {
            ContainerType::Mp4 => {
                if let Ok(mp4_ranges) = Self::inspect_mp4_full(reader, total_size).await {
                    ranges.extend(mp4_ranges);
                }
            }
            ContainerType::Matroska => {
                if let Ok(mkv_ranges) = Self::inspect_mkv_full(reader, total_size).await {
                    ranges.extend(mkv_ranges);
                }
            }
            ContainerType::Unknown => {
                if total_size > 128_000 {
                    ranges.push((total_size - 128_000, 128_000));
                }
            }
        }

        ranges
    }

    /// Find keyframe byte offsets for prioritization
    pub async fn find_keyframe_offsets<R>(
        reader: &mut R,
        total_size: u64,
        filename: &str,
    ) -> Vec<u64>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let container = Self::guess_container(filename);

        match container {
            ContainerType::Mp4 => Self::find_mp4_keyframes(reader, total_size)
                .await
                .unwrap_or_default(),
            ContainerType::Matroska => Self::find_mkv_keyframes(reader, total_size)
                .await
                .unwrap_or_default(),
            ContainerType::Unknown => Vec::new(),
        }
    }

    fn guess_container(filename: &str) -> ContainerType {
        let name = filename.to_lowercase();
        if name.ends_with(".mp4") || name.ends_with(".m4v") || name.ends_with(".mov") {
            ContainerType::Mp4
        } else if name.ends_with(".mkv") || name.ends_with(".webm") {
            ContainerType::Matroska
        } else {
            ContainerType::Unknown
        }
    }

    // =========================================================================
    // MKV/Matroska Parsing
    // =========================================================================

    async fn inspect_mkv_full<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Vec<(u64, u64)>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let mut ranges = Vec::new();

        // First 1MB for EBML header, SeekHead, Segment info
        ranges.push((0, 1_048_576.min(total_size)));

        // Try to find Cues location from SeekHead. A crafted SeekHead can point
        // past EOF, so only trust offsets inside the file.
        if let Ok(Some(cues_offset)) = Self::find_mkv_cues_offset(reader, total_size).await
            && cues_offset < total_size
        {
            info!("MetadataInspector: Found Cues at offset {}", cues_offset);
            // Prioritize Cues area (usually 100KB-500KB)
            let cues_size = 500_000.min(total_size - cues_offset);
            ranges.push((cues_offset, cues_size));
        } else {
            // Fallback: Cues are often at the end
            if total_size > 1_000_000 {
                ranges.push((total_size - 1_000_000, 1_000_000));
            }
        }

        Ok(ranges)
    }

    async fn find_mkv_cues_offset<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Option<u64>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        reader.seek(SeekFrom::Start(0)).await?;

        // Read first 100KB to find SeekHead
        let read_size = 102_400.min(total_size) as usize;
        let mut buffer = vec![0u8; read_size];
        reader.read_exact(&mut buffer).await?;

        // Find EBML header and Segment
        let mut pos = 0;

        // Skip EBML header (starts with 0x1A45DFA3)
        while pos + 4 < buffer.len() {
            if buffer[pos] == 0x1A
                && buffer[pos + 1] == 0x45
                && buffer[pos + 2] == 0xDF
                && buffer[pos + 3] == 0xA3
            {
                // Found EBML header, skip it
                if let Some((_, size)) = Self::parse_ebml_element_header(&buffer[pos..]) {
                    pos += size;
                    break;
                }
            }
            pos += 1;
        }

        // Now find Segment (0x18538067)
        let mut segment_data_start = 0u64;
        while pos + 4 < buffer.len() {
            if buffer[pos] == 0x18
                && buffer[pos + 1] == 0x53
                && buffer[pos + 2] == 0x80
                && buffer[pos + 3] == 0x67
            {
                // Found Segment
                if let Some((_, header_size)) = Self::parse_ebml_element_header(&buffer[pos..]) {
                    segment_data_start = (pos + header_size) as u64;
                    pos += header_size;
                    break;
                }
            }
            pos += 1;
        }

        // Now look for SeekHead within Segment (0x114D9B74)
        while pos + 4 < buffer.len() {
            // Check for SeekHead
            if buffer[pos] == 0x11
                && buffer[pos + 1] == 0x4D
                && buffer[pos + 2] == 0x9B
                && buffer[pos + 3] == 0x74
            {
                debug!("MetadataInspector: Found SeekHead at {}", pos);

                if let Some((size, header_size)) = Self::parse_ebml_element_header(&buffer[pos..]) {
                    let seek_head_data = &buffer[pos + header_size..];
                    let seek_head_size = (size as usize).min(seek_head_data.len());

                    // Parse SeekHead to find Cues offset
                    if let Some(cues_pos) = Self::parse_seek_head(&seek_head_data[..seek_head_size])
                    {
                        // Cues position is relative to Segment data start.
                        // Saturate: a crafted SeekPosition near u64::MAX must not
                        // overflow; the callers' `< total_size` guards reject it.
                        return Ok(Some(segment_data_start.saturating_add(cues_pos)));
                    }
                }
            }

            // Check for Cluster (0x1F43B675) - stop if we hit clusters
            if buffer[pos] == 0x1F
                && buffer[pos + 1] == 0x43
                && buffer[pos + 2] == 0xB6
                && buffer[pos + 3] == 0x75
            {
                break;
            }

            pos += 1;
        }

        Ok(None)
    }

    fn parse_ebml_element_header(data: &[u8]) -> Option<(u64, usize)> {
        if data.is_empty() {
            return None;
        }

        // Parse VINT for element ID
        let (_, id_len) = Self::parse_vint(data)?;
        if data.len() < id_len {
            return None;
        }

        // Parse VINT for size
        let (size, size_len) = Self::parse_vint(&data[id_len..])?;

        Some((size, id_len + size_len))
    }

    fn parse_vint(data: &[u8]) -> Option<(u64, usize)> {
        if data.is_empty() {
            return None;
        }

        let first = data[0];
        let len = first.leading_zeros() as usize + 1;

        if len > 8 || data.len() < len {
            return None;
        }

        let mut value = (first as u64) & ((1 << (8 - len)) - 1);
        for &byte in data.iter().take(len).skip(1) {
            value = (value << 8) | u64::from(byte);
        }

        Some((value, len))
    }

    fn parse_seek_head(data: &[u8]) -> Option<u64> {
        let mut pos = 0;

        while pos + 2 < data.len() {
            // Look for Seek element (0x4DBB)
            if data[pos] == 0x4D
                && data[pos + 1] == 0xBB
                && let Some((size, header_size)) = Self::parse_ebml_element_header(&data[pos..])
            {
                let seek_data = &data[pos + header_size..];
                let seek_size = (size as usize).min(seek_data.len());

                // Parse Seek element to find Cues reference
                if let Some(cues_pos) = Self::parse_seek_element(&seek_data[..seek_size]) {
                    return Some(cues_pos);
                }

                // Crafted sizes can overflow the advance; saturate and let the
                // loop condition terminate the scan.
                pos = pos
                    .saturating_add(header_size)
                    .saturating_add(size as usize);
                continue;
            }
            pos += 1;
        }

        None
    }

    fn parse_seek_element(data: &[u8]) -> Option<u64> {
        let mut pos = 0;
        let mut seek_id: Option<u32> = None;
        let mut seek_position: Option<u64> = None;

        while pos + 2 < data.len() {
            // SeekID (0x53AB)
            if data[pos] == 0x53
                && data[pos + 1] == 0xAB
                && let Some((size, header_size)) = Self::parse_ebml_element_header(&data[pos..])
            {
                let id_data = &data[pos + header_size..];
                if size as usize <= id_data.len() {
                    // Parse the ID bytes
                    let mut id = 0u32;
                    for &byte in id_data.iter().take(size as usize) {
                        id = (id << 8) | u32::from(byte);
                    }
                    seek_id = Some(id);
                }
                pos = pos
                    .saturating_add(header_size)
                    .saturating_add(size as usize);
                continue;
            }

            // SeekPosition (0x53AC)
            if data[pos] == 0x53
                && data[pos + 1] == 0xAC
                && let Some((size, header_size)) = Self::parse_ebml_element_header(&data[pos..])
            {
                let pos_data = &data[pos + header_size..];
                if size as usize <= pos_data.len() {
                    let mut position = 0u64;
                    for &byte in pos_data.iter().take(size as usize) {
                        position = (position << 8) | u64::from(byte);
                    }
                    seek_position = Some(position);
                }
                pos = pos
                    .saturating_add(header_size)
                    .saturating_add(size as usize);
                continue;
            }

            pos += 1;
        }

        // Check if this Seek element points to Cues (0x1C53BB6B)
        if let (Some(id), Some(position)) = (seek_id, seek_position)
            && id == ebml_ids::CUES
        {
            return Some(position);
        }

        None
    }

    async fn find_mkv_keyframes<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Vec<u64>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let mut keyframes = Vec::new();

        // Try to find and parse Cues (ignore crafted offsets past EOF)
        if let Ok(Some(cues_offset)) = Self::find_mkv_cues_offset(reader, total_size).await
            && cues_offset < total_size
        {
            reader.seek(SeekFrom::Start(cues_offset)).await?;

            // Read Cues data (up to 1MB)
            let read_size = 1_048_576.min(total_size - cues_offset) as usize;
            let mut buffer = vec![0u8; read_size];
            let bytes_read = reader.read(&mut buffer).await?;
            buffer.truncate(bytes_read);

            // Parse CuePoints to extract cluster positions
            keyframes = Self::parse_cue_points(&buffer);

            if !keyframes.is_empty() {
                info!(
                    "MetadataInspector: Found {} keyframe positions from Cues",
                    keyframes.len()
                );
            }
        }

        Ok(keyframes)
    }

    fn parse_cue_points(data: &[u8]) -> Vec<u64> {
        let mut positions = Vec::new();
        let mut pos = 0;

        // Skip Cues element header if present
        if pos + 4 < data.len()
            && data[pos] == 0x1C
            && data[pos + 1] == 0x53
            && data[pos + 2] == 0xBB
            && data[pos + 3] == 0x6B
            && let Some((_, header_size)) = Self::parse_ebml_element_header(&data[pos..])
        {
            pos += header_size;
        }

        // Look for CuePoint elements (0xBB)
        while pos + 1 < data.len() && positions.len() < 50 {
            if data[pos] == 0xBB
                && let Some((size, header_size)) = Self::parse_ebml_element_header(&data[pos..])
            {
                let cue_data = &data[pos + header_size..];
                let cue_size = (size as usize).min(cue_data.len());

                // Parse CuePoint to find CueClusterPosition
                if let Some(cluster_pos) = Self::parse_cue_point(&cue_data[..cue_size]) {
                    positions.push(cluster_pos);
                }

                pos = pos
                    .saturating_add(header_size)
                    .saturating_add(size as usize);
                continue;
            }
            pos += 1;
        }

        positions
    }

    fn parse_cue_point(data: &[u8]) -> Option<u64> {
        let mut pos = 0;

        while pos + 1 < data.len() {
            // CueTrackPositions (0xB7)
            if data[pos] == 0xB7
                && let Some((size, header_size)) = Self::parse_ebml_element_header(&data[pos..])
            {
                let track_data = &data[pos + header_size..];
                let track_size = (size as usize).min(track_data.len());

                // Look for CueClusterPosition (0xF1)
                let mut tpos = 0;
                while tpos + 1 < track_size {
                    if track_data[tpos] == 0xF1
                        && let Some((psize, pheader)) =
                            Self::parse_ebml_element_header(&track_data[tpos..])
                    {
                        let pdata = &track_data[tpos + pheader..];
                        if psize as usize <= pdata.len() {
                            let mut cluster_pos = 0u64;
                            for &byte in pdata.iter().take(psize as usize) {
                                cluster_pos = (cluster_pos << 8) | u64::from(byte);
                            }
                            return Some(cluster_pos);
                        }
                    }
                    tpos += 1;
                }

                pos = pos
                    .saturating_add(header_size)
                    .saturating_add(size as usize);
                continue;
            }
            pos += 1;
        }

        None
    }

    // =========================================================================
    // MP4 Parsing
    // =========================================================================

    async fn inspect_mp4_full<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Vec<(u64, u64)>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let mut ranges = Vec::new();
        let mut moov_found = false;
        let mut pos = 0u64;
        let mut buffer = [0u8; 16];

        // Scan for atoms
        let mut iterations = 0;
        while pos + 8 <= total_size && iterations < 100 {
            iterations += 1;
            reader.seek(SeekFrom::Start(pos)).await?;

            if reader.read(&mut buffer[..8]).await? < 8 {
                break;
            }

            let box_size = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as u64;
            // Copy box_type to avoid borrow conflict with later mutable borrow
            let box_type: [u8; 4] = [buffer[4], buffer[5], buffer[6], buffer[7]];

            // Handle extended size (box_size == 1)
            let actual_size = if box_size == 1 {
                if reader.read(&mut buffer[8..16]).await? < 8 {
                    break;
                }
                u64::from_be_bytes([
                    buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13],
                    buffer[14], buffer[15],
                ])
            } else if box_size == 0 {
                total_size - pos // Box extends to end of file
            } else {
                box_size
            };

            if box_type == *b"moov" {
                info!(
                    "MetadataInspector: Found 'moov' at {} size {}",
                    pos, actual_size
                );
                ranges.push((pos, actual_size.min(50_000_000))); // Cap at 50MB
                moov_found = true;
                break;
            }

            if box_type == *b"mdat" && actual_size > 10_000_000 {
                // Large mdat before moov - check end of file
                debug!("MetadataInspector: Large 'mdat', checking end for 'moov'");
                if let Ok(end_ranges) = Self::check_mp4_end(reader, total_size).await
                    && !end_ranges.is_empty()
                {
                    ranges.extend(end_ranges);
                    moov_found = true;
                    break;
                }
            }

            if actual_size < 8 || actual_size > total_size - pos {
                break;
            }
            pos += actual_size;
        }

        if !moov_found {
            // Fallback: prioritize end of file
            if total_size > 2_000_000 {
                ranges.push((total_size - 2_000_000, 2_000_000));
            }
        }

        Ok(ranges)
    }

    async fn check_mp4_end<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Vec<(u64, u64)>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let check_size = 5_000_000.min(total_size);
        let start_pos = total_size - check_size;
        reader.seek(SeekFrom::Start(start_pos)).await?;

        let mut buffer = vec![0u8; check_size as usize];
        let bytes_read = reader.read(&mut buffer).await?;
        buffer.truncate(bytes_read);

        for i in 0..(buffer.len().saturating_sub(8)) {
            if &buffer[i + 4..i + 8] == b"moov" {
                let box_size =
                    u32::from_be_bytes([buffer[i], buffer[i + 1], buffer[i + 2], buffer[i + 3]])
                        as u64;
                let global_pos = start_pos + i as u64;
                info!(
                    "MetadataInspector: Found 'moov' at end: {} size {}",
                    global_pos, box_size
                );
                return Ok(vec![(global_pos, box_size.min(50_000_000))]);
            }
        }

        Ok(vec![])
    }

    async fn find_mp4_keyframes<R>(reader: &mut R, total_size: u64) -> anyhow::Result<Vec<u64>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        // Find moov first
        let moov_ranges = Self::inspect_mp4_full(reader, total_size).await?;

        if moov_ranges.is_empty() {
            return Ok(Vec::new());
        }

        let (moov_offset, moov_size) = moov_ranges[0];

        // Read moov atom
        reader.seek(SeekFrom::Start(moov_offset)).await?;
        let read_size = moov_size.min(20_000_000) as usize; // Cap at 20MB
        let mut moov_data = vec![0u8; read_size];
        let bytes_read = reader.read(&mut moov_data).await?;
        moov_data.truncate(bytes_read);

        // Find stss (sync sample) atom for keyframe indices
        // And stco/co64 for chunk offsets
        let keyframes = Self::parse_moov_for_keyframes(&moov_data);

        if !keyframes.is_empty() {
            info!(
                "MetadataInspector: Found {} keyframe byte offsets from MP4",
                keyframes.len()
            );
        }

        Ok(keyframes)
    }

    fn parse_moov_for_keyframes(moov_data: &[u8]) -> Vec<u64> {
        let mut keyframe_indices = Vec::new();
        let mut chunk_offsets = Vec::new();
        let mut samples_per_chunk = Vec::new();

        // Find stss (sync sample table) - contains keyframe sample numbers
        if let Some(stss_pos) = Self::find_atom(moov_data, b"stss") {
            keyframe_indices = Self::parse_stss(&moov_data[stss_pos..]);
            debug!(
                "MetadataInspector: Found {} keyframe indices in stss",
                keyframe_indices.len()
            );
        }

        // Find stco or co64 (chunk offset table)
        if let Some(stco_pos) = Self::find_atom(moov_data, b"stco") {
            chunk_offsets = Self::parse_stco(&moov_data[stco_pos..], false);
        } else if let Some(co64_pos) = Self::find_atom(moov_data, b"co64") {
            chunk_offsets = Self::parse_stco(&moov_data[co64_pos..], true);
        }

        // Find stsc (sample-to-chunk table)
        if let Some(stsc_pos) = Self::find_atom(moov_data, b"stsc") {
            samples_per_chunk = Self::parse_stsc(&moov_data[stsc_pos..]);
        }

        // Convert keyframe sample indices to byte offsets
        // This is a simplified conversion - for full accuracy we'd need stsz too
        if !keyframe_indices.is_empty() && !chunk_offsets.is_empty() {
            let mut result = Vec::new();

            // Estimate which chunks contain keyframes
            for &kf_idx in keyframe_indices.iter().take(20) {
                // Simple estimation: assume ~1 sample per chunk initially
                let chunk_idx = if samples_per_chunk.is_empty() {
                    (kf_idx as usize).saturating_sub(1)
                } else {
                    // More accurate: use stsc data
                    Self::sample_to_chunk(kf_idx, &samples_per_chunk).saturating_sub(1)
                };

                if chunk_idx < chunk_offsets.len() {
                    result.push(chunk_offsets[chunk_idx]);
                }
            }

            return result;
        }

        // Fallback: return chunk offsets for first few chunks
        chunk_offsets.into_iter().take(10).collect()
    }

    fn find_atom(data: &[u8], atom_type: &[u8; 4]) -> Option<usize> {
        for i in 0..data.len().saturating_sub(8) {
            if &data[i + 4..i + 8] == atom_type {
                return Some(i);
            }
        }
        None
    }

    fn parse_stss(data: &[u8]) -> Vec<u32> {
        // stss: 4 bytes size, 4 bytes type, 1 byte version, 3 bytes flags, 4 bytes entry count, then entries
        if data.len() < 16 {
            return Vec::new();
        }

        let entry_count = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let mut indices = Vec::with_capacity(entry_count.min(100));

        let entries_start = 16;
        for i in 0..entry_count.min(100) {
            let offset = entries_start + i * 4;
            if offset + 4 > data.len() {
                break;
            }
            let sample_number = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            indices.push(sample_number);
        }

        indices
    }

    fn parse_stco(data: &[u8], is_64bit: bool) -> Vec<u64> {
        if data.len() < 16 {
            return Vec::new();
        }

        let entry_count = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let entry_size = if is_64bit { 8 } else { 4 };
        let mut offsets = Vec::with_capacity(entry_count.min(1000));

        let entries_start = 16;
        for i in 0..entry_count.min(1000) {
            let offset = entries_start + i * entry_size;
            if offset + entry_size > data.len() {
                break;
            }

            let chunk_offset = if is_64bit {
                u64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ])
            } else {
                u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as u64
            };

            offsets.push(chunk_offset);
        }

        offsets
    }

    fn parse_stsc(data: &[u8]) -> Vec<(u32, u32)> {
        // stsc: first_chunk, samples_per_chunk, sample_description_index
        if data.len() < 16 {
            return Vec::new();
        }

        let entry_count = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let mut entries = Vec::with_capacity(entry_count.min(100));

        let entries_start = 16;
        for i in 0..entry_count.min(100) {
            let offset = entries_start + i * 12;
            if offset + 12 > data.len() {
                break;
            }
            let first_chunk = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            let samples_per_chunk = u32::from_be_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            entries.push((first_chunk, samples_per_chunk));
        }

        entries
    }

    fn sample_to_chunk(sample_number: u32, stsc: &[(u32, u32)]) -> usize {
        if stsc.is_empty() {
            return sample_number as usize;
        }

        let mut chunk = 1u32;
        let mut sample = 1u32;
        // A crafted stsc can declare samples_per_chunk == 0, which would never
        // advance `sample` and spin until `chunk` overflows; clamp to 1.
        let mut samples_per_chunk = stsc[0].1.max(1);
        let mut stsc_idx = 0;

        while sample < sample_number {
            if stsc_idx + 1 < stsc.len() && chunk >= stsc[stsc_idx + 1].0 {
                stsc_idx += 1;
                samples_per_chunk = stsc[stsc_idx].1.max(1);
            }

            // Overflow-safe form of `sample + samples_per_chunk > sample_number`
            // (the loop condition guarantees sample < sample_number here).
            if samples_per_chunk > sample_number - sample {
                break;
            }

            sample += samples_per_chunk;
            chunk = chunk.saturating_add(1);
        }

        chunk as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal MKV prefix: EBML header, Segment, and a SeekHead whose
    /// single Seek entry points the Cues at `cues_pos` (relative to segment data
    /// start). The file is zero-padded to `total_size`. Returns the bytes and
    /// the segment data start offset.
    fn mkv_with_cues_pos(cues_pos: u64, total_size: usize) -> (Vec<u8>, u64) {
        let mut file = Vec::new();
        // EBML header: 4-byte id, 1-byte size vint (4), 4 payload bytes
        file.extend_from_slice(&[0x1A, 0x45, 0xDF, 0xA3, 0x84, 0, 0, 0, 0]);
        // Segment: 4-byte id + unknown-size vint
        file.extend_from_slice(&[0x18, 0x53, 0x80, 0x67, 0xFF]);
        let segment_data_start = file.len() as u64;
        // Seek entry: SeekID (Cues) + SeekPosition
        let mut seek_entry = Vec::new();
        seek_entry.extend_from_slice(&[0x53, 0xAB, 0x84, 0x1C, 0x53, 0xBB, 0x6B]);
        seek_entry.extend_from_slice(&[0x53, 0xAC, 0x88]);
        seek_entry.extend_from_slice(&cues_pos.to_be_bytes());
        // Seek element wrapping the entry
        let mut seek = vec![0x4D, 0xBB, 0x80 | seek_entry.len() as u8];
        seek.extend_from_slice(&seek_entry);
        // SeekHead wrapping the Seek element
        file.extend_from_slice(&[0x11, 0x4D, 0x9B, 0x74, 0x80 | seek.len() as u8]);
        file.extend_from_slice(&seek);
        file.resize(total_size, 0);
        (file, segment_data_start)
    }

    #[tokio::test]
    async fn mkv_seekhead_pointing_past_eof_falls_back() {
        let total = 1_100_000usize;
        let (file, _) = mkv_with_cues_pos(0xFFFF_FFFF, total);
        let mut cursor = Cursor::new(file);
        let ranges =
            MetadataInspector::find_critical_ranges(&mut cursor, total as u64, "movie.mkv").await;
        assert!(
            ranges.contains(&(total as u64 - 1_000_000, 1_000_000)),
            "must fall back to the end-of-file range: {ranges:?}"
        );
    }

    #[tokio::test]
    async fn mkv_seekhead_valid_cues_prioritized() {
        let total = 400_000usize;
        let (file, segment_data_start) = mkv_with_cues_pos(300_000, total);
        let mut cursor = Cursor::new(file);
        let ranges =
            MetadataInspector::find_critical_ranges(&mut cursor, total as u64, "movie.mkv").await;
        let cues_offset = segment_data_start + 300_000;
        assert!(
            ranges
                .iter()
                .any(|&(start, len)| start == cues_offset && len == total as u64 - cues_offset),
            "cues range must be prioritized: {ranges:?}"
        );
    }

    #[tokio::test]
    async fn mkv_keyframes_with_cues_past_eof_no_panic() {
        let total = 200_000usize;
        let (file, _) = mkv_with_cues_pos(u64::MAX, total);
        let mut cursor = Cursor::new(file);
        let keyframes =
            MetadataInspector::find_keyframe_offsets(&mut cursor, total as u64, "movie.mkv").await;
        assert!(keyframes.is_empty());
    }

    #[test]
    fn stsc_zero_samples_per_chunk_terminates() {
        // A crafted stsc with samples_per_chunk == 0 must not spin forever /
        // overflow the chunk counter; it is treated as 1 sample per chunk.
        assert_eq!(MetadataInspector::sample_to_chunk(5_000, &[(1, 0)]), 5_000);
        // Zero in a later entry as well
        assert_eq!(MetadataInspector::sample_to_chunk(10, &[(1, 2), (3, 0)]), 8);
    }

    #[test]
    fn stsc_huge_samples_per_chunk_no_overflow() {
        // sample + samples_per_chunk must not overflow u32
        assert_eq!(
            MetadataInspector::sample_to_chunk(u32::MAX, &[(1, u32::MAX)]),
            1
        );
    }

    #[test]
    fn stsc_normal_mapping_unchanged() {
        // 2 samples per chunk: samples 1-2 -> chunk 1, 3-4 -> chunk 2, 5 -> chunk 3
        assert_eq!(MetadataInspector::sample_to_chunk(5, &[(1, 2)]), 3);
        assert_eq!(MetadataInspector::sample_to_chunk(1, &[(1, 2)]), 1);
    }

    /// Build an MP4 box: 32-bit size, 4-byte type, payload.
    fn mp4_box(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(payload.len() as u32 + 8).to_be_bytes());
        b.extend_from_slice(kind);
        b.extend_from_slice(payload);
        b
    }

    #[tokio::test]
    async fn mp4_moov_at_front_yields_head_range() {
        let mut file = mp4_box(b"ftyp", &[0u8; 8]);
        let moov_pos = file.len() as u64;
        let moov = mp4_box(b"moov", &[0u8; 100]);
        let moov_len = moov.len() as u64;
        file.extend_from_slice(&moov);
        let total = file.len() as u64;
        let mut cursor = Cursor::new(file);
        let ranges = MetadataInspector::find_critical_ranges(&mut cursor, total, "movie.mp4").await;
        assert!(
            ranges.contains(&(moov_pos, moov_len)),
            "moov head range expected: {ranges:?}"
        );
        // No range beyond the moov itself (no tail fallback)
        assert!(ranges.iter().all(|&(start, _)| start <= moov_pos));
    }

    #[tokio::test]
    async fn mp4_moov_at_end_tail_range_covers_moov() {
        let mut file = mp4_box(b"ftyp", &[0u8; 8]);
        // mdat declaring a huge (>10MB) size triggers the end-of-file moov scan
        file.extend_from_slice(&20_000_000u32.to_be_bytes());
        file.extend_from_slice(b"mdat");
        file.extend_from_slice(&[0u8; 4096]);
        let moov_pos = file.len() as u64;
        let moov = mp4_box(b"moov", &[0u8; 64]);
        let moov_len = moov.len() as u64;
        file.extend_from_slice(&moov);
        let total = file.len() as u64;
        let mut cursor = Cursor::new(file);
        let ranges = MetadataInspector::find_critical_ranges(&mut cursor, total, "movie.mp4").await;
        assert!(
            ranges
                .iter()
                .any(|&(start, len)| start == moov_pos && len >= moov_len),
            "tail range must cover moov: {ranges:?}"
        );
    }

    #[tokio::test]
    async fn mp4_zero_and_overflowing_box_sizes_terminate() {
        // Box size 0: extends to end of file, scan must stop there
        let mut file = Vec::new();
        file.extend_from_slice(&0u32.to_be_bytes());
        file.extend_from_slice(b"free");
        file.extend_from_slice(&[0u8; 64]);
        let total = file.len() as u64;
        let mut cursor = Cursor::new(file);
        let _ = MetadataInspector::find_critical_ranges(&mut cursor, total, "a.mp4").await;

        // 32-bit box size far past the end of the file
        let mut file = Vec::new();
        file.extend_from_slice(&0xFFFF_FFF0u32.to_be_bytes());
        file.extend_from_slice(b"free");
        file.extend_from_slice(&[0u8; 64]);
        let total = file.len() as u64;
        let mut cursor = Cursor::new(file);
        let _ = MetadataInspector::find_critical_ranges(&mut cursor, total, "a.mp4").await;

        // Extended (64-bit) box size of u64::MAX
        let mut file = Vec::new();
        file.extend_from_slice(&1u32.to_be_bytes());
        file.extend_from_slice(b"free");
        file.extend_from_slice(&u64::MAX.to_be_bytes());
        file.extend_from_slice(&[0u8; 64]);
        let total = file.len() as u64;
        let mut cursor = Cursor::new(file);
        let _ = MetadataInspector::find_critical_ranges(&mut cursor, total, "a.mp4").await;
    }

    #[tokio::test]
    async fn truncated_ebml_returns_cleanly() {
        // Claims 2MB but only the 9-byte EBML header is present: the SeekHead
        // read fails and the inspector must fall back without panicking.
        let file = vec![0x1A, 0x45, 0xDF, 0xA3, 0x84, 0, 0, 0, 0];
        let mut cursor = Cursor::new(file);
        let ranges =
            MetadataInspector::find_critical_ranges(&mut cursor, 2_000_000, "movie.mkv").await;
        assert!(ranges.contains(&(1_000_000, 1_000_000)), "{ranges:?}");
    }
}
