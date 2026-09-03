use serde::{Deserialize, Serialize};

/// Startup is gated on the actual first readable bytes. Keep speculative work
/// near MPV's 4 MiB network buffer so rare seek/Cues pieces are not starved by
/// a large urgent head window.
pub const MIN_STARTUP_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_STARTUP_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_SEEK_HOT_WINDOW_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_WARM_WINDOW_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_CONTAINER_METADATA_WINDOW_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_DOWNLOAD_RANGE_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
pub const SMALL_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum pieces to prioritize before first byte is delivered.
pub const MAX_STARTUP_PIECES: i32 = 4;
pub const MAX_SMALL_FILE_STARTUP_PIECES: i32 = 32;

/// Minimum pieces to prioritize before first byte is delivered.
pub const MIN_STARTUP_PIECES: i32 = 2;

/// Aggressive seek/read-ahead defaults. These are internal on purpose: tuning is
/// driven by runtime measurements and logs rather than user-facing settings.
pub const MIN_SEEK_HOT_PIECES: i32 = 24;
pub const SEEK_IMMEDIATE_PIECES: i32 = 12;
pub const MAX_HOT_PIECES: i32 = 96;
pub const MAX_WARM_PIECES: i32 = 192;
pub const BLOCKED_REPLAN_INTERVAL_MS: u64 = 8_000;
const MIN_PIECE_DEADLINE_STEP_MS: u64 = 500;
const MAX_PIECE_DEADLINE_STEP_MS: u64 = 30_000;

/// Start treating reads as "container metadata" when they fall in the last 10MB
/// or the last 5% of the file, whichever starts earlier.
pub fn container_metadata_start(file_size: u64) -> u64 {
    if file_size == 0 {
        0
    } else if file_size < SMALL_FILE_BYTES {
        file_size.saturating_mul(95) / 100
    } else {
        file_size
            .saturating_sub(10 * 1024 * 1024)
            .min(file_size.saturating_mul(95) / 100)
    }
}

pub fn is_container_metadata_request(start: u64, requested_len: u64, file_size: u64) -> bool {
    start > 0
        && file_size > 0
        && requested_len > 0
        && requested_len <= MAX_CONTAINER_METADATA_WINDOW_BYTES
        && start >= container_metadata_start(file_size)
}

/// How far ahead a *playback* stream reads.
///
/// The read-ahead windows are constants tuned for a healthy connection and a
/// patient player. A spotty link -- or a receiver with a shallower buffer than
/// mpv's -- wants more of the file fetched before it is needed, at the cost of
/// downloading further ahead than will necessarily be watched. This is that
/// choice, as a multiplier applied to the playback windows.
///
/// It deliberately does **not** touch the startup window
/// ([`MAX_STARTUP_WINDOW_BYTES`]): the narrow first-frame want-set is what
/// makes playback start quickly, and widening it would spend that latency to
/// buy read-ahead the very next request already provides. Every profile
/// starts a stream the same way and differs only once bytes are flowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BufferProfile {
    /// Today's behaviour, and the default.
    #[default]
    Normal,
    /// Twice the playback read-ahead.
    Large,
    /// Four times the playback read-ahead.
    Maximum,
}

impl BufferProfile {
    /// Every profile, in ascending window order -- for enumerating the choice
    /// in a UI or a test.
    pub const ALL: [BufferProfile; 3] = [Self::Normal, Self::Large, Self::Maximum];

    /// The wire spelling, matching the `serde` representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Large => "large",
            Self::Maximum => "maximum",
        }
    }

    /// Parse a wire spelling. Surrounding whitespace and case are ignored;
    /// anything else is `None`, which callers turn into "use the default"
    /// rather than into a failed request.
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        Self::ALL
            .into_iter()
            .find(|profile| value.eq_ignore_ascii_case(profile.as_str()))
    }

    /// The multiplier this profile applies to a playback window.
    const fn window_scale(self) -> u64 {
        match self {
            Self::Normal => 1,
            Self::Large => 2,
            Self::Maximum => 4,
        }
    }

    /// Scale a playback read-ahead window in bytes. Saturating, so no profile
    /// can wrap a large window round to a small one.
    pub const fn scale_playback_window(self, bytes: u64) -> u64 {
        bytes.saturating_mul(self.window_scale())
    }

    /// Scale a playback read-ahead window measured in pieces.
    pub const fn scale_playback_pieces(self, pieces: i32) -> i32 {
        pieces.saturating_mul(self.window_scale() as i32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlaybackIntent {
    DirectInitial,
    DirectSeek,
    DirectSequential,
    DownloadFull,
    DownloadRange,
    ContainerMetadata,
    InternalProbe,
    Background,
}

impl PlaybackIntent {
    pub fn sequential_after_first_byte(self) -> Self {
        match self {
            Self::DirectInitial | Self::DirectSeek | Self::DirectSequential => {
                Self::DirectSequential
            }
            Self::DownloadFull | Self::DownloadRange => self,
            other => other,
        }
    }

    pub fn seek_for_same_family(self) -> Self {
        if matches!(self, Self::DownloadFull | Self::DownloadRange) {
            Self::DownloadRange
        } else {
            Self::DirectSeek
        }
    }
}

pub fn disk_backed_sequential_download(intent: PlaybackIntent) -> bool {
    matches!(intent, PlaybackIntent::DownloadFull)
}

pub fn disk_backed_file_baseline_priority(intent: PlaybackIntent) -> i32 {
    match intent {
        PlaybackIntent::DownloadFull | PlaybackIntent::DownloadRange => 7,
        // Every streaming intent keeps the WHOLE file minimally wanted
        // (priority 1). A baseline of 0 leaves only the forward window wanted, so
        // once that small window verifies the torrent reports is_finished and
        // drops to seeding/idle -- the download rate craters, read-ahead stops,
        // and (with seeding disabled) the upload is throttled mid-stream. With
        // baseline 1 the file stays "downloading" until it is actually complete;
        // the forward window (priority 7 + deadlines) still concentrates
        // bandwidth on the requested region, so seek/startup stay fast.
        PlaybackIntent::DirectInitial
        | PlaybackIntent::DirectSeek
        | PlaybackIntent::ContainerMetadata
        | PlaybackIntent::DirectSequential
        | PlaybackIntent::InternalProbe
        | PlaybackIntent::Background => 1,
    }
}

pub fn disk_backed_forward_window_pieces(intent: PlaybackIntent, buffer: BufferProfile) -> i32 {
    match intent {
        PlaybackIntent::DownloadFull => 64,
        PlaybackIntent::DownloadRange => 8,
        // Startup is not scaled by the buffer profile: see [`BufferProfile`].
        PlaybackIntent::DirectInitial => MAX_STARTUP_PIECES,
        PlaybackIntent::DirectSeek | PlaybackIntent::DirectSequential => {
            buffer.scale_playback_pieces(32)
        }
        // The container seek index (MKV Cues / MP4 moov) spans several pieces;
        // 16 lets the 16 MB MAX_CONTAINER_METADATA_WINDOW_BYTES cap govern (via
        // cap_pieces_by_bytes) so the whole region downloads in parallel instead
        // of one rare piece at a time.
        PlaybackIntent::ContainerMetadata => 16,
        PlaybackIntent::InternalProbe => 1,
        PlaybackIntent::Background => 1,
    }
}

pub fn disk_backed_forward_window_pieces_for(
    intent: PlaybackIntent,
    piece_length: u64,
    buffer: BufferProfile,
) -> i32 {
    let pieces = disk_backed_forward_window_pieces(intent, buffer);
    let byte_cap = match intent {
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange => MAX_DOWNLOAD_RANGE_WINDOW_BYTES,
        // Startup is not scaled by the buffer profile: see [`BufferProfile`].
        PlaybackIntent::DirectInitial => MAX_STARTUP_WINDOW_BYTES,
        PlaybackIntent::DirectSeek | PlaybackIntent::DirectSequential => {
            buffer.scale_playback_window(MAX_SEEK_HOT_WINDOW_BYTES)
        }
        PlaybackIntent::ContainerMetadata | PlaybackIntent::InternalProbe => {
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        }
        PlaybackIntent::Background => MAX_CONTAINER_METADATA_WINDOW_BYTES,
    };
    cap_pieces_by_bytes(pieces, piece_length, byte_cap)
}

/// Per-stream lookahead window (in bytes) for librqbit's `FileStreamOptions`,
/// sized by playback intent instead of librqbit's fixed 32 MiB default. Reuses
/// the same `MAX_*_WINDOW_BYTES` caps `disk_backed_forward_window_pieces_for`
/// maps each intent onto, so librqbit reads ahead by the same byte budget the
/// disk-cache path uses. `stream_with_options` rejects a zero window, so the
/// result is clamped to at least 1 (all constants are already > 0).
///
/// `buffer` is the viewer's read-ahead choice and scales the playback windows
/// only -- never the startup one, see [`BufferProfile`].
pub fn librqbit_stream_lookahead_bytes(intent: PlaybackIntent, buffer: BufferProfile) -> u64 {
    match intent {
        // First-frame latency: narrow the startup want-set (4 MiB) so the head
        // pieces verify faster than under librqbit's 32 MiB default. This is
        // the one window the buffer profile leaves alone: widening it would
        // trade first-frame latency away for read-ahead the next request
        // (already DirectSequential, already scaled) supplies anyway.
        PlaybackIntent::DirectInitial => MAX_STARTUP_WINDOW_BYTES,
        // Hot read-ahead once playing / after a seek -- what the viewer's
        // buffer choice is actually about.
        PlaybackIntent::DirectSeek | PlaybackIntent::DirectSequential => {
            buffer.scale_playback_window(MAX_SEEK_HOT_WINDOW_BYTES)
        }
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange => MAX_DOWNLOAD_RANGE_WINDOW_BYTES,
        PlaybackIntent::ContainerMetadata
        | PlaybackIntent::InternalProbe
        | PlaybackIntent::Background => MAX_CONTAINER_METADATA_WINDOW_BYTES,
    }
    .max(1)
}

/// Progress of the priority window a stream is waiting on: how much of it is
/// covered by verified pieces. `have_piece` answers for absolute torrent
/// piece indices. Returns `(ready_bytes, window_bytes)`; an empty window
/// yields `(0, 0)`, which callers treat as ready, and `ready == window`
/// means every byte the reader is about to ask for is servable.
///
/// `read_from` is the offset **inside the file** the reader is positioned
/// at, not always 0: the window follows the reader. Anchoring it at the
/// file head meant that after a seek the number described bytes nobody was
/// fetching and sat at 0% while the seek region streamed perfectly.
///
/// The window is then **expanded to whole pieces**, because a piece is the
/// unit that becomes readable: none of an 8 MiB piece can be served until
/// all 8 MiB of it verifies. Reporting a 4 MiB window inside a 16 MiB piece
/// described a quantity that did not exist -- the client could only ever
/// see 0% or 100% of it, and on a 7.5 GB torrent (16 MiB pieces) at
/// 300 kB/s that is 55 seconds of literal "0%" while the download runs
/// perfectly. The denominator is now what actually has to arrive, and
/// `EngineStats::piece_length` says how big the steps are, so a client can
/// say "waiting for the first piece (16 MiB)" instead of showing a stalled
/// percentage. `ready == window` is unchanged either way: it still means
/// exactly "every piece the window touches is verified".
pub fn initial_window_progress(
    file_offset: u64,
    file_len: u64,
    piece_length: u64,
    window_bytes: u64,
    read_from: u64,
    have_piece: impl Fn(u64) -> bool,
) -> (u64, u64) {
    let read_from = read_from.min(file_len);
    let window = window_bytes.min(file_len - read_from);
    if window == 0 || piece_length == 0 {
        return (0, window);
    }
    let window_start = file_offset + read_from;
    let window_end = window_start + window;
    let first_piece = window_start / piece_length;
    let last_piece = (window_end - 1) / piece_length;
    // Whole pieces, clipped to the file: bytes of a straddling piece that
    // belong to a neighbouring file are that file's business, and the
    // reader can be served as soon as the piece verifies either way.
    let span_start = (first_piece * piece_length).max(file_offset);
    let span_end = ((last_piece + 1) * piece_length).min(file_offset + file_len);
    let mut ready = 0u64;
    for piece in first_piece..=last_piece {
        if !have_piece(piece) {
            continue;
        }
        let piece_start = (piece * piece_length).max(span_start);
        let piece_end = ((piece + 1) * piece_length).min(span_end);
        ready += piece_end.saturating_sub(piece_start);
    }
    (ready, span_end - span_start)
}

/// The index of the piece an open reader is sitting on: the piece that has
/// to arrive before the reader can advance, and so the one whose sub-piece
/// progress is worth showing (see `backend::InFlightPiece`).
///
/// `read_from` is the reader's offset **inside the file**, the same offset
/// [`initial_window_progress`] measures its window from, and the result is a
/// **torrent-wide** piece index. An offset at or past the end of the file is
/// clamped to its last byte, so a reader parked on the end still names the
/// piece it last needed rather than a neighbouring file's.
///
/// `None` for an empty file or an unknown piece length: there is no piece to
/// wait for, and absence is the honest answer.
pub fn reader_piece_index(
    file_offset: u64,
    file_len: u64,
    piece_length: u64,
    read_from: u64,
) -> Option<u64> {
    if file_len == 0 || piece_length == 0 {
        return None;
    }
    let within = read_from.min(file_len - 1);
    Some((file_offset + within) / piece_length)
}

pub fn playback_deadline_step_ms(
    piece_length: u64,
    bitrate_bytes_per_sec: Option<u64>,
    download_rate_bytes_per_sec: u64,
) -> i32 {
    let bitrate = bitrate_bytes_per_sec.filter(|rate| *rate > 0);
    let download_rate = (download_rate_bytes_per_sec > 0).then_some(download_rate_bytes_per_sec);
    let effective_rate = match (bitrate, download_rate) {
        (Some(bitrate), Some(download_rate)) => bitrate.min(download_rate),
        (Some(bitrate), None) => bitrate,
        (None, Some(download_rate)) => download_rate,
        (None, None) => 1024 * 1024,
    };
    let step_ms = piece_length
        .saturating_mul(1_000)
        .div_ceil(effective_rate)
        .clamp(MIN_PIECE_DEADLINE_STEP_MS, MAX_PIECE_DEADLINE_STEP_MS);
    step_ms as i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemoryPressure {
    Normal,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PriorityBand {
    Immediate,
    Hot,
    Warm,
    Metadata,
    Background,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EngineCacheConfig {
    pub size: u64,
    pub enabled: bool,
}

impl Default for EngineCacheConfig {
    fn default() -> Self {
        Self {
            size: 10 * 1024 * 1024 * 1024, // 10 GB
            enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PriorityContext {
    pub intent: PlaybackIntent,
    /// The viewer's read-ahead choice; scales the playback windows only.
    pub buffer: BufferProfile,
    pub current_piece: i32,
    pub first_piece: i32,
    pub last_piece: i32,
    pub piece_length: u64,
    pub file_size: u64,
    pub bitrate_bytes_per_sec: Option<u64>,
    pub download_rate_bytes_per_sec: u64,
    pub peers: u64,
    pub cache_size_bytes: u64,
    pub memory_pressure: MemoryPressure,
    pub consecutive_waits: u32,
    pub first_byte_sent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityAssignment {
    pub piece_idx: i32,
    pub piece_priority: i32,
    pub deadline: i32,
    pub band: PriorityBand,
}

pub type PriorityItem = PriorityAssignment;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityDecision {
    pub assignments: Vec<PriorityAssignment>,
    pub target_window_pieces: i32,
    pub immediate_pieces: i32,
    pub hot_window_pieces: i32,
    pub warm_window_pieces: i32,
    pub reason: String,
}

pub struct PlaybackPriorityPolicy;

impl PlaybackPriorityPolicy {
    pub fn decide(ctx: PriorityContext) -> PriorityDecision {
        if ctx.piece_length == 0
            || ctx.current_piece < ctx.first_piece
            || ctx.last_piece < ctx.first_piece
            || ctx.current_piece > ctx.last_piece
        {
            return PriorityDecision {
                assignments: Vec::new(),
                target_window_pieces: 0,
                immediate_pieces: 0,
                hot_window_pieces: 0,
                warm_window_pieces: 0,
                reason: "invalid-context".to_string(),
            };
        }

        let max_cache_pieces = if ctx.cache_size_bytes > 0 {
            // Clamp before the i32 cast: an unlimited cache (u64::MAX) divided
            // by a power-of-two piece length has all-ones low 32 bits, which a
            // bare `as i32` would truncate to -1 and poison every window below.
            (ctx.cache_size_bytes / ctx.piece_length).clamp(1, i32::MAX as u64) as i32
        } else {
            MAX_HOT_PIECES
        };
        let remaining_pieces = ctx.last_piece.saturating_sub(ctx.current_piece) + 1;

        let bitrate_ratio = ctx
            .bitrate_bytes_per_sec
            .filter(|bitrate| *bitrate > 0)
            .map(|bitrate| ctx.download_rate_bytes_per_sec as f64 / bitrate as f64);

        let mut reason = match ctx.intent {
            PlaybackIntent::DirectInitial => "initial".to_string(),
            PlaybackIntent::DirectSeek => "seek".to_string(),
            PlaybackIntent::DirectSequential => "sequential".to_string(),
            PlaybackIntent::DownloadFull => "download-full".to_string(),
            PlaybackIntent::DownloadRange => "download-range".to_string(),
            PlaybackIntent::ContainerMetadata => "container-metadata".to_string(),
            PlaybackIntent::InternalProbe => "internal-probe".to_string(),
            PlaybackIntent::Background => "background".to_string(),
        };

        let (mut immediate, mut hot, mut warm) = match ctx.intent {
            PlaybackIntent::DirectInitial if !ctx.first_byte_sent => {
                let target_bytes = match ctx.bitrate_bytes_per_sec {
                    Some(bitrate) => bitrate.saturating_mul(10).max(MIN_STARTUP_BYTES),
                    None if ctx.file_size > 0 && ctx.file_size <= SMALL_FILE_BYTES => {
                        ctx.file_size.min(MAX_STARTUP_WINDOW_BYTES)
                    }
                    None => MIN_STARTUP_BYTES,
                }
                .max(ctx.piece_length)
                .min(MAX_STARTUP_WINDOW_BYTES.max(ctx.piece_length));
                let pieces = target_bytes.saturating_add(ctx.piece_length.saturating_sub(1))
                    / ctx.piece_length;
                let max_startup_pieces =
                    if ctx.bitrate_bytes_per_sec.is_none() && ctx.file_size <= SMALL_FILE_BYTES {
                        pieces_for_bytes(
                            ctx.file_size.min(MAX_STARTUP_WINDOW_BYTES),
                            ctx.piece_length,
                        )
                        .clamp(1, MAX_SMALL_FILE_STARTUP_PIECES)
                    } else {
                        MAX_STARTUP_PIECES
                    };
                let min_startup_pieces = MIN_STARTUP_PIECES.min(max_startup_pieces).max(1);
                let pieces = (pieces as i32).clamp(min_startup_pieces, max_startup_pieces);
                // Keep the immediate band small so the swarm focuses bandwidth
                // on the head pieces the player needs for its first bytes; the
                // rest of the startup window rides in the hot band behind it.
                (pieces.min(MAX_STARTUP_PIECES), pieces, 0)
            }
            PlaybackIntent::DirectInitial | PlaybackIntent::DirectSequential => {
                let hot = dynamic_hot_window(&ctx, bitrate_ratio);
                (2, hot, 32)
            }
            PlaybackIntent::DownloadFull => (2, 16, 0),
            PlaybackIntent::DownloadRange => (1, 4, 0),
            PlaybackIntent::DirectSeek if !ctx.first_byte_sent => {
                reason.push_str("-first-piece-only");
                (1, 1, 0)
            }
            PlaybackIntent::DirectSeek => {
                let mut hot = dynamic_hot_window(&ctx, bitrate_ratio).max(MIN_SEEK_HOT_PIECES);
                let mut immediate = SEEK_IMMEDIATE_PIECES;
                if ctx.consecutive_waits >= 3 {
                    hot = (hot * 2).min(MAX_HOT_PIECES);
                    immediate = (immediate * 2).min(hot);
                    reason.push_str("-blocked-expand");
                }
                (immediate, hot, 32)
            }
            PlaybackIntent::ContainerMetadata => (1, 2, 0),
            PlaybackIntent::InternalProbe => (0, 2, 0),
            PlaybackIntent::Background => (0, 4, 0),
        };

        if matches!(ctx.memory_pressure, MemoryPressure::High) {
            hot = hot.min(MIN_SEEK_HOT_PIECES);
            warm = 0;
            reason.push_str("-memory-clamp");
        }

        if matches!(
            ctx.intent,
            PlaybackIntent::Background | PlaybackIntent::InternalProbe
        ) {
            warm = 0;
        }

        let original_hot = hot;
        let original_warm = warm;
        hot = cap_pieces_by_bytes(hot, ctx.piece_length, hot_byte_cap(&ctx));
        warm = cap_pieces_by_bytes(
            warm,
            ctx.piece_length,
            warm_byte_cap(ctx.intent, ctx.buffer),
        );
        if hot < original_hot || warm < original_warm {
            reason.push_str("-byte-cap");
        }

        hot = hot
            .clamp(0, ctx.buffer.scale_playback_pieces(MAX_HOT_PIECES))
            .min(max_cache_pieces)
            .min(remaining_pieces);
        warm = warm
            .clamp(0, ctx.buffer.scale_playback_pieces(MAX_WARM_PIECES))
            .min(max_cache_pieces.saturating_sub(hot))
            .min(remaining_pieces.saturating_sub(hot));
        immediate = immediate.min(hot).max(0);

        let target_window = hot + warm;
        let mut assignments = Vec::with_capacity(target_window as usize);
        for distance in 0..target_window {
            let piece_idx = ctx.current_piece + distance;
            if piece_idx > ctx.last_piece {
                break;
            }

            let (band, piece_priority, deadline) = assignment_for(&ctx, distance, immediate, hot);
            assignments.push(PriorityAssignment {
                piece_idx,
                piece_priority,
                deadline,
                band,
            });
        }

        PriorityDecision {
            assignments,
            target_window_pieces: target_window,
            immediate_pieces: immediate,
            hot_window_pieces: hot,
            warm_window_pieces: warm,
            reason,
        }
    }
}

fn pieces_for_bytes(bytes: u64, piece_length: u64) -> i32 {
    if bytes == 0 || piece_length == 0 {
        return 0;
    }

    let pieces = bytes.saturating_add(piece_length.saturating_sub(1)) / piece_length;
    pieces.clamp(1, i32::MAX as u64) as i32
}

fn cap_pieces_by_bytes(pieces: i32, piece_length: u64, max_bytes: u64) -> i32 {
    if pieces <= 0 {
        return 0;
    }
    if max_bytes == 0 {
        return 0;
    }

    pieces.min(pieces_for_bytes(max_bytes, piece_length).max(1))
}

fn hot_byte_cap(ctx: &PriorityContext) -> u64 {
    match ctx.intent {
        // Startup is not scaled by the buffer profile: see [`BufferProfile`].
        PlaybackIntent::DirectInitial if !ctx.first_byte_sent => MAX_STARTUP_WINDOW_BYTES,
        PlaybackIntent::DirectInitial
        | PlaybackIntent::DirectSeek
        | PlaybackIntent::DirectSequential => {
            ctx.buffer.scale_playback_window(MAX_SEEK_HOT_WINDOW_BYTES)
        }
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange => MAX_DOWNLOAD_RANGE_WINDOW_BYTES,
        PlaybackIntent::ContainerMetadata | PlaybackIntent::InternalProbe => {
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        }
        PlaybackIntent::Background => MAX_CONTAINER_METADATA_WINDOW_BYTES,
    }
}

fn warm_byte_cap(intent: PlaybackIntent, buffer: BufferProfile) -> u64 {
    match intent {
        // The warm band trails the hot one through the same file: it is part
        // of the read-ahead the viewer chose, so it scales with it. A pinned
        // download's warm window is not a playback choice and does not.
        PlaybackIntent::DirectInitial
        | PlaybackIntent::DirectSeek
        | PlaybackIntent::DirectSequential => buffer.scale_playback_window(MAX_WARM_WINDOW_BYTES),
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange
        | PlaybackIntent::ContainerMetadata
        | PlaybackIntent::InternalProbe
        | PlaybackIntent::Background => 0,
    }
}

fn dynamic_hot_window(ctx: &PriorityContext, bitrate_ratio: Option<f64>) -> i32 {
    let mut hot = if let Some(ratio) = bitrate_ratio {
        if ratio >= 3.0 {
            96
        } else if ratio >= 1.5 {
            48
        } else if ratio >= 1.0 {
            32
        } else {
            MIN_SEEK_HOT_PIECES
        }
    } else if ctx.download_rate_bytes_per_sec > 10 * 1024 * 1024 {
        96
    } else if ctx.download_rate_bytes_per_sec > 5 * 1024 * 1024 {
        48
    } else if ctx.download_rate_bytes_per_sec > 1024 * 1024 {
        MIN_SEEK_HOT_PIECES
    } else {
        16
    };

    if let Some(bitrate) = ctx.bitrate_bytes_per_sec.filter(|bitrate| *bitrate > 0) {
        let pieces_for_10s = ((bitrate.saturating_mul(10)) / ctx.piece_length).max(1) as i32;
        hot = hot.max(pieces_for_10s);
    }

    if ctx.peers < 3 {
        hot = hot.min(MIN_SEEK_HOT_PIECES);
    }

    // Whatever the policy sized the hot band at, the viewer's buffer choice
    // multiplies it -- including the few-peers clamp above, which exists to
    // avoid spreading a thin swarm thin, and which a viewer asking for more
    // read-ahead on a bad connection is deliberately overriding.
    ctx.buffer.scale_playback_pieces(hot)
}

fn assignment_for(
    ctx: &PriorityContext,
    distance: i32,
    immediate_pieces: i32,
    hot_pieces: i32,
) -> (PriorityBand, i32, i32) {
    let deadline_step = playback_deadline_step_ms(
        ctx.piece_length,
        ctx.bitrate_bytes_per_sec,
        ctx.download_rate_bytes_per_sec,
    );
    match ctx.intent {
        PlaybackIntent::ContainerMetadata => (PriorityBand::Metadata, 7, distance * deadline_step),
        PlaybackIntent::InternalProbe => (
            PriorityBand::Background,
            1,
            10_000 + distance * deadline_step,
        ),
        PlaybackIntent::Background => (
            PriorityBand::Background,
            1,
            30_000 + distance * deadline_step,
        ),
        _ if distance < immediate_pieces => (PriorityBand::Immediate, 7, distance * deadline_step),
        _ if distance < hot_pieces => (PriorityBand::Hot, 4, distance * deadline_step),
        _ => (PriorityBand::Warm, 2, 10_000 + distance * deadline_step),
    }
}

/// Backward-compatible wrapper used by older callers and tests.
pub fn calculate_priorities(
    current_piece: i32,
    total_pieces: i32,
    piece_length: u64,
    config: &EngineCacheConfig,
    priority: u8,
    download_speed: u64,
    bitrate: Option<u64>,
) -> Vec<PriorityItem> {
    let intent = if priority >= 250 {
        PlaybackIntent::InternalProbe
    } else if priority >= 100 {
        PlaybackIntent::DirectSeek
    } else if priority == 0 {
        PlaybackIntent::Background
    } else {
        PlaybackIntent::DirectSequential
    };

    PlaybackPriorityPolicy::decide(PriorityContext {
        intent,
        buffer: BufferProfile::Normal,
        current_piece,
        first_piece: 0,
        last_piece: total_pieces.saturating_sub(1),
        piece_length,
        file_size: total_pieces.max(0) as u64 * piece_length,
        bitrate_bytes_per_sec: bitrate,
        download_rate_bytes_per_sec: download_speed,
        peers: 8,
        cache_size_bytes: if config.enabled { config.size } else { 0 },
        memory_pressure: MemoryPressure::Normal,
        consecutive_waits: 0,
        first_byte_sent: true,
    })
    .assignments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_context(intent: PlaybackIntent) -> PriorityContext {
        PriorityContext {
            intent,
            buffer: BufferProfile::Normal,
            current_piece: 100,
            first_piece: 0,
            last_piece: 999,
            piece_length: 1024 * 1024,
            file_size: 1000 * 1024 * 1024,
            bitrate_bytes_per_sec: None,
            download_rate_bytes_per_sec: 2 * 1024 * 1024,
            peers: 10,
            cache_size_bytes: 1024 * 1024 * 1024,
            memory_pressure: MemoryPressure::Normal,
            consecutive_waits: 0,
            first_byte_sent: true,
        }
    }

    #[test]
    fn initial_before_first_byte_is_small() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.target_window_pieces >= MIN_STARTUP_PIECES);
        assert!(decision.target_window_pieces <= MAX_STARTUP_PIECES);
        assert_eq!(decision.assignments[0].deadline, 0);
        assert_eq!(decision.assignments[0].piece_priority, 7);
    }

    #[test]
    fn unlimited_cache_size_does_not_overflow_windows() {
        // cacheSize:null (unlimited) flows in as u64::MAX; with a power-of-two
        // piece length the quotient's low 32 bits are all ones, which used to
        // truncate to max_cache_pieces = -1 and panic on Vec::with_capacity.
        let mut ctx = base_context(PlaybackIntent::DirectSequential);
        ctx.cache_size_bytes = u64::MAX;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.target_window_pieces > 0);
        assert!(decision.hot_window_pieces >= MIN_SEEK_HOT_PIECES);
        assert!(!decision.assignments.is_empty());
    }

    #[test]
    fn current_piece_past_last_piece_does_not_panic() {
        // current_piece can exceed last_piece when a seek lands past the
        // torrent's declared piece count (e.g. a stale/racy stats snapshot,
        // or a crafted seek offset). Previously nothing clamped this, so
        // remaining_pieces went negative, hot/warm followed it negative,
        // and `Vec::with_capacity(target_window as usize)` turned a small
        // negative i32 into a huge usize and panicked with "capacity
        // overflow".
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.first_byte_sent = true;
        ctx.current_piece = 5000;
        ctx.last_piece = 999;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.target_window_pieces >= 0);
        assert!(decision.hot_window_pieces >= 0);
        assert!(decision.warm_window_pieces >= 0);
        assert!(decision.assignments.is_empty());
    }

    #[test]
    fn initial_after_first_byte_expands() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DirectInitial));

        assert!(decision.hot_window_pieces >= MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.assignments[0].band, PriorityBand::Immediate);
    }

    #[test]
    fn direct_seek_has_minimum_hot_window() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DirectSeek));

        assert!(decision.hot_window_pieces >= MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.immediate_pieces, SEEK_IMMEDIATE_PIECES);
        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert_eq!(decision.assignments[3].piece_priority, 7);
        assert_eq!(decision.assignments[4].piece_priority, 7);
        assert_eq!(
            decision.assignments[SEEK_IMMEDIATE_PIECES as usize - 1].piece_priority,
            7
        );
        assert_eq!(
            decision.assignments[SEEK_IMMEDIATE_PIECES as usize].piece_priority,
            4
        );
    }

    #[test]
    fn fast_swarm_expands_seek_window() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.hot_window_pieces >= 96);
    }

    #[test]
    fn memory_pressure_clamps_window() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        ctx.memory_pressure = MemoryPressure::High;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.warm_window_pieces, 0);
    }

    #[test]
    fn internal_probe_uses_low_priority() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::InternalProbe));

        assert!(
            decision
                .assignments
                .iter()
                .all(|item| item.piece_priority <= 1)
        );
    }

    #[test]
    fn compatibility_wrapper_still_returns_priorities() {
        let config = EngineCacheConfig {
            size: 200 * 1024 * 1024,
            enabled: true,
        };
        let priorities = calculate_priorities(0, 1000, 10 * 1024 * 1024, &config, 1, 0, None);

        assert!(!priorities.is_empty());
        assert_eq!(priorities[0].piece_idx, 0);
    }

    #[test]
    fn disk_backed_sequential_is_only_for_full_downloads() {
        assert!(disk_backed_sequential_download(
            PlaybackIntent::DownloadFull
        ));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::DownloadRange
        ));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::DirectInitial
        ));
        assert!(!disk_backed_sequential_download(PlaybackIntent::DirectSeek));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::ContainerMetadata
        ));
    }

    #[test]
    fn disk_backed_container_metadata_window_covers_cues_region() {
        // The Cues/moov region spans several pieces; the window is wide enough
        // for the 16 MB byte cap (cap_pieces_by_bytes) to govern so the whole
        // index downloads in parallel instead of one rare piece at a time.
        assert_eq!(
            disk_backed_forward_window_pieces(
                PlaybackIntent::ContainerMetadata,
                BufferProfile::Normal
            ),
            16
        );
        assert!(
            disk_backed_forward_window_pieces(PlaybackIntent::DownloadFull, BufferProfile::Normal)
                > disk_backed_forward_window_pieces(
                    PlaybackIntent::DownloadRange,
                    BufferProfile::Normal
                )
        );
    }

    #[test]
    fn disk_backed_streaming_keeps_whole_file_wanted() {
        // Streaming intents keep the whole file minimally wanted (priority 1) so
        // the torrent never reports is_finished after just the forward window
        // completes (which stalls read-ahead / idles the download). Downloads
        // stay at 7.
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectInitial),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectSeek),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::ContainerMetadata),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectSequential),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DownloadFull),
            7
        );
    }

    #[test]
    fn blocking_container_metadata_is_urgent() {
        let decision =
            PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::ContainerMetadata));

        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert_eq!(decision.assignments[0].deadline, 0);
    }

    #[test]
    fn small_file_metadata_starts_at_final_five_percent() {
        let file_size = 8 * 1024 * 1024;
        assert_eq!(container_metadata_start(file_size), file_size * 95 / 100);
        assert!(!is_container_metadata_request(1024 * 1024, 1024, file_size));
        assert!(is_container_metadata_request(
            container_metadata_start(file_size),
            1024,
            file_size
        ));
    }

    #[test]
    fn large_near_end_playback_range_is_not_metadata_when_range_is_large() {
        let file_size = 10 * 1024 * 1024 * 1024;
        let start = container_metadata_start(file_size);

        assert!(is_container_metadata_request(
            start,
            MAX_CONTAINER_METADATA_WINDOW_BYTES,
            file_size
        ));
        assert!(!is_container_metadata_request(
            start,
            MAX_CONTAINER_METADATA_WINDOW_BYTES + 1,
            file_size
        ));
    }

    #[test]
    fn download_range_priority_is_bounded() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DownloadRange));

        assert_eq!(decision.hot_window_pieces, 4);
        assert_eq!(decision.warm_window_pieces, 0);
        assert_eq!(decision.assignments[0].piece_priority, 7);
    }

    #[test]
    fn seek_window_is_capped_by_bytes_for_large_pieces() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.piece_length = 16 * 1024 * 1024;
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        let expected_hot = (MAX_SEEK_HOT_WINDOW_BYTES / ctx.piece_length) as i32;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, expected_hot);
        assert!(decision.reason.contains("byte-cap"));
    }

    #[test]
    fn startup_window_is_capped_by_bytes_for_huge_pieces() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        ctx.piece_length = 64 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, 1);
        assert!(decision.target_window_pieces <= 1);
    }

    #[test]
    fn small_file_startup_respects_mpv_buffer_cap() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        ctx.file_size = 8 * 1024 * 1024;
        ctx.last_piece = 7;
        ctx.piece_length = 1024 * 1024;
        let piece_length = ctx.piece_length;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, 4);
        assert!(decision.hot_window_pieces as u64 * piece_length <= MAX_STARTUP_WINDOW_BYTES);
        assert_eq!(decision.warm_window_pieces, 0);
        // Immediate band stays focused on the head so the first pieces get
        // all the bandwidth instead of the whole file downloading in parallel.
        assert!(decision.immediate_pieces <= MAX_STARTUP_PIECES);
    }

    #[test]
    fn disk_backed_forward_window_respects_piece_size_byte_cap() {
        assert_eq!(
            disk_backed_forward_window_pieces_for(
                PlaybackIntent::DownloadRange,
                64 * 1024 * 1024,
                BufferProfile::Normal
            ),
            1
        );
    }

    #[test]
    fn cold_seek_only_prioritizes_the_requested_piece() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.first_byte_sent = false;
        ctx.piece_length = 16 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.assignments.len(), 1);
        assert_eq!(decision.assignments[0].piece_idx, 100);
        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert!(decision.reason.contains("first-piece-only"));
    }

    #[test]
    fn librqbit_lookahead_maps_each_intent_to_its_window_cap() {
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DirectInitial, BufferProfile::Normal),
            MAX_STARTUP_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, BufferProfile::Normal),
            MAX_SEEK_HOT_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(
                PlaybackIntent::DirectSequential,
                BufferProfile::Normal
            ),
            MAX_SEEK_HOT_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DownloadFull, BufferProfile::Normal),
            MAX_WARM_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DownloadRange, BufferProfile::Normal),
            MAX_DOWNLOAD_RANGE_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(
                PlaybackIntent::ContainerMetadata,
                BufferProfile::Normal
            ),
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::InternalProbe, BufferProfile::Normal),
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::Background, BufferProfile::Normal),
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        );
        // Every intent must produce a positive window (stream_with_options
        // rejects 0).
        for intent in [
            PlaybackIntent::DirectInitial,
            PlaybackIntent::DirectSeek,
            PlaybackIntent::DirectSequential,
            PlaybackIntent::DownloadFull,
            PlaybackIntent::DownloadRange,
            PlaybackIntent::ContainerMetadata,
            PlaybackIntent::InternalProbe,
            PlaybackIntent::Background,
        ] {
            assert!(librqbit_stream_lookahead_bytes(intent, BufferProfile::Normal) > 0);
        }
    }

    #[test]
    fn deadlines_track_the_slower_of_playback_and_download_rates() {
        let mut ctx = base_context(PlaybackIntent::DirectSequential);
        ctx.piece_length = 16 * 1024 * 1024;
        ctx.bitrate_bytes_per_sec = Some(8 * 1024 * 1024);
        ctx.download_rate_bytes_per_sec = 32 * 1024 * 1024;
        let deadline_step = playback_deadline_step_ms(
            ctx.piece_length,
            ctx.bitrate_bytes_per_sec,
            ctx.download_rate_bytes_per_sec,
        );
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(deadline_step, 2_000);
        assert_eq!(decision.assignments[0].deadline, 0);
        assert_eq!(decision.assignments[1].deadline, 2_000);
    }

    #[test]
    fn initial_window_progress_counts_whole_pieces_the_window_touches() {
        // File at torrent offset 100, length 1000, pieces of 256 bytes, 512
        // byte window from the head -> the window covers [100, 612), which
        // touches pieces 0, 1 and 2, i.e. [100, 768) of the torrent once
        // expanded to whole pieces and clipped to the file's start.
        let have_all = |_: u64| true;
        assert_eq!(
            initial_window_progress(100, 1000, 256, 512, 0, have_all),
            (668, 668)
        );
        let have_first_two = |p: u64| p < 2;
        assert_eq!(
            initial_window_progress(100, 1000, 256, 512, 0, have_first_two),
            (412, 668)
        );
        let have_none = |_: u64| false;
        assert_eq!(
            initial_window_progress(100, 1000, 256, 512, 0, have_none),
            (0, 668)
        );
    }

    /// A 4 MiB window inside a 16 MiB piece described a quantity that could
    /// only read 0% or 100%: on a 7.5 GB torrent at 300 kB/s that is 55
    /// seconds of "0%" while the download is perfectly healthy. The
    /// denominator has to be what actually must arrive.
    #[test]
    fn initial_window_progress_reports_the_piece_that_must_arrive() {
        let piece = 16 * 1024 * 1024u64;
        let file_len = 8 * 1024 * 1024 * 1024u64;
        let have_none = |_: u64| false;
        assert_eq!(
            initial_window_progress(0, file_len, piece, MAX_STARTUP_WINDOW_BYTES, 0, have_none),
            (0, piece),
            "the window is the piece, not the 4 MiB inside it"
        );
        let have_first = |p: u64| p == 0;
        assert_eq!(
            initial_window_progress(0, file_len, piece, MAX_STARTUP_WINDOW_BYTES, 0, have_first),
            (piece, piece),
            "and one piece is the whole of it"
        );
    }

    /// The window follows the reader. Anchored at the file head it
    /// described bytes nobody was fetching after a seek, and read 0%
    /// forever while the seek region streamed fine.
    #[test]
    fn initial_window_progress_follows_the_read_position() {
        let piece = 256u64;
        // Seek to byte 1000 of a file at torrent offset 0: that is piece 3
        // ([768, 1024)) and piece 4, not piece 0.
        let have_head = |p: u64| p == 0;
        assert_eq!(
            initial_window_progress(0, 4096, piece, 100, 1000, have_head),
            (0, 512),
            "a verified head says nothing about where the reader is"
        );
        let have_seek_region = |p: u64| p == 3 || p == 4;
        assert_eq!(
            initial_window_progress(0, 4096, piece, 100, 1000, have_seek_region),
            (512, 512),
            "and the pieces under the reader are what count"
        );
    }

    #[test]
    fn initial_window_progress_clamps_window_to_file_length() {
        // A 300-byte file with a 4 MiB window: the window is the whole file,
        // and the tail piece is clipped to the file's end.
        let have_all = |_: u64| true;
        assert_eq!(
            initial_window_progress(0, 300, 256, MAX_STARTUP_WINDOW_BYTES, 0, have_all),
            (300, 300)
        );
        // Only the second piece (bytes 256..300 of the file) is present.
        let have_second = |p: u64| p == 1;
        assert_eq!(
            initial_window_progress(0, 300, 256, MAX_STARTUP_WINDOW_BYTES, 0, have_second),
            (44, 300)
        );
    }

    #[test]
    fn initial_window_progress_handles_degenerate_inputs() {
        let have_all = |_: u64| true;
        // Zero-length file: nothing to fetch, (0, 0) reads as ready.
        assert_eq!(
            initial_window_progress(0, 0, 256, 1024, 0, have_all),
            (0, 0)
        );
        // Zero window: never ready-by-bytes but never panics.
        assert_eq!(initial_window_progress(0, 100, 256, 0, 0, have_all), (0, 0));
        // Zero piece length is impossible in a valid torrent; report nothing ready.
        assert_eq!(
            initial_window_progress(0, 100, 0, 1024, 0, have_all),
            (0, 100)
        );
        // A reader at (or past) EOF has nothing left to wait for.
        assert_eq!(
            initial_window_progress(0, 100, 256, 1024, 100, have_all),
            (0, 0)
        );
        assert_eq!(
            initial_window_progress(0, 100, 256, 1024, 5_000, have_all),
            (0, 0)
        );
    }

    #[test]
    fn buffer_profiles_round_trip_their_wire_spelling() {
        for profile in BufferProfile::ALL {
            assert_eq!(BufferProfile::parse(profile.as_str()), Some(profile));
            // What `serde` writes and what `parse` reads must be one spelling:
            // the setting and the `buffer=` query parameter share it.
            assert_eq!(
                serde_json::to_value(profile).unwrap(),
                serde_json::Value::String(profile.as_str().to_string())
            );
        }
        assert_eq!(BufferProfile::parse("  LARGE "), Some(BufferProfile::Large));
        for unknown in ["", "huge", "2", "normalish"] {
            assert_eq!(BufferProfile::parse(unknown), None, "value {unknown:?}");
        }
        assert_eq!(BufferProfile::default(), BufferProfile::Normal);
    }

    #[test]
    fn buffer_profiles_scale_the_playback_lookahead_window() {
        let normal =
            librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, BufferProfile::Normal);
        assert_eq!(normal, MAX_SEEK_HOT_WINDOW_BYTES);
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, BufferProfile::Large),
            2 * MAX_SEEK_HOT_WINDOW_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, BufferProfile::Maximum),
            4 * MAX_SEEK_HOT_WINDOW_BYTES
        );
        // The window a playing stream actually uses is the sequential one, and
        // it scales the same way.
        for profile in BufferProfile::ALL {
            assert_eq!(
                librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSequential, profile),
                librqbit_stream_lookahead_bytes(PlaybackIntent::DirectSeek, profile),
                "profile {}",
                profile.as_str()
            );
        }
    }

    #[test]
    fn the_startup_window_is_the_same_under_every_buffer_profile() {
        // Deliberate: the narrow first-frame want-set is what makes playback
        // start quickly. Widening it would trade that latency away.
        for profile in BufferProfile::ALL {
            assert_eq!(
                librqbit_stream_lookahead_bytes(PlaybackIntent::DirectInitial, profile),
                MAX_STARTUP_WINDOW_BYTES,
                "profile {}",
                profile.as_str()
            );
            let mut ctx = base_context(PlaybackIntent::DirectInitial);
            ctx.buffer = profile;
            ctx.first_byte_sent = false;
            let decision = PlaybackPriorityPolicy::decide(ctx);
            assert!(
                decision.hot_window_pieces as u64 * 1024 * 1024 <= MAX_STARTUP_WINDOW_BYTES,
                "profile {} widened the startup window to {} pieces",
                profile.as_str(),
                decision.hot_window_pieces
            );
        }
    }

    #[test]
    fn non_playback_windows_ignore_the_buffer_profile() {
        for intent in [
            PlaybackIntent::DownloadFull,
            PlaybackIntent::DownloadRange,
            PlaybackIntent::ContainerMetadata,
            PlaybackIntent::InternalProbe,
            PlaybackIntent::Background,
        ] {
            let normal = librqbit_stream_lookahead_bytes(intent, BufferProfile::Normal);
            for profile in BufferProfile::ALL {
                assert_eq!(
                    librqbit_stream_lookahead_bytes(intent, profile),
                    normal,
                    "{intent:?} under {}",
                    profile.as_str()
                );
            }
        }
    }

    #[test]
    fn buffer_profiles_scale_the_disk_backed_forward_window() {
        let piece_length = 1024 * 1024;
        let normal = disk_backed_forward_window_pieces_for(
            PlaybackIntent::DirectSequential,
            piece_length,
            BufferProfile::Normal,
        );
        assert_eq!(
            disk_backed_forward_window_pieces_for(
                PlaybackIntent::DirectSequential,
                piece_length,
                BufferProfile::Large,
            ),
            2 * normal
        );
        assert_eq!(
            disk_backed_forward_window_pieces_for(
                PlaybackIntent::DirectSequential,
                piece_length,
                BufferProfile::Maximum,
            ),
            4 * normal
        );
        // Startup stays put here too.
        for profile in BufferProfile::ALL {
            assert_eq!(
                disk_backed_forward_window_pieces_for(
                    PlaybackIntent::DirectInitial,
                    piece_length,
                    profile,
                ),
                disk_backed_forward_window_pieces_for(
                    PlaybackIntent::DirectInitial,
                    piece_length,
                    BufferProfile::Normal,
                ),
            );
        }
    }

    #[test]
    fn buffer_profiles_widen_the_policy_hot_window() {
        let mut normal_ctx = base_context(PlaybackIntent::DirectSequential);
        normal_ctx.cache_size_bytes = u64::MAX;
        let mut previous = 0;
        for profile in BufferProfile::ALL {
            let mut ctx = normal_ctx.clone();
            ctx.buffer = profile;
            let decision = PlaybackPriorityPolicy::decide(ctx);
            assert!(
                decision.hot_window_pieces > previous,
                "{} did not widen the hot window past {previous} pieces",
                profile.as_str()
            );
            previous = decision.hot_window_pieces;
        }
    }
    /// The piece whose sub-piece progress is worth showing is the one the
    /// reader sits on, in torrent-wide indices -- a file that does not start
    /// at the torrent's head is offset by its own position.
    #[test]
    fn reader_piece_index_names_the_piece_under_the_reader() {
        let piece = 1024 * 1024u64;
        // File at the torrent head, reader at the head: the first piece.
        assert_eq!(reader_piece_index(0, 10 * piece, piece, 0), Some(0));
        // Anywhere inside a piece names that piece, not the next one.
        assert_eq!(reader_piece_index(0, 10 * piece, piece, piece - 1), Some(0));
        assert_eq!(reader_piece_index(0, 10 * piece, piece, piece), Some(1));
        // A second file's offset shifts the index: a reader at that file's
        // head waits on the piece the torrent has there.
        assert_eq!(reader_piece_index(3 * piece, piece, piece, 0), Some(3));
        // Past the end is clamped to the file's last byte rather than
        // naming a piece that belongs to whatever follows it.
        assert_eq!(reader_piece_index(0, 2 * piece, piece, 99 * piece), Some(1));
        // Nothing to wait for.
        assert_eq!(reader_piece_index(0, 0, piece, 0), None);
        assert_eq!(reader_piece_index(0, piece, 0, 0), None);
    }

    /// Chunks are 16 KiB each except the last chunk of the torrent's last
    /// piece, so chunks-to-bytes is not a flat multiplication: the product
    /// is clamped to the piece's real length. Getting this wrong would have
    /// the final piece of a file report more bytes than it contains, and a
    /// finished download sit at 103%.
    #[test]
    fn in_flight_piece_bytes_clamp_to_a_short_last_piece() {
        use crate::backend::InFlightPiece;
        let chunk = 16 * 1024u64;

        // A full 16 MiB piece, 400 of its 1024 chunks written: exactly the
        // "6.2 of 16 MB" a client renders.
        let partial = InFlightPiece::from_chunks(0, 400, chunk, 1024 * chunk, false);
        assert_eq!(partial.downloaded_bytes, 400 * chunk);
        assert_eq!(partial.total_bytes, 1024 * chunk);
        assert!(!partial.verified);

        // The torrent's last piece is short (3 chunks would be 49152 bytes;
        // the piece holds 40000). Complete means exactly its own length.
        let short = InFlightPiece::from_chunks(41, 3, chunk, 40_000, true);
        assert_eq!(short.downloaded_bytes, 40_000);
        assert_eq!(short.total_bytes, 40_000);
        assert!(short.verified);

        // Part-way through the same short piece, still under the cap.
        let short_partial = InFlightPiece::from_chunks(41, 1, chunk, 40_000, false);
        assert_eq!(short_partial.downloaded_bytes, chunk);
        assert_eq!(short_partial.total_bytes, 40_000);

        // Nothing written yet is a real, renderable state -- 0 of the
        // piece, not absence.
        let empty = InFlightPiece::from_chunks(7, 0, chunk, 1024 * chunk, false);
        assert_eq!(empty.downloaded_bytes, 0);
        assert_eq!(empty.total_bytes, 1024 * chunk);
    }
}
