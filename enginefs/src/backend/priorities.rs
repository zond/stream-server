use serde::{Deserialize, Serialize};

/// Startup is gated on the actual first readable bytes. Keep speculative work
/// near MPV's 4 MiB network buffer so rare seek/Cues pieces are not starved by
/// a large urgent head window.
pub const MAX_STARTUP_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_SEEK_HOT_WINDOW_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_WARM_WINDOW_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_CONTAINER_METADATA_WINDOW_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_DOWNLOAD_RANGE_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
pub const SMALL_FILE_BYTES: u64 = 64 * 1024 * 1024;

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
    /// Whether a read made with this intent is somebody *playing* the file,
    /// and so whether its position is the file's own -- what the retention
    /// window is drawn round, what the stats split behind and ahead at, and
    /// what a paused film leaves behind. See [`Reading`].
    ///
    /// Only the three direct intents are. The others are all reads of a
    /// region nobody is watching from: the container index at the tail
    /// (`ContainerMetadata`, which is a *player's* request and delivers
    /// bytes like any other), the server's own probe, a background fetch,
    /// and a download -- a download walks the whole file at whatever rate
    /// the swarm gives, and letting it carry the file's head would move the
    /// window off the viewer who is watching the same file while it runs.
    /// Each of them still gets a window of its own round where it is
    /// reading while it is open ([`Reading::Probe`]); none of them leaves
    /// one behind.
    ///
    /// [`Reading`]: crate::retention::owner::Reading
    pub fn reading(self) -> crate::retention::owner::Reading {
        use crate::retention::owner::Reading;
        match self {
            Self::DirectInitial | Self::DirectSeek | Self::DirectSequential => Reading::Playback,
            Self::DownloadFull
            | Self::DownloadRange
            | Self::ContainerMetadata
            | Self::InternalProbe
            | Self::Background => Reading::Probe,
        }
    }
}

/// The most a stream reads ahead of itself, in bytes, by playback intent:
/// the cap on librqbit's per-stream lookahead, in place of its fixed 32 MiB
/// default.
///
/// **One of two inputs.** `Engine::try_get_file_with_intent` opens the
/// reader with the smaller of this and the retention window's reach ahead of
/// the reader (`Engine::fetch_bound`), so a stream never asks the swarm for a
/// piece the next retention pass would reclaim; this is the whole of the
/// lookahead only for a file nothing bounds. `stream_with_options` rejects a
/// zero window, so the result is at least 1 (all constants are already > 0).
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

#[cfg(test)]
mod tests {
    use super::*;

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
