use serde::{Deserialize, Serialize};

/// How much of the film the committed set may come to, stated as seconds of
/// it, whatever the buffer profile.
///
/// The committed set is what we offer a peer, drawn at random across the
/// whole file, so this is a size and not a region of the film. Sized from
/// the budget alone it was `cacheSize / 2` -- 151 MB on the field device,
/// about fifty-one seconds of a 23 Mbps film -- so the forward buffer had
/// the other half of a disk far too small to give half of away. Time is the
/// unit that says what it is for: what we offer is stated against the film,
/// as the buffer is.
pub const COMMITTED_SECONDS: u64 = 90;

/// **How much of the film the viewer wants in hand**, in seconds of it.
///
/// A spotty link -- or a receiver with a shallower buffer than mpv's --
/// wants more of the file fetched before it is needed, at the cost of
/// downloading further ahead than will necessarily be watched. This is that
/// choice, and it is stated in the unit the viewer chose in: "how much of
/// this film do I want in hand", not "what fraction of my disk".
///
/// **It is one number and it governs both halves**: how far ahead the
/// stream reads and how long the retention window may be
/// ([`Self::window_seconds`]), because the two are the same question asked
/// of the swarm and of the disk. The bytes it comes to are the film's own
/// bitrate times these seconds. Sized from the disk instead, the forward
/// reach was a fraction of `cacheSize`, so a viewer who gave the app a
/// bigger cache silently bought a bigger mobile-data bill.
///
/// It does not scale the fallback a stream reads ahead at before a duration
/// has been stated ([`STREAMING_LOOKAHEAD_BYTES`]): those first seconds are
/// where a narrow want-set makes the first frame arrive, and multiplying a
/// number that exists because nothing is known yet would be scaling a
/// guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BufferProfile {
    /// About a minute and a half of the stream buffered ahead, and today's
    /// read-ahead cap. The default.
    #[default]
    Normal,
    /// About four minutes ahead, and twice the playback read-ahead.
    Large,
    /// **The whole file, while you are watching it**, where the cache
    /// budget covers it -- there is no time cap at all -- and the widest
    /// window the budget allows where it does not. Four times the playback
    /// read-ahead.
    ///
    /// **Maximum is for this viewing; a pin is for later.** The retention
    /// owner takes these bytes back as soon as the stream is not the live
    /// one, or the volume needs the room; a pin survives a restart and the
    /// launch sweep spares it. That is the whole difference, and it is the
    /// reason this profile is worth having rather than being a bigger
    /// number.
    ///
    /// On a metered connection it means the whole film over mobile data,
    /// and downloading is not what the sharing switch governs. Whoever
    /// picks it is choosing that, so the setting has to say so.
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

    /// How many seconds of the stream the retention window's forward reach
    /// may buy, or `None` for [`Self::Maximum`], which asks for the file.
    ///
    /// The unit is the one the viewer chose in: "how much of this film do I
    /// want in hand", not "what fraction of my disk". The bytes it comes to
    /// are the film's own bitrate times this, and it is only ever a *cap*
    /// -- the budget and the lookahead floor still bound it from the other
    /// side.
    pub const fn window_seconds(self) -> Option<u64> {
        match self {
            Self::Normal => Some(90),
            Self::Large => Some(4 * 60),
            Self::Maximum => None,
        }
    }
}

/// What a read is for, which after the read-pattern rewrite is one bit:
/// whether somebody is watching it or it is being fetched for later.
///
/// **This decides one number and nothing else** -- how far ahead of the
/// reader the swarm is asked to fetch *when the film's own arithmetic
/// cannot say*. A playing stream reads ahead at the film's bitrate times
/// the seconds the viewer asked to have buffered
/// (`Engine::try_get_file_with_intent`), and what is kept on the disk comes
/// from the read-pattern detector; neither asks this.
///
/// **It used to be eight variants classifying the geometry of the range
/// header** -- a first read, a seek, a sequential read, a full download, a
/// ranged download, a crawl over the container index at the tail, a probe,
/// a background fetch -- each with a window of its own, and an arm telling
/// the retention which reader was the viewer. Every one of those questions
/// is now answered by watching what the reads do
/// (`crate::retention::streams`): in the field log of 2026-09-14 mpv's
/// index crawler measured 91 B/s beside the viewer's 1.2 MB/s, two reads a
/// range header cannot tell apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Fetching {
    /// Somebody is watching. The fallback is deliberately small: a stream
    /// that has not been told a duration yet is a stream in its first
    /// seconds, where a narrow want-set is what makes the first frame
    /// arrive -- and the next request, by which time a duration has usually
    /// been stated, is sized from the film.
    Streaming,
    /// A file being fetched for later, which **never has a duration**: a
    /// duration is stated by a player, and nothing has played this. So for
    /// a download the fallback is not a fallback but the number in force
    /// for the whole fetch, and it is sized to keep the swarm busy rather
    /// than to sit a fixed distance ahead of a playhead that does not
    /// exist.
    Download,
}

/// What a playing stream reads ahead of itself before a duration has been
/// stated; see [`Fetching::Streaming`].
pub const STREAMING_LOOKAHEAD_BYTES: u64 = 4 * 1024 * 1024;

/// What a download reads ahead of itself, for its whole life; see
/// [`Fetching::Download`].
pub const DOWNLOAD_LOOKAHEAD_BYTES: u64 = 256 * 1024 * 1024;

/// The most a stream reads ahead of itself, in bytes, **when the film's own
/// arithmetic cannot say**: the cap on librqbit's per-stream lookahead, in
/// place of its fixed 32 MiB default.
///
/// **One of two inputs even then.** The reader is opened with the smaller of
/// this and the retention window's reach ahead of it
/// (`Engine::fetch_bound`), so a stream never asks the swarm for a piece the
/// next retention pass would reclaim. `stream_with_options` rejects a zero
/// window, so the result is at least 1 (both constants are already > 0).
///
/// **The buffer profile does not scale it.** The profile says how many
/// seconds of the stream to hold, which is applied where the seconds are
/// known; multiplying a fallback that exists because nothing is known would
/// be scaling a guess.
pub const fn librqbit_stream_lookahead_bytes(fetching: Fetching) -> u64 {
    match fetching {
        Fetching::Streaming => STREAMING_LOOKAHEAD_BYTES,
        Fetching::Download => DOWNLOAD_LOOKAHEAD_BYTES,
    }
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

    /// **Two numbers, and which one a read gets.** A stream reading ahead
    /// before a duration has been stated gets the small one, because those
    /// are the seconds a narrow want-set buys a first frame in; a download
    /// gets the large one for its whole life, because nothing will ever
    /// state it a duration to be sized from.
    #[test]
    fn the_fallback_is_one_of_two_numbers() {
        assert_eq!(
            librqbit_stream_lookahead_bytes(Fetching::Streaming),
            STREAMING_LOOKAHEAD_BYTES
        );
        assert_eq!(
            librqbit_stream_lookahead_bytes(Fetching::Download),
            DOWNLOAD_LOOKAHEAD_BYTES
        );
        // `stream_with_options` rejects a zero window.
        for fetching in [Fetching::Streaming, Fetching::Download] {
            assert!(librqbit_stream_lookahead_bytes(fetching) > 0);
        }
        const { assert!(DOWNLOAD_LOOKAHEAD_BYTES > STREAMING_LOOKAHEAD_BYTES) };
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
            initial_window_progress(0, file_len, piece, STREAMING_LOOKAHEAD_BYTES, 0, have_none),
            (0, piece),
            "the window is the piece, not the 4 MiB inside it"
        );
        let have_first = |p: u64| p == 0;
        assert_eq!(
            initial_window_progress(0, file_len, piece, STREAMING_LOOKAHEAD_BYTES, 0, have_first),
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
            initial_window_progress(0, 300, 256, STREAMING_LOOKAHEAD_BYTES, 0, have_all),
            (300, 300)
        );
        // Only the second piece (bytes 256..300 of the file) is present.
        let have_second = |p: u64| p == 1;
        assert_eq!(
            initial_window_progress(0, 300, 256, STREAMING_LOOKAHEAD_BYTES, 0, have_second),
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

    /// **The buffer profile scales the seconds, not the fallback.** What a
    /// stream reads ahead is those seconds times the film's bitrate; the
    /// fallback exists precisely because neither is known yet, and
    /// multiplying it would be scaling a guess.
    #[test]
    fn the_fallback_is_the_same_under_every_buffer_profile() {
        assert_eq!(BufferProfile::Normal.window_seconds(), Some(90));
        assert_eq!(BufferProfile::Large.window_seconds(), Some(4 * 60));
        assert_eq!(BufferProfile::Maximum.window_seconds(), None);
        for fetching in [Fetching::Streaming, Fetching::Download] {
            let bytes = librqbit_stream_lookahead_bytes(fetching);
            for profile in BufferProfile::ALL {
                assert_eq!(
                    librqbit_stream_lookahead_bytes(fetching),
                    bytes,
                    "{fetching:?} under {}",
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
