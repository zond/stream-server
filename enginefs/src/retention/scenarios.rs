//! **Scenarios run through the harness, against the policy that is in
//! `owner` today.**
//!
//! Separate from [`crate::retention::scenario`] on purpose: that module is
//! the harness, this one is what is asked of it, and only one of the two is
//! meant to survive unchanged when the policy is swapped
//! (`docs/read-pattern-retention.md`). A scenario here is a statement about
//! a *cache*, not about an implementation, so the same file is what the
//! replacement is measured against.

use std::time::Duration;

use super::scenario::{
    CONTAINER_METADATA_LOOKAHEAD, FIELD_FILM, Film, Log, NORMAL_WINDOW_SECONDS, PLAYBACK_LOOKAHEAD,
    Scenario, Step,
};

/// A phone with four gibibytes of cache to spend on this film.
///
/// The field report does not carry the budget, and it has to be *some*
/// number. What the scenario turns on is not the value: at any budget that
/// does not cover a 23 GB film, the window is capped by the buffer
/// profile's ninety seconds of stream long before the disk binds, so the
/// window is sixty-six pieces here and would be sixty-six pieces at eight
/// gibibytes too.
const BUDGET: u64 = 4 * 1024 * 1024 * 1024;

/// Where the viewer is: sixteen minutes into the film, which is where the
/// field's log put them.
const VIEWER_AT: Duration = Duration::from_secs(16 * 60);

/// The first byte of piece 5560 -- the offset every one of the field's
/// failing reads stopped at, and the boundary the second track could not
/// get past.
const PIECE_5560: u64 = 23_320_330_240;

/// Where the second track's responses resumed. The field's three logged
/// retries served 40,898, 40,851 and 40,804 bytes, "each one the distance
/// from where it resumed to piece 5560", so the response opens inside piece
/// 5559 and its next piece is 5560.
const SECOND_TRACK_AT: u64 = PIECE_5560 - 40_898;

/// What the swarm manages in one beat, in pieces: **everything the backend
/// is still asking for**.
///
/// Deliberately larger than the film. The field's seventeen seeders put
/// 1.6 GB on the disk in about a minute and the question the scenario asks
/// is why one 4 MiB piece did not arrive in twenty seconds, so the swarm is
/// modelled as never the limit at all. That leaves exactly one reason a
/// piece can be missing at the end of a beat, which is what makes the
/// answer evidence rather than a race: nothing was asking for it.
const EVERYTHING_WANTED: usize = 10_000;

/// How long the second track is watched for. Long enough to contain the
/// twenty seconds the field reported a read blocked for.
const SESSION: u64 = 30_000;

/// The one cycle in which the swarm does not put the reclaimed pieces back
/// before the next pass looks -- a peer choking, a hash check, a phone's
/// radio asleep for a second. It is the whole of what separates the loop
/// from the wall.
const THE_SWARM_FALLS_BEHIND: u64 = 6_000;

fn ms(at: u64) -> Duration {
    Duration::from_millis(at)
}

/// The viewer's byte offset at [`VIEWER_AT`], converted at the film's
/// average rate.
///
/// Nothing tells the server where a viewer is any more -- where an entity
/// is being consumed is where its reads are -- so this is the scenario's
/// own arithmetic, turning the time the field report states into the offset
/// the viewer's response is opened at.
fn viewer_offset(film: &Film) -> u64 {
    (film.bytes as f64 * (VIEWER_AT.as_secs_f64() / film.duration.as_secs_f64())) as u64
}

fn reads(reader: &'static str) -> Step {
    Step::Reads {
        reader,
        bytes: 256 * 1024,
    }
}

fn opens_the_second_track() -> Step {
    Step::Opens {
        reader: "second-track",
        offset: SECOND_TRACK_AT,
        lookahead: CONTAINER_METADATA_LOOKAHEAD,
        window_seconds: Some(NORMAL_WINDOW_SECONDS),
    }
}

/// One of the second track's bursts, as the field logged them: a response
/// opens where the last one gave up, is polled, is polled again, and is
/// closed -- 46 of them in 70 seconds, each serving 24-41 kB or nothing.
/// Then the reconciler's pass, then the swarm.
///
/// `the_swarm_keeps_up` is the one thing a scenario varies here, and it is
/// not a tuning knob: it is whether the four pieces this pass has just
/// taken off the disk are back before the next pass looks. While they are,
/// the session is a loop -- fetched, deleted, fetched again, every two
/// seconds, for ever. The moment one cycle is missed the loop becomes a
/// wall, and that is what the session below is for.
fn burst(script: &mut Vec<(Duration, Step)>, from: u64, the_swarm_keeps_up: bool) {
    script.push((ms(from), opens_the_second_track()));
    script.push((ms(from + 200), reads("second-track")));
    script.push((ms(from + 400), reads("second-track")));
    // While the response is still open, so its lookahead is one of the
    // things the swarm is pulling for. Held out of the burst entirely, the
    // scenario could only ever show the swarm working on a selection no
    // stream was reading over -- the one case where the two halves cannot
    // interact, and their interaction is the whole subject. Under the same
    // gate as the beat after the pass: a swarm that has fallen behind has
    // fallen behind here too.
    if the_swarm_keeps_up {
        script.push((
            ms(from + 450),
            Step::Swarm {
                pieces: EVERYTHING_WANTED,
            },
        ));
    }
    script.push((ms(from + 500), reads("viewer")));
    script.push((
        ms(from + 600),
        Step::Closes {
            reader: "second-track",
        },
    ));
    script.push((ms(from + 800), Step::Pass));
    if the_swarm_keeps_up {
        script.push((
            ms(from + 1_100),
            Step::Swarm {
                pieces: EVERYTHING_WANTED,
            },
        ));
    }
}

/// **The field's session.**
///
/// A viewer sixteen minutes into a 23 GB film, and a second track muxed
/// near the end of `mdat` reading it in short bursts. The disk starts where
/// the field's trace line found it: the viewer's own window, and at the
/// tail the four pieces the second track's *previous* response pulled in
/// over its 16 MiB lookahead -- 5560 to 5563. Piece 5559, the one the next
/// response opens inside, is **not** there; a pass took it while no
/// response was open over it, which is the state the scenario then
/// reproduces on its own from piece 5560 onwards.
///
/// The first response is opened and not yet polled when the pass runs,
/// which is the instant the field's line describes: a response has a head
/// from the moment it opens (`ReaderState::opened_at`) and has promised
/// nothing, because a promise is made by `poll_read`'s `Poll::Pending` arm
/// and it has not been polled.
fn field_session() -> Log {
    let film = FIELD_FILM;
    let mut script = vec![
        (
            ms(0),
            Step::Opens {
                reader: "viewer",
                offset: viewer_offset(&film),
                lookahead: PLAYBACK_LOOKAHEAD,
                window_seconds: Some(NORMAL_WINDOW_SECONDS),
            },
        ),
        (ms(0), reads("viewer")),
        (ms(500), opens_the_second_track()),
        // The pass the field's line was written by: a response open at the
        // tail, nothing polled yet, and its next four pieces on the disk.
        (ms(1_000), Step::Pass),
        (ms(1_100), reads("second-track")),
        (
            ms(1_200),
            Step::Closes {
                reader: "second-track",
            },
        ),
        (
            ms(1_500),
            Step::Swarm {
                pieces: EVERYTHING_WANTED,
            },
        ),
    ];
    for from in (2_000..SESSION).step_by(2_000) {
        // One cycle in which the swarm does not manage to put the four
        // pieces back before the next pass looks. Everything else about
        // every burst is identical.
        burst(&mut script, from, from != THE_SWARM_FALLS_BEHIND);
    }
    Scenario::new(film, BUDGET)
        .on_disk(620..710)
        .on_disk(5560..5564)
        .run(&script)
}

/// **The field's failure, gone.**
///
/// The line the whole replacement was written for:
///
/// ```text
/// enginefs::retention::trace: a retention pass planned to reclaim inside an open
/// stream's lookahead; pieces=[5560, 5561, 5562] reader_start=5559 reader_end=5563
/// lookahead_bytes=16777216
/// ```
///
/// It was produced by a read of the last sixteen megabytes of a file being
/// classified as a container-index probe -- which is what a second audio or
/// subtitle track muxed near the end of `mdat` looks like to a rule made of
/// range geometry. Nothing classifies anything now: a consumer is the
/// unbroken run of disk it caused, and a track reading at twenty kilobytes
/// a second is a consumer with a small window, not a probe with none.
///
/// Thirty seconds of the same session, and the line never appears.
#[test]
fn no_pass_plans_to_reclaim_inside_an_open_streams_lookahead() {
    // The film is the field's, and the piece arithmetic is the field's:
    // 5,567 pieces, the last one short, with piece 5560 beginning on the
    // byte every failing read stopped at.
    assert_eq!(FIELD_FILM.pieces(), 5_567);
    assert_eq!(FIELD_FILM.at_piece(5_560), PIECE_5560);
    assert_ne!(
        FIELD_FILM.bytes % FIELD_FILM.piece,
        0,
        "the field's film does not end on a piece boundary and neither should this one"
    );

    let log = field_session();
    let planned = log.lines("inside an open stream's lookahead");
    assert!(
        planned.is_empty(),
        "a pass planned to take what a live read was reading ahead over: \
         pieces={:?}",
        planned.first().and_then(|line| line.field("pieces"))
    );
    for pass in &log.passes {
        assert!(
            pass.inside_a_lookahead().is_empty(),
            "the pass at {:?} planned inside a lookahead: {:?}",
            pass.at,
            pass.inside_a_lookahead()
        );
    }
}

/// **The second track's pieces stay, and they stay once it has closed.**
///
/// This is the half the old keep set could not do. A window was a promise
/// kept by asking the door at the instant of every unlink, which is right
/// and which protects nothing between two bursts: a track that serves
/// 24-41 kB per response and opens forty-six of them in seventy seconds is
/// not there for most of any given second, and every pass in the gap took
/// its working set.
///
/// What keeps them now is not a reader being there. It is that the reads
/// happened: they caused a run of disk, that run is a consumer, and a
/// consumer's window is what no unlink may touch -- for as long as the
/// consumer lasts, which is a measured idleness and not the lifetime of an
/// HTTP response.
#[test]
fn the_second_tracks_pieces_stay_after_its_response_has_closed() {
    let log = field_session();
    let after_the_burst = &log.passes[1];
    for piece in [5559, 5560, 5561, 5562, 5563] {
        assert!(
            after_the_burst
                .kept
                .iter()
                .any(|window| window.contains(&piece))
                || !after_the_burst.unlinked.contains(&piece),
            "piece {piece} went in the pass at {:?} with the track's response \
             closed; what went, pass by pass: {:?}",
            after_the_burst.at,
            log.unlinked()
        );
    }
}

/// **And nothing is paid for twice.**
///
/// The field's loop was a pass taking a piece, the swarm putting it back,
/// and the next pass taking it again -- a hundred and seventeen megabytes
/// of fetch every two seconds for a film playing at 2.8 MB/s. It is the
/// shape of "1.6 GB fetched to play about a hundred megabytes".
///
/// The loop cannot form here: what a pass gives up is what no consumer is
/// asking for, so the want set does not order it back.
#[test]
fn no_piece_is_taken_and_paid_for_again() {
    let log = field_session();
    let mut taken: Vec<u32> = Vec::new();
    for pass in &log.passes {
        for piece in &pass.unlinked {
            assert!(
                !taken.contains(piece),
                "piece {piece} was taken again by the pass at {:?}: the swarm \
                 put back what the last pass gave up, which is the loop this \
                 replaces",
                pass.at
            );
            taken.push(*piece);
        }
    }
}

/// **Every pass orders what the second track is reading.**
///
/// The old want set never did, in thirty seconds of passes: a probe's
/// window was kept and not wanted, deliberately, because ordering the whole
/// forward reach of a window round a 16 MiB read of the tail was a hundred
/// and thirty-eight megabytes the next pass reclaimed. The answer to that
/// was to order nothing at all, which left the track's pieces to its own
/// lookahead and to whatever the swarm felt like.
///
/// A consumer is fetched for now, and the size of what it is fetched is its
/// own measured rate rather than a profile's: ninety seconds of twenty
/// kilobytes a second is under two megabytes, which is not a hundred and
/// thirty-eight.
#[test]
fn the_second_tracks_pieces_are_ordered_while_it_is_reading() {
    let log = field_session();
    let ordering_the_tail = log
        .passes
        .iter()
        .filter(|pass| pass.wanted.iter().any(|window| window.start >= 5_000))
        .count();
    assert!(
        ordering_the_tail > 0,
        "no pass ordered anything of the second track: {:?}",
        log.passes
            .iter()
            .map(|pass| &pass.wanted)
            .collect::<Vec<_>>()
    );
}

/// **And the track does not block.**
///
/// The field's twenty-second stall was a read waiting for a piece nothing
/// had ordered: the pass had spent thirty seconds declining to want it and
/// had taken it off the disk four times, so the only thing fetching it was
/// the response's own lookahead, over a swarm giving 193 kB/s.
///
/// The waiting is measured per reader rather than per response, which is
/// the only way it can be measured: the field's track opened forty-six
/// responses in seventy seconds and no single one of them waited twenty
/// seconds for anything.
#[test]
fn the_second_track_never_blocks_on_a_piece_a_pass_took() {
    let log = field_session();
    let blocked: Vec<u32> = log
        .reads
        .iter()
        .filter(|read| read.reader == "second-track" && read.blocked())
        .map(|read| read.piece)
        .collect();
    for piece in &blocked {
        assert!(
            log.passes
                .iter()
                .all(|pass| !pass.unselected.contains(piece)),
            "the track parked on piece {piece}, which a pass had stopped \
             wanting: nothing was fetching it but the response's own \
             lookahead"
        );
        assert!(
            !log.passes.iter().any(|pass| pass.unlinked.contains(piece)),
            "the track parked on piece {piece}, which a pass had taken: the \
             burst is paying for the last pass again"
        );
    }
}

/// **The control: a response that is still open when a pass runs *does*
/// get the piece it is parked on ordered.**
///
/// Same film, same disk, same reader -- one difference. Here the response
/// is polled before the pass instead of after it, so it is parked, and a
/// parked read promises the piece under its cursor (`files.rs::poll_read`'s
/// `Poll::Pending` arm, added in `87e8c9b`). The pass sees that promise and
/// makes it the whole want-set, the swarm delivers, and the next poll is
/// served.
///
/// It is here because it is what makes the verdict on the field failure
/// evidence rather than argument. The keep set and the want set are both
/// visibly wrong in the session above -- pieces go from under a reader that
/// is about to ask for them, *and* no pass ever orders them -- and only one
/// of those can explain twenty seconds of silence from seventeen seeders. A
/// parked read is woken by the piece arriving, so a read that waits twenty
/// seconds is a read whose piece never arrived, and a piece that never
/// arrives from a healthy swarm is a piece nobody asked for. This test is
/// the other side of that: when something *does* ask, the wait is one pass
/// long.
///
/// Which is also why the failure survives the promise path. The promise
/// exists only while the response does, and the field's track opened
/// forty-six responses in seventy seconds, each serving 24-41 kB and giving
/// up. A promise a pass never sees orders nothing.
#[test]
fn a_response_still_open_at_a_pass_has_the_piece_it_is_parked_on_ordered() {
    let film = FIELD_FILM;
    let log = Scenario::new(film, BUDGET)
        .on_disk(620..710)
        .on_disk(5560..5564)
        .run(&[
            (
                ms(0),
                Step::Opens {
                    reader: "viewer",
                    offset: viewer_offset(&film),
                    lookahead: PLAYBACK_LOOKAHEAD,
                    window_seconds: Some(NORMAL_WINDOW_SECONDS),
                },
            ),
            (ms(0), reads("viewer")),
            (ms(500), opens_the_second_track()),
            // Polled *before* the pass, so it is parked and has promised.
            (ms(600), reads("second-track")),
            (ms(1_000), Step::Pass),
            (
                ms(1_500),
                // One piece, so that only what the read is parked on
                // arrives: what the second poll is then served says where
                // the next hole is.
                Step::Swarm { pieces: 1 },
            ),
            (ms(2_000), reads("second-track")),
            (
                ms(2_500),
                Step::Reads {
                    reader: "second-track",
                    bytes: 64 * 1024 * 1024,
                },
            ),
        ]);
    let pass = &log.passes[0];
    assert!(
        pass.wanted.iter().any(|window| window.contains(&5559)),
        "the pass at {:?} did not order the piece a live read was parked on: {:?}",
        pass.at,
        pass.wanted
    );
    let served = log
        .reads
        .iter()
        .find(|read| read.reader == "second-track" && !read.blocked())
        .expect("the second track was never served");
    assert_eq!(served.at, ms(2_000));
    assert!(
        log.longest_block("second-track") < Duration::from_secs(2),
        "the second track waited {:?} for a piece something had ordered",
        log.longest_block("second-track")
    );
    // And the field's signature, from the other side: a read is served to
    // the first piece the disk does not have and stops there, however much
    // it asked for. This one asks for 64 MiB and gets 16,555,970 bytes,
    // ending on the first byte of piece 5564 -- the same shape as the
    // field's 40,898 bytes ending on the first byte of piece 5560.
    let short = log
        .reads
        .iter()
        .find(|read| read.at == ms(2_500))
        .expect("the second poll");
    assert_eq!(
        short.offset + short.delivered,
        FIELD_FILM.at_piece(5564),
        "a read ran past the first piece the disk does not hold"
    );
    assert!(short.delivered < short.asked, "the read was not short");
}
