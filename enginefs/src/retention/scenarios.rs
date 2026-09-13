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

use super::owner::Reading;
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

/// The viewer's byte offset at [`VIEWER_AT`], as the player's own report
/// converts it -- at the film's average rate, which is what
/// `Retention::note_playhead_at` does with it.
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
        reading: Reading::Probe,
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
                reading: Reading::Playback,
                lookahead: PLAYBACK_LOOKAHEAD,
                window_seconds: Some(NORMAL_WINDOW_SECONDS),
            },
        ),
        (ms(0), reads("viewer")),
        (ms(0), Step::Says { film: VIEWER_AT }),
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

/// **The field's trace line, reproduced number for number.**
///
/// Diagnostics, 4 MiB pieces, a 23,346,250,742-byte film:
///
/// ```text
/// enginefs::retention::trace: a retention pass planned to reclaim inside an open
/// stream's lookahead; pieces=[5560, 5561, 5562] reader_start=5559 reader_end=5563
/// lookahead_bytes=16777216
/// ```
///
/// None of those numbers is written down by the scenario. `lookahead_bytes`
/// is what librqbit grants a read labelled `ContainerMetadata`
/// (`MAX_CONTAINER_METADATA_WINDOW_BYTES`), which is what
/// `playback_intent_for_request` calls a read of the last sixteen megabytes
/// of a file -- and a second audio or subtitle track muxed near the end of
/// `mdat` is exactly that shape, which is the misreading the whole
/// replacement exists to end. `reader_start` is the piece the response
/// opened in; `reader_end` is four pieces on, because four 4 MiB pieces is
/// what 16 MiB of lookahead covers; and the three pieces between them are
/// the ones that are on the disk, outside the viewer's window, and so in
/// the plan. Piece 5559 is absent from the plan for the same reason the
/// response is about to block on it: it is not on the disk.
///
/// **And this pass keeps them.** The response is open, so
/// [`Door::windows_now`] answers with a window round its head at the
/// instant of every unlink and the runs are cut; nothing at the tail goes.
/// That is the keep set working exactly as designed, and it is the
/// exception rather than the rule -- see the next test.
///
/// [`Door::windows_now`]: crate::retention::owner::Door::windows_now
#[test]
fn a_pass_plans_to_reclaim_inside_the_second_tracks_lookahead_and_keeps_it_while_it_is_open() {
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
    let first = planned.first().expect("the field's trace line");
    assert_eq!(first.field("pieces"), Some("[5560, 5561, 5562]"));
    assert_eq!(first.field("reader_start"), Some("5559"));
    assert_eq!(first.field("reader_end"), Some("5563"));
    assert_eq!(first.field("lookahead_bytes"), Some("16777216"));

    let over_the_burst = &log.passes[0];
    assert!(
        !over_the_burst.inside_a_lookahead().is_empty(),
        "the first pass is not the one the trace line is about"
    );
    assert!(
        over_the_burst
            .kept
            .iter()
            .any(|window| window.contains(&5560)),
        "the pass at {:?} kept no window over the second track: {:?}",
        over_the_burst.at,
        over_the_burst.kept
    );
    assert!(
        over_the_burst.unlinked.iter().all(|piece| *piece < 5_000),
        "the pass that planned to reclaim inside the lookahead also did it: {:?}",
        over_the_burst.unlinked
    );
    // Nor does it stop the swarm wanting them. The fork's `drop_pieces`
    // refuses a piece inside a live stream's lookahead, so while the
    // response is open the backend is still being asked for the four
    // pieces it is reading ahead over -- which is the other half of what
    // makes the *close* the moment everything changes.
    assert!(
        !over_the_burst.unselected.contains(&5559),
        "the pass stopped wanting a piece a live response was reading ahead over"
    );
}

/// **The keep set is asked at the door, so it protects nothing between two
/// bursts**: the pieces the pass above kept are taken by the very next
/// pass, whose only difference is that the second track's response had
/// closed.
///
/// A window is a promise not to delete, and the owner keeps it by asking
/// the door at the instant of every unlink rather than trusting a
/// measurement taken before two awaited backend calls. That is right, and
/// it is why nothing is ever deleted from under a live read. What it cannot
/// do is keep a promise to a reader that is not there -- and a track that
/// serves 24-41 kB per response and opens forty-six of them in seventy
/// seconds is not there for most of any given second.
///
/// Piece 5560 is the exception, and not for a reason anybody would want: it
/// is the one piece this track managed to deliver a byte out of, so
/// `Reader::note` filed it in the entity's `structural` set -- "the pieces
/// a container cannot be played without". It is media data in the middle of
/// `mdat`, it is now kept for the life of the entity, and it has taken one
/// of the eight slots the film's actual `moov` needs.
#[test]
fn the_second_tracks_pieces_go_in_the_first_pass_with_no_response_open_over_them() {
    let log = field_session();
    let after_the_burst = &log.passes[1];
    for piece in [5559, 5561, 5562, 5563] {
        assert!(
            after_the_burst.unlinked.contains(&piece),
            "piece {piece} survived the pass at {:?} with nothing open over it; \
             what went, pass by pass: {:?}",
            after_the_burst.at,
            log.unlinked()
        );
    }
    assert!(
        !after_the_burst.unlinked.contains(&5560),
        "piece 5560 went: the structural set did not keep it"
    );
    assert!(
        after_the_burst.kept.contains(&(5560..5561)),
        "5560 is kept by a window rather than as a structural piece: {:?}",
        after_the_burst.kept
    );
}

/// **And they are paid for again, every two seconds, for as long as the
/// swarm keeps up.**
///
/// The three passes after the first take the same four tail pieces off the
/// disk, because the swarm has put all four back in between -- and it puts
/// them back because [`Backing::want`] works out what to stop wanting from
/// the listing it took *before* the reclaim, so a piece the same pass is
/// about to unlink is still on the disk when the drop set is computed and
/// is therefore left selected. The pass deletes what it has just told the
/// backend to keep wanting.
///
/// It is not only the tail. The same passes take twenty-four pieces of the
/// *viewer's* own file -- everything between the disk's edge and the
/// window's -- and those come back too: a hundred and seventeen megabytes
/// of fetch every two seconds, for a film playing at 2.8 MB/s and a track
/// reading 20 kB/s. That is the shape of "1.6 GB fetched to play about a
/// hundred megabytes", measurable here rather than inferred from piece
/// counts afterwards.
///
/// [`Backing::want`]: crate::retention::owner::Backing::want
#[test]
fn the_same_pieces_are_reclaimed_and_refetched_on_every_pass() {
    let log = field_session();
    for pass in &log.passes[1..4] {
        assert!(
            pass.unlinked.contains(&5559),
            "the pass at {:?} did not take piece 5559 again: {:?}",
            pass.at,
            pass.unlinked
        );
        assert!(
            pass.unlinked.len() >= 24,
            "the pass at {:?} reclaimed only {} pieces",
            pass.at,
            pass.unlinked.len()
        );
    }
}

/// **The want set never orders a piece of the second track, in thirty
/// seconds of passes -- and the first pass that finds one missing takes it
/// out of the swarm's reach for good.**
///
/// This is the half the two surveys disagreed about, and it is the half
/// that turns the loop into a wall. A `Reading::Probe`'s window is kept and
/// never wanted, which is right for a sixteen-megabyte read of a container
/// index and is what a second track gets called, so no pass ever asks the
/// swarm for anything at the tail. While the loop above is running that
/// costs only bandwidth: the pieces stay selected by accident, because they
/// were on the disk when the drop set was computed. The moment one cycle is
/// missed they are *not* on the disk when the next pass looks, no response
/// is open over them, and they are dropped with
/// `AfterRelease::LeaveDropped` -- "not had, and now not wanted". Nothing
/// puts them back, because nothing ever wanted them on purpose.
#[test]
fn no_pass_ever_orders_a_piece_of_the_second_track_and_one_unselects_them() {
    let log = field_session();
    for pass in &log.passes {
        assert!(
            pass.wanted.iter().all(|window| window.end <= 5_000),
            "the pass at {:?} ordered the tail: {:?}",
            pass.at,
            pass.wanted
        );
    }
    let trap = log
        .passes
        .iter()
        .find(|pass| pass.unselected.contains(&5559))
        .expect("no pass took the piece the second track is waiting for out of the want-set");
    assert_eq!(
        trap.at,
        ms(THE_SWARM_FALLS_BEHIND + 2_800),
        "the piece left the want-set somewhere other than the first pass that found it missing"
    );
}

/// **And so the read starves: twenty seconds blocked on piece 5559, with a
/// swarm that would deliver every piece it was asked for in the beat it was
/// asked.**
///
/// The field's line is "reads blocked up to 20 s on those pieces while 17
/// seeders were connected". Here the swarm is not merely healthy, it is
/// unlimited -- [`EVERYTHING_WANTED`] is larger than the film -- so there is
/// exactly one reason a piece can still be missing twenty seconds later,
/// and it is that nothing is asking for it.
///
/// The waiting is measured per reader rather than per response, which is
/// the only way it can be measured: the field's track opened forty-six
/// responses in seventy seconds and no single one of them waited twenty
/// seconds for anything.
#[test]
fn the_second_track_is_blocked_for_twenty_seconds_with_the_swarm_unlimited() {
    let log = field_session();
    assert!(
        log.longest_block("second-track") >= Duration::from_secs(20),
        "the second track was not starved: longest block {:?}",
        log.longest_block("second-track")
    );
    for read in log.reads.iter().filter(|read| {
        read.reader == "second-track" && read.at > ms(THE_SWARM_FALLS_BEHIND + 2_800)
    }) {
        assert!(
            read.blocked(),
            "a read of the second track at {:?} was served {} bytes at piece {} \
             after the piece had left the want-set",
            read.at,
            read.delivered,
            read.piece,
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
                    reading: Reading::Playback,
                    lookahead: PLAYBACK_LOOKAHEAD,
                    window_seconds: Some(NORMAL_WINDOW_SECONDS),
                },
            ),
            (ms(0), reads("viewer")),
            (ms(0), Step::Says { film: VIEWER_AT }),
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
