# A rendition, read the way a Cast receiver reads it

A Cast receiver plays a rendition with Chrome's `FFmpegDemuxer`, which is
libavformat's MP4 demuxer -- and a Chromecast with Google TV's Chrome is
92, whose libavformat is of 4.4's age. What that demuxer makes of a
fragmented MP4 is not what `ffprobe` 6.1 on a development machine makes of
it, and the rendition's layout was settled by measuring both
(`docs/design/renditions.md`: the `sidx` label taken as a decode time, the
`styp` double entry, picture and sound interleaved, a slot in two parts,
`dts_shift`).

This is the measuring tool. It is not built by cargo and no test runs it:
it needs an FFmpeg checkout and a few minutes per version.

```bash
tools/lavf-harness/build.sh n4.4 n6.1 master      # once; ~/.cache/lavf-harness
tools/lavf-harness/compare.py film.mp4 rendition.mp4
```

* `av_dump.c` -- opens a file through its own 32 KiB reader as Chrome
  does, reads it whole or reads some seconds and then seeks the video
  stream backward to each of a list of times (`av_seek_frame`,
  `AVSEEK_FLAG_BACKWARD`, which is `FFmpegDemuxer::Seek`), and prints every
  packet it is handed: stream, presentation time, key flag, decode time.
  Packets FFmpeg marks to be discarded are left out, as a player drops
  them. On stderr, every seek libavformat asks its reader to make to somewhere
  other than where it is.
* `build.sh <tag>...` -- a minimal static libavformat of each FFmpeg tag
  and `av_dump` linked against it.
* `compare.py <film> <rendition>` -- per version: every packet of picture
  and of sound against the film's, the picture's decode times strictly
  increasing, no
  seek asked of the reader in a straight read, and for each seek where
  the picture lands and the three seconds after it. Exits 1 on any
  finding.

Where a rendition to measure comes from:

* this repository's own muxer over a film `ffmpeg` encodes --
  `RENDITION_FILM_DUMP=<dir> cargo test -p server --test rendition_films
  dump -- --ignored` writes `film.mp4` and `stream.mp4` (the knobs are
  environment variables beside the test: `RENDITION_FILM_PICTURE`
  (`hevc`, else H.264), `_LOOK` (`app`, `open`), `_SECONDS` (60), `_KBPS`
  (8000), `_GOP` (180), `_NO_INDEX`);
* the app's producer over any file -- xtremio's `rust/tests/rendition.rs`,
  `serve_a_rendition_until_told_to_stop`, and `curl` the URL it prints.
  Its Matroska films come out 21 ms later than the source (`--shift-us
  21000`).

What it cannot say: anything above the demuxer. Whether a receiver asks
for a byte in one request or a hundred is Chrome's reader -- its 32 KiB
blocks are why slots begin on 32 KiB boundaries -- and is read off a
request log of a real cast (`adb reverse` to a relay that logs `Range`).
What it can say is where the demuxer gives that reader a reason: a seek
in a straight read. A slot padded with one `free` box was one seek a slot, and
a television with nothing buffered asked again at each (zond's 10 GB
film, a request every two seconds after a seek); padded with `free` boxes
of 1 KiB, the last under 2 KiB (`layout::PAD_BOX`, `layout::padding`), the
first part's padding too, there are none.
