# Renditions: a cast the receiver can decode, produced on demand, nothing on disk

Design, 2026-10-01; §2.8 2026-10-02. **F1 (the server side) is built, F0 landed in the app, F1½ answered (libavformat), F2's producer built and its file seekable by bytes (§2.8); F3-F5 are not.**

> **Amended in F2 (zond, 2026-10-01): a rendition is ONE progressive
> fragmented MP4, not HLS.** Measured on zond's Chromecast with Google TV
> (§5, F2): its Media Source path plays no HLS above 720p -- ours, ffmpeg's
> or Mux's own 1080p variant, TS or fMP4, even at 2 Mbit/s -- while the
> same 1080p film plays as one progressive fragmented MP4 through its
> plain `<video src>` path. "Why keep HLS if everything works with
> progressive fMP4?" (zond) -- so HLS is gone: no playlists, no
> `init.mp4`/`{n}.m4s` routes, no `hlsSegmentFormat` hints. The rendition
> is `GET /cast/{token}/stream.mp4`. Everything behind the routes stays as
> designed below -- the run, the cut rule, the ring, the lookahead, the
> idle release, the speed rule, the muxer, the frozen formats -- with the
> segment now a unit inside the file. Where the sections below say
> playlist, `#EXTINF`, `index.m3u8` or "a request for segment N", read the
> file and the slot a range asks for.
>
> **Amended again (zond, 2026-10-02): the file has a length and ranges,
> and the receiver seeks in it by bytes** (§2.8). The first F2 stream had
> neither -- `?from=<ms>` started it elsewhere, and a receiver's own seek
> made it fetch the stream again from its start, which the app undid by
> loading it again where the receiver had been. Measured by hand on the TV
> the same day (§2.8): with a length, ranges that answer exact bytes, and
> a `sidx` after the `moov`, a seek with the TV's remote is one `Range`
> straight at the target fragment, and plays. So the layout of the file is
> fixed before it is made -- each segment in a slot as long as its share
> of the source, padded -- the `?from=` start and the restart count are
> gone, and the app's seeks are the receiver's again. This is step F
of `docs/design/media-pipeline.md` (§2.10 fixed what it must not break;
this note is the design that section asked for). Written against
stream-server `410a1e2`, xtremio `c9cc260`, flutter_chrome_cast 1.4.8 and
the vendored `media_kit_libs_android_video` (`full` flavour, libmpv-android
v1.1.11). Where this and the code disagree once steps land, the code is the
answer.

Agreed principles (zond), which everything below keeps:

* **No cache, nothing on disk.** A rendition is produced on demand, at
  least as fast as it is watched, or refused. Nothing is written to a file,
  not even a scratch one; what is produced lives in a small ring in memory
  and is dropped.
* **The decision is the app's, the bytes are the server's.** Which
  rendition a stream needs is decided in Dart, from what mpv reports and a
  table of receivers. The route, the playlist and the ring are the
  server's. The producer -- the thing that demuxes, decodes and encodes --
  is the embedder's, installed into the server as a trait object, because
  the server crate is a plain `rlib` with no JNI (AGENTS.md) and the only
  encoder there is to use is Android's.
* **A cast that cannot keep up ends with a sentence**, not a spinner.

## 1. What each case needs today

`CastCompatibility.of` (xtremio `lib/features/cast/cast_compatibility.dart`)
answers `CastReady(contentType)` or `CastRefused(reason, explanation)`; every
refusal's sentence ends "Casting it would need conversion, which this app
cannot do yet." What it judges from: the container by file extension (MP4,
M4V, WebM castable), the video codec from mpv's `video-codec` (H.264, HEVC,
VP8, VP9 castable, for every receiver alike), and the audio codec from mpv's
`audio-codec-name` keyed by container (AAC or MP3 in MP4, Opus or Vorbis in
WebM), with the release's own claims (`StreamFacts` tags, the filename)
believed only when they say something is wrong. The receiver in the room is
not consulted: the class comment says so, and says HEVC and VP9 are let
through for receivers that cannot decode them.

What each case needs, against the receivers that matter:

| Source | Receiver | Today | Needs |
|---|---|---|---|
| MP4/WebM, H.264, AAC | any | `CastReady` | Nothing: as-is, `/cast/{token}`. |
| MKV (or any non-MP4 container), H.264, AAC | any | `CastRefused(container)` | **Repackage**: the same samples in fragmented MP4. No codec work. |
| H.264 + AC3/E-AC3 (most web releases) | zond's TV (Chromecast with Google TV, Bluetooth audio) | MKV: refused (container). MP4: `CastRefused(audioCodec)` | **Audio to stereo AAC**. Cast *succeeds* with AC3/E-AC3 and plays **silent** over Bluetooth (zond's measurement), so this is the default for surround whatever the receiver says. |
| DTS, TrueHD, FLAC, Opus-in-MKV | any | refused | Audio to stereo AAC. |
| HEVC 8-bit | Chromecast 1st-3rd gen (no HEVC) | `CastReady` or container refusal -- the 3rd-gen black screen the class comment admits | **Full transcode to H.264**. |
| HEVC | CCwGTV 4K (`sabrina`), CCwGTV HD (`boreal`), Ultra | MKV: refused | Repackage (video copied). |
| H.264 above the receiver's resolution (4K to a 1080p receiver) | Chromecast 1st-3rd gen, `boreal` | `CastReady`, then fails on the device | Full transcode, scaled. |
| MPEG-4 Part 2, MPEG-2, VC-1 | any | `CastRefused(videoCodec)` | Full transcode, if the phone has a decoder for it (§8). |
| AV1 | receivers without AV1 | `CastRefused(videoCodec)` | Full transcode, if the phone decodes AV1. |

The model name the Cast SDK reports does not tell these receivers apart:
the Chromecast with Google TV announces itself as "Chromecast", as the
first three generations do (`CastDevice.model` is `device.modelName`,
`google_cast_client.dart:217`). The codename does:
`https://<ip>:8443/setup/eureka_info?params=device_info` answers a
`product_name` -- `sabrina` is the CCwGTV 4K, `boreal` the CCwGTV HD --
over a self-signed certificate, with no authentication (zond's finding; not
re-measured for this note). `CastDevice.address` already carries the
receiver's IP once a session has started.

**A prerequisite that is not built.** Step C's server half is done -- the
LAN listener serves `/cast/{token}` and nothing else (`cast::router`,
`server/src/cast.rs:194`). Its app half is not: xtremio `c9cc260` pins
stream-server `29e377e`, which is after step C (`113a556`), and
`_castUrl` (`player_screen_casting.dart:439`) still rebuilds the loopback
URL on the LAN base, which that server answers `404`; nothing in xtremio's
`lib/` or `rust/src/` calls `publish`. Renditions publish too, so the app
half of C is step F's first dependency (F0, §5).

## 2. The abstraction

```
 media id ── reader (2.4 of media-pipeline) ── JNI MediaDataSource ── Kotlin producer ──┐
                                                                                         │ samples
 receiver ── /cast/{token}/hls/index.m3u8 ── playlist (written up front)                 │ (pts, key, bytes)
          ── /cast/{token}/hls/init.mp4   ── init segment ──┐                            │
          ── /cast/{token}/hls/{n}.m4s    ── ring ── muxer ─┴── sample sink ◄────────────┘
            (server: route, playlist, cut rule, muxer, ring, speed)      (embedder: demux, decode, encode)
```

### 2.1 The rendition: a published token with a ring behind it

A rendition is a publication (`cast::Publication`), with the same token
rules -- 128 random bits, memory only, a lease on the id, cut by
`unpublish` and by the listener's stop, never logged (`log_path` already
elides everything under `/cast`, and its test already spells
`/cast/0011/hls/master.m3u8`, `routes/util.rs:406`). What it adds is a
`Rendition` beside the id:

```rust
/// What the app asks for. Crosses FFI from Dart, so plain data.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenditionSpec {
    /// The film's duration, from mpv: the playlist is written from it.
    pub duration_ms: u64,
    /// Target segment length (6000; §6).
    pub segment_ms: u32,
    /// Where the receiver will start: the first run begins at this
    /// segment, so the init segment and the first segment come from one run.
    pub start_ms: u64,
    pub video: VideoPlan,
    pub audio: AudioPlan,
    /// Which of the source's audio tracks, by its ordinal among audio
    /// tracks (mpv's selection, mapped by the app).
    pub audio_track: u32,
}

pub enum VideoPlan { Copy, H264 { width: u32, height: u32, bitrate: u32 } }
pub enum AudioPlan { Copy, AacStereo { bitrate: u32 } }
```

"Repackage only" is `Copy`/`Copy`; "audio only" is `Copy`/`AacStereo`; a
full transcode is `H264` with either audio plan. Audio is copied only when
it is AAC (the decision's rule, 2.7), so the muxer knows two audio sample
entries' worth of nothing beyond AAC.

**The playlist is written once, up front**, from the duration: a VOD media
playlist (`#EXT-X-PLAYLIST-TYPE:VOD`, `#EXT-X-ENDLIST`), `#EXT-X-MAP` naming
`init.mp4`, and `ceil(duration / T)` entries `#EXTINF:T` naming `0.m4s`,
`1.m4s`, ... -- relative, so the playlist names no host and the receiver
resolves it against the LAN base the app handed it. Nothing about the
playlist waits for the producer.

**The cut rule: segment N begins at the first video sync sample whose
presentation time is at or after N x T**, and ends where segment N+1
begins; each audio sample belongs to the segment its presentation time
falls in. One rule for every plan:

* When video is encoded, the producer forces a sync frame at every N x T
  (2.4), so the rule cuts at exactly N x T and every segment is T long.
* When video is copied, the cuts are wherever the source's keyframes are,
  at or after N x T. Every segment then starts with a keyframe, which a
  receiver needs after a seek (a media segment appended after a
  discontinuity must begin at a random access point, or the decoder drops
  frames until one), and segments are contiguous and never overlap,
  because the cut is a function of N and the source alone. What the rule
  costs is accuracy: `#EXTINF` says T and a segment is T plus or minus a
  GOP, and a GOP longer than T leaves a segment empty (§8). The fragment's
  `tfdt` carries the true decode time, so the timeline the receiver plays
  is right even where the playlist's arithmetic is not.

**Segment N is produced from N's timestamp, and produced whole before it
is answered**, so `MediaRange` and `Content-Length` hold
(`routes/util.rs:63`, the framing every media route shares): a segment is
`Bytes` in memory, ranged like any other resource. A request for segment N:

1. **In the ring**: answered at once.
2. **Being produced, or within the lookahead** (N at most L past the
   segment in production): waits for it. A request for the segment in
   production joins that production; it never restarts it, so a receiver
   that times out and asks again finds the same run further along.
3. **Anything else** -- behind the ring, or further ahead than L: a
   **seek**. The run is dropped (its reader cancelled, its codecs
   released) and a new one starts at N x T.

A receiver fetches the segments of one stream one after another, so
playback never trips the third case; a seek always does.

**The ring** holds the last few segments served and the L produced ahead:
pick 2 behind and L = 2 ahead, bounded by 96 MiB whichever is smaller,
never fewer than the segment being served. The behind half answers a
receiver's retry; the ahead half is what lets production run ahead of
playback at all. A pause is the receiver asking for nothing: the run
produces L ahead, then the sink's next write blocks (2.2), and the
producer's thread, its reader and its codecs sit idle until a request
moves the window. **An idle run is let go after `IDLE_RELEASE` (60 s) with
no request** (zond, over the draft's "no timer"): a phone's hardware codec
instances are few and shared with every other app, and a cast paused for
an hour must not hold two of them. Letting go drops the run (reader,
codecs) and keeps the ring; the next request is served from the ring or
starts a new run at that segment, which is the seek path and costs one
GOP. A paused receiver asks for nothing, so nothing distinguishes a long
pause from the viewer having walked away, and both are answered the same.

**The init segment** needs the tracks' codec configuration (SPS/PPS,
AudioSpecificConfig), which exists only once a run has produced -- an
encoder reports its output format with its first output. A request for
`init.mp4` before any run starts the first run at `start_ms`'s segment and
waits for the formats. **The first run's formats are frozen into the init
segment**; a later run (a seek) whose formats differ fails the rendition
with a sentence (2.5) rather than serving segments the init segment does
not describe (§8 for why that could happen and what `avc3` would buy).

**Cancellation.** `unpublish` (and the listener's stop) cancels the
publication's cut, as for a plain cast. For a rendition the cut also
drops the run: the sink answers `Stopped` to the producer's next write,
the run's `Canceller` wakes a read parked in the producer's
`MediaDataSource`, the ring is dropped, and a request waiting on a segment
is woken by the cut and answered with an error -- the same rule as
`CastBody`'s: a broken source, never a clean end the receiver reads as the
film being over.

### 2.2 The `Producer` trait: what the server asks the embedder

```rust
/// Installed once by the embedder (`ServerHandle::install_producer`).
/// Turns a reader over a media id into encoded samples, from a time.
pub trait Producer: Send + Sync + 'static {
    /// Begin a run and return at once. The run reads `job.reader` and
    /// writes to `job.sink` **on the producer's own thread**, never the
    /// caller's; it ends when the sink answers `Stopped`, the reader
    /// fails, or the source ends (`sink.end()`).
    fn start(&self, job: Job) -> Result<(), ProducerRefusal>;
}

pub struct Job {
    /// Opened by the server with the publication's play, as a cast body is
    /// (`Registry::open_reader`): the reads are the viewer's playback.
    pub reader: MediaReader,
    pub spec: RenditionSpec,
    /// Start here: N x T for a run that begins at segment N. The producer
    /// starts at the sync sample at or before it; the server discards what
    /// precedes the cut (2.1).
    pub from: Duration,
    pub sink: SampleSink,
}

/// The run's way back into the server. Blocking, for a foreign thread,
/// in `MediaReader`'s mould: calling it from inside a tokio runtime is an
/// error, not a blocked worker.
impl SampleSink {
    /// A track's format: codec, codec configuration bytes (`csd-0`,
    /// `csd-1` as `MediaFormat` carries them), size or rate and channels.
    pub fn format(&self, track: TrackKind, format: TrackFormat) -> Result<(), Stopped>;
    /// One access unit. Blocks while the run is L segments ahead of the
    /// last request (the pause), returns `Stopped` once the run is dropped.
    pub fn sample(&self, sample: Sample) -> Result<(), Stopped>;
    /// The source ended: the last segment is whatever is buffered.
    pub fn end(self);
    /// The run cannot go on, with the sentence a viewer is shown.
    pub fn fail(self, sentence: String);
}

pub struct Sample { pub track: TrackKind, pub pts_us: i64, pub key: bool, pub data: Bytes }
```

Synchronous and tiny on purpose. All async stays in the server: the sink
is a bounded channel into a run task on the server's runtime, which muxes,
cuts, fills the ring and measures speed. The producer never sees a
segment, a playlist, a token or an HTTP request; it sees a reader, a plan,
a time, and somewhere to put samples. That is also what makes F1 testable
with a producer written in Rust on a plain thread.

**The muxer is the server's**, in Rust, hand-written: the boxes this needs
are a short list -- `ftyp`, `moov` (`mvhd`, `mvex`/`trex`/`mehd`, one
`trak` per track with `avc1`+`avcC`, `hvc1`+`hvcC` or `mp4a`+`esds`) for
the init segment, and `moof` (`mfhd`, one `traf` per track with
`tfhd`, `tfdt`, `trun`) and `mdat` per segment, after a `styp` only when
the segment does not open at its `sidx` label (the mirror layout, below,
says why). It converts Annex-B
samples (what Android's extractors and encoders hand out, with start
codes) to length-prefixed, and builds `avcC`/`hvcC` from the parameter
sets in `csd-0`/`csd-1` -- an `hvcC` keeping the SEI messages beside them
(x265 writes an HDR10 film's mastering display and light levels with its
headers, which a Matroska file keeps in its `hvcC` and nowhere in its
samples), and an `hvc1` followed by a `colr` (`nclx`) with the colour
description of the SPS's VUI, so a demuxer reading the container sees PQ or
HLG on BT.2020 as the Matroska `Colour` element said it. Copied H.264 or HEVC with B-frames needs a
decode time per sample, which `MediaExtractor` does not report (only a
presentation time); the muxer derives decode times as the sorted
presentation times of the reorder window, and writes composition offsets
in a version-1 `trun` (signed). Encoded video is asked for no B-frames,
so there decode time is presentation time. No new dependency, nothing
native, and `cargo test` covers it -- which a muxer on the Kotlin side
would not be.

### 2.3 Reading the source: a `MediaDataSource` over the media id's reader

The producer reads the media id through 2.4 of the pipeline: the server
opens a `MediaReader` with the publication's play and hands it to the
producer in the `Job`. xtremio's crate implements `Producer` and keeps the
reader in a handle table; the Kotlin side reads it through an
`android.media.MediaDataSource` whose two methods are JNI exports from
that crate:

| Kotlin | Rust (xtremio's crate, `#[no_mangle] extern "system"`) |
|---|---|
| `getSize()` | `MediaReader::len` |
| `readAt(position, buffer, offset, size)` | `seek` to `position` when the reader is not there, then one `read` (at least one byte, whatever has arrived -- `readAt` may return fewer than asked) into the Java array |
| `close()` | drops nothing; the run owns the reader |

This is the shape xtremio's `mpv_stream.rs` already has for mpv's
`stream_cb`: a blocking reader behind a mutex, a `Canceller` beside it so a
cancel never waits for the read it interrupts. A cancelled read answers
`IOException`, and `MediaExtractor` fails the call it was serving, which
ends the run.

**Kotlin pulls; Rust never calls into Java.** A Kotlin worker thread blocks
in one JNI export, `awaitJob()`, that returns a job handle when the
server's `Producer::start` has queued one; every other call -- `readAt`,
`format`, `sample`, `end`, `fail` -- is Kotlin calling Rust with that
handle. So the crate needs no `JavaVM`, attaches no threads and holds no
global references, and `Producer::start` returning at once is a channel
send. The worker is started by the app after the server starts
(`System.loadLibrary("xtremio_core")`, then a thread per job), and the
crate calls `install_producer` when the Kotlin side has said it is there.

`jni` 0.22.4 is already in xtremio's lock (through
`rustls-platform-verifier`, under `reqwest`), so the exports add no crate.
Nothing in the crate exports a JNI symbol today.

### 2.4 The Kotlin producer

One `MediaExtractor` per run, `setDataSource(MediaDataSource)` (API 23;
the app's `minSdk` is Flutter's default, 24), with the video track and the
chosen audio track both selected -- one extractor, interleaved reads, **one
reader per run**. Then per plan:

* **Repackage (`Copy`/`Copy`).** `readSampleData` straight to
  `sink.sample`, with `getSampleTime` and `SAMPLE_FLAG_SYNC`. **No
  `MediaCodec` at all.** The run is as fast as the source can be read.
* **Audio only (`Copy`/`AacStereo`).** Video samples as above. Audio
  samples into a `MediaCodec` decoder for the source's MIME, its PCM
  downmixed to stereo (a fixed ITU-R BS.775 matrix over the decoded
  frames), into a `MediaCodec` encoder `audio/mp4a-latm` (AAC-LC, 48 kHz,
  2 channels), whose output goes to the sink. **The video encoder is never
  created**; the only codec work is audio, which is cheap.
* **Full transcode (`H264`).** A `MediaCodec` decoder for the source
  video, rendering into a `Surface`; the encoder (`video/avc`, the plan's
  size and bitrate, `KEY_MAX_B_FRAMES` 0) takes its input from
  `createInputSurface()`. Same size: the decoder renders straight into the
  encoder's surface. A different size (4K to 1080p): one GL pass between
  them (a `SurfaceTexture`, one textured quad into the encoder's surface
  with `eglPresentationTimeANDROID`). **The surface path, not byte
  buffers**: decoder output in byte buffers is a vendor YUV layout and a
  CPU copy per frame, which at 4K is the whole budget. A sync frame is
  requested (`PARAMETER_KEY_REQUEST_SYNC_FRAME`) when the first frame at or
  after each N x T is released to the encoder, which is what makes the cut
  rule land on N x T.

**A seek is a new run.** `seekTo(N x T, SEEK_TO_PREVIOUS_SYNC)` puts the
extractor on the keyframe at or before N's timestamp, and everything
before N x T is decoded and **discarded**: a decoded video frame before
it is released with `render = false` (never reaching the encoder); audio
is fed to the encoder from one AAC frame before N x T, and encoder output
before N x T is dropped, so the encoder's priming delay is spent on audio
nobody hears. In copy mode nothing is decoded and the server discards
samples before the cut. Every timestamp the sink sees is the source's own
presentation time (minus the source's first, so the film starts at zero),
so a restart cannot shift the timeline: `tfdt` is computed from the
source's clock, never from a count of what this run produced.

**What the extractor cannot read, the run refuses**, with a sentence:
`setDataSource` or the track selection failing is `sink.fail`. Whether
`MediaExtractor`'s Matroska extractor exposes every audio codec the decision
needs (DTS, TrueHD) and whether it reads AVI at all is not verified here
(§9); 2.6 has the way out if it does not.

### 2.5 Speed: measured, and the cast ended with a sentence

The run task measures **production speed: media time written to the sink
over the run's busy time**, where busy time excludes two waits the
producer is not responsible for: the sink blocking (the pause, a full
lookahead) and the reader waiting for bytes (the JNI `readAt` is timed in
Rust, so the server knows how long the source kept it). A run below 1.0x
over its last 10 seconds of busy time -- once its first segment is out, so
a codec's start-up is not counted against it -- **fails the rendition**:
nothing more is produced, waiting requests are answered `503`, and
`rendition_state` reports the sentence, e.g. "This phone cannot convert
this film to H.264 fast enough for the television: it made 7 seconds of
film in 10." The app polls that state with its cast watchdog and ends the
cast with the sentence (2.7).

A slow *source* is not a failure: a torrent that stalls is the stall the
receiver buffers through, as it does for a plain cast, and the speed
excludes it. The two are told apart because the server serves both the
reads and the sink.

The other failures `rendition_state` carries the same way: a producer's
`fail` (an extractor that cannot read the file, no decoder for the codec
on this phone, a codec that errors), and formats that change across runs
(2.1).

### 2.6 What was weighed for the producer, and the one fact that changes F3

**mpv's ffmpeg cannot be the producer**, now verified: the vendored
`full` libmpv (`build/media_kit_libs_android_video/v1.1.11/full-arm64-v8a.jar`,
`lib/arm64-v8a/libmpv.so`) carries its ffmpeg configure line, and it says
`--disable-muxers --disable-encoders` (with only `mjpeg`, `ljpeg`,
`jpegls`, `jpeg2000` and `png` encoders turned back on). There is no H.264
or AAC encoder and no muxer in it.

**But it has every decoder and every demuxer, and exports them.** The same
line says `--enable-decoders --enable-demuxers --enable-parsers
--enable-bsfs --enable-swresample`, and `libmpv.so`'s dynamic symbol table
holds 870 `av*` symbols, among them `avformat_open_input`,
`avio_alloc_context`, `av_read_frame`, `av_seek_frame`,
`avcodec_find_decoder`, `avcodec_send_packet`, `av_bsf_get_by_name` and
`swr_alloc_set_opts2` (`llvm-readelf --dyn-syms`, arm64). This matters
for F3, because **the audio that most needs re-encoding is the audio a
phone is least likely to have a `MediaCodec` decoder for**: AC3/E-AC3
decoders ship only on Dolby-licensed devices, DTS rarely, TrueHD almost
never (general Android knowledge, not measured on zond's phone). xtremio's
crate already `dlopen`s this library to find `mpv_stream_cb_add_ro`
(`mpv_stream.rs`); the same handle finds `avcodec_*`. So the audio path is:
**a `MediaCodec` decoder when `MediaCodecList` has one for the source's
MIME; otherwise libavcodec from `libmpv.so`**, the compressed audio sample
handed to Rust over JNI and PCM handed back, with `swr` doing the
downmix. AAC encoding stays `MediaCodec` (AOSP's software AAC encoder is
on every device). If `MediaExtractor` turns out not to expose a DTS or
TrueHD track at all, demuxing moves to libavformat in the same library,
reading the same `MediaReader` through `avio_alloc_context` -- the variant
§6 offers zond.

**Not `MediaMuxer`** -- verified against `android.jar` (API 36):
`MediaMuxer.OutputFormat` is `MUXER_OUTPUT_MPEG_4`, `WEBM`, `3GPP`,
`HEIF`, `OGG`, and nothing else. There is no MPEG-TS output, no fragment
control, and the constructors take a path or a `FileDescriptor`: it writes
a whole MP4 to a file and finishes it in `stop()`. A whole MP4 per segment
is not an HLS segment, and a file is what this design does not write. The
muxer is the server's (2.2).

**Not Media3 Transformer** (not in the app today: `build.gradle.kts` names
`core-ktx`, `play-services-cast`, `play-services-auth` and nothing from
`androidx.media3`). Weighed honestly, from Media3's documentation, not
from a build in this tree:

* *A custom data source: yes.* Transformer loads through an asset loader
  whose `MediaSource.Factory` can be built on a custom `DataSource.Factory`
  -- Media3's `DataSource` (`open(DataSpec)`, `read`) is if anything a
  closer fit to `MediaReader` than `MediaDataSource` is.
* *But it is an exporter.* It runs a composition to completion as fast as
  it can and hands the result to a `Muxer` that writes a path. A segment on
  demand means a new `Transformer` per run with a clipping start, pacing by
  blocking its muxer thread from a custom `Muxer.Factory`, and its own
  muxer replaced -- three fights with its model, for what F2 does with no
  codec at all.
* *It decodes audio with `MediaCodec` too*, so the AC3/DTS gap above is
  the same (unless Media3's ffmpeg extension, which is a native build of
  ffmpeg we would maintain).
* *What it would bring*: HDR-to-SDR tone mapping in its effects pipeline,
  encoder fallback when a configuration is refused, and A/V
  synchronisation that is someone else's tested code. The tone mapping is
  the one with no cheap substitute here (§7).
* *Cost*: `media3-transformer` with `media3-exoplayer`, `media3-effect`,
  `media3-muxer` and `media3-common` behind it -- several MB of dex
  (size not measured).

Pick: raw `MediaExtractor` + `MediaCodec`, and the server's muxer.
Revisit Transformer only if HDR sources to SDR receivers become a case
zond hits (§6).

### 2.7 The app: the receiver table, the decision, and a third outcome

**The receiver table lives in the app** (`lib/features/cast/`), keyed by
codename, because the decision is the app's:

```dart
/// What one receiver model decodes, from Google's per-model table.
final class ReceiverCaps {
  final Set<String> video;            // 'H.264', 'HEVC', 'VP9', 'AV1'
  final ({int width, int height}) max;
  final bool hdr;
}
const Map<String, ReceiverCaps> receiverCaps = {
  'sabrina': ...,   // Chromecast with Google TV (4K)
  'boreal': ...,    // Chromecast with Google TV (HD)
  ...
};
/// A codename this table does not know, or no eureka_info answer: the
/// least capable row (H.264 up to 1080p, AAC stereo).
const ReceiverCaps unknownReceiver = ...;
```

The rows are Google's published per-model table, entered by hand; their
contents are not verified in this note. The codename is fetched once per
Cast device id when a session starts (`CastDevice.address` is known then):
`GET https://<address>:8443/setup/eureka_info?params=device_info` with a
`badCertificateCallback` that accepts **only that host and port**, read for
`device_info.product_name` and nothing else, cached for the process. A
receiver that does not answer within a couple of seconds is
`unknownReceiver`. What a forged answer can do is make the app choose a
different rendition for a device on the same network; there is nothing
else it is believed for.

**The stats poll gains the audio channel count.** `PlaybackStats.mpvProperties`
(`playback_stats.dart:58`) polls `video-codec` and `audio-codec-name`;
the channel layout exists only per track today (`PlaybackTrack.channels`,
from media_kit's track list). Add the selected track's own count,
`current-tracks/audio/demux-channel-count` (the container's figure, so it
is right before the first frame is decoded; mpv property, not exercised
here).

**The decision**, in order, from mpv's report and the receiver's row:

1. Audio: AAC with at most two channels is `Copy`; anything else --
   AC3, E-AC3, DTS, TrueHD, FLAC, Opus, or AAC with more than two channels
   -- is `AacStereo` (192 kbit/s). **Surround is re-encoded by default**,
   whatever the receiver claims, because of the silent Bluetooth case.
2. Video: a codec in the row's set within its size is `Copy`; otherwise
   `H264` at the row's size (capped at the source's), at a bitrate by
   size (8 Mbit/s at 1080p).
3. All `Copy` and an MP4/M4V/WebM container whose audio the container
   rule already allows: **as-is**, `CastReady`, `/cast/{token}`.
   Otherwise a rendition.

`CastCompatibility` gains its third outcome beside `CastReady` and
`CastRefused`:

```dart
/// The stream can be cast as a rendition the receiver decodes.
final class CastRendition extends CastCompatibility {
  final RenditionSpec spec;      // what publishRendition is handed
  final String summary;          // "Sound converted to stereo" -- shown on the remote
}
```

`_handToReceiver` publishes with `publishRendition(id, spec, play)` and
loads `<lan base>/cast/<token>/hls/index.m3u8` with `contentType:
application/x-mpegurl`, `hlsSegmentFormat: fmp4`, `hlsVideoSegmentFormat:
fmp4` and the stream duration. flutter_chrome_cast 1.4.8 already carries
those to the Cast SDK (`MediaInfoExtensions.kt` calls
`setHlsSegmentFormat`/`setHlsVideoSegmentFormat`, whose comment says the
default receiver otherwise "stays stuck in LOADING" on fMP4 HLS, and
`setStreamDuration`); what xtremio lacks is the fields: `CastMedia`
(`cast_client.dart:116`) has `url`, `contentType`, `title`, `subtitle`, and
`GoogleCastClient.load` (`google_cast_client.dart:410-429`) sets only
those. The cast watchdog (`_castFetchCheck`) reads `rendition_state`
beside the two counts, and a failed rendition ends the cast with its
sentence.

### 2.8 Seeking by bytes: the mirror layout

*Measured 2026-10-02 on zond's Chromecast with Google TV, by hand from a
desktop over `adb reverse`, with Python servers serving a 10-minute 1080p
H.264/AAC film repackaged by `ffmpeg` (`frag_keyframe+empty_moov`, the
moov's lengths patched; the scratch servers and the TV's logs are in the
session's `f2/` scratchpad, `TV_FINDINGS.md` and `handseek_f2.py`):*

1. **No length, no ranges** (the first F2 stream): a seek past what is
   buffered -- the remote's or a sender's -- makes the receiver fetch the
   stream again from byte 0; the target is never reported.
2. **A length and `Accept-Ranges`, but a far `Range` answered with a fresh
   file** (a new `ftyp`/`moov` from the target time) **fails at load**: the
   TV's Chrome reads in ranged blocks even when it reads forward
   (`bytes=1.0M-`, `1.5M-`, `2.0M-`, ..., each connection dropped after
   about 3.3 MB), so every range must be the exact, consistent bytes of
   one file.
3. **A length, ranges and exact bytes, no index**: a seek makes FFmpeg's
   MP4 demuxer (inside Chrome) walk every `moof` from the start -- about
   one ranged request per fragment, skipping the `mdat`s. Fine when every
   byte is local; far too slow behind a torrent. An `mfra` at the end is
   **ignored** by FFmpeg 6 (it peeks at the last 4-16 bytes for its size
   and never reads it).
4. **Plus a `sidx`** (version 1, `reference_ID` the video track, its
   timescale, `earliest_presentation_time`, `first_offset` 0, one
   reference per fragment: `referenced_size` the fragment's bytes,
   `subsegment_duration`, `starts_with_SAP` 1) **right after the `moov`**:
   `ffprobe` seeks to 5:00 in four requests (the start, a 4-byte peek at
   the end, the first fragment, one jump to the target fragment), and on
   the TV zond's remote seeks were **one** `Range: bytes=X-` at the target
   fragment each, and played (5:00, 7:16). The receiver showed 10:00 from
   the `mdhd`.

So the file needs a length and a `sidx` before anything is produced, and
every byte answered the same whoever asks and whenever. The design zond
approved, **the mirror layout**, built in `rendition/layout.rs`,
`slots.rs`, `run.rs` and `cast.rs`:

* **The header** is the init segment and the `sidx`. Then **one slot per
  segment**: slot `k` holds segment `k`'s fragment, padded with a `free`
  box to the slot's end. The `sidx` gives each slot's size and duration,
  so FFmpeg jumps to the slot that holds a time. **The video's `sidx`,
  and every track on its 90 kHz clock** (the sound's `mdhd` too). FFmpeg
  finds a stream the `sidx` does not index by the indexed track's times
  (`get_frag_time`), so the sound lands in the picture's slot -- but
  before 6.0 (commit e1e981c, May 2022) it compared those times with the
  sound's seek time **unscaled**: with the sound on 48 kHz, a seek to
  85 s put the sound at 85 x 48/90 = 45 s and the demuxer read on from
  there. That is what zond's Chromecast with Google TV did (Android TV
  14, `mediashell` 3.72): remote seeks to 1:25 and 4:14 asked for the
  target and, at once, 0:42 and 2:12, and played after ~30 s of the TV
  walking the fragments between. On one clock the comparison is right
  in every version.
* **An estimated layout indexes the sound again**, in a second `sidx`
  (the video's `first_offset` steps over it) labelled at each cut less
  the sound's lead, not a GOP late. A seek places the picture on a sync
  sample inside the slot its late label picked, often before that label;
  FFmpeg then seeks the sound to that sample's time, which by the
  picture's labels is the slot before, and asked for it (6.0 to 8.0; 4.4
  and 5.1 walked from much further back). Labelled early, the sound's
  slot is the picture's or a later one. A mirrored layout needs no second
  index: its labels are the sync samples. (Last round one `sidx` per
  track was tried with both labelled late, and the sound picked an
  earlier slot than the picture -- that is the late label, not the
  second index.)
* **Measured** (2026-10-02) with libavformat 4.4, 5.1, 6.0, 6.1, 7.0,
  7.1, 8.0 and master, as Chrome's `FFmpegDemuxer` drives it (demux to
  42 s, `av_seek_frame` on the video, backward, demux 20 s on), over
  HTTP against a fresh server per probe, on zond's 10-minute film as
  Matroska (mirrored) and as a transport stream (estimated); requests
  behind the target slot's after the seek:

  | layout | seek | before (90/48 kHz, one `sidx`) | now |
  |---|---|---|---|
  | mirrored | 85 s | 4.4, 5.1: picture at 42 s, 1 | 0 everywhere |
  | mirrored | 252 s | 4.4, 5.1: 20 (from 135 s) | 0 everywhere |
  | estimated | 85 s | 4.4, 5.1: 14; 6.0-8.0: 2; master: 0 | 0 everywhere |
  | estimated | 252 s | 4.4, 5.1: 40; 6.0-8.0: 2; master: 0 | 0 everywhere |

  Both streams land in the target slot in every version now (the
  estimated layout on the slot's last sync sample before the target,
  reading forward from it). Headless Chrome 154, played 15 s then
  seeked: one far `Range` per seek on both.
* **A slot opens with its `moof` when it opens at its label; with a
  `styp` otherwise.** FFmpeg keeps one fragment-index entry per offset:
  the `sidx`'s reference and each `moof` it reads.
  * Apart (a `styp` first), they are two entries for one fragment, and
    after a seek to it FFmpeg parsed its `moof` twice -- seeking the
    second stream picks the `moof`'s own entry, unread -- doubling its
    samples in the index. Forward seeks survived that; a seek **back** to
    a fragment read neither at the start nor since did not: its samples
    went in between the doubles, the index was out of order, and the
    search landed at the end of what was read at the start. zond's TV
    (Chrome 92), back to 0:43 after seeks to 1:55, 7:05 and 8:41, asked
    for 0:43's slot, then 0:18's, and walked forward a slot per request,
    buffering, for over a minute. Every version, 4.4 to master, did the
    same on a file (the video landing at 16 s), and headless Chrome 154
    made 30 requests for that seek.
  * Together, FFmpeg up to 4.4 -- the TV's -- takes a fragment's decode
    time from the `sidx` label instead of its `tfdt` (5.0 added
    `use_tfdt`, on by default). Right only where the label is the
    segment's start.

  So a slot opens with its `moof` exactly when it is mirrored and opens
  with its own segment (not with what the slot before spilled): its label
  is its cut. Then the back seek lands in its slot in every version, and
  in Chrome it is one request. **An estimated layout keeps the `styp`**
  -- its labels are a GOP late (the sound's early), and without it FFmpeg
  4.4 put picture and sound 10 s apart -- **and with it the walk on a
  seek back** to a fragment not read before (headless Chrome: 7 to 17
  requests; on the TV it may not recover). Fixing that needs a label that
  is each slot's real start before the slot is made, which an estimate
  does not have: open.
* **With the source's index, the slots mirror the source.** The producer
  reports the video's sync samples -- `(pts, byte position)`, from
  libavformat's index (Matroska cues, an MP4's sample tables, an AVI's
  `idx1`), refined for Matroska with each cue's `CueRelativePosition` --
  when the job asks for them (`Job::wants_index`, the first run only).
  Segment `k` is cut at the first indexed sync sample at or after `k x T`
  (each a different one: a GOP longer than `T` makes one segment, not an
  empty one), and slot `k` is the source's bytes from that sync sample to
  the next segment's -- the same samples, so about the same bytes -- plus
  **headroom**, 8 KiB and a 64th of the span: a fragment carries a `moof`
  where a Matroska cluster carries a few bytes per block, and an MP4
  source keeps its sample tables in its `moov`, so the same samples are
  never quite the same size. The last slot runs to the source's end. The
  file's length is the header plus every slot, exact before a byte is
  made; the `sidx`'s durations are the indexed times, exact by
  construction. An index that starts more than a minute into the film, stops
  more than a minute (or a twentieth of the film) before its end, or has
  a gap of more than a minute is not the film's -- it is the few entries a
  demuxer adds as it reads or seeks in a file that has none (a transport
  stream's seek leaves its start and its end: measured, it made a 6-minute
  film two slots) -- and is estimated.
* **Without one, the slots are estimated**: segments on the `k x T` grid
  (a segment's real start may be up to a GOP later, which the `tfdt`
  says), slot `k` in proportion to time over the source's size, **15%**
  larger and 8 KiB on top. The `sidx` labels an estimated slot **10 s
  after its cut** (the longest GOP assumed), so the slot a demuxer picks
  for a time began before it: labelled at the cut, Chrome picked a slot
  whose first sync sample was after the target, found none at or before
  it, and decoded forward from the start of what it had read -- 24 s to
  5:00 on zond's 10-minute film as a transport stream (headless Chrome,
  `tool/rendition-video`). Labelled late, the same seek is one far
  `Range`; the demuxer starts at the slot's last sync sample before the
  target, a few seconds of decoding at most. `ffprobe` did not show this:
  it seeks to the target in either.
* **The overflow rule** (`slots.rs`). A segment whose fragment does not
  leave 24 bytes of its slot (a `free` header and a zero tail) keeps the
  longest prefix that fits -- cut in decode order only where nothing kept
  is shown after anything left over, so decode times stay in order -- and
  the rest **spills** to the start of the next slot, before that
  segment's own samples; when the next slot's start is already decided (it
  was made, or a run was started at it -- checked when the end is
  recorded, since two runs may be live) or there is no next slot, the rest
  is **dropped** instead, and logged. The decision is recorded per slot the first time, so a slot
  made again -- the ring let it go, or a run was started at it -- is made
  the same way: a slot's start is where the slot before spilled to, or its
  own segment's beginning. Mirrored from a real index, a spill is rare
  (the headroom absorbs the difference in overhead); estimated, it is how
  a stretch of the film denser than the average is carried.
* **The same bytes every time.** The muxer is a pure function of a slot's
  samples, and a slot's samples do not depend on where its run started:
  a run made for a slot asks its producer to start **two seconds before**
  the cut (`SEEK_BACK`), so a sample a container stores before the sync
  sample it seeks to (audio interleaved ahead of video) is still read,
  and everything before the cut is discarded as before.
  A run from the film's start seeks to the start too (the producer's
  rule, xtremio `rendition.rs`), as the first run does to read the cues:
  a seek is not a read from the file's first byte -- zond's film's audio
  packet at -21 ms, before the first cluster's key, comes back only
  without one -- and slot 0 made again by a run that did not seek had an
  audio frame more (found replaying the TV's requests against a fresh
  server; every slot now matches a sequential read).
* **The last 16 bytes of every slot are zeros**, answered without making
  anything: FFmpeg's peek at the file's end for an `mfra` size reads 0.
* **Requests, and nothing else, make slots** (zond, 2026-10-04: "always
  just give the tv what it asks for", as the phone's own player is given
  what it reads). `HEAD` and any `GET` wait for the layout -- the first
  run's formats and index -- starting that run from the film's start, what
  a reader of the header reads on into; where the receiver was told to
  start decides nothing here.
  A range that begins in a slot waits for that slot's fragment before it
  answers (a failure is a `503`); one that begins in the header answers at
  once. **Up to two runs** (`MAX_RUNS`), each with its own lookahead
  (counted from the last slot asked of it, and from where it starts), its
  own idle clock and its own waiters: a slot in the ring is answered; one a
  run will make within its lookahead joins that run; otherwise the request
  starts a run there beside the live one, or -- both taken -- in place of
  the least recently asked run **nobody waits on**, and only a range's
  first look at its first slot may take a run somebody waits on. So two
  readers far apart (`ffprobe` keeping its first connection open while it
  seeks on a second; zond's TV reading on from 0:30 while it seeks to
  1:00) each keep their run, instead of taking one run from each other
  in turn -- millions of times a second in the first build, every few
  seconds on the TV in the second. **The receiver's opening read**, from
  the header on into slot 0, is a request like any other: Chrome 92's
  FFmpeg demuxer on zond's TV probes slot 0 before it seeks to
  `currentTime`, and when a rule kept that read from starting a run (the
  first run was at the spec's start) it sat on the header -- 10704 bytes
  -- for a minute until the receiver gave up. **A run is never let go while a request waits for it to
  make a slot**, however long the making takes. **The ring keeps what it
  can hold** (96 MiB), the slots farthest from where any run is asked
  dropped first, never the lookahead ahead of one: a body's read on runs
  ahead of what the receiver took into socket buffers, and a slot dropped
  two behind the last request was asked for again by the receiver's next
  ranged block and made again by a new run (headless Chrome, a seek to
  60 s). The speed rule and the absence of any give-up timer (a stalled
  source is waited for) are unchanged.
* **A segment's sound begins 64 ms before its cut** (`AUDIO_LEAD_US`, more
  than one AAC frame): the audio frame playing at the sync sample is in the
  sync sample's fragment. FFmpeg n6's `mov_read_seek` seeks the video, then
  every other stream to the sync sample's time, backward: with each
  fragment's sound cut at or after the sync sample -- up to a frame later
  -- the target fragment held no sound at or before it, and the sound
  stream fell back to the last frame it knew, from the reads at the
  file's opening: `ffmpeg -ss 60` asked `0-`, the target, then 4.6 MB (6 s)
  and the target again. With the lead, `ffmpeg -ss` makes two requests --
  the start, the target -- at 30, 60, 90 and 300 s on the mirrored film,
  and headless Chrome one far `Range` per seek. zond's TV, seeking to
  1:00, read on from 0:30 and stalled buffering; that was not this but the
  clocks above (60 x 48/90 = 32 s), and it went on with the lead.
* **Prepared before the load** (2026-10-04). zond's phone cast an HEVC
  Matroska film from a torrent with one or two peers: the first piece took
  38 s and the last (the Cues) 26 s before the first run could fix the
  layout, and the receiver (the Chromecast default receiver, Chrome 92)
  gave up on a first answer that silent; the same film from a Real-Debrid
  link cast fine. So the app asks `ServerHandle::prepare_rendition` right
  after publishing: a simulated receiver, through the same request path,
  asking for what the receiver will ask first -- the header (which waits,
  counted among the first run's waiters, so never let go, for the layout),
  slot 0 (the demuxer's probe), and the slot for the start it will be told
  (`Layout::slot_for_time`: the last slot whose `sidx` label is at or
  before it). It loads the receiver once `rendition_readiness` says
  `ready`, at the start the preparation asked for. The phone shows the
  phase meanwhile, paused at that position. (The same
  investigation found the torrent stopped under the cast once anything else
  opened; that was the reconciler's guess at "playing", replaced by explicit
  holds -- `docs/storage.md`, *Who keeps a torrent running*.)
* **The app** (xtremio) loads `stream.mp4` with the receiver told to start
  at the phone's position, and a seek on the phone is a plain `SEEK` to
  the receiver again; the reload and the undo of the receiver's restarts
  are gone, and with them `ServerHandle::rendition_restarts`.

What a TV hand test of this is, before the app: §5, F2.

## 3. Routes and the contract with the client

**On the LAN listener**, under the same token and the same cut, beside
`/cast/{token}` and nothing else:

| Route | Answers |
|---|---|
| `GET/HEAD /cast/{token}/hls/index.m3u8` | The playlist, `application/vnd.apple.mpegurl`. Written at publish; never waits. |
| `GET/HEAD /cast/{token}/hls/init.mp4` | The init segment, `video/mp4`. Waits for the first run's formats. |
| `GET/HEAD /cast/{token}/hls/{n}.m4s` | Segment `n`, `video/mp4`, ranged by `MediaRange` over the whole segment in memory. `404` past the last; `503` with `{refused, message}` once the rendition has failed. |

A rendition token's `/cast/{token}` is `404`, and a plain token's `/hls/...`
is `404`: a token names one or the other. The LAN CORS layer already
allows `Range` and exposes `Content-Length`/`Content-Range`
(`lan_cors_layer`, `lib.rs:2186`), which a receiver's segment fetches
need. Every response under a rendition token counts as a request; a
segment body counts as a body (`record_body`), so the three-way watchdog
reading of step C holds unchanged.

**`ServerHandle` methods** (a capability is a method, never a route):

| Method | Does |
|---|---|
| `install_producer(Arc<dyn Producer>)` | Once, by the embedder. Without one, `publish_rendition` refuses `noProducer`. |
| `publish_rendition(&MediaId, RenditionSpec, Option<PlayToken>) -> anyhow::Result<CastToken>` | As `publish`: refused while the listener is down; holds the id's lease; writes the playlist. Starts no run: the receiver's first request does. |
| `rendition_state(&CastToken) -> Option<RenditionState>` | `{ producing, segmentsServed, speed, failed: Option<{refused, message}> }`. Cheap, no runtime hop, safe to poll. |
| `prepare_rendition(&CastToken) -> bool` | Starts the first run and makes the receiver's first slot with no request (§2.8, *Prepared before the load*). |
| `rendition_readiness(&CastToken) -> RenditionReadiness` | `index`, `start`, `ready`, `failed{sentence}` or `ended`. Cheap, safe to poll. |
| `unpublish(&CastToken) -> bool` | Unchanged; for a rendition it also drops the run and the ring (2.1). |

`Producer`, `Job`, `SampleSink` and `TrackFormat` are Rust API for the
embedder's crate; FRB never sees them. `RenditionSpec` and `RenditionState`
cross to Dart and are `serde`.

**Dart**: `ReceiverCaps` and the table, the eureka_info fetch,
`CastRendition`, the channel count in the stats poll, the HLS fields on
`CastMedia`, and `rendition_state` in the watchdog.

## 4. What is deleted and what changes

* **`CastRefusal.container`, `.videoCodec` and `.audioCodec` stop being
  refusals.** The enum's own comment names them "the three a remux could
  answer"; they become decisions -- `CastRendition` with a plan -- and
  their sentences ("Casting it would need conversion, which this app
  cannot do yet") go with them. What remains a refusal: a rendition the
  phone cannot make (no decoder for the codec, a file the extractor cannot
  read), said by the producer at the first run rather than guessed up
  front, and `containerPending` while mpv has not reported codecs.
  `unknownContainer` becomes a rendition (the extractor finds out), and
  `proxied` went with step C (an origin that will not range is
  `noRanges` on the server).
* **`_castableVideo` stops being one set for every receiver**: the table
  replaces it, and with it the "leans permissive over HEVC" paragraph,
  which described the bug this fixes.
* `CastMedia` gains the HLS fields; `GoogleCastClient.load` passes them.
* `cast::router()` gains three routes; `Publication` gains an optional
  rendition. Nothing else in the server changes shape.
* **Not deleted**: the as-is path. A stream the receiver can play
  untouched is served as it is, with ranges, exactly as step C serves it;
  a rendition is for the rest.

## 5. Steps, in order, each shippable

Each lands green with its own tests, revert-proven hunk by hunk.

F0. **App: cast by publishing** (step C's app half; M). `_castUrl`'s
   rebuild becomes `publish(id, play)` and `/cast/<token>`; the three-way
   watchdog. Without it nothing casts on `c9cc260`, renditions included.

F1. **Server: the rendition route, the muxer, the ring, the trait, with a
   test producer** (M-L). `Producer`, `Job`, `SampleSink`; the run task
   (cut rule, discard before the cut, lookahead blocking, restart on a
   seek, speed); the fMP4 muxer; the three routes; the handle methods.
   Tests, with a Rust producer on a plain thread that emits synthetic
   samples (a fixed GOP, AAC frames every 21.33 ms): the playlist has
   `ceil(d/T)` entries and `#EXT-X-ENDLIST`; `init.mp4` parses as `ftyp`
   + `moov` with the producer's `avcC` bytes; segment N's `tfdt` is the
   first key at or after N x T, and its samples are exactly those up to
   the next cut; a request for segment 40 after segment 3 starts a new run
   at 40 x T and the old run's sink answers `Stopped`; a request for the
   segment in production does not start a run; with nothing requested the
   producer blocks after L segments (observable: a probe on the sink, never
   a sleep); `unpublish` wakes a request waiting on a segment with an error
   and stops the producer; a producer slower than real time is failed with
   its sentence and `rendition_state` says so; a range of a segment is
   `206` with the right `Content-Range`; no token in any log line. A
   by-hand check, not CI: `ffprobe` and a desktop Shaka player over the
   test producer's output, which proves the boxes decode-side.
   *(Done: `server/src/rendition/` -- `mod.rs` (the types, the
   `Rendition` with its ring and request rules), `run.rs` (the run task
   and the `Cutter`), `mux.rs` (the fMP4 boxes, `avcC`/`hvcC`/`esds`,
   Annex-B to length-prefixed), `speed.rs` (the wait clocks and the
   window); the three routes in `cast.rs`; `ServerHandle::{install_producer,
   publish_rendition, rendition_state}`; `server/tests/renditions.rs` with
   the test producer in `server/tests/support/test_producer.rs`, and the
   rendition token in `log_redaction.rs`. By hand: `ffprobe` over an
   `init.mp4` and six segments concatenated reads an `avc1` H.264 High
   320x240 stream at 90 kHz and an `mp4a` AAC-LC 48 kHz stereo stream,
   156 video packets (6.24 s, keys every 0.48 s, `K` flags where the
   producer set them) and 282 audio packets (6.016 s), and the playlist as
   a 30 s HLS input; its only complaints are about the fake slice payloads.
   No Shaka or Cast receiver has played it. Where the code differs from
   the sections above:*
   * *`rendition_state` answers a `RenditionState` -- `producing`,
     `idle`, `failed {sentence}`, `ended` (not published, or not a
     rendition) -- not an `Option` of a struct with `segmentsServed` and
     `speed`; those were not built.*
   * *A rendition's token also serves the source as it is at
     `/cast/{token}` (the step's brief, over §3's "a token names one or the
     other"); a plain token's `hls/` is `404` as §3 says.*
   * *`TrackFormat` is an enum: `H264 {width, height, csd0, csd1}`,
     `Hevc {width, height, csd0}`, `Aac {sample_rate, channels, csd0}`,
     each `csd` as `MediaFormat` carries it (Annex-B parameter sets, the
     AudioSpecificConfig). A producer reports every track's format before
     its first sample: the formats are taken as they stand then.
     `Producer::start` answers a `ProducerRefusal(sentence)`, which fails
     the rendition with it.*
   * *Audio is assigned on the N x T grid, as 2.1 says; a segment is
     complete once video has passed its cut and audio has reached
     (N+1) x T, or at the source's end. What precedes a run's first cut is
     never emitted: a run's output starts at its own segment. A segment
     with no samples at all (a GOP longer than T with no audio) is a
     `moof` with only its `mfhd`, and an empty `mdat`.*
   * *Clocks: video on 90 kHz, audio on the video's 90 kHz too (its sample
     rate when there is no video), `mvhd` in ms,
     the duration in `mehd`. A segment's decode times are its presentation
     times sorted (2.2's reorder window is the segment). AUDs are dropped
     from samples; a sample with no start code is taken as length-prefixed
     already.*
   * *The lookahead: a run completes at most L segments past the last
     request (or stops at the 96 MiB cap), then stops reading its sink,
     whose channel holds 32 samples; the producer blocks once that fills.
     The ring keeps `[last request - 2, last request + 2]`.*
   * *Speed: media time is measured at the sink as samples are written,
     busy time is the run's wall time less the time the producer spent
     blocked in the sink and in the reader (both timed on the producer's
     thread), checked every tenth of the window. A producer that reads on
     one thread while another blocks in the sink would have overlapping
     waits subtracted twice; Android's producer is to keep reads and
     writes on one thread, or this changes.*
   * *Cut: a request waiting on a segment is woken through the run (its
     stop is a child of the cut, and every run's end wakes the waiters)
     and answered `503` `{refused: "unpublished"}`; a failed rendition is
     `503` `{refused: "renditionFailed", message}`. `init.mp4` and segment
     `GET`s count as bodies; the playlist does not.*
   * *`set_rendition_tuning` and `rendition_probe` (doc-hidden) make the
     release period and the speed window settable and the ring observable
     for the tests; `SampleSink::probe` shows whether the producer is
     blocked in it.*

F1½. **A one-hour spike on zond's phone, before F2 is written**: does
   `MediaExtractor` over a `MediaDataSource` expose DTS and TrueHD audio
   tracks of an MKV, hand out Matroska H.264/HEVC samples in Annex-B, and
   open an AVI at all? Three files, one Kotlin test activity, no server.
   The answer picks the demuxer for F2 (`MediaExtractor`, or libavformat
   from `libmpv.so` reading the same `MediaReader` through
   `avio_alloc_context`) before anything is built on it. (zond, over
   "decide after F2".)
   *(Done 2026-10-01 on zond's phone -- motorola edge 60 pro, Android 16,
   API 36 -- with a throwaway app reading through a `MediaDataSource`.
   The answer is **libavformat**. MediaExtractor's Matroska extractor
   drops every Dolby and DTS audio track ("A_DTS / A_TRUEHD / A_AC3 /
   A_EAC3 is not supported"): a five-track MKV came back as its video
   alone, although the phone HAS Dolby AC3/E-AC3 decoders -- so "a
   decoder exists" and "the extractor exposes the track" are separate
   facts. There is no AVI extractor at all ("Failed to instantiate
   extractor"). H.264/HEVC samples do come out in Annex-B with csd in
   Annex-B (HEVC: csd-0 only, VPS+SPS+PPS+SEI), times are PTS in decode
   order. Decoders on the phone: AC3/E-AC3 (c2.dolby.*), none for DTS or
   TrueHD; encoders for AAC, AVC, HEVC. So F2 demuxes with libavformat
   (Annex-B via the `*_mp4toannexb` bitstream filters, or length-prefixed
   samples the muxer already accepts), MediaCodec stays for decode where a
   decoder exists and for every encode, libavcodec for DTS/TrueHD.)*

F2. **Kotlin producer, repackage only, end to end on the phone** (M). The
   JNI exports (`awaitJob`, `readAt`, `size`, `format`, `sample`, `end`,
   `fail`), the `MediaDataSource`, `MediaExtractor` to the sink, no codec;
   `install_producer` on Android. The decision ships with one rule: an
   H.264/AAC MKV is `Copy`/`Copy`. Proven on zond's TV with an MKV that
   today is refused. This step is where the unverified JNI facts get
   verified (§9), and where the empty-segment question for copied video
   gets a measured answer on real files.

   *(Built in xtremio as a Rust producer over libavformat from the
   vendored libmpv (F1½), not Kotlin over JNI: `rust/src/libav.rs`,
   `rust/src/rendition.rs`. What zond's TV did with it, 2026-10-01, all
   with the default receiver:*
   * *As HLS (fMP4, a media playlist): fetched the playlist, `init.mp4`
     and segment 0 (the 1080p clip), then IDLE/ERROR. On a desktop the
     receiver framework's default Shaka Player (4.15.56;
     `use_shaka_for_hls` defaults to true) failed the same files' first
     append (`MEDIA_SOURCE_OPERATION_FAILED`): handed a bare media playlist
     of muxed fMP4 it types the buffer from the video codec alone. A master
     playlist naming the `CODECS` fixed that on the desktop (stream-server
     `8897914`), and the TV still failed after segment 0.*
   * *zond then bisected on the TV from a desktop with pychromecast:
     every 1080p HLS stream fails there -- ours, ffmpeg's of the same clip
     (fMP4 or TS), Mux's own 1080p variant served alone, the clip at about
     2 Mbit/s, the clip without audio or without B-frames -- and every
     stream of 720p or less plays (Mux's public multi-variant stream, whose
     ABR picks 720p; the clip re-encoded at 720p, B-frames and all). The
     same 1080p clip plays as a progressive MP4, and as one fragmented MP4
     (`frag_keyframe+empty_moov+default_base_moof`) served as `video/mp4`.
     A diagnosis for HLS on this receiver is not needed any more, so none
     was made.*
   * *Hence the amendment at the top: one progressive fragmented MP4 at
     `/cast/{token}/stream.mp4`, HLS removed (and with it `8897914`'s
     master playlist). CORS headers are not needed by a `<video src>`
     stream; the LAN listener sends `Access-Control-Allow-Origin: *` on
     every response anyway, as it always has (`lan_cors_layer`).*
   * *Verified on zond's TV 2026-10-02 (xtremio 244209d): a 1080p H.264 +
     AAC MKV the Chromecast refuses plays as a rendition with sound; the
     receiver shows the full 10:00 from the first byte (`mdhd`); a phone seek
     reloads `?from=` and lands (5:00); a TV-remote seek is undone back to
     where the receiver was (the target is never reported); Stop hands the
     film back to the phone at the receiver's position.*
   * *The same day, by hand (§2.8): a file with a length, exact ranges and
     a `sidx` makes the TV's remote seeks one `Range` each, and they play.
     So the file is laid out before it is made (§2.8), `?from=` and the
     restart count went, and the app hands the receiver `stream.mp4` and
     its seeks back as `SEEK`s. Not on the TV yet as built; the hand test
     is in the xtremio change's report.*
   * *HEVC (2026-10-03): the decision copies HEVC + AAC Matroska too, for
     zond's Chromecast with Google TV 4K (`sabrina`, HEVC Main and Main 10
     up to 4K; a constant until F5's table). The producer hands the
     `hvcC`'s sets and SEI as `csd-0` and its samples in Annex-B (so the
     length-prefix reading of a sample never meets a length of 256 to 511),
     the muxer writes `hvc1` + `hvcC` + `colr` and four-byte lengths, and
     the key flags are the container's -- Matroska's for HEVC are the IRAP
     pictures its cues index, x265's open GOPs making every key after the
     first a CRA. Dolby Vision is not signalled: profiles 7 and 8 go over as
     their base layer with the RPU and enhancement-layer NAL units (62, 63)
     dropped, and profile 5 (no compatible base layer) fails the rendition
     with a sentence. Checked end to end on a desktop (xtremio
     `rust/tests/rendition.rs`); not on the TV yet.*

F3. **Audio to stereo AAC** (M). `MediaCodec` decode when the phone has a
   decoder, libavcodec from `libmpv.so` otherwise (2.6), downmix,
   `MediaCodec` AAC encode, encoder priming spent before the cut. The
   decision's audio rule. Proven with the H.264 + E-AC3 film that plays
   silent on zond's TV today.

F4. **Video transcode to H.264** (L). Surface path, the GL scaling pass,
   forced sync frames at N x T, speed measured on zond's phone at 1080p
   from HEVC. The decision's video rule.

F5. **The receiver table and the decision UI** (M). `ReceiverCaps`, the
   eureka_info fetch, `CastRendition`, the channel count in the stats poll,
   the summary on the remote, `rendition_state` in the watchdog; the three
   refusals deleted (§4). F2-F4 ship with the decision hard-wired for
   zond's receiver; F5 makes it a table.

## 6. Decisions taken here, for zond to overrule

* **Segment length 6 s.** Long enough that a copied video's GOP rarely
  exceeds it (the empty-segment case, §8), short enough that a seek costs
  one GOP of decoding plus six seconds of production before the receiver
  has a segment. 4 s if seeks feel slow in F4; 10 s if F2 finds empty
  segments in real files.
* **fMP4, not MPEG-TS.** HEVC copied to a 4K receiver needs fMP4 in HLS
  (`hvc1`); TS would be simpler for H.264 (no init segment, Annex-B and
  ADTS as they come, independently decodable segments, and the default
  receiver's default segment format). One muxer, so fMP4. A TS writer is
  the fallback if a receiver will not take fMP4.
* **Muxed audio and video in one playlist**, the three routes as zond
  named them. Whether the default receiver's HLS player takes muxed fMP4 is
  **not verified**; the fallback is a master playlist with separate video
  and audio media playlists (`#EXT-X-MEDIA`), two more routes and the
  same muxer writing two track sets. F2 finds out.
* **HLS, not one progressive fMP4.** *(Reversed in F2, measured: see the
  amendment at the top.)* A progressive transcode has no length
  to put in `Content-Length` and no index to seek by time without one; a
  repackage-only progressive MP4 would need the whole source's sample
  table up front, which is a read of the whole file. HLS is the shape where
  "segment N from N's timestamp" is the protocol.
* **Raw `MediaExtractor` + `MediaCodec`, not Transformer** (2.6).
  **Demuxing by `MediaExtractor`, not libmpv's libavformat** -- zond's
  choice, kept, with the alternative open: libavformat in `libmpv.so` is
  exported, reads AVI and every Matroska codec, and would make the demuxer
  the same one that played the film locally. Decide after F2 on real
  files.
* **Audio decode falls back to libavcodec in `libmpv.so`** when the phone
  has no `MediaCodec` decoder for it (2.6). Without it, F3 does not fix
  the case it exists for on most phones.
* **The muxer is in the server, in Rust** (2.2), so the producer hands
  samples and F1 is testable without a phone.
* **Kotlin pulls jobs**; Rust never calls into Java (2.3).
* **The receiver table lives in the app**, keyed by eureka_info's codename,
  unknown models treated as the least capable.
* **The self-signed eureka_info fetch is accepted** for that host and port
  only, believed for the codename only.
* **The first run's codec configuration is frozen** into `init.mp4`, and a
  run that disagrees fails the rendition rather than switch to `avc3`
  in-band parameter sets.
* **An idle run is released after 60 s** and restarted on demand from the
  ring's edge (2.1); zond's call, over the draft's "kept, no timer".
* **Speed excludes the source's waits**: a stalled torrent buffers, a slow
  encoder ends the cast.

## 6½. Two constraints the draft under-stated

* **The libavcodec fallback is bound to one ffmpeg version.** The `av*`
  symbols in `libmpv.so` belong to the ffmpeg the vendored
  `media_kit_libs_android_video` jar was built with; the Rust bindings for
  them must be generated against that exact version (struct layouts change
  between ffmpeg majors), and a bump of the jar re-generates them. A
  desktop's system libmpv links a different ffmpeg, so this path is
  Android-only by construction; a desktop casts as-is or refuses.
* **Production with the screen off needs the foreground service.** Android
  stops an app's threads in the background; the downloads already run
  under a foreground service with a notification (`docs/ANDROID.md`), and a
  rendition's producer runs under the same one for the length of the cast,
  or the receiver stalls the moment the phone sleeps.

## 7. What this does not do

It does not write anything to disk, and it does not keep a rendition: a
second cast of the same film produces it again. It does not transcode for
this device's own player -- mpv decodes what it plays.

It does not **tone-map**. An HDR10 or Dolby Vision source transcoded to an
SDR receiver comes out washed out: the surface path converts 10-bit to
8-bit and does nothing about the transfer function. Such a source to an SDR
receiver is refused with a sentence, until Transformer's tone mapping (or a
GL shader of our own) is worth its cost. A Dolby Vision profile 5 file is
refused for every receiver that is not DV-capable; nothing here converts
it.

It does not carry **subtitles**. A rendition has a video and one audio
track; text in the source is not in the playlist. WebVTT as an HLS
subtitle playlist, or burning in on the transcode path, is later and
separate.

It does not offer a choice of audio tracks on the receiver: the one the
viewer had selected is the one produced. A track change is a new
publication.

It does not cast from **iOS** or a desktop: there is no producer there, and
`publish_rendition` refuses.

It does not make a slow phone fast. A film the phone cannot transcode in
real time is refused after ten seconds of trying, with the measurement in
the sentence.

## 8. Risks

* **Hardware encoder availability and speed.** H.264 encoding at 1080p30
  in real time is ordinary for a phone's hardware encoder; decoding 4K
  HEVC 10-bit while doing it, plus a GL scale, is not guaranteed, and the
  encoder's instances are shared with the camera and with mpv's own
  decoder (which the cast pauses, not releases). A configuration the
  encoder refuses is `sink.fail` with the codec's name. F4 measures zond's
  phone before the decision offers a full transcode of 4K sources.
* **Surface versus byte buffers.** The surface path is the fast one and
  the only one this design builds; it carries the presentation time
  through `releaseOutputBuffer` and `eglPresentationTimeANDROID`, and a
  vendor decoder that misreports timestamps on a surface would break the
  cut rule silently. The byte-buffer path is not a fallback here.
* **A/V sync at a restart.** Video restarts at the keyframe before N x T
  and audio from one AAC frame before it, both discarded up to the cut,
  both stamped from the source's clock -- so a restart cannot drift, but
  an AAC encoder's priming delay that differs from one frame, or a
  decoder that drops its first output, would shift audio by tens of
  milliseconds per seek. F3's test is a lip-sync clip seeked ten times.
* **Copied video and the cut rule.** A source whose GOP is longer than T
  makes a segment with no keyframe in its span: it is empty, and its
  `#EXTINF` still says T. How the default receiver's HLS player treats a
  zero-length segment is not known; F2 measures real files, and the answer
  is a longer T or a playlist built lazily from keyframe times.
* **Codec configuration across runs.** An encoder created afresh on a
  seek may emit SPS/PPS that differ from the first run's (a vendor's
  encoder is not obliged to be byte-identical). That fails the rendition
  (2.1); `avc3` with in-band parameter sets is the fix if it happens.
* **The receiver's buffering against on-demand production.** A receiver
  that buffers tens of seconds ahead asks for segments faster than real
  time at the start and after every seek; the lookahead of L = 2 means it
  waits on production for each one. That is the design working, but a
  receiver whose segment request times out before a segment is produced
  (a cold start of a full transcode can be seconds) retries; the retry
  joins the production, which is why the rule is "join, never restart".
  The receiver's own timeout is not known here.
* **Two readers on one entity.** While a rendition is cast, mpv's reader
  over the same id is still open (the cast pauses local playback rather
  than stopping it), and the producer opens a second reader with the same
  play token: two consumers of one file, each with a read-ahead window the
  retention owner keeps, the paused one stale. Both are bounded by the
  budget and the play session does not move (same token, same screen),
  but a phone short of cache spends half its window on a reader nobody is
  reading. Better: the app closes mpv's stream when a rendition starts and
  reopens at the receiver's position when the cast ends.
* **Scattered reads.** `MediaExtractor` over an MP4 whose audio and video
  chunks are far apart reads back and forth, and every `readAt` off the
  reader's position is a seek, which reopens the source (pipeline 2.4): on
  a torrent, a new file handle and a re-prioritised swarm per hop. Matroska
  is interleaved and reads forward; a non-interleaved MP4 is rare but
  costly. A small read-back window in the JNI `readAt` is the fix if F2
  sees it.
* **A screen that turns off.** A cast runs while the phone sleeps. A
  rendition is CPU and codec work on top of serving bytes; whether Android
  keeps the producer's threads running with the screen off without a
  foreground service and a wake lock is not verified (the downloads
  service is the precedent the app has).

## 9. Verified, and not

**Verified in the tree or the build artifacts**: `cast::router` and
`Publication` (`cast.rs:106`, `:194`); `CastBody` polls the cut first
(`cast.rs:337`); `Registry::open_reader`, `open_source` and `lease`
(`media/registry.rs:552-575`); `MediaReader`'s blocking calls refuse a
runtime thread (`media/reader.rs:233`) and its `Canceller`;
`MediaRange` (`routes/util.rs:63`); `log_path` elides `/cast/...`
including an `/hls/` path (`routes/util.rs:173`, test at `:406`);
`lan_cors_layer` (`lib.rs:2186`); `publish`/`unpublish` and
`block_on_server` (`lib.rs:1408`, `:1420`, `:1445` -- §8 of the pipeline
note still says `:1097`); no trait-object hook exists in `ServerConfig`
today; the server crate is an `rlib`. In xtremio: `CastCompatibility` and
`CastRefusal` as quoted; `CastDevice.model` from `modelName`; `CastMedia`'s
four fields and `load`'s; `PlaybackStats.mpvProperties` without a channel
count; `PlaybackTrack.channels` per track; `_castUrl` rebuilding on the LAN
base and no `publish` anywhere in `lib/` or `rust/src/`; no `androidx.media3`
and no `MediaCodec` code in the app; `jni` 0.22.4 in the lock (via
`rustls-platform-verifier`); no JNI symbol in the crate; Flutter's default
`minSdkVersion` 24. flutter_chrome_cast 1.4.8 passes `hlsSegmentFormat`,
`hlsVideoSegmentFormat` and the duration to the Android SDK. The vendored
libmpv: encoders and muxers disabled, decoders, demuxers, bsfs and
swresample enabled, and the `av*` API exported (arm64 jar only). `android.jar`
(API 36): `MediaMuxer`'s five output formats and its path/fd constructors,
`MediaExtractor.setDataSource(MediaDataSource)`, `SEEK_TO_PREVIOUS_SYNC`,
`MediaDataSource`'s `getSize`/`readAt`, and `MediaFormat`'s MIME constants
for AC3, E-AC3, DTS and TrueHD. **Release `strip = true` keeps exports**:
the release `libxtremio_core.so` (arm64, built 2026-09-29) has no
`.symtab` and still exports FRB's 25 `#[no_mangle]` functions in
`.dynsym`, so a `#[no_mangle]` JNI export survives the same way.

**Not verified**: that `System.loadLibrary("xtremio_core")` from Kotlin
answers the instance Dart already `dlopen`ed (standard linker behaviour in
one classloader namespace; untried); that `MediaExtractor`'s Matroska
extractor exposes DTS and TrueHD tracks and that `MediaExtractor` reads
AVI; that `MediaExtractor` hands H.264/HEVC samples from Matroska in
Annex-B form; which phones carry AC3/E-AC3/DTS decoders (zond's in
particular); whether the default receiver takes muxed fMP4 HLS, and what
it does with a zero-length segment or a segment request that takes
seconds; the receiver table's contents (Google's page, not copied here);
the eureka_info endpoint and its codenames (zond's finding); Media3
Transformer's API and size (from its documentation); mpv's
`current-tracks/audio/demux-channel-count` on the vendored build;
production speeds on any phone; screen-off behaviour.
