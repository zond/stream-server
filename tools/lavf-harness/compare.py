#!/usr/bin/env python3
"""compare.py <film> <rendition.mp4> [--versions n4.4,n6.1,master]
              [--seeks 300,100,43] [--shift-us 0] [--ahead-us 500000]

Reads a rendition with each FFmpeg version's probe (build.sh) the way
Chrome's FFmpegDemuxer does -- its own 32 KiB reader, av_seek_frame on the
video stream, backward -- and holds it against the film it was made from
(read with the system ffprobe):

  straight   every packet of picture and of sound is the film's at its time
             (to 12 us), decode times strictly increase, and the reader is
             never asked to seek (a seek past what a receiver holds is a
             new request there);
  each seek  after reading 16 s: where the picture lands against the key of
             the slot the time falls in (slots begin --ahead-us before their
             key is shown), then 3 s of both tracks against the film's.

--shift-us is what the producer adds to every time: xtremio's shifts a
Matroska film by its sound's priming (21 ms for AAC at 48 kHz).
One line a version; anything but "ok", "+0.00" and no reader seek is a
finding.
"""
import argparse, bisect, os, subprocess, sys

ap = argparse.ArgumentParser()
ap.add_argument("film"); ap.add_argument("rendition")
ap.add_argument("--versions", default="n4.4,n6.1,master")
ap.add_argument("--seeks", default="300,100,250,330,43,30,301,5")
ap.add_argument("--shift-us", type=int, default=0)
ap.add_argument("--ahead-us", type=int, default=500_000)
ap.add_argument("--dir", default=os.environ.get("LAVF_HARNESS_DIR", os.path.expanduser("~/.cache/lavf-harness")))
a = ap.parse_args()

def film_packets():
    out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "packet=stream_index,pts_time,flags",
                          "-of", "csv=p=0", a.film], capture_output=True, text=True).stdout
    tracks, keys = {0: [], 1: []}, []
    for line in out.split("\n"):
        p = line.split(",")
        if len(p) < 3 or p[1] in ("", "N/A"): continue
        s, t = int(p[0]), round(float(p[1]) * 1e6) + a.shift_us
        if s == 1 and t < 0: continue  # the sound's priming: not carried
        if s not in tracks: continue
        tracks[s].append(t)
        if s == 0 and "K" in p[2]: keys.append(t)
    return tracks, sorted(keys)

def run_ok(got, film):
    """`got` is a run of `film`, in order, from wherever it starts."""
    if not got: return "empty"
    try: i = next(j for j, t in enumerate(film) if abs(t - got[0]) <= 12)
    except StopIteration: return "start %d not in film" % got[0]
    for k, t in enumerate(got):
        if i + k >= len(film) or abs(film[i + k] - t) > 12:
            return "packet %d: %d vs film %d" % (k, t, film[i + k] if i + k < len(film) else -1)
    return "ok"

film, keys = film_packets()
bad = False
for v in a.versions.split(","):
    probe = os.path.join(a.dir, "ad-" + v)
    ran = subprocess.run([probe, a.rendition, "0", "-", "0"], capture_output=True, text=True)
    out, jumps = ran.stdout, sum(1 for line in ran.stderr.split("\n") if line.startswith("J "))
    read, dts = {0: [], 1: []}, []
    for line in out.split("\n"):
        p = line.split()
        if len(p) == 5 and int(p[1]) in read:
            read[int(p[1])].append(round(float(p[2]) * 1e6))
            if p[1] == "0": dts.append(int(p[4]))
    back = sum(1 for x, y in zip(dts, dts[1:]) if y <= x)
    whole = [run_ok(read[s], film[s]) if len(read[s]) == len(film[s]) else "n%d/%d" % (len(read[s]), len(film[s])) for s in (0, 1)]
    out = subprocess.run([probe, a.rendition, "16", a.seeks, "3"], capture_output=True, text=True).stdout
    results, cur = [], None
    def close():
        if cur is None: return
        at, landed, after = cur
        key = keys[bisect.bisect_right(keys, round(at * 1e6) + a.ahead_us) - 1]
        results.append("%g:%+.2f/%s/%s" % (at, (landed - key) / 1e6, run_ok(after[0], film[0]), run_ok(after[1], film[1])))
    for line in out.split("\n"):
        p = line.split()
        if not p: continue
        if p[0] == "L":
            close(); cur = (float(p[1]), round(float(p[2]) * 1e6), {0: [], 1: []})
        elif p[0] == "S" and cur and int(p[1]) in cur[2]:
            cur[2][int(p[1])].append(round(float(p[2]) * 1e6))
    close()
    line = "%-7s straight: decode steps <= 0: %d  reader seeks: %d  picture: %s  sound: %s | seeks: %s" % (v, back, jumps, whole[0], whole[1], " ".join(results))
    print(line, flush=True)
    bad = bad or back or jumps or whole != ["ok", "ok"] or any("+0.00/ok/ok" not in r for r in results)
sys.exit(1 if bad else 0)
