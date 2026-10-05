#!/usr/bin/env bash
# build.sh <ffmpeg tag> [...]: a minimal static libavformat of each tag (the
# MP4 demuxer and little else) under $LAVF_HARNESS_DIR/inst-<tag>, and the
# probe linked against it at $LAVF_HARNESS_DIR/ad-<tag>.
#
#   tools/lavf-harness/build.sh n4.4 n6.1 master
#
# n4.4 is what a Chromecast with Google TV runs (Chrome 92's FFmpeg is of
# that age: a fragment's first decode time from its sidx label, no
# use_tfdt); 6.1 is what Ubuntu 24.04's ffprobe is; master is where it is
# going. $LAVF_HARNESS_DIR defaults to ~/.cache/lavf-harness and holds a
# bare clone of FFmpeg (about 400 MB) and a build tree per tag.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir="${LAVF_HARNESS_DIR:-$HOME/.cache/lavf-harness}"
mkdir -p "$dir"
if [ ! -d "$dir/ffsrc.git" ]; then
  git clone --bare https://git.ffmpeg.org/ffmpeg.git "$dir/ffsrc.git"
fi
for tag in "$@"; do
  inst="$dir/inst-$tag"
  if [ ! -f "$inst/lib/libavformat.a" ]; then
    src="$dir/src-$tag"
    rm -rf "$src" && mkdir -p "$src"
    git -C "$dir/ffsrc.git" archive "$tag" | tar x -C "$src"
    (cd "$src" && ./configure --prefix="$inst" --disable-everything \
        --disable-programs --disable-doc --disable-asm --disable-autodetect \
        --enable-demuxer=mov --enable-protocol=file,http,tcp --enable-network \
        --enable-parser=h264,aac,hevc,ac3 --enable-bsf=null > configure.log 2>&1 \
      && make -j"$(nproc)" > make.log 2>&1 && make install > install.log 2>&1)
    rm -rf "$src"
  fi
  cc -O1 -o "$dir/ad-$tag" "$here/av_dump.c" -I"$inst/include" -L"$inst/lib" \
    -lavformat -lavcodec -lavutil -lm -lpthread
  echo "built $dir/ad-$tag"
done
