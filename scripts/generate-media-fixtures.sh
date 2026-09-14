#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/.." && pwd)"
asset_dir="$repo_dir/crates/app/assets"

command -v ffmpeg >/dev/null || { echo "ffmpeg is required" >&2; exit 1; }
command -v cwebp >/dev/null || { echo "cwebp is required" >&2; exit 1; }
mkdir -p "$asset_dir"

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'testsrc2=size=640x360:rate=1' -frames:v 1 \
  -pix_fmt yuv420p -map_metadata -1 "$asset_dir/fixture.png"
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'testsrc2=size=640x360:rate=1' -frames:v 1 \
  -q:v 3 -map_metadata -1 "$asset_dir/fixture.jpg"
cwebp -quiet -q 80 "$asset_dir/fixture.png" -o "$asset_dir/fixture.webp"

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'testsrc2=size=640x360:rate=24' -t 3 -an \
  -c:v libx264 -preset medium -crf 28 -pix_fmt yuv420p \
  -movflags +faststart -map_metadata -1 "$asset_dir/fixture.mp4"
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'testsrc2=size=640x360:rate=24' -t 3 -an \
  -c:v libvpx-vp9 -b:v 350k -pix_fmt yuv420p -row-mt 1 \
  -map_metadata -1 "$asset_dir/fixture.webm"

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'sine=frequency=440:sample_rate=8000:duration=3' \
  -ac 1 -c:a pcm_s16le -map_metadata -1 "$asset_dir/fixture.wav"
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i 'sine=frequency=440:sample_rate=8000:duration=3' \
  -ac 1 -c:a libmp3lame -b:a 32k -map_metadata -1 "$asset_dir/fixture.mp3"

file "$asset_dir"/fixture.*
