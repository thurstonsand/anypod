# Media Clip Streaming: Research & Learnings

This document captures technical insights discovered while implementing media clip streaming for Anypod.

## Table of Contents

1. [YouTube's Approach](#youtubes-approach)
2. [FFmpeg Seeking Behavior](#ffmpeg-seeking-behavior)
3. [Streaming Without Disk I/O](#streaming-without-disk-io)
4. [Container Format Considerations](#container-format-considerations)
5. [Stream Copy vs Transcode](#stream-copy-vs-transcode)
6. [Implementation Trade-offs](#implementation-trade-offs)

---

## YouTube's Approach

YouTube handles clip sharing through **URL parameters**, not server-side processing:

- **`t=` parameter**: Seeks to a specific timestamp (e.g., `?t=6m48s` or `?t=408`)
- **`start=` and `end=` parameters**: Work on embed URLs only (`/embed/VIDEO_ID?start=30&end=60`)
- **Key insight**: YouTube's player handles the start/end client-side; the server still sends the full video

This means YouTube doesn't actually extract clips server-side—they rely on:
1. The player seeking to the start position
2. The player stopping at the end position
3. Range requests to avoid downloading the entire file

For a podcast/download use case, this approach doesn't work because:
- Podcast players don't understand these parameters
- Users want to download/share the actual clip file
- We need server-side extraction

**Sources**: [YouTube Player Parameters](https://developers.google.com/youtube/player_parameters), [YouTube URL Structures](https://bbaovanc.com/blog/youtube-url-structures-you-should-know/)

---

## FFmpeg Seeking Behavior

### Position of `-ss` Matters (But Less Than It Used To)

| Placement | Speed | Accuracy | Notes |
|-----------|-------|----------|-------|
| `-ss` before `-i` with `-c copy` | Fastest | Keyframe-only | Can be ±1-3 seconds off |
| `-ss` before `-i` (transcoding) | Fast | Frame-accurate | Since FFmpeg 2.1+ |
| `-ss` after `-i` | Slow | Frame-accurate | Decodes from start |
| Combined (before AND after `-i`) | Medium | Frame-accurate | Best of both worlds |

### The Combined Seeking Trick

```bash
ffmpeg -ss 00:02:30 -i input.mp4 -ss 00:00:10 -t 00:01:00 -c copy output.mp4
```

This first seeks to 2:30 by keyframe (fast), then seeks forward 10 seconds precisely.

### Keyframe Intervals

Default x264 GOP (Group of Pictures) size is 250 frames ≈ 10 seconds at 25fps. This means with `-c copy`, cuts can be off by up to 10 seconds!

### Frame-Accurate Requires Transcoding

There's no way around this: if you need frame-exact cuts, you must re-encode. The `-c copy` option only copies complete GOPs.

**Sources**: [FFmpeg Seeking Wiki](https://fftrac-bg.ffmpeg.org/wiki/Seeking), [FFmpeg ss/t/to Explanation](https://usercomp.com/news/1315331/ffmpeg-ss-t-to-streamcopy-and-keyframe-explanation)

---

## Streaming Without Disk I/O

### The Pipe Protocol

FFmpeg can read from and write to pipes:
- `pipe:0` = stdin
- `pipe:1` = stdout
- `pipe:2` = stderr

### MP4 Container Problem

**Critical discovery**: Standard MP4 files are NOT streamable!

MP4 stores the `moov` atom (metadata) at the end of the file by default. This means:
- The muxer needs to seek back to write final metadata
- Pipes don't support seeking
- Naive `ffmpeg ... -f mp4 pipe:1` will **fail**

### The Solution: Fragmented MP4

Use `movflags` to create a "fragmented" MP4 that's streamable:

```bash
ffmpeg -i input.mp4 -ss 10 -t 30 \
  -movflags "frag_keyframe+empty_moov+default_base_moof" \
  -f mp4 pipe:1
```

**What each flag does**:
- `frag_keyframe`: Create a new fragment at each keyframe
- `empty_moov`: Put an empty `moov` atom at the start (no seeking needed)
- `default_base_moof`: Modern flag for better fragment parsing

### Seeking Within Fragmented MP4

**Caveat**: Fragmented MP4 doesn't support seeking within the stream. This is fine for our use case (serving a clip), but it means:
- Progress bars won't work in some players until fully downloaded
- Range requests within the clip aren't meaningful

**Sources**: [MDN MSE Transcoding](https://developer.mozilla.org/en-US/docs/Web/API/Media_Source_Extensions_API/Transcoding_assets_for_MSE), [Piping MP4 from FFmpeg](https://www.jaburjak.cz/posts/ffmpeg-pipe-mp4/)

---

## Container Format Considerations

### Audio Formats

| Source Format | Best Clip Container | Notes |
|---------------|---------------------|-------|
| AAC (from MP4) | `.m4a` or `.mp4` | Stream copy works perfectly |
| MP3 | `.mp3` | Stream copy works, but frame boundaries matter |
| Opus (from WebM) | `.opus` or `.ogg` | May need transcoding for wide compatibility |
| Vorbis | `.ogg` | Stream copy usually works |

### Video Formats

| Source Format | Clip Container | Notes |
|---------------|----------------|-------|
| H.264 (MP4) | Fragmented MP4 | Needs `movflags` for streaming |
| VP9 (WebM) | WebM | Simpler container, streams natively |
| AV1 | MP4 or WebM | Newer, check player support |

### The M4A Special Case

When extracting AAC audio from MP4 video:
- The audio stream is already AAC
- Using `-c:a copy` preserves quality perfectly
- Output should be `.m4a` (same container family)

**Sources**: [Mux Audio Extraction Guide](https://www.mux.com/articles/extract-audio-from-a-video-file-with-ffmpeg)

---

## Stream Copy vs Transcode

### Stream Copy (`-c copy`)

**Pros**:
- Extremely fast (just copying bytes)
- Zero quality loss
- Low CPU usage

**Cons**:
- Cuts only at keyframes (can be seconds off)
- Output format must match input format
- Can't apply filters

### Transcode

**Pros**:
- Frame-accurate cuts
- Can change format/codec
- Can apply filters (normalize audio, etc.)

**Cons**:
- CPU-intensive
- Quality loss (re-encoding)
- Slower

### Quality Loss Reality

Each generation of lossy encoding degrades quality. For a single transcode:
- **AAC → AAC**: Usually imperceptible at 128kbps+
- **MP3 → MP3**: More noticeable, avoid if possible
- **Video**: Depends heavily on bitrate, can be significant

### Our Approach Decision

For clips, we'll transcode because:
1. Frame-accurate start/end is important for sharing
2. A single transcode at reasonable quality is acceptable
3. Stream copy would require complex logic to explain keyframe limitations

**Sources**: [FFmpeg Documentation](https://www.ffmpeg.org/ffmpeg.html), [Cloudinary FFmpeg Copy Guide](https://cloudinary.com/guides/front-end-development/ffmpeg-copy-video)

---

## Implementation Trade-offs

### URL Parameter Design

Considered approaches:
1. `?clip=10-60` (compact but less readable)
2. `?start=10&end=60` (YouTube-style, explicit)
3. `?t=10&duration=50` (start + length)

Chose **option 2** (`start`/`end`) because:
- Familiar from YouTube
- Self-documenting
- Easy to mentally calculate clip length

### Time Format

Accepting multiple formats:
- Seconds: `?start=90&end=150`
- MM:SS: `?start=1:30&end=2:30`
- HH:MM:SS: `?start=0:01:30&end=0:02:30`

### Streaming Architecture

```
Request → Validate params → Spawn FFmpeg → Stream stdout → Response
```

Key considerations:
1. **Process cleanup**: Must kill FFmpeg if client disconnects
2. **Timeout**: Set reasonable limits to prevent resource exhaustion
3. **Error handling**: FFmpeg errors go to stderr, need to capture

### Content-Length Header

**Problem**: We can't know the output size before FFmpeg finishes.

**Solutions**:
1. Omit `Content-Length` entirely (chunked encoding)
2. Estimate based on bitrate × duration
3. Two-pass (unacceptable latency)

We use **option 1**: Modern HTTP clients handle chunked encoding well, and it's the only honest approach for live transcoding.

### Caching

Clip URLs are unique (different start/end = different content), so:
- Can't reuse existing file caches
- Could theoretically cache clips, but storage cost likely exceeds benefit
- Set short cache headers since regeneration is cheap

---

## Edge Cases Discovered

1. **Clips longer than source**: FFmpeg handles gracefully, outputs until EOF
2. **Start after end of file**: FFmpeg outputs nothing (0-byte response)
3. **Negative times**: FFmpeg treats as 0
4. **Very short clips (<1s)**: May not contain any keyframes, blank output with `-c copy`
5. **Audio-only sources**: Work perfectly, simpler pipeline
6. **VBR audio**: Duration estimation less accurate

---

## Future Considerations

### Range Requests for Clips

Currently not supported because fragmented MP4 streams can't be seeked. Options:
1. Accept the limitation
2. Generate clip to temp file, serve with range support, delete after
3. Implement HTTP Live Streaming (HLS) for clips

### Clip URL Signing

For public deployments, consider:
- Time-limited clip URLs (expires parameter)
- HMAC signatures to prevent abuse
- Rate limiting per IP

### Quality Presets

Could offer quality tiers:
- `quality=high`: Higher bitrate, slower
- `quality=fast`: Lower bitrate, instant
