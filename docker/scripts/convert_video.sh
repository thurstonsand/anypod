#!/bin/bash -e
# Video conversion script for anypod
# Converts non-optimal codecs to h.265/AAC for efficient podcast delivery
# Usage: convert_video.sh <video_file>

FILENAME="$1"

# Exit if file doesn't exist
if [ ! -f "$FILENAME" ]; then
    echo "Error: File not found: $FILENAME" >&2
    exit 1
fi

# Check video and audio codecs
VIDEO_CODEC=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$FILENAME" 2>/dev/null)
AUDIO_CODEC=$(ffprobe -v quiet -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "$FILENAME" 2>/dev/null)
EXTENSION="${FILENAME##*.}" # Get file extension (container format)
EXTENSION=$(echo "$EXTENSION" | tr '[:upper:]' '[:lower:]')  # Convert to lowercase

# Determine what conversions are needed
CONVERT_VIDEO=false
CONVERT_AUDIO=false
CONVERT_CONTAINER=false

# Check if video needs conversion (skip if already h265/hevc/av1)
if [[ "$VIDEO_CODEC" != "hevc" && "$VIDEO_CODEC" != "h265" && "$VIDEO_CODEC" != "av1" ]]; then
    CONVERT_VIDEO=true
    echo "Video codec is $VIDEO_CODEC, will convert to H.265" >&2
else
    echo "Video codec is $VIDEO_CODEC - no video conversion needed" >&2
fi

# Check if audio needs conversion (skip if already AAC)
if [[ "$AUDIO_CODEC" != "aac" ]]; then
    CONVERT_AUDIO=true
    echo "Audio codec is $AUDIO_CODEC, will convert to AAC" >&2
else
    echo "Audio codec is $AUDIO_CODEC - no audio conversion needed" >&2
fi

# Check if container needs conversion to MP4
if [[ "$EXTENSION" != "mp4" ]]; then
    CONVERT_CONTAINER=true
    echo "Container is $EXTENSION, will convert to MP4" >&2
else
    echo "Container is $EXTENSION - no container conversion needed" >&2
fi

# Perform conversion if needed
if [[ "$CONVERT_VIDEO" == true || "$CONVERT_AUDIO" == true || "$CONVERT_CONTAINER" == true ]]; then
    # Determine output filename
    BASE_NAME="${FILENAME%.*}"
    TEMP_FILE="${BASE_NAME}.converting.mp4"

    # Build ffmpeg command based on what needs conversion
    VIDEO_ARGS="-c:v copy"
    AUDIO_ARGS="-c:a copy"

    if [[ "$CONVERT_VIDEO" == true ]]; then
        VIDEO_ARGS="-c:v libx265 -preset medium -crf 28 -tag:v hvc1"
    fi

    if [[ "$CONVERT_AUDIO" == true ]]; then
        AUDIO_ARGS="-c:a aac_at -q:a 9"
    fi

    # Run ffmpeg with determined arguments
    ffmpeg -hide_banner -loglevel error -i "$FILENAME" \
        $VIDEO_ARGS \
        $AUDIO_ARGS \
        "$TEMP_FILE"

    # Check if conversion was successful
    if [ $? -eq 0 ] && [ -f "$TEMP_FILE" ]; then
        # Replace original with converted file
        FINAL_FILE="${BASE_NAME}.mp4"
        mv "$TEMP_FILE" "$FINAL_FILE"

        # Remove original file if it has a different extension
        if [[ "$FINAL_FILE" != "$FILENAME" ]]; then
            rm -f "$FILENAME"
        fi

        echo "Conversion successful: $FINAL_FILE" >&2
    else
        echo "Conversion failed!" >&2
        rm -f "$TEMP_FILE"
        exit 1
    fi
else
    echo "No conversion needed, keeping original file" >&2
fi