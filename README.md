# AnyPod

## Development

run with `uv run anypod --config-file example_feeds.yaml`

## Gotchas

- if you unskip a download, it may immediately get archived based on your retention rules
- you cannot use the following settings as part of your ytdlp args; they will be overridden by the application:
  - ...TODO
- in order to get cookies, I have successfully followed these instructions:
  - [How to pass cookies to yt-dlp](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp)
  - [Error 429: Too many requests](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#http-error-429-too-many-requests-or-402-payment-required)
  - a couple comments:
    - specifically, I've used the [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc) Chrome extension to retrieve them in a file
    - if you are on Windows, watch out for the newlines. The Docker container will expect `LF`, and Windows might default to `CRLF`
    - for Youtube cookies, you need to [follow special instructions](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies). but this should only be needed if you are trying to access private playlists, age-restricted videos, or members-only content
    - if downloading from Patreon behind a paywall, it can help help to [simply add `--referer https://www.patreon.com`](https://github.com/yt-dlp/yt-dlp/issues/13263#issuecomment-2903954393) as well to prevent HTTP 403's
- I would not test out feeds using PocketCasts, since they permanently cache on their end. So if you need to modify anything, PocketCasts will not pick up that change -- you'd need to change the feed id to make it pick up a "new" feed. Apple Podcasts is a safe bet
- if passing in a youtube video that is part of a playlist (example: "https://www.youtube.com/watch?v=aqz-KE-bpKQ&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU", note the `&list=`), yt-dlp will treat this as if it is just the playlist, not the individual video. Typically, I would recommend replacing this with the playlist url ("https://youtube.com/playlist?list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU&si=kpqBVoLcbAWiCVaO"), but it will act as a playlist regardless of which link is used