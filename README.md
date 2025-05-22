# AnyPod

## Development

run with `uv run anypod --config-file example_feeds.yaml`

## Gotchas

- if you unskip a download, it may immediately get archived based on your retention rules
- you cannot use the following settings as part of your ytdlp args; they will be overridden by the application:
  - ...TODO