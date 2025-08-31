# POT Provider Integration (bgutil for yt-dlp)

This document captures the plan to integrate the BgUtils Proof-of-Origin Token (POT) provider with Anypod.

Goals:
- Enable yt-dlp to use the `bgutil` POT provider when configured.
- Default to fully disabling POT fetching when not configured.
- Keep runtime (binary) and test (pip) environments functional and discoverable.
- Make Docker and docker-compose usage straightforward.

## Background

- yt-dlp supports “PO Tokens” for YouTube via provider plugins. The `bgutil-ytdlp-pot-provider` adds providers `bgutil:http` and `bgutil:script`.
- yt-dlp loads plugins automatically from system/user plugin directories and supports zipped plugin packages.
- We use the yt-dlp binary in production containers, and pip-installed yt-dlp in integration tests.

Key references (HIGHLY RECOMMENDED to read these into context before starting):
- yt-dlp fetch_pot: @docs/references/yt-dlp/yt-dlp-README.md#L1821
- yt-dlp plugins docs: @docs/references/yt-dlp/yt-dlp-README.md#L1919-1975
- bg-pot-provider docs: @docs/references/yt-dlp/POT-provider/bgutils-ytdlp-pot-provider-README.md

## Configuration

- Add top-level setting `pot_provider_url: str | None` (env: `POT_PROVIDER_URL`).
  - When unset or empty: yt-dlp is forced to never fetch POT — inject `--extractor-args "youtube:fetch_pot=never"`.
  - When set: yt-dlp is configured to use the HTTP provider — inject `--extractor-args "youtubepot-bgutilhttp:base_url=<pot_provider_url>"`.
  - If a user already passes conflicting extractor args for the same targets in `yt_args`, skip our injection to avoid duplication.

Rationale: This provides an explicit off-switch and a single knob to enable the provider without per-feed changes.

Files:
- src/anypod/config/config.py
- README.md
- example_feeds.yaml

## Code Changes

1) (COMPLETE) `AppSettings` (src/anypod/config/config.py)
- Add field: `pot_provider_url: str | None = Field(default=None, validation_alias="POT_PROVIDER_URL", description="URL for bgutil POT provider HTTP server (e.g., http://bgutil-provider:4416)")`.

2) `YtdlpWrapper` injection
- Update constructor to accept `pot_provider_url: str | None` and store it.
- In all methods where `YtdlpArgs` is composed (`fetch_playlist_metadata`, `download_feed_thumbnail`, `fetch_new_downloads_metadata`, `download_media_to_file`), inject extractor args according to the configuration:
  - If `pot_provider_url` is None/empty: append `--extractor-args` for `youtube:fetch_pot=never`.
  - Else: append `--extractor-args` for `youtubepot-bgutilhttp:base_url=<url>`.
- Guard: If `user_yt_cli_args` already includes `--extractor-args` for `youtube:` or `youtubepot-...`, do not inject ours.

3) Call sites
- Pass `settings.pot_provider_url` into the `YtdlpWrapper` when constructed in CLI modes (default, debug_ytdlp, debug_enqueuer, debug_downloader).

Files:
- src/anypod/ytdlp_wrapper/ytdlp_wrapper.py
- src/anypod/cli/default.py
- src/anypod/cli/debug_ytdlp.py
- src/anypod/cli/debug_enqueuer.py
- src/anypod/cli/debug_downloader.py
- src/anypod/ytdlp_wrapper/core/args.py (used for composing args; no structural change expected)

## Docker Image (binary runtime)

- Install the plugin zip directly into a system plugin directory so yt-dlp auto-loads it:
  - Create directory `/etc/yt-dlp/plugins/`.
  - Download latest (or pinned) release zip and place it at `/etc/yt-dlp/plugins/bgutil-ytdlp-pot-provider.zip`.
- Continue installing yt-dlp binary (existing). Optionally, we can later pass explicit versions via build-args.

Notes:
- yt-dlp discovers zipped plugin packages containing a root `yt_dlp_plugins/` directory — no unzip required.
- This does not enable the provider by itself; usage still depends on `pot_provider_url` and extractor args injection.

Files:
- Dockerfile

## docker-compose

Add a `bgutil-provider` service and wire Anypod to it via env:

```yaml
services:
  anypod:
    image: ghcr.io/thurstonsand/anypod:nightly
    environment:
      POT_PROVIDER_URL: http://bgutil-provider:4416
    depends_on:
      - bgutil-provider

  bgutil-provider:
    image: brainicism/bgutil-ytdlp-pot-provider:latest
    container_name: bgutil-provider
    restart: unless-stopped
    ports:
      - "4416:4416"  # optional for host access
```

Operational guidance:
- If using a local proxy, you may need `--net=host` on the provider container as per upstream docs.
- If tokens stop working, users can add to feed `yt_args`: `--extractor-args "youtubepot-bgutilhttp:disable_innertube=1"`.

Files:
- docker-compose.yaml

## Integration Tests (pip runtime)

- Add dev dependency: `bgutil-ytdlp-pot-provider` so that pip-installed yt-dlp in tests can discover the plugin via Python site-packages.
- Tests can opt-in by setting `POT_PROVIDER_URL` in test env to use the provider; otherwise POT is disabled by default via `fetch_pot=never` injection.

Files:
- pyproject.toml
- .github/workflows/update-ytdlp-commit.yml (keeps pip yt-dlp fresh)

## Optional: Nightly Version Refresh

We may add a scheduled GitHub workflow to keep the runtime image fresh:
- Daily job fetches latest yt-dlp + bgutil plugin release tags via GitHub API.
- Compares to currently published image labels.
- If newer, rebuild and push (no-cache) with build-args to bake accurate labels and versions.

This is orthogonal to function; the existing `update-ytdlp-commit.yml` continues to keep pip tests up-to-date.

Files:
- .github/workflows/docker-publish.yml (optional: pass build-args/labels)
- .github/workflows/runtime-refresh.yml (new; scheduled job to rebuild when versions change)

## Risks & Considerations

- Plugin loading: All plugins found are imported by yt-dlp. We only install the single provider zip under `/etc/yt-dlp/plugins/`.
- Backward compatibility: With `pot_provider_url` unset, behavior is explicit (POT disabled) and safe for existing deployments.
