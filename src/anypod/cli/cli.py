import argparse
import pathlib

from .debug_ytdlp import run_debug_ytdlp_mode
from .default import default as run_default_mode

# __file__ is src/anypod/cli/cli.py
# parents[0] is src/anypod/cli
# parents[1] is src/anypod
# parents[2] is src
# parents[3] is the workspace root /Users/thurstonsand/Develop/anypod
DEBUG_YAML_PATH = pathlib.Path(__file__).resolve().parents[3] / "debug.yaml"


def main_cli():
    parser = argparse.ArgumentParser(description="Anypod CLI tool.", allow_abbrev=False)
    parser.add_argument(
        "--debug-ytdlp",
        action="store_true",
        help="Run in yt-dlp debug mode, using a debug.yaml configuration file in the workspace root directory.",
    )
    # Add other application-wide arguments here if needed in the future

    args, _ = parser.parse_known_args()

    if args.debug_ytdlp:
        print(f"Running in yt-dlp debug mode with config: {DEBUG_YAML_PATH}")
        run_debug_ytdlp_mode(DEBUG_YAML_PATH)
    else:
        run_default_mode()
