import asyncio
import contextlib

from .cli import main_cli


def main() -> None:
    """Entry point for the anypod CLI application."""
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main_cli())


if __name__ == "__main__":
    main()
