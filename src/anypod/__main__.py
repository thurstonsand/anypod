import asyncio

from .cli import main_cli


def main() -> None:
    """Entry point for the anypod CLI application."""
    asyncio.run(main_cli())


if __name__ == "__main__":
    main()
