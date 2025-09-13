#!/usr/bin/env python3
"""Script to create AGENTS.md symlinks to CLAUDE.md files."""

from pathlib import Path
import sys


def main() -> int:
    """Create AGENTS.md symlinks wherever CLAUDE.md exists."""
    repo_root = Path(__file__).parent.parent

    # Find all CLAUDE.md files
    claude_files = list(repo_root.glob("**/CLAUDE.md"))

    if not claude_files:
        print("No CLAUDE.md files found", file=sys.stderr)
        return 1

    for claude_file in claude_files:
        agents_file = claude_file.parent / "AGENTS.md"

        # Remove existing AGENTS.md if it exists
        if agents_file.exists() or agents_file.is_symlink():
            agents_file.unlink()

        # Create symlink from AGENTS.md to CLAUDE.md
        try:
            agents_file.symlink_to("CLAUDE.md")
            print(f"Created symlink: {agents_file} -> CLAUDE.md")
        except OSError as e:
            print(f"Error creating symlink {agents_file}: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
