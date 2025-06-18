#!/usr/bin/env python3
"""Script to create Cursor .mdc rules symlinks from CLAUDE.md files."""

from pathlib import Path
import sys


def main() -> int:
    """Create .mdc symlinks in .cursor/rules/ for CLAUDE.md files."""
    repo_root = Path(__file__).parent.parent
    cursor_rules_dir = repo_root / ".cursor" / "rules"

    # Ensure .cursor/rules directory exists
    cursor_rules_dir.mkdir(parents=True, exist_ok=True)

    # Find all CLAUDE.md files
    claude_files = list(repo_root.glob("**/CLAUDE.md"))

    if not claude_files:
        print("No CLAUDE.md files found", file=sys.stderr)
        return 1

    for claude_file in claude_files:
        # Determine the appropriate .mdc filename and glob pattern
        if claude_file.parent == repo_root:
            # Top-level CLAUDE.md -> applies to all files
            mdc_file = cursor_rules_dir / "claude-project-rules.mdc"
            globs = "*"
            always_apply = True
            description = "Project-wide rules from top-level CLAUDE.md"
        else:
            # Other directories -> apply to that folder only
            folder_name = claude_file.parent.name
            mdc_file = cursor_rules_dir / f"claude-{folder_name}-rules.mdc"
            relative_path = claude_file.parent.relative_to(repo_root)
            globs = f"{relative_path}/**/*"
            always_apply = False
            description = (
                f"Rules for {folder_name} folder from {relative_path}/CLAUDE.md"
            )

        # Remove existing .mdc file if it exists
        if mdc_file.exists() or mdc_file.is_symlink():
            mdc_file.unlink()

        # Create the .mdc file with frontmatter and CLAUDE.md content
        try:
            # Create a wrapper file that includes frontmatter and references the CLAUDE.md
            frontmatter = f"""---
description: {description}
globs: {globs}
alwaysApply: {str(always_apply).lower()}
---

"""

            # Read the CLAUDE.md content
            claude_content = claude_file.read_text(encoding="utf-8")

            # Write the combined content to the .mdc file
            mdc_file.write_text(frontmatter + claude_content, encoding="utf-8")

            print(f"Created Cursor rule: {mdc_file.name} (globs: {globs})")
        except Exception as e:
            print(f"Error creating Cursor rule {mdc_file}: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
