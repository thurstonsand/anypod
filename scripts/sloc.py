#!/usr/bin/env python3
"""Print repository SLOC totals, with and without tests."""

import json
from pathlib import Path
import subprocess
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
INCLUDE_DIRS = ("src", "scripts", "alembic", "docker", "docs", "tests")


def run_pygount(paths: list[Path]) -> dict[str, Any]:
    """Run pygount and return the JSON output."""
    cmd = ["uvx", "pygount", "--format=json", "--folders-to-skip", "__pycache__"]
    cmd.extend(str(p) for p in paths)
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def extract_totals(data: dict[str, Any]) -> tuple[int, int]:
    """Extract total and Python code counts from pygount JSON output."""
    total = data.get("summary", {}).get("totalCodeCount", 0)
    python = next(
        (
            lang.get("codeCount", 0)
            for lang in data.get("languages", [])
            if lang.get("language") == "Python"
        ),
        0,
    )
    return total, python


def main() -> None:
    """Main function to run the script."""
    all_paths = [REPO_ROOT / d for d in INCLUDE_DIRS]
    no_tests_paths = [p for p in all_paths if p.name != "tests"]

    all_data = run_pygount(all_paths)
    no_tests_data = run_pygount(no_tests_paths)

    all_total, all_python = extract_totals(all_data)
    no_tests_total, no_tests_python = extract_totals(no_tests_data)

    print(
        f"""
Lines of code (including tests): {all_total}
Lines of code (excluding tests): {no_tests_total}

Python-only (including tests): {all_python}
Python-only (excluding tests): {no_tests_python}
    """.strip()
    )


if __name__ == "__main__":
    main()
