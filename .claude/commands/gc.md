---
description: Create a git commit
---

## Context

- Current git status: !`git status`
- Current git diff (staged changes only): !`git diff --cached`
- Current branch: !`git branch --show-current`
- Recent commits: !`git log --oneline -10`
- !`uv run pre-commit run --all-files || true`
- !`uv run pre-commit run --all-files || true`
  (run a second time to see if first time auto-fixed everything)
- Do not stage any additional files (ignore anything unstaged)

## Your task

Based on the above changes, check to make sure that documentation is up-to-date and then create a single git commit.

- Read the entire contents of @README.md, @DESIGN_DOC.md, @CLAUDE.md, and @tests/CLAUDE.md, ensure they reflect the changes present in this commit
  - keep updates at the same level of abstraction as what is currently present in these files -- do NOT add unnecessary or out-of-scope details
  - after any updates, make sure to (selectively) `git add` those files
- Unless the commit specifically and only concerns testing, keep comments about test files concise, at most to one line
- Primarily adhere to the changes that are actually present in this commit -- don't overly reference changes that have happened in other commits already
- Additional notes (if any): $ARGUMENTS