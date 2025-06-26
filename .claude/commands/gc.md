---
description: Create a git commit
---

## Context

- Current git status: !`git status`
- Current git diff (staged changes only): !`git diff --cached`
- Current branch: !`git branch --show-current`
- Recent commits: !`git log --oneline -10`
- `pre-commit` hooks will trigger on commit. If they error, the commit will fail; address any issues and try the commit again
- Do not stage any additional files (ignore anything unstaged)

## Your task

Based on the above changes, create a single git commit.

- Unless the commit specifically and only concerns testing, keep comments about test files concise, at most to one line
- Primarily adhere to the changes that are actually present in this commit -- don't overly reference changes that have happened in other commits already
- Additional notes (if any): $ARGUMENTS