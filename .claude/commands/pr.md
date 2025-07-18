---
description: Create a pull request from the current branch
---

## Context

- Current git status: !`git status`
- Current git diff (all changes since diverging from main): !`git diff main...HEAD`
- Current branch: !`git branch --show-current`
- Recent commits on this branch: !`git log --oneline main..HEAD`
- Remote tracking status: !`git status -b --porcelain | head -1`
- Check if current branch tracks a remote: !`git rev-parse --abbrev-ref @{upstream} 2>/dev/null || echo "No upstream tracking"`

## Your task

Based on the above changes, create a pull request from the current branch to main.

- Analyze ALL commits that will be included in the pull request (not just the latest commit)
- Draft a comprehensive PR summary that covers the full scope of changes
- Ensure the current branch is pushed to remote with proper upstream tracking
- Create the PR using `gh pr create` with proper title and body formatting
- Use a HEREDOC for the PR body to ensure correct formatting
- Additional notes (if any): $ARGUMENTS