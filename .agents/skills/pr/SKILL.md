---
name: pr
description: Create an AnyPod pull request from the current branch with full branch analysis, user-reviewed PR copy, remote push handling, and gh CLI publishing.
---

# PR Review Helper

## What This Skill Delivers

Creates a pull request from the current branch to `main` after analyzing all branch changes and getting user approval for the PR description.

Use this skill when the user asks to create, open, or publish a pull request.

## Workflow

### 1. Gather git context

Collect the full branch context before drafting anything:

```bash
git status
git diff main...HEAD
git branch --show-current
git log main..HEAD
git status -b --porcelain | head -1
git rev-parse --abbrev-ref @{upstream} 2>/dev/null || echo "No upstream tracking"
```

If the branch is `main`, stop. Pull requests must come from a feature branch.

### 2. Analyze the whole branch

Review all commits and the full diff since divergence from `main`. Do not summarize only the latest commit.

If the user provided additional notes, incorporate them into the PR description where they add useful context.

### 3. Draft the PR description

Write the body to `pr-description.md` for review before creating the PR:

```markdown
## Summary

- <main change>
- <supporting change>
- <important behavior or workflow impact>

## Test plan

- [ ] <verification command or manual check>
```

Use a clear PR title in the file or state it alongside the draft.

### 4. Get user approval

Show the proposed title and `pr-description.md` content. Wait for the user to approve or request edits.

Do not create the PR before approval. Humans dislike surprise publication. This is one of their more reasonable traits.

### 5. Push the branch

Ensure the branch has upstream tracking:

```bash
git push -u origin $(git branch --show-current)
```

If the branch already tracks a remote, a normal `git push` is acceptable.

### 6. Create the PR

Use the reviewed file as the body:

```bash
gh pr create --base main --title "<reviewed title>" --body-file pr-description.md
```

After GitHub returns the PR URL, delete the temporary description file:

```bash
rm pr-description.md
```

### 7. Report the result

Return the PR URL and note any validation or checks already run.
