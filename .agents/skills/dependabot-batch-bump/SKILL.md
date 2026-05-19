---
name: dependabot-batch-bump
description: Consolidate Dependabot PRs by applying a blanket uv dependency refresh on main, verifying GitHub Actions, and closing stale Dependabot PRs.
---

# Dependabot Batch Bump

## What This Skill Delivers

Consolidates open Dependabot dependency PRs into one direct commit on `main`, verifies CI, and watches the Dependabot PRs close or handles stale duplicates.

Use this skill when Dependabot has opened a batch of dependency PRs and the maintainer wants one blanket dependency refresh instead of merging each PR.

## Workflow

### 1. Inspect repository state

Confirm the worktree is clean and identify open Dependabot PRs:

```bash
git status --short --branch
gh pr list --author app/dependabot --state open \
  --json number,title,headRefName,url,updatedAt,baseRefName --limit 50
```

If the worktree has unrelated changes, stop and ask before proceeding.

### 2. Apply the blanket uv refresh

Run a full resolver upgrade from the repository root:

```bash
uv lock --upgrade
```

Inspect the diff. If Dependabot PRs modify direct constraints in `pyproject.toml`, raise those lower bounds too so the manifest records the intended minimum versions. Then refresh the lockfile again:

```bash
uv lock
```

Do not cherry-pick individual Dependabot branches. The point is one consolidated main-branch update.

### 3. Validate locally

Run the repository’s full local gate:

```bash
uv run pre-commit run --all-files
```

Fix failures before committing.

### 4. Commit and push to main

Use a conventional dependency-maintenance commit. Include the local verification in the body.

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): update dependencies" \
  -m "Why: Dependabot opened multiple dependency PRs. Consolidating them keeps maintenance linear." \
  -m "Approach: Ran uv lock --upgrade and adjusted direct lower bounds for the Dependabot-targeted packages." \
  -m "Verified: Ran uv run pre-commit run --all-files."
git push origin main
```

### 5. Monitor GitHub Actions

Find the run for the pushed commit and wait for it to finish:

```bash
HEAD_SHA=$(git rev-parse HEAD)
gh run list --branch main --json databaseId,headSha,status,conclusion,workflowName,url --limit 20
gh run watch <run-id> --exit-status
```

If Actions fail, inspect the failed job logs, fix the issue on `main`, run local validation, push, and monitor the replacement run.

### 6. Watch Dependabot PR cleanup

After Actions passes, watch open Dependabot PRs for up to 10 minutes:

```bash
gh pr list --author app/dependabot --state all \
  --json number,title,state,headRefName,updatedAt,closedAt,url --limit 50
```

Most PRs should auto-close once GitHub sees that `main` already contains their update.

If a PR remains open but its diff is already superseded by `main`, close it with a factual comment:

```bash
gh pr close <number> --comment \
  "Closing as superseded by <commit>, which applies this dependency update on main as part of a consolidated dependency refresh."
```

Before manually closing, verify the PR is stale or duplicate. Do not close a Dependabot PR that still contains a real update not present on `main`.

### 7. Report outcome

Report:

- commit SHA pushed to `main`
- local validation command and result
- GitHub Actions run result and URL
- Dependabot PRs that auto-closed
- any PRs manually closed as superseded
