---
description: Plan and implement a feature (PRD workflow)
---

## Context

- Read project guardrails and architecture first:
  - @DESIGN_DOC.md
  - @README.md

## Your task

Follow a Gather → Plan → Act workflow to deliver a focused feature.

1) Gather all context necessary for implementation
- Inspect relevant code paths for the feature area
- Understand prior art, existing code patterns, structures, and organization
- Identify caveats, missing information, assumptions

2) Plan
- Draft a short design in `docs/designs/` named `NN-<feature>.md` with:
  - existing designs (you don't have to read them, but know they are there): !`ls docs/designs/`
  - Problem/goal, assumptions, constraints
  - Impacted modules and minimal changes
  - Step-by-step implementation plan and test plan
    - For each actionable step/code change, use a md task: `- [ ]`
- Stop here and request review/approval before coding.

3) Act (after approval)
- Implement only the approved scope. Keep changes small and cohesive.
- Add/adjust unit tests as you implement each feature; don't move on to a new feature until the existing one has test coverage
  - Make sure to run the new/adjusted tests to ensure they pass
- Check off each item as you complete them: `- [x]`
- Update docs/examples if behavior or configuration changed.

5) Handoff
- Summarize changes, notable decisions, and next steps.

### Deliverables

- Design doc in `docs/designs/NN-<feature>.md` (gather + plan)
- Focused implementation and tests (act)
- Passing lint, type check, and tests (validate)
- Brief summary with file references (handoff)

### Notes

- Keep the surface area minimal; align with existing code patterns in the code base
- Ask for clarification if the scope is ambiguous.

## Feature Details

$ARGUMENTS
