---
description: Stage 2 — draft, revise, or approve the implementation plan for an approved spec
argument-hint: <NN>  |  <NN> revise "<feedback>"  |  <NN> approve
---

# /plan — Stage 2: Implementation plan

Arguments: `$ARGUMENTS`

Read `CLAUDE.md` and `.claude/specs/NN-*.md` first. The plan says **how**.
It must follow CLAUDE.md's stack constraints and conventions and must not
re-argue them.

## Modes

### `draft` — `<NN>`
1. The spec must be `status: approved`; otherwise stop and point to
   `/spec NN approve`.
2. No plan may exist yet for NN; if one does, suggest `revise`.
3. Read the existing code relevant to the step so the plan fits what is
   actually there.
4. Write `.claude/plans/NN-slug.md` (same slug) with `status: draft`.
5. Any new dependency or CLAUDE.md-breaking change goes under "Decisions
   needing approval", never straight into the steps.

### `revise` — `<NN> revise "<feedback>"`
Only while `status: draft`. Apply the feedback; keep the AC mapping complete.

### `approve` — `<NN> approve`
Only the user runs this mode. Never run it on your own initiative.
1. Check `status: draft`, every spec AC appears in the test plan, and every
   "Decisions needing approval" item has an answer.
2. Set `status: approved` and `approved: <today's date>`.

## Plan template

```markdown
---
step: NN
slug: <slug>
spec: .claude/specs/NN-slug.md
status: draft
created: <YYYY-MM-DD>
approved:
---

# Plan NN — <Title>

## Approach
Short design summary, and why this rather than the obvious alternative.

## Tables & schema changes
Unity Catalog tables/volumes created or changed (layer, columns, keys,
write mode: append / overwrite / MERGE); setup DDL expected.

## Notebooks, functions, jobs
| Notebook / function | Input | Output table | Parameters |

## Implementation steps
- [ ] 1. …
- [ ] 2. …

## Test plan
| AC | Test module | What it asserts |
Every spec AC has a row, including data-quality and re-run cases.

## Manual Databricks checks
What the user runs in the workspace and what result to report back.

## Risks
Data-loss, duplication, schema and access risks, plus what is done about each.

## Decisions needing approval
- (none)

## Files changed
_(filled in by /build)_

## Test results
_(filled in by /build)_

## Review log
_(filled in by /review)_
```

## Final report

End with exactly this and nothing after it:

```
File:   .claude/plans/NN-slug.md
Mode:   draft | revise | approve
Status: <new status>  (<one-line note>)
Next:   <exact command>
```

After draft or revise: `/plan NN approve` (or `/plan NN revise "…"`); after
approve: `/build NN`.
