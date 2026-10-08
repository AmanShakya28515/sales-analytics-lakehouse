---
description: Stage 4 — one concise review of a built step; log findings by severity
argument-hint: <NN>  |  <NN> recheck
---

# /review — Stage 4: Review

Arguments: `$ARGUMENTS`

Read `CLAUDE.md`, the approved spec and the plan for NN first.

## Modes

- **`<NN>`:** plan must be `status: built`. One concise review.
- **`<NN> recheck`:** plan is `built` and already has a review log entry.
  Re-review **only** the fixed findings and their files. Only after a High or
  important Medium fix, never for Lows alone.

## Flow

1. **Scope:** the plan's "Files changed" list, plus `git diff` if git exists.
2. **Review against the spec, the plan and CLAUDE.md**, mainly for:
   1. Correctness — ACs really implemented; no regressions or wrong edges
      (nulls, duplicates, join fan-out, wrong aggregation grain).
   2. Security and access control where relevant — Unity Catalog grants,
      SQL built from parameters, secrets or PII printed or stored.
   3. Data integrity — idempotent writes, keys, MERGE conditions, schema
      drift, silent row loss.
   4. Performance hazards that matter — `collect()`/`toPandas()`, driver
      loops, unbounded cross joins.
   5. Architecture/maintainability — only issues that materially affect
      future work (e.g. hard-coded catalog names blocking Jobs later).
   Accepted trade-offs in CLAUDE.md are not findings.
3. **Rank findings High / Medium / Low**, each with `file:line`, evidence and
   a plain-language fix. No long lists of cosmetic Lows.
4. **Log the review** as a dated entry under "Review log" in the plan, IDs
   continuing the existing numbering (`S-n` security, `Q-n` other), label
   `open`.
5. **Don't fix anything.** The user decides (via `/build NN retest`).
6. **Set `status: reviewed`** when no High finding is open; otherwise leave
   `built`.

## Optional specialist reviewers

`security-reviewer` only for grants/permissions/secrets/external sources/
PII/dynamic SQL; `quality-reviewer` only for substantial architecture
changes or risky refactors. Summarise only their actionable findings.

## Final report

Print the ranked findings table (or "No findings"), then end with exactly:

```
File:   .claude/plans/NN-slug.md
Mode:   review | recheck
Status: reviewed | built (<n> High open)
Found:  High <n> · Medium <n> · Low <n>
Next:   <exact command>
```

If reviewed: `/done NN` (or `/build NN retest` when recommending an
important Medium fix); if High open: `/build NN retest` once the user says
which to fix.
