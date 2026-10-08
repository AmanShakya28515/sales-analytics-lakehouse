---
description: Stage 3 — Build & Verify an approved plan (implement, write tests, run verification)
argument-hint: <NN>  |  <NN> resume  |  <NN> retest
---

# /build — Stage 3: Build & Verify

Arguments: `$ARGUMENTS`

Read `CLAUDE.md`, the approved spec and the approved plan for NN first.

## Modes

- **`<NN>` (implement):** plan must be `status: approved`. Run the full flow.
- **`<NN> resume`:** plan is `approved` and some steps are ticked. Continue
  from the first unticked step.
- **`<NN> retest`:** plan is `built`, `approved` or `reviewed`, and named
  findings or failures need fixing. Fix only those, re-run verification for
  the changed area (and the full suite when appropriate), set the plan back
  to `built`.

If the status doesn't match the mode, stop and name the right command.

## Flow

1. **Implement only what the approved plan allows.** Work through
   "Implementation steps" in order and tick each (`- [x]`).
   - If the **technical** approach must change, stop and recommend
     `/plan NN revise "…"`.
   - If **product behaviour** would change, stop and recommend reopening the
     spec (`/spec NN revise "…"`).
2. **Tables and schema:** if tables change, put the DDL in the versioned,
   safe-to-re-run setup code (CLAUDE.md → Conventions). The user runs it in
   Databricks.
3. **Write or update tests yourself:** derived from the spec's ACs, not the
   implementation; each names its AC; behaviour-focused; never weaken an
   existing test unless the spec deliberately changes that behaviour (say so).
4. **Verify in CLAUDE.md's standard order:** run the local syntax check
   yourself. Then stop once and give the user a concise Databricks checklist:
   the setup notebook if needed, focused tests, the full suite, and the
   plan's manual checks, with the exact result to report for each. Resume
   when the user reports back.
5. **If a test exposes a pipeline bug:** fix it within the plan; don't
   weaken the test; if the fix goes beyond the plan, stop and report.
   Environment failures (cluster, permissions, Free Edition limits): stop
   and report.
6. **Keep user-reported results apart from what you measured yourself.**
7. **Record concisely in the plan:** "Files changed", important decisions
   and deviations, "Test results" (commands, pass/fail counts, who ran them,
   unresolved).
8. **Set `status: built`** only when verification is green or the user has
   accepted the failures.

## Optional sub-agents

`test-author` / `test-runner` only when independent test design or
verification materially improves correctness (tricky dedup/merge logic,
late or changed records, access rules). Never invoke them automatically.

## Token efficiency

Don't send large copies of CLAUDE.md/spec/plan to sub-agents. Keep reports
short when checks pass. Don't repeat passed checks unless affected code
changed.

## Final report

End with exactly this and nothing after it:

```
File:   .claude/plans/NN-slug.md
Mode:   implement | resume | retest
Status: built | approved (blocked: <reason>)
Tests:  <passed>/<total> (user-reported); syntax: ok|issues
Next:   <exact command>
```

If built: `/review NN` (or `/done NN` after a retest needing no further
review); if blocked: the exact fix command.
