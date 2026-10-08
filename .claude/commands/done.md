---
description: Stage 5 — close a reviewed step, settle findings, and update CLAUDE.md
argument-hint: <NN>
---

# /done — Stage 5: Done

Arguments: `$ARGUMENTS`

## Flow

1. **Confirm before closing:** plan is `status: reviewed` and spec is
   `approved`; verification recorded green; no High finding unresolved; every
   important Medium fixed, accepted or deliberately deferred.
2. **Verify only if needed.** If no code changed since the last green full
   suite, don't re-run. Otherwise run the local syntax check and ask the user
   to re-run focused tests + the full suite in Databricks. If anything fails,
   stop and recommend `/build NN retest`.
3. **Settle open findings.** Ask the user once, listing them; label each:
   High must be resolved; Medium fixed/accepted/deferred; Low accepted by
   default.
4. **Update `CLAUDE.md` with lasting knowledge only:** structural decisions
   with reasons (tables per layer, keys, write modes), important conventions,
   genuinely useful known limitations, and a "Steps landed" line
   `NN — <title>: <one-sentence summary>` with the correct test count. No
   cosmetic findings, no file lists.
5. **Set the spec and plan to `status: done`.**
6. If git exists, suggest a commit message (`NN: <title>`) but **don't
   commit** unless asked.

## Final report

End with exactly this and nothing after it:

```
File:   .claude/specs/NN-slug.md, .claude/plans/NN-slug.md, CLAUDE.md
Mode:   close
Status: done  (tests <passed>/<total>)
Next:   /spec <NN+1> <slug> "<description>"
```
