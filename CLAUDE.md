# CLAUDE.md

Guidance for Claude Code in this repository. This file records **why**
decisions were made, not just what they are. If a rule here looks wrong, raise
it with the user. Do not quietly work around it, and do not reopen a settled
choice in a spec or plan unless the user asks.

---

## What this project is

**Sales Analytics Lakehouse** is an end-to-end Databricks data engineering
project. It takes raw sales data (customers, products, orders, order items)
and processes it through Bronze, Silver and Gold layers using PySpark, Spark
SQL, Delta Lake and Unity Catalog. The result is clean, analytics-ready
datasets that can later feed dashboards and automated workflows.

Goals, in this order:
1. Learn core Databricks and data engineering concepts by building the
   project step by step.
2. Turn the project into a more production-like pipeline using Jobs,
   Workflows, Git, testing, monitoring, and later CI/CD or Databricks Asset
   Bundles.

**Why the order matters:** the Databricks fundamentals and the pipeline flow
need to be clearly understood first. Production engineering complexity is
added only after that, each piece as its own deliberate step.

There is **no web app and no frontend**. Do not create Django, Flask,
templates, React, Tailwind or any UI unless a later spec asks for it.

---

## Common commands

Pipeline code and tests run **inside Databricks** (Free Edition, serverless
compute), not on this machine. Claude cannot run them directly. When a step
needs a Databricks run, Claude gives the user a short checklist and records
the results the user reports, kept separate from anything Claude measured
itself.

The local `env/` virtualenv (Python 3.12) is used **only** for local syntax
checks. Always call `env/Scripts/python.exe` explicitly, never bare `python`.
`env/` also contains Django from earlier experiments. Django is not a project
dependency, so do not import it, and leave `env/` alone.

| Purpose | Command |
|---|---|
| Dev server | n/a (run notebooks in the Databricks workspace) |
| Focused tests | In Databricks: run `tests/run_tests` with `pattern` = `test_<module>.py` (user runs it and reports back) |
| Full test suite | In Databricks: run `tests/run_tests` with the default `pattern` (user runs it and reports back) |
| Migration drift | n/a (no ORM; table DDL is versioned in code, see Conventions) |
| System checks | `env/Scripts/python.exe -m compileall -q src tests` (local syntax check) |
| Apply migrations | n/a (setup/DDL notebooks run in Databricks) |
| Install deps | n/a (use what the Databricks runtime provides) |

**Standard verification order:** local syntax check, then focused tests,
then the full suite. Cheapest, most specific feedback first. Claude runs the
syntax check and the user runs the Databricks steps. (Skip any step marked
n/a.)

---

## Feature workflow

Every change goes through five stages: **SPEC → PLAN → BUILD & VERIFY →
REVIEW → DONE**. A **gate** is an explicit human approval. Claude never
approves its own spec or plan, and never moves past a gate on its own.

| # | Stage | Command | Output | Gate to leave the stage |
|---|---|---|---|---|
| 1 | Spec | `/spec` | `.claude/specs/NN-slug.md` | User runs `/spec NN approve` |
| 2 | Plan | `/plan` | `.claude/plans/NN-slug.md` | User runs `/plan NN approve` |
| 3 | Build & Verify | `/build` | Code, table DDL, tests, and verification in the standard order | Everything green, or the user accepts the failures |
| 4 | Review | `/review` | One concise review (correctness, security/access control, data integrity, important maintainability), logged in the plan | User decides which findings to fix |
| 5 | Done | `/done` | Spec and plan marked done; CLAUDE.md architecture and history updated | — |

**Status lifecycle** (front-matter `status:`):
- Spec: `draft` → `approved` → `done`
- Plan: `draft` → `approved` → `built` → `reviewed` → `done`

Each command checks the status it needs first and refuses with a clear
message if it is wrong.

**Numbering:** two-digit, sequential (`01`, `02`, …). Spec and plan share the
same `NN-slug` filename.

**Why this workflow:**
- Specs hold *what* and *why*; plans hold *how*. Keeping them apart stops
  implementation details being debated before the goal is agreed.
- Acceptance criteria are numbered (`AC-1`, …) so tests, plan steps and
  review findings can point to them. "Done" becomes checkable.
- Tests are derived from the **spec**, not the code; tests written from code
  repeat the code's bugs.
- Small numbered steps map cleanly onto future Jobs/Workflows tasks and CI
  pipeline runs.

**Finding "the diff":** the project is not a git repository yet. The plan's
"Files changed" section, kept up to date by /build, is the official list of
what a step touched.

### Token-efficient execution
Optimise for correctness, learning value and progress per token.
- **Specs stay concise:** goal and context; user-visible behaviour; numbered
  acceptance criteria; out of scope; only open questions that matter. Point
  to CLAUDE.md instead of restating it.
- **Plans stay concise:** key technical decisions; real files affected;
  table/schema changes; meaningful risks; test and verification approach.
  Don't re-explain settled architecture.
- Reuse established patterns rather than rediscovering them.
- Don't paste large parts of CLAUDE.md, a spec or a plan into agent prompts.
- Prefer one implementation-and-verification pass and one concise review.
- No review → fix → recheck cycles for minor clean-up.
- Token efficiency means less repetition, not less correctness. Never change
  architecture just to save tokens.

### Sub-agents
Optional tools, not workflow stages. Use one only when an independent
context really improves correctness.
- **`test-author`:** complex ACs (deduplication, late or changed records,
  joins, aggregations) where independent test design adds value.
- **`test-runner`:** only when independent verification matters.
- **`security-reviewer`:** changes touching Unity Catalog grants and
  permissions, secrets, external data sources, PII, or dynamic SQL built
  from parameters.
- **`quality-reviewer`:** substantial architecture changes, major refactors,
  technically risky work.

### Review findings
- **High:** normally fixed before the step closes.
- **Medium:** fixed when it materially affects correctness, security, access
  control, data integrity, architecture or maintainability.
- **Low:** accepted by default unless it affects one of the above.

Never reopen a build or start another review cycle only for cosmetic
clean-up, wording, duplicated test data, minor comments or optional
refactors. Low findings that may matter later go into Known limitations.

---

## Stack constraints

### Use:
- **Python 3.12 / PySpark and Spark SQL on the Databricks runtime
  (serverless, Free Edition).** The project is about learning Databricks, so
  the platform's own engine does the work.
- **Delta Lake tables in Unity Catalog**, using three-level names
  (`catalog.schema.table`). These are the project's database. Do not use
  SQLite or PostgreSQL. Write portable Spark/Delta code. Do not depend on
  paid or workspace-specific features that Free Edition lacks unless a spec
  asks for them.
- **Medallion layers:** Bronze holds raw data as ingested, plus ingestion
  metadata. Silver holds cleaned, typed and deduplicated data. Gold holds
  business-level aggregates ready for analytics.
- **Raw input as CSV/JSON/Parquet files** (sample data in a Unity Catalog
  volume), loaded into Bronze Delta tables.
- **Databricks notebooks** (source-format `.py` files, so they diff cleanly)
  and Spark SQL. No frontend. Databricks SQL dashboards come later, through
  their own spec.
- **Python `unittest` (standard library)** for tests, run inside Databricks
  against a real SparkSession. It needs no extra dependency.
- **No dependency file yet.** Add `requirements.txt` only when an approved
  plan adds a dependency.

### Do NOT add without being asked:
- Django, Django REST Framework, Flask, FastAPI
- React, Vue, Angular, npm or any build tooling
- Celery, Redis
- PostgreSQL/MySQL client libraries
- Extra authentication packages
- Airflow, Kafka, dbt, MLflow
- pandas (unless specifically needed)
- Third-party Spark/Databricks libraries
- Any cloud SDK (AWS, Azure, GCP)
- Any new Python dependency the approved plan does not require

Prefer built-in Databricks and Spark functionality first.

**Why so strict:** every dependency should be a deliberate, approved step
with its own spec. If a feature seems to need one, say so in the spec's
"Open questions" and let the user decide.

### Accepted trade-offs (for now)
This is a learning project. Local or sample data and Databricks Free Edition
limitations are acceptable. Until a later step deliberately introduces them,
reviewers must not raise findings about missing enterprise-scale security,
CI/CD, streaming, secret management or cloud storage integration.

---

## Architecture

*`/done` adds to this as each step lands. Record only what was actually
built, plus the reasoning behind any structural choice.*

(empty)

### Known limitations (deliberately deferred)
(none yet)

### Steps landed
(none yet)

---

## Testing

- `/build` writes tests directly; use `test-author` only when independent
  test design genuinely helps.
- **Tests come from the approved spec**, not the implementation. Every
  important AC gets coverage, and the test names its AC (`AC-3` in a comment
  or docstring).
- Test behaviour: given small in-memory input DataFrames, assert the output
  rows, schema and counts. Avoid tests that inspect source code or depend on
  physical plans.
- Tests live in `tests/` (`tests/test_<module>.py`) and are run by the
  `tests/run_tests` notebook.
- **Test data quality, not just the happy path:** for every transformation,
  cover nulls in key columns, duplicates, malformed or unexpected values,
  empty input, and idempotency (a re-run does not duplicate rows).
- Tests must not depend on order, on production tables, or on the network.
  Use small in-memory DataFrames, or isolated test tables that the test
  creates and drops itself.
- When a test exposes a pipeline bug, fix the pipeline within the plan's
  scope. Never weaken the test to make the suite pass.

---

## Frontend and UI work
None for now. Do not build any UI. If a later spec adds dashboards, use
Databricks SQL dashboards unless that spec says otherwise.

---

## Conventions

- Keep transformation logic in plain functions (DataFrame in, DataFrame out)
  under `src/`, so it can be tested. Notebooks orchestrate: they read
  parameters, call the functions and write tables.
- Do not hard-code the catalog or schema. Pass them as parameters (notebook
  widgets or job parameters), so dev and test runs never touch each other's
  tables.
- Writes must be idempotent. Use `MERGE` or a deliberate overwrite, so a
  re-run produces the same result instead of duplicating rows.
- Declare schemas explicitly for Bronze ingestion. Do not rely on schema
  inference beyond exploration.
- Table DDL and schema changes live in versioned setup code that is safe to
  re-run (`CREATE ... IF NOT EXISTS`, explicit `ALTER`). Never edit the
  schema of an existing table by hand.
- Never build SQL by string formatting with untrusted values. Validate
  identifiers taken from parameters, and use parameter markers or the
  DataFrame API for values.
- Avoid `collect()`, `toPandas()` and driver-side loops over data in
  pipeline code.
- Access in Unity Catalog is granted to groups, scoped to the narrowest
  schema or table needed.
- Keep each change within its plan. If something outside the plan needs to
  change, stop and say so.
- Never commit secrets or tokens. Read them from Databricks secrets or the
  environment, and never print them.
