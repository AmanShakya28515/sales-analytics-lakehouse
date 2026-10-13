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

**Finding "the diff":** the project is a git repository on GitHub
(`AmanShakya28515/sales-analytics-lakehouse`, branch `main`). The plan's
"Files changed" section, kept up to date by /build, is the official list of
what a step touched. Use `git diff <commit before the step>` alongside it.

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

- **Unity Catalog layout:** one catalog (parameter, default `sales_lakehouse`;
  Free Edition allowed creating it) with one schema per layer per environment:
  `<env>_bronze`, `<env>_silver`, `<env>_gold` (`dev`, `test`). Raw files live in
  the managed volume `<env>_bronze.raw_data`, one folder per dataset
  (`raw_data/<dataset>/<file>`). *Why:* volumes belong to schemas, and
  per-environment schemas keep dev and test fully separate.
- **Setup is code, not hand edits:** `setup/00` (catalog objects, all
  `IF NOT EXISTS`), `01` (copy sample files: empty each dataset folder, then
  copy), `02` (verify). If the catalog cannot be created, setup stops and
  tells the user to re-run with `catalog=workspace`. There is no automatic fallback,
  so the target catalog is always explicit.
- **Identifiers:** catalog and env are validated (`[A-Za-z0-9_]+`, lowercased)
  in `sales_lakehouse.naming` before any SQL, then backtick-quoted. Reuse
  `layer_names()` and `quote()` for every new name built from parameters.
- **Raw data contract:** `sales_lakehouse.raw_datasets` is the single registry
  (columns, PK/FKs, format, expected rows, `KNOWN_ISSUES`). Raw reads use an
  explicit **all-STRING** schema, so malformed values survive into Bronze;
  intended types are applied in Silver (see `docs/data_dictionary.md`). CSV has a
  header row; orders is JSON Lines.
- **Sample data** (`data/sample/`, versioned): 23 customers, 15 products,
  41 orders, 90 order items, including 10 documented dirty records (null key,
  exact duplicate, changed record, malformed value, invalid value, orphan
  reference). Silver steps should handle each of them deliberately.
- **Code delivery:** a Databricks Git folder cloned from GitHub
  (`AmanShakya28515/sales-analytics-lakehouse`). Push locally, then Pull in
  the Git folder before running. Notebooks add `<repo>/src` to `sys.path` from
  their own folder and purge cached `sales_lakehouse` modules.
- **Tests:** `tests/run_tests` (widgets `pattern`, `catalog`). Integration tests
  create and drop their own random `t01_<hex>` schemas and are skipped when
  `catalog` is empty. Spark-free logic is tested with a recording stub.
  Files in a UC volume cannot be opened in append mode (`Errno 29`). Tests
  that change a raw file read it and rewrite it with `"w"`.
- **Bronze (step 02):** `<env>_bronze.customers`, `products`, `orders` and
  `order_items` are managed Delta tables. Each has the source columns as
  `STRING` in registry order, then `_ingested_at TIMESTAMP`, `_source_file`
  and `_run_id` (all `NOT NULL`). There are no keys or constraints on data
  columns. Bronze keeps nulls, duplicates and malformed values as delivered.
  `notebooks/bronze_ingest` (`catalog`, `env`, optional `run_id` for Jobs)
  calls `sales_lakehouse.bronze.ingest_bronze`.
  - **Write mode: full refresh.** Each run replaces every table with
    `writeTo(table).overwrite(lit(True))`. This keeps the predeclared DDL,
    comment and Delta history (one `WRITE` version per run). *Why:* simple
    and idempotent, and it matches step 01's "reload replaces files".
    Incremental ingestion (`COPY INTO`/Auto Loader) is a later step.
  - **All or nothing.** `validate_raw_structure` checks all four datasets
    before anything is created or written. A dataset fails if it is
    missing, has no data rows, has a wrong CSV header or field count, or
    has JSON records whose field names are not exactly the documented ones
    (`json_object_keys`, because a schema'd JSON read hides missing keys).
  - **DDL runs inside ingestion,** after validation (plan 02 D1). *Why:*
    `setup/00` promises no tables, and a failed run must create none.
  - `_source_file` comes from `_metadata.file_path`, because
    `input_file_name()` is not available on serverless/UC. `_ingested_at`
    and `_run_id` are literals, identical across the four tables of a run.
- **Silver (step 03):** `<env>_silver.customers`, `products`, `orders` and
  `order_items` are typed (types and NOT NULLs from `SILVER_SCHEMA` in
  `sales_lakehouse.silver`, matching `docs/data_dictionary.md`). Each also
  has `_run_id`, `_processed_at` and `_bronze_run_id`. One shared
  `<env>_silver.quarantine` table (plan 03 D1) holds every rejected row with
  `dataset`, `record_key`, `reasons ARRAY<STRUCT<reason, column>>` (every
  rule broken) and `source_record MAP` (the original Bronze text).
  `notebooks/silver_transform` (`catalog`, `env`, optional `run_id`) calls
  `transform_silver`. The cleaning logic is pure DataFrame functions
  (`build_silver`), so unit tests need no tables.
  - **Checked, pinned input.** All four Bronze tables must exist and hold
    exactly one `_run_id` between them. If not, the run raises before any
    DDL or write. Bronze is read `VERSION AS OF` the version that passed the
    check.
  - **Rule order:**
    1. Trim, and turn `''` into NULL.
    2. Collapse exact duplicates.
    3. Resolve key duplicates. For customers the latest `updated_at` wins.
       Anything unresolvable is `conflicting_duplicate`.
    4. Apply the row rules: `null_key`, `missing_value`, `malformed_value`,
       `invalid_value`.
    5. Check references against the parent's **Silver** keys:
       `orphan_reference` / `parent_rejected`, so rejections cascade to
       children.

    *Why duplicates come before the row rules (D3):* an invalid latest
    version is quarantined, and an older version is never promoted in its
    place. A null foreign key is only `missing_value` (D2).
  - Typing uses `try_*` functions (`try_cast`, `try_to_timestamp`), because
    serverless runs in ANSI mode, where a plain `cast` fails the whole query.
  - **Full refresh**, the same pattern as Bronze: DDL inside the run after
    the check, then `writeTo().overwrite(lit(True))` for all five tables.

### Known limitations (deliberately deferred)
- `verify_raw_datasets` (`setup/02`): its own CSV-header and
  missing-JSON-column checks have no test (01 Q-1). Bronze uses
  `validate_raw_structure` instead, which is fully tested.
- No `.gitattributes`. Sample files are LF in git, but line endings are not
  enforced (01 Q-3).
- Sample loading is not atomic across datasets. A failed copy leaves that
  folder empty until re-run, and verify reports it as `MISSING` (01 Q-4).
- Bronze's four table writes are separate Delta transactions. A crash
  mid-run can leave tables from different runs (visible as mixed
  `_run_id`s) until the next successful run (02 Q-2). Silver's five writes
  have the same limitation (03 Q-1).
- Silver doesn't cache anything, so each write re-evaluates the whole
  lineage, re-reading Bronze each time. This is fine at sample scale.
  Revisit it with larger data or incremental processing (03 Q-3).

### Steps landed
- 01 — Project foundation: project layout, per-environment UC schemas and
  `raw_data` volume, versioned sample datasets with documented dirty records,
  and a re-runnable load-and-verify flow (31 tests, user-reported green).
- 02 — Bronze ingestion: all-or-nothing validated, full-refresh ingestion of
  the four raw datasets into Bronze Delta tables with `_ingested_at`,
  `_source_file` and `_run_id` (suite 59 tests: 28 new, user-reported green).
- 03 — Silver cleaning: typed, deduplicated Silver tables in which every
  foreign key resolves, plus a shared quarantine recording every rejection
  reason, built from checked, version-pinned Bronze by full refresh (suite
  95 tests: 36 new, user-reported all passed).

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
