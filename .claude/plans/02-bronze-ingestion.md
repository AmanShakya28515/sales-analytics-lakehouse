---
step: 02
slug: bronze-ingestion
spec: .claude/specs/02-bronze-ingestion.md
status: reviewed
created: 2026-10-08
approved: 2026-10-08
---

# Plan 02 — Bronze ingestion

## Approach
One run has three phases, in this order:
1. **Validate all four datasets** without writing anything (Q2). The run
   checks that every dataset folder has files and that every file matches
   its documented structure.
   - **CSV:** read with the explicit all-string schema in `FAILFAST` mode
     with `enforceSchema=false`, and force a full parse with a per-column
     `count`. A wrong header or a line with the wrong number of fields
     raises an error.
   - **JSON Lines:** read as text, and require
     `array_sort(json_object_keys(value))` to equal the documented columns
     for every non-blank line. A missing field, an extra field and a line
     that is not valid JSON (keys = `NULL`) are all caught. A `FAILFAST`
     JSON read cannot see a missing key, which is why this check exists.

   If any dataset fails, the run raises an error that names every failing
   dataset. Nothing has been created or written at that point.
2. **Ensure the tables exist**: `CREATE TABLE IF NOT EXISTS` for the four
   Bronze tables, with explicit columns (see D1).
3. **Full refresh** (Q1): for each dataset, read the raw files and add the
   metadata columns, then replace the whole table with
   `df.writeTo(table).overwrite(lit(True))`. That is a Delta overwrite
   that keeps the table, its comment and its history. Every run is a new
   Delta version, so `DESCRIBE HISTORY` and time travel can be learned
   here.

The other pieces:
- **Run metadata.** `_run_id` comes from an optional `run_id` widget, so a
  Job can later pass `{{job.run_id}}`. If it is empty, a `uuid4` hex is
  generated. `_ingested_at` is a single UTC timestamp taken once per run.
  Both are added as literals, so they are identical across all four tables.
  `_source_file` is `_metadata.file_path`. `input_file_name()` is not
  supported on serverless/UC, which is why the plan uses `_metadata`.
- **Pure, testable function.** `with_ingestion_metadata(df, dataset,
  run_id, ingested_at)` takes the data columns plus `_source_file` and
  returns them in the documented order with the metadata columns appended.
  It never filters, casts or deduplicates (AC-3). Unit tests feed it small
  in-memory DataFrames.
- **Why not `mode("overwrite").saveAsTable`:** on an existing table it can
  replace the table definition (comment, NOT NULL constraints). Writing
  into the predeclared table with `writeTo().overwrite()` keeps the DDL as
  the single source of the schema. Any schema mismatch fails the write
  instead of silently changing the table.

## Tables & schema changes
Four managed Delta tables in `<catalog>.<env>_bronze`. The DDL is versioned
in `src/sales_lakehouse/bronze.py`, is re-runnable
(`CREATE TABLE IF NOT EXISTS ... USING DELTA COMMENT '...'`), and is run by
the ingestion after validation passes.

| Table | Data columns (all `STRING`, nullable, registry order) | Metadata columns | Write mode |
|---|---|---|---|
| `customers` | customer_id … updated_at (8) | `_ingested_at TIMESTAMP NOT NULL`, `_source_file STRING NOT NULL`, `_run_id STRING NOT NULL` | full overwrite |
| `products` | product_id … is_active (5) | same | full overwrite |
| `orders` | order_id … channel (5) | same | full overwrite |
| `order_items` | order_item_id … unit_price (5) | same | full overwrite |

There are no keys or constraints on the data columns. Bronze keeps nulls and
duplicates by design. Silver and Gold are unchanged.

## Notebooks, functions, jobs
| Notebook / function | Input | Output table | Parameters |
|---|---|---|---|
| `raw_datasets.validate_raw_structure(spark, volume_root)` | raw volume | list of `(dataset, problem)`; empty = valid | — |
| `bronze.bronze_table_statements(names)` | validated names | 4 `CREATE TABLE IF NOT EXISTS` strings | — |
| `bronze.read_raw_with_source(spark, volume_root, dataset)` | dataset folder | DataFrame with data columns + `_source_file` | — |
| `bronze.with_ingestion_metadata(df, dataset, run_id, ingested_at)` | DataFrame | Bronze-shaped DataFrame | — |
| `bronze.ingest_bronze(spark, catalog, env, run_id=None)` | raw volume | the 4 Bronze tables; returns `{table: row_count}`, run_id | — |
| `notebooks/bronze_ingest.py` | widgets | the 4 Bronze tables; prints table + count | `catalog`, `env`, `run_id` (optional) |

`ingest_bronze` validates names first (AC-8), then runs validation, DDL and
the writes. The run raises `BronzeValidationError` listing each failing
dataset.

## Implementation steps
- [x] 1. `raw_datasets.validate_raw_structure` (CSV full parse plus the
  header check; JSON key-set check).
- [x] 2. `src/sales_lakehouse/bronze.py`: table comments, DDL statements,
  `read_raw_with_source`, `with_ingestion_metadata`, `ingest_bronze`,
  `BronzeValidationError`, `run_id` handling (non-empty, ≤ 64 characters).
- [x] 3. `notebooks/bronze_ingest.py` (widgets → `ingest_bronze` → print
  counts; replaces `notebooks/README.md` content with a one-line index).
- [x] 4. `tests/_context.py`: add `require_spark()`, which skips only when no
  Spark is available, for Spark unit tests that need no catalog.
- [x] 5. Tests: `test_bronze.py` (unit) and `test_bronze_integration.py`.
- [x] 6. Docs: a Bronze section in `docs/data_dictionary.md` (tables,
  metadata columns, full-refresh semantics). README "Getting started"
  rewritten for the Git-folder workflow and the run order including
  `notebooks/bronze_ingest` (resolves 01 Q-2).
- [x] 7. Local syntax check.

## Test plan
| AC | Test module | What it asserts |
|---|---|---|
| AC-1 | `test_bronze_integration` | After load + ingest in a `t01_*` env: the 4 tables exist, `DESCRIBE DETAIL` format is `delta`, and the counts are 23/15/41/90. `ingest_bronze` returns the same counts |
| AC-1, AC-10 | `test_bronze` | DDL: exactly 4 statements, all `IF NOT EXISTS`, all in `<env>_bronze`, none in silver/gold |
| AC-2 | `test_bronze` | `with_ingestion_metadata` on an in-memory DataFrame: column order = registry + `_ingested_at, _source_file, _run_id`; types string/timestamp; metadata equals the inputs on every row |
| AC-2 | `test_bronze_integration` | Table schema matches the DDL. One distinct `_run_id` and one `_ingested_at` across all 4 tables. `_source_file` ends with `/<dataset>/<file>`. Zero nulls in metadata |
| AC-3 | `test_bronze` | Input with a null key, an exact duplicate row, `abc` and an empty string comes out with the same rows (`exceptAll` both ways empty, same count). Empty input gives an empty output with the full schema. A missing input column raises |
| AC-3 | `test_bronze_integration` | Bronze data columns equal the raw file read with its explicit schema, as multisets (`exceptAll` both ways). Each `KNOWN_ISSUES` row is present (e.g. 2× C007, 1 null customer_id, P015 = `abc`, O0025 = `2024-04-31`) |
| AC-4 | `test_bronze_integration` | Ingest twice: same counts, data rows equal, `_run_id` changed, one distinct `_run_id` per table |
| AC-5 | `test_bronze_integration` | Replace `products.csv` in the volume with 2 rows removed, 1 changed and 1 added, then ingest. The table equals the new file and none of the removed or old values remain |
| AC-6 | `test_bronze_integration` | (a) Fresh env with orders missing: the error names `orders`, and no Bronze table exists. (b) After a good ingest, with an empty products folder: the error names `products`, and the Delta version of all 4 tables is unchanged |
| AC-7 | `test_bronze_integration` | Each case is run separately after a good ingest: a customers header renamed, an orders line missing `channel`, an order_items line with an extra field. Each raises naming the dataset, and all 4 table versions are unchanged. A `validate_raw_structure` unit-level check is also run on the same files |
| AC-8 | `test_bronze` | Bad `catalog`/`env` raises `ValueError` and a recording stub sees zero Spark calls. An empty or over-long `run_id` is handled |
| AC-9 | `test_bronze_integration` | Two `t01_*` envs: ingesting the second leaves the first's table versions and `_run_id` unchanged |
| AC-10 | `test_bronze_integration` | After ingest, the raw files in the volume are byte-identical to `data/sample`, and the silver/gold schemas have 0 tables |
| AC-11 | manual check 3 | The notebook prints 4 tables with counts. `ingest_bronze` return values are covered by the AC-1 test |
| AC-12 | `test_bronze_integration` + `test_bronze_docs` | Every table has a non-empty comment (`DESCRIBE DETAIL description`). The data dictionary names the 4 Bronze tables and 3 metadata columns. The README mentions the Git folder and `notebooks/bronze_ingest`. The `test_project_layout` README table still passes |

Integration tests use their own `t01_*` env. Each test re-loads the sample
files and ingests from a known state, so test order does not matter. The
env is dropped in `tearDownClass`.

## Manual Databricks checks
1. Commit and push, then **Pull** in the Git folder.
2. If `dev` is not loaded yet, run `setup/00` and `setup/01` for `dev`.
3. Run `notebooks/bronze_ingest` with `catalog=sales_lakehouse` and
   `env=dev`. Report the printed table and counts (expected 23/15/41/90).
4. Run it again, then run
   `DESCRIBE HISTORY sales_lakehouse.dev_bronze.customers` and
   `SELECT DISTINCT _run_id FROM sales_lakehouse.dev_bronze.customers`.
   Report: 2 or more `WRITE`/overwrite versions, and exactly 1 `_run_id`.
5. Run `tests/run_tests` with `pattern=test_bronze*.py` and `catalog` set,
   then with the default pattern. Report both `Ran …` lines.
6. Run `SHOW SCHEMAS IN sales_lakehouse LIKE 't01_*'`. Report whether it
   is empty.

## Risks
- **Cross-table atomicity:** Delta makes each table write atomic, but the
  4 writes are not one transaction. Validation removes the expected
  failure causes before the first write. An infrastructure failure
  mid-run could leave some tables on the new run and some on the old. A
  re-run fixes it, because full refresh is idempotent, and the mix is
  visible through `_run_id`. This is recorded as a known limitation.
- **Overwrite data loss:** Bronze is rebuilt from the raw files, which are
  the source of truth, and Delta history allows `RESTORE` / time travel.
- **Schema drift:** there is no `overwriteSchema` or `mergeSchema`. A
  DataFrame that does not match the DDL fails the write, and the table
  keeps its old version.
- **Files changing between validation and write:** acceptable for a
  single-user learning setup.
- **Serverless features** (`_metadata.file_path`, `json_object_keys`,
  `writeTo().overwrite`): all standard on current runtimes. If one is
  refused, the build stops and reports it rather than working around it.
- **Test cleanup:** the existing `t01_*`-only drop guard is reused.

## Decisions needing approval
- **D1 Where the Bronze DDL runs — APPROVED (2026-10-08):** the
  `CREATE TABLE IF NOT EXISTS` statements stay versioned in
  `src/sales_lakehouse/bronze.py`. They run inside the ingestion flow, and
  only after all four raw datasets have passed validation. There is no
  separate table-creation notebook.

## Files changed
- `src/sales_lakehouse/raw_datasets.py`: added `validate_raw_structure` and
  `_json_lines_with_wrong_fields`. Moved error shortening into `_short_error`,
  which `verify_raw_datasets` now uses too (behaviour unchanged).
- `src/sales_lakehouse/bronze.py` (new): DDL, `resolve_run_id`,
  `read_raw_with_source`, `with_ingestion_metadata`, `ingest_bronze`,
  `BronzeValidationError`.
- `notebooks/bronze_ingest.py` (new); `notebooks/README.md` (index).
- `tests/_context.py`: added `require_spark()`.
- `tests/test_bronze.py` (13 tests), `tests/test_bronze_docs.py` (2),
  `tests/test_bronze_integration.py` (12), all new.
- `docs/data_dictionary.md`: new "Bronze tables" section.
- `README.md`: "Getting started" rewritten for the Git folder and run order
  (resolves 01 Q-2).

**Deviations / notes:**
- Test-only fix outside plan scope (user-approved): `tests/test_raw_load_integration.py`
  no longer appends to a volume file (Errno 29).
- `ingest_bronze` returns `(run_id, counts)`, as planned. The counts come
  from `spark.table(...).count()` after each write.
- Tests for AC-7 also call `validate_raw_structure` directly, asserting
  exactly one failing dataset.

## Test results
**Measured by Claude (2026-10-08):**
- `env/Scripts/python.exe -m compileall -q src tests setup notebooks`: OK.

**User-reported (Databricks, 2026-10-08):**
- `notebooks/bronze_ingest` for `dev`: run id `749c62dd3cee4a3794485509f18ea202`,
  counts customers 23, products 15, orders 41, order_items 90 (as expected).
- Focused run 1 (`pattern=test_bronze*.py`, `catalog=sales_lakehouse`):
  **Ran 27: failures=0, errors=1, skipped=0**. The error was in
  `test_malformed_csv_line_changes_no_table`: `OSError: [Errno 29] Illegal seek`
  from `open(..., "a")` on a UC volume file. This is a **test defect, not a
  pipeline bug**: volume files do not support append mode.
  - Fix (test-only): read the file, then rewrite it with `"w"`, as the header
    and JSON tests already do. Production code is unchanged.
  - The same pattern existed in step 01 `test_raw_load_integration.py:74`
    (`test_extra_row_is_reported_as_count_mismatch`). The user approved fixing
    it the same way in this build, although it is outside plan 02. This
    suggests the step 01 run reported as "all passed" did not run that
    test successfully. The next full run will confirm.
- Focused run 2 (after fix `b467fec`, same parameters): **Ran 27 in 373s: failures=0,
  errors=0, skipped=0. OK.**
- Full suite (default pattern, `catalog=sales_lakehouse`): **Ran 58: failures=0, errors=0,
  skipped=0.** This includes step 01 `test_extra_row_is_reported_as_count_mismatch`
  after the append fix.
- `bronze_ingest` re-run for `dev`: run id `ec3fad97d3254041b951e4fa3e002161`, counts
  23/15/41/90. `DESCRIBE HISTORY dev_bronze.customers` shows 4 versions: v0 CREATE
  TABLE, then v1–v3 WRITE (one per run, so overwrite does not append).
  `SELECT DISTINCT _run_id` gives exactly one value, `ec3fad97…`.
- `SHOW SCHEMAS IN sales_lakehouse LIKE 't01_*'`: no rows, so the tests cleaned up.

**Unresolved:** none.

## Review log

### 2026-10-08 — review
Scope: the "Files changed" list and `git diff 4e36c0a` (11 files). Checked
against AC-1 to AC-12, plan D1 and CLAUDE.md. Security was checked inline:
- DDL names come from validated `layer_names()` plus registry constants, and
  the comments are constants.
- `run_id` only reaches Spark through `F.lit`.
- No grants, no secrets, no real PII.

No security findings.

| ID | Sev | Where | Finding | Fix | State |
|---|---|---|---|---|---|
| Q-1 | Medium | `src/sales_lakehouse/raw_datasets.py:229-248`, `src/sales_lakehouse/bronze.py:117` | **An empty file passes validation and the full refresh empties the table.** The checks only fail an empty *folder*. A 0-byte file, a header-only CSV, or a folder holding only `_`/`.`-prefixed files that Spark ignores yields 0 rows with no error. The overwrite then replaces a good Bronze table with nothing, and every table is overwritten the same way. This is silent data loss in the AC-6 spirit ("missing or empty dataset → fail, change nothing"). It is recoverable via Delta time travel, but nothing flags it. | In `validate_raw_structure`, treat a dataset with 0 data rows as a problem: CSV — add `count(lit(1))` to the existing agg; JSON — count non-blank lines. Add one integration test (0-byte `products.csv` → `BronzeValidationError` naming products, versions unchanged). | fixed (retest) |
| Q-2 | Low | `src/sales_lakehouse/bronze.py:111-118` | The four table writes are separate Delta transactions (a planned risk). An infrastructure failure mid-run leaves mixed `_run_id`s across tables. A re-run fixes it. | Accept; record under Known limitations at `/done`. | open |
| Q-3 | Low | `docs/data_dictionary.md:85` | `_ingested_at` is documented as "when the run started", but it is taken after validation and DDL, just before the writes. | Reword to "when the run began writing (UTC)". | fixed (retest) |

No High findings. AC-1 to AC-12 are implemented and covered (58/58 user-reported green).

### 2026-10-08 — retest (Q-1, Q-3)
- **Q-1 fixed:** `validate_raw_structure` now also fails a dataset with 0 data
  rows. For CSV it uses `count(lit(1))` in the existing full-parse aggregate.
  For JSON, `_json_line_counts` returns records and bad records over the
  non-blank lines. New test `test_file_without_data_rows_changes_no_table`
  covers a 0-byte `products.csv`, a header-only `customers.csv` and a
  blank-line-only `orders.json`. Each must fail validation naming only that
  dataset and leave all 4 table versions unchanged.
- **Q-3 fixed:** the data dictionary now says `_ingested_at` is taken when
  the run begins writing (after validation), and lists "no data rows" among
  the failure causes.
- Q-2 stays open (Low; goes to Known limitations at `/done`).
- Measured by Claude: local syntax check OK. Databricks re-run: _pending_
  (expected focused Ran 28, full Ran 59).
