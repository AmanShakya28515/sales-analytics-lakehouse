---
step: 01
slug: project-foundation
spec: .claude/specs/01-project-foundation.md
status: done
created: 2026-10-08
approved: 2026-10-08
---

# Plan 01 — Project foundation

## Approach
- **Pure functions first.** Identifier validation, the list of setup DDL
  statements, the dataset registry (paths, formats, explicit schemas,
  expected counts, known dirty records) and the raw-data verification live in
  a small package under `src/sales_lakehouse/`. Notebooks only read widgets,
  call these functions and print results. This keeps the DDL and checks
  testable without touching a real catalog.
- **Sample data is checked in** as hand-written files under `data/sample/`,
  mirroring the volume layout. Checked-in files are reproducible and readable
  in a diff. A generator script would add code to maintain for ~150 rows.
- **Raw schemas are all-string.** The explicit read schemas in this step
  declare every column as `STRING`. Raw data is kept as delivered, so a
  malformed value (e.g. `"abc"` as a price) is preserved, not silently turned
  into `NULL`. The *intended* types are documented in the data dictionary and
  applied in Silver later.
- **Catalog handling:** setup checks whether the `catalog` parameter exists.
  If it does not, it runs `CREATE CATALOG IF NOT EXISTS`. If Free Edition
  refuses, setup stops with a clear message to re-run with the workspace
  catalog (e.g. `workspace`). It does not silently fall back (see Decision 2).
- **Loading into the volume** uses Python file copies from the workspace copy
  of the repo (`/Workspace/.../data/sample/<dataset>/`) to
  `/Volumes/<catalog>/<env>_bronze/raw_data/<dataset>/`. Before copying, the
  dataset folder in the volume is emptied, so a re-load replaces the files
  instead of adding to them (AC-12).
- **Imports from notebooks:** each notebook adds the repo root to `sys.path`
  (derived from its own working directory), then imports `sales_lakehouse`.
  No packaging or wheel is needed.

## Tables & schema changes
No tables. Setup DDL is re-runnable and run per environment:

| Object | Name | Notes |
|---|---|---|
| Catalog | `<catalog>` (default widget value `sales_lakehouse`) | `CREATE CATALOG IF NOT EXISTS` only when it is missing |
| Schema | `<catalog>.<env>_bronze` | `CREATE SCHEMA IF NOT EXISTS`, constant comment |
| Schema | `<catalog>.<env>_silver` | as above, left empty |
| Schema | `<catalog>.<env>_gold` | as above, left empty |
| Managed volume | `<catalog>.<env>_bronze.raw_data` | `CREATE VOLUME IF NOT EXISTS` |

Volume layout: `raw_data/customers/customers.csv`,
`raw_data/products/products.csv`, `raw_data/orders/orders.json` (JSON
Lines, one object per line), `raw_data/order_items/order_items.csv`.

Identifiers are validated with `^[A-Za-z0-9_]+$` (max 64 chars for the
catalog, 20 for the env), lowercased, then backtick-quoted. Validation runs
before any SQL (AC-6).

## Notebooks, functions, jobs
| Notebook / function | Input | Output | Parameters |
|---|---|---|---|
| `src/sales_lakehouse/naming.py` → `validate_identifier`, `layer_names(catalog, env)` | strings | validated names / `ValueError` | — |
| `src/sales_lakehouse/setup_ddl.py` → `setup_statements(catalog, env)`, `run_setup(spark, catalog, env)` | names | list of SQL strings / executed DDL | — |
| `src/sales_lakehouse/raw_datasets.py` → `DATASETS` registry, `KNOWN_ISSUES`, `raw_schema(name)` | — | metadata + `StructType`s | — |
| `src/sales_lakehouse/raw_datasets.py` → `load_sample_files(src_root, volume_root)` | repo sample folder | files in volume | — |
| `src/sales_lakehouse/raw_datasets.py` → `verify_raw_datasets(spark, volume_root)` | volume path | list of per-dataset results (`OK` / `MISSING` / `COUNT_MISMATCH` / `COLUMN_MISMATCH`) | — |
| `setup/00_setup_catalog_objects.py` | — | catalog, schemas, volume | `catalog`, `env` |
| `setup/01_load_sample_data.py` | `data/sample/` | files in `raw_data` | `catalog`, `env` |
| `setup/02_verify_raw_data.py` | `raw_data` volume | printed report; fails if any dataset is not `OK` | `catalog`, `env` |
| `tests/run_tests.py` | — | unittest results | `pattern`, `catalog` |

`verify_raw_datasets` uses `spark.read` with the explicit schema,
`mode=FAILFAST`, and `count()`. It never collects data to the driver.

## Implementation steps
- [x] 1. Create the layout: `src/sales_lakehouse/`, `setup/`, `notebooks/`
  (empty with `.gitkeep`, for later pipeline notebooks), `tests/`,
  `data/sample/`, `docs/`. Add `README.md` describing each folder and
  the run order (AC-1).
- [x] 2. `naming.py` with identifier validation and layer/volume names.
- [x] 3. `setup_ddl.py` with the statement list and `run_setup`.
  `run_setup` validates first, then checks for and creates the catalog with a
  clear error if creation is refused, then runs the schema and volume DDL.
- [x] 4. Write the four sample files: about 20 customers, 15 products,
  40 orders and 90 order items. Names are fictional and emails use
  `example.com`. Include the dirty records listed in step 5.
- [x] 5. `raw_datasets.py`: registry (file name, format, columns, expected
  row count), all-string schemas, `KNOWN_ISSUES`, loader and verifier.
  Planned known issues, at minimum:
  customers: null `customer_id`; an exact duplicate row; the same
  `customer_id` with a changed email/city and a later `updated_at`.
  products: non-numeric `unit_price`.
  orders: unparseable `order_date`; an orphan `customer_id`; an exact
  duplicate order line.
  order_items: orphan `product_id`; null `order_id`; `quantity` ≤ 0.
- [x] 6. `docs/data_dictionary.md`: per dataset, columns with meaning,
  intended type, nullability, PK, FKs, expected row count, and a table of
  known issues (dataset, key, issue). Values match `DATASETS`/`KNOWN_ISSUES`.
- [x] 7. Setup notebooks `00`, `01`, `02` (widgets → functions).
- [x] 8. `tests/run_tests.py` runner: discovers `tests/test_*.py` by
  `pattern` and exposes `catalog` to tests via a small `tests/_context.py`.
  It fails the notebook if any test fails.
- [x] 9. Tests (see Test plan); local syntax check.

## Test plan
| AC | Test module | What it asserts |
|---|---|---|
| AC-1 | `test_project_layout.py` | Every folder named in the README exists in the repo |
| AC-2 | local `compileall` | Syntax check passes |
| AC-3 | `test_setup_ddl.py` | Statements create exactly `<env>_bronze/_silver/_gold` and the `raw_data` volume inside `<env>_bronze`; no `CREATE TABLE`; all use `IF NOT EXISTS` |
| AC-3, AC-4 | `test_setup_integration.py` | With a random env (e.g. `t01_<hex>`) in the `catalog` param: run `run_setup` twice → no error; the 3 schemas and 1 volume exist, with no duplicates; 0 tables in the 3 schemas. tearDown drops only the schemas it created (`CASCADE`) |
| AC-5 | `test_setup_ddl.py` + `test_setup_integration.py` | `dev` vs `test` statements share no schema or volume name. Two random envs: a file loaded into one volume is absent from the other |
| AC-6 | `test_naming.py`, `test_setup_ddl.py` | `x; DROP`, a backtick, `""`, a space, `a.b` and an over-long name raise `ValueError`. `run_setup` with a bad name raises before any SQL (recording stub for `spark.sql` shows zero calls) |
| AC-7 | review check + manual check 2 | Setup runs against a non-default catalog/env; reviewer greps `src/` and `setup/` for literal catalog/schema names outside widget defaults |
| AC-8 | `test_sample_data.py` | The four files exist at the registry paths; the CSVs have a header matching the registry columns; orders parses as JSON Lines |
| AC-9 | `test_sample_data.py` | Row counts in the files equal `DATASETS` expected counts. Manual review confirms the data dictionary matches the registry |
| AC-10 | `test_sample_data.py` | Each file has < 500 rows. Excluding `KNOWN_ISSUES` rows: every order → existing customer; every item → existing order and product; quantity and unit_price parse as > 0 |
| AC-11 | `test_sample_data.py` | Each `KNOWN_ISSUES` entry is actually present in its file. Together they cover the 5 required categories (null key, exact dup, changed record, malformed value, orphan) |
| AC-12 | `test_raw_load_integration.py` | `load_sample_files` into the test volume puts each dataset in its own folder. Running it twice leaves exactly 1 file per dataset folder; a stray extra file placed beforehand is removed |
| AC-13 | `test_raw_load_integration.py` | `verify_raw_datasets` after load returns `OK` for all 4, with counts equal to the expected counts and columns equal to the registry |
| AC-14 | `test_raw_load_integration.py` | After deleting one dataset folder (and, separately, emptying one), the result marks that dataset `MISSING` and the others `OK`. The `02` notebook raises when any dataset is not `OK` |

The sample-data tests use only the standard `csv`/`json` modules on the repo
files, so they need no Spark. The integration tests are skipped with a clear
message when `catalog` is empty.

## Manual Databricks checks
1. Import the project folder into the workspace (see Decision 1).
2. Run `setup/00` with `catalog=sales_lakehouse`, `env=dev`. Report whether
   the catalog was created or refused. If refused, re-run with
   `catalog=workspace` and report the result.
3. Run `00` again with the same parameters, then with `env=test`. Report
   `SHOW SCHEMAS IN <catalog>` and `SHOW VOLUMES IN <catalog>.dev_bronze`.
4. Run `01` then `02` for `dev`. Report the verification table (4 × `OK`
   with counts). Run `01` again and confirm `02` shows the same counts.
5. Run `tests/run_tests` with `catalog=<catalog used>` and the default
   pattern. Report pass/fail/skip counts and any failure tracebacks.

## Risks
- **Data loss on re-load:** emptying a dataset folder deletes files. The
  delete is limited to `/Volumes/<catalog>/<env>_bronze/raw_data/<dataset>/`
  built from validated names and registry constants. No user-supplied path
  is accepted.
- **Test cleanup dropping real schemas:** integration tests only create and
  drop schemas with a random `t01_<hex>` env prefix that they created in the
  same test. They never touch `dev_*` or `test_*`.
- **SQL injection via widgets:** names are validated before any SQL and
  then backtick-quoted. Comments are constants.
- **Free Edition limits** (catalog creation, `/Workspace` file access from
  serverless): surfaced by manual checks 2 and 4. The fallback is a
  parameter change, not a code change.
- **PII:** none is real. Fictional names and `example.com` emails only.

## Decisions needing approval
- **D1 Getting code into Databricks — APPROVED (2026-10-08):** import the
  project folder manually into the workspace through the UI, and re-import
  after changes. Git folders and CLI deployment come in a later step.
- **D2 Catalog fallback — APPROVED (2026-10-08):** if `CREATE CATALOG` is
  refused, setup stops with a clear message telling the user to re-run
  with the workspace catalog (e.g. `catalog=workspace`). There is no
  automatic fallback.

## Files changed
- `README.md`: project layout, UC objects, run order, D1/D2 notes
- `notebooks/README.md`: placeholder for step 02+ notebooks
- `src/sales_lakehouse/__init__.py`, `naming.py`, `setup_ddl.py`, `raw_datasets.py`
- `setup/00_setup_catalog_objects.py`, `01_load_sample_data.py`, `02_verify_raw_data.py`
- `data/sample/customers/customers.csv` (23 rows), `products/products.csv` (15),
  `orders/orders.json` (41, JSON Lines), `order_items/order_items.csv` (90)
- `docs/data_dictionary.md`
- `tests/_context.py`, `run_tests.py`, `test_project_layout.py`, `test_naming.py`,
  `test_setup_ddl.py`, `test_sample_data.py`, `test_setup_integration.py`,
  `test_raw_load_integration.py`

**Deviations from the plan (all minor, within scope):**
- D1 in practice: the user put the code into Databricks through a
  **Databricks Git folder** cloned from
  `github.com/AmanShakya28515/sales-analytics-lakehouse`, not a manual
  import. No code change is needed, because notebooks find `src/` relative
  to their own folder either way. The README still describes manual import
  and is updated in the next step that touches it.
- `notebooks/` has a `README.md` instead of `.gitkeep`. The workspace UI
  import may skip dotfiles, which would leave the folder missing (AC-1).
- `verify_raw_datasets` adds a `READ_ERROR` status for files Spark cannot
  parse (e.g. a CSV header that differs from the schema, checked with
  `enforceSchema=false`). A missing JSON column is detected as
  `COLUMN_MISMATCH` (the column has no values at all).
- The registry also records `foreign_keys`, and `KNOWN_ISSUES` adds an
  `invalid_value` category for `quantity = 0`. Both are used by the
  sample-data tests.
- `load_sample_files` refuses any target path that is not
  `/Volumes/.../raw_data` (guard for the delete-before-copy).
- AC-7 grep note: `sales_lakehouse` is both the Python package name and
  the default catalog widget value. The only catalog literal is the widget
  default.

## Test results
**Measured by Claude (2026-10-08):**
- `env/Scripts/python.exe -m compileall -q src tests setup`: OK.
- The sample files are LF-only. Line counts give 23/15/41/90 data rows.

**User-reported (Databricks, 2026-10-08), via a Git folder on serverless:**
- Step 2: catalog `sales_lakehouse` was **created**, so the fallback to `workspace` was not needed.
- The user reported that all checklist steps passed: setup, the re-runs for `dev` and `test`, load and
  verify (twice), the focused `test_sample_data.py` run, the full suite with
  `catalog` set, and no leftover `t01_*` schemas.
- The exact `Ran …` summary lines and verify-table values were not pasted.
  The suite contains 31 tests (layout 1, naming 5, setup_ddl 7, sample_data 9,
  setup_integration 3, raw_load_integration 6).

**Unresolved:** none.

## Review log

### 2026-10-08 — review
Scope: the "Files changed" list (git has only the initial commit, plus this
plan's edits). Checked against AC-1 to AC-14, the plan and CLAUDE.md. Security
was checked inline, without the sub-agent: catalog and env are validated as
`[A-Za-z0-9_]+` before any `spark.sql`, then backtick-quoted, and comments are
constants. There are no grants, no secrets, and no real PII (fictional names,
`example.com`). No security findings.

| ID | Sev | Where | Finding | Fix | State |
|---|---|---|---|---|---|
| Q-1 | Low | `src/sales_lakehouse/raw_datasets.py:179`, `:216-221` | The verifier's column checks are never exercised by a test: no test feeds a CSV with a wrong header (relies on `enforceSchema=false` → `READ_ERROR`) or a JSON file missing a column (→ `COLUMN_MISMATCH`). AC-13 column conformance of the repo files *is* covered by `test_sample_data`, and the volume copy is byte-identical, so step 01 is correct. The gap matters only if the verifier is reused on non-sample files. | Add two integration tests (rewrite a header; drop a JSON key) asserting a non-`OK` status. | accepted |
| Q-2 | Low | `README.md:32-33` | "Getting started" says to import the folder manually, but the project is now used through a Databricks Git folder (see Deviations). A new reader would follow the wrong path. | Change step 1 to: clone/Pull the Git folder; commit and push locally, then Pull before re-running. | accepted |
| Q-3 | Low | repo root (no `.gitattributes`) | `core.autocrlf=true` locally, and there are no explicit line-ending rules for the sample data. Today the blobs are LF, but a commit from another machine or setting could introduce CRLF into the CSV/JSON. | Add `.gitattributes` with `* text=auto eol=lf`. | accepted |
| Q-4 | Low | `src/sales_lakehouse/raw_datasets.py:165-170` | The load is not atomic across datasets. If a copy fails mid-run, that dataset's folder is left empty. Verify then reports `MISSING`, and a re-run restores it (the repo is the source of truth), so no data is lost. | Accept. If needed later, copy to a temp name and rename. | accepted |

No High or Medium findings. All ACs are implemented and covered (user-reported green run).

### 2026-10-08 — close
The user accepted all four Lows (Q-1 to Q-4). They are recorded under Known
limitations in CLAUDE.md. Q-2 (README) will be fixed in the next step that
touches the README. No code changed after the green run, so it was not re-run.
