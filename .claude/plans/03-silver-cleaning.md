---
step: 03
slug: silver-cleaning
spec: .claude/specs/03-silver-cleaning.md
status: done
created: 2026-10-13
approved: 2026-10-13
---

# Plan 03 — Silver cleaning

## Approach
One run has four phases, in this order:

1. **Check inputs, then pin them (AC-18).** Validate the names. Check that
   all four Bronze tables exist, and that together they hold exactly one
   distinct `_run_id`. Then record each Bronze table's current Delta
   version, and read every Bronze table in this run with
   `spark.read.option("versionAsOf", v)`. So even if Bronze is re-ingested
   mid-run, Silver uses exactly the data that passed the check. If the
   check fails, the run raises `SilverInputError` before any DDL or write.
2. **Build in memory (pure DataFrame functions).** For each dataset, in
   parent-first order (customers, products, orders, then order_items):
   1. **Normalise (AC-3).** Keep the original Bronze values in a
      `MAP<STRING,STRING>` for quarantine (AC-15). Then trim every data
      column and turn `''` into `NULL`.
   2. **Collapse exact duplicates (AC-9).** `dropDuplicates` over the
      normalised source columns.
   3. **Resolve key duplicates (AC-10, AC-11).** These are rows that share a
      non-null primary key but differ in other values.
      - **Customers:** the version with the latest parsed `updated_at`
        wins, and the others are *superseded*: dropped and counted, not
        quarantined. If the maximum is tied, or any version's `updated_at`
        cannot be parsed, every version gets `conflicting_duplicate`.
      - **Other datasets:** every version gets `conflicting_duplicate`.

      Duplicates are resolved **before** the row rules on purpose (D3).
   4. **Row rules (AC-4 to AC-7).** Type each column with `try_*` functions,
      because serverless runs in ANSI mode and a plain `cast` would fail
      the whole query. Then attach every failing reason:
      - `null_key`: the primary key is null.
      - `missing_value`: a required column is null.
      - `malformed_value`: the source value is non-null but its typed value
        is null.
      - `invalid_value`: the value is outside its documented domain.

      **Parsing rules:**
      - Dates: `try_to_date(x, 'yyyy-MM-dd')`, so `2024-04-31` becomes
        NULL, which is `malformed_value`.
      - Timestamps: `try_to_timestamp(x, "yyyy-MM-dd'T'HH:mm:ss")`.
      - Decimal and int: `try_cast`.
      - Boolean: only `true` or `false`, in any case. Anything else is
        malformed.
   5. **References (AC-13).** Left-join each non-null foreign key to the
      parent's *Silver* keys, and to the parent's *Bronze* keys (normalised,
      non-null, distinct). A key found in neither gives `orphan_reference`.
      A key found in Bronze but not in Silver gives `parent_rejected`. A
      null foreign key is not checked here, because `missing_value` already
      covers it (D2). Every reason is kept, as an
      `ARRAY<STRUCT<reason, column>>`, so OI0073 carries `parent_rejected`
      twice: once on `order_id` and once on `product_id` (AC-8).
   6. **Split.** Rows with no reasons go to Silver, with the typed columns
      plus metadata. Rows with any reason go to the quarantine rows.
3. **Ensure the tables exist:** `CREATE TABLE IF NOT EXISTS` for the four
   Silver tables and `quarantine`. This runs only after step 1 passes, the
   same pattern as Bronze D1.
4. **Full refresh (AC-17):** `writeTo(table).overwrite(lit(True))` for all
   five tables, as in Bronze. This keeps the DDL, comments and history.
   Then build the report (AC-20) from counts on the written tables plus
   the build statistics.

**Why pure functions over in-memory DataFrames:** the cleaning rules are
the riskiest logic in this step, and tests can feed tiny DataFrames to
`build_silver(bronze_dfs, run_meta)` with no tables involved. Only the
input check, the DDL and the writes touch the catalog. **Why no
`cache()`:** serverless doesn't support persisting, and the data is tiny.
The lazy plans re-read the pinned Bronze versions, which gives the same
result.

Run metadata reuses `bronze.resolve_run_id` (the optional `run_id` widget,
or a generated `uuid4`). `_processed_at` is taken once per run, in UTC.

## Tables & schema changes
Five managed Delta tables in `<catalog>.<env>_silver`. The DDL is versioned
in `src/sales_lakehouse/silver.py` and runs inside the transform after the
input check. Write mode is a full overwrite for all five.

| Table | Data columns (intended types, data dictionary order) | NOT NULL |
|---|---|---|
| `customers` | customer_id STRING, first_name, last_name, email, city, country STRING, signup_date DATE, updated_at TIMESTAMP | every column except `city`, `country` |
| `products` | product_id, product_name, category STRING, unit_price DECIMAL(10,2), is_active BOOLEAN | all |
| `orders` | order_id, customer_id STRING, order_date DATE, status, channel STRING | all |
| `order_items` | order_item_id, order_id, product_id STRING, quantity INT, unit_price DECIMAL(10,2) | all |
| `quarantine` | `dataset STRING NOT NULL`, `record_key STRING` (normalised PK, may be null), `reasons ARRAY<STRUCT<reason STRING, column STRING>> NOT NULL`, `source_record MAP<STRING,STRING> NOT NULL` (original Bronze values), `_source_file STRING` | per column |

The four data tables also get `_run_id STRING NOT NULL`, `_processed_at
TIMESTAMP NOT NULL` and `_bronze_run_id STRING NOT NULL` (AC-2), and so
does `quarantine`.

The NOT NULL constraints are a safety net. If the cleaning logic ever lets
a null required value through, the write fails instead of storing bad data.
Primary-key uniqueness can't be declared in Delta, so tests assert it
instead (AC-12). Each table gets a short comment. Bronze and Gold are
unchanged.

## Notebooks, functions, jobs
| Notebook / function | Input | Output | Parameters |
|---|---|---|---|
| `silver.SILVER_SCHEMA` | — | per dataset: `(column, type, nullable)` plus domain rules (allowed values, > 0) | — |
| `silver.check_bronze_inputs(spark, names)` | the Bronze tables | `(bronze_run_id, {dataset: version})`, or raises `SilverInputError` | — |
| `silver.normalise(df, dataset)` | Bronze DataFrame | trimmed, with nulls for empty strings, plus `_source_record` | — |
| `silver.collapse_and_resolve(df, dataset)` | normalised DataFrame | rows with `conflicting_duplicate` reasons, superseded rows dropped, plus counts | — |
| `silver.apply_row_rules(df, dataset)` | DataFrame | typed columns plus a `_reasons` array | — |
| `silver.apply_reference_rules(df, fk, parent_silver, parent_bronze)` | child plus parent DataFrames | `_reasons` extended | — |
| `silver.build_silver(bronze_dfs, run_meta)` | `{dataset: Bronze DataFrame}` | `{dataset: Silver DataFrame}`, quarantine DataFrame, stats | — |
| `silver.silver_table_statements(names)` | validated names | 5 `CREATE TABLE IF NOT EXISTS` statements | — |
| `silver.transform_silver(spark, catalog, env, run_id=None)` | Bronze tables | the 5 Silver tables; returns `(run_id, report)` | — |
| `notebooks/silver_transform.py` | widgets | the 5 tables; prints the AC-20 report | `catalog`, `env`, `run_id` (optional) |

`collect()` is used only on tiny metadata: the distinct Bronze run ids
(at most 4 rows) and the aggregated report rows. It is never used on data
rows.

## Implementation steps
- [x] 1. `src/sales_lakehouse/silver.py`:
  - schema and domain registry, `SilverInputError` and the input check
  - `normalise`, `collapse_and_resolve`, `apply_row_rules`,
    `apply_reference_rules` and `build_silver`
  - the DDL, and `transform_silver` with the report
- [x] 2. `notebooks/silver_transform.py`: widgets, path bootstrap as in
  `bronze_ingest`, then `transform_silver`, then print the report. Add it
  to the `notebooks/README.md` index.
- [x] 3. Tests: `tests/test_silver.py` (Spark unit, in-memory DataFrames)
  and `tests/test_silver_integration.py` (its own `t01_*` env: load, then
  Bronze, then Silver). The unit cases for the duplicate, cascade and
  multi-reason rules are designed with the `test-author` sub-agent, because
  they are complex ACs (CLAUDE.md → Sub-agents).
- [x] 4. Docs: add "Silver tables" and "Quarantine" sections to
  `docs/data_dictionary.md`, covering the types, metadata, reason codes and
  the expected sample outcome. Add the Silver step to the README run order.
- [x] 5. Local syntax check.

## Test plan
| AC | Test module | What it asserts |
|---|---|---|
| AC-1 | `test_silver_integration` | All 5 tables exist and are `delta`. Each table's column names, types and nullability match `SILVER_SCHEMA` |
| AC-1 | `test_silver` | `build_silver` output schemas use the intended types. The DDL has 5 statements, all `IF NOT EXISTS`, all in `<env>_silver` |
| AC-2 | `test_silver` + integration | Every row has a non-null `_run_id`, `_processed_at` and `_bronze_run_id`. Each has exactly one distinct value across all 5 tables, and `_bronze_run_id` equals Bronze's `_run_id` |
| AC-3 | `test_silver` | `'  C001 '` becomes `C001`. `''` in a required column becomes `missing_value`. `''` in `city` becomes NULL and the row stays valid |
| AC-4 | `test_silver` | A null or blank PK gives `null_key`, and the row is in quarantine, not Silver |
| AC-5 | `test_silver` | A null required non-key column gives `missing_value`, naming the column |
| AC-6 | `test_silver` | `abc` price, `2024-04-31` date, `x` quantity, `yes` boolean and a bad timestamp each give `malformed_value` |
| AC-7 | `test_silver` | Price 0 or −1, quantity 0, status `lost` and channel `fax` each give `invalid_value`. The boundary values price `0.01` and quantity `1` are valid |
| AC-8 | `test_silver` | A row that has both a null key and a malformed date has both reasons. OI0073-like rows have two `parent_rejected` entries (`order_id`, `product_id`) |
| AC-9 | `test_silver` | Three identical rows give one Silver row, the collapsed count is 2, and nothing is quarantined. Rows differing only in surrounding whitespace collapse too |
| AC-10 | `test_silver` | Two versions of a customer: the later `updated_at` wins whatever the input order, the superseded count is 1, and nothing is quarantined |
| AC-10, D3 | `test_silver` | The latest version is invalid: it is quarantined, and the older version does **not** appear in Silver |
| AC-11 | `test_silver` | A tied `updated_at`, an unparseable `updated_at`, and a product or order key with differing versions all put every version in quarantine with `conflicting_duplicate` |
| AC-12 | `test_silver` + integration | The PK is non-null and `count == countDistinct(pk)` in every Silver table |
| AC-13 | `test_silver` | A missing parent gives `orphan_reference`. A quarantined parent gives `parent_rejected`. The cascade covers an order's items and a product's items. A null FK gives only `missing_value` (D2) |
| AC-14 | integration | Anti-joins between each FK and its parent Silver table return 0 rows |
| AC-15 | integration | For `P015`, the quarantine row has dataset `products`, reason `malformed_value` on `unit_price`, `source_record['unit_price'] == 'abc'` (unchanged text), and both run ids |
| AC-16 | integration | Counts are 20/1, 14/1, 38/2 and 76/14. The quarantined keys equal the spec's list exactly. C007 and O0010 appear once each. C012 has the 2024-03-18 email and city |
| AC-17 | integration | Run Silver twice: data rows are equal both ways (`exceptAll`), `_run_id` differs, and nothing doubles. Then re-ingest Bronze from a modified `products.csv` that fixes P015's price, and run Silver: P015 and its 7 items move from quarantine into Silver, so nothing is carried over |
| AC-17 | `test_silver` | `build_silver` on the same input twice gives the same rows |
| AC-18 | integration | (a) A fresh env with no Bronze tables raises `SilverInputError`, and no Silver table exists. (b) After a good run, `UPDATE` one Bronze table's `_run_id` to another value and run again: it raises, naming the mismatch, and all 5 Silver table versions are unchanged. (c) A bad `catalog`/`env` raises `ValueError` before any Spark call (recording stub) |
| AC-19 | `test_silver` | Empty input for all datasets, or input where every row is invalid, gives empty Silver DataFrames with the full schema and no error |
| AC-20 | integration | The report has, per dataset, the Bronze, Silver, collapsed, superseded and quarantined counts, plus quarantined counts per reason. With the sample data: customers 23/20/1/1/1. order_items reasons: `parent_rejected` 11 rows, `orphan_reference` 1, `missing_value` 1, `invalid_value` 1 |
| docs | `test_silver` | The data dictionary names the 5 Silver tables and every reason code |

Integration tests follow `test_bronze_integration`: their own `t01_*` env,
reloaded from the sample data in `setUp`, and dropped in `tearDownClass`.
Order does not matter.

## Manual Databricks checks
1. Commit and push, then **Pull** in the Git folder.
2. Run `notebooks/bronze_ingest` for `dev` (to make sure Bronze is current),
   then `notebooks/silver_transform` with `catalog=sales_lakehouse` and
   `env=dev`. Report the printed report. Expected Silver/quarantine:
   20/1, 14/1, 38/2, 76/14.
3. `SELECT dataset, record_key, reasons FROM sales_lakehouse.dev_silver.quarantine ORDER BY dataset, record_key`.
   Report the row count (expected 18).
4. Run `tests/run_tests` with `pattern=test_silver*.py` and `catalog` set,
   then with the default pattern. Report both `Ran …` lines.
5. Run `SHOW SCHEMAS IN sales_lakehouse LIKE 't01_*'`. Report whether it
   is empty.

## Risks
- **Silently promoting stale data:** if invalid rows were removed before
  deduplication, an older valid customer version could replace the bad
  latest one. Resolving duplicates first (D3), with a test, prevents it.
- **Cross-table atomicity:** the five writes are separate Delta transactions,
  the same as Bronze 02 Q-2. Silver's input check stops the predictable
  failures before the first write. A crash mid-run leaves mixed `_run_id`s
  across the Silver tables until the next run. This will be recorded as a
  known limitation.
- **Inconsistent reads:** pinning the Bronze versions removes the risk of
  Bronze being re-ingested between the check and the reads.
- **ANSI casts:** using `try_*` everywhere avoids query failures on dirty
  values. If `try_to_date` or `try_to_timestamp` is unavailable on the
  runtime, the build stops and reports it rather than working around it.
- **Decimal rounding:** `try_cast('1.239' AS DECIMAL(10,2))` rounds to
  1.24. No such values are in the sample data, so this is accepted, and the
  data dictionary documents that prices have 2 decimal places.
- **Overwrite data loss:** Silver is fully derived from Bronze, and Delta
  history allows time travel or `RESTORE`.
- **Schema drift:** no `overwriteSchema` or `mergeSchema`. A mismatch fails
  the write.

## Decisions needing approval
- **D1 One shared `quarantine` table — APPROVED (2026-10-13):** a single `<env>_silver.quarantine`
  table with `dataset`, `record_key`, `reasons` and `source_record`, rather
  than four `<dataset>_quarantine` tables. *Why:* one place to inspect, one
  schema for every dataset, and a `MAP` keeps the original text for any
  column set.
- **D2 OI0060 reason — APPROVED (2026-10-13):** OI0060 has an empty `order_id`, which is a required
  foreign key, not its primary key. So its reason is `missing_value`
  (column `order_id`), and the reference check skips null foreign keys. The
  spec's AC-16 label "null key" is the known-issue category. The counts are
  unchanged.
- **D3 Duplicates before row rules — APPROVED (2026-10-13):** key duplicates are resolved before the
  row rules. If the latest customer version is invalid, it is quarantined,
  and the older valid version is superseded rather than promoted.

## Files changed
- `src/sales_lakehouse/silver.py` (new): `SILVER_SCHEMA`, `REASONS`, DDL,
  `check_bronze_inputs`, `normalise`, `collapse_exact_duplicates`,
  `resolve_key_duplicates`, `apply_row_rules`, `apply_reference_rules`,
  `build_silver`, `silver_report`, `transform_silver`, `SilverInputError`.
- `notebooks/silver_transform.py` (new); `notebooks/README.md` (index line).
- `tests/test_silver.py` (25 tests, designed by `test-author`),
  `tests/test_silver_integration.py` (11 tests), both new.
- `docs/data_dictionary.md`: new "Silver tables" and "Quarantine" sections.
- `README.md`: Silver step added to the run order.

**Deviations / notes:**
- The plan's `collapse_and_resolve` is split into `collapse_exact_duplicates`
  and `resolve_key_duplicates`. The behaviour is the same.
- `build_silver(bronze_dfs, run_id, processed_at)`: `_bronze_run_id` comes
  from each Bronze row's `_run_id`, not from a parameter.
- Bronze versions are pinned with `SELECT * FROM t VERSION AS OF <int>`.
- Dates are parsed with `to_date(try_to_timestamp(x, 'yyyy-MM-dd'))`, not
  `try_to_date`, so the code doesn't depend on that function existing.
- Per-reason counts count rows (`array_distinct`), so OI0073 counts once for
  `parent_rejected`.

## Test results
**Measured by Claude (2026-10-13):**
- `env/Scripts/python.exe -m compileall -q src tests setup notebooks`: OK.

**User-reported (Databricks, 2026-10-13):**
- `notebooks/silver_transform` for `dev`, on the existing step-02 Bronze
  tables: run id `80f1443b239d4820ab4e1360c3ddf59c`. The bronze / silver /
  collapsed / superseded / quarantined counts were:
  - customers: 23 / 20 / 1 / 1 / 1 (`null_key`=1)
  - products: 15 / 14 / 0 / 0 / 1 (`malformed_value`=1)
  - orders: 41 / 38 / 1 / 0 / 2 (`malformed_value`=1, `orphan_reference`=1)
  - order_items: 90 / 76 / 0 / 0 / 14 (`invalid_value`=1, `missing_value`=1,
    `orphan_reference`=1, `parent_rejected`=11)

  All counts match AC-16 and AC-20.
- Quarantine query, focused Silver tests (`pattern=test_silver*.py`), full
  suite (default pattern) and the `t01_*` cleanup check: the user reported
  "all test passed" for all four. The exact `Ran …` lines were not given;
  the expected values were 18 quarantine rows, 36 and 95 tests, and no
  `t01_*` schemas.

**Unresolved:** none.

## Review log

### 2026-10-13 — review
Scope: the "Files changed" list and `git diff be7fbec 195d988` (9 files).
Checked against AC-1 to AC-20, D1 to D3 and CLAUDE.md.

Security was checked inline:
- SQL is built only from validated `layer_names()` and registry constants.
- `VERSION AS OF` takes an `int` read from history, and the `try_cast`
  expressions are built from constants.
- Run ids reach Spark only through `F.lit`.
- No grants or secrets are involved. Quarantine `source_record` keeps
  fictional sample PII, which is an accepted trade-off.

No security findings.

Correctness, checked and OK:
- Reference joins use distinct parent keys, so there is no fan-out.
- The cascade uses the parent's Silver keys.
- Null keys are excluded from duplicate windows.
- Exact-duplicate collapse is deterministic.
- `try_*` is used throughout, so no ANSI failures.
- `collect()` is used only on run-id metadata and aggregated report rows.

| ID | Sev | Where | Finding | Fix | State |
|---|---|---|---|---|---|
| Q-1 | Low | `src/sales_lakehouse/silver.py:443-445` | The five Silver writes are separate Delta transactions, the same as Bronze 02 Q-2. The input check removes the expected causes, but a failure in a later write leaves earlier tables on the new run while the rest stay on the old one. The mix is visible through `_run_id`, and a re-run fixes it. | Accept. Record under Known limitations at `/done`. | accepted |
| Q-2 | Low | `src/sales_lakehouse/silver.py:211-213`, `docs/data_dictionary.md` (Silver section) | `signup_date`, `order_date` and `updated_at` have no time zone, so they are parsed in the Spark session time zone (UTC on serverless). The data dictionary doesn't say so. | Add one sentence to the data dictionary: Silver timestamps are interpreted in the session time zone (UTC). | fixed |
| Q-3 | Low | `src/sales_lakehouse/silver.py:359-401`, `:404-424` | Nothing is cached, so each of the five writes and the report re-evaluate the whole lineage: order_items re-reads all four pinned Bronze versions and every window. This is negligible at sample scale, but it grows with data. | Accept for now. Revisit when data grows or incremental processing arrives, for example by writing parents first and joining against their Silver tables. | accepted |

No High or Medium findings. AC-1 to AC-20 are implemented and covered.
The user-reported results were the Silver run counts (exact match with
AC-16 and AC-20) and "all test passed" for the focused suite, the full
suite and cleanup. The exact `Ran …` lines were not recorded.

### 2026-10-13 — retest (Q-2)
- **Q-2 fixed:** the Silver cleaning rules in `docs/data_dictionary.md` now
  say that source dates and timestamps carry no time zone, so they are read
  in the Spark session time zone (UTC on serverless). This is a docs-only
  change.
- Measured by Claude: local syntax check OK. No code changed, so nothing
  was re-run in Databricks. The `test_silver.py` docs test only checks
  table names and reason codes.
- Q-1 and Q-3 stay open (Low). They go to Known limitations at `/done`.

### 2026-10-13 — close
Q-2 was fixed in the retest (docs only, so nothing was re-run). Q-1 and Q-3
(Low) were accepted by default and recorded under Known limitations in
CLAUDE.md. The user reported the full suite green; the exact count was not
given, and 95 tests were expected.
