---
step: 04
slug: gold-aggregates
spec: .claude/specs/04-gold-aggregates.md
status: approved
created: 2026-10-13
approved: 2026-10-13
---

# Plan 04 — Gold aggregates

## Approach
This uses the same run shape as Silver: **check and pin the input, build
in memory, then DDL, then a full refresh**.

1. **Check and pin the Silver input (AC-11, AC-13).**
   - Validate the names.
   - The four Silver *data* tables must exist. `quarantine` is never read.
   - Every non-empty table must carry one and the same `_run_id`.
   - Each table is read `VERSION AS OF` the version that passed the check.
   - **Empty Silver tables are allowed (D2).** Silver legitimately writes
     an empty table when no row is clean (03 AC-19), so an empty table has
     no `_run_id` to compare and is skipped by the run-id check.
   - If the check fails, the run raises `GoldInputError` before any DDL or
     write.
2. **Build in memory, order grain first (AC-6, AC-7).**
   - `qualifying_orders`: Silver orders with status in
     (`completed`, `shipped`).
   - `qualifying_items`: Silver order items inner-joined to
     `qualifying_orders` on `order_id`. `order_id` is unique in Silver, so
     the join cannot multiply rows. `line_revenue = quantity * unit_price`.
   - `order_totals`: one row per qualifying order, with units and revenue
     summed over its items, and left-joined back to `qualifying_orders`. An
     order with no Silver items gets 0, so it still counts as an order.
   - **daily_sales** = `order_totals` grouped by (`order_date`, `channel`):
     `count(*)` orders, plus units, revenue and AOV.
   - **customer_summary** = Silver customers left-joined to `order_totals`
     grouped by `customer_id`: orders, revenue, AOV, min/max `order_date`.
     Missing values become 0, and AOV and dates stay NULL.
   - **product_sales** = Silver products left-joined to `qualifying_items`
     grouped by `product_id`: `countDistinct(order_id)`, units, revenue.
     Missing values become 0.

   *Why order grain first:* counting orders as `count(*)` over item rows
   is the classic fan-out bug (an order with 3 items would count 3 times).
   Aggregating to one row per order first makes "an order counts once"
   true by construction. Only product sales works from item rows, and it
   uses `countDistinct(order_id)` there.
3. **Money.**
   - Revenue sums are cast to `DECIMAL(18,2)`, which is exact. They are
     never doubles.
   - AOV = `round(revenue / orders, 2)`. Spark's `round` is HALF_UP. The
     result is NULL when there are 0 orders, guarded with `when`, so there
     is never a division error.
4. **DDL, then full refresh (AC-10):** `CREATE TABLE IF NOT EXISTS` for the
   three Gold tables after the check, then `writeTo().overwrite(lit(True))`.
   Same pattern as Bronze and Silver.

`build_gold(silver_dfs, run_id, processed_at, silver_run_id)` is a pure
function, and unit tests feed it small typed DataFrames. Run metadata
reuses `bronze.resolve_run_id`.

## Tables & schema changes
Three managed Delta tables in `<catalog>.<env>_gold`. The DDL is versioned
in `src/sales_lakehouse/gold.py`, and all three are written by full
overwrite. Bronze and Silver are unchanged.

| Table | Columns (type, NOT NULL unless marked) | Grain |
|---|---|---|
| `daily_sales` | `order_date DATE`, `channel STRING`, `order_count BIGINT`, `units BIGINT`, `revenue DECIMAL(18,2)`, `avg_order_value DECIMAL(18,2)` | (`order_date`, `channel`) |
| `product_sales` | `product_id`, `product_name`, `category STRING`, `is_active BOOLEAN`, `order_count BIGINT`, `units BIGINT`, `revenue DECIMAL(18,2)` | `product_id` |
| `customer_summary` | `customer_id`, `first_name`, `last_name`, `email STRING`; `country STRING` (nullable); `order_count BIGINT`, `revenue DECIMAL(18,2)`; `avg_order_value DECIMAL(18,2)`, `first_order_date DATE`, `last_order_date DATE` (these three nullable) | `customer_id` |

All three tables also get `_run_id STRING NOT NULL`, `_processed_at
TIMESTAMP NOT NULL` and `_silver_run_id STRING NOT NULL` (AC-5), plus a
table comment each.

## Notebooks, functions, jobs
| Notebook / function | Input | Output | Parameters |
|---|---|---|---|
| `inputs.pin_single_run(spark, names, schema, tables, allow_empty, error)` (D1) | the named tables | `(run_id, {table: DataFrame at the pinned version})`, or raises `error` | — |
| `gold.GOLD_SCHEMA`, `gold.gold_table_statements(names)` | validated names | 3 `CREATE TABLE IF NOT EXISTS` | — |
| `gold.check_silver_inputs(spark, names)` | the 4 Silver data tables | `(silver_run_id, frames)`, or raises `GoldInputError` | — |
| `gold.build_gold(silver_dfs, run_id, processed_at, silver_run_id)` | `{dataset: Silver DataFrame}` | `{table: Gold DataFrame}` | — |
| `gold.transform_gold(spark, catalog, env, run_id=None)` | the Silver tables | the 3 Gold tables; returns `(run_id, report)` (rows per table plus the revenue, units and order totals) | — |
| `notebooks/gold_aggregate.py` | widgets | the 3 tables; prints the report | `catalog`, `env`, `run_id` (optional) |

## Implementation steps
- [x] 1. `src/sales_lakehouse/inputs.py` (D1): `pin_single_run`. Point
  `silver.check_bronze_inputs` at it, keeping the same behaviour, messages
  and return value. The existing Silver tests cover this.
- [x] 2. `src/sales_lakehouse/gold.py`: the schema registry, DDL,
  `GoldInputError`, `check_silver_inputs`, `build_gold`, `transform_gold`
  and the report.
- [x] 3. `notebooks/gold_aggregate.py` (the same bootstrap as the other
  notebooks), plus a line in the `notebooks/README.md` index.
- [x] 4. Tests: `tests/test_gold.py` (Spark unit, small typed in-memory
  Silver frames) and `tests/test_gold_integration.py` (its own `t01_*` env:
  load, then Bronze, then Silver, then Gold). Written directly in /build.
  The independent check is the reconciliation tests, which recompute the
  totals from Silver separately.
- [x] 5. Docs: a "Gold tables" section in `docs/data_dictionary.md`
  (columns, the revenue definition, the qualifying statuses, and the
  sample totals), and the Gold step in the README run order.
- [x] 6. Local syntax check.

## Test plan
| AC | Test module | What it asserts |
|---|---|---|
| AC-1 | `test_gold_integration` | The 3 tables exist, are `delta`, and have a comment. Column names, types and nullability match `GOLD_SCHEMA` |
| AC-1 | `test_gold` | The DDL has 3 statements, all `IF NOT EXISTS`, all in `<env>_gold`. `build_gold` output schemas match `GOLD_SCHEMA` |
| AC-2 | `test_gold` | 2 orders on the same date and channel plus 1 on another channel give 2 rows with the right orders, units, revenue and AOV. A date with only pending orders has no row |
| AC-3 | `test_gold` | A product with no items, or only items of cancelled orders, shows 0/0/0. Name, category and `is_active` are carried over. An order with the same product on 2 lines has order_count 1 and units summed |
| AC-4 | `test_gold` | A customer without orders shows 0 orders, 0 revenue, and null AOV and dates. Another customer's first and last dates are their min and max qualifying dates, ignoring a later pending order |
| AC-5 | `test_gold` + integration | Every row has a non-null `_run_id`, `_processed_at` and `_silver_run_id`. Each has exactly one distinct value per run, and `_silver_run_id` equals Silver's `_run_id` |
| AC-6 | `test_gold` | Adding pending and cancelled orders with items to the input changes no figure in any table (compared with the build without them) |
| AC-7 | `test_gold` + integration | An order with 3 items counts once in daily and customer order counts. Keys are unique (`count == countDistinct(key)`) in all 3 tables |
| AC-8 | `test_gold` + integration | The revenue sums of all 3 tables and the units sums of daily and product match each other and an independent Silver computation. The order sums of daily and customer equal the qualifying Silver orders |
| AC-9 | integration | 29 daily rows. 14 product rows, 1 of them with zeros. 20 customer rows, 2 of them with zeros. 29 orders, 113 units, revenue `9837.12` (as a Decimal) |
| AC-10 | integration | Run Gold twice: the rows are equal to the previous version both ways (`VERSION AS OF`), `_run_id` differs, and nothing doubles. Then change Silver (cancel one order via a Bronze file edit and rerun Bronze and Silver) and run Gold: the totals drop by exactly that order's revenue |
| AC-11 | integration | (a) An env with Bronze only raises `GoldInputError` naming the missing tables, and no Gold table exists. (b) After a good run, `UPDATE` one Silver table's `_run_id` and run again: it raises, and the 3 Gold versions are unchanged. (c) A bad `catalog`/`env` raises `ValueError` before any Spark call (stub) |
| AC-12 | `test_gold` | With no qualifying orders, daily is empty, and every product and customer row shows zeros and nulls with the full schema. With all inputs empty, all 3 are empty and there is no error |
| AC-13 | `test_gold` + integration | `build_gold` accepts only the 4 data frames. In the sample run, quarantined P015 and the customer with no id are absent, and the totals equal Silver-only figures (AC-8) |
| D2 | `test_gold` (input check) / integration | An empty Silver table doesn't fail the run-id check. Two non-empty tables with different `_run_id`s do |
| docs | `test_gold` | The data dictionary names the 3 Gold tables, the qualifying statuses and the revenue definition |

Integration tests follow the Silver pattern: their own `t01_*` env, one
read-only class built once, mutating tests in a class that rebuilds in
`setUp`, and everything dropped in `tearDownClass`.

## Manual Databricks checks
1. Commit and push, then **Pull** in the Git folder.
2. Run `notebooks/gold_aggregate` with `catalog=sales_lakehouse` and
   `env=dev`, using the current `dev` Silver. Report the printed report.
   Expected rows 29, 14 and 20, orders 29, units 113, revenue 9837.12.
3. Run `SELECT * FROM sales_lakehouse.dev_gold.customer_summary ORDER BY revenue DESC LIMIT 5`.
   Report whether the result looks sensible (a sanity look only).
4. Run `tests/run_tests` with `pattern=test_gold*.py` and `catalog` set,
   then with the default pattern. Report **both exact `Ran …` lines**.
5. Run `SHOW SCHEMAS IN sales_lakehouse LIKE 't01_*'` and report whether it
   is empty.

## Risks
- **Join fan-out or double counting:** aggregating to order grain first
  (and using `countDistinct` in product sales) prevents it. The AC-7 and
  AC-8 tests prove it.
- **Money precision:** everything stays DECIMAL, and the tests compare
  against a `Decimal` value. Summing `DECIMAL(10,2)×INT` stays well inside
  `DECIMAL(18,2)` at this scale.
- **Stale or mixed input:** the run-id check plus pinned versions. The
  check skips empty tables (D2), so if every Silver table is empty Gold
  has no run id to record. Gold then writes empty tables, so
  `_silver_run_id` is never needed as NOT NULL. This is tested.
- **Cross-table atomicity:** the 3 writes are separate transactions, the
  same as 02 Q-2 and 03 Q-1. It would be recorded as a known limitation.
- **Refactoring Silver's input check (D1):** behaviour-preserving, and
  covered by the existing Silver integration tests (AC-18 a/b) in the full
  suite.

## Decisions needing approval
- **D1 A shared input-check helper — APPROVED (2026-10-13):** move the "tables exist, one
  `_run_id`, pin versions" logic out of `silver.py` into a small
  `sales_lakehouse/inputs.py`, used by both Silver and Gold. Silver's
  behaviour and error messages stay the same. *Why:* the alternative is
  copying about 30 lines of the same logic into `gold.py`. One version is
  easier to keep correct, and later steps (dashboards, Jobs) will need the
  same check. The trade-off is that it touches already-reviewed Silver
  code, so the full suite must be re-run.
- **D2 Empty Silver tables pass the run-id check — APPROVED (2026-10-13):** a Silver table with 0
  rows has no `_run_id`, so the check compares only non-empty tables. A
  table still has to *exist*. *Why:* Silver deliberately produces empty
  tables when nothing is clean (03 AC-19). Gold AC-12 requires handling
  that, and failing would make Gold refuse valid input. Bronze's stricter
  check is unchanged, because Bronze guarantees non-empty tables.

## Files changed
- `src/sales_lakehouse/inputs.py` (new, D1): `pin_single_run`.
- `src/sales_lakehouse/silver.py`: `check_bronze_inputs` now delegates to
  `pin_single_run`. Messages and return value are unchanged.
- `src/sales_lakehouse/gold.py` (new): `GOLD_SCHEMA`, DDL, `GoldInputError`,
  `check_silver_inputs`, `build_gold`, `gold_report`, `transform_gold`.
- `notebooks/gold_aggregate.py` (new); `notebooks/README.md` (index line).
- `tests/test_gold.py` (21 tests: 10 run without Spark, using a fake Spark
  for the input check) and `tests/test_gold_integration.py` (12 tests),
  both new.
- `docs/data_dictionary.md`: new "Gold tables" section. `README.md`: Gold
  step added to the run order.

**Deviations / notes:**
- `pin_single_run` takes `catalog` and `schema` strings rather than `names`,
  so the same call works for any layer.
- `check_silver_inputs` returns `(silver_run_id, frames)`. Versions are not
  needed by Gold.
- `_silver_run_id` is a literal from the check. When every Silver table is
  empty it is NULL, but then every Gold table is empty too, so the NOT NULL
  column never receives a row.

## Test results
**Measured by Claude (2026-10-13):**
- `env/Scripts/python.exe -m compileall -q src tests setup notebooks`: OK.
  (pyspark isn't installed locally, so no test runs here.)

**User-reported (Databricks):** pending.

## Review log
_(filled in by /review)_
