---
step: 05
slug: analytics-dashboard
spec: .claude/specs/05-analytics-dashboard.md
status: approved
created: 2026-10-13
approved: 2026-10-13
---

# Plan 05 — Analytics dashboard

## Approach
There are two layers. The **SQL is the source of truth**, and the
**dashboard is a thin layout over it**.

1. **Six versioned, read-only queries** in `dashboards/sql/`. Each reads
   only Gold:

   | Query | Visual | Reads |
   |---|---|---|
   | `kpis.sql` | 5 KPI tiles (AC-1) | `daily_sales` (revenue, orders, units, AOV), `customer_summary` (active customers) |
   | `revenue_trend.sql` | line or bar chart by month × channel (AC-2) | `daily_sales` grouped by `date_trunc('MONTH', order_date)`, `channel` |
   | `top_products.sql` | table (AC-3) | `product_sales` where `order_count > 0`, ordered by `revenue DESC, product_id` |
   | `unsold_products.sql` | table (AC-3) | `product_sales` where `order_count = 0`, ordered by `product_id` |
   | `top_customers.sql` | table (AC-4) | `customer_summary` where `order_count > 0`, ordered by `revenue DESC, customer_id` |
   | `inactive_customers.sql` | table (AC-4) | `customer_summary` where `order_count = 0`, ordered by `customer_id` |

   - **KPI math:** sums use `coalesce(…, 0)`, so empty data shows 0. AOV is
     `round(sum(revenue) / nullif(sum(order_count), 0), 2)`, so it is NULL
     (shown empty) when there are no orders, never an error (AC-6). It is
     computed from the Gold totals, not as an average of averages.
   - **Every result is a plain function of Gold**, so the AC-5 totals agree
     by construction. They're asserted anyway.
2. **Parameterised table names (AC-7, AC-8).** Every table reference is
   ``IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`<table>`')``.
   - `:catalog` and `:env` are **named parameter markers**. Values are
     bound as data, never pasted into the SQL text, so there is no
     injection.
   - The backticks force each value to be **one** name part. A value
     containing `.` can't redirect the query to another schema, and a value
     containing a backtick is a parse error.
   - Every query also starts with a guard:
     `assert_true(:catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$', '…')`.
     It sits in a one-row CTE that the main query cross-joins, so an
     invalid value fails with a clear message instead of "table not found".
   - No query contains a literal catalog or schema name.
3. **The dashboard file (D1).** An AI/BI (Lakeview) dashboard,
   `dashboards/sales_overview.lvdash.json`, kept in the Git folder.
   - Its six datasets are the six SQL files, pasted verbatim.
   - Each dataset declares the `catalog` and `env` parameters (defaults
     `sales_lakehouse` and `dev`).
   - One page-level filter widget drives both parameters for all datasets.
   - A test checks that the file and the SQL files never drift apart.
4. **A small Python helper** `sales_lakehouse/dashboard.py`:
   - `QUERIES` (the name-to-file map)
   - `load_query(name)`
   - `run_query(spark, name, catalog, env)`, which validates the names
     with `layer_names()` first and then calls
     `spark.sql(text, args={"catalog": …, "env": …})`.

   The dashboard itself doesn't need it; it's used by the tests and is
   handy for ad-hoc notebook checks. It's the same SQL text that the
   dashboard runs.

*Why the SQL files are the source of truth, not only the dashboard JSON:*
the JSON is long, machine-generated and hard to review, while `.sql` files
diff cleanly and can be executed by tests (AC-9). The drift test keeps the
two identical.

## Tables & schema changes
None. The queries are read-only (AC-10) and no table, view or grant is
created. Bronze, Silver and Gold are unchanged.

## Notebooks, functions, jobs
| Artifact / function | Input | Output | Parameters |
|---|---|---|---|
| `dashboards/sql/*.sql` (6) | Gold tables | result sets for the visuals | `:catalog`, `:env` |
| `dashboards/sales_overview.lvdash.json` | the 6 queries | AI/BI dashboard (1 page) | `catalog` (default `sales_lakehouse`), `env` (default `dev`) |
| `dashboard.load_query(name)` | name | SQL text | — |
| `dashboard.run_query(spark, name, catalog, env)` | Gold tables | DataFrame | validated `catalog`, `env` |
| `dashboards/README.md` | — | how to import or open, refresh, change the parameters; what each visual shows; the revenue definition (AC-11) | — |

There are no new notebooks or jobs. `dashboards/` is a new top-level
folder, added to the README layout table.

## Implementation steps
- [x] 1. `dashboards/sql/`: the six queries, each with the guard CTE and
  `IDENTIFIER(…)` table references, and a one-line header comment saying
  which visual it feeds.
- [x] 2. `src/sales_lakehouse/dashboard.py`: `QUERIES`, `load_query`,
  `run_query`.
- [x] 3. `dashboards/README.md`: a click-by-click build guide for D1 (create
  the dashboard in the Git folder, add 6 datasets from the files, add the
  `catalog`/`env` parameters and one filter widget bound to them, and the
  layout: 5 KPI counters, trend chart, 4 tables). Also how to re-import and
  refresh, what each visual shows, and the revenue definition. Add the
  folder to the root README layout table and run order.
- [x] 4. Tests: `tests/test_dashboard.py` (file checks, no Spark) and
  `tests/test_dashboard_integration.py` (load, Bronze, Silver, Gold in a
  `t01_*` env, then run every query).
- [ ] 5. Local syntax check. **Hand-off:** the user builds the dashboard in
  the Git folder following the README, then commits the generated
  `sales_overview.lvdash.json`. The file-check tests on it run after that.

## Test plan
| AC | Test module | What it asserts |
|---|---|---|
| AC-1 | `test_dashboard_integration` | `kpis` on the sample: revenue `9837.12`, orders 29, units 113, AOV `339.21`, active customers 18 (Decimal or int exact) |
| AC-2 | `test_dashboard_integration` | `revenue_trend`: months 2024-01 to 2024-06, at most 3 channels per month, sorted by month. The sum is 9837.12. Per channel: mobile 3732.65, store 3104.87, web 2999.60 |
| AC-3 | `test_dashboard_integration` | `top_products`: the first three ids are P009, P012, P010 with 2835.00, 1937.00 and 1047.00, and it has name, category, units and orders columns. `unsold_products` is exactly [P001] |
| AC-4 | `test_dashboard_integration` | `top_customers`: the first three ids are C013, C017, C018 with 1860.00, 1116.80 and 949.50, with name, order count and last order date. `inactive_customers` has 2 rows |
| AC-5 | `test_dashboard` + integration | No query mentions a Bronze or Silver table (only the three Gold names). The KPI revenue equals the trend sum, the `top_products` sum and the `top_customers` sum, and each equals the Gold `daily_sales` total |
| AC-6 | `test_dashboard_integration` | **Ties:** after `UPDATE` makes two products' revenue equal in the test env's Gold, they come out in id order. **Empty:** in an env where every order is cancelled (Bronze file edit, then Silver and Gold), `kpis` gives 0/0/0/NULL/0 and the 4 lists and the trend give 0 rows, with no error |
| AC-7 | `test_dashboard_integration` | Two `t01_*` envs where the second has one order cancelled: the same query with different `env` values returns different totals. `test_dashboard` checks that the defaults in the JSON are `sales_lakehouse` and `dev` |
| AC-8 | `test_dashboard` + integration | **No hard-coding:** no SQL file or JSON dataset contains `sales_lakehouse`, `dev_`, `_gold.` outside the `IDENTIFIER` pattern, or `t01_`. **Invalid values:** `run_query` with `dev;drop`, `a.b` or `` a`b `` raises `ValueError` before Spark. Raw `spark.sql` of a query with `env='a.b'` fails on the guard (`assert_true`) |
| AC-9 | `test_dashboard` | Every name in `QUERIES` has a file. The JSON has exactly 6 datasets, and each dataset's query text equals its `.sql` file (whitespace-normalised). Each dataset declares `catalog` and `env` |
| AC-10 | `test_dashboard` + integration | Each query, after comments are stripped, starts with `WITH` or `SELECT` and contains no `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `DROP`, `ALTER` or `TRUNCATE`. Running all 6 leaves the Delta versions of every Gold table unchanged |
| AC-11 | `test_dashboard` | `dashboards/README.md` names every query and visual, the `catalog`/`env` parameters, the import/refresh steps, and the revenue definition (`completed`, `shipped`, quantity × unit_price). The root README lists `dashboards/` |

The integration tests follow the Gold pattern: one read-only class over
the sample data, and mutating cases in their own class or env, all dropped
in `tearDownClass`.

## Manual Databricks checks
1. Commit and push, then **Pull** in the Git folder.
2. **Build the dashboard** by following `dashboards/README.md` (D1). Check
   that the five KPI tiles read 9837.12 / 29 / 113 / 339.21 / 18 with
   `env=dev`. Then commit `dashboards/sales_overview.lvdash.json` from the
   Git folder and push.
3. Change the `env` filter to a value that doesn't exist, such as `nope`.
   Report whether the visuals show an error or no data (expected: an
   error, not wrong data). Switch back to `dev`.
4. Run `tests/run_tests` with `pattern=test_dashboard*.py` and `catalog`
   set, then with the default pattern. Report **both exact `Ran …` lines**.
5. Run `SHOW SCHEMAS IN sales_lakehouse LIKE 't01_*'` and report whether it
   is empty.

## Risks
- **`IDENTIFIER` with parameter markers in AI/BI datasets:** this is
  supported on current Databricks SQL. If the dashboard editor rejects it,
  the build stops and reports. The fallback would be a plan revision; there
  will be no quiet hard-coding.
- **Free Edition:** AI/BI dashboards run on the Free Edition serverless SQL
  warehouse. If dashboards are unavailable, stop and report.
- **Drift between the JSON and the SQL files:** the AC-9 equality test
  fails on any difference. Edits go to the `.sql` file first, then into the
  dashboard.
- **Hand-built layout (D1):** the visual layout itself isn't tested, only
  the datasets and parameters are. The user's check in manual step 2
  covers the visuals.
- **AOV meaning:** the KPI AOV is total revenue ÷ total orders. It is not
  the average of the daily AOVs, which would be wrong. The 339.21 test
  pins this.
- **No data risk:** read-only queries, no grants, and no new tables.

## Decisions needing approval
- **D1 How the dashboard file is produced — APPROVED (2026-10-13):** you build the dashboard once
  in the Databricks UI, inside the Git folder, by following the README
  guide (about 15 minutes). Databricks then saves it as
  `dashboards/sales_overview.lvdash.json`, which you commit.
  - The datasets are the versioned SQL files pasted in, and the tests
    enforce that they match exactly.
  - *Alternative:* I hand-write the whole `.lvdash.json` (widget specs,
    encodings, positions). That's possible, but the format is large,
    undocumented in places and version-sensitive, so it would likely take
    several fix-and-retry rounds.
  - *Why D1:* it's reliable, you learn the dashboard editor (the point of
    the step), and the logic still lives in reviewed `.sql` files.
- **D2 Tests run the dashboard SQL through Spark — APPROVED (2026-10-13):** the integration tests
  execute the same `.sql` files with `spark.sql(text, args=…)` on the
  notebook's serverless compute, not on the SQL warehouse. *Why:* it fits
  the existing `run_tests` flow and needs no warehouse API or token. Both
  engines run the same Databricks SQL. The user's visual check (manual
  step 2) covers the warehouse side.

## Files changed
- `dashboards/sql/kpis.sql`, `revenue_trend.sql`, `top_products.sql`,
  `unsold_products.sql`, `top_customers.sql`, `inactive_customers.sql` (new)
- `dashboards/README.md` (new): visuals, parameters, build guide, refresh/re-import
- `src/sales_lakehouse/dashboard.py` (new): `QUERIES`, `load_query`, `run_query`
- `tests/test_dashboard.py` (new, 16 tests, no Spark),
  `tests/test_dashboard_integration.py` (new, 10 tests)
- `README.md`: `dashboards/` in the layout table, dashboard in the run order
- *Pending hand-off (step 5):* `dashboards/sales_overview.lvdash.json`, built
  by the user

**Build deviations (no change to the approach or ACs):**
- **Guard form:** the main query filters on `(SELECT ok FROM guard) IS NULL`
  instead of cross-joining the guard CTE. A scalar subquery can't be pruned
  by the optimizer the way an unused cross-join column can.
- **Where an invalid value fails:** `IDENTIFIER` is resolved while the query
  is analysed, before `assert_true` runs. So a value such as `a.b` fails as
  "table not found" or a parse error, not on the guard. The integration
  test asserts only that it raises. Safety is unchanged (bound parameters
  plus backtick quoting). The guard still rejects odd names that do exist.
- **Two filter widgets** (`catalog`, `env`): a single-value filter drives
  one parameter, so one widget per parameter.
- **AC-6 empty case:** `top_products`, `top_customers` and `revenue_trend`
  return 0 rows. `unsold_products` and `inactive_customers` correctly list
  all 14 and 20 rows, not 0 as the plan's table said.
- `test_dashboard.py` names the three Gold tables itself rather than
  importing `gold.py` (which needs pyspark), so it also runs locally.
- Customer emails are not shown on the dashboard.

## Test results
**Claude (local):**
- `env/Scripts/python.exe -m compileall -q src tests setup notebooks`: OK
- `test_dashboard` run locally (no Spark): 11/11 OK. `DashboardDefinitionTest`
  (5) fails as intended until the `.lvdash.json` is committed. Against a mock
  `.lvdash.json`, the 5 tests passed, which checks the test logic only.

**User (Databricks):** _pending_

## Review log
_(filled in by /review)_
