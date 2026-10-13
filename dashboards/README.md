# Sales overview dashboard

An AI/BI (Lakeview) dashboard over the **Gold** tables. It never reads
Silver, Bronze or the quarantine, and it only reads. It never creates,
changes or deletes a table.

| File | What it is |
|---|---|
| `sql/*.sql` | The six dashboard queries. **These are the source of truth.** |
| `sales_overview.lvdash.json` | The dashboard itself (datasets, parameters, layout), saved by Databricks. |

A test (`tests/test_dashboard.py`) fails if a dataset in the JSON is not the
same text as its `.sql` file. To change a query, edit the `.sql` file first,
then paste it into the dashboard dataset.

## What each visual shows

All money figures use the step 04 revenue definition: only **completed** and
**shipped** orders qualify, and revenue is the sum of
**quantity × unit_price** (the price paid at the time of sale, not the list
price) over their items.

| Visual | Query | Shows | Sample data (`dev`) |
|---|---|---|---|
| 5 KPI counters | `kpis` | `total_revenue`, `qualifying_orders`, `units`, `avg_order_value` (revenue ÷ orders, 2 places, empty when there are no orders), `active_customers` (customers with at least one qualifying order) | 9837.12 · 29 · 113 · 339.21 · 18 |
| Revenue trend (line chart) | `revenue_trend` | revenue per calendar `month` and `channel` | 2024-01 to 2024-06; mobile 3732.65, store 3104.87, web 2999.60 |
| Top products (table) | `top_products` | products with sales, highest revenue first (ties by id), with name, category, units and orders | P009, P012, P010 first |
| Unsold products (table) | `unsold_products` | products with no qualifying orders | P001 |
| Top customers (table) | `top_customers` | customers with qualifying orders, highest revenue first (ties by id), with name, order count and last order date | C013, C017, C018 first |
| Inactive customers (table) | `inactive_customers` | customers with no qualifying orders | 2 customers |

Customer emails are deliberately not shown.

## Parameters

Every query has two parameters:

| Parameter | Default | Meaning |
|---|---|---|
| `catalog` | `sales_lakehouse` | Unity Catalog catalog |
| `env` | `dev` | environment; the queries read `<catalog>.<env>_gold` |

Each table reference is
``IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`<table>`')``. The
values are bound as data, and the backticks keep each value one name part.
A guard (`assert_true`) also rejects values that are not only letters,
digits and `_`. A value that names no existing environment fails with an
error instead of showing data.

## Build the dashboard (once)

Run the pipeline for `dev` first (see the root `README.md`), then:

1. In the workspace, open the Git folder and go into `dashboards/`. Click
   **Create → Dashboard**. Rename it `sales_overview`, so that Databricks
   saves it here as `sales_overview.lvdash.json`.
2. **Data** tab, six times, once per file in `sql/`:
   1. **Create from SQL**, paste the whole file (the comment line can stay).
   2. Rename the dataset to the file name without `.sql` (for example
      `kpis`). The test matches datasets to files by this name.
   3. The editor detects `:catalog` and `:env` as parameters. Set the type
      to **String** and the defaults to `sales_lakehouse` and `dev`.
   4. **Run** it and check that it returns rows.
3. **Canvas** tab, add the widgets:
   - 5 × **Counter**, dataset `kpis`, one per column: `total_revenue`,
     `qualifying_orders`, `units`, `avg_order_value`, `active_customers`.
   - 1 × **Line** chart, dataset `revenue_trend`: X `month`, Y `revenue`
     (sum), color `channel`.
   - 4 × **Table**, one each for `top_products`, `unsold_products`,
     `top_customers` and `inactive_customers`, showing all columns.
4. Add 2 × **Filter → Single value** widgets: one titled `catalog` and one
   titled `env`. In each, under **Parameters**, select that parameter in
   **every** dataset, so one change drives all visuals.
5. Check the KPI counters against the sample-data column above.
6. Commit and push from the Git folder (branch name → **Commit & Push**),
   with the new `dashboards/sales_overview.lvdash.json`. Then `git pull`
   locally.

## Re-import or refresh

- **Refresh:** open the dashboard and click **Refresh**. Every visual re-runs
  its query on the SQL warehouse against the current Gold tables. After a
  new `gold_aggregate` run, a refresh shows the new figures.
- **Another environment:** change the `catalog` / `env` filters, for
  example `env=test`.
- **Re-import:** in a workspace where the repository is cloned as a Git
  folder, the dashboard appears as `dashboards/sales_overview`. Open it to
  use it. Elsewhere, use **Import dashboard** (Dashboards page → arrow next
  to **Create dashboard**) and choose the `.lvdash.json` file.
- **Changing a query:** edit the `.sql` file, paste it into the dataset, save,
  and commit both files together.

Running a query from a notebook, for checks:

```python
from sales_lakehouse.dashboard import run_query
display(run_query(spark, "kpis", "sales_lakehouse", "dev"))
```
