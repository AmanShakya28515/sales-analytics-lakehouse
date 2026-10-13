# Databricks notebook source
# MAGIC %md
# MAGIC # Gold aggregates
# MAGIC
# MAGIC Builds the Gold tables `<catalog>.<env>_gold.daily_sales`, `product_sales` and
# MAGIC `customer_summary` from the Silver data tables. Revenue counts only `completed` and
# MAGIC `shipped` orders, at the price paid (quantity × order item unit_price).
# MAGIC
# MAGIC * **Full refresh:** every run replaces the three tables, so re-running never duplicates rows.
# MAGIC * **Checked input:** the four Silver tables must exist and come from one Silver run. Otherwise
# MAGIC   the run fails and no Gold table is created or changed.
# MAGIC * `run_id` is optional. Leave it empty to generate one, or pass `{{job.run_id}}` from a Job.
# MAGIC
# MAGIC Run `notebooks/silver_transform` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "sales_lakehouse", "Catalog")
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("run_id", "", "Run id (optional)")

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
for module in [m for m in sys.modules if m.split(".")[0] == "sales_lakehouse"]:
    del sys.modules[module]

from sales_lakehouse.gold import transform_gold

# COMMAND ----------

run_id, report = transform_gold(
    spark,
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("env"),
    dbutils.widgets.get("run_id"),
)

print(f"Run id: {run_id}")
print(f"{'table':17} {'rows':>5} {'orders':>7} {'units':>6} {'revenue':>10}")
for table, r in report.items():
    print(f"{table:17} {r['rows']:>5} {str(r.get('order_count', '-')):>7} "
          f"{str(r.get('units', '-')):>6} {str(r['revenue']):>10}")
