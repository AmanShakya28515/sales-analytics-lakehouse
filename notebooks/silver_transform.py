# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transform
# MAGIC
# MAGIC Cleans the four Bronze tables `<catalog>.<env>_bronze.<dataset>` into typed, deduplicated
# MAGIC Silver tables `<catalog>.<env>_silver.<dataset>`. Every rejected row goes to
# MAGIC `<catalog>.<env>_silver.quarantine` with its original values and every reason.
# MAGIC
# MAGIC * **Full refresh:** every run replaces the five tables, so re-running never duplicates rows.
# MAGIC * **Checked input:** the four Bronze tables must exist and come from one Bronze run. Otherwise
# MAGIC   the run fails and no Silver table is created or changed.
# MAGIC * `run_id` is optional. Leave it empty to generate one, or pass `{{job.run_id}}` from a Job.
# MAGIC
# MAGIC Run `notebooks/bronze_ingest` first.

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

from sales_lakehouse.silver import transform_silver

# COMMAND ----------

run_id, report = transform_silver(
    spark,
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("env"),
    dbutils.widgets.get("run_id"),
)

print(f"Run id: {run_id}")
print(f"{'dataset':12} {'bronze':>6} {'silver':>6} {'collapsed':>9} {'superseded':>10} {'quarantined':>11}  reasons")
for dataset, r in report.items():
    reasons = ", ".join(f"{reason}={n}" for reason, n in sorted(r["reasons"].items()))
    print(f"{dataset:12} {r['bronze']:>6} {r['silver']:>6} {r['collapsed']:>9} "
          f"{r['superseded']:>10} {r['quarantined']:>11}  {reasons}")
