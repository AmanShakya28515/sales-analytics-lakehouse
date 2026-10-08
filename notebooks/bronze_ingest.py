# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Loads the four raw datasets from `/Volumes/<catalog>/<env>_bronze/raw_data/` into the Bronze
# MAGIC Delta tables `<catalog>.<env>_bronze.<dataset>`, exactly as delivered plus `_ingested_at`,
# MAGIC `_source_file` and `_run_id`.
# MAGIC
# MAGIC * **Full refresh:** every run replaces each table, so re-running never duplicates rows.
# MAGIC * **All or nothing:** all four datasets are validated first. If any is missing or malformed,
# MAGIC   the run fails and no table is created or changed.
# MAGIC * `run_id` is optional. Leave it empty to generate one, or pass `{{job.run_id}}` from a Job.
# MAGIC
# MAGIC Run `setup/00` and `setup/01` first.

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

from sales_lakehouse.bronze import ingest_bronze

# COMMAND ----------

run_id, counts = ingest_bronze(
    spark,
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("env"),
    dbutils.widgets.get("run_id"),
)

print(f"Run id: {run_id}")
print(f"{'table':12} {'rows':>6}")
for table, rows in counts.items():
    print(f"{table:12} {rows:>6}")
