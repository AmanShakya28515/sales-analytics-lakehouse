# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Set up catalog objects
# MAGIC
# MAGIC Creates, for one environment, the schemas `<env>_bronze`, `<env>_silver`, `<env>_gold`
# MAGIC and the `raw_data` volume inside `<env>_bronze`. No tables are created.
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC
# MAGIC If the catalog does not exist and Databricks refuses to create it, this notebook stops.
# MAGIC Re-run it with `catalog = workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sales_lakehouse", "Catalog")
dbutils.widgets.text("env", "dev", "Environment")

# COMMAND ----------

import os
import sys

# Make the repo's src/ importable and drop stale copies after a re-import.
REPO_ROOT = os.path.dirname(os.getcwd())
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
for module in [m for m in sys.modules if m.split(".")[0] == "sales_lakehouse"]:
    del sys.modules[module]

from sales_lakehouse.naming import quote
from sales_lakehouse.setup_ddl import run_setup

# COMMAND ----------

names = run_setup(spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("env"))
print(f"Catalog: {names.catalog}")
print(f"Schemas: {', '.join(names.schemas)}")
print(f"Raw volume: {names.raw_volume_path}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {quote(names.catalog)}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {quote(names.catalog, names.bronze)}"))
