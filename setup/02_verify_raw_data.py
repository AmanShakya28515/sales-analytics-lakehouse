# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Verify raw data
# MAGIC
# MAGIC Reads each dataset in the raw volume with its explicit (all-string) schema and checks
# MAGIC that it is present, readable, has every column and has the documented row count.
# MAGIC Fails if any dataset is not `OK`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sales_lakehouse", "Catalog")
dbutils.widgets.text("env", "dev", "Environment")

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
for module in [m for m in sys.modules if m.split(".")[0] == "sales_lakehouse"]:
    del sys.modules[module]

from sales_lakehouse.naming import layer_names
from sales_lakehouse.raw_datasets import assert_all_ok, verify_raw_datasets

# COMMAND ----------

names = layer_names(dbutils.widgets.get("catalog"), dbutils.widgets.get("env"))
results = verify_raw_datasets(spark, names.raw_volume_path)

print(f"{'dataset':12} {'status':16} {'expected':>8} {'actual':>8}  detail")
for r in results:
    actual = "-" if r.actual_rows is None else r.actual_rows
    print(f"{r.dataset:12} {r.status:16} {r.expected_rows:>8} {actual:>8}  {r.detail}")

assert_all_ok(results)
