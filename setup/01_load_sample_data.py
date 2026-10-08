# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Load sample data into the raw volume
# MAGIC
# MAGIC Copies the versioned sample files from `data/sample/` into
# MAGIC `/Volumes/<catalog>/<env>_bronze/raw_data/<dataset>/`. Each dataset folder is emptied
# MAGIC first, so re-running replaces the files instead of adding copies.
# MAGIC Run `00_setup_catalog_objects` first.

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
from sales_lakehouse.raw_datasets import load_sample_files

# COMMAND ----------

names = layer_names(dbutils.widgets.get("catalog"), dbutils.widgets.get("env"))
loaded = load_sample_files(os.path.join(REPO_ROOT, "data", "sample"), names.raw_volume_path)
for dataset, path in loaded.items():
    print(f"{dataset:12} -> {path}")
