# Databricks notebook source
# MAGIC %md
# MAGIC # Run tests
# MAGIC
# MAGIC Discovers `tests/test_*.py` (or the given `pattern`) and runs them with `unittest`.
# MAGIC Set `catalog` to run the integration tests; they create and drop their own
# MAGIC temporary `t01_<random>` schemas in that catalog. Leave it empty to skip them.
# MAGIC The notebook fails if any test fails.

# COMMAND ----------

dbutils.widgets.text("pattern", "test_*.py", "Pattern")
dbutils.widgets.text("catalog", "", "Catalog (integration tests)")

# COMMAND ----------

import os
import sys
import unittest

TESTS_DIR = os.getcwd()
SRC = os.path.join(os.path.dirname(TESTS_DIR), "src")
for path in (SRC, TESTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

# Drop stale copies so a re-imported project is picked up.
for module in list(sys.modules):
    if module.split(".")[0] in ("sales_lakehouse", "_context") or module.startswith("test_"):
        del sys.modules[module]

import _context

_context.SPARK = spark
_context.CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

suite = unittest.defaultTestLoader.discover(
    TESTS_DIR, pattern=dbutils.widgets.get("pattern"), top_level_dir=TESTS_DIR
)
result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)

print(
    f"\nRan {result.testsRun}: failures={len(result.failures)}, "
    f"errors={len(result.errors)}, skipped={len(result.skipped)}"
)
if not result.wasSuccessful():
    raise AssertionError("Some tests failed; see the output above.")
