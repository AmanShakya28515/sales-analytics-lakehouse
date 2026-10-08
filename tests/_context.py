"""Shared test context. SPARK and CATALOG are filled in by the run_tests notebook."""

import unittest
import uuid
from pathlib import Path

from sales_lakehouse.naming import quote

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_ROOT = REPO_ROOT / "data" / "sample"

SPARK = None
CATALOG = ""

TEST_ENV_PREFIX = "t01_"


def require_integration():
    """Return (spark, catalog), or skip when no catalog was given to run_tests."""
    if SPARK is None or not CATALOG:
        raise unittest.SkipTest("Integration test: set the 'catalog' widget in tests/run_tests.")
    return SPARK, CATALOG


def random_test_env():
    return f"{TEST_ENV_PREFIX}{uuid.uuid4().hex[:8]}"


def drop_test_environment(spark, names):
    """Drop the schemas of a temporary test environment (never dev/test)."""
    if not names.env.startswith(TEST_ENV_PREFIX):
        raise ValueError(f"Refusing to drop non-test environment {names.env!r}")
    for schema in names.schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {quote(names.catalog, schema)} CASCADE")
