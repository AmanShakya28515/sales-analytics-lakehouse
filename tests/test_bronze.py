"""Unit tests for Bronze ingestion: DDL, run ids, guards and the metadata transform."""

import datetime
import re
import unittest

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import _context
from sales_lakehouse.bronze import (
    METADATA_COLUMNS, bronze_table_statements, ingest_bronze, resolve_run_id,
    with_ingestion_metadata,
)
from sales_lakehouse.naming import layer_names
from sales_lakehouse.raw_datasets import DATASETS, get_dataset

RUN_AT = datetime.datetime(2026, 10, 8, 12, 0, tzinfo=datetime.timezone.utc)
SOURCE = "/Volumes/cat/dev_bronze/raw_data/products/products.csv"


class BronzeTableStatementsTest(unittest.TestCase):
    def test_one_rerunnable_delta_table_per_dataset_in_bronze_only(self):
        """AC-1, AC-10"""
        statements = bronze_table_statements(layer_names("cat", "dev"))
        self.assertEqual(len(statements), 4)
        created = set()
        for statement in statements:
            match = re.match(r"CREATE TABLE IF NOT EXISTS `cat`\.`dev_bronze`\.`(\w+)` ", statement)
            self.assertIsNotNone(match, statement)
            created.add(match.group(1))
            self.assertIn("USING DELTA", statement)
            self.assertNotIn("silver", statement)
            self.assertNotIn("gold", statement)
        self.assertEqual(created, {d.name for d in DATASETS})

    def test_columns_are_text_then_not_null_metadata_in_order(self):
        """AC-2"""
        for dataset, statement in zip(DATASETS, bronze_table_statements(layer_names("cat", "dev"))):
            with self.subTest(dataset=dataset.name):
                expected = [f"`{c}` STRING" for c in dataset.columns] + [
                    "`_ingested_at` TIMESTAMP NOT NULL",
                    "`_source_file` STRING NOT NULL",
                    "`_run_id` STRING NOT NULL",
                ]
                column_list = statement.split(" (", 1)[1].split(") USING DELTA", 1)[0]
                self.assertEqual(column_list.split(", "), expected)

    def test_every_table_has_a_comment(self):
        """AC-12"""
        for statement in bronze_table_statements(layer_names("cat", "dev")):
            self.assertRegex(statement, r"COMMENT '[^']{20,}'$")


class RunIdTest(unittest.TestCase):
    def test_empty_run_id_generates_a_new_one_each_time(self):
        """AC-2, AC-4: runs get different ids."""
        first, second = resolve_run_id(None), resolve_run_id("  ")
        self.assertRegex(first, r"^[0-9a-f]{32}$")
        self.assertNotEqual(first, second)

    def test_given_run_id_is_kept(self):
        self.assertEqual(resolve_run_id(" 12345 "), "12345")

    def test_invalid_run_id_is_rejected(self):
        for value in ("x" * 65, 42):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    resolve_run_id(value)


class _UntouchableSpark:
    """Fails the test if ingestion touches Spark at all."""

    def __init__(self):
        self.touched = []

    def __getattr__(self, name):
        self.touched.append(name)
        raise AssertionError(f"Spark was used ({name}) before parameters were validated")


class IngestGuardsTest(unittest.TestCase):
    def test_invalid_parameters_stop_before_any_spark_call(self):
        """AC-8"""
        cases = [("x; DROP", "dev", None), ("cat", "a`b", None), ("", "dev", None),
                 ("cat", "", None), ("cat", "dev", "r" * 65)]
        for catalog, env, run_id in cases:
            with self.subTest(catalog=catalog, env=env):
                spark = _UntouchableSpark()
                with self.assertRaises(ValueError):
                    ingest_bronze(spark, catalog, env, run_id)
                self.assertEqual(spark.touched, [])


class WithIngestionMetadataTest(unittest.TestCase):
    def setUp(self):
        self.spark = _context.require_spark()
        self.dataset = get_dataset("products")
        self.schema = StructType(
            [StructField(c, StringType(), True) for c in (*self.dataset.columns, "_source_file")]
        )
        # Dirty on purpose: exact duplicate, null key, malformed price, empty strings.
        self.rows = [
            ("P001", "Wireless Mouse", "Electronics", "24.99", "true", SOURCE),
            ("P001", "Wireless Mouse", "Electronics", "24.99", "true", SOURCE),
            (None, "No Key", "Misc", "1.00", "true", SOURCE),
            ("P015", "Cable Organiser", "Accessories", "abc", "true", SOURCE),
            ("P016", "", "Accessories", "", "false", SOURCE),
        ]
        self.input = self.spark.createDataFrame(self.rows, self.schema)

    def _bronze(self, df=None):
        return with_ingestion_metadata(df if df is not None else self.input,
                                       self.dataset, "run-1", RUN_AT)

    def test_columns_are_documented_order_then_metadata(self):
        """AC-2"""
        out = self._bronze()
        self.assertEqual(out.columns, [*self.dataset.columns, *METADATA_COLUMNS])
        types = dict(out.dtypes)
        self.assertEqual({types[c] for c in self.dataset.columns}, {"string"})
        self.assertEqual(types["_ingested_at"], "timestamp")
        self.assertEqual(types["_source_file"], "string")
        self.assertEqual(types["_run_id"], "string")

    def test_input_column_order_does_not_matter(self):
        """AC-2"""
        shuffled = self.input.select("_source_file", *reversed(self.dataset.columns))
        self.assertEqual(self._bronze(shuffled).columns, [*self.dataset.columns, *METADATA_COLUMNS])

    def test_rows_pass_through_unchanged_including_dirty_ones(self):
        """AC-3: nothing cleaned, typed, deduplicated or dropped."""
        out = self._bronze().select(*self.dataset.columns)
        expected = self.input.select(*self.dataset.columns)
        self.assertEqual(out.count(), len(self.rows))
        self.assertEqual(out.exceptAll(expected).count(), 0)
        self.assertEqual(expected.exceptAll(out).count(), 0)

    def test_metadata_is_set_on_every_row(self):
        """AC-2"""
        out = self._bronze()
        total = out.count()
        self.assertEqual(out.where(F.col("_run_id") == "run-1").count(), total)
        self.assertEqual(out.where(F.col("_ingested_at") == F.lit(RUN_AT)).count(), total)
        self.assertEqual(out.where(F.col("_source_file") == SOURCE).count(), total)

    def test_empty_input_gives_empty_output_with_full_schema(self):
        """AC-3"""
        out = self._bronze(self.spark.createDataFrame([], self.schema))
        self.assertEqual(out.count(), 0)
        self.assertEqual(out.columns, [*self.dataset.columns, *METADATA_COLUMNS])

    def test_missing_input_column_raises(self):
        """AC-3, AC-7"""
        with self.assertRaisesRegex(ValueError, "unit_price"):
            self._bronze(self.input.drop("unit_price"))


if __name__ == "__main__":
    unittest.main()
