"""Runs Bronze ingestion end to end in throwaway t01_* schemas of the given catalog."""

import json
import os
import shutil
import unittest

from pyspark.sql import functions as F

import _context
from sales_lakehouse.bronze import BronzeValidationError, METADATA_COLUMNS, ingest_bronze
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import (
    DATASETS, dataset_dir, get_dataset, load_sample_files, read_raw, validate_raw_structure,
)
from sales_lakehouse.setup_ddl import run_setup


def _new_environment(spark, catalog):
    names = layer_names(catalog, _context.random_test_env())
    try:
        run_setup(spark, names.catalog, names.env)
        load_sample_files(str(_context.SAMPLE_ROOT), names.raw_volume_path)
    except Exception:
        _context.drop_test_environment(spark, names)
        raise
    return names


class BronzeIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.catalog = _context.require_integration()
        cls.names = _new_environment(cls.spark, cls.catalog)
        cls.volume = cls.names.raw_volume_path

    @classmethod
    def tearDownClass(cls):
        _context.drop_test_environment(cls.spark, cls.names)

    def setUp(self):
        # Every test starts from the unmodified sample files, so order does not matter.
        load_sample_files(str(_context.SAMPLE_ROOT), self.volume)

    # -- helpers ---------------------------------------------------------------

    def _table(self, name, names=None):
        names = names or self.names
        return quote(names.catalog, names.bronze, name)

    def _exists(self, name, names=None):
        names = names or self.names
        return self.spark.catalog.tableExists(f"{names.catalog}.{names.bronze}.{name}")

    def _versions(self, names=None):
        return {
            d.name: self.spark.sql(f"DESCRIBE HISTORY {self._table(d.name, names)} LIMIT 1")
            .first()["version"]
            for d in DATASETS
        }

    def _data(self, name):
        return self.spark.table(self._table(name)).select(*get_dataset(name).columns)

    def _raw(self, name):
        return read_raw(self.spark, self.volume, get_dataset(name)).select(*get_dataset(name).columns)

    def _assert_same_rows(self, actual, expected):
        self.assertEqual(actual.count(), expected.count())
        self.assertEqual(actual.exceptAll(expected).count(), 0)
        self.assertEqual(expected.exceptAll(actual).count(), 0)

    def _file(self, name):
        dataset = get_dataset(name)
        return f"{dataset_dir(self.volume, name)}/{dataset.file_name}"

    def _assert_run_fails_without_changes(self, dataset_name):
        before = self._versions()
        with self.assertRaisesRegex(BronzeValidationError, dataset_name):
            ingest_bronze(self.spark, self.names.catalog, self.names.env)
        self.assertEqual(self._versions(), before)

    # -- tables and content ----------------------------------------------------

    def test_tables_are_delta_with_documented_row_counts(self):
        """AC-1, AC-12"""
        _, counts = ingest_bronze(self.spark, self.names.catalog, self.names.env)
        self.assertEqual(counts, {d.name: d.expected_rows for d in DATASETS})
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                table = self._table(dataset.name)
                self.assertEqual(self.spark.table(table).count(), dataset.expected_rows)
                detail = self.spark.sql(f"DESCRIBE DETAIL {table}").first()
                self.assertEqual(detail["format"], "delta")
                self.assertTrue(detail["description"])

    def test_schema_and_ingestion_metadata(self):
        """AC-2"""
        run_id, _ = ingest_bronze(self.spark, self.names.catalog, self.names.env)
        run_ids, timestamps = set(), set()
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                df = self.spark.table(self._table(dataset.name))
                fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields]
                expected = [(c, "string", True) for c in dataset.columns] + [
                    ("_ingested_at", "timestamp", False),
                    ("_source_file", "string", False),
                    ("_run_id", "string", False),
                ]
                self.assertEqual(fields, expected)
                for column in METADATA_COLUMNS:
                    self.assertEqual(df.where(F.col(column).isNull()).count(), 0)
                suffix = f"/{dataset.name}/{dataset.file_name}"
                self.assertEqual(df.where(~F.col("_source_file").endswith(suffix)).count(), 0)
                run_ids |= {r[0] for r in df.select("_run_id").distinct().collect()}
                timestamps |= {r[0] for r in df.select("_ingested_at").distinct().collect()}
        self.assertEqual(run_ids, {run_id})
        self.assertEqual(len(timestamps), 1)

    def test_rows_equal_raw_files_including_dirty_records(self):
        """AC-3"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                self._assert_same_rows(self._data(dataset.name), self._raw(dataset.name))

        customers, products = self._data("customers"), self._data("products")
        orders, items = self._data("orders"), self._data("order_items")
        self.assertEqual(customers.where("customer_id = 'C007'").count(), 2)
        self.assertEqual(customers.where("customer_id IS NULL").count(), 1)
        self.assertEqual(customers.where("customer_id = 'C012'").select("email").distinct().count(), 2)
        self.assertEqual(products.where("product_id = 'P015'").first()["unit_price"], "abc")
        self.assertEqual(orders.where("order_id = 'O0010'").count(), 2)
        self.assertEqual(orders.where("order_id = 'O0025'").first()["order_date"], "2024-04-31")
        self.assertEqual(orders.where("order_id = 'O0033'").first()["customer_id"], "C999")
        self.assertEqual(items.where("order_item_id = 'OI0045'").first()["product_id"], "P999")
        self.assertIsNone(items.where("order_item_id = 'OI0060'").first()["order_id"])
        self.assertEqual(items.where("order_item_id = 'OI0075'").first()["quantity"], "0")

    # -- idempotency and change ------------------------------------------------

    def test_rerun_replaces_rows_without_duplicates(self):
        """AC-4"""
        first_run, _ = ingest_bronze(self.spark, self.names.catalog, self.names.env)
        versions = self._versions()
        second_run, counts = ingest_bronze(self.spark, self.names.catalog, self.names.env)

        self.assertNotEqual(first_run, second_run)
        self.assertEqual(counts, {d.name: d.expected_rows for d in DATASETS})
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                self._assert_same_rows(self._data(dataset.name), self._raw(dataset.name))
                run_ids = self.spark.table(self._table(dataset.name)).select("_run_id").distinct()
                self.assertEqual([r[0] for r in run_ids.collect()], [second_run])
        self.assertEqual(self._versions(), {d: v + 1 for d, v in versions.items()})

    def test_replaced_file_is_reflected_exactly(self):
        """AC-5: rows removed, changed and added; nothing from the old file remains."""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        with open(self._file("products"), encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        new_lines = [l for l in lines if not l.startswith(("P002,", "P003,"))]
        new_lines = [l.replace("P004,27in Monitor,Electronics,219.00",
                               "P004,27in Monitor,Electronics,199.00") for l in new_lines]
        new_lines.append("P016,Monitor Arm,Accessories,45.00,true")
        with open(self._file("products"), "w", encoding="utf-8") as handle:
            handle.write("\n".join(new_lines) + "\n")

        _, counts = ingest_bronze(self.spark, self.names.catalog, self.names.env)

        products = self._data("products")
        self.assertEqual(counts["products"], 14)
        self._assert_same_rows(products, self._raw("products"))
        self.assertEqual(products.where("product_id IN ('P002', 'P003')").count(), 0)
        self.assertEqual([r[0] for r in products.where("product_id = 'P004'")
                          .select("unit_price").collect()], ["199.00"])
        self.assertEqual(products.where("product_id = 'P016'").count(), 1)

    # -- failure handling ------------------------------------------------------

    def test_missing_dataset_on_fresh_environment_creates_no_table(self):
        """AC-6"""
        names = _new_environment(self.spark, self.catalog)
        self.addCleanup(_context.drop_test_environment, self.spark, names)
        shutil.rmtree(dataset_dir(names.raw_volume_path, "orders"))

        with self.assertRaisesRegex(BronzeValidationError, "orders"):
            ingest_bronze(self.spark, names.catalog, names.env)
        for dataset in DATASETS:
            self.assertFalse(self._exists(dataset.name, names), dataset.name)

    def test_empty_dataset_changes_no_table(self):
        """AC-6"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        os.remove(self._file("products"))
        self._assert_run_fails_without_changes("products")

    def test_wrong_csv_header_changes_no_table(self):
        """AC-7"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        with open(self._file("customers"), encoding="utf-8") as handle:
            content = handle.read()
        with open(self._file("customers"), "w", encoding="utf-8") as handle:
            handle.write(content.replace("customer_id,", "cust_id,", 1))

        self.assertEqual([d for d, _ in validate_raw_structure(self.spark, self.volume)], ["customers"])
        self._assert_run_fails_without_changes("customers")

    def test_json_record_missing_a_field_changes_no_table(self):
        """AC-7"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        with open(self._file("orders"), encoding="utf-8") as handle:
            records = [json.loads(line) for line in handle if line.strip()]
        del records[5]["channel"]
        with open(self._file("orders"), "w", encoding="utf-8") as handle:
            handle.write("".join(json.dumps(r) + "\n" for r in records))

        self.assertEqual([d for d, _ in validate_raw_structure(self.spark, self.volume)], ["orders"])
        self._assert_run_fails_without_changes("orders")

    def test_malformed_csv_line_changes_no_table(self):
        """AC-7"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        with open(self._file("order_items"), "a", encoding="utf-8") as handle:
            handle.write("OI0091,O0001,P001,1,24.99,UNEXPECTED\n")

        self.assertEqual([d for d, _ in validate_raw_structure(self.spark, self.volume)], ["order_items"])
        self._assert_run_fails_without_changes("order_items")

    # -- isolation and side effects -------------------------------------------

    def test_other_environment_is_untouched(self):
        """AC-9"""
        run_id, _ = ingest_bronze(self.spark, self.names.catalog, self.names.env)
        versions = self._versions()
        other = _new_environment(self.spark, self.catalog)
        self.addCleanup(_context.drop_test_environment, self.spark, other)

        ingest_bronze(self.spark, other.catalog, other.env)
        ingest_bronze(self.spark, other.catalog, other.env)

        self.assertEqual(self._versions(), versions)
        for dataset in DATASETS:
            run_ids = self.spark.table(self._table(dataset.name)).select("_run_id").distinct()
            self.assertEqual([r[0] for r in run_ids.collect()], [run_id])

    def test_raw_files_untouched_and_silver_gold_empty(self):
        """AC-10"""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                self.assertEqual(os.listdir(dataset_dir(self.volume, dataset.name)), [dataset.file_name])
                with open(self._file(dataset.name), "rb") as copied, \
                        open(_context.SAMPLE_ROOT / dataset.name / dataset.file_name, "rb") as original:
                    self.assertEqual(copied.read(), original.read())
        tables = self.spark.sql(
            f"SELECT * FROM {quote(self.names.catalog)}.information_schema.tables "
            "WHERE table_schema IN (:silver, :gold)",
            args={"silver": self.names.silver, "gold": self.names.gold},
        ).count()
        self.assertEqual(tables, 0)


if __name__ == "__main__":
    unittest.main()
