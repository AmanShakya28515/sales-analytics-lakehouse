"""Loads the sample files into a throwaway t01_* volume and verifies them."""

import os
import shutil
import unittest

import _context
from sales_lakehouse.naming import layer_names
from sales_lakehouse.raw_datasets import (
    DATASETS, assert_all_ok, dataset_dir, load_sample_files, read_raw, verify_raw_datasets,
)
from sales_lakehouse.setup_ddl import run_setup


class RawLoadIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, catalog = _context.require_integration()
        cls.names = layer_names(catalog, _context.random_test_env())
        try:
            run_setup(cls.spark, cls.names.catalog, cls.names.env)
        except Exception:
            _context.drop_test_environment(cls.spark, cls.names)
            raise
        cls.volume = cls.names.raw_volume_path

    @classmethod
    def tearDownClass(cls):
        _context.drop_test_environment(cls.spark, cls.names)

    def setUp(self):
        # Every test starts from a fresh load, so test order does not matter.
        load_sample_files(str(_context.SAMPLE_ROOT), self.volume)

    def _statuses(self):
        return {r.dataset: r.status for r in verify_raw_datasets(self.spark, self.volume)}

    def test_each_dataset_sits_in_its_own_folder(self):
        """AC-12"""
        self.assertEqual(sorted(os.listdir(self.volume)), sorted(d.name for d in DATASETS))
        for dataset in DATASETS:
            self.assertEqual(os.listdir(dataset_dir(self.volume, dataset.name)), [dataset.file_name])

    def test_reload_replaces_files_instead_of_adding_copies(self):
        """AC-12"""
        stray = f"{dataset_dir(self.volume, 'customers')}/customers_copy.csv"
        shutil.copyfile(f"{dataset_dir(self.volume, 'customers')}/customers.csv", stray)

        load_sample_files(str(_context.SAMPLE_ROOT), self.volume)
        load_sample_files(str(_context.SAMPLE_ROOT), self.volume)

        for dataset in DATASETS:
            self.assertEqual(os.listdir(dataset_dir(self.volume, dataset.name)), [dataset.file_name])
        results = verify_raw_datasets(self.spark, self.volume)
        self.assertEqual({r.dataset: r.actual_rows for r in results},
                         {d.name: d.expected_rows for d in DATASETS})

    def test_verify_reads_expected_counts_and_columns(self):
        """AC-13: explicit all-string schema, documented columns and row counts."""
        results = verify_raw_datasets(self.spark, self.volume)
        for result in results:
            with self.subTest(dataset=result.dataset):
                self.assertEqual(result.status, "OK", result.detail)
                self.assertEqual(result.actual_rows, result.expected_rows)
        for dataset in DATASETS:
            df = read_raw(self.spark, self.volume, dataset)
            self.assertEqual(tuple(df.columns), dataset.columns)
            self.assertEqual({t for _, t in df.dtypes}, {"string"})
        assert_all_ok(results)

    def test_extra_row_is_reported_as_count_mismatch(self):
        """AC-13"""
        path = f"{dataset_dir(self.volume, 'products')}/products.csv"
        # Volume files cannot be opened in append mode (Errno 29), so rewrite the whole file.
        with open(path, encoding="utf-8") as handle:
            content = handle.read()
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content + "P016,Extra Product,Accessories,5.00,true\n")
        statuses = self._statuses()
        self.assertEqual(statuses["products"], "COUNT_MISMATCH")
        self.assertEqual({s for d, s in statuses.items() if d != "products"}, {"OK"})

    def test_missing_dataset_folder_is_reported(self):
        """AC-14"""
        shutil.rmtree(dataset_dir(self.volume, "products"))
        results = verify_raw_datasets(self.spark, self.volume)
        statuses = {r.dataset: r.status for r in results}
        self.assertEqual(statuses["products"], "MISSING")
        self.assertEqual({s for d, s in statuses.items() if d != "products"}, {"OK"})
        with self.assertRaisesRegex(RuntimeError, "products: MISSING"):
            assert_all_ok(results)

    def test_empty_dataset_folder_is_reported(self):
        """AC-14"""
        os.remove(f"{dataset_dir(self.volume, 'orders')}/orders.json")
        results = verify_raw_datasets(self.spark, self.volume)
        self.assertEqual({r.dataset: r.status for r in results}["orders"], "MISSING")
        with self.assertRaisesRegex(RuntimeError, "orders: MISSING"):
            assert_all_ok(results)


if __name__ == "__main__":
    unittest.main()
