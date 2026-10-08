"""Runs the real setup against the catalog given to run_tests, in throwaway t01_* schemas."""

import os
import unittest

import _context
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.setup_ddl import run_setup


def _schemas(spark, names):
    rows = spark.sql(
        f"SELECT schema_name FROM {quote(names.catalog)}.information_schema.schemata "
        "WHERE startswith(schema_name, :prefix)",
        args={"prefix": f"{names.env}_"},
    ).collect()
    return sorted(r.schema_name for r in rows)


def _volumes(spark, names):
    rows = spark.sql(
        f"SELECT volume_name FROM {quote(names.catalog)}.information_schema.volumes "
        "WHERE volume_schema = :schema",
        args={"schema": names.bronze},
    ).collect()
    return sorted(r.volume_name for r in rows)


def _table_count(spark, names):
    return spark.sql(
        f"SELECT * FROM {quote(names.catalog)}.information_schema.tables "
        "WHERE table_schema IN (:bronze, :silver, :gold)",
        args={"bronze": names.bronze, "silver": names.silver, "gold": names.gold},
    ).count()


class SetupIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.spark, self.catalog = _context.require_integration()
        self.created = []

    def tearDown(self):
        for names in self.created:
            _context.drop_test_environment(self.spark, names)

    def _new_env(self):
        names = layer_names(self.catalog, _context.random_test_env())
        self.created.append(names)
        return names

    def test_setup_creates_layer_schemas_and_raw_volume_but_no_tables(self):
        """AC-3"""
        names = self._new_env()
        run_setup(self.spark, names.catalog, names.env)

        self.assertEqual(_schemas(self.spark, names), sorted(names.schemas))
        self.assertEqual(_volumes(self.spark, names), ["raw_data"])
        self.assertTrue(os.path.isdir(names.raw_volume_path))
        self.assertEqual(_table_count(self.spark, names), 0)

    def test_rerun_creates_nothing_extra_and_keeps_files(self):
        """AC-4"""
        names = self._new_env()
        run_setup(self.spark, names.catalog, names.env)
        marker = f"{names.raw_volume_path}/marker.txt"
        with open(marker, "w") as handle:
            handle.write("keep me")

        run_setup(self.spark, names.catalog, names.env)

        self.assertEqual(_schemas(self.spark, names), sorted(names.schemas))
        self.assertEqual(_volumes(self.spark, names), ["raw_data"])
        self.assertEqual(_table_count(self.spark, names), 0)
        self.assertEqual(os.listdir(names.raw_volume_path), ["marker.txt"])

    def test_environments_are_fully_separate(self):
        """AC-5"""
        first, second = self._new_env(), self._new_env()
        run_setup(self.spark, first.catalog, first.env)
        run_setup(self.spark, second.catalog, second.env)

        self.assertFalse(set(_schemas(self.spark, first)) & set(_schemas(self.spark, second)))
        self.assertNotEqual(first.raw_volume_path, second.raw_volume_path)

        with open(f"{first.raw_volume_path}/only_here.txt", "w") as handle:
            handle.write("first environment only")
        self.assertEqual(os.listdir(second.raw_volume_path), [])
        self.assertFalse(os.path.exists(f"{second.raw_volume_path}/only_here.txt"))


if __name__ == "__main__":
    unittest.main()
