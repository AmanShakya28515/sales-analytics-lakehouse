"""Runs Bronze ingestion then Silver end to end in throwaway t01_* schemas of the given catalog."""

import unittest

from pyspark.sql import functions as F

import _context
from sales_lakehouse.bronze import ingest_bronze
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import DATASETS, dataset_dir, get_dataset, load_sample_files
from sales_lakehouse.setup_ddl import run_setup
from sales_lakehouse.silver import (
    METADATA_COLUMNS, QUARANTINE_TABLE, SILVER_SCHEMA, SilverInputError, transform_silver,
)

SILVER_TABLES = (*[d.name for d in DATASETS], QUARANTINE_TABLE)

# AC-16: expected outcome with the sample data.
EXPECTED_COUNTS = {  # dataset: (silver rows, quarantined rows)
    "customers": (20, 1),
    "products": (14, 1),
    "orders": (38, 2),
    "order_items": (76, 14),
}
EXPECTED_QUARANTINED_KEYS = {
    "customers": {None},
    "products": {"P015"},
    "orders": {"O0025", "O0033"},
    "order_items": {
        "OI0045", "OI0060", "OI0075",  # orphan P999, missing order_id, quantity 0
        "OI0055", "OI0056", "OI0073", "OI0074",  # items of rejected orders O0025, O0033
        "OI0005", "OI0016", "OI0028", "OI0039", "OI0050", "OI0061", "OI0084",  # items of P015
    },
}


def _comparable(df):
    """Set operations reject MAP columns; compare the quarantine's map as JSON text."""
    return df.select(*[
        F.to_json(F.col(f.name)).alias(f.name) if f.dataType.typeName() == "map" else F.col(f.name)
        for f in df.schema.fields
    ])


def _new_environment(spark, catalog):
    names = layer_names(catalog, _context.random_test_env())
    try:
        run_setup(spark, names.catalog, names.env)
        load_sample_files(str(_context.SAMPLE_ROOT), names.raw_volume_path)
    except Exception:
        _context.drop_test_environment(spark, names)
        raise
    return names


class _SilverEnvTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.catalog = _context.require_integration()
        cls.names = _new_environment(cls.spark, cls.catalog)

    @classmethod
    def tearDownClass(cls):
        _context.drop_test_environment(cls.spark, cls.names)

    @classmethod
    def _bronze_and_silver(cls):
        bronze_run_id, _ = ingest_bronze(cls.spark, cls.names.catalog, cls.names.env)
        run_id, report = transform_silver(cls.spark, cls.names.catalog, cls.names.env)
        return bronze_run_id, run_id, report

    def _table(self, name, schema=None):
        return quote(self.names.catalog, schema or self.names.silver, name)

    def _silver(self, name):
        return self.spark.table(self._table(name))

    def _exists(self, name):
        return self.spark.catalog.tableExists(f"{self.names.catalog}.{self.names.silver}.{name}")

    def _versions(self):
        return {
            t: self.spark.sql(f"DESCRIBE HISTORY {self._table(t)} LIMIT 1").first()["version"]
            for t in SILVER_TABLES
        }

    def _quarantined_keys(self, dataset):
        rows = self._silver(QUARANTINE_TABLE).where(F.col("dataset") == dataset).select("record_key")
        return [r["record_key"] for r in rows.collect()]

    def _data(self, name):
        return self._silver(name).drop(*METADATA_COLUMNS)


class SilverSampleDataTest(_SilverEnvTest):
    """One Bronze + Silver run over the sample data; these tests only read."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        try:
            cls.bronze_run_id, cls.run_id, cls.report = cls._bronze_and_silver()
        except Exception:
            _context.drop_test_environment(cls.spark, cls.names)
            raise

    def test_tables_exist_as_delta_with_the_documented_schema(self):
        """AC-1"""
        for name, columns in SILVER_SCHEMA.items():
            with self.subTest(table=name):
                detail = self.spark.sql(f"DESCRIBE DETAIL {self._table(name)}").first()
                self.assertEqual(detail["format"], "delta")
                self.assertTrue(detail["description"])
                fields = {f.name: f for f in self._silver(name).schema.fields}
                self.assertEqual([f for f in fields if f not in METADATA_COLUMNS],
                                 [c.name for c in columns])
                for column in columns:
                    self.assertEqual(fields[column.name].dataType.simpleString(),
                                     column.sql_type.lower())
                    self.assertEqual(fields[column.name].nullable, column.nullable)
        self.assertTrue(self._exists(QUARANTINE_TABLE))

    def test_every_row_carries_one_run_lineage(self):
        """AC-2"""
        for name in SILVER_TABLES:
            with self.subTest(table=name):
                meta = self._silver(name).agg(
                    *[F.count(F.col(c)).alias(c) for c in METADATA_COLUMNS],
                    F.count(F.lit(1)).alias("rows"),
                    F.countDistinct("_run_id").alias("run_ids"),
                    F.countDistinct("_processed_at").alias("times"),
                    F.countDistinct("_bronze_run_id").alias("bronze_ids"),
                    F.first("_run_id").alias("run_id"),
                    F.first("_bronze_run_id").alias("bronze_run_id"),
                ).first()
                for column in METADATA_COLUMNS:
                    self.assertEqual(meta[column], meta["rows"])
                self.assertEqual((meta["run_ids"], meta["times"], meta["bronze_ids"]), (1, 1, 1))
                self.assertEqual(meta["run_id"], self.run_id)
                self.assertEqual(meta["bronze_run_id"], self.bronze_run_id)

    def test_primary_keys_are_present_and_unique(self):
        """AC-12"""
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                counts = self._silver(dataset.name).agg(
                    F.count(F.lit(1)).alias("rows"),
                    F.count(dataset.primary_key).alias("keys"),
                    F.countDistinct(dataset.primary_key).alias("distinct"),
                ).first()
                self.assertEqual(counts["rows"], counts["keys"])
                self.assertEqual(counts["rows"], counts["distinct"])

    def test_every_foreign_key_resolves_to_a_silver_parent(self):
        """AC-14"""
        for dataset in DATASETS:
            for fk_column, parent_name in dataset.foreign_keys:
                with self.subTest(dataset=dataset.name, fk=fk_column):
                    parent = self._silver(parent_name).select(
                        F.col(get_dataset(parent_name).primary_key).alias(fk_column))
                    orphans = self._silver(dataset.name).join(parent, fk_column, "left_anti")
                    self.assertEqual(orphans.count(), 0)

    def test_quarantine_keeps_original_values_and_reasons(self):
        """AC-15"""
        row = (self._silver(QUARANTINE_TABLE)
               .where((F.col("dataset") == "products") & (F.col("record_key") == "P015"))
               .collect())
        self.assertEqual(len(row), 1)
        row = row[0]
        self.assertEqual({(r["reason"], r["column"]) for r in row["reasons"]},
                         {("malformed_value", "unit_price")})
        self.assertEqual(row["source_record"]["unit_price"], "abc")
        self.assertEqual(row["source_record"]["product_name"], "Cable Organiser")
        self.assertTrue(row["_source_file"].endswith("/products/products.csv"))
        self.assertEqual(row["_run_id"], self.run_id)
        self.assertEqual(row["_bronze_run_id"], self.bronze_run_id)

    def test_sample_data_outcome(self):
        """AC-16, also AC-8 and D2 on the sample's known issues"""
        for dataset, (silver_rows, quarantined) in EXPECTED_COUNTS.items():
            with self.subTest(dataset=dataset):
                self.assertEqual(self._silver(dataset).count(), silver_rows)
                keys = self._quarantined_keys(dataset)
                self.assertEqual(len(keys), quarantined)
                self.assertEqual(set(keys), EXPECTED_QUARANTINED_KEYS[dataset])

        customers = self._silver("customers")
        self.assertEqual(customers.where(F.col("customer_id") == "C007").count(), 1)
        c012 = customers.where(F.col("customer_id") == "C012").collect()
        self.assertEqual(len(c012), 1)
        self.assertEqual((c012[0]["email"], c012[0]["city"]), ("leo.novak.work@example.com", "Brno"))
        self.assertEqual(self._silver("orders").where(F.col("order_id") == "O0010").count(), 1)

        reasons = {
            r["record_key"]: {(x["reason"], x["column"]) for x in r["reasons"]}
            for r in self._silver(QUARANTINE_TABLE).where(F.col("dataset") == "order_items").collect()
        }
        self.assertEqual(reasons["OI0060"], {("missing_value", "order_id")})
        self.assertEqual(reasons["OI0045"], {("orphan_reference", "product_id")})
        self.assertEqual(reasons["OI0075"], {("invalid_value", "quantity")})
        self.assertEqual(reasons["OI0073"], {("parent_rejected", "order_id"),
                                             ("parent_rejected", "product_id")})

    def test_report_counts(self):
        """AC-20"""
        self.assertEqual(self.report["customers"], {
            "bronze": 23, "silver": 20, "collapsed": 1, "superseded": 1, "quarantined": 1,
            "reasons": {"null_key": 1},
        })
        self.assertEqual(self.report["orders"]["collapsed"], 1)
        self.assertEqual(self.report["orders"]["reasons"],
                         {"malformed_value": 1, "orphan_reference": 1})
        self.assertEqual(self.report["order_items"]["reasons"], {
            "parent_rejected": 11, "orphan_reference": 1, "missing_value": 1, "invalid_value": 1,
        })
        for dataset, (silver_rows, quarantined) in EXPECTED_COUNTS.items():
            self.assertEqual(self.report[dataset]["silver"], silver_rows)
            self.assertEqual(self.report[dataset]["quarantined"], quarantined)


class SilverRerunTest(_SilverEnvTest):
    """Tests that change Bronze; each one starts from a fresh Bronze + Silver run."""

    def setUp(self):
        load_sample_files(str(_context.SAMPLE_ROOT), self.names.raw_volume_path)
        self.bronze_run_id, self.run_id, self.report = self._bronze_and_silver()

    def test_rerun_gives_the_same_rows(self):
        """AC-17"""
        before = self._versions()
        run_id, _ = transform_silver(self.spark, self.names.catalog, self.names.env)
        self.assertNotEqual(run_id, self.run_id)
        for table in SILVER_TABLES:
            with self.subTest(table=table):
                previous = self.spark.sql(
                    f"SELECT * FROM {self._table(table)} VERSION AS OF {before[table]}")
                after = _comparable(self._data(table))
                expected = _comparable(previous.drop(*METADATA_COLUMNS))
                self.assertEqual(after.count(), expected.count())
                self.assertEqual(after.exceptAll(expected).count(), 0)
                self.assertEqual(expected.exceptAll(after).count(), 0)
                self.assertEqual(self._silver(table).select("_run_id").distinct().count(), 1)

    def test_fixed_bronze_data_moves_rows_out_of_quarantine(self):
        """AC-17: nothing is carried over from the earlier run."""
        path = f"{dataset_dir(self.names.raw_volume_path, 'products')}/products.csv"
        with open(path, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("P015,Cable Organiser,Accessories,abc,true", text)
        # Volume files cannot be appended to; rewrite the whole file.
        with open(path, "w", encoding="utf-8") as f:
            f.write(text.replace("Accessories,abc,", "Accessories,14.50,"))

        self._bronze_and_silver()
        self.assertEqual(self._silver("products").count(), 15)
        self.assertEqual(self._silver("order_items").count(), 83)
        self.assertNotIn("P015", self._quarantined_keys("products"))
        items = set(self._quarantined_keys("order_items"))
        self.assertEqual(len(items), 7)
        self.assertIn("OI0073", items)  # still rejected: its order O0033 is an orphan

    def test_mixed_bronze_runs_change_no_table(self):
        """AC-18 (b)"""
        before = self._versions()
        self.spark.sql(
            f"UPDATE {self._table('products', self.names.bronze)} SET _run_id = 'another_run'")
        with self.assertRaisesRegex(SilverInputError, "single Bronze run"):
            transform_silver(self.spark, self.names.catalog, self.names.env)
        self.assertEqual(self._versions(), before)


class SilverMissingBronzeTest(_SilverEnvTest):
    def test_missing_bronze_tables_create_no_silver_table(self):
        """AC-18 (a): fresh env with raw files but no Bronze run."""
        with self.assertRaisesRegex(SilverInputError, "missing"):
            transform_silver(self.spark, self.names.catalog, self.names.env)
        for table in SILVER_TABLES:
            self.assertFalse(self._exists(table), table)


if __name__ == "__main__":
    unittest.main()
