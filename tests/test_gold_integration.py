"""Runs Bronze, Silver then Gold end to end in throwaway t01_* schemas of the given catalog."""

import unittest
from decimal import Decimal

from pyspark.sql import functions as F

import _context
from sales_lakehouse.bronze import ingest_bronze
from sales_lakehouse.gold import (
    GOLD_SCHEMA, GoldInputError, METADATA_COLUMNS, QUALIFYING_STATUSES, transform_gold,
)
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import dataset_dir, load_sample_files
from sales_lakehouse.setup_ddl import run_setup
from sales_lakehouse.silver import transform_silver

GOLD_TABLES = tuple(GOLD_SCHEMA)
KEYS = {"daily_sales": ("order_date", "channel"), "product_sales": ("product_id",),
        "customer_summary": ("customer_id",)}


def _new_environment(spark, catalog):
    names = layer_names(catalog, _context.random_test_env())
    try:
        run_setup(spark, names.catalog, names.env)
        load_sample_files(str(_context.SAMPLE_ROOT), names.raw_volume_path)
    except Exception:
        _context.drop_test_environment(spark, names)
        raise
    return names


class _GoldEnvTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.catalog = _context.require_integration()
        cls.names = _new_environment(cls.spark, cls.catalog)

    @classmethod
    def tearDownClass(cls):
        _context.drop_test_environment(cls.spark, cls.names)

    @classmethod
    def _all_layers(cls):
        args = (cls.spark, cls.names.catalog, cls.names.env)
        ingest_bronze(*args)
        silver_run_id, _ = transform_silver(*args)
        run_id, report = transform_gold(*args)
        return silver_run_id, run_id, report

    def _table(self, name, schema=None):
        return quote(self.names.catalog, schema or self.names.gold, name)

    def _gold(self, name):
        return self.spark.table(self._table(name))

    def _silver(self, name):
        return self.spark.table(self._table(name, self.names.silver))

    def _exists(self, name):
        return self.spark.catalog.tableExists(f"{self.names.catalog}.{self.names.gold}.{name}")

    def _versions(self):
        return {t: self.spark.sql(f"DESCRIBE HISTORY {self._table(t)} LIMIT 1").first()["version"]
                for t in GOLD_TABLES}

    def _sum(self, table, column):
        return self._gold(table).agg(F.sum(column).alias("s")).first()["s"]

    def _silver_totals(self):
        """Revenue, units and orders computed straight from Silver, independently of Gold."""
        orders = self._silver("orders").where(F.col("status").isin(*QUALIFYING_STATUSES))
        items = self._silver("order_items").join(orders.select("order_id"), "order_id")
        row = items.agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
                        F.sum("quantity").alias("units")).first()
        return row["revenue"], row["units"], orders.count()


class GoldSampleDataTest(_GoldEnvTest):
    """One Bronze + Silver + Gold run over the sample data; these tests only read."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        try:
            cls.silver_run_id, cls.run_id, cls.report = cls._all_layers()
        except Exception:
            _context.drop_test_environment(cls.spark, cls.names)
            raise

    def test_tables_exist_as_delta_with_the_documented_schema(self):
        """AC-1"""
        for table, columns in GOLD_SCHEMA.items():
            with self.subTest(table=table):
                detail = self.spark.sql(f"DESCRIBE DETAIL {self._table(table)}").first()
                self.assertEqual(detail["format"], "delta")
                self.assertTrue(detail["description"])
                fields = {f.name: f for f in self._gold(table).schema.fields}
                self.assertEqual([f for f in fields if f not in METADATA_COLUMNS],
                                 [c.name for c in columns])
                for column in columns:
                    self.assertEqual(fields[column.name].dataType.simpleString(),
                                     column.sql_type.lower())
                    self.assertEqual(fields[column.name].nullable, column.nullable)

    def test_every_row_carries_one_run_lineage(self):
        """AC-5"""
        for table in GOLD_TABLES:
            with self.subTest(table=table):
                meta = self._gold(table).agg(
                    F.count(F.lit(1)).alias("rows"),
                    *[F.count(c).alias(c) for c in METADATA_COLUMNS],
                    *[F.countDistinct(c).alias(f"d{c}") for c in METADATA_COLUMNS],
                    F.first("_run_id").alias("run_id"),
                    F.first("_silver_run_id").alias("silver_run_id"),
                ).first()
                for column in METADATA_COLUMNS:
                    self.assertEqual(meta[column], meta["rows"])
                    self.assertEqual(meta[f"d{column}"], 1)
                self.assertEqual(meta["run_id"], self.run_id)
                self.assertEqual(meta["silver_run_id"], self.silver_run_id)

    def test_keys_are_unique(self):
        """AC-7"""
        for table, keys in KEYS.items():
            with self.subTest(table=table):
                df = self._gold(table)
                self.assertEqual(df.count(), df.select(*keys).distinct().count())

    def test_totals_reconcile_with_silver(self):
        """AC-8, AC-13: Gold equals an independent Silver-only computation."""
        revenue, units, orders = self._silver_totals()
        for table in GOLD_TABLES:
            self.assertEqual(self._sum(table, "revenue"), revenue, table)
        self.assertEqual(self._sum("daily_sales", "units"), units)
        self.assertEqual(self._sum("product_sales", "units"), units)
        self.assertEqual(self._sum("daily_sales", "order_count"), orders)
        self.assertEqual(self._sum("customer_summary", "order_count"), orders)

    def test_sample_data_outcome(self):
        """AC-9"""
        self.assertEqual(self._gold("daily_sales").count(), 29)
        products = self._gold("product_sales")
        self.assertEqual(products.count(), 14)
        self.assertEqual(products.where(F.col("order_count") == 0).count(), 1)
        customers = self._gold("customer_summary")
        self.assertEqual(customers.count(), 20)
        self.assertEqual(customers.where(F.col("order_count") == 0).count(), 2)
        self.assertEqual(self._sum("daily_sales", "order_count"), 29)
        self.assertEqual(self._sum("daily_sales", "units"), 113)
        self.assertEqual(self._sum("daily_sales", "revenue"), Decimal("9837.12"))

    def test_quarantined_rows_never_reach_gold(self):
        """AC-13: P015 and the customer without an id are absent."""
        self.assertEqual(self._gold("product_sales").where(F.col("product_id") == "P015").count(), 0)
        self.assertEqual(self._gold("customer_summary")
                         .where(F.col("customer_id").isNull()).count(), 0)

    def test_report(self):
        """Notebook report: rows and totals per table."""
        self.assertEqual({t: r["rows"] for t, r in self.report.items()},
                         {"daily_sales": 29, "product_sales": 14, "customer_summary": 20})
        for table in GOLD_TABLES:
            self.assertEqual(self.report[table]["revenue"], Decimal("9837.12"))


class GoldRerunTest(_GoldEnvTest):
    """Tests that change the input; each starts from a fresh Bronze + Silver + Gold run."""

    def setUp(self):
        load_sample_files(str(_context.SAMPLE_ROOT), self.names.raw_volume_path)
        self.silver_run_id, self.run_id, _ = self._all_layers()

    def test_rerun_gives_the_same_rows(self):
        """AC-10"""
        before = self._versions()
        run_id, _ = transform_gold(self.spark, self.names.catalog, self.names.env)
        self.assertNotEqual(run_id, self.run_id)
        for table in GOLD_TABLES:
            with self.subTest(table=table):
                previous = self.spark.sql(
                    f"SELECT * FROM {self._table(table)} VERSION AS OF {before[table]}"
                ).drop(*METADATA_COLUMNS)
                after = self._gold(table).drop(*METADATA_COLUMNS)
                self.assertEqual(after.count(), previous.count())
                self.assertEqual(after.exceptAll(previous).count(), 0)
                self.assertEqual(previous.exceptAll(after).count(), 0)
                self.assertEqual(self._gold(table).select("_run_id").distinct().count(), 1)

    def test_cancelled_order_drops_out(self):
        """AC-10, AC-6: nothing is carried over when Silver changes."""
        o0001 = (self._silver("order_items").where(F.col("order_id") == "O0001")
                 .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("r")).first()["r"])
        revenue_before = self._sum("daily_sales", "revenue")

        path = f"{dataset_dir(self.names.raw_volume_path, 'orders')}/orders.json"
        with open(path, encoding="utf-8") as f:
            text = f.read()
        old = '"order_id": "O0001", "customer_id": "C008", "order_date": "2024-01-04", "status": "completed"'
        self.assertIn(old, text)
        # Volume files cannot be appended to; rewrite the whole file.
        with open(path, "w", encoding="utf-8") as f:
            f.write(text.replace(old, old.replace("completed", "cancelled")))

        self._all_layers()
        for table in GOLD_TABLES:
            self.assertEqual(self._sum(table, "revenue"), revenue_before - o0001, table)
        self.assertEqual(self._sum("daily_sales", "order_count"), 28)
        self.assertEqual(self._sum("customer_summary", "order_count"), 28)

    def test_mixed_silver_runs_change_no_table(self):
        """AC-11 (b)"""
        before = self._versions()
        self.spark.sql(
            f"UPDATE {self._table('products', self.names.silver)} SET _run_id = 'another_run'")
        with self.assertRaisesRegex(GoldInputError, "single Silver run"):
            transform_gold(self.spark, self.names.catalog, self.names.env)
        self.assertEqual(self._versions(), before)

    def test_empty_silver_table_is_accepted(self):
        """Plan D2, AC-12: an existing but empty Silver table does not fail the run."""
        self.spark.sql(f"DELETE FROM {self._table('order_items', self.names.silver)}")
        transform_gold(self.spark, self.names.catalog, self.names.env)
        self.assertEqual(self._sum("product_sales", "revenue"), Decimal("0.00"))
        self.assertEqual(self._sum("customer_summary", "revenue"), Decimal("0.00"))
        self.assertEqual(self._sum("daily_sales", "order_count"), 29)


class GoldMissingSilverTest(_GoldEnvTest):
    def test_missing_silver_tables_create_no_gold_table(self):
        """AC-11 (a): Bronze exists but Silver was never run."""
        ingest_bronze(self.spark, self.names.catalog, self.names.env)
        with self.assertRaisesRegex(GoldInputError, "missing"):
            transform_gold(self.spark, self.names.catalog, self.names.env)
        for table in GOLD_TABLES:
            self.assertFalse(self._exists(table), table)


if __name__ == "__main__":
    unittest.main()
