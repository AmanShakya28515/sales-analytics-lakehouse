"""Runs the dashboard queries (step 05) through spark.sql on Gold built in throwaway t01_* schemas.

The queries run exactly as the dashboard runs them: the same `.sql` text with
`:catalog` and `:env` bound as parameters (plan 05 D2).
"""

import datetime
import unittest
from decimal import Decimal

import _context
from sales_lakehouse.bronze import ingest_bronze
from sales_lakehouse.dashboard import QUERIES, load_query, run_query
from sales_lakehouse.gold import GOLD_SCHEMA, transform_gold
from sales_lakehouse.naming import quote
from sales_lakehouse.raw_datasets import dataset_dir, load_sample_files
from sales_lakehouse.silver import transform_silver
from test_gold_integration import _new_environment

GOLD_TABLES = tuple(GOLD_SCHEMA)
LISTS = ("top_products", "unsold_products", "top_customers", "inactive_customers")


def _build_environment(spark, catalog, edit_orders=None):
    """Create a t01_* env, load the sample files (optionally edited), run Bronze, Silver, Gold."""
    names = _new_environment(spark, catalog)
    try:
        if edit_orders:
            path = f"{dataset_dir(names.raw_volume_path, 'orders')}/orders.json"
            with open(path, encoding="utf-8") as f:
                text = f.read()
            # Volume files cannot be appended to; rewrite the whole file.
            with open(path, "w", encoding="utf-8") as f:
                f.write(edit_orders(text))
        args = (spark, names.catalog, names.env)
        ingest_bronze(*args)
        transform_silver(*args)
        transform_gold(*args)
    except Exception:
        _context.drop_test_environment(spark, names)
        raise
    return names


def _cancel_every_order(text):
    for status in ("completed", "shipped"):
        text = text.replace(f'"status": "{status}"', '"status": "cancelled"')
    return text


class _DashboardTest(unittest.TestCase):
    def _run(self, name, names=None):
        names = names or self.names
        return run_query(self.spark, name, names.catalog, names.env).collect()

    def _versions(self, names):
        return {t: self.spark.sql(
                    f"DESCRIBE HISTORY {quote(names.catalog, names.gold, t)} LIMIT 1"
                ).first()["version"]
                for t in GOLD_TABLES}


class DashboardSampleDataTest(_DashboardTest):
    """One Bronze + Silver + Gold run over the sample data; the queries only read."""

    @classmethod
    def setUpClass(cls):
        cls.spark, cls.catalog = _context.require_integration()
        cls.names = _build_environment(cls.spark, cls.catalog)

    @classmethod
    def tearDownClass(cls):
        _context.drop_test_environment(cls.spark, cls.names)

    def test_kpis(self):
        """AC-1"""
        [row] = self._run("kpis")
        self.assertEqual(row["total_revenue"], Decimal("9837.12"))
        self.assertEqual(row["qualifying_orders"], 29)
        self.assertEqual(row["units"], 113)
        self.assertEqual(row["avg_order_value"], Decimal("339.21"))
        self.assertEqual(row["active_customers"], 18)

    def test_revenue_trend(self):
        """AC-2"""
        rows = self._run("revenue_trend")
        months = [r["month"] for r in rows]
        self.assertEqual(months, sorted(months))
        self.assertEqual(sorted(set(months)),
                         [datetime.date(2024, m, 1) for m in range(1, 7)])
        self.assertEqual(len(rows), len({(r["month"], r["channel"]) for r in rows}))
        self.assertEqual(sum(r["revenue"] for r in rows), Decimal("9837.12"))
        by_channel = {}
        for r in rows:
            by_channel[r["channel"]] = by_channel.get(r["channel"], 0) + r["revenue"]
        self.assertEqual(by_channel, {"mobile": Decimal("3732.65"), "store": Decimal("3104.87"),
                                      "web": Decimal("2999.60")})

    def test_top_and_unsold_products(self):
        """AC-3"""
        rows = self._run("top_products")
        self.assertEqual([(r["product_id"], r["revenue"]) for r in rows[:3]],
                         [("P009", Decimal("2835.00")), ("P012", Decimal("1937.00")),
                          ("P010", Decimal("1047.00"))])
        self.assertLessEqual({"product_name", "category", "units", "order_count"},
                             set(rows[0].asDict()))
        self.assertEqual([r["product_id"] for r in self._run("unsold_products")], ["P001"])

    def test_top_and_inactive_customers(self):
        """AC-4"""
        rows = self._run("top_customers")
        self.assertEqual([(r["customer_id"], r["revenue"]) for r in rows[:3]],
                         [("C013", Decimal("1860.00")), ("C017", Decimal("1116.80")),
                          ("C018", Decimal("949.50"))])
        self.assertLessEqual({"first_name", "last_name", "order_count", "last_order_date"},
                             set(rows[0].asDict()))
        self.assertEqual(len(self._run("inactive_customers")), 2)

    def test_revenue_totals_agree_with_gold(self):
        """AC-5"""
        gold = self.spark.sql(
            f"SELECT sum(revenue) AS r FROM {quote(self.names.catalog, self.names.gold, 'daily_sales')}"
        ).first()["r"]
        totals = {
            "kpis": self._run("kpis")[0]["total_revenue"],
            "revenue_trend": sum(r["revenue"] for r in self._run("revenue_trend")),
            "top_products": sum(r["revenue"] for r in self._run("top_products")),
            "top_customers": sum(r["revenue"] for r in self._run("top_customers")),
        }
        for name, total in totals.items():
            self.assertEqual(total, gold, name)

    def test_queries_change_no_table(self):
        """AC-10"""
        before = self._versions(self.names)
        tables = self.spark.sql(
            f"SHOW TABLES IN {quote(self.names.catalog, self.names.gold)}").collect()
        for name in QUERIES:
            self._run(name)
        self.assertEqual(self._versions(self.names), before)
        self.assertEqual(self.spark.sql(
            f"SHOW TABLES IN {quote(self.names.catalog, self.names.gold)}").collect(), tables)

    def test_invalid_parameter_values_are_rejected(self):
        """AC-8: raw SQL with a bad value fails instead of reading another schema."""
        for env in ("a.b", "x`y", f"{self.names.env}_gold`.`daily_sales"):
            with self.subTest(env=env):
                with self.assertRaises(Exception):
                    self.spark.sql(load_query("kpis"),
                                   args={"catalog": self.names.catalog, "env": env}).collect()


class DashboardEdgeCaseTest(_DashboardTest):
    """Ties, empty data and switching environments, each in its own t01_* env."""

    @classmethod
    def setUpClass(cls):
        cls.spark, cls.catalog = _context.require_integration()
        cls.envs = []
        try:
            cls.names = _build_environment(cls.spark, cls.catalog)
            cls.envs.append(cls.names)
            cls.empty = _build_environment(cls.spark, cls.catalog, _cancel_every_order)
            cls.envs.append(cls.empty)
            # Two products and two customers with equal revenue.
            gold = quote(cls.names.catalog, cls.names.gold)
            cls.spark.sql(f"UPDATE {gold}.`product_sales` SET revenue = 2835.00 "
                          "WHERE product_id = 'P012'")
            cls.spark.sql(f"UPDATE {gold}.`customer_summary` SET revenue = 1860.00 "
                          "WHERE customer_id = 'C017'")
        except Exception:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        for names in cls.envs:
            _context.drop_test_environment(cls.spark, names)

    def test_ties_are_broken_by_id(self):
        """AC-6"""
        self.assertEqual([r["product_id"] for r in self._run("top_products")[:2]],
                         ["P009", "P012"])
        self.assertEqual([r["customer_id"] for r in self._run("top_customers")[:2]],
                         ["C013", "C017"])

    def test_no_qualifying_orders(self):
        """AC-6: zeros, an empty average, no rows, and no error."""
        [row] = self._run("kpis", self.empty)
        self.assertEqual((row["total_revenue"], row["qualifying_orders"], row["units"],
                          row["active_customers"]), (0, 0, 0, 0))
        self.assertIsNone(row["avg_order_value"])
        for name in ("revenue_trend", "top_products", "top_customers"):
            self.assertEqual(self._run(name, self.empty), [], name)
        # Every product and customer is now unsold or inactive.
        self.assertEqual(len(self._run("unsold_products", self.empty)), 14)
        self.assertEqual(len(self._run("inactive_customers", self.empty)), 20)

    def test_env_parameter_switches_the_data(self):
        """AC-7: the same query text reads whichever env it is given."""
        sample = self._run("kpis", self.names)[0]
        empty = self._run("kpis", self.empty)[0]
        self.assertEqual(sample["total_revenue"], Decimal("9837.12"))
        self.assertEqual(empty["total_revenue"], 0)
        self.assertNotEqual(sample["qualifying_orders"], empty["qualifying_orders"])


if __name__ == "__main__":
    unittest.main()
