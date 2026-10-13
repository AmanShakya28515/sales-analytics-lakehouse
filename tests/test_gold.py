"""Unit tests for Gold aggregates (step 04), derived from spec 04's acceptance criteria.

Spark tests feed small typed in-memory Silver DataFrames to `build_gold` and
assert the Gold rows. The shared input check is tested with a fake Spark
object. No table is read or written.

Scenario used by the main class (qualifying = completed or shipped):
  O1 C1 2024-01-01 web    completed  P1 2x10.00, P2 1x5.50, P1 1x10.00  -> 4 units, 35.50
  O2 C1 2024-01-01 web    completed  P2 3x5.50                          -> 3 units, 16.50
  O3 C2 2024-01-01 mobile shipped    P1 1x9.99                          -> 1 unit,   9.99
  O4 C1 2024-02-01 web    pending    P1 5x10.00                         -> excluded
  O5 C2 2024-01-02 store  cancelled  P4 1x7.00                          -> excluded
  O6 C2 2024-01-03 store  completed  (no Silver items)                  -> 0 units,  0.00
Totals: 4 qualifying orders, 8 units, revenue 61.99. C3 and P3/P4 have no sales.
"""

import datetime
import re
import unittest
from decimal import Decimal

from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)

import _context
from _context import REPO_ROOT
from sales_lakehouse.gold import (
    GOLD_SCHEMA, GoldInputError, METADATA_COLUMNS, QUALIFYING_STATUSES, build_gold,
    gold_table_statements, transform_gold,
)
from sales_lakehouse.inputs import pin_single_run
from sales_lakehouse.naming import layer_names
from sales_lakehouse.silver import SilverInputError

RUN_ID = "gold-run-1"
SILVER_RUN_ID = "silver-run-1"
PROCESSED_AT = datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc)
D = datetime.date
M = Decimal

_META = [StructField("_run_id", StringType(), False),
         StructField("_processed_at", TimestampType(), False),
         StructField("_bronze_run_id", StringType(), False)]
SILVER_SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType()), StructField("first_name", StringType()),
        StructField("last_name", StringType()), StructField("email", StringType()),
        StructField("city", StringType()), StructField("country", StringType()),
        StructField("signup_date", DateType()), StructField("updated_at", TimestampType()),
        *_META]),
    "products": StructType([
        StructField("product_id", StringType()), StructField("product_name", StringType()),
        StructField("category", StringType()), StructField("unit_price", DecimalType(10, 2)),
        StructField("is_active", BooleanType()), *_META]),
    "orders": StructType([
        StructField("order_id", StringType()), StructField("customer_id", StringType()),
        StructField("order_date", DateType()), StructField("status", StringType()),
        StructField("channel", StringType()), *_META]),
    "order_items": StructType([
        StructField("order_item_id", StringType()), StructField("order_id", StringType()),
        StructField("product_id", StringType()), StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)), *_META]),
}
_SILVER_META = (SILVER_RUN_ID, PROCESSED_AT, "bronze-run-1")


def customer(cid, country="United Kingdom"):
    return (cid, f"First{cid}", f"Last{cid}", f"{cid.lower()}@example.com", "London", country,
            D(2023, 1, 1), datetime.datetime(2023, 1, 1, 9, 0))


def product(pid, active=True):
    return (pid, f"Product {pid}", "Electronics", M("99.00"), active)


def order(oid, cid, date, status, channel):
    return (oid, cid, date, status, channel)


def item(iid, oid, pid, qty, price):
    return (iid, oid, pid, qty, M(price))


CUSTOMERS = [customer("C1"), customer("C2", country=None), customer("C3")]
PRODUCTS = [product("P1"), product("P2"), product("P3", active=False), product("P4")]
QUALIFYING_ORDERS = [
    order("O1", "C1", D(2024, 1, 1), "completed", "web"),
    order("O2", "C1", D(2024, 1, 1), "completed", "web"),
    order("O3", "C2", D(2024, 1, 1), "shipped", "mobile"),
    order("O6", "C2", D(2024, 1, 3), "completed", "store"),
]
OTHER_ORDERS = [
    order("O4", "C1", D(2024, 2, 1), "pending", "web"),
    order("O5", "C2", D(2024, 1, 2), "cancelled", "store"),
]
QUALIFYING_ITEMS = [
    item("I1", "O1", "P1", 2, "10.00"),
    item("I2", "O1", "P2", 1, "5.50"),
    item("I3", "O1", "P1", 1, "10.00"),
    item("I4", "O2", "P2", 3, "5.50"),
    item("I5", "O3", "P1", 1, "9.99"),
]
OTHER_ITEMS = [
    item("I6", "O4", "P1", 5, "10.00"),
    item("I7", "O5", "P4", 1, "7.00"),
]


def silver_frames(spark, customers=CUSTOMERS, products=PRODUCTS,
                  orders=QUALIFYING_ORDERS + OTHER_ORDERS,
                  order_items=QUALIFYING_ITEMS + OTHER_ITEMS):
    rows = {"customers": customers, "products": products, "orders": orders,
            "order_items": order_items}
    return {name: spark.createDataFrame([r + _SILVER_META for r in data], SILVER_SCHEMAS[name])
            for name, data in rows.items()}


def collect(gold):
    """{table: list of row dicts without run metadata}."""
    return {table: [{k: v for k, v in r.asDict().items() if k not in METADATA_COLUMNS}
                    for r in df.collect()]
            for table, df in gold.items()}


def keyed(rows, *keys):
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r for r in rows}


def as_set(rows):
    return {tuple(sorted(r.items())) for r in rows}


# -- no Spark needed -----------------------------------------------------------------

class GoldTableStatementsTest(unittest.TestCase):
    def test_three_rerunnable_delta_tables_in_gold_only(self):
        """AC-1"""
        statements = gold_table_statements(layer_names("cat", "dev"))
        self.assertEqual(len(statements), 3)
        created = set()
        for statement in statements:
            match = re.match(r"CREATE TABLE IF NOT EXISTS `cat`\.`dev_gold`\.`(\w+)` ", statement)
            self.assertIsNotNone(match, statement)
            created.add(match.group(1))
            self.assertIn("USING DELTA", statement)
            self.assertRegex(statement, r"COMMENT '[^']{20,}'$")
            self.assertNotIn("dev_bronze", statement)
            self.assertNotIn("dev_silver", statement)
        self.assertEqual(created, set(GOLD_SCHEMA))

    def test_columns_types_and_metadata(self):
        """AC-1, AC-2 to AC-5: documented columns, nullability, NOT NULL metadata."""
        for (table, columns), statement in zip(GOLD_SCHEMA.items(),
                                               gold_table_statements(layer_names("cat", "dev"))):
            with self.subTest(table=table):
                column_list = statement.split(" (", 1)[1].split(") USING DELTA", 1)[0]
                expected = [f"`{c.name}` {c.sql_type}" + ("" if c.nullable else " NOT NULL")
                            for c in columns] + [
                    "`_run_id` STRING NOT NULL", "`_processed_at` TIMESTAMP NOT NULL",
                    "`_silver_run_id` STRING NOT NULL"]
                self.assertEqual(column_list.split(", "), expected)
        nullable = {(t, c.name) for t, cols in GOLD_SCHEMA.items() for c in cols if c.nullable}
        self.assertEqual(nullable, {
            ("customer_summary", "country"), ("customer_summary", "avg_order_value"),
            ("customer_summary", "first_order_date"), ("customer_summary", "last_order_date"),
        })

    def test_qualifying_statuses(self):
        """Spec definition: completed and shipped only."""
        self.assertEqual(set(QUALIFYING_STATUSES), {"completed", "shipped"})


class _UntouchableSpark:
    """Fails the test if the transform touches Spark at all."""

    def __getattr__(self, name):
        raise AssertionError(f"Spark was used ({name}) before parameters were validated")


class TransformGuardsTest(unittest.TestCase):
    def test_invalid_parameters_stop_before_any_spark_call(self):
        """AC-11 (c)"""
        for catalog, env in (("bad name", "dev"), ("cat", "dev;drop"), ("", "dev")):
            with self.subTest(catalog=catalog, env=env):
                with self.assertRaises(ValueError):
                    transform_gold(_UntouchableSpark(), catalog, env)


class _FakeFrame:
    def __init__(self, run_ids):
        self.run_ids = run_ids

    def select(self, *_):
        return self

    def distinct(self):
        return _FakeFrame(sorted(set(self.run_ids)))

    def limit(self, n):
        return _FakeFrame(self.run_ids[:n])

    def collect(self):
        return [{"_run_id": r} for r in self.run_ids]


class _FakeSpark:
    """Tables -> list of row run ids; a missing key means the table does not exist."""

    def __init__(self, tables):
        self.tables = tables
        self.catalog = self

    def tableExists(self, name):
        return name.split(".")[-1] in self.tables

    def sql(self, statement):
        table = re.search(r"`(\w+)`(?: LIMIT| VERSION)", statement).group(1)
        if statement.startswith("DESCRIBE HISTORY"):
            return _FakeHistory()
        return _FakeFrame(self.tables[table])


class _FakeHistory:
    def first(self):
        return {"version": 7}


def _pin(tables, allow_empty, error=GoldInputError):
    return pin_single_run(
        _FakeSpark(tables), "cat", "dev_silver", list(("a", "b", "c")),
        layer="Silver", consumer="Gold", notebook="silver_transform", error=error,
        allow_empty=allow_empty,
    )


class PinSingleRunTest(unittest.TestCase):
    def test_one_run_is_returned_with_pinned_versions(self):
        """AC-11"""
        run_id, frames, versions = _pin({"a": ["r1", "r1"], "b": ["r1"], "c": ["r1"]}, True)
        self.assertEqual(run_id, "r1")
        self.assertEqual(versions, {"a": 7, "b": 7, "c": 7})
        self.assertEqual(set(frames), {"a", "b", "c"})

    def test_missing_table_is_named(self):
        """AC-11 (a)"""
        with self.assertRaisesRegex(GoldInputError, r"Silver tables missing .*\['c'\]"):
            _pin({"a": ["r1"], "b": ["r1"]}, True)

    def test_mixed_runs_fail(self):
        """AC-11 (b): across tables and inside one table."""
        for tables in ({"a": ["r1"], "b": ["r2"], "c": ["r1"]},
                       {"a": ["r1", "r2"], "b": ["r1"], "c": ["r1"]}):
            with self.subTest(tables=tables):
                with self.assertRaisesRegex(GoldInputError, "single Silver run"):
                    _pin(tables, True)

    def test_empty_tables_are_skipped_when_allowed(self):
        """Plan D2: an existing empty table passes; only non-empty tables are compared."""
        run_id, _, _ = _pin({"a": ["r1"], "b": [], "c": ["r1"]}, True)
        self.assertEqual(run_id, "r1")
        run_id, _, _ = _pin({"a": [], "b": [], "c": []}, True)
        self.assertIsNone(run_id)
        with self.assertRaisesRegex(GoldInputError, "single Silver run"):
            _pin({"a": ["r1"], "b": [], "c": ["r2"]}, True)

    def test_empty_table_fails_without_allow_empty(self):
        """Plan D1: Silver's stricter Bronze check is unchanged."""
        with self.assertRaisesRegex(SilverInputError, "single Silver run"):
            _pin({"a": ["r1"], "b": [], "c": ["r1"]}, False, error=SilverInputError)


class GoldDocsTest(unittest.TestCase):
    def test_data_dictionary_describes_gold(self):
        """AC-1: tables, qualifying statuses and the revenue definition are documented."""
        text = (REPO_ROOT / "docs" / "data_dictionary.md").read_text(encoding="utf-8")
        section = text.split("\n## Gold tables", 1)[1].split("\n## ", 1)[0]
        for table in GOLD_SCHEMA:
            self.assertIn(f"`<env>_gold.{table}`", section)
        for status in QUALIFYING_STATUSES:
            self.assertIn(f"`{status}`", section)
        self.assertIn("quantity × unit_price", section)


# -- Spark ---------------------------------------------------------------------------

class GoldScenarioTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = _context.require_spark()
        cls.gold = build_gold(silver_frames(cls.spark), RUN_ID, PROCESSED_AT, SILVER_RUN_ID)
        cls.rows = collect(cls.gold)

    def test_output_schemas_match_gold_schema(self):
        """AC-1"""
        for table, columns in GOLD_SCHEMA.items():
            with self.subTest(table=table):
                fields = self.gold[table].schema.fields
                self.assertEqual([(f.name, f.dataType.simpleString()) for f in fields],
                                 [(c.name, c.sql_type.lower()) for c in columns]
                                 + [("_run_id", "string"), ("_processed_at", "timestamp"),
                                    ("_silver_run_id", "string")])

    def test_daily_sales(self):
        """AC-2, AC-6, AC-7: one row per (date, channel) with qualifying orders only."""
        daily = keyed(self.rows["daily_sales"], "order_date", "channel")
        self.assertEqual(set(daily), {(D(2024, 1, 1), "web"), (D(2024, 1, 1), "mobile"),
                                      (D(2024, 1, 3), "store")})
        web = daily[(D(2024, 1, 1), "web")]
        self.assertEqual((web["order_count"], web["units"], web["revenue"], web["avg_order_value"]),
                         (2, 7, M("52.00"), M("26.00")))
        mobile = daily[(D(2024, 1, 1), "mobile")]
        self.assertEqual((mobile["order_count"], mobile["units"], mobile["revenue"]),
                         (1, 1, M("9.99")))
        store = daily[(D(2024, 1, 3), "store")]  # a qualifying order without Silver items
        self.assertEqual((store["order_count"], store["units"], store["revenue"],
                          store["avg_order_value"]), (1, 0, M("0.00"), M("0.00")))

    def test_product_sales(self):
        """AC-3, AC-6, AC-7: every product, zeros when unsold, distinct orders."""
        products = keyed(self.rows["product_sales"], "product_id")
        self.assertEqual(set(products), {"P1", "P2", "P3", "P4"})
        p1 = products["P1"]  # two lines in O1 count as one order
        self.assertEqual((p1["order_count"], p1["units"], p1["revenue"]), (2, 4, M("39.99")))
        p2 = products["P2"]
        self.assertEqual((p2["order_count"], p2["units"], p2["revenue"]), (2, 4, M("22.00")))
        for pid in ("P3", "P4"):  # never sold / only in a cancelled order
            p = products[pid]
            self.assertEqual((p["order_count"], p["units"], p["revenue"]), (0, 0, M("0.00")))
        self.assertEqual((products["P3"]["product_name"], products["P3"]["category"],
                          products["P3"]["is_active"]), ("Product P3", "Electronics", False))

    def test_customer_summary(self):
        """AC-4, AC-6, AC-7: every customer; zeros and nulls without qualifying orders."""
        customers = keyed(self.rows["customer_summary"], "customer_id")
        self.assertEqual(set(customers), {"C1", "C2", "C3"})
        c1 = customers["C1"]  # the later pending order is ignored
        self.assertEqual((c1["order_count"], c1["revenue"], c1["avg_order_value"],
                          c1["first_order_date"], c1["last_order_date"]),
                         (2, M("52.00"), M("26.00"), D(2024, 1, 1), D(2024, 1, 1)))
        c2 = customers["C2"]  # 9.99 / 2 = 4.995 rounds half-up to 5.00
        self.assertEqual((c2["order_count"], c2["revenue"], c2["avg_order_value"],
                          c2["first_order_date"], c2["last_order_date"], c2["country"]),
                         (2, M("9.99"), M("5.00"), D(2024, 1, 1), D(2024, 1, 3), None))
        c3 = customers["C3"]
        self.assertEqual((c3["order_count"], c3["revenue"], c3["avg_order_value"],
                          c3["first_order_date"], c3["last_order_date"]),
                         (0, M("0.00"), None, None, None))
        self.assertEqual((c3["first_name"], c3["email"]), ("FirstC3", "c3@example.com"))

    def test_keys_are_unique(self):
        """AC-7"""
        for table, keys in (("daily_sales", ("order_date", "channel")),
                            ("product_sales", ("product_id",)),
                            ("customer_summary", ("customer_id",))):
            with self.subTest(table=table):
                rows = self.rows[table]
                self.assertEqual(len(rows), len({tuple(r[k] for k in keys) for r in rows}))

    def test_totals_reconcile(self):
        """AC-8: revenue across all three, units across daily/product, orders across daily/customer."""
        total = {t: {c: sum(r[c] for r in rows) for c in ("revenue", "units", "order_count")
                     if c in rows[0]} for t, rows in self.rows.items()}
        for table in GOLD_SCHEMA:
            self.assertEqual(total[table]["revenue"], M("61.99"), table)
        self.assertEqual(total["daily_sales"]["units"], 8)
        self.assertEqual(total["product_sales"]["units"], 8)
        self.assertEqual(total["daily_sales"]["order_count"], len(QUALIFYING_ORDERS))
        self.assertEqual(total["customer_summary"]["order_count"], len(QUALIFYING_ORDERS))

    def test_every_row_carries_run_metadata(self):
        """AC-5"""
        for table, df in self.gold.items():
            with self.subTest(table=table):
                meta = {(r["_run_id"], r["_processed_at"], r["_silver_run_id"]) for r in df.collect()}
                self.assertEqual(len(meta), 1)
                run_id, processed_at, silver_run_id = meta.pop()
                self.assertEqual((run_id, silver_run_id), (RUN_ID, SILVER_RUN_ID))
                self.assertIsNotNone(processed_at)

    def test_non_qualifying_orders_change_nothing(self):
        """AC-6: the same build without pending/cancelled orders and their items."""
        without = collect(build_gold(
            silver_frames(self.spark, orders=QUALIFYING_ORDERS, order_items=QUALIFYING_ITEMS),
            RUN_ID, PROCESSED_AT, SILVER_RUN_ID))
        for table in GOLD_SCHEMA:
            self.assertEqual(as_set(without[table]), as_set(self.rows[table]), table)

    def test_same_input_twice_gives_same_rows(self):
        """AC-10 (build level)"""
        again = collect(build_gold(silver_frames(self.spark), "gold-run-2", PROCESSED_AT,
                                   SILVER_RUN_ID))
        for table in GOLD_SCHEMA:
            self.assertEqual(as_set(again[table]), as_set(self.rows[table]), table)


class GoldEmptyInputTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = _context.require_spark()

    def _assert_schema(self, gold):
        for table, columns in GOLD_SCHEMA.items():
            self.assertEqual([f.name for f in gold[table].schema.fields][:len(columns)],
                             [c.name for c in columns])

    def test_no_qualifying_orders(self):
        """AC-12: daily empty; every product and customer with zeros and nulls."""
        gold = build_gold(silver_frames(self.spark, orders=OTHER_ORDERS, order_items=OTHER_ITEMS),
                          RUN_ID, PROCESSED_AT, SILVER_RUN_ID)
        self._assert_schema(gold)
        rows = collect(gold)
        self.assertEqual(rows["daily_sales"], [])
        self.assertEqual(len(rows["product_sales"]), len(PRODUCTS))
        self.assertEqual(len(rows["customer_summary"]), len(CUSTOMERS))
        for r in rows["product_sales"]:
            self.assertEqual((r["order_count"], r["units"], r["revenue"]), (0, 0, M("0.00")))
        for r in rows["customer_summary"]:
            self.assertEqual((r["order_count"], r["revenue"], r["avg_order_value"],
                              r["first_order_date"], r["last_order_date"]),
                             (0, M("0.00"), None, None, None))

    def test_all_inputs_empty(self):
        """AC-12, plan D2: no Silver run id when every table is empty, and no error."""
        gold = build_gold(silver_frames(self.spark, customers=[], products=[], orders=[],
                                        order_items=[]), RUN_ID, PROCESSED_AT, None)
        self._assert_schema(gold)
        for table, df in gold.items():
            self.assertEqual(df.count(), 0, table)


if __name__ == "__main__":
    unittest.main()
