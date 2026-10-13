"""Unit tests for Silver cleaning (step 03), derived from spec 03's acceptance criteria.

Spark tests feed tiny in-memory Bronze DataFrames to `build_silver` and assert
the Silver rows, the quarantine rows and their reasons. No table is read or
written. Reasons are compared as sorted (reason, column) pairs, never by order.
"""

import datetime
import re
import unittest
from collections import Counter
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

import _context
from _context import REPO_ROOT
from sales_lakehouse.naming import layer_names
from sales_lakehouse.raw_datasets import DATASETS
from sales_lakehouse.silver import (
    QUARANTINE_TABLE, REASONS, build_silver, silver_table_statements, transform_silver,
)

BRONZE_RUN_ID = "bronze-run-1"
RUN_ID = "silver-run-1"
INGESTED_AT = datetime.datetime(2026, 10, 13, 8, 0, tzinfo=datetime.timezone.utc)
PROCESSED_AT = datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc)

PK = {d.name: d.primary_key for d in DATASETS}
SILVER_TABLES = {d.name for d in DATASETS} | {QUARANTINE_TABLE}

# Intended types from the data dictionary / plan table (AC-1), in dictionary order.
METADATA_TYPES = [("_run_id", "string"), ("_processed_at", "timestamp"), ("_bronze_run_id", "string")]
EXPECTED_TYPES = {
    "customers": [("customer_id", "string"), ("first_name", "string"), ("last_name", "string"),
                  ("email", "string"), ("city", "string"), ("country", "string"),
                  ("signup_date", "date"), ("updated_at", "timestamp")],
    "products": [("product_id", "string"), ("product_name", "string"), ("category", "string"),
                 ("unit_price", "decimal(10,2)"), ("is_active", "boolean")],
    "orders": [("order_id", "string"), ("customer_id", "string"), ("order_date", "date"),
               ("status", "string"), ("channel", "string")],
    "order_items": [("order_item_id", "string"), ("order_id", "string"), ("product_id", "string"),
                    ("quantity", "int"), ("unit_price", "decimal(10,2)")],
}
QUARANTINE_TYPES = dict([
    ("dataset", "string"), ("record_key", "string"),
    ("reasons", "array<struct<reason:string,column:string>>"),
    ("source_record", "map<string,string>"), ("_source_file", "string"), *METADATA_TYPES,
])

# Clean values; each test overrides only what it needs. Parents C001/P001/O0001 are valid.
DEFAULTS = {
    "customers": dict(first_name="Ava", last_name="Thornton", email="ava@example.com",
                      city="London", country="United Kingdom", signup_date="2023-01-15",
                      updated_at="2023-01-15T09:00:00"),
    "products": dict(product_name="Wireless Mouse", category="Electronics",
                     unit_price="24.99", is_active="true"),
    "orders": dict(customer_id="C001", order_date="2024-01-04", status="completed", channel="web"),
    "order_items": dict(order_id="O0001", product_id="P001", quantity="1", unit_price="10.00"),
}


def row(dataset, key, **values):
    """One Bronze row (dict of text values) with clean defaults."""
    return {PK[dataset]: key, **DEFAULTS[dataset], **values}


def base_rows():
    return {
        "customers": [row("customers", "C001")],
        "products": [row("products", "P001")],
        "orders": [row("orders", "O0001")],
        "order_items": [row("order_items", "OI0001")],
    }


def source_file(dataset):
    return f"/Volumes/cat/dev_bronze/raw_data/{dataset.name}/{dataset.file_name}"


def bronze_frames(spark, **rows_by_dataset):
    """Bronze-shaped DataFrames: all-STRING source columns plus Bronze metadata."""
    defaults = base_rows()
    frames = {}
    for dataset in DATASETS:
        rows = rows_by_dataset.get(dataset.name, defaults[dataset.name])
        schema = StructType(
            [StructField(c, StringType(), True) for c in dataset.columns]
            + [StructField("_ingested_at", TimestampType(), False),
               StructField("_source_file", StringType(), False),
               StructField("_run_id", StringType(), False)]
        )
        data = [tuple(r.get(c) for c in dataset.columns)
                + (INGESTED_AT, source_file(dataset), BRONZE_RUN_ID) for r in rows]
        frames[dataset.name] = spark.createDataFrame(data, schema)
    return frames


def pairs(reasons):
    """Sorted (reason, column) pairs; duplicates are kept on purpose."""
    return tuple(sorted(((r["reason"], r["column"]) for r in reasons),
                        key=lambda p: (p[0] or "", p[1] or "")))


def P(*items):
    return tuple(sorted(items, key=lambda p: (p[0] or "", p[1] or "")))


class Outcome:
    """Collected result of one build: Silver rows per dataset and quarantine rows."""

    def __init__(self, build):
        self.build = build
        self.silver = {name: build.tables[name].collect() for name in PK}
        self.quarantine = [
            {"dataset": q["dataset"], "key": q["record_key"], "reasons": pairs(q["reasons"]),
             "source": dict(q["source_record"]), "file": q["_source_file"]}
            for q in build.quarantine.collect()
        ]

    def keys(self, dataset):
        return [r[PK[dataset]] for r in self.silver[dataset]]

    def by_key(self, dataset):
        return {r[PK[dataset]]: r for r in self.silver[dataset]}

    def rejected(self, dataset=None):
        """Counter of (dataset, record_key, reasons) for exact comparisons."""
        return Counter((q["dataset"], q["key"], q["reasons"]) for q in self.quarantine
                       if dataset is None or q["dataset"] == dataset)

    def rejected_names(self, dataset):
        """Counter of (record_key, sorted distinct reason names)."""
        return Counter((q["key"], tuple(sorted({r for r, _ in q["reasons"]})))
                       for q in self.quarantine if q["dataset"] == dataset)


class SilverAssertions:
    def assert_pk_non_null_unique(self, outcome):
        """AC-12"""
        for dataset in PK:
            keys = outcome.keys(dataset)
            self.assertNotIn(None, keys, dataset)
            self.assertEqual(len(keys), len(set(keys)), f"{dataset}: duplicate keys {keys}")

    def assert_full_schema(self, build):
        """AC-1, AC-19"""
        for dataset, expected in EXPECTED_TYPES.items():
            self.assertEqual(build.tables[dataset].dtypes, expected + METADATA_TYPES, dataset)
        self.assertEqual(dict(build.quarantine.dtypes), QUARANTINE_TYPES)


# -- DDL, guards and docs (no Spark) ------------------------------------------------

class SilverTableStatementsTest(unittest.TestCase):
    def setUp(self):
        self.statements = silver_table_statements(layer_names("cat", "dev"))

    def test_five_rerunnable_delta_tables_in_silver_schema_only(self):
        """AC-1: 4 data tables + quarantine, all IF NOT EXISTS in <env>_silver."""
        self.assertEqual(len(self.statements), 5)
        created = set()
        for statement in self.statements:
            match = re.match(r"CREATE TABLE IF NOT EXISTS `cat`\.`dev_silver`\.`(\w+)` ", statement)
            self.assertIsNotNone(match, statement)
            created.add(match.group(1))
            self.assertIn("USING DELTA", statement)
            self.assertNotIn("_bronze`", statement)
            self.assertNotIn("_gold`", statement)
        self.assertEqual(created, SILVER_TABLES)

    def test_columns_have_intended_types_and_metadata(self):
        """AC-1, AC-2"""
        by_table = {re.search(r"`dev_silver`\.`(\w+)`", s).group(1): s for s in self.statements}
        expected = {
            "customers": ["`customer_id` STRING NOT NULL", "`city` STRING", "`country` STRING",
                          "`signup_date` DATE NOT NULL", "`updated_at` TIMESTAMP NOT NULL"],
            "products": ["`unit_price` DECIMAL(10,2) NOT NULL", "`is_active` BOOLEAN NOT NULL"],
            "orders": ["`order_date` DATE NOT NULL", "`status` STRING NOT NULL",
                       "`customer_id` STRING NOT NULL"],
            "order_items": ["`quantity` INT NOT NULL", "`unit_price` DECIMAL(10,2) NOT NULL",
                            "`order_id` STRING NOT NULL", "`product_id` STRING NOT NULL"],
        }
        for table, fragments in expected.items():
            column_list = by_table[table].split(" (", 1)[1].split(") USING DELTA", 1)[0].split(", ")
            with self.subTest(table=table):
                for fragment in fragments:
                    self.assertIn(fragment, column_list)
                self.assertEqual([c.split(" ", 1)[0].strip("`") for c in column_list],
                                 [c for c, _ in EXPECTED_TYPES[table] + METADATA_TYPES])
        for table in SILVER_TABLES:
            with self.subTest(table=table):
                for column in ("_run_id", "_processed_at", "_bronze_run_id"):
                    self.assertRegex(by_table[table], rf"`{column}` \w+ NOT NULL")
        quarantine = by_table[QUARANTINE_TABLE]
        for column in ("dataset", "record_key", "reasons", "source_record"):
            self.assertIn(f"`{column}`", quarantine)


class _UntouchableSpark:
    """Fails the test if the transform touches Spark at all."""

    def __init__(self):
        self.touched = []

    def __getattr__(self, name):
        self.touched.append(name)
        raise AssertionError(f"Spark was used ({name}) before parameters were validated")


class TransformGuardsTest(unittest.TestCase):
    def test_invalid_parameters_stop_before_any_spark_call(self):
        """AC-18(c)"""
        cases = [("x; DROP", "dev", None), ("cat", "a`b", None), ("", "dev", None),
                 ("cat", "", None), ("cat", "dev", "r" * 65)]
        for catalog, env, run_id in cases:
            with self.subTest(catalog=catalog, env=env):
                spark = _UntouchableSpark()
                with self.assertRaises(ValueError):
                    transform_silver(spark, catalog, env, run_id)
                self.assertEqual(spark.touched, [])


class SilverDocsTest(unittest.TestCase):
    def test_data_dictionary_names_silver_tables_and_reason_codes(self):
        """docs (plan 03 test plan)"""
        text = (REPO_ROOT / "docs" / "data_dictionary.md").read_text(encoding="utf-8")
        for table in sorted(SILVER_TABLES):
            self.assertIn(f"<env>_silver.{table}", text)
        for reason in REASONS:
            self.assertIn(reason, text)
        self.assertEqual(set(REASONS), {
            "null_key", "missing_value", "malformed_value", "invalid_value",
            "conflicting_duplicate", "orphan_reference", "parent_rejected",
        })


# -- Row rules: one build covering AC-1..AC-8, AC-12 --------------------------------

class RowRulesTest(SilverAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = _context.require_spark()
        c, p, o, i = "customers", "products", "orders", "order_items"
        cls.frames = bronze_frames(
            spark,
            customers=[
                row(c, "  C001 ", first_name=" Ava "),                    # AC-3 trimmed, valid
                row(c, "C002", city="", country="   "),                  # AC-3 optional '' -> NULL, valid
                row(c, "C003", first_name=""),                           # AC-3/AC-5 '' required
                row(c, "C004", email=None),                              # AC-5
                row(c, "C005", updated_at="not-a-time"),                 # AC-6 timestamp
                row(c, "C006", signup_date="2024-04-31"),                # AC-6 impossible date
                row(c, None, first_name="Jordan", signup_date="2024-04-31"),  # AC-4 + AC-8
                row(c, "   ", first_name="Blank"),                       # AC-4 blank key
            ],
            products=[
                row(p, "P001", unit_price="0.01"),                       # AC-7 boundary valid
                row(p, "P002", is_active="FALSE"),                       # boolean, any case
                row(p, "P003", unit_price="abc"),                        # AC-6
                row(p, "P004", unit_price="0"),                          # AC-7
                row(p, "P005", unit_price="-1"),                         # AC-7
                row(p, "P006", is_active="yes"),                         # AC-6
            ],
            orders=[
                row(o, "O0001"),
                row(o, "O0002", order_date="2024-04-31"),                # AC-6
                row(o, "O0003", status="lost"),                          # AC-7
                row(o, "O0004", channel="fax"),                          # AC-7
                row(o, "O0005", customer_id="C002", status=" shipped "),  # AC-3 trimmed, valid
            ],
            order_items=[
                row(i, "OI0001", quantity="1"),                          # AC-7 boundary valid
                row(i, "OI0002", quantity=" x "),                        # AC-6
                row(i, "OI0003", quantity="0"),                          # AC-7
                row(i, "OI0004", unit_price="0"),                        # AC-7
                row(i, "OI0006", quantity=""),                           # AC-5
            ],
        )
        cls.build = build_silver(cls.frames, RUN_ID, PROCESSED_AT)
        cls.out = Outcome(cls.build)

    def test_valid_rows_reach_silver_and_rejected_rows_do_not(self):
        """AC-4..AC-7: a rejected row is in quarantine and not in its Silver table."""
        self.assertEqual(set(self.out.keys("customers")), {"C001", "C002"})
        self.assertEqual(set(self.out.keys("products")), {"P001", "P002"})
        self.assertEqual(set(self.out.keys("orders")), {"O0001", "O0005"})
        self.assertEqual(set(self.out.keys("order_items")), {"OI0001"})

    def test_each_rejection_has_exactly_its_reasons(self):
        """AC-4, AC-5, AC-6, AC-7, AC-8"""
        expected = Counter({
            ("customers", "C003", P(("missing_value", "first_name"))): 1,
            ("customers", "C004", P(("missing_value", "email"))): 1,
            ("customers", "C005", P(("malformed_value", "updated_at"))): 1,
            ("customers", "C006", P(("malformed_value", "signup_date"))): 1,
            ("customers", None, P(("null_key", "customer_id"),
                                  ("malformed_value", "signup_date"))): 1,
            ("customers", None, P(("null_key", "customer_id"))): 1,
            ("products", "P003", P(("malformed_value", "unit_price"))): 1,
            ("products", "P004", P(("invalid_value", "unit_price"))): 1,
            ("products", "P005", P(("invalid_value", "unit_price"))): 1,
            ("products", "P006", P(("malformed_value", "is_active"))): 1,
            ("orders", "O0002", P(("malformed_value", "order_date"))): 1,
            ("orders", "O0003", P(("invalid_value", "status"))): 1,
            ("orders", "O0004", P(("invalid_value", "channel"))): 1,
            ("order_items", "OI0002", P(("malformed_value", "quantity"))): 1,
            ("order_items", "OI0003", P(("invalid_value", "quantity"))): 1,
            ("order_items", "OI0004", P(("invalid_value", "unit_price"))): 1,
            ("order_items", "OI0006", P(("missing_value", "quantity"))): 1,
        })
        self.assertEqual(self.out.rejected(), expected)

    def test_values_are_trimmed_typed_and_empty_text_is_null(self):
        """AC-1, AC-3"""
        customers = self.out.by_key("customers")
        self.assertEqual(customers["C001"]["first_name"], "Ava")
        self.assertEqual(customers["C001"]["signup_date"], datetime.date(2023, 1, 15))
        self.assertEqual(customers["C001"]["updated_at"], datetime.datetime(2023, 1, 15, 9, 0))
        self.assertIsNone(customers["C002"]["city"])
        self.assertIsNone(customers["C002"]["country"])
        products = self.out.by_key("products")
        self.assertEqual(products["P001"]["unit_price"], Decimal("0.01"))
        self.assertIs(products["P001"]["is_active"], True)
        self.assertIs(products["P002"]["is_active"], False)
        self.assertEqual(self.out.by_key("orders")["O0005"]["status"], "shipped")
        item = self.out.by_key("order_items")["OI0001"]
        self.assertEqual((item["quantity"], item["unit_price"]), (1, Decimal("10.00")))

    def test_quarantine_keeps_original_text_and_source_file(self):
        """AC-15 (unit level): source_record holds the untrimmed Bronze text."""
        by_key = {(q["dataset"], q["key"]): q for q in self.out.quarantine if q["key"]}
        self.assertEqual(by_key[("products", "P003")]["source"]["unit_price"], "abc")
        self.assertEqual(by_key[("order_items", "OI0002")]["source"]["quantity"], " x ")
        self.assertEqual(by_key[("products", "P003")]["file"], source_file(DATASETS[1]))
        blank = [q for q in self.out.quarantine
                 if q["dataset"] == "customers" and q["source"].get("first_name") == "Blank"]
        self.assertEqual(len(blank), 1)
        self.assertEqual(blank[0]["source"]["customer_id"], "   ")

    def test_output_schemas_use_intended_types(self):
        """AC-1"""
        self.assert_full_schema(self.build)

    def test_every_row_carries_run_metadata(self):
        """AC-2: Silver run id, processed time and Bronze run id on every row."""
        expected = ((F.col("_run_id") == RUN_ID)
                    & (F.col("_processed_at") == F.lit(PROCESSED_AT))
                    & (F.col("_bronze_run_id") == BRONZE_RUN_ID))
        for name, df in [*self.build.tables.items(), ("quarantine", self.build.quarantine)]:
            with self.subTest(table=name):
                counts = df.agg(F.count(F.lit(1)).alias("rows"),
                                F.count(F.when(expected, 1)).alias("ok")).first()
                self.assertGreater(counts["rows"], 0)
                self.assertEqual(counts["ok"], counts["rows"])

    def test_primary_keys_are_non_null_and_unique(self):
        """AC-12"""
        self.assert_pk_non_null_unique(self.out)


# -- Duplicates: one build covering AC-9..AC-12 -------------------------------------

class DuplicatesTest(SilverAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = _context.require_spark()
        c, p, o, i = "customers", "products", "orders", "order_items"
        old, new = "2024-01-10T09:00:00", "2024-03-18T09:00:00"
        cls.frames = bronze_frames(
            spark,
            customers=[
                *[row(c, "C010")] * 3,                                   # AC-9: 3 identical
                row(c, "C011", first_name="Ava"),                        # AC-9: whitespace-only
                row(c, " C011 ", first_name="Ava  "),
                row(c, "C012", updated_at=new, city="York"),             # AC-10: newest first
                row(c, "C012", updated_at=old, city="Leeds"),
                row(c, "C013", updated_at=old, city="Leeds"),            # AC-10: oldest first
                row(c, "C013", updated_at=new, city="York"),
                row(c, "C014", updated_at="2024-01-01T09:00:00"),        # D3: older valid
                row(c, "C014", updated_at="2024-02-01T09:00:00", signup_date="2024-04-31"),
                row(c, "C015", updated_at=new, email="a@example.com"),   # AC-11: tie
                row(c, "C015", updated_at=new, email="b@example.com"),
                row(c, "C016", updated_at=old),                          # AC-11: unparseable
                row(c, "C016", updated_at="garbage"),
            ],
            products=[
                row(p, "P001"),
                row(p, "P020", unit_price="1.00"),                       # AC-11: products
                row(p, "P020", unit_price="2.00"),
                *[row(p, "P021")] * 2,                                   # AC-9
            ],
            orders=[
                row(o, "O0001", customer_id="C010"),
                row(o, "O0020", customer_id="C010", status="pending"),   # AC-11: orders
                row(o, "O0020", customer_id="C010", status="shipped"),
                *[row(o, "O0021", customer_id="C010")] * 2,              # AC-9
            ],
            order_items=[
                row(i, "OI0001"),
                row(i, "OI0020", quantity="1"),                          # AC-11: order items
                row(i, "OI0020", quantity="2"),
            ],
        )
        cls.build = build_silver(cls.frames, RUN_ID, PROCESSED_AT)
        cls.out = Outcome(cls.build)

    def test_identical_rows_collapse_to_one_silver_row(self):
        """AC-9: exact duplicates (also after trimming) are collapsed, not rejected."""
        for dataset, key in (("customers", "C010"), ("customers", "C011"),
                             ("products", "P021"), ("orders", "O0021")):
            with self.subTest(key=key):
                self.assertEqual(self.out.keys(dataset).count(key), 1)
                self.assertNotIn(key, {q["key"] for q in self.out.quarantine})

    def test_latest_updated_at_wins_whatever_the_input_order(self):
        """AC-10: older versions are superseded, not rejected."""
        customers = self.out.by_key("customers")
        for key in ("C012", "C013"):
            with self.subTest(key=key):
                self.assertEqual(self.out.keys("customers").count(key), 1)
                self.assertEqual(customers[key]["city"], "York")
                self.assertEqual(customers[key]["updated_at"], datetime.datetime(2024, 3, 18, 9, 0))
        self.assertFalse({"C012", "C013"} & {q["key"] for q in self.out.quarantine})

    def test_invalid_latest_version_is_quarantined_and_older_not_promoted(self):
        """AC-10, D3"""
        self.assertNotIn("C014", self.out.keys("customers"))
        c014 = [q for q in self.out.quarantine if q["key"] == "C014"]
        self.assertEqual(len(c014), 1)
        self.assertEqual(c014[0]["reasons"], P(("malformed_value", "signup_date")))
        self.assertEqual(c014[0]["source"]["updated_at"], "2024-02-01T09:00:00")

    def test_unresolvable_key_duplicates_are_all_rejected(self):
        """AC-11: tie, unparseable updated_at, and non-customer key conflicts."""
        conflict = ("conflicting_duplicate",)
        self.assertEqual(self.out.rejected_names("customers"), Counter({
            ("C014", ("malformed_value",)): 1,
            ("C015", conflict): 2,
            ("C016", conflict): 1,
            ("C016", ("conflicting_duplicate", "malformed_value")): 1,  # AC-8: 'garbage' too
        }))
        self.assertEqual(self.out.rejected_names("products"), Counter({("P020", conflict): 2}))
        self.assertEqual(self.out.rejected_names("orders"), Counter({("O0020", conflict): 2}))
        self.assertEqual(self.out.rejected_names("order_items"), Counter({("OI0020", conflict): 2}))
        for dataset, key in (("customers", "C015"), ("customers", "C016"),
                             ("products", "P020"), ("orders", "O0020"),
                             ("order_items", "OI0020")):
            self.assertNotIn(key, self.out.keys(dataset))

    def test_silver_rows_per_dataset(self):
        """AC-9..AC-11"""
        self.assertEqual(sorted(self.out.keys("customers")), ["C010", "C011", "C012", "C013"])
        self.assertEqual(sorted(self.out.keys("products")), ["P001", "P021"])
        self.assertEqual(sorted(self.out.keys("orders")), ["O0001", "O0021"])
        self.assertEqual(sorted(self.out.keys("order_items")), ["OI0001"])

    def test_collapsed_and_superseded_counts(self):
        """AC-9, AC-10 (counts feed the AC-20 report)."""
        def counts(dataset):
            stage = self.build.stages[dataset]
            bronze, distinct, resolved = (stage[k].count() for k in ("bronze", "distinct", "resolved"))
            return {"collapsed": bronze - distinct, "superseded": distinct - resolved}
        # C010 x3 -> 2 collapsed, C011 whitespace -> 1; C012, C013, C014 -> 3 superseded.
        self.assertEqual(counts("customers"), {"collapsed": 3, "superseded": 3})
        # Conflicting products are rejected, never superseded.
        self.assertEqual(counts("products"), {"collapsed": 1, "superseded": 0})

    def test_primary_keys_are_non_null_and_unique(self):
        """AC-12"""
        self.assert_pk_non_null_unique(self.out)


# -- References: one build covering AC-8, AC-13, AC-14 ------------------------------

def reference_rows():
    c, p, o, i = "customers", "products", "orders", "order_items"
    return dict(
        customers=[row(c, "C001"), row(c, "C002", first_name="")],          # C002 rejected
        products=[row(p, "P001"), row(p, "P015", unit_price="abc")],        # P015 rejected
        orders=[
            row(o, "O0001", customer_id="C001"),
            row(o, "O0002", customer_id="C002"),          # parent rejected
            row(o, "O0033", customer_id="C999"),          # orphan
            row(o, "O0003", customer_id=""),              # null FK: missing_value only (D2)
            row(o, "O0004", customer_id="C001", order_date="2024-04-31"),  # own rule
        ],
        order_items=[
            row(i, "OI0001", order_id="O0001", product_id="P001"),
            row(i, "OI0002", order_id="O0001", product_id="P015"),
            row(i, "OI0003", order_id="O0001", product_id="P015"),
            row(i, "OI0004", order_id="O0002", product_id="P001"),   # cascade 2 levels
            row(i, "OI0005", order_id="O0033", product_id="P015"),   # OI0073-like
            row(i, "OI0006", order_id="O0001", product_id="P999"),
            row(i, "OI0007", order_id="O9999", product_id="P001"),
            row(i, "OI0008", order_id="", product_id="P001"),        # D2
            row(i, "OI0009", order_id="O0004", product_id="P001"),
            row(i, "OI0010", order_id="O0003", product_id="P001"),
            row(i, "OI0011", order_id="O0002", product_id="P001", quantity="0"),
            row(i, "OI0012", order_id=" O0001 ", product_id=" P001 "),  # trimmed FKs resolve
        ],
    )


class ReferencesTest(SilverAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = _context.require_spark()
        cls.build = build_silver(bronze_frames(spark, **reference_rows()), RUN_ID, PROCESSED_AT)
        cls.out = Outcome(cls.build)

    def test_orders_reference_rules(self):
        """AC-13, D2"""
        self.assertEqual(self.out.rejected("orders"), Counter({
            ("orders", "O0002", P(("parent_rejected", "customer_id"))): 1,
            ("orders", "O0033", P(("orphan_reference", "customer_id"))): 1,
            ("orders", "O0003", P(("missing_value", "customer_id"))): 1,
            ("orders", "O0004", P(("malformed_value", "order_date"))): 1,
        }))
        self.assertEqual(sorted(self.out.keys("orders")), ["O0001"])

    def test_order_items_cascade_over_orders_and_products(self):
        """AC-8, AC-13, D2: OI0005 carries parent_rejected twice (order_id, product_id)."""
        pr_order, pr_product = ("parent_rejected", "order_id"), ("parent_rejected", "product_id")
        self.assertEqual(self.out.rejected("order_items"), Counter({
            ("order_items", "OI0002", P(pr_product)): 1,
            ("order_items", "OI0003", P(pr_product)): 1,
            ("order_items", "OI0004", P(pr_order)): 1,
            ("order_items", "OI0005", P(pr_order, pr_product)): 1,
            ("order_items", "OI0006", P(("orphan_reference", "product_id"))): 1,
            ("order_items", "OI0007", P(("orphan_reference", "order_id"))): 1,
            ("order_items", "OI0008", P(("missing_value", "order_id"))): 1,
            ("order_items", "OI0009", P(pr_order)): 1,
            ("order_items", "OI0010", P(pr_order)): 1,
            ("order_items", "OI0011", P(("invalid_value", "quantity"), pr_order)): 1,
        }))
        self.assertEqual(sorted(self.out.keys("order_items")), ["OI0001", "OI0012"])

    def test_every_silver_foreign_key_resolves(self):
        """AC-14 (unit level)"""
        customers = set(self.out.keys("customers"))
        orders, products = set(self.out.keys("orders")), set(self.out.keys("products"))
        self.assertTrue({r["customer_id"] for r in self.out.silver["orders"]} <= customers)
        self.assertTrue({r["order_id"] for r in self.out.silver["order_items"]} <= orders)
        self.assertTrue({r["product_id"] for r in self.out.silver["order_items"]} <= products)

    def test_primary_keys_are_non_null_and_unique(self):
        """AC-12"""
        self.assert_pk_non_null_unique(self.out)


# -- Run behaviour: AC-17, AC-19 ----------------------------------------------------

def _data_rows(out):
    """Comparable Silver + quarantine content without the Silver run metadata."""
    silver = {name: sorted(tuple(v for k, v in r.asDict().items()
                                 if k not in ("_run_id", "_processed_at")) for r in rows)
              for name, rows in out.silver.items()}
    quarantine = sorted((q["dataset"], q["key"] or "", q["reasons"],
                         tuple(sorted((k, v or "") for k, v in q["source"].items())))
                        for q in out.quarantine)
    return silver, quarantine


class RunBehaviourTest(SilverAssertions, unittest.TestCase):
    def setUp(self):
        self.spark = _context.require_spark()

    def test_same_input_twice_gives_same_rows(self):
        """AC-17: only run metadata differs; nothing doubles."""
        frames = bronze_frames(self.spark, **reference_rows())
        first = Outcome(build_silver(frames, "silver-run-1", PROCESSED_AT))
        second = Outcome(build_silver(frames, "silver-run-2",
                                      PROCESSED_AT + datetime.timedelta(hours=1)))
        self.assertEqual(_data_rows(first), _data_rows(second))
        self.assertEqual({r["_run_id"] for r in second.silver["orders"]}, {"silver-run-2"})
        self.assert_pk_non_null_unique(second)

    def test_empty_input_gives_empty_frames_with_full_schema(self):
        """AC-19"""
        build = build_silver(bronze_frames(self.spark, customers=[], products=[], orders=[],
                                           order_items=[]), RUN_ID, PROCESSED_AT)
        self.assert_full_schema(build)
        out = Outcome(build)
        for dataset in PK:
            self.assertEqual(out.silver[dataset], [], dataset)
        self.assertEqual(out.quarantine, [])

    def test_all_invalid_input_gives_empty_silver_with_full_schema(self):
        """AC-19: zero clean rows is not an error; every row is quarantined."""
        c, p, o, i = "customers", "products", "orders", "order_items"
        build = build_silver(bronze_frames(
            self.spark,
            customers=[row(c, None)],
            products=[row(p, "P001", unit_price="abc")],
            orders=[row(o, "O0001", status="lost")],
            order_items=[row(i, "OI0001", quantity="0")],
        ), RUN_ID, PROCESSED_AT)
        self.assert_full_schema(build)
        out = Outcome(build)
        for dataset in PK:
            self.assertEqual(out.silver[dataset], [], dataset)
        self.assertEqual(Counter(q["dataset"] for q in out.quarantine),
                         Counter({c: 1, p: 1, o: 1, i: 1}))


if __name__ == "__main__":
    unittest.main()
