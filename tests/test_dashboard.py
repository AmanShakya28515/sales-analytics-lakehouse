"""File checks for the analytics dashboard (step 05), derived from spec 05.

No Spark: these read the versioned `.sql` files, the dashboard definition
`dashboards/sales_overview.lvdash.json` and the docs. `run_query` is checked
with a recording fake Spark object.
"""

import json
import re
import unittest

from _context import REPO_ROOT
from sales_lakehouse.dashboard import DASHBOARD_FILE, QUERIES, SQL_DIR, load_query, run_query

# The three Gold tables of spec 04 (not imported from gold.py, which needs pyspark).
GOLD_TABLES = ("daily_sales", "product_sales", "customer_summary")

TABLE_REF = re.compile(
    r"IDENTIFIER\('`' \|\| :catalog \|\| '`\.`' \|\| :env \|\| '_gold`\.`(\w+)`'\)"
)
WRITE_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER",
                  "TRUNCATE", "REPLACE", "GRANT", "REVOKE", "COPY", "OPTIMIZE", "VACUUM")
HARD_CODED = ("sales_lakehouse", "dev_", "test_", "t01_", "_gold.", "_silver", "_bronze",
              "quarantine")


def _strip_comments(text):
    return re.sub(r"--[^\n]*", "", text)


def _normalise(text):
    return " ".join(_strip_comments(text).split())


def _dataset_query(dataset):
    return dataset.get("query") or "".join(dataset.get("queryLines", []))


def _strings(node):
    """Every string value inside a JSON node."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _strings(value)


class _RecordingSpark:
    def __init__(self):
        self.calls = []

    def sql(self, text, args=None):
        self.calls.append((text, args))
        return "frame"


class QueryFileTest(unittest.TestCase):
    def test_every_query_has_a_file(self):
        """AC-9"""
        self.assertEqual(len(QUERIES), 6)
        for name, file_name in QUERIES.items():
            self.assertTrue((SQL_DIR / file_name).is_file(), file_name)
        self.assertEqual(sorted(p.name for p in SQL_DIR.glob("*.sql")), sorted(QUERIES.values()))

    def test_queries_read_only_gold_through_parameters(self):
        """AC-5, AC-8: every table is a parameterised Gold table; nothing else is read."""
        for name in QUERIES:
            with self.subTest(query=name):
                text = _strip_comments(load_query(name))
                tables = TABLE_REF.findall(text)
                self.assertTrue(tables)
                self.assertLessEqual(set(tables), set(GOLD_TABLES))
                ctes = set(re.findall(r"(\w+) AS \(", text))
                for target in re.findall(r"\b(?:FROM|JOIN)\s+(\S+)", text, flags=re.IGNORECASE):
                    if target.startswith("IDENTIFIER("):
                        continue
                    self.assertIn(target.rstrip(")"), ctes, f"{name} reads {target}")
                self.assertEqual(text.count("IDENTIFIER("), len(tables))

    def test_no_catalog_or_schema_is_hard_coded(self):
        """AC-8"""
        for name in QUERIES:
            with self.subTest(query=name):
                text = load_query(name)
                for literal in HARD_CODED:
                    self.assertNotIn(literal, text)

    def test_every_query_has_the_identifier_guard(self):
        """AC-8: invalid parameter values are rejected by the query itself."""
        guard = ":catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$'"
        for name in QUERIES:
            with self.subTest(query=name):
                text = load_query(name)
                self.assertIn("assert_true(", text)
                self.assertIn(guard, text)
                uses = len(TABLE_REF.findall(text))
                self.assertEqual(text.count("(SELECT ok FROM guard) IS NULL"), uses)

    def test_queries_only_read(self):
        """AC-10"""
        for name in QUERIES:
            with self.subTest(query=name):
                text = _strip_comments(load_query(name)).strip()
                self.assertRegex(text, r"^(WITH|SELECT)\b")
                self.assertNotIn(";", text)
                for keyword in WRITE_KEYWORDS:
                    self.assertNotRegex(text.upper(), rf"\b{keyword}\b")

    def test_rankings_break_ties_by_id(self):
        """AC-6"""
        self.assertIn("ORDER BY revenue DESC, product_id", load_query("top_products"))
        self.assertIn("ORDER BY revenue DESC, customer_id", load_query("top_customers"))


class RunQueryTest(unittest.TestCase):
    def test_binds_validated_names_as_parameters(self):
        """AC-7, AC-8: values go in as parameters, never into the SQL text."""
        spark = _RecordingSpark()
        self.assertEqual(run_query(spark, "kpis", "Sales_Lakehouse", "DEV"), "frame")
        [(text, args)] = spark.calls
        self.assertEqual(text, load_query("kpis"))
        self.assertEqual(args, {"catalog": "sales_lakehouse", "env": "dev"})

    def test_invalid_names_are_rejected_before_spark(self):
        """AC-8"""
        spark = _RecordingSpark()
        for catalog, env in (("sales_lakehouse", "dev;drop"), ("sales_lakehouse", "a.b"),
                             ("sales_lakehouse", "a`b"), ("a-b", "dev"), ("", "dev")):
            with self.subTest(catalog=catalog, env=env):
                with self.assertRaises(ValueError):
                    run_query(spark, "kpis", catalog, env)
        self.assertEqual(spark.calls, [])

    def test_unknown_query_name(self):
        with self.assertRaises(ValueError):
            load_query("../setup")


class DashboardDefinitionTest(unittest.TestCase):
    """The dashboard file is created in the Databricks UI (plan 05 D1), then committed."""

    @classmethod
    def setUpClass(cls):
        if not DASHBOARD_FILE.is_file():
            raise AssertionError(
                f"{DASHBOARD_FILE.name} is missing: build the dashboard as described in "
                "dashboards/README.md and commit the file."
            )
        cls.dashboard = json.loads(DASHBOARD_FILE.read_text(encoding="utf-8"))
        cls.datasets = {d["displayName"]: d for d in cls.dashboard["datasets"]}

    def test_has_one_dataset_per_query(self):
        """AC-9"""
        self.assertEqual(len(self.dashboard["datasets"]), len(QUERIES))
        self.assertEqual(set(self.datasets), set(QUERIES))

    def test_datasets_equal_the_sql_files(self):
        """AC-9: no drift between the dashboard and the versioned queries."""
        for name, dataset in self.datasets.items():
            with self.subTest(dataset=name):
                self.assertEqual(_normalise(_dataset_query(dataset)), _normalise(load_query(name)))

    def test_datasets_declare_catalog_and_env_with_defaults(self):
        """AC-7"""
        for name, dataset in self.datasets.items():
            with self.subTest(dataset=name):
                params = {p["keyword"]: p for p in dataset.get("parameters", [])}
                self.assertEqual(set(params), {"catalog", "env"})
                for keyword, default in (("catalog", "sales_lakehouse"), ("env", "dev")):
                    values = list(_strings(params[keyword].get("defaultSelection", {})))
                    self.assertIn(default, values, f"{name}.{keyword}")

    def test_every_dataset_feeds_a_visual(self):
        """AC-9: the layout is versioned and uses every query."""
        pages = json.dumps(self.dashboard.get("pages", []))
        for name, dataset in self.datasets.items():
            with self.subTest(dataset=name):
                self.assertIn(f'"datasetName": "{dataset["name"]}"', pages)

    def test_no_catalog_or_schema_is_hard_coded(self):
        """AC-8: only the parameter defaults may name the catalog and env."""
        for name, dataset in self.datasets.items():
            with self.subTest(dataset=name):
                for literal in HARD_CODED:
                    self.assertNotIn(literal, _dataset_query(dataset))
        self.assertNotIn("t01_", DASHBOARD_FILE.read_text(encoding="utf-8"))


class DashboardDocsTest(unittest.TestCase):
    def test_dashboard_readme(self):
        """AC-11"""
        text = (REPO_ROOT / "dashboards" / "README.md").read_text(encoding="utf-8")
        for name in QUERIES:
            self.assertIn(f"`{name}`", text)
        for phrase in ("`catalog`", "`env`", "sales_lakehouse", "Refresh", "Import dashboard",
                       "completed", "shipped", "quantity × unit_price", "Counter", "Line",
                       "Table"):
            self.assertIn(phrase, text)

    def test_root_readme_lists_the_dashboard(self):
        """AC-11"""
        text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("| `dashboards/` |", text)
        self.assertIn("dashboards/README.md", text)


if __name__ == "__main__":
    unittest.main()
