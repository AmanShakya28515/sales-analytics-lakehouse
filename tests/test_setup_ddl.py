import re
import unittest
from types import SimpleNamespace

from sales_lakehouse.setup_ddl import CatalogCreationRefused, run_setup, setup_statements


class _RecordingCatalog:
    def __init__(self, spark, existing):
        self._spark = spark
        self._existing = existing

    def listCatalogs(self):
        self._spark.calls.append("listCatalogs")
        return [SimpleNamespace(name=name) for name in self._existing]


class RecordingSpark:
    """Stands in for SparkSession: records SQL instead of running it."""

    def __init__(self, existing_catalogs=(), refuse_create_catalog=False):
        self.calls = []
        self.statements = []
        self.catalog = _RecordingCatalog(self, existing_catalogs)
        self._refuse = refuse_create_catalog

    def sql(self, statement):
        self.calls.append("sql")
        self.statements.append(statement)
        if self._refuse and statement.startswith("CREATE CATALOG"):
            raise RuntimeError("PERMISSION_DENIED: cannot create catalog")


def _created(statements, kind):
    pattern = re.compile(rf"CREATE {kind} IF NOT EXISTS ((?:`[^`]+`\.?)+)")
    return {m.group(1) for s in statements for m in [pattern.match(s)] if m}


class SetupStatementsTest(unittest.TestCase):
    def test_creates_three_layer_schemas_and_raw_volume_in_bronze(self):
        """AC-3"""
        statements = setup_statements("cat", "dev")
        self.assertEqual(
            _created(statements, "SCHEMA"),
            {"`cat`.`dev_bronze`", "`cat`.`dev_silver`", "`cat`.`dev_gold`"},
        )
        self.assertEqual(_created(statements, "VOLUME"), {"`cat`.`dev_bronze`.`raw_data`"})
        self.assertEqual(len(statements), 4)

    def test_no_tables_and_every_statement_is_rerunnable(self):
        """AC-3, AC-4"""
        for statement in setup_statements("cat", "dev"):
            self.assertIn("IF NOT EXISTS", statement)
            self.assertNotIn("TABLE", statement.upper())

    def test_dev_and_test_share_no_schema_or_volume(self):
        """AC-5"""
        dev = setup_statements("cat", "dev")
        test = setup_statements("cat", "test")
        for kind in ("SCHEMA", "VOLUME"):
            self.assertFalse(_created(dev, kind) & _created(test, kind))
        self.assertEqual(
            _created(test, "SCHEMA"),
            {"`cat`.`test_bronze`", "`cat`.`test_silver`", "`cat`.`test_gold`"},
        )


class RunSetupTest(unittest.TestCase):
    def test_invalid_names_stop_before_any_spark_call(self):
        """AC-6"""
        for catalog, env in [("x; DROP", "dev"), ("cat", "a`b"), ("", "dev"), ("cat", "")]:
            with self.subTest(catalog=catalog, env=env):
                spark = RecordingSpark(existing_catalogs=("cat",))
                with self.assertRaises(ValueError):
                    run_setup(spark, catalog, env)
                self.assertEqual(spark.calls, [])

    def test_existing_catalog_is_not_recreated(self):
        """AC-3, AC-4"""
        spark = RecordingSpark(existing_catalogs=("cat",))
        run_setup(spark, "cat", "dev")
        self.assertFalse(any(s.startswith("CREATE CATALOG") for s in spark.statements))
        self.assertEqual(spark.statements, setup_statements("cat", "dev"))

    def test_missing_catalog_is_created_first(self):
        """AC-3"""
        spark = RecordingSpark(existing_catalogs=("workspace",))
        run_setup(spark, "cat", "dev")
        self.assertEqual(spark.statements[0], "CREATE CATALOG IF NOT EXISTS `cat`")
        self.assertEqual(spark.statements[1:], setup_statements("cat", "dev"))

    def test_refused_catalog_stops_without_fallback(self):
        """AC-3 / plan D2: stop with a clear message; never switch catalogs."""
        spark = RecordingSpark(existing_catalogs=("workspace",), refuse_create_catalog=True)
        with self.assertRaises(CatalogCreationRefused) as raised:
            run_setup(spark, "cat", "dev")
        self.assertIn("catalog=workspace", str(raised.exception))
        self.assertEqual(spark.statements, ["CREATE CATALOG IF NOT EXISTS `cat`"])


if __name__ == "__main__":
    unittest.main()
