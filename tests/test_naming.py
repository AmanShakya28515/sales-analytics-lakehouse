import unittest

from sales_lakehouse.naming import layer_names, validate_identifier


class ValidateIdentifierTest(unittest.TestCase):
    def test_valid_names_are_lowercased(self):
        self.assertEqual(validate_identifier("Sales_Lakehouse1", "catalog", 64), "sales_lakehouse1")
        self.assertEqual(validate_identifier("dev", "environment", 20), "dev")

    def test_unsafe_names_are_rejected(self):
        """AC-6: anything outside letters, digits and underscores is refused."""
        bad_values = ["x; DROP", "a`b", "", " ", "a.b", "a-b", "dev\n", "dév", None, 42]
        for value in bad_values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validate_identifier(value, "catalog", 64)

    def test_over_long_names_are_rejected(self):
        """AC-6"""
        with self.assertRaises(ValueError):
            layer_names("c" * 65, "dev")
        with self.assertRaises(ValueError):
            layer_names("cat", "e" * 21)


class LayerNamesTest(unittest.TestCase):
    def test_names_for_an_environment(self):
        """AC-3: one schema per layer per environment; raw_data lives in bronze."""
        names = layer_names("Cat", "Dev")
        self.assertEqual(names.schemas, ("dev_bronze", "dev_silver", "dev_gold"))
        self.assertEqual(names.raw_volume_path, "/Volumes/cat/dev_bronze/raw_data")

    def test_invalid_environment_is_rejected(self):
        """AC-6"""
        with self.assertRaises(ValueError):
            layer_names("cat", "dev; DROP SCHEMA x")


if __name__ == "__main__":
    unittest.main()
