import unittest

from _context import REPO_ROOT
from sales_lakehouse.bronze import METADATA_COLUMNS
from sales_lakehouse.raw_datasets import DATASETS


class BronzeDocsTest(unittest.TestCase):
    def test_data_dictionary_describes_bronze_tables_and_metadata(self):
        """AC-12"""
        text = (REPO_ROOT / "docs" / "data_dictionary.md").read_text(encoding="utf-8")
        section = text.split("\n## Bronze tables", 1)[1].split("\n## ", 1)[0]
        for dataset in DATASETS:
            self.assertIn(f"`<env>_bronze.{dataset.name}`", section)
        for column in METADATA_COLUMNS:
            self.assertIn(f"`{column}`", section)

    def test_readme_covers_git_folder_and_bronze_run_order(self):
        """AC-12 (resolves step 01 finding Q-2)"""
        text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("Git folder", text)
        self.assertLess(text.index("setup/01_load_sample_data"),
                        text.index("notebooks/bronze_ingest"))


if __name__ == "__main__":
    unittest.main()
