import re
import unittest

from _context import REPO_ROOT


class ProjectLayoutTest(unittest.TestCase):
    def test_every_folder_described_in_readme_exists(self):
        """AC-1: the README describes each top-level folder, and each one exists."""
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        folders = re.findall(r"^\|\s*`([\w./-]+/)`\s*\|", readme, flags=re.MULTILINE)

        for expected in ("src/", "setup/", "notebooks/", "tests/", "data/sample/", "docs/"):
            self.assertIn(expected, folders)
        for folder in folders:
            self.assertTrue((REPO_ROOT / folder).is_dir(), f"{folder} is described but missing")


if __name__ == "__main__":
    unittest.main()
