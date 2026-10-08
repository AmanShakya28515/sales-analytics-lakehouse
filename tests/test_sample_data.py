"""Checks on the versioned sample files themselves, using only csv/json."""

import csv
import datetime
import json
import unittest
from decimal import Decimal, InvalidOperation

from _context import REPO_ROOT, SAMPLE_ROOT
from sales_lakehouse.raw_datasets import DATASETS, ISSUE_CATEGORIES, KNOWN_ISSUES, get_dataset

REQUIRED_CATEGORIES = {"null_key", "exact_duplicate", "changed_record",
                       "malformed_value", "orphan_reference"}


def _read_rows(dataset):
    path = SAMPLE_ROOT / dataset.name / dataset.file_name
    with open(path, encoding="utf-8", newline="") as handle:
        if dataset.file_format == "csv":
            return list(csv.DictReader(handle))
        return [json.loads(line) for line in handle if line.strip()]


def _is_empty(value):
    return value is None or value == ""


def _matches(row, dataset, row_key):
    key = row.get(dataset.primary_key)
    return _is_empty(key) if row_key is None else key == row_key


def _number(value):
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _parses_as_intended(column, value):
    if column.endswith("_date"):
        try:
            datetime.date.fromisoformat(value)
            return True
        except (TypeError, ValueError):
            return False
    return _number(value) is not None


class SampleDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = {d.name: _read_rows(d) for d in DATASETS}

    def _clean_rows(self, name):
        """Rows not named by any known issue."""
        dataset = get_dataset(name)
        issues = [i for i in KNOWN_ISSUES if i.dataset == name]
        return [r for r in self.rows[name]
                if not any(_matches(r, dataset, i.row_key) for i in issues)]

    def _keys(self, name):
        dataset = get_dataset(name)
        return {r[dataset.primary_key] for r in self.rows[name]
                if not _is_empty(r[dataset.primary_key])}

    def test_four_files_exist_in_expected_formats(self):
        """AC-8: CSV with a header for three datasets, JSON Lines for orders."""
        self.assertEqual({d.name for d in DATASETS},
                         {"customers", "products", "orders", "order_items"})
        for dataset in DATASETS:
            path = SAMPLE_ROOT / dataset.name / dataset.file_name
            with self.subTest(dataset=dataset.name):
                self.assertTrue(path.is_file())
                expected_format = "json" if dataset.name == "orders" else "csv"
                self.assertEqual(dataset.file_format, expected_format)
                self.assertTrue(path.name.endswith(f".{expected_format}"))

    def test_columns_match_registry(self):
        """AC-8, AC-9: CSV headers and every JSON object match the documented columns."""
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                path = SAMPLE_ROOT / dataset.name / dataset.file_name
                if dataset.file_format == "csv":
                    with open(path, encoding="utf-8", newline="") as handle:
                        self.assertEqual(tuple(next(csv.reader(handle))), dataset.columns)
                else:
                    for row in self.rows[dataset.name]:
                        self.assertEqual(tuple(row.keys()), dataset.columns)

    def test_row_counts_match_registry_and_are_small(self):
        """AC-9, AC-10"""
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                self.assertEqual(len(self.rows[dataset.name]), dataset.expected_rows)
                self.assertLess(dataset.expected_rows, 500)

    def test_data_dictionary_documents_columns_counts_and_issues(self):
        """AC-9, AC-11: the dictionary agrees with the registry and the known issues."""
        text = (REPO_ROOT / "docs" / "data_dictionary.md").read_text(encoding="utf-8")
        sections = {s.split("\n", 1)[0].strip(): s for s in text.split("\n## ")[1:]}
        for dataset in DATASETS:
            with self.subTest(dataset=dataset.name):
                section = sections[dataset.name]
                self.assertIn(f"Expected rows: {dataset.expected_rows}", section)
                for column in dataset.columns:
                    self.assertIn(f"| `{column}` |", section)
        issues_section = sections["Known data-quality issues"]
        for issue in KNOWN_ISSUES:
            key = "(empty)" if issue.row_key is None else issue.row_key
            self.assertIn(f"| {issue.dataset} | {key} | `{issue.column}` | {issue.category} |",
                          issues_section)

    def test_clean_records_reference_existing_parents(self):
        """AC-10: outside the known issues, every reference resolves."""
        for dataset in DATASETS:
            for column, parent in dataset.foreign_keys:
                parent_keys = self._keys(parent)
                for row in self._clean_rows(dataset.name):
                    with self.subTest(dataset=dataset.name, row=row[dataset.primary_key]):
                        self.assertIn(row[column], parent_keys)

    def test_clean_records_have_keys_and_positive_amounts(self):
        """AC-10"""
        for dataset in DATASETS:
            for row in self._clean_rows(dataset.name):
                self.assertFalse(_is_empty(row[dataset.primary_key]))
        for name, columns in (("products", ("unit_price",)),
                              ("order_items", ("quantity", "unit_price"))):
            for row in self._clean_rows(name):
                for column in columns:
                    with self.subTest(dataset=name, column=column, value=row[column]):
                        value = _number(row[column])
                        self.assertIsNotNone(value)
                        self.assertGreater(value, 0)

    def test_clean_records_have_unique_keys(self):
        """AC-10: duplicates exist only where a known issue says so."""
        for dataset in DATASETS:
            keys = [r[dataset.primary_key] for r in self._clean_rows(dataset.name)]
            self.assertEqual(len(keys), len(set(keys)), dataset.name)

    def test_required_issue_categories_are_covered(self):
        """AC-11"""
        categories = {issue.category for issue in KNOWN_ISSUES}
        self.assertTrue(REQUIRED_CATEGORIES <= categories)
        self.assertTrue(categories <= set(ISSUE_CATEGORIES))

    def test_every_known_issue_is_present_in_the_data(self):
        """AC-11: each documented problem really is in its file."""
        for issue in KNOWN_ISSUES:
            dataset = get_dataset(issue.dataset)
            rows = [r for r in self.rows[issue.dataset] if _matches(r, dataset, issue.row_key)]
            with self.subTest(issue=issue.description):
                self.assertTrue(rows, "no matching rows")
                values = [r[issue.column] for r in rows]
                if issue.category == "null_key":
                    self.assertTrue(all(_is_empty(v) for v in values))
                elif issue.category == "exact_duplicate":
                    self.assertGreaterEqual(len(rows), 2)
                    self.assertTrue(all(r == rows[0] for r in rows))
                elif issue.category == "changed_record":
                    self.assertGreaterEqual(len(rows), 2)
                    self.assertGreater(len(set(values)), 1)
                elif issue.category == "malformed_value":
                    self.assertTrue(all(not _parses_as_intended(issue.column, v) for v in values))
                elif issue.category == "invalid_value":
                    self.assertTrue(all(_number(v) is not None and _number(v) <= 0 for v in values))
                elif issue.category == "orphan_reference":
                    parent = dict(dataset.foreign_keys)[issue.column]
                    self.assertTrue(all(v not in self._keys(parent) for v in values))
                else:
                    self.fail(f"Unknown category {issue.category}")


if __name__ == "__main__":
    unittest.main()
