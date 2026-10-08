"""The four raw sample datasets: registry, explicit schemas, loading and verification.

Raw read schemas declare every column as STRING on purpose. Raw data is kept
exactly as delivered, so a malformed value (e.g. "abc" as a price) is
preserved rather than silently becoming NULL. Intended types are documented
in docs/data_dictionary.md and applied in Silver.
"""

import os
import shutil
from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from sales_lakehouse.naming import RAW_VOLUME


@dataclass(frozen=True)
class RawDataset:
    name: str
    file_name: str
    file_format: str  # "csv" (with header row) or "json" (JSON Lines)
    columns: tuple
    primary_key: str
    foreign_keys: tuple  # (column, parent dataset name)
    expected_rows: int


DATASETS = (
    RawDataset(
        name="customers",
        file_name="customers.csv",
        file_format="csv",
        columns=("customer_id", "first_name", "last_name", "email", "city",
                 "country", "signup_date", "updated_at"),
        primary_key="customer_id",
        foreign_keys=(),
        expected_rows=23,
    ),
    RawDataset(
        name="products",
        file_name="products.csv",
        file_format="csv",
        columns=("product_id", "product_name", "category", "unit_price", "is_active"),
        primary_key="product_id",
        foreign_keys=(),
        expected_rows=15,
    ),
    RawDataset(
        name="orders",
        file_name="orders.json",
        file_format="json",
        columns=("order_id", "customer_id", "order_date", "status", "channel"),
        primary_key="order_id",
        foreign_keys=(("customer_id", "customers"),),
        expected_rows=41,
    ),
    RawDataset(
        name="order_items",
        file_name="order_items.csv",
        file_format="csv",
        columns=("order_item_id", "order_id", "product_id", "quantity", "unit_price"),
        primary_key="order_item_id",
        foreign_keys=(("order_id", "orders"), ("product_id", "products")),
        expected_rows=90,
    ),
)


@dataclass(frozen=True)
class KnownIssue:
    """A deliberate data-quality problem in the sample data.

    `row_key` is the primary-key value of the affected rows; None means the
    rows whose primary key is empty.
    """
    dataset: str
    row_key: object
    column: str
    category: str
    description: str


ISSUE_CATEGORIES = (
    "null_key", "exact_duplicate", "changed_record",
    "malformed_value", "invalid_value", "orphan_reference",
)

KNOWN_ISSUES = (
    KnownIssue("customers", None, "customer_id", "null_key",
               "A customer row (Jordan Lee) has no customer_id."),
    KnownIssue("customers", "C007", "customer_id", "exact_duplicate",
               "C007 appears twice with identical values."),
    KnownIssue("customers", "C012", "email", "changed_record",
               "C012 appears twice; the later row (updated_at 2024-03-18) has a new email and city."),
    KnownIssue("products", "P015", "unit_price", "malformed_value",
               "unit_price is 'abc', not a number."),
    KnownIssue("orders", "O0010", "order_id", "exact_duplicate",
               "O0010 appears twice with identical values."),
    KnownIssue("orders", "O0025", "order_date", "malformed_value",
               "order_date is 2024-04-31, which is not a real date."),
    KnownIssue("orders", "O0033", "customer_id", "orphan_reference",
               "customer_id C999 does not exist in customers."),
    KnownIssue("order_items", "OI0045", "product_id", "orphan_reference",
               "product_id P999 does not exist in products."),
    KnownIssue("order_items", "OI0060", "order_id", "null_key",
               "order_id is empty."),
    KnownIssue("order_items", "OI0075", "quantity", "invalid_value",
               "quantity is 0."),
)


def get_dataset(name):
    for dataset in DATASETS:
        if dataset.name == name:
            return dataset
    raise KeyError(f"Unknown dataset {name!r}")


def raw_schema(name):
    """Explicit all-string read schema for a raw dataset (no inference)."""
    return StructType(
        [StructField(column, StringType(), True) for column in get_dataset(name).columns]
    )


def dataset_dir(volume_root, name):
    return f"{volume_root}/{name}"


def _check_volume_root(volume_root):
    # Loading deletes files, so only ever act inside a raw_data volume.
    if not (volume_root.startswith("/Volumes/") and volume_root.endswith(f"/{RAW_VOLUME}")):
        raise ValueError(f"Not a raw data volume path: {volume_root!r}")
    if not os.path.isdir(volume_root):
        raise FileNotFoundError(
            f"Volume path {volume_root} not found. Run setup/00_setup_catalog_objects first."
        )


def _empty_dir(path):
    os.makedirs(path, exist_ok=True)
    for entry in os.listdir(path):
        entry_path = os.path.join(path, entry)
        if os.path.isdir(entry_path):
            shutil.rmtree(entry_path)
        else:
            os.remove(entry_path)


def load_sample_files(source_root, volume_root):
    """Copy each sample file into its own folder in the volume.

    Each dataset folder is emptied first, so a re-load replaces the files
    instead of adding copies. Returns {dataset name: target file path}.
    """
    _check_volume_root(volume_root)
    sources = {d.name: os.path.join(source_root, d.name, d.file_name) for d in DATASETS}
    missing = [path for path in sources.values() if not os.path.isfile(path)]
    if missing:
        raise FileNotFoundError(f"Sample files not found: {missing}")

    loaded = {}
    for dataset in DATASETS:
        target_dir = dataset_dir(volume_root, dataset.name)
        _empty_dir(target_dir)
        target = f"{target_dir}/{dataset.file_name}"
        shutil.copyfile(sources[dataset.name], target)
        loaded[dataset.name] = target
    return loaded


def read_raw(spark, volume_root, dataset):
    """Read one raw dataset folder with its explicit schema."""
    reader = spark.read.schema(raw_schema(dataset.name)).option("mode", "FAILFAST")
    if dataset.file_format == "csv":
        # enforceSchema=False makes Spark check the header against the schema.
        reader = reader.option("header", True).option("enforceSchema", False)
    return reader.format(dataset.file_format).load(dataset_dir(volume_root, dataset.name))


@dataclass(frozen=True)
class VerifyResult:
    dataset: str
    status: str  # OK | MISSING | COUNT_MISMATCH | COLUMN_MISMATCH | READ_ERROR
    expected_rows: int
    actual_rows: object
    detail: str


def _has_files(path):
    return os.path.isdir(path) and any(
        os.path.isfile(os.path.join(path, entry)) for entry in os.listdir(path)
    )


def _short_error(exc):
    text = str(exc).strip()
    return text.splitlines()[0][:300] if text else repr(exc)


def _json_lines_with_wrong_fields(spark, folder, columns):
    """Count non-blank JSON lines whose field names are not exactly `columns`.

    A FAILFAST JSON read with a schema silently turns a missing field into
    NULL, so the field names are checked on the raw text instead. Invalid JSON
    gives NULL keys and is counted too.
    """
    expected = F.array_sort(F.array(*[F.lit(c) for c in columns]))
    keys = F.array_sort(F.expr("json_object_keys(value)"))
    return (
        spark.read.text(folder)
        .where(F.trim(F.col("value")) != "")
        .where(F.coalesce(keys != expected, F.lit(True)))
        .count()
    )


def validate_raw_structure(spark, volume_root):
    """Check that every dataset's files match the documented structure.

    Row counts are deliberately not checked: the content may change between
    runs. Returns a list of (dataset name, problem); an empty list means valid.
    """
    problems = []
    for dataset in DATASETS:
        folder = dataset_dir(volume_root, dataset.name)
        if not _has_files(folder):
            problems.append((dataset.name, f"no files in {folder}"))
            continue
        try:
            if dataset.file_format == "csv":
                # Counting every column forces a full FAILFAST parse, so a wrong
                # header or a line with the wrong number of fields raises.
                read_raw(spark, volume_root, dataset).agg(
                    *[F.count(F.col(c)) for c in dataset.columns]
                ).first()
            else:
                bad_lines = _json_lines_with_wrong_fields(spark, folder, dataset.columns)
                if bad_lines:
                    problems.append((
                        dataset.name,
                        f"{bad_lines} line(s) are not valid JSON or do not have exactly "
                        f"the fields {list(dataset.columns)}",
                    ))
        except Exception as exc:
            problems.append((dataset.name, _short_error(exc)))
    return problems


def verify_raw_datasets(spark, volume_root):
    """Check every dataset folder: present, readable, expected columns and row count."""
    results = []
    for dataset in DATASETS:
        folder = dataset_dir(volume_root, dataset.name)
        if not _has_files(folder):
            results.append(VerifyResult(dataset.name, "MISSING", dataset.expected_rows,
                                        None, f"No files in {folder}"))
            continue
        try:
            counts = read_raw(spark, volume_root, dataset).agg(
                F.count(F.lit(1)).alias("__rows"),
                *[F.count(F.col(c)).alias(c) for c in dataset.columns],
            ).first()
        except Exception as exc:
            results.append(VerifyResult(dataset.name, "READ_ERROR", dataset.expected_rows,
                                        None, _short_error(exc)))
            continue

        actual = counts["__rows"]
        # A column with no values at all means it is missing from the file.
        empty_columns = [c for c in dataset.columns if counts[c] == 0]
        if empty_columns:
            status, detail = "COLUMN_MISMATCH", f"Columns with no values: {empty_columns}"
        elif actual != dataset.expected_rows:
            status, detail = "COUNT_MISMATCH", f"Expected {dataset.expected_rows} rows, found {actual}"
        else:
            status, detail = "OK", ""
        results.append(VerifyResult(dataset.name, status, dataset.expected_rows, actual, detail))
    return results


def assert_all_ok(results):
    """Raise if any dataset failed verification, naming each one."""
    failed = [r for r in results if r.status != "OK"]
    if failed:
        raise RuntimeError(
            "Raw data verification failed: "
            + "; ".join(f"{r.dataset}: {r.status} ({r.detail})" for r in failed)
        )
