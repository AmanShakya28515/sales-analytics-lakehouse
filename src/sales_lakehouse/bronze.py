"""Bronze ingestion: raw files -> Bronze Delta tables, as delivered plus metadata.

Full refresh: each run first validates all four raw datasets. If any one is
missing or structurally invalid, nothing is created or written. Otherwise the
Bronze tables are created if needed (DDL below) and each one is replaced with
exactly the current raw files. Bronze never cleans, casts, deduplicates or
drops rows; that is Silver's job.
"""

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import DATASETS, read_raw, validate_raw_structure

METADATA_COLUMNS = ("_ingested_at", "_source_file", "_run_id")
MAX_RUN_ID_LENGTH = 64

TABLE_COMMENTS = {
    "customers": "Bronze customers: raw rows exactly as delivered (all STRING) plus ingestion metadata. Full refresh each run.",
    "products": "Bronze products: raw rows exactly as delivered (all STRING) plus ingestion metadata. Full refresh each run.",
    "orders": "Bronze orders: raw JSON records exactly as delivered (all STRING) plus ingestion metadata. Full refresh each run.",
    "order_items": "Bronze order items: raw rows exactly as delivered (all STRING) plus ingestion metadata. Full refresh each run.",
}


class BronzeValidationError(RuntimeError):
    """At least one raw dataset is missing or invalid; no table was changed."""


def bronze_table_name(names, dataset):
    return quote(names.catalog, names.bronze, dataset.name)


def bronze_table_statements(names):
    """Re-runnable DDL for the four Bronze tables of one environment."""
    statements = []
    for dataset in DATASETS:
        columns = [f"`{column}` STRING" for column in dataset.columns] + [
            "`_ingested_at` TIMESTAMP NOT NULL",
            "`_source_file` STRING NOT NULL",
            "`_run_id` STRING NOT NULL",
        ]
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {bronze_table_name(names, dataset)} "
            f"({', '.join(columns)}) USING DELTA "
            f"COMMENT '{TABLE_COMMENTS[dataset.name]}'"
        )
    return statements


def resolve_run_id(run_id=None):
    """Use the given run id (e.g. a Job run id), or generate one when empty."""
    if run_id is None or (isinstance(run_id, str) and not run_id.strip()):
        return uuid.uuid4().hex
    if not isinstance(run_id, str):
        raise ValueError(f"Invalid run_id {run_id!r}: must be text.")
    run_id = run_id.strip()
    if len(run_id) > MAX_RUN_ID_LENGTH:
        raise ValueError(f"Invalid run_id: longer than {MAX_RUN_ID_LENGTH} characters.")
    return run_id


def read_raw_with_source(spark, volume_root, dataset):
    """Raw dataset with its explicit schema, plus the file each row came from."""
    return read_raw(spark, volume_root, dataset).select(
        *dataset.columns, F.col("_metadata.file_path").alias("_source_file")
    )


def with_ingestion_metadata(df, dataset, run_id, ingested_at):
    """Return the data columns in documented order, then the metadata columns.

    `df` must hold the dataset's columns and `_source_file`. Rows pass through
    untouched: no filtering, casting or deduplication.
    """
    missing = [c for c in (*dataset.columns, "_source_file") if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset.name}: input is missing columns {missing}")
    return df.select(
        *[F.col(c) for c in dataset.columns],
        F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
        F.col("_source_file"),
        F.lit(run_id).alias("_run_id"),
    )


def ingest_bronze(spark, catalog, env, run_id=None):
    """Validate all raw datasets, then fully refresh the four Bronze tables.

    Returns (run_id, {dataset name: row count}).
    """
    names = layer_names(catalog, env)
    run_id = resolve_run_id(run_id)
    volume_root = names.raw_volume_path

    problems = validate_raw_structure(spark, volume_root)
    if problems:
        raise BronzeValidationError(
            "Bronze ingestion stopped before changing any table. "
            + "; ".join(f"{dataset}: {problem}" for dataset, problem in problems)
        )

    for statement in bronze_table_statements(names):
        spark.sql(statement)

    ingested_at = datetime.now(timezone.utc)
    counts = {}
    for dataset in DATASETS:
        table = bronze_table_name(names, dataset)
        bronze_df = with_ingestion_metadata(
            read_raw_with_source(spark, volume_root, dataset), dataset, run_id, ingested_at
        )
        # Replace all rows of the existing table; its DDL, comment and history stay.
        bronze_df.writeTo(table).overwrite(F.lit(True))
        counts[dataset.name] = spark.table(table).count()
    return run_id, counts
