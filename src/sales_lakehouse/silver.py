"""Silver cleaning: Bronze tables -> typed, deduplicated Silver tables plus quarantine.

One run: check that the four Bronze tables exist and come from a single
Bronze run, pin their Delta versions, build every Silver table and the
quarantine in memory, then fully refresh all five tables.

Per dataset, in parent-first order:
1. normalise: trim text, '' -> NULL, keep the original values for quarantine
2. collapse exact duplicates, then resolve key duplicates (customers: latest
   `updated_at` wins; anything unresolvable is `conflicting_duplicate`).
   Duplicates are resolved before the row rules on purpose (plan 03 D3), so
   an older valid version is never promoted over an invalid latest one.
3. row rules: null_key, missing_value, malformed_value, invalid_value
4. reference rules: orphan_reference / parent_rejected against the parent's
   Silver keys, so every Silver foreign key resolves.
Rows with no reasons go to Silver; every other row goes to quarantine with
all of its reasons.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from sales_lakehouse.bronze import resolve_run_id
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import DATASETS

DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

METADATA_COLUMNS = ("_run_id", "_processed_at", "_bronze_run_id")
QUARANTINE_TABLE = "quarantine"
QUARANTINE_COLUMNS = ("dataset", "record_key", "reasons", "source_record", "_source_file",
                      *METADATA_COLUMNS)

REASONS = (
    "null_key", "missing_value", "malformed_value", "invalid_value",
    "conflicting_duplicate", "orphan_reference", "parent_rejected",
)

REASONS_TYPE = "ARRAY<STRUCT<`reason`: STRING, `column`: STRING>>"


@dataclass(frozen=True)
class SilverColumn:
    name: str
    sql_type: str  # STRING | DATE | TIMESTAMP | DECIMAL(10,2) | INT | BOOLEAN
    nullable: bool = False
    allowed: tuple = ()  # allowed values; empty = no list
    positive: bool = False  # value must be > 0


_PRICE = "DECIMAL(10,2)"

SILVER_SCHEMA = {
    "customers": (
        SilverColumn("customer_id", "STRING"),
        SilverColumn("first_name", "STRING"),
        SilverColumn("last_name", "STRING"),
        SilverColumn("email", "STRING"),
        SilverColumn("city", "STRING", nullable=True),
        SilverColumn("country", "STRING", nullable=True),
        SilverColumn("signup_date", "DATE"),
        SilverColumn("updated_at", "TIMESTAMP"),
    ),
    "products": (
        SilverColumn("product_id", "STRING"),
        SilverColumn("product_name", "STRING"),
        SilverColumn("category", "STRING"),
        SilverColumn("unit_price", _PRICE, positive=True),
        SilverColumn("is_active", "BOOLEAN"),
    ),
    "orders": (
        SilverColumn("order_id", "STRING"),
        SilverColumn("customer_id", "STRING"),
        SilverColumn("order_date", "DATE"),
        SilverColumn("status", "STRING",
                     allowed=("completed", "shipped", "pending", "cancelled")),
        SilverColumn("channel", "STRING", allowed=("web", "mobile", "store")),
    ),
    "order_items": (
        SilverColumn("order_item_id", "STRING"),
        SilverColumn("order_id", "STRING"),
        SilverColumn("product_id", "STRING"),
        SilverColumn("quantity", "INT", positive=True),
        SilverColumn("unit_price", _PRICE, positive=True),
    ),
}

# Datasets whose key duplicates can be resolved: the latest version wins.
VERSION_COLUMNS = {"customers": "updated_at"}

TABLE_COMMENTS = {
    "customers": "Silver customers: trimmed, typed, one row per customer_id (latest updated_at). Full refresh each run.",
    "products": "Silver products: trimmed, typed, one row per product_id. Full refresh each run.",
    "orders": "Silver orders: trimmed, typed, one row per order_id; every customer_id resolves. Full refresh each run.",
    "order_items": "Silver order items: trimmed, typed, one row per order_item_id; every order_id and product_id resolves. Full refresh each run.",
    QUARANTINE_TABLE: "Silver quarantine: Bronze rows rejected by Silver, with original values and every reason. Full refresh each run.",
}


class SilverInputError(RuntimeError):
    """The Bronze input is missing or inconsistent; no Silver table was changed."""


@dataclass(frozen=True)
class SilverBuild:
    tables: dict  # {dataset: Silver DataFrame}
    quarantine: object  # DataFrame
    stages: dict  # {dataset: {"bronze", "distinct", "resolved": DataFrame}} for the report


# -- names and DDL ----------------------------------------------------------------

def silver_table_name(names, table):
    return quote(names.catalog, names.silver, table)


def silver_table_statements(names):
    """Re-runnable DDL for the four Silver tables and the quarantine table."""
    metadata = [
        "`_run_id` STRING NOT NULL",
        "`_processed_at` TIMESTAMP NOT NULL",
        "`_bronze_run_id` STRING NOT NULL",
    ]
    tables = {
        name: [f"`{c.name}` {c.sql_type}" + ("" if c.nullable else " NOT NULL") for c in columns]
        + metadata
        for name, columns in SILVER_SCHEMA.items()
    }
    tables[QUARANTINE_TABLE] = [
        "`dataset` STRING NOT NULL",
        "`record_key` STRING",
        f"`reasons` {REASONS_TYPE} NOT NULL",
        "`source_record` MAP<STRING, STRING> NOT NULL",
        "`_source_file` STRING",
        *metadata,
    ]
    return [
        f"CREATE TABLE IF NOT EXISTS {silver_table_name(names, name)} "
        f"({', '.join(columns)}) USING DELTA COMMENT '{TABLE_COMMENTS[name]}'"
        for name, columns in tables.items()
    ]


# -- input check -------------------------------------------------------------------

def check_bronze_inputs(spark, names):
    """Check the four Bronze tables, then pin their current Delta versions.

    Returns (bronze_run_id, {dataset: DataFrame at the pinned version},
    {dataset: version}). Raises SilverInputError when a table is missing, or
    when the tables do not hold exactly one `_run_id` between them.
    """
    missing = [
        d.name for d in DATASETS
        if not spark.catalog.tableExists(f"{names.catalog}.{names.bronze}.{d.name}")
    ]
    if missing:
        raise SilverInputError(
            f"Silver stopped before changing any table: Bronze tables missing in "
            f"{names.catalog}.{names.bronze}: {missing}. Run notebooks/bronze_ingest first."
        )

    versions, frames, run_ids = {}, {}, {}
    for dataset in DATASETS:
        table = quote(names.catalog, names.bronze, dataset.name)
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        versions[dataset.name] = version
        frames[dataset.name] = spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}")
        # At most a few rows of metadata, never data rows.
        run_ids[dataset.name] = sorted(
            r["_run_id"] for r in frames[dataset.name].select("_run_id").distinct().limit(5).collect()
        )

    distinct = {run_id for ids in run_ids.values() for run_id in ids}
    if len(distinct) != 1 or any(len(ids) != 1 for ids in run_ids.values()):
        raise SilverInputError(
            "Silver stopped before changing any table: the Bronze tables are not from a "
            f"single Bronze run (_run_id per table: {run_ids}). Re-run notebooks/bronze_ingest."
        )
    return distinct.pop(), frames, versions


# -- reason helpers ----------------------------------------------------------------

def _reason(reason, column):
    return F.struct(F.lit(reason).alias("reason"), F.lit(column).alias("column"))


def _reasons(*checks):
    """Array of (reason, column) structs for every check whose condition is true."""
    return F.filter(
        F.array(*[F.when(condition, _reason(reason, column)) for condition, reason, column in checks]),
        lambda r: r.isNotNull(),
    )


def _add_reasons(df, *checks):
    return df.withColumn("_reasons", F.concat(F.col("_reasons"), _reasons(*checks)))


def _typed(column):
    """Typed value of a normalised text column; NULL when it cannot be converted."""
    c = F.col(column.name)
    if column.sql_type == "STRING":
        return c
    if column.sql_type == "DATE":
        return F.to_date(F.call_function("try_to_timestamp", c, F.lit(DATE_FORMAT)))
    if column.sql_type == "TIMESTAMP":
        return F.call_function("try_to_timestamp", c, F.lit(TIMESTAMP_FORMAT))
    if column.sql_type == "BOOLEAN":
        lowered = F.lower(c)
        return F.when(lowered.isin("true", "false"), lowered == "true")
    # Numbers: try_cast never fails the query (serverless runs in ANSI mode).
    return F.expr(f"try_cast(`{column.name}` AS {column.sql_type})")


def _typed_name(column):
    return f"__typed_{column.name}"


# -- per-dataset stages ------------------------------------------------------------

def normalise(df, dataset):
    """Trim every data column and turn '' into NULL.

    Keeps the original Bronze values in `_source_record`, the file in
    `_source_file`, and the Bronze `_run_id` as `_bronze_run_id`.
    """
    source_record = F.create_map(*[part for c in dataset.columns for part in (F.lit(c), F.col(c))])
    return df.select(
        *[F.when(F.trim(F.col(c)) != "", F.trim(F.col(c))).alias(c) for c in dataset.columns],
        source_record.alias("_source_record"),
        F.col("_source_file"),
        F.col("_run_id").alias("_bronze_run_id"),
    )


def collapse_exact_duplicates(df, dataset):
    """Keep one row per identical set of normalised values (AC-9)."""
    window = Window.partitionBy(*dataset.columns).orderBy(
        F.to_json(F.col("_source_record")), F.col("_source_file")
    )
    return (
        df.withColumn("__copy", F.row_number().over(window))
        .where(F.col("__copy") == 1)
        .drop("__copy")
    )


def resolve_key_duplicates(df, dataset):
    """Resolve rows that share a primary key but differ (AC-10, AC-11).

    Adds `_reasons` (with `conflicting_duplicate` where unresolvable) and
    drops superseded customer versions. Rows with a null key are left alone.
    """
    key = F.col(dataset.primary_key)
    by_key = Window.partitionBy(dataset.primary_key)
    key_count = F.when(key.isNotNull(), F.count(F.lit(1)).over(by_key)).otherwise(F.lit(1))
    df = df.withColumn("__key_count", key_count)

    version_column = VERSION_COLUMNS.get(dataset.name)
    if version_column:
        spec = next(c for c in SILVER_SCHEMA[dataset.name] if c.name == version_column)
        df = (
            df.withColumn("__version", _typed(spec))
            .withColumn("__max_version", F.max("__version").over(by_key))
            .withColumn("__unparsed", F.sum(F.col("__version").isNull().cast("int")).over(by_key))
            .withColumn("__at_max", F.sum((F.col("__version") == F.col("__max_version")).cast("int"))
                        .over(by_key))
        )
        resolvable = (F.col("__unparsed") == 0) & (F.col("__at_max") == 1)
        superseded = resolvable & (F.col("__version") != F.col("__max_version"))
        drop = ["__version", "__max_version", "__unparsed", "__at_max"]
    else:
        resolvable = F.lit(False)
        superseded = F.lit(False)
        drop = []

    duplicated = F.col("__key_count") > 1
    return (
        df.where(~(duplicated & superseded))
        .withColumn("_reasons", _reasons(
            (duplicated & ~resolvable, "conflicting_duplicate", dataset.primary_key),
        ))
        .drop("__key_count", *drop)
    )


def apply_row_rules(df, dataset):
    """Add typed values and the row-rule reasons (AC-4 to AC-7)."""
    checks = []
    typed = []
    for column in SILVER_SCHEMA[dataset.name]:
        source = F.col(column.name)
        value = F.col(_typed_name(column))
        typed.append(_typed(column).alias(_typed_name(column)))
        if column.name == dataset.primary_key:
            checks.append((source.isNull(), "null_key", column.name))
        elif not column.nullable:
            checks.append((source.isNull(), "missing_value", column.name))
        checks.append((source.isNotNull() & value.isNull(), "malformed_value", column.name))
        if column.positive:
            checks.append((value <= 0, "invalid_value", column.name))
        if column.allowed:
            checks.append((~value.isin(*column.allowed), "invalid_value", column.name))
    return _add_reasons(df.select("*", *typed), *checks)


def parent_keys(df, dataset):
    """Distinct non-null primary keys of a normalised or Silver DataFrame, as `__key`."""
    return (
        df.select(F.col(dataset.primary_key).alias("__key"))
        .where(F.col("__key").isNotNull())
        .distinct()
    )


def apply_reference_rules(df, fk_column, silver_keys, bronze_keys):
    """Reject rows whose foreign key is not a Silver parent (AC-13).

    `orphan_reference` when the key is not in Bronze at all, `parent_rejected`
    when the parent exists in Bronze but did not reach Silver. A null foreign
    key is left to the missing_value rule (plan 03 D2).
    """
    in_silver, in_bronze = f"__in_silver_{fk_column}", f"__in_bronze_{fk_column}"
    joined = (
        df.join(
            silver_keys.select(F.col("__key").alias(in_silver)),
            F.col(fk_column) == F.col(in_silver), "left",
        )
        .join(
            bronze_keys.select(F.col("__key").alias(in_bronze)),
            F.col(fk_column) == F.col(in_bronze), "left",
        )
    )
    fk = F.col(fk_column)
    missing_in_silver = fk.isNotNull() & F.col(in_silver).isNull()
    return _add_reasons(
        joined,
        (missing_in_silver & F.col(in_bronze).isNull(), "orphan_reference", fk_column),
        (missing_in_silver & F.col(in_bronze).isNotNull(), "parent_rejected", fk_column),
    ).drop(in_silver, in_bronze)


# -- assembly ----------------------------------------------------------------------

def _run_metadata(run_id, processed_at):
    return (
        F.lit(run_id).alias("_run_id"),
        F.lit(processed_at).cast("timestamp").alias("_processed_at"),
        F.col("_bronze_run_id"),
    )


def build_silver(bronze_dfs, run_id, processed_at):
    """Build the four Silver DataFrames and the quarantine from Bronze DataFrames.

    `bronze_dfs` maps dataset name -> DataFrame with the dataset's columns
    plus `_source_file` and `_run_id`. Nothing is written.
    """
    tables, quarantines, stages = {}, [], {}
    normalised_by_name = {}
    for dataset in DATASETS:  # parents come first in the registry
        normalised = normalise(bronze_dfs[dataset.name], dataset)
        normalised_by_name[dataset.name] = normalised
        distinct = collapse_exact_duplicates(normalised, dataset)
        resolved = resolve_key_duplicates(distinct, dataset)
        checked = apply_row_rules(resolved, dataset)
        for fk_column, parent_name in dataset.foreign_keys:
            parent = next(d for d in DATASETS if d.name == parent_name)
            checked = apply_reference_rules(
                checked, fk_column,
                parent_keys(tables[parent_name], parent),
                parent_keys(normalised_by_name[parent_name], parent),
            )

        valid = F.size(F.col("_reasons")) == 0
        tables[dataset.name] = checked.where(valid).select(
            *[F.col(_typed_name(c)).cast(c.sql_type).alias(c.name)
              for c in SILVER_SCHEMA[dataset.name]],
            *_run_metadata(run_id, processed_at),
        )
        quarantines.append(checked.where(~valid).select(
            F.lit(dataset.name).alias("dataset"),
            F.col(dataset.primary_key).alias("record_key"),
            F.col("_reasons").alias("reasons"),
            F.col("_source_record").alias("source_record"),
            F.col("_source_file"),
            *_run_metadata(run_id, processed_at),
        ))
        stages[dataset.name] = {"bronze": bronze_dfs[dataset.name], "distinct": distinct,
                                "resolved": resolved}

    quarantine = quarantines[0]
    for other in quarantines[1:]:
        quarantine = quarantine.unionByName(other)
    return SilverBuild(tables=tables, quarantine=quarantine, stages=stages)


def silver_report(spark, names, build):
    """Per-dataset counts after a run (AC-20): read from the written tables."""
    quarantine = spark.table(silver_table_name(names, QUARANTINE_TABLE))
    per_reason = (
        quarantine.select("dataset", F.explode(F.array_distinct(F.col("reasons.reason"))).alias("reason"))
        .groupBy("dataset", "reason").count()
        .collect()  # aggregated: at most datasets x reasons rows
    )
    report = {}
    for dataset in DATASETS:
        stage = build.stages[dataset.name]
        bronze, distinct, resolved = (stage[k].count() for k in ("bronze", "distinct", "resolved"))
        report[dataset.name] = {
            "bronze": bronze,
            "silver": spark.table(silver_table_name(names, dataset.name)).count(),
            "collapsed": bronze - distinct,
            "superseded": distinct - resolved,
            "quarantined": quarantine.where(F.col("dataset") == dataset.name).count(),
            "reasons": {r["reason"]: r["count"] for r in per_reason if r["dataset"] == dataset.name},
        }
    return report


def transform_silver(spark, catalog, env, run_id=None):
    """Check Bronze, build Silver, and fully refresh the five Silver tables.

    Returns (run_id, report).
    """
    names = layer_names(catalog, env)
    run_id = resolve_run_id(run_id)

    _, bronze_dfs, _ = check_bronze_inputs(spark, names)
    processed_at = datetime.now(timezone.utc)
    build = build_silver(bronze_dfs, run_id, processed_at)

    for statement in silver_table_statements(names):
        spark.sql(statement)

    for dataset in DATASETS:
        # Replace all rows of the existing table; its DDL, comment and history stay.
        build.tables[dataset.name].writeTo(silver_table_name(names, dataset.name)).overwrite(F.lit(True))
    build.quarantine.writeTo(silver_table_name(names, QUARANTINE_TABLE)).overwrite(F.lit(True))

    return run_id, silver_report(spark, names, build)
