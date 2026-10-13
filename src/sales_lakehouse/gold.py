"""Gold aggregates: Silver tables -> daily sales, product sales and customer summary.

One run: check that the four Silver data tables exist and come from a single
Silver run (empty tables are allowed, plan 04 D2), pin their Delta versions,
build the three Gold tables in memory, then fully refresh them. The
quarantine is never read.

Revenue counts only qualifying orders (`completed`, `shipped`) and uses the
price at time of sale: quantity x order item unit_price. Orders are first
aggregated to one row per order, so an order is counted once however many
items it has; only product sales works from item rows, with a distinct count.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import functions as F

from sales_lakehouse.bronze import resolve_run_id
from sales_lakehouse.inputs import pin_single_run
from sales_lakehouse.naming import layer_names, quote
from sales_lakehouse.raw_datasets import DATASETS

QUALIFYING_STATUSES = ("completed", "shipped")
MONEY = "DECIMAL(18,2)"
METADATA_COLUMNS = ("_run_id", "_processed_at", "_silver_run_id")


@dataclass(frozen=True)
class GoldColumn:
    name: str
    sql_type: str
    nullable: bool = False


GOLD_SCHEMA = {
    "daily_sales": (
        GoldColumn("order_date", "DATE"),
        GoldColumn("channel", "STRING"),
        GoldColumn("order_count", "BIGINT"),
        GoldColumn("units", "BIGINT"),
        GoldColumn("revenue", MONEY),
        GoldColumn("avg_order_value", MONEY),
    ),
    "product_sales": (
        GoldColumn("product_id", "STRING"),
        GoldColumn("product_name", "STRING"),
        GoldColumn("category", "STRING"),
        GoldColumn("is_active", "BOOLEAN"),
        GoldColumn("order_count", "BIGINT"),
        GoldColumn("units", "BIGINT"),
        GoldColumn("revenue", MONEY),
    ),
    "customer_summary": (
        GoldColumn("customer_id", "STRING"),
        GoldColumn("first_name", "STRING"),
        GoldColumn("last_name", "STRING"),
        GoldColumn("email", "STRING"),
        GoldColumn("country", "STRING", nullable=True),
        GoldColumn("order_count", "BIGINT"),
        GoldColumn("revenue", MONEY),
        GoldColumn("avg_order_value", MONEY, nullable=True),
        GoldColumn("first_order_date", "DATE", nullable=True),
        GoldColumn("last_order_date", "DATE", nullable=True),
    ),
}

TABLE_COMMENTS = {
    "daily_sales": "Gold daily sales: one row per order_date and channel with qualifying (completed/shipped) orders, units, revenue and average order value. Full refresh each run.",
    "product_sales": "Gold product sales: one row per Silver product (zeros when unsold) with qualifying orders, units and revenue. Full refresh each run.",
    "customer_summary": "Gold customer summary: one row per Silver customer (zeros when no qualifying orders) with orders, revenue, average order value and first/last order dates. Full refresh each run.",
}


class GoldInputError(RuntimeError):
    """The Silver input is missing or inconsistent; no Gold table was changed."""


def gold_table_name(names, table):
    return quote(names.catalog, names.gold, table)


def gold_table_statements(names):
    """Re-runnable DDL for the three Gold tables."""
    metadata = [
        "`_run_id` STRING NOT NULL",
        "`_processed_at` TIMESTAMP NOT NULL",
        "`_silver_run_id` STRING NOT NULL",
    ]
    statements = []
    for table, columns in GOLD_SCHEMA.items():
        definitions = [
            f"`{c.name}` {c.sql_type}" + ("" if c.nullable else " NOT NULL") for c in columns
        ] + metadata
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {gold_table_name(names, table)} "
            f"({', '.join(definitions)}) USING DELTA COMMENT '{TABLE_COMMENTS[table]}'"
        )
    return statements


def check_silver_inputs(spark, names):
    """Check and pin the four Silver data tables (never the quarantine).

    Returns (silver_run_id or None when every table is empty, {dataset: DataFrame}).
    """
    run_id, frames, _ = pin_single_run(
        spark, names.catalog, names.silver, [d.name for d in DATASETS],
        layer="Silver", consumer="Gold", notebook="silver_transform", error=GoldInputError,
        allow_empty=True,
    )
    return run_id, frames


def _average(revenue, orders):
    """revenue / orders rounded half-up to 2 places; NULL when there are no orders."""
    return F.when(orders > 0, F.round(revenue / orders, 2))


def _shape(df, table, run_id, processed_at, silver_run_id):
    """Columns in GOLD_SCHEMA order and types, then the run metadata."""
    return df.select(
        *[F.col(c.name).cast(c.sql_type).alias(c.name) for c in GOLD_SCHEMA[table]],
        F.lit(run_id).alias("_run_id"),
        F.lit(processed_at).cast("timestamp").alias("_processed_at"),
        F.lit(silver_run_id).cast("string").alias("_silver_run_id"),
    )


def build_gold(silver_dfs, run_id, processed_at, silver_run_id):
    """Build the three Gold DataFrames from the four Silver data DataFrames.

    `silver_dfs` maps dataset name -> typed Silver DataFrame. Nothing is written.
    """
    zero_money = F.lit(0).cast(MONEY)
    zero_count = F.lit(0).cast("bigint")

    orders = (
        silver_dfs["orders"]
        .where(F.col("status").isin(*QUALIFYING_STATUSES))
        .select("order_id", "customer_id", "order_date", "channel")
    )
    # order_id is unique in Silver orders, so this join cannot multiply item rows.
    items = (
        silver_dfs["order_items"]
        .join(orders.select("order_id"), "order_id", "inner")
        .select("order_id", "product_id", "quantity",
                (F.col("quantity") * F.col("unit_price")).alias("line_revenue"))
    )
    per_order = items.groupBy("order_id").agg(
        F.sum("quantity").alias("units"), F.sum("line_revenue").alias("revenue"),
    )
    # One row per qualifying order; an order without Silver items still counts, with zeros.
    order_totals = orders.join(per_order, "order_id", "left").select(
        "order_id", "customer_id", "order_date", "channel",
        F.coalesce("units", zero_count).alias("units"),
        F.coalesce("revenue", zero_money).alias("revenue"),
    )

    daily = order_totals.groupBy("order_date", "channel").agg(
        F.count(F.lit(1)).alias("order_count"),
        F.sum("units").alias("units"),
        F.sum("revenue").alias("revenue"),
    ).withColumn("avg_order_value", _average(F.col("revenue"), F.col("order_count")))

    per_product = items.groupBy("product_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units"),
        F.sum("line_revenue").alias("revenue"),
    )
    products = (
        silver_dfs["products"]
        .select("product_id", "product_name", "category", "is_active")
        .join(per_product, "product_id", "left")
        .select(
            "product_id", "product_name", "category", "is_active",
            F.coalesce("order_count", zero_count).alias("order_count"),
            F.coalesce("units", zero_count).alias("units"),
            F.coalesce("revenue", zero_money).alias("revenue"),
        )
    )

    per_customer = order_totals.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("order_count"),
        F.sum("revenue").alias("revenue"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    customers = (
        silver_dfs["customers"]
        .select("customer_id", "first_name", "last_name", "email", "country")
        .join(per_customer, "customer_id", "left")
        .withColumn("order_count", F.coalesce("order_count", zero_count))
        .withColumn("revenue", F.coalesce("revenue", zero_money))
        .withColumn("avg_order_value", _average(F.col("revenue"), F.col("order_count")))
    )

    meta = (run_id, processed_at, silver_run_id)
    return {
        "daily_sales": _shape(daily, "daily_sales", *meta),
        "product_sales": _shape(products, "product_sales", *meta),
        "customer_summary": _shape(customers, "customer_summary", *meta),
    }


def gold_report(spark, names):
    """Rows and totals per Gold table, read back from the written tables."""
    figures = {
        "daily_sales": ("order_count", "units", "revenue"),
        "product_sales": ("units", "revenue"),
        "customer_summary": ("order_count", "revenue"),
    }
    report = {}
    for table, columns in figures.items():
        row = spark.table(gold_table_name(names, table)).agg(
            F.count(F.lit(1)).alias("rows"), *[F.sum(c).alias(c) for c in columns]
        ).first()
        report[table] = {"rows": row["rows"], **{c: row[c] for c in columns}}
    return report


def transform_gold(spark, catalog, env, run_id=None):
    """Check Silver, build Gold, and fully refresh the three Gold tables.

    Returns (run_id, report).
    """
    names = layer_names(catalog, env)
    run_id = resolve_run_id(run_id)

    silver_run_id, silver_dfs = check_silver_inputs(spark, names)
    processed_at = datetime.now(timezone.utc)
    gold = build_gold(silver_dfs, run_id, processed_at, silver_run_id)

    for statement in gold_table_statements(names):
        spark.sql(statement)
    for table, df in gold.items():
        # Replace all rows of the existing table; its DDL, comment and history stay.
        df.writeTo(gold_table_name(names, table)).overwrite(F.lit(True))

    return run_id, gold_report(spark, names)
