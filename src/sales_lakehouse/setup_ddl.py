"""Re-runnable DDL that creates the catalog objects for one environment.

Creates `<env>_bronze`, `<env>_silver`, `<env>_gold` and the `raw_data`
volume inside `<env>_bronze`. No tables are created here. Every statement uses
IF NOT EXISTS, so running setup again changes nothing.
"""

from sales_lakehouse.naming import layer_names, quote

SCHEMA_COMMENTS = {
    "bronze": "Bronze layer: raw data as ingested, plus ingestion metadata.",
    "silver": "Silver layer: cleaned, typed and deduplicated data.",
    "gold": "Gold layer: business-level aggregates ready for analytics.",
}


class CatalogCreationRefused(RuntimeError):
    """The catalog does not exist and Databricks refused to create it."""


def setup_statements(catalog, env):
    """Return the schema and volume DDL for one environment (catalog excluded)."""
    names = layer_names(catalog, env)
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {quote(names.catalog, schema)} "
        f"COMMENT '{SCHEMA_COMMENTS[layer]}'"
        for layer, schema in zip(("bronze", "silver", "gold"), names.schemas)
    ]
    statements.append(
        f"CREATE VOLUME IF NOT EXISTS {quote(names.catalog, names.bronze, names.raw_volume)} "
        "COMMENT 'Raw source files (CSV/JSON) awaiting Bronze ingestion.'"
    )
    return statements


def catalog_exists(spark, catalog):
    return any(c.name == catalog for c in spark.catalog.listCatalogs())


def run_setup(spark, catalog, env):
    """Validate the names, make sure the catalog exists, then run the DDL.

    If the catalog is missing and cannot be created (e.g. a Free Edition
    limit), stop with a clear message. There is deliberately no automatic
    fallback to another catalog.
    """
    names = layer_names(catalog, env)
    statements = setup_statements(names.catalog, names.env)

    if not catalog_exists(spark, names.catalog):
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote(names.catalog)}")
        except Exception as exc:
            raise CatalogCreationRefused(
                f"Could not create catalog '{names.catalog}'. This workspace may "
                "not allow creating catalogs (Databricks Free Edition limit). "
                "Re-run this setup with catalog=workspace (the workspace "
                f"catalog). Original error: {exc}"
            ) from exc

    for statement in statements:
        spark.sql(statement)
    return names
