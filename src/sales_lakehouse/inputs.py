"""Shared input check for layers that read another layer's tables.

A run must only use tables that all came from one run of the layer before
it. This checks that the tables exist and share one `_run_id`, then pins
each table to the Delta version that passed the check, so a concurrent
rewrite of the input cannot change what this run reads.
"""

from sales_lakehouse.naming import quote


def pin_single_run(spark, catalog, schema, tables, *, layer, consumer, notebook, error,
                   allow_empty=False):
    """Check `catalog.schema.<table>` for each table, then pin their current versions.

    `layer` names the input layer and `consumer` the layer running the check
    (both used in messages); `notebook` is the notebook that produces the
    input. Raises `error` when a table is missing or the tables do not share
    exactly one `_run_id`.

    With `allow_empty`, an empty table (which has no `_run_id`) is skipped by
    the run-id comparison; the run id is then None if every table is empty.

    Returns (run_id, {table: DataFrame at the pinned version}, {table: version}).
    """
    missing = [t for t in tables if not spark.catalog.tableExists(f"{catalog}.{schema}.{t}")]
    if missing:
        raise error(
            f"{consumer} stopped before changing any table: {layer} tables missing in "
            f"{catalog}.{schema}: {missing}. Run notebooks/{notebook} first."
        )

    versions, frames, run_ids = {}, {}, {}
    for table in tables:
        name = quote(catalog, schema, table)
        version = int(spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"])
        versions[table] = version
        frames[table] = spark.sql(f"SELECT * FROM {name} VERSION AS OF {version}")
        # At most a few rows of metadata, never data rows.
        run_ids[table] = sorted(
            r["_run_id"] for r in frames[table].select("_run_id").distinct().limit(5).collect()
        )

    compared = {t: ids for t, ids in run_ids.items() if ids or not allow_empty}
    distinct = {run_id for ids in compared.values() for run_id in ids}
    all_empty = allow_empty and not compared
    if not all_empty and (len(distinct) != 1 or any(len(ids) != 1 for ids in compared.values())):
        raise error(
            f"{consumer} stopped before changing any table: the {layer} tables are not from a "
            f"single {layer} run (_run_id per table: {run_ids}). Re-run notebooks/{notebook}."
        )
    return (distinct.pop() if distinct else None), frames, versions
