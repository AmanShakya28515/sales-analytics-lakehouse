# Sales Analytics Lakehouse

A step-by-step Databricks learning project. Raw sales data (customers,
products, orders, order items) moves through Bronze, Silver and Gold Delta
tables in Unity Catalog. See `CLAUDE.md` for goals, conventions and the
feature workflow.

## Project layout

| Folder | What belongs in it |
|---|---|
| `src/` | Plain Python functions (the `sales_lakehouse` package): naming, setup DDL, dataset registry, later the transformations. No notebook code. |
| `setup/` | Re-runnable setup notebooks: catalog objects, loading sample files, verifying raw data. |
| `notebooks/` | Pipeline orchestration notebooks (Bronze ingestion onwards, from step 02). |
| `tests/` | `unittest` tests (`test_<module>.py`) and the `run_tests` notebook that runs them in Databricks. |
| `data/sample/` | The versioned raw sample files, one folder per dataset. |
| `docs/` | Documentation, including the sample data dictionary. |

## Unity Catalog objects (per environment)

| Object | Name |
|---|---|
| Catalog | `<catalog>` parameter, default `sales_lakehouse` |
| Schemas | `<env>_bronze`, `<env>_silver`, `<env>_gold` |
| Volume | `<catalog>.<env>_bronze.raw_data` |

Raw files sit at `/Volumes/<catalog>/<env>_bronze/raw_data/<dataset>/`.
Use `env=dev` for working data and `env=test` for test runs.

## Getting started (Databricks Free Edition)

1. Import this project folder into your Databricks workspace (Workspace →
   your folder → Import, or drag and drop). Re-import after local changes.
2. Run `setup/00_setup_catalog_objects` with `catalog` and `env`.
   If Databricks refuses to create the catalog, the notebook stops and says
   so. Re-run it with `catalog=workspace`. It never switches catalogs on
   its own.
3. Run `setup/01_load_sample_data` with the same parameters. It replaces the
   files in the volume, so re-running it is safe.
4. Run `setup/02_verify_raw_data`. It fails if any dataset is missing or has
   the wrong row count or columns.
5. Run `tests/run_tests`. Set `catalog` to run the integration tests, which
   create and drop their own temporary `t01_<random>` schemas.

The sample data and its deliberate quality problems are documented in
`docs/data_dictionary.md`.
