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

The project runs from a **Databricks Git folder** cloned from this GitHub
repository. After changing code locally, commit and push. Then, in the Git
folder, click the branch name and **Pull** before running anything.

Run these in order with the same `catalog` and `env` widgets:

1. `setup/00_setup_catalog_objects` creates the catalog (if allowed), the
   `<env>_bronze/_silver/_gold` schemas and the `raw_data` volume. If
   Databricks refuses to create the catalog, the notebook stops and says so.
   Re-run it with `catalog=workspace`. It never switches catalogs on its own.
2. `setup/01_load_sample_data` copies `data/sample/` into the volume. It
   replaces existing files, so re-running is safe.
3. `setup/02_verify_raw_data` fails if any dataset is missing or has the
   wrong row count or columns.
4. `notebooks/bronze_ingest` loads the raw files into the Bronze Delta tables.
   Each run is a full refresh with ingestion metadata. If any dataset is
   invalid, nothing is changed.
5. `tests/run_tests` runs the tests. Set `catalog` to run the integration
   tests, which create and drop their own temporary `t01_<random>` schemas.

The sample data, the Bronze tables and the deliberate quality problems are
documented in `docs/data_dictionary.md`.
