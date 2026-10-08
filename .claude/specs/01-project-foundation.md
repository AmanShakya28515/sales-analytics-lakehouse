---
step: 01
slug: project-foundation
status: done
created: 2026-10-08
approved: 2026-10-08
---

# 01 — Project foundation

## Goal
A clean, reproducible starting point for the Sales Analytics Lakehouse: an
agreed project layout, the Unity Catalog containers for each medallion layer,
and small sample raw datasets (customers, products, orders, order_items)
stored in a Unity Catalog volume, ready for Bronze ingestion in the next step.

## Context / why now
Nothing exists yet besides CLAUDE.md. Every later step (Bronze ingestion,
Silver cleaning, Gold aggregates, Jobs) needs a known place for code, a known
place for raw files, and known sample data to test against. Settling these
first means later steps only add pipeline logic, not structure.

## User stories
- As the learner, I want a documented project layout, so that I know where
  source logic, notebooks, setup code, tests and sample data belong.
- As the learner, I want one re-runnable setup that creates the catalog
  objects and raw storage, so that I can rebuild the environment from scratch
  and understand each Unity Catalog object it creates.
- As the learner, I want small, realistic, documented sample datasets with
  some deliberate quality problems, so that later Bronze/Silver steps have
  something meaningful to ingest and clean.
- As the learner, I want dev and test runs to use separate, parameterised
  locations, so that tests never touch my working data.

## Acceptance criteria

**Project layout**
- **AC-1** Given the repository, when I read the project README (or the
  CLAUDE.md architecture section), then it describes each top-level folder
  and what belongs in it (source logic, orchestration notebooks, setup code,
  tests, sample data), and every described folder exists.
- **AC-2** Given the repository, when the local syntax check is run, then it
  passes with no errors.

**Workspace resources**
- **AC-3** Given a Databricks Free Edition workspace, when the setup is run
  with a catalog name and an environment name (e.g. `dev`, `test`) as
  parameters, then the catalog exists (created as a dedicated project
  catalog where Free Edition allows it, otherwise the existing workspace
  catalog is used), the schemas `<env>_bronze`, `<env>_silver` and
  `<env>_gold` exist in it, and a Unity Catalog volume named `raw_data`
  exists inside `<env>_bronze`. No tables are created in any of the three
  schemas.
- **AC-4** Given the setup has already run, when it is run again with the
  same parameters, then it completes without error, creates nothing extra,
  and leaves existing objects and files unchanged in count.
- **AC-5** Given the environments `dev` and `test`, when the setup runs for
  each, then `dev_bronze`/`dev_silver`/`dev_gold` and
  `test_bronze`/`test_silver`/`test_gold` all exist, each environment has
  its own `raw_data` volume, and files loaded into one are not visible in
  the other.
- **AC-6** Given a catalog or environment name parameter containing
  characters outside letters, digits and underscores (e.g. `x; DROP`, a
  backtick, an empty value), when the setup runs, then it stops with a clear
  error before creating anything.
- **AC-7** No catalog, schema or volume name is hard-coded in setup or
  pipeline code; defaults may appear only as parameter defaults.

**Sample data**
- **AC-8** Given the repository, then it contains the four sample datasets
  (customers, products, orders, order_items), versioned with the project, so
  the same raw files can be reproduced in any workspace. Customers, products
  and order_items are CSV files with a header row; orders is a JSON file.
- **AC-9** Given the sample data documentation (data dictionary), then for
  each dataset it lists every column with its meaning, intended type,
  nullability, primary key, and relationships to other datasets, plus the
  expected row count of each file.
- **AC-10** Given the sample datasets, then they are small (each under
  ~500 rows) and the "clean" majority of records is consistent: every
  order references an existing customer, every order item references an
  existing order and product, and amounts/quantities are positive.
- **AC-11** Given the sample datasets, then they also contain a small,
  documented set of deliberate data-quality issues for later steps to
  handle, covering at least: a null in a key column, an exact duplicate row,
  a duplicate key with differing values (a changed record), a malformed
  value (e.g. unparseable date or non-numeric amount), and an orphan
  reference (e.g. order item with an unknown product). Each issue is listed
  in the data dictionary with the dataset and key it affects.

**Raw storage and access**
- **AC-12** Given setup has run, when the sample files are loaded into the
  environment's `raw_data` volume (in `<env>_bronze`), then each dataset
  sits in its own clearly named location in the volume, and re-loading
  replaces the files rather than adding copies.
- **AC-13** Given the raw files are in the volume, when a verification
  notebook reads each dataset using an explicitly declared schema (no
  inference), then the row count for each dataset equals the documented
  count and the columns match the data dictionary.
- **AC-14** Given an empty or missing dataset location, when the
  verification runs, then it reports which dataset is missing instead of
  passing silently.

## Out of scope
- Bronze, Silver and Gold tables or transformations (Bronze ingestion is
  step 02). Only the empty schemas and the `raw_data` volume are created
  here.
- Jobs, Workflows, Asset Bundles, CI/CD, monitoring, git setup.
- Unity Catalog grants beyond the workspace owner's default access (Free
  Edition is single-user; group-based grants come with a later step).
- Large or generated-at-scale data, streaming or incremental arrival,
  external cloud storage.
- Dashboards or any UI.

## Decisions (answered open questions)
- **Q1 Catalog:** a dedicated project catalog if Free Edition allows
  creating one, otherwise the workspace's default catalog. The catalog name
  is always a parameter.
- **Q2 Schemas:** one schema per medallion layer per environment
  (`dev_bronze`, `dev_silver`, `dev_gold`, `test_bronze`, `test_silver`,
  `test_gold`).
- **Q3 Formats:** CSV for customers, products and order_items; JSON for
  orders.
- **Q4 Dirty records:** included now (AC-11), so the same raw files serve
  Bronze ingestion and Silver cleaning.

## Open questions
- (none)
