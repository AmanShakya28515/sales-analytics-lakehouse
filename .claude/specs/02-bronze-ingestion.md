---
step: 02
slug: bronze-ingestion
status: approved
created: 2026-10-08
approved: 2026-10-08
---

# 02 — Bronze ingestion

## Goal
Load the four raw datasets from the environment's `raw_data` volume into
Bronze Delta tables, exactly as delivered plus ingestion metadata. A re-run
gives the same result, never duplicated rows.

## Context / why now
Step 01 built the per-environment schemas, the `raw_data` volume, the
versioned sample files (including 10 deliberately dirty records) and a
verifier (see CLAUDE.md → Architecture). Nothing is in Delta yet. Bronze is
the first table layer and the input for Silver cleaning, so it comes next.

## User stories
- As the learner, I want each raw dataset as a Bronze Delta table, so that I
  can query raw data with SQL and see what Delta adds over plain files.
- As the learner, I want to know when and from which file each row was
  ingested, so that I can trace any Silver/Gold value back to its source.
- As the learner, I want to be able to re-run ingestion safely, so that a
  repeated or scheduled run never doubles the data.
- As a future Silver step, I want Bronze to keep the dirty records unchanged,
  so that cleaning rules can be built and tested against them.

## Acceptance criteria

**Tables and content**
- **AC-1** Given setup and the raw files are in place for an environment,
  when Bronze ingestion runs with `catalog` and `env` parameters, then the
  Delta tables `<catalog>.<env>_bronze.customers`, `.products`, `.orders` and
  `.order_items` exist, with 23, 15, 41 and 90 rows respectively.
- **AC-2** Each Bronze table has every source column, in the documented
  order and as text, followed by these ingestion metadata columns:
  - `_ingested_at`: the ingestion timestamp, the same for every row of one run
  - `_source_file`: the full source file path
  - `_run_id`: a run identifier, the same across all four tables for one
    run and different between runs

  No metadata value is null.
- **AC-3** Bronze rows equal the source rows exactly: as a multiset, the
  data columns of each table match the rows in the raw file. All 10
  documented dirty records are present and unchanged. That includes both
  copies of the exact duplicates, both versions of the changed customer,
  the null keys, `abc`, `2024-04-31`, `C999`, `P999` and the zero quantity.
  Nothing is cleaned, typed, deduplicated or dropped.

**Idempotency and change** (full refresh: every run replaces each table)
- **AC-4** Given ingestion has run, when it runs again with unchanged raw
  files, then each table has the same row count and the same data rows. No
  rows are duplicated. Every row carries the new run's `_run_id` and
  `_ingested_at`.
- **AC-5** Given the raw files of a dataset are replaced with different
  content (rows added, removed or changed) and ingestion runs, then that
  Bronze table reflects exactly the new file, and no rows from the old file
  remain.

**Failure handling**
- **AC-6** Given a dataset folder in the volume is missing or empty, when
  ingestion runs, then it fails with a message naming that dataset. No
  Bronze table is created or changed in that run, including the other
  three. All four datasets are validated before any table is written, and
  good datasets are never ingested on their own.
- **AC-7** Given a raw file that does not match its documented structure,
  when ingestion runs, then it fails naming the dataset and no Bronze table
  is changed. This covers three cases: a CSV header that differs from the
  documented columns, a JSON record without a documented field, and a
  structurally malformed line.
- **AC-8** Given an invalid `catalog` or `env` value (same rules as step 01),
  when ingestion runs, then it stops with a clear error before reading or
  writing anything. No catalog, schema or table name is hard-coded.

**Isolation and side effects**
- **AC-9** Given Bronze tables exist for `dev`, when ingestion runs for
  `test`, then the `dev` tables are unchanged, and the reverse.
- **AC-10** After ingestion, the raw files in the volume are unchanged
  (nothing moved or deleted), and the Silver and Gold schemas still contain
  no tables.

**Documentation and operation**
- **AC-11** Ingestion is a single notebook run with `catalog` and `env`
  parameters, so a Databricks Job can call it later. It prints each table
  with its row count.
- **AC-12** Each Bronze table carries a comment that says what it holds.
  The data dictionary describes the Bronze tables and the metadata columns.
  The README "Getting started" covers the Git-folder workflow and the run
  order, including Bronze ingestion, which resolves step 01 finding Q-2.

## Out of scope
- Silver and Gold transformations: typing, cleaning, deduplication,
  referential checks.
- Incremental or streaming ingestion (Auto Loader, `COPY INTO`, change
  tracking). See decision Q1.
- Jobs/Workflows scheduling, alerts, monitoring dashboards.
- Unity Catalog grants beyond the owner's default access.
- New source datasets or changes to the sample data.

## Decisions (answered open questions)
- **Q1 Idempotency model:** full refresh. Every run replaces each Bronze
  table so that it exactly matches the current raw files. `COPY INTO`, Auto
  Loader and incremental ingestion come in a later step.
- **Q2 Failure scope:** validate all four datasets first. If any one is
  missing or structurally invalid, fail the whole run before any Bronze
  table changes. Good datasets are never ingested on their own.
- **Q3 Metadata columns:** `_ingested_at`, `_source_file`, `_run_id`.

## Open questions
- (none)
