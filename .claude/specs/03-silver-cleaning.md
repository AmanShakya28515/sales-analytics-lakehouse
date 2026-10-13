---
step: 03
slug: silver-cleaning
status: done
created: 2026-10-13
approved: 2026-10-13
---

# 03 — Silver cleaning

## Goal
Turn the four Bronze tables into clean, typed, deduplicated Silver tables in
which every key is present and unique and every reference resolves. Every
row that cannot be cleaned goes to quarantine with the reason, so nothing is
lost silently.

## Context / why now
Step 02 loads the raw data into Bronze exactly as delivered: all text, with
the 10 documented dirty records still in it (see CLAUDE.md → Architecture
and `docs/data_dictionary.md`). Gold aggregates need trustworthy, typed data,
so Silver comes next. Decisions already made with the user: bad rows go to
**quarantine**, orphan references are **rejected like bad rows**, and tables
are written by **full refresh**, the same as Bronze.

## User stories
- As the learner, I want Silver tables with real types (dates, decimals,
  integers, booleans), so that I can query and aggregate them correctly.
- As the learner, I want each dirty-record category handled deliberately and
  visibly, so that I understand what cleaning a Silver layer involves.
- As a data consumer, I want to see which rows were rejected and why, so that
  I can trust the clean tables and follow up on the bad data.
- As a future Gold step, I want unique keys and resolving references, so
  that joins and aggregates don't double-count or drop data silently.

## Acceptance criteria

**Tables and types**
- **AC-1** Given the four Bronze tables of an environment, when Silver runs
  with `catalog` and `env` parameters, then `<catalog>.<env>_silver.customers`,
  `.products`, `.orders` and `.order_items` exist. Each has the documented
  columns, with the intended types from the data dictionary.
- **AC-2** Every Silver row carries lineage metadata: the Silver run
  identifier, when the Silver run happened, and the Bronze run identifier
  it was built from.
- **AC-3** Text values are trimmed, and empty text counts as null for every
  rule below.

**Row rules.** A row that breaks any of these goes to quarantine and is
not in its Silver table:
- **AC-4** A row whose primary key is null is rejected (`null_key`).
- **AC-5** A row with a required column (not nullable in the data dictionary)
  that is null is rejected (`missing_value`).
- **AC-6** A row with a value that cannot be converted to its intended type is
  rejected (`malformed_value`). For example `abc` as a price, or `2024-04-31`
  as a date.
- **AC-7** A row with a value outside its documented domain is rejected
  (`invalid_value`). Prices and quantities must be > 0, `status` must be
  one of `completed`, `shipped`, `pending` or `cancelled`, and `channel`
  must be one of `web`, `mobile` or `store`.
- **AC-8** When a row breaks several rules, quarantine records **all** of the
  reasons that apply.

**Duplicates**
- **AC-9** Rows that are identical in every source column collapse to one
  Silver row. This is not a rejection.
- **AC-10** Customers with the same `customer_id` but different values keep
  only the version with the latest `updated_at`. The older versions are
  superseded, not rejected.
- **AC-11** Duplicate keys whose versions disagree and cannot be resolved by
  AC-10 are all rejected (`conflicting_duplicate`). That covers any dataset
  other than customers, and customers whose latest `updated_at` is tied. No
  version is picked arbitrarily.
- **AC-12** After the run, each Silver table's primary key is non-null and
  unique.

**References**
- **AC-13** An order whose `customer_id` is not among the Silver customers is
  rejected. An order item whose `order_id` is not among the Silver orders, or
  whose `product_id` is not among the Silver products, is rejected. The
  reason is `orphan_reference` when the parent isn't in Bronze at all, and
  `parent_rejected` when the parent exists in Bronze but was quarantined.
  So the rejection cascades: every item of a rejected order or of a
  rejected product is rejected too. For example, every item of P015, whose
  list price is malformed, is rejected even when its own `unit_price` is
  valid (Q-1).
- **AC-14** After the run, every foreign key in Silver resolves to a row in
  the parent Silver table.

**Quarantine**
- **AC-15** Each rejected row is inspectable in the environment's Silver
  schema with: its dataset, its original Bronze values (unchanged text), its
  reason(s), and the Silver and Bronze run identifiers.
- **AC-16** With the sample data, the outcome is:

  | Dataset | Bronze rows | Silver rows | Quarantined rows |
  |---|---|---|---|
  | customers | 23 | 20 | 1 (null key) |
  | products | 15 | 14 | 1 (P015 malformed price) |
  | orders | 41 | 38 | 2 (O0025 malformed date, O0033 orphan) |
  | order_items | 90 | 76 | 14 (OI0045 orphan, OI0060 null key, OI0075 quantity 0, 4 items of rejected orders, 7 more items of rejected product P015) |

  C007 and O0010 collapse to one row each (AC-9), and C012 keeps the
  2024-03-18 version (AC-10). OI0073 belongs to both rejected order O0033 and
  rejected product P015, so it is counted once and both reasons apply (AC-8).

**Run behaviour**
- **AC-17** Full refresh: each run replaces every Silver table and the
  quarantine with the result for the current Bronze data. A re-run on the
  same Bronze data gives the same rows (only run metadata differs), and
  nothing is duplicated or carried over from earlier runs.
- **AC-18** All or nothing on inputs: if any Bronze table is missing, or the
  four Bronze tables don't all come from the same single Bronze run (mixed
  or multiple `_run_id`s, see 02 Q-2), the run fails with a clear message
  and no Silver table or quarantine is created or changed.
- **AC-19** Empty input is handled: a dataset with zero clean rows produces
  an empty Silver table (with the correct schema), not an error.
- **AC-20** The run reports, for each dataset, the Bronze, Silver,
  superseded/collapsed and quarantined row counts, and the quarantined
  counts per reason.

## Out of scope
- Gold aggregates and dashboards.
- Incremental processing / `MERGE`, history of changes (SCD2). Superseded
  customer versions are not kept.
- Checking that identifiers match a pattern (for example `C` + 3 digits).
- Recomputing or cross-checking order item prices against product prices.
- Rejecting an order because some of its items were rejected. The order
  stays, with fewer items.
- Unity Catalog grants, Jobs/Workflows orchestration, alerting on rejects.

## Open questions
- **Q-1 (answered 2026-10-13): strict cascade.** P015 is quarantined for its
  malformed list price, and all of its order items are quarantined as
  `parent_rejected`. Silver guarantees that every foreign key resolves to a
  valid Silver parent (AC-13, AC-14). The AC-16 counts stand.
