---
step: 04
slug: gold-aggregates
status: done
created: 2026-10-13
approved: 2026-10-13
---

# 04 — Gold aggregates

## Goal
Build three business-level Gold tables from Silver, ready for analytics and
later dashboards: daily sales, product sales and a customer summary. They
share one revenue definition, so their totals always agree.

## Context / why now
Step 03 produced typed Silver tables with unique keys and resolving foreign
keys, plus a quarantine (see CLAUDE.md → Architecture). Nothing in the
project answers a business question yet. Gold is the last medallion layer
and the input for the dashboards that come later. Decisions already made
with the user:
- the three tables below
- revenue counts only `completed` and `shipped` orders
- every Silver customer and product appears, with zeros when it has no sales.

## Definitions (apply to every AC)
- **Qualifying order:** a Silver order with status `completed` or
  `shipped`. `pending` and `cancelled` orders never count towards orders,
  units or revenue.
- **Line revenue:** `quantity × unit_price` of a Silver order item, using
  the price at time of sale, not the product's list price. **Revenue** is
  the sum of line revenue over the items of qualifying orders.
- **Units:** the sum of `quantity` over the items of qualifying orders.
- Monetary values are exact decimals with 2 decimal places. Averages are
  rounded half-up to 2 decimal places.

## User stories
- As an analyst, I want revenue per day and channel, so that I can see
  trends and compare web, mobile and store.
- As an analyst, I want sales per product, including products that didn't
  sell, so that I can find best sellers and dead stock.
- As an analyst, I want one row per customer with their spending, including
  customers who never bought, so that I can find top and inactive customers.
- As the learner, I want the three tables to reconcile with each other and
  with Silver, so that I trust Gold and learn how aggregates are checked.

## Acceptance criteria

**Tables**
- **AC-1** Given the Silver tables of an environment, when Gold runs with
  `catalog` and `env` parameters, then three Delta tables exist in
  `<catalog>.<env>_gold`. Each has the documented columns and types, and
  each has a table comment.
- **AC-2 Daily sales:** one row per (`order_date`, `channel`) that has at
  least one qualifying order. The columns are the order date, the channel,
  the number of qualifying orders, units, revenue, and average order value
  (revenue ÷ orders). Days with no qualifying orders have no row.
- **AC-3 Product sales:** one row per Silver product, including products
  with no sales. The columns are the product id, name, category,
  `is_active`, the number of distinct qualifying orders containing the
  product, units and revenue. Products without sales show 0 for all three.
- **AC-4 Customer summary:** one row per Silver customer, including
  customers with no qualifying orders. The columns are the customer id,
  first name, last name, email, country, the number of qualifying orders,
  total revenue, average order value, and first and last qualifying order
  dates.
  - Customers without qualifying orders show 0 orders and 0 revenue.
  - Their average order value and both dates are empty (null), never a
    division error.
- **AC-5** Every row carries lineage metadata: the Gold run identifier,
  when the Gold run happened, and the Silver run identifier it was built
  from.

**Correctness**
- **AC-6** Pending and cancelled orders and their items contribute nothing
  to any count, units or revenue figure.
- **AC-7** Grain and keys are unique: daily sales has one row per
  (date, channel), and product sales and customer summary have one row per
  product or customer. Joins never multiply rows. An order with several
  items is counted once in every order count.
- **AC-8 Reconciliation:** totals computed directly from Silver must match
  the Gold tables:
  - **Revenue:** the totals in daily sales, product sales and customer
    summary are all equal, and equal Silver revenue.
  - **Units:** the totals in daily sales and product sales are equal, and
    equal Silver units. Customer summary has no units column, so it isn't
    part of this check.
  - **Orders:** the totals of the daily sales and customer summary order
    counts are equal, and equal the number of qualifying Silver orders.
    Product sales counts orders per product, so an order with several
    products appears under each of them, and it isn't part of this check.
- **AC-9** With the sample data, the outcome is:

  | Figure | Expected |
  |---|---|
  | daily sales rows | 29 |
  | product sales rows | 14 (13 with sales, 1 with zeros) |
  | customer summary rows | 20 (18 with qualifying orders, 2 with zeros) |
  | qualifying orders | 29 |
  | total units | 113 |
  | total revenue | 9837.12 |

**Run behaviour**
- **AC-10** Full refresh: each run replaces all three Gold tables with the
  result for the current Silver data. A re-run on the same Silver data
  gives the same rows (only run metadata differs), and nothing is
  duplicated or carried over.
- **AC-11** All or nothing on inputs: if any Silver table is missing, or
  the Silver tables don't all come from one single Silver run, the run
  fails with a clear message and no Gold table is created or changed.
- **AC-12** Empty input is handled. With no qualifying orders, daily sales
  is empty, and every product and customer row shows zeros and nulls as
  above, with the correct schema and no error.
- **AC-13** Gold reads only the Silver data tables. Quarantined rows never
  reach Gold.

## Out of scope
- Dashboards and visualisations (a later step).
- Calendar filling (rows for days with no sales), and weekly, monthly or
  category roll-ups.
- Incremental refresh or `MERGE`, history and snapshots.
- Correcting revenue for rejected order items. Orders that lost items to
  quarantine in step 03 count only their Silver items.
- Returns, refunds, discounts, taxes and currency handling.
- Unity Catalog grants and Jobs/Workflows orchestration.

## Open questions
- (none)
