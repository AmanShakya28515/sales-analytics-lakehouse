---
step: 05
slug: analytics-dashboard
status: approved
created: 2026-10-13
approved: 2026-10-13
---

# 05 — Analytics dashboard

## Goal
A Databricks SQL dashboard built only on the Gold tables. It shows headline
KPIs, the revenue trend by channel, top products and top customers, so the
pipeline's results can be seen at a glance. Its queries and its layout are
versioned in Git, so it can be recreated for any environment.

## Context / why now
Step 04 produced three reconciled Gold tables (see CLAUDE.md →
Architecture). Today they can only be read with ad-hoc SQL, and CLAUDE.md
planned Databricks SQL dashboards as their own later step. Decisions
already made with the user:
- the four visual groups below
- every query and the dashboard definition are versioned in the repository
- catalog and environment are dashboard parameters, defaulting to
  `sales_lakehouse` / `dev`.

## User stories
- As an analyst, I want the headline numbers on one screen, so that I can
  see how the business is doing without writing SQL.
- As an analyst, I want revenue over time per channel, so that I can spot
  trends and compare web, mobile and store.
- As an analyst, I want the best and the unsold products, and the best and
  the inactive customers, so that I know where to act.
- As the learner, I want the dashboard defined in Git and parameterised, so
  that I can recreate it for `dev` or `test` and review changes in diffs.

## Acceptance criteria

**Content.** With the sample data in `dev`, each figure must match Gold
exactly.
- **AC-1 KPI tiles** show five figures:
  - total revenue: **9837.12**
  - qualifying orders: **29**
  - units: **113**
  - average order value: revenue ÷ orders, rounded to 2 places, **339.21**
  - active customers (customers with at least one qualifying order): **18**
- **AC-2 Revenue trend:** revenue per calendar month and channel, in date
  order. With the sample data that is 6 months (2024-01 to 2024-06) × the
  3 channels. The monthly totals sum to 9837.12, and per channel they are
  mobile 3732.65, store 3104.87 and web 2999.60.
- **AC-3 Top products:** products ranked by revenue (highest first), with
  name, category, units and orders. The top three are **P009** (2835.00),
  **P012** (1937.00) and **P010** (1047.00). A separate list shows the
  products with no sales, which is **P001**.
- **AC-4 Top customers:** customers ranked by revenue (highest first), with
  name, order count and last order date. The top three are **C013**
  (1860.00), **C017** (1116.80) and **C018** (949.50). A separate list
  shows the customers with no qualifying orders, which is **2** customers.
- **AC-5 Consistency with Gold:** every figure on the dashboard comes from
  the Gold tables, and none is recomputed from Silver or Bronze. All
  revenue totals on the dashboard (KPI tile, trend total, product total,
  customer total) are equal.
- **AC-6 Ties and empty data:**
  - Rankings break revenue ties by id, so the order is stable.
  - With no qualifying orders, every tile shows 0 and the average order
    value shows empty, never an error.
  - Charts and lists show no data rather than failing.

**Parameters and environments**
- **AC-7** The dashboard has `catalog` and `env` parameters, defaulting to
  `sales_lakehouse` and `dev`. Changing them makes every visual read that
  environment's Gold tables instead.
- **AC-8** No catalog or schema name is hard-coded in any query or in the
  dashboard definition. A parameter value that isn't a valid identifier
  (only letters, digits and `_`) is rejected rather than run.

**Versioning and safety**
- **AC-9** Every dashboard query is a versioned file in the repository, and
  the dashboard definition (its layout, visuals and the parameters above)
  is also versioned. Importing that definition into a workspace recreates
  the dashboard.
- **AC-10** Every query only reads data. Running the dashboard never
  creates, changes or deletes a table.
- **AC-11** The repository documents how to import or refresh the dashboard
  and what each visual shows, including the revenue definition from step
  04.

## Out of scope
- Scheduled refreshes, alerts, email subscriptions and published or shared
  links.
- Unity Catalog grants and dashboard permissions for other users or groups.
- New Gold tables or changes to the Gold definitions. If a visual needs
  data that Gold doesn't have, that is a separate step.
- Jobs and Workflows orchestration (a later step).
- Any UI outside Databricks SQL dashboards.

## Open questions
- (none)
