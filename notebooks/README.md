# Pipeline notebooks

Orchestration notebooks: they read parameters, call functions from `src/`
and write tables.

- `bronze_ingest`: raw volume → Bronze Delta tables (full refresh).
- `silver_transform`: Bronze tables → typed, deduplicated Silver tables plus
  `quarantine` (full refresh).
- `gold_aggregate`: Silver tables → Gold `daily_sales`, `product_sales` and
  `customer_summary` (full refresh).
