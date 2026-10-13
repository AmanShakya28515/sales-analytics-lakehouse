# Pipeline notebooks

Orchestration notebooks: they read parameters, call functions from `src/`
and write tables.

- `bronze_ingest`: raw volume → Bronze Delta tables (full refresh).
- `silver_transform`: Bronze tables → typed, deduplicated Silver tables plus
  `quarantine` (full refresh).
