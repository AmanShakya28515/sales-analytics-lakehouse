# Sample data dictionary

The four raw sample datasets live in `data/sample/<dataset>/`. They are copied
into each environment's `raw_data` volume as
`/Volumes/<catalog>/<env>_bronze/raw_data/<dataset>/<file>`.

Raw files are read with an explicit **all-string** schema. The "Intended type"
column is the type each value should have once it is cleaned in Silver.
All people are fictional and every email uses `example.com`.

Relationships:
- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`

## customers

File: `customers.csv` (CSV with header row). Expected rows: 23

| Column | Meaning | Intended type | Nullable | Key |
|---|---|---|---|---|
| `customer_id` | Customer identifier, `C` + 3 digits | STRING | No | PK |
| `first_name` | First name | STRING | No | |
| `last_name` | Last name | STRING | No | |
| `email` | Contact email | STRING | No | |
| `city` | City of residence | STRING | Yes | |
| `country` | Country of residence | STRING | Yes | |
| `signup_date` | Date the customer registered | DATE | No | |
| `updated_at` | When this customer record last changed | TIMESTAMP | No | |

## products

File: `products.csv` (CSV with header row). Expected rows: 15

| Column | Meaning | Intended type | Nullable | Key |
|---|---|---|---|---|
| `product_id` | Product identifier, `P` + 3 digits | STRING | No | PK |
| `product_name` | Display name | STRING | No | |
| `category` | Product category | STRING | No | |
| `unit_price` | Current list price, > 0 | DECIMAL(10,2) | No | |
| `is_active` | Whether the product is still sold | BOOLEAN | No | |

## orders

File: `orders.json` (JSON Lines: one object per line). Expected rows: 41

| Column | Meaning | Intended type | Nullable | Key |
|---|---|---|---|---|
| `order_id` | Order identifier, `O` + 4 digits | STRING | No | PK |
| `customer_id` | Customer who placed the order | STRING | No | FK → customers |
| `order_date` | Date the order was placed | DATE | No | |
| `status` | `completed`, `shipped`, `pending` or `cancelled` | STRING | No | |
| `channel` | `web`, `mobile` or `store` | STRING | No | |

## order_items

File: `order_items.csv` (CSV with header row). Expected rows: 90

| Column | Meaning | Intended type | Nullable | Key |
|---|---|---|---|---|
| `order_item_id` | Order line identifier, `OI` + 4 digits | STRING | No | PK |
| `order_id` | Order the line belongs to | STRING | No | FK → orders |
| `product_id` | Product sold | STRING | No | FK → products |
| `quantity` | Units sold, > 0 | INT | No | |
| `unit_price` | Price per unit at time of sale, > 0 | DECIMAL(10,2) | No | |

## Bronze tables

`notebooks/bronze_ingest` loads each raw dataset into a Delta table in the
environment's Bronze schema:

| Table | Source | Rows (sample data) |
|---|---|---|
| `<env>_bronze.customers` | `raw_data/customers/` | 23 |
| `<env>_bronze.products` | `raw_data/products/` | 15 |
| `<env>_bronze.orders` | `raw_data/orders/` | 41 |
| `<env>_bronze.order_items` | `raw_data/order_items/` | 90 |

Each table has the dataset's columns above, in the same order and all
`STRING`, exactly as delivered. Nothing is cleaned, typed or deduplicated, so
every known issue below is present in Bronze. These metadata columns follow:

| Column | Type | Meaning |
|---|---|---|
| `_ingested_at` | TIMESTAMP, not null | When the run started (UTC). The same for every row of one run. |
| `_source_file` | STRING, not null | Full path of the raw file the row came from. |
| `_run_id` | STRING, not null | Run identifier, the same across all four tables of one run. It is generated unless a Job passes one. |

**Full refresh:** every run replaces each table with exactly the current raw
files, so a re-run never duplicates rows. Earlier versions remain in Delta
history (`DESCRIBE HISTORY`). **All or nothing:** all four datasets are
validated first. If any is missing, has a wrong header, has a malformed line
or has a JSON record with missing or extra fields, the run fails and no table
is created or changed.

## Known data-quality issues

These records are deliberately dirty, so that later steps have problems to
handle. Every other record is clean: keys are present and unique, every
reference resolves, and amounts and quantities are positive.

| Dataset | Row key | Column | Issue | Description |
|---|---|---|---|---|
| customers | (empty) | `customer_id` | null_key | A customer row (Jordan Lee) has no customer_id. |
| customers | C007 | `customer_id` | exact_duplicate | C007 appears twice with identical values. |
| customers | C012 | `email` | changed_record | C012 appears twice; the later row (updated_at 2024-03-18) has a new email and city. |
| products | P015 | `unit_price` | malformed_value | unit_price is 'abc', not a number. |
| orders | O0010 | `order_id` | exact_duplicate | O0010 appears twice with identical values. |
| orders | O0025 | `order_date` | malformed_value | order_date is 2024-04-31, which is not a real date. |
| orders | O0033 | `customer_id` | orphan_reference | customer_id C999 does not exist in customers. |
| order_items | OI0045 | `product_id` | orphan_reference | product_id P999 does not exist in products. |
| order_items | OI0060 | `order_id` | null_key | order_id is empty. |
| order_items | OI0075 | `quantity` | invalid_value | quantity is 0. |
