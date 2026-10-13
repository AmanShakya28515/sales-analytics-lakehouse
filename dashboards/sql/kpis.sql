-- KPI tiles (AC-1): total revenue, qualifying orders, units, average order value, active customers.
WITH guard AS (
  SELECT assert_true(
    :catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$',
    'catalog and env may only contain letters, digits and _'
  ) AS ok
),
sales AS (
  SELECT
    coalesce(sum(revenue), 0) AS total_revenue,
    coalesce(sum(order_count), 0) AS qualifying_orders,
    coalesce(sum(units), 0) AS units,
    round(sum(revenue) / nullif(sum(order_count), 0), 2) AS avg_order_value
  FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`daily_sales`')
  WHERE (SELECT ok FROM guard) IS NULL
),
customers AS (
  SELECT count_if(order_count > 0) AS active_customers
  FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`customer_summary`')
  WHERE (SELECT ok FROM guard) IS NULL
)
SELECT
  s.total_revenue,
  s.qualifying_orders,
  s.units,
  s.avg_order_value,
  c.active_customers
FROM sales AS s
CROSS JOIN customers AS c
