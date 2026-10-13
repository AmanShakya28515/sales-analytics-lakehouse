-- Top customers table (AC-4): customers with qualifying orders, highest revenue first, ties by customer_id.
WITH guard AS (
  SELECT assert_true(
    :catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$',
    'catalog and env may only contain letters, digits and _'
  ) AS ok
)
SELECT
  customer_id,
  first_name,
  last_name,
  country,
  order_count,
  revenue,
  avg_order_value,
  last_order_date
FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`customer_summary`')
WHERE (SELECT ok FROM guard) IS NULL
  AND order_count > 0
ORDER BY revenue DESC, customer_id
