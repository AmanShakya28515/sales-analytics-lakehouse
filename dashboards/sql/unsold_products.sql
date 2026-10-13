-- Unsold products table (AC-3): products with no qualifying orders.
WITH guard AS (
  SELECT assert_true(
    :catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$',
    'catalog and env may only contain letters, digits and _'
  ) AS ok
)
SELECT
  product_id,
  product_name,
  category,
  is_active
FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`product_sales`')
WHERE (SELECT ok FROM guard) IS NULL
  AND order_count = 0
ORDER BY product_id
