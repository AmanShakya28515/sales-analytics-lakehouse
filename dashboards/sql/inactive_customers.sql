-- Inactive customers table (AC-4): customers with no qualifying orders.
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
  country
FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`customer_summary`')
WHERE (SELECT ok FROM guard) IS NULL
  AND order_count = 0
ORDER BY customer_id
