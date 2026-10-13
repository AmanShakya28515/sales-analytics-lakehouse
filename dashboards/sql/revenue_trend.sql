-- Revenue trend chart (AC-2): revenue per calendar month and channel, in date order.
WITH guard AS (
  SELECT assert_true(
    :catalog RLIKE '^[A-Za-z0-9_]+$' AND :env RLIKE '^[A-Za-z0-9_]+$',
    'catalog and env may only contain letters, digits and _'
  ) AS ok
)
SELECT
  CAST(date_trunc('MONTH', order_date) AS DATE) AS month,
  channel,
  sum(revenue) AS revenue,
  sum(order_count) AS order_count,
  sum(units) AS units
FROM IDENTIFIER('`' || :catalog || '`.`' || :env || '_gold`.`daily_sales`')
WHERE (SELECT ok FROM guard) IS NULL
GROUP BY 1, 2
ORDER BY month, channel
