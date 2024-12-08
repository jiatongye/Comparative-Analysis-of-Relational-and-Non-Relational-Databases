WITH BusinessCounts AS (
  SELECT 
    state,
    COUNT(*) AS total_businesses
  FROM "Business" as business
  GROUP BY state
),
TakeOutCounts AS (
  SELECT 
    state,
    COUNT(*) AS takeout_businesses
  FROM "Business" AS business
  WHERE attributes->>'RestaurantsTakeOut' = 'True'
  GROUP BY state
),
Proportion AS (
  SELECT 
    b.state,
    (t.takeout_businesses * 1.0 / b.total_businesses) AS takeout_proportion
  FROM BusinessCounts b
  JOIN TakeOutCounts t ON b.state = t.state
)
SELECT state, takeout_proportion
FROM Proportion
ORDER BY takeout_proportion DESC
LIMIT 5;
