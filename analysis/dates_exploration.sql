-- Find the date of the first and last order
-- How many years of sales ara abaiable

SELECT 
MIN(order_date), 
MAX(order_date),
DATEDIFF(year, MIN(order_date), MAX(order_date)) AS order_range_years,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS order_range_months
FROM gold.fact_sales

-- Find the youngest and the oldest customers

SELECT 
MIN(brithdate) AS oldest_customer,
DATEDIFF(year, MIN(brithdate), GETDATE()) AS oldest_age,
MAX(brithdate) AS youngest_customer,
DATEDiFF(year, MAX(brithdate), GETDATE()) AS youngest_age
FROM gold.dim_customers 