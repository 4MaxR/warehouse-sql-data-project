-- Which 5 products generate the highest revenue?

SELECT TOP 5
p.product_name,
SUM(f.sales_amount) AS Total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY Total_sales DESC

SELECT 
p.product_name,
SUM(f.sales_amount) AS Total_sales,
ROW_NUMBER() OVER(ORDER BY SUM(f.sales_amount) DESC) as rank_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name

-- What are the 5 worst-performing products in terms of sales?

SELECT TOP 5
p.product_name,
SUM(f.sales_amount) AS Total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY Total_sales 

-- which 5 subcategories generate the highest revenue?
SELECT TOP 5
p.subcategory,
SUM(f.sales_amount) AS Total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.subcategory
ORDER BY Total_sales DESC

-- what are the worst-performing subcategories in terms of sales?

SELECT TOP 5
p.subcategory,
SUM(f.sales_amount) AS Total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.subcategory
ORDER BY Total_sales 

-- Top costemrs

SELECT TOP 5
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_orders DESC

-- Ranking lowest the customers had orders

SELECT TOP 3
c.customer_key,
c.first_name,
c.last_name,
COUNT(f.order_number) AS total_orders,
ROW_NUMBER() OVER(ORDER BY SUM(f.sales_amount) ) AS rank_customer
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name