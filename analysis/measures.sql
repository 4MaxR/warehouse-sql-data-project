
-- FIND the total sales
SELECT SUM(sales_amount) AS total FROM gold.fact_sales

-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales

-- Find the average selling price
SELECT AVG(price) AS average_price FROM gold.fact_sales

-- Find the total number of orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales

-- Find the total number of products
SELECT COUNT(product_key) AS total_products FROM gold.dim_products
SELECT COUNT(DISTINCT product_key) as total_products FROM gold.dim_products

-- Find the total number of customers
SELECT COUNT(DISTINCT customer_key) as total_customers FROM gold.dim_customers

-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales

SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
	SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
	UNION ALL
	SELECT 'Average Price', AVG(price) AS average_price FROM gold.fact_sales
	UNION ALL
	SELECT 'Total Nr.Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
	UNION ALL
	SELECT 'Total Nr.Products', COUNT(DISTINCT product_key) FROM gold.dim_products
	UNION ALL
	SELECT 'Total Nr.Customers', COUNT(DISTINCT customer_key) FROM gold.dim_customers
