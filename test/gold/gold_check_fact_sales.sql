/*
===============================================================================
Data Quality Audit: Orphaned Sales Records
===============================================================================
Purpose:
    - Identifies sales records in the fact table that do not have a matching 
      product in the product dimension (Data Integrity Check).

Performance Optimization Note:
    - Uses OPTION (HASH JOIN) to prevent the Query Optimizer from defaulting 
      to a Nested Loop execution plan. 
    - Because the underlying Gold layer dimensions are complex Views utilizing 
      ROW_NUMBER() window functions, a Hash Match forces the engine to 
      materialize the dimensions in memory once, preventing a CPU-crushing 
      infinite execution loop during the View Expansion phase.
===============================================================================
*/

SELECT 
    * FROM gold.fact_sales f

-- 1. Join to Customer Dimension to pull in master customer details
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key

-- 2. Join to Product Dimension to pull in master catalog details
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key

-- 3. Anti-Join Filter: Keep ONLY the sales where the product lookup failed
WHERE p.product_key IS NULL

-- 4. Execution Plan Override: Force a Hash Table build for heavy nested views
OPTION (HASH JOIN);
