/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- ============================================================================
-- Create Dimension: gold.dim_customers
-- ============================================================================
CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT 
    -- 1. Surrogate Key Generation
    -- Creates a unique integer key for the Gold layer Star Schema
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- 2. Core CRM Identifiers
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    
    -- 3. Demographic Information
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_material_status AS marital_status,
    
    -- 4. Master Data Management (MDM): Gender Backfill Logic
    -- Business Rule: CRM is the master source for gender. 
    -- If CRM is missing ('n/a'), fallback to ERP data. If both are missing, output 'n/a'.
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE COALESCE(ca.gen, 'n/a') 
    END AS gender,
    
    -- Typo fixed: changed 'brithdate' to 'birthdate'
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

-- Join ERP Customer Data to enrich demographics (birthdate & fallback gender)
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Join ERP Location Data to enrich geographic details (country)
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


/*
===============================================================================
Create Dimension: gold.dim_products
===============================================================================
Purpose: 
    - Creates a dimensional view for products by joining CRM product info 
      with ERP category details.
    - Filters out historical records to only show currently active products.
===============================================================================
*/

CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
    -- Typo fixed: prodcuct_key changed to product_key
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcate AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
    
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical dates (Active products only)

/*
===============================================================================
Create facts: gold.fact_sales
===============================================================================
Purpose: 
    - Creates the central Fact Table for sales transactions.
    - Resolves natural keys from the Silver layer into surrogate keys 
      from the Gold layer dimensions to establish the Star Schema.
    - Serves as the primary source of truth for business metrics.
===============================================================================
*/

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    -- 1. Transaction Identifiers
    sd.sls_ord_num AS order_number,

    -- 2. Surrogate Keys (Foreign Keys to Dimensions)
    pr.product_key,
    cu.customer_key,

    -- 3. Temporal Data (Dates)
    sd.sls_ord_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,

    -- 4. Business Measures / Metrics
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity, -- Typo fixed: changed 'quanity' to 'quantity'
    sd.sls_price AS price

FROM silver.crm_sales_details sd

-- ============================================================================
-- Surrogate Key Resolution
-- ============================================================================

-- Match the string SKU from sales to the string product_number in the catalog
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Match the numeric CRM ID from sales to the numeric customer_id in the dimension
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
