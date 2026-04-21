/*
===============================================================================
Data Transformation & Load: Bronze to Silver (Sales Details)
===============================================================================
Purpose: 
    - Extract raw sales records from the Bronze layer.
    - Validate and format integer-based dates (YYYYMMDD) into standard DATE types.
    - Recalculate and cleanse financial anomalies (Sales, Quantity, Price).
    - Load the refined data into the Silver layer for analytical reporting.
===============================================================================
*/

-- ============================================================================
-- 1. DATA QUALITY PROFILING (Exploration Phase)
-- ============================================================================
/*
-- Check for Invalid dates
SELECT ISNULL(sls_due_dt,0) AS sls_due_dt FROM bronze.crm_sales_details ...

-- Check for Invalid date Orders
SELECT * FROM silver.crm_sales_details ...

-- Check Data Consistency: Between SALES, Quantity, And price.
SELECT DISTINCT sls_sales, sls_quantity, sls_price FROM silver.crm_sales_details ...
*/

-- ============================================================================
-- 2. DATA TRANSFORMATION & LOAD (ETL Phase)
-- ============================================================================

-- Enforce idempotency: Clear existing Silver data before loading new data
TRUNCATE TABLE silver.crm_sales_details; 

-- Insert cleansed and standardized data into the target Silver table
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    [sls_ord_num],
    [sls_prd_key],
    [sls_cust_id],

    -- ==========================================
    -- 1. DATE CLEANSING & FORMATTING
    -- ==========================================
    -- Business Rule: Dates are stored as integers (YYYYMMDD). 
    -- If the value is 0 or not exactly 8 digits, flag it as NULL (invalid).
    -- Otherwise, cast the integer to a string, then to a standard SQL DATE.
    
    CASE 
        WHEN sls_ord_dt = 0 OR LEN(sls_ord_dt) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_ord_dt AS VARCHAR) AS DATE)
    END AS sls_ord_dt,

    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -- ==========================================
    -- 2. FINANCIAL DATA CLEANSING
    -- ==========================================
    -- Business Rule: Total Sales must equal Quantity * Price.
    -- Recalculate if Sales is NULL, negative, or mathematically incorrect.
    CASE 
	    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		    THEN sls_quantity * ABS(sls_price)
	    ELSE sls_sales
    END AS sls_sales,

    [sls_quantity],

    -- Business Rule: Price must be a positive number.
    -- Recalculate using Sales / Quantity if missing or invalid. NULLIF prevents division by zero.
    CASE
	    WHEN sls_price IS NULL OR sls_price <= 0 
		    THEN sls_sales / NULLIF(sls_quantity, 0)
	    ELSE sls_price
    END AS sls_price

FROM [DataWarehouse].[bronze].[crm_sales_details];