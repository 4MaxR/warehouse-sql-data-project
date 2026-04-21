/*
===============================================================================
Data Transformation & Load: Bronze to Silver (ERP Product Categories)
===============================================================================
Purpose: 
    - Profile raw product category data to identify formatting anomalies.
    - Cleanse and standardize category, subcategory, and maintenance flags.
    - Safely load the refined category hierarchy into the Silver layer for 
      downstream e-commerce reporting and analytics.
===============================================================================
*/

-- ==========================================
-- 1. DATA QUALITY (DQ) PROFILING
-- ==========================================
-- Note: These queries are used for initial data exploration and auditing.
-- They are retained as comments for documentation and future debugging.

/* -- Check for Unwanted Spaces:
-- Expectation: No results. Identifies if the source system is passing trailing/leading spaces.
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcate != TRIM(subcate) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency Checks:
-- Expectation: Review distinct values to identify spelling variations or messy data entries.
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcate FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;
*/

-- ==========================================
-- 2. DATA TRANSFORMATION & LOAD (ETL)
-- ==========================================

-- Enforce idempotency: Clear existing Silver data before loading to prevent duplicate records.
TRUNCATE TABLE silver.erp_px_cat_g1v2;

-- Insert cleansed records into the target Silver table.
INSERT INTO silver.erp_px_cat_g1v2 (
    id, 
    cat, 
    subcate, 
    maintenance
)
SELECT 
    id,
    -- Apply TRIM() proactively to ensure no whitespace anomalies enter the Silver layer
    TRIM(cat) AS cat,             
    TRIM(subcate) AS subcate,     
    TRIM(maintenance) AS maintenance 
FROM bronze.erp_px_cat_g1v2;

-- ==========================================
-- 3. AUDIT & VERIFICATION
-- ==========================================

-- Final visual check to verify the data was successfully transformed and loaded.
-- SELECT * FROM silver.erp_px_cat_g1v2;