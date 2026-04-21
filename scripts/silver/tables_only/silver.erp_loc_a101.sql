/*
===============================================================================
Data Transformation & Load: Bronze to Silver (ERP Location Data)
===============================================================================
Purpose: 
    - Cleanse and standardize location data (Country names).
    - Standardize Customer IDs to match the format of the CRM system.
    - Safely load the refined data into the Silver layer.
===============================================================================
*/

-- ==========================================
-- 1. DATA QUALITY (DQ) PROFILING
-- ==========================================
-- Note: The query below is a Referential Integrity check. 
-- It identifies "orphaned" location records in the ERP system that do not have 
-- a matching Customer Key in the central CRM table.
/* SELECT * FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cst_key FROM silver.crm_cust_info
); 
*/

-- ==========================================
-- 2. DATA TRANSFORMATION & LOAD (ETL)
-- ==========================================

-- Enforce idempotency: Clear existing Silver data before loading to prevent duplicate records.
TRUNCATE TABLE silver.erp_loc_a101;

-- Insert cleansed and standardized records into the target Silver table.
INSERT INTO silver.erp_loc_a101 (
    cid, 
    cntry
)
SELECT 
    -- ------------------------------------------
    -- Transformation Rule 1: Customer ID Standardization
    -- ------------------------------------------
    -- Business Rule: The ERP system stores IDs with hyphens (e.g., 'CUST-123'), 
    -- but the CRM system does not ('CUST123'). Remove hyphens to allow for 
    -- seamless JOINs between tables later.
    REPLACE(cid, '-', '') AS cid,
    
    -- ------------------------------------------
    -- Transformation Rule 2: Country Name Standardization
    -- ------------------------------------------
    -- Business Rule: Map various country abbreviations into standardized full names 
    -- for cleaner reporting. Handle missing or empty values by assigning 'n/a'.
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry) -- Keep all other countries as-is, just strip any spaces
    END AS cntry

FROM bronze.erp_loc_a101;

-- ==========================================
-- 3. AUDIT & VERIFICATION
-- ==========================================


-- Check the unique list of standardized countries to ensure no messy abbreviations slipped through.

/*
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
*/

-- Final visual check of the fully loaded Silver table.
-- SELECT * FROM silver.erp_loc_a101;