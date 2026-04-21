/*
===============================================================================
Data Transformation & Load: Bronze to Silver (Customer Info)
===============================================================================
Purpose: 
    - Perform Data Quality (DQ) checks on raw Bronze data.
    - Clean, standardize, and deduplicate customer records.
    - Load the transformed data into the Silver layer for downstream analytics.
===============================================================================
*/

-- ============================================================================
-- 1. DATA QUALITY (DQ) PROFILING (Exploration Phase)
-- ============================================================================
/*
    [PASTE YOUR PROFILING QUERIES HERE - Lines 17 through 32]
    -- Check for NULLs or Duplicates in the Primary Key column...
    -- Check for unwanted leading/trailing spaces...
    -- Identify distinct values for Data Standardization...
*/

-- ============================================================================
-- 2. DATA TRANSFORMATION & LOAD (ETL Phase)
-- ============================================================================

-- Enforce idempotency: Clear existing Silver data before loading
TRUNCATE TABLE silver.crm_cust_info;

-- Insert cleaned, standardized, and deduplicated records into the Silver layer.
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    
    -- Clean text fields
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname,   
    
    -- Standardize Marital Status abbreviations into readable dimensions
    CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
         ELSE 'n/a' -- Handle missing or unexpected values gracefully
    END AS cst_material_status,
    
    -- Standardize Gender abbreviations into readable dimensions
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         ELSE 'n/a'
    END AS cst_gndr,
    
    cst_create_date

FROM (
    -- Subquery: Deduplicate records by selecting the most recent entry per customer ID
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL -- Filter out invalid primary keys early in the pipeline
) t 
WHERE flag_last = 1; -- Keep only the latest record for each customer

-- ============================================================================
-- 3. POST-LOAD VALIDATION CHECKS
-- ============================================================================
/*
    -- Check for NULLs or Duplicates in Primary key column...
    -- Check for unwanted spaces...
    -- Data Standardization & Consistency...

    SELECT 
         cst_id,
         COUNT(*)
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL;

        -- Check for unwanted spaces
        -- Expectation: No Result.

        SELECT cst_gndr
        FROM silver.crm_cust_info
        WHERE cst_gndr != TRIM(cst_gndr);

        -- Data Standardization & Consistency

        SELECT DISTINCT cst_gndr
        FROM silver.crm_cust_info;

        SELECT * FROM silver.crm_cust_info;
*/