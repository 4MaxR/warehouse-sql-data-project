/*
===============================================================================
Data Transformation & Load: Bronze to Silver (ERP Customer Data)
===============================================================================
Purpose: 
    - Profile and identify data anomalies in the raw ERP customer data.
    - Cleanse Customer IDs by removing legacy system prefixes.
    - Validate birthdates to ensure logical consistency.
    - Standardize categorical demographic fields (Gender).
    - Safely load the refined data into the Silver layer.
===============================================================================
*/

-- ==========================================
-- 1. DATA QUALITY (DQ) PROFILING
-- ==========================================
-- Note: These queries are used for initial data exploration and are 
-- kept as comments for future auditing and debugging purposes.

/* -- Identify Out-of-Range Dates:
-- Expectation: No birthdates should be in the future, or unrealistically old.
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standardization & Consistency Check:
-- Expectation: Identify all unique variations of gender inputs to build the mapping logic.
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12; 
*/

-- ==========================================
-- 2. DATA TRANSFORMATION & LOAD (ETL)
-- ==========================================

-- Enforce idempotency: Clear existing Silver data before loading to prevent duplicate records.
TRUNCATE TABLE silver.erp_cust_az12;

-- Insert cleansed and standardized records into the target Silver table.
INSERT INTO silver.erp_cust_az12 (
    cid, 
    bdate, 
    gen
)
SELECT
    -- ------------------------------------------
    -- Transformation Rule 1: Customer ID Cleansing
    -- ------------------------------------------
    -- Business Rule: Remove the legacy 'NAS' prefix from Customer IDs.
    -- If the ID starts with 'NAS', extract everything from the 4th character onward.
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    -- ------------------------------------------
    -- Transformation Rule 2: Date Validation
    -- ------------------------------------------
    -- Business Rule: A birthdate cannot physically be in the future. 
    -- If bdate is greater than today's date (GETDATE()), flag it as NULL (invalid).
    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    -- ------------------------------------------
    -- Transformation Rule 3: Demographic Standardization
    -- ------------------------------------------
    -- Business Rule: Map various messy gender inputs ('F', 'FEMALE', 'M', 'MALE') 
    -- into clean, standardized 'Female' or 'Male' dimensions. 
    -- Unrecognized or missing values default to 'n/a'.
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;

-- ==========================================
-- 3. VERIFICATION
-- ==========================================
-- Quick check to ensure the data was loaded and transformed successfully.

-- SELECT * FROM silver.erp_cust_az12;