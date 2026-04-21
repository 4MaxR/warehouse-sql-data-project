/* SELECT 
*/

/*
===============================================================================
Data Transformation & Load: Bronze to Silver (CRM Product Info)
===============================================================================
Purpose: 
    - Extract raw product data from the Bronze layer.
    - Cleanse and normalize product keys and category IDs.
    - Handle missing costs and map product line codes to descriptive values.
    - Calculate historical product validity dates using Window Functions.
===============================================================================
*/

-- ============================================================================
-- 1. DATA QUALITY PROFILING & DDL (Exploration Phase)
-- ============================================================================
/* 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1 

SELECT DISTINCT id from bronze.erp_px_cat_g1v2;
SELECT * FROM bronze.crm_prd_info;

WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

SELECT *, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS test_end_dt
FROM bronze.crm_prd_info
WHERE prd_key in ( 'AC-HE-HL-U509-R', 'AC-HE-HL-U509');

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(255),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 DEFAULT GETDATE()
);

*/

-- ============================================================================
-- 2. DATA TRANSFORMATION & LOAD (ETL Phase)
-- ============================================================================

-- Enforce idempotency: Clear existing data to avoid duplicates
TRUNCATE TABLE silver.crm_prd_info; 

-- Insert transformed data into the Silver layer
INSERT INTO silver.crm_prd_info (   -- Insert transformed data into silver layer
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, -- Extract category ID
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		 -- Extract product key
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost, -- Replace missing information to 0
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line, -- Map product line codes to descriptive values
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
	AS DATE
) AS prd_end_dt -- Calculate end date as one day before the next start date 
FROM bronze.crm_prd_info


-- ============================================================================
-- 3. POST-LOAD VALIDATION CHECKS
-- ============================================================================
/*
    -- Check for NULLs or Duplicates in Primary key column.
	-- Expectation: No Result.

	SELECT 
	 prd_id,
	 COUNT(*)
	FROM silver.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL;

	-- Check for unwanted spaces
	-- Expectation: No Result.

	SELECT prd_nm
	FROM silver.crm_prd_info
	WHERE prd_nm != TRIM(prd_nm);

	-- Data Standardization & Consistency

	SELECT DISTINCT prd_line
	FROM silver.crm_prd_info;

	SELECT * FROM bronze.crm_prd_info;

	-- Check for NULLS or Negative Numbers depends on data and business
	-- Expectation: No Result.

	SELECT prd_cost
	FROM silver.crm_prd_info
	WHERE prd_cost IS NULL OR prd_cost < 0;

	-- Check for Invalid Dates

	SELECT * 
	FROM silver.crm_prd_info
	WHERE prd_end_dt < prd_start_dt

*/

