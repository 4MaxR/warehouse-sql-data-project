/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE();

    BEGIN TRY
        PRINT '==================================================';
        PRINT '>>> Executing ETL Pipeline: Bronze -> Silver Layer';
        PRINT '==================================================';

        PRINT '--------------------------------------------------';
        PRINT '[*] LOADING CRM SYSTEM TABLES';
        PRINT '--------------------------------------------------';

        -- =======================================================
        -- 1. LOAD TABLE: silver.crm_cust_info
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.crm_cust_info...';
        
        -- ==========================================
        -- 1. DATA TRANSFORMATION & LOAD (ETL) for silver.crm_cust_info
        -- ==========================================

        -- Enforce idempotency: Clear existing Silver data before loading to prevent duplication across pipeline runs.
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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- =======================================================
        -- 2. LOAD TABLE: silver.crm_prd_info
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.crm_prd_info...';
        
        TRUNCATE TABLE silver.crm_prd_info; -- Clear existing data to avoid duplicates before loading new data
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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- =======================================================
        -- 3. LOAD TABLE: silver.crm_sales_details
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.crm_sales_details...';

        -- Enforce idempotency: Clear existing Silver data before loading to prevent duplicate records.
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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        PRINT '--------------------------------------------------';
        PRINT '[*] LOADING ERP SYSTEM TABLES';
        PRINT '--------------------------------------------------';

        -- =======================================================
        -- 4. LOAD TABLE: silver.erp_cust_az12
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.erp_cust_az12...';
        
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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- =======================================================
        -- 5. LOAD TABLE: silver.erp_loc_a101
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.erp_loc_a101...';
        
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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- =======================================================
        -- 6. LOAD TABLE: silver.erp_px_cat_g1v2
        -- =======================================================
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.erp_px_cat_g1v2...';

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
        
        SET @end_time = GETDATE();
        PRINT '   - Load Successful. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';


        -- =======================================================
        -- COMPLETION LOGGING
        -- =======================================================
        SET @batch_end_time = GETDATE();
        PRINT '==================================================';
        PRINT '[OK] Silver Layer Load Completed Successfully!';
        PRINT '   Total Execution Time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
        PRINT '==================================================';

    END TRY
    BEGIN CATCH
        -- Error handling mechanism to capture pipeline failures
        PRINT '==================================================';
        PRINT '[FAILED] ERROR: ETL Pipeline Failed during Silver Load.';
        PRINT '   Error Message: ' + ERROR_MESSAGE();
        PRINT '   Error State: '   + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==================================================';
    END CATCH
END;
