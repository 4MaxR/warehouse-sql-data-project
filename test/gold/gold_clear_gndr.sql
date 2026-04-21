/*
===============================================================================
Data Quality Check: Gender Master Data Management (MDM) Validation
===============================================================================
Purpose: 
    - This query acts as a unit test for the dimension transformation logic.
    - It extracts every unique combination of gender data from both systems.
    - It verifies that the CASE WHEN logic successfully creates the unified 
      'new_gndr' column according to the strict business hierarchy.
===============================================================================
*/

SELECT DISTINCT
    -- 1. Raw System Data (The "Before" State)
    ci.cst_gndr AS crm_gender,  -- The primary source (CRM)
    ca.gen AS erp_gender,       -- The secondary fallback source (ERP)

    -- 2. Transformed Data (The "After" State)
    -- Business Rule: CRM is the Master. 
    -- If CRM has a valid value, use it. 
    -- If CRM is missing ('n/a'), fall back to the ERP value using COALESCE.
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE COALESCE(ca.gen, 'n/a') 
    END AS new_gndr

FROM silver.crm_cust_info ci

-- 3. Join Logic to bridge the CRM and ERP systems
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- (Optional for this specific test, but kept for structural consistency)
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid

-- 4. Sort the output to easily spot edge cases and NULLs
ORDER BY 1, 2;
