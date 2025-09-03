SELECT 
cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key from silver.crm_cust_info;

-- Checking matching values of cid with csk_key from silver.crm_cust_info table
SELECT
REPLACE(cid,'-','') AS cid -- Replaced '-' with '' to standardize
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key from silver.crm_cust_info)

-- Data standardization & consistency

SELECT cntry FROM bronze.erp_loc_a101
WHERE cntry != TRIM(cntry)

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101