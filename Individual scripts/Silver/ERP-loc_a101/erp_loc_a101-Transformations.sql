-- DATA TRANSFORMATIONS
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)

SELECT 
REPLACE(cid,'-','') AS CID, -- Formatting as per cst_key from silver.crm_cust_info
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry -- Data normalization & handling missing country codes
FROM bronze.erp_loc_a101
