-- TRANSFORMATIONS
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)

SELECT
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove NAS prefix if present
	ELSE cid
END AS cid, 
CASE 
	WHEN bdate > GETDATE() THEN NULL -- Set future bdates to null
	ELSE bdate
END AS bdate,
CASE
	WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
	ELSE 'n/a'
END as gen -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12

--SELECT COUNT(*) FROM silver.erp_cust_az12
