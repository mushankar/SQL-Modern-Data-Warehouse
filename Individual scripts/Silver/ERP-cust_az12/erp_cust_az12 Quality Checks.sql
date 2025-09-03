-- DATA QUALITY CHECKS for cust_az12


-- SELECT*FROM bronze.erp_cust_az12

SELECT
CID, BDATE, GEN
FROM silver.erp_cust_az12

-- checking duplicates
SELECT DISTINCT
GEN
FROM silver.erp_cust_az12

-- Silver cust info has something similar to CID in our erp_cust table in bronze.
SELECT*FROM silver.crm_cust_info

SELECT
CASE
	WHEN CID LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(CID))
	ELSE CID
END AS CID
FROM silver.erp_cust_az12
WHERE CASE
	WHEN CID LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(CID))
	ELSE CID
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info) -- CHECK to see if our CID match crm table

-- CHECKING INVALID BDATES
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

-- Checking final quality of Gender column.
SELECT DISTINCT
CASE
	WHEN GEN = 'M' THEN 'Male'
	WHEN GEN = 'F' THEN 'Female'
	WHEN GEN IS NULL OR GEN = '' THEN 'n/a'
	ELSE GEN
END as GEN
FROM bronze.erp_cust_az12