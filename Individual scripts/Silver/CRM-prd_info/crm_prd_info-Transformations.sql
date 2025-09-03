/*
We have successfully completed the following operations in our data
1. Removed unwanted spaces
2. Data Normalization & Standardization - for user friendly descriptions
3. Handled missing values
4. Filtered out unnecessary data or removed duplicates from primary key
5. Transformed irrelevant dates to meaningful dates.

We have # derived columns as a solution in our silver module for silver.crm_prd_info
*/

INSERT INTO silver.crm_prd_info (
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
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- We have this cat_id in erp_px_cat_g1v2
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extracted prd_key as per erp_px_cat_g1v2
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
/*CASE
	WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'*/
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line, -- Map product line codes to descriptive values
CAST(prd_start_dt AS DATE) AS prd_start_dt,
DATEADD(DAY, -1, CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)) AS prd_end_dt
--  Calculate end day as one day before the next start date (# Data Enrichment)
FROM bronze.crm_prd_info




