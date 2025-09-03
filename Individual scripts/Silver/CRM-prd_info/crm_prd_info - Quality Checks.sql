-- QUALITY CHECK
-- All below results should fetch either no values or clean values without duplicates


-- check for duplicates in primary key

SELECT * FROM silver.crm_prd_info

SELECT 
prd_id, count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) >1 OR prd_id IS NULL

-- Checking if there are any values from cat_id which do not match id values from px_cat_g1v2 file.

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- We have this cat_id in erp_px_cat_g1v2
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM silver.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN
(SELECT DISTINCT id FROM silver.erp_px_cat_g1v2)

-- Check if there are any values that match the prd_key in sales table

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- We have this cat_id in erp_px_cat_g1v2
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM silver.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN
(SELECT sls_prd_key FROM silver.crm_sales_details WHERE sls_prd_key LIKE 'FR%')

-- Check unwanted spaces in string values

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check data standardization and consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Nulls of negative numbers

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- Check for Invalid Date Orders

SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt
