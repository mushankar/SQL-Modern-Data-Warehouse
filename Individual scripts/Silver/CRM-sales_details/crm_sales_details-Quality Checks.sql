-- QUALITY CHECKS
-- All below results should fetch either no values or clean values without duplicates.
-- In order to identify redundancy please replace .silver to .silver.
-- Following checks were first performed for bronze tables then replaced with .silver to recheck at the end.



--SELECT * FROM silver.crm_sales_details

-- Check for unwanted spaces
SELECT
sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Check prd_key information not available in silver.crm_prd_info as theres a connection
-- Use the same code for sls_cust_id 
SELECT
sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- Check invalid dates and Outliers (Note: dates in bronze table are in number format)

-- order_dt checks

SELECT 
sls_order_dt -- Converts mentioned values to NULL
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101 -- Checks outlier dates

-- ship_dt checks

SELECT 
NULLIF(sls_ship_dt,0) AS sls_ship_dt -- Converts mentioned values to NULL
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101 -- Checks outlier dates

-- due_dr checks

SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt -- Converts mentioned values to NULL
FROM bronze.crm_sales_details
WHERE  sls_due_dt <= 0 OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 OR sls_due_dt < 19000101 -- Checks outlier dates

-- mutual date checks

SELECT
sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- BUSINESS RULES
-- SUM(SALES) = QUANTITY*PRICE
-- Negative, Zeros, NULLS are Not Allowed

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
