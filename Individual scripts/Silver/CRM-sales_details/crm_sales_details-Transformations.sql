-- TRANFORMATIONS

INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
-- We cannot convert a numeric value directly to date in SQL.
-- We need to convert it to a string or VARCHAR first
CASE
	WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
-- We can apply the same rules as above in case in future we have same type of issues in the data
-- I'm preferring not to use as of now in this file so the code is easily readable.
CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,
CASE
	WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity*ABS(sls_price)
	THEN sls_quantity* ABS(sls_price)
	ELSE sls_sales
END AS sls_sales, -- Based on Business Rules of Sales and Checks
NULLIF(sls_quantity,0) AS sls_quantity,
CASE
	WHEN sls_price IS NULL OR sls_price <= 0 
	THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price -- Based on Business Rules of sales and Checks
FROM bronze.crm_sales_details	

