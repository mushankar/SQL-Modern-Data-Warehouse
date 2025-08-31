/*
STORED PROCEDURE TO LOAD: SILVER LAYER (All data in bronze layer to silver with transformations)

Purpose:
1. This stored procedure loads data from bronze layer tables to silver layer by transforming each table 
2. Procedure:
    1. Truncate table to remove existing data.
    2. Load data into table using 'INSERT INTO' Function.
3. Above procedure is followed so we do not have duplication values.

Usage:
EXECUTE silver.load_bronze

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batchstart_time DATETIME, @batchend_time DATETIME;
	BEGIN TRY
		SET @batchstart_time = GETDATE();

		PRINT'================================================'
		PRINT'LOADING SILVER LAYER'
		PRINT'================================================'

		PRINT'------------------------------------------------'
		PRINT'LOADING CRM TABLES'
		PRINT'------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'INSERTING DATA INTO: silver.crm_cust_info'

		INSERT INTO 
			silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END as cst_marital_status, 
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr,
		cst_create_date

		FROM (
		SELECT * , ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info 
		WHERE cst_id IS NOT NULL
		) as t
		WHERE flag_last = 1

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'
	

		---------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'INSERTING DATA INTO: silver.crm_prd_info'

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
		
		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'

		-----------------------------------------------------------------------------
		SET @start_time = GETDATE();
		
		PRINT 'TRUNCATING TABLE: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'INSERTING DATA INTO: silver.crm_sales_details'

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

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'


		----------------------------------------------------------------------------------
		PRINT'------------------------------------------------'
		PRINT'LOADING ERP TABLES'
		PRINT'------------------------------------------------'
		
		
		SET @start_time = GETDATE()
		
		PRINT 'TRUNCATING TABLE: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'INSERTING DATA INTO: silver.erp_cust_az12'

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

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'

		----------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		
		PRINT 'TRUNCATING TABLE: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'INSERTING DATA INTO: silver.erp_loc_a101'

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

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'

		-------------------------------------------------------------------------------------
		SET @start_time = GETDATE();

		PRINT 'TRUNCATING TABLE: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'INSERTING DATA INTO: silver.erp_px_cat_g1v2'

		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)

		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------------'

		SET @batchend_time = GETDATE();
		PRINT'================================================'
		PRINT'LOADING SILVER LAYER IS COMPLETED'
		PRINT' TOTAL LOAD TIME: ' + CAST(DATEDIFF(SECOND, @batchstart_time, @batchend_time) AS NVARCHAR) + 'Seconds'
		PRINT'================================================'


	
	END TRY
	BEGIN CATCH
	PRINT'==========================================='
	PRINT'Error Message ' + ERROR_MESSAGE();
	PRINT'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT'==========================================='

	END CATCH
END
