/*
STORED PROCEDURE TO LOAD: BRONZE LAYER (All the raw data from tables as they are)

Purpose:
1. This stored procedure loads data from external tables to 'Bronze' Schema. 
2. Procedure:
    1. Truncate table to remove existing data.
    2. Load data into table using 'Bulk Insert'.
3. Above procedure is followed so we do not have duplication of our dataset.

Usage:
EXECUTE bronze.load_bronze

*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batchstart_time DATETIME, @batchend_time DATETIME;
	BEGIN TRY
		SET @batchstart_time = GETDATE();

		SET @start_time = GETDATE();

		PRINT '=======================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=======================================';

		PRINT '---------------------------------------';
		PRINT 'LOAD CRM TABLES';
		PRINT '---------------------------------------';

		-- TRUNCATE OR EMPTY TABLE BEFORE LOADING FOR FULL LOAD, TO AVOID DUPLICATION
		-- CRM_CUST_INFO

		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'INSERTING INTO bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';

		--SELECT * FROM bronze.crm_cust_info
		SELECT COUNT(*) FROM bronze.crm_cust_info

		-- CRM_PRD_INFO
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'INSERTING INTO bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';

		--SELECT*FROM bronze.crm_prd_info
		SELECT COUNT(*)FROM bronze.crm_prd_info

		--CRM_SALES_DETAILS
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'INSERTING INTO bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';

		--SELECT*FROM bronze.crm_sales_details
		SELECT COUNT(*) FROM bronze.crm_sales_details

		PRINT '---------------------------------------';
		PRINT 'LOAD ERP TABLES';
		PRINT '---------------------------------------';

		--ERP_CUST_AZ12

		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'INSERTING INTO bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
				);
		SET @end_time = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		--SELECT*FROM bronze.erp_cust_az12
		SELECT COUNT(*) FROM bronze.erp_cust_az12

		--ERP_LOC_A101
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'INSERTING INTO bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		--SELECT*FROM bronze.erp_loc_a101
		SELECT COUNT(*) FROM bronze.erp_loc_a101

		--ERP_PX_CAT_G1V2
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'INSERTING INTO bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\1992s\OneDrive\Development\SQL modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		--SELECT*FROM bronze.erp_px_cat_g1v2
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2

		SET @batchend_time = GETDATE()
		PRINT '============================================================================================='
		PRINT ' TOTAL LOAD DURATION: ' + CAST(DATEDIFF(second, @batchstart_time, @batchend_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================================================================='
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'Error occurred while loading bronze layer, following are the details of error'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END

