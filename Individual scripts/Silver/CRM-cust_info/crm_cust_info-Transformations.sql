-- we have noticed nulls and the previous records or duplicates under cst_id from crm_cust_info table.
-- We removed the previous records by ranking over cst_created-date in descending order.
-- Remove unwanted spaces
-- Insert into the rows


/*
We have successfully completed the following operations in our data
1. Removed unwanted spaces
2. Data Normalization & Standardization - for user friendly descriptions
3. Handled missing values
4. Filtered out unnecessary data or removed duplicates from primary key
*/

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
END as cst_marital_status, -- Removed unneccessary spaces & Normalized the column to readable format
CASE
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'n/a'
END as cst_gndr, -- Removed unneccessary spaces & Normalized the column to readable format
cst_create_date

FROM (
SELECT * , ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL
) as t
WHERE flag_last = 1 -- Selects the most recent record as per cst_create_date
