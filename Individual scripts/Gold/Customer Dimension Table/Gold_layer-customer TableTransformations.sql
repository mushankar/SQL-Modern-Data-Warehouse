
-- GOLD LAYER CUSTOMER TABLE TRANSFORMATIONS

-- We have solved integration isssues through transformations for gender column
-- Reordered columns as per the logical understanding of columns or logical sequence
-- Renamed columns for better understanding
-- Created Surrogate Key to each record of table - customer_key
-- Finally We've created our first object, a view named as gold.dim_customers

-- Now we Have dimension table for customers as per our data model
---------------------
-- CRM Master table joins with 2 compatible tables
---------------------

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER( ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE
	WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master file
	ELSE COALESCE(ca.gen,'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid
