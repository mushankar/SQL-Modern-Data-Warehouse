-- QUALITY CHECK
-- All below results should fetch either no values or clean values without duplicates


-- check for duplicates in primary key

SELECT * FROM silver.crm_cust_info

SELECT 
cst_id,count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) >1

-- Check unwanted spaces in string values

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- check data standardization and consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

