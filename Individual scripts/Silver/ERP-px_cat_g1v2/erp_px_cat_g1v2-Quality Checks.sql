SELECT*FROM bronze.erp_px_cat_g1v2

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- checking unwanted spaces
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance) OR cat != TRIM(cat) OR subcat != TRIM(subcat)

-- Checking if data is standard
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

-- All values look great

SELECT cat_id FROM silver.crm_prd_info; -- We have generated this column during transformations

SELECT id FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)
-- There is only 1 value which is not in silver.crm_prd_info which should not be a problem
-- All other columns are perfect