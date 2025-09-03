-- GOLD LAYER PRODUCT TABLE DATA QUALITY CHECKS

-- Checking Duplicates in primary key after joining

SELECT prd_key, COUNT(*) FROM (
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL ) t
GROUP BY prd_key
HAVING COUNT(*) > 1