-- GOLD LAYER PRODUCT TABLE TRANSFORMATIONS

-- We have created a surrogate key
-- We have filtered our all historical data identifying end_dt NULL values as ongoing records
-- Created view for our dimension products table


CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER( ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance As maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS startdate
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data

-- Identifying NULLs in end_date as latest records and filtered out historical data
-- So we have not included the end_dt

