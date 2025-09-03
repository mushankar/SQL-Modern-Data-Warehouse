-- GOLD LAYER FACT SALES TABLE QUALITY CHECKS

-- Foreign key integrity checks

SELECT*FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key != f.customer_key OR c.customer_key IS NULL;


SELECT*FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key != f.product_key OR f.product_key IS NULL

