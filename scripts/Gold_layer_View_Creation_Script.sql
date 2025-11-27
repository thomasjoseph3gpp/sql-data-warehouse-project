--GOLD LAYER 



/*

	This is the GOLD LAYER to which we provide access to Data Analysist, Data Engineers, Data Scientists and Business Analysts.
	In order to access this layer, we have created VIEWS against each tavle and are given below.
	There are two dimension tables and one fact tables
	Surrogate keys are created for dimension tables in order to link those with the fact tables.
	This gold layer follows a star schema


	*/

	--Creating VIEW FOR gold.dim_customers
GO
IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers


GO
CREATE VIEW gold.dim_customers AS

SELECT
	ROW_NUMBER() OVER(ORDER BY(ci.cst_id)) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
	WHEN ci.cst_gndr  != 'n/a' OR ci.cst_gndr != 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(ce.gen,'N/A')
	END AS gender,
	ce.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ce
ON ci.cst_key = ce.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid



-- Creating VIEW for gold.dim_products
GO
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products

GO
CREATE VIEW gold.dim_products AS

SELECT
	ROW_NUMBER() OVER( ORDER BY pr.prd_start_dt,pr.prd_id) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	ct.cat AS category,
	ct.subcat AS subcategory,
	ct.maintenance AS maintenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 ct
ON pr.cat_id = ct.id
WHERE pr.prd_end_dt IS NULL


-- Creating VIEW for gold.fact_sales

GO
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales

GO
CREATE VIEW gold.fact_sales AS

SELECT
	sd.sls_ord_num AS order_number,
	p.product_key AS product_key,
	c.customer_key AS customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers c
ON sd.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p
ON sd.sls_prd_key = p.product_number


GO




