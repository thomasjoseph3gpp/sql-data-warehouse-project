/*


----------------------------------------------------------------------------------------------------

silver Layer Table Creation


General information

This script has three parts

Part 1: It creates all silver layer tables
Part 2: It inserts all the data from the bronze layer into the corresponding tables with necessary changes
		Changes include Data Cleaning, Data Standardization, Data normalization, Derived Columns and Data Enrichments
Part 3: Verification and quality checks of the loaded data

NOTE: There is a separate stored procedure created for BULK INSERTION OF DATA INTO THE TABLES AND THAT IS GIVEN AS silver.load_silver CAN BE FOUND INSIDE PROGRAMMABILITY.

-----------------------------------------------------------------------------------------------------

Part 1

This script is used to create the necessary tables for the silver layer.
It checks wherther the table exists before creation, if exists, it drops the table and recreate it.


Warning

This script drops the existing table before creation, 
so proper backup has to be taken if needed before execution.

-----------------------------------------------------------------------------------------------------

*/

PRINT '-------------------------------------'
PRINT 'SILVER LAYER TABLE CREATION BEGINS...'
PRINT '-------------------------------------'

USE DataWarehouse;

IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

GO
CREATE TABLE silver.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_creation_date DATETIME2 DEFAULT  GETDATE()
)

PRINT 'TABLE silver.crm_cust_info CREATED'

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info

GO
CREATE TABLE silver.crm_prd_info (
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt VARCHAR(50),
prd_end_dt VARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
)

PRINT'TABLE silver.crm_prd_info CREATED'

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details

GO
CREATE TABLE silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id VARCHAR(50),
sls_order_dt VARCHAR(50),
sls_ship_dt VARCHAR(50),
sls_due_dt VARCHAR(50),
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_creation_date DATETIME2 DEFAULT GETDATE()
)
PRINT 'TABLE silver.crm_sales_details CREATED'

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12

GO
CREATE TABLE silver.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
)

PRINT 'TABLE silver.erp_cust_az12 CREATED'

IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101

GO
CREATE TABLE silver.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
)

PRINT'TABLE silver.erp_loc_a101 CREATED'

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2

GO
CREATE TABLE silver.erp_px_cat_g1v2 (
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
)

PRINT'TABLE silver.erp_px_cat_g1v2 CREATED'

