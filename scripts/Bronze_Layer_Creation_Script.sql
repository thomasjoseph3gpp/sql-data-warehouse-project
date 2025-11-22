/*


----------------------------------------------------------------------------------------------------

Bronze Layer Table Creation


General information

This script has three parts

Part 1: It creates all bronze layer tables
Part 2: It inserts all the data from the source system into the corresponsinng tables
Part 3: Verification and quality checks of the loaded data

NOTE: There is a separate stored procedure created for BULK INSERTION OF DATA INTO THE TABLES AND THAT IS GIVEN AS bronze.load_bronze CAN BE FOUND INSIDE PROGRAMMABILITY.

-----------------------------------------------------------------------------------------------------

Part 1

This script is used to create the necessary tables for the bronze layer.
It checks wherther the table exists before creation, if exists, it drops the table and recreate it.


Warning

This script drops the existing table before creation, 
so proper backup has to be taken if needed before execution.

-----------------------------------------------------------------------------------------------------

*/

PRINT '-------------------------------------'
PRINT 'BRONZE LAYER TABLE CREATION BEGINS...'
PRINT '-------------------------------------'

USE DataWarehouse;

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

GO
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
)

PRINT 'TABLE bronze.crm_cust_info CREATED'

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info

GO
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt VARCHAR(50),
prd_end_dt VARCHAR(50)
)

PRINT'TABLE bronze.crm_prd_info CREATED'

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details

GO
CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id VARCHAR(50),
sls_order_dt VARCHAR(50),
sls_ship_dt VARCHAR(50),
sls_due_dt VARCHAR(50),
sls_sales INT,
sls_quantity INT,
sls_price INT
)
PRINT 'TABLE bronze.crm_sales_details CREATED'

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12

GO
CREATE TABLE bronze.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
)

PRINT 'TABLE bronze.erp_cust_az12 CREATED'

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101

GO
CREATE TABLE bronze.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50)
)

PRINT'TABLE bronze.erp_loc_a101 CREATED'

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2

GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50)
)

PRINT'TABLE bronze.erp_px_cat_g1v2 CREATED'


/*

-------------------------------------------

Part 2

This part of script is made in order to insert data from the source system into the corresponding tables.
TRUNCATE is performed before loading the data in order to avoid duplicates.
BULK INSERT is performed to insert the data from the source system

Warning

If needed, please take necessary backups as the TRUCATE will clear the table completely

*/

PRINT'---------------------------------------'
PRINT'DATA INSERSION BEGINS......'
PRINT'---------------------------------------'

GO
TRUNCATE TABLE bronze.crm_cust_info

GO
BULK INSERT bronze.crm_cust_info
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH ( 
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)

PRINT'DATA INSERSION COMPLETED FOR bronze.crm_cust_info, PLEASE VERIFY ONCE'

GO
TRUNCATE TABLE bronze.crm_prd_info

GO
BULK INSERT bronze.crm_prd_info
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)

PRINT'DATA INSERSION COMPLETED FOR bronze.crm_prd_info, PLEASE VERIFY ONCE'

GO
TRUNCATE TABLE bronze.crm_sales_details

GO
BULK INSERT bronze.crm_sales_details
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)

PRINT'DATA INSERSION COMPLETED FOR bronze.crm_sales_details, PLEASE VERIFY ONCE'

GO
TRUNCATE TABLE bronze.erp_cust_az12

GO
BULK INSERT bronze.erp_cust_az12
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
PRINT'DATA INSERSION COMPLETED FOR bronze.erp_cust_az12, PLEASE VERIFY ONCE'

GO
TRUNCATE TABLE bronze.erp_loc_a101

GO
BULK INSERT bronze.erp_loc_a101
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
PRINT'DATA INSERSION COMPLETED FOR bronze.erp_loc_a101, PLEASE VERIFY ONCE'
GO
TRUNCATE TABLE bronze.erp_px_cat_g1v2

GO
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
)
PRINT'DATA INSERSION COMPLETED FOR bronze.erp_px_cat_g1v2, PLEASE VERIFY ONCE'


/*

PART 3

Verification of the Loaded Data

Need to check the result of the script manually with the sourde data
Checks included are the Data alignment and Data Count

*/

-- bronze.crm_cust_info verification

PRINT'DATA VERIFICATION BEGINS..'

SELECT
	TOP 1*
FROM bronze.crm_cust_info;

SELECT 
	COUNT(*) AS row_count
FROM bronze.crm_cust_info

--bronze.crm_prd_info verification

SELECT
TOP 1 *
FROM bronze.crm_prd_info;

SELECT 
COUNT(*) AS row_count
FROM bronze.crm_prd_info

--bronze.crm_sales_details verification

SELECT 
TOP 1 *
FROM bronze.crm_sales_details

SELECT 
COUNT(*) AS row_count
FROM bronze.crm_sales_details

--bronze.erp_cust_az12 verification

SELECT 
TOP 1 *
FROM bronze.erp_cust_az12

SELECT 
COUNT(*) AS row_count
FROM bronze.erp_cust_az12;

--bronze.erp_loc_a101 verification

SELECT 
TOP 1 *
FROM bronze.erp_loc_a101

SELECT
COUNT(*) AS row_count
FROM bronze.erp_loc_a101

--bronze.erp_loc_a101 verification

SELECT 
TOP 1 *
FROM  bronze.erp_loc_a101

SELECT
COUNT(*) AS row_count 
FROM bronze.erp_loc_a101



















