/*

- This is a stored procedure created in order to load bronze layer
- This helps to load the bronze layer all at once from teh data sources

Usage
-Pease run the below command to load bronze layer

EXEC bronze.load_silver;

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	BEGIN TRY
		

		SET @start_time = GETDATE()
		PRINT'---------------------------------------'
		PRINT'DATA INSERSION BEGINS......'
		PRINT'---------------------------------------'


		TRUNCATE TABLE bronze.crm_cust_info


		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)

		PRINT'DATA INSERSION COMPLETED FOR bronze.crm_cust_info, PLEASE VERIFY ONCE'


		TRUNCATE TABLE bronze.crm_prd_info


		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)

		PRINT'DATA INSERSION COMPLETED FOR bronze.crm_prd_info, PLEASE VERIFY ONCE'


		TRUNCATE TABLE bronze.crm_sales_details


		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)

		PRINT'DATA INSERSION COMPLETED FOR bronze.crm_sales_details, PLEASE VERIFY ONCE'


		TRUNCATE TABLE bronze.erp_cust_az12


		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
		PRINT'DATA INSERSION COMPLETED FOR bronze.erp_cust_az12, PLEASE VERIFY ONCE'


		TRUNCATE TABLE bronze.erp_loc_a101


		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL\Baara\Data_WareHouse_project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
		PRINT'DATA INSERSION COMPLETED FOR bronze.erp_loc_a101, PLEASE VERIFY ONCE'

		TRUNCATE TABLE bronze.erp_px_cat_g1v2


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

		PRINT'ALL VERIFICATION COMPLETED'
		SET @end_time = GETDATE()
		PRINT'>>>>>>>>>>>>>>><><<<<<<<<<<<<<<<<<'
		PRINT'Overall time to load bronze layer:'+ CAST((DATEDIFF(second,@start_time,@end_time)) AS VARCHAR(50)) +'seconds';
		PRINT'>>>>>>>>>>>>>>><><<<<<<<<<<<<<<<<<'
	END TRY
	
	BEGIN CATCH
		PRINT'========================================='
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error Message:'+ERROR_MESSAGE();
		PRINT'Error Message:'+CAST(ERROR_NUMBER() AS VARCHAR(50));
		PRINT'Error Mesaage:'+CAST(ERROR_STATE() AS VARCHAR(50));
		PRINT'========================================='
	END CATCH


END
