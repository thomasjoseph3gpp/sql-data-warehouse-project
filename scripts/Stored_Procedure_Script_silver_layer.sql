-- Data Cleaning

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	

	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME
		

		SET @start_time = GETDATE()

		PRINT'-------------------------------'
		PRINT'>>>>>>DATA CLEANING BEGINS<<<<<'
		PRINT'-------------------------------'

		PRINT'----------bronze.crm_cust_info vleaning begins----------'

		--Cleaning bronze.crm_cust_info

		SELECT 
		TOP 1 *
		FROM bronze.crm_cust_info;-- to select the top 1 row in torder to visualize the table structure.

		SELECT *
		FROM bronze.crm_cust_info
		WHERE cst_id IS NULL;-- to check for any cst_is is null

		SELECT 
		cst_id,
		COUNT(*) AS cst_id_count
		FROM bronze.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(cst_id) >1 OR cst_id IS NULL;-- to check for any duplicate cst_id or NULL cst_id

		SELECT *
		FROM bronze.crm_cust_info
		WHERE cst_id  = 29466;--to check what is the issue with a duplicated sample cst_id = 29466

		SELECT *,
		ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Row_Count
		FROM bronze.crm_cust_info
		WHERE cst_id  = 29466;--to check what is the issue with a duplicated sample cst_id = 29466

		SELECT 
		*,
		ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_count
		FROM bronze.crm_cust_info
		WHERE cst_id = 29483;--to check what is the issue with a duplicated sample cst_id = 29483


		SELECT
		cst_key
		FROM bronze.crm_cust_info
		WHERE TRIM(cst_key) = cst_key;-- to check whether any whitespace present in cst_key column


		SELECT
		cst_firstname
		FROM bronze.crm_cust_info
		WHERE cst_firstname != TRIM(cst_firstname);-- to check whether any whitespace present in cst_firstname

		SELECT
		*
		FROM bronze.crm_cust_info
		WHERE cst_firstname IS NULL;-- to check whether any cst_firstname is NULL

		SELECT 
		cst_lastname
		FROM bronze.crm_cust_info
		WHERE cst_lastname != TRIM(cst_lastname);-- to check whether any whitespace present in cst_lastname

		SELECT
		*
		FROM bronze.crm_cust_info
		WHERE cst_lastname IS NULL;-- to check whether any cst_lastname is NULL


		SELECT
		*
		FROM bronze.crm_cust_info
		WHERE cst_marital_status != TRIM(cst_marital_status);-- to check whether any whitespace present in cst_marital_status


		SELECT
		*
		FROM bronze.crm_cust_info
		WHERE cst_gndr != TRIM(cst_gndr);-- to check whether any whitespace present in cst_gndr

		SELECT DISTINCT cst_marital_status
		FROM bronze.crm_cust_info;-- to check the unique values present in cst_marital_status


		SELECT DISTINCT cst_gndr
		FROM bronze.crm_cust_info;-- to check the unique values present in cst_gndr

		/*

		Checks Included for bronze.crm_cust_info

			Unwanted white space in each column
			Duplicates in primary key column
			NULL Check in each column
			Data Standardization
			Data Normalization

		--Methods Used

			DISTINCT
			IS NULL
			TRIM
			UPPER
			CASE WHEN THEN
			ROW_NUMBER()

			*/

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.crm_cust_info BEGINS<<<<<<<<<<'

		TRUNCATE TABLE silver.crm_cust_info

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		INSERT INTO silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date

		FROM
		(
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE
				WHEN cst_gndr = 'M' THEN 'Male'
				WHEN cst_gndr = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr	,
			cst_create_date,
			ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date) AS row_num

		FROM
		bronze.crm_cust_info)t

		WHERE t.cst_id IS NOT NULL AND t.row_num = 1;

		PRINT'>>>>>>>>>>DATA INSERTION INTO silver.crm_cust_info COMPLETED<<<<<<<<<<'
		PRINT'>>>>>>>>>>DATA CHECK POST INSERTION BEGINS<<<<<<<<<<'

		--Data Checks after inserting data into silver.crm_cust_info


		SELECT 
		TOP 1 *
		FROM silver.crm_cust_info;-- to check the silver.crm_cust_info table structure

		SELECT *
		FROM
		(
		SELECT
		*,
		ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date) AS row_num

		FROM silver.crm_cust_info)T
		WHERE row_num >1;-- To check for any duplicate cst_ids

		SELECT 
		*
		FROM silver.crm_cust_info
		WHERE cst_id IS NULL;-- to check for any NULL cst_ids


		SELECT
		*
		FROM silver.crm_cust_info
		WHERE cst_firstname != TRIM(cst_firstname);-- to check for any whitespaces in cst_firstname


		SELECT 
		*
		FROM silver.crm_cust_info
		WHERE cst_lastname != TRIM(cst_lastname)-- to check for any whitespaces in cst_lastname

		SELECT
		DISTINCT cst_marital_status
		FROM silver.crm_cust_info;-- to check the distinct values cst_marital_status

		SELECT
		DISTINCT cst_gndr
		FROM silver.crm_cust_info;-- to check the distinct values cst_gndr

		SELECT
		*
		FROM silver.crm_cust_info
		WHERE cst_create_date IS NULL;-- to check for any NULL in cst_create_date


		-- Cleaning crm.prd_info before inserting into silver.crm_prd_info



		PRINT'----------bronze.crm_prd_info cleaning begins----------'

		SELECT
		TOP 1*
		FROM bronze.crm_prd_info;-- to visualize the bronze.crm_prd_info table structure

		SELECT
		*
		FROM
		(
		SELECT
		*,
		ROW_NUMBER() OVER( PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS row_count
		FROM bronze.crm_prd_info)t
		WHERE row_count >1 OR prd_id IS NULL;-- to check for any NULL values or duplicate values in prd_id

		SELECT 
		*
		FROM bronze.crm_prd_info
		WHERE prd_key IS NULL;-- to check for NULL in prd_key

		SELECT
		*
		FROM bronze.crm_prd_info
		WHERE prd_nm IS NULL;-- to check for any duplicates in prd_nm

		SELECT *
		FROM bronze.crm_prd_info
		WHERE prd_line IS NULL;-- to check for any duplicates in prd_line

		SELECT 
		*
		FROM bronze.crm_prd_info
		WHERE prd_start_dt IS NULL;-- to check for any NULLs in prd_start_dt 


		SELECT 
		*
		FROM bronze.crm_prd_info
		WHERE prd_end_dt IS NULL;-- to check for any NULLS in prd_end_dt


		SELECT
		*
		FROM bronze.crm_prd_info
		WHERE prd_key != TRIM(prd_key);-- to check for any whitespace in prd_key

		SELECT
		*
		FROM bronze.crm_prd_info
		WHERE prd_nm != TRIM( prd_nm);-- to check for any whitespace in prd_nm


		SELECT
		*
		FROM
		bronze.crm_prd_info
		WHERE prd_line != TRIM(prd_line);-- to check for any whitespace in prd_line

		SELECT
		*
		FROM bronze.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;-- to check for any prd_cost is negative or NULL

		SELECT 
		*,
		ROW_NUMBER() OVER( PARTITION BY prd_key ORDER BY prd_start_dt) AS row_num
		FROM bronze.crm_prd_info
		WHERE prd_start_dt > prd_end_dt-- to check whether any columns have prd_start_dt > prd_end_dt


		/*

		Checks Included for bronze.crm_prd_info

			Unwanted white space in each column
			Duplicates in primary key column
			NULL Check in each column
			Data Standardization
			Data Normalization
			Data Integrity	Check

		Methods Used

			DISTINCT
			IS NULL
			TRIM
			UPPER
			CASE WHEN THEN
			ROW_NUMBER()
			REPLACE
			SUBSTRING
			ISNULL

			*/
		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.crm_prd_info BEGINS<<<<<<<<<<'

		TRUNCATE TABLE silver.crm_prd_info--TRUNCATING TABLE silver.crm_prd_info

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		-- INSERTING DATA INTO silver.crm_prd_info

		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

		)

		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CASE 
				WHEN prd_end_dt < prd_start_dt THEN CAST(LEAD(DATEADD(DAY,-1,prd_start_dt)) OVER ( PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)
				ELSE CAST(prd_end_dt AS DATE)
			END AS prd_end_dt
		FROM bronze.crm_prd_info--inserting data into silver.crm_prd_info

		PRINT'>>>>>>>>>>DATA INSERTION INTO silver.crm_prd_info COMPLETED<<<<<<<<<<'
		PRINT'>>>>>>>>>>DATA CHECK POST INSERTION BEGINS<<<<<<<<<<'


		--DATA VERIFICATION AFTER CLEAN DATA INSERTION INTO silver.crm_prd_info

		SELECT 
		TOP 1 *
		FROM silver.crm_prd_info;-- to check the table structure of silver.crm_prd_info

		SELECT
		*
		FROM
		(

		SELECT 
		*,
		ROW_NUMBER() OVER( PARTITION BY prd_key ORDER BY prd_start_dt) AS row_num

		FROM silver.crm_prd_info)T
		WHERE prd_start_dt > prd_end_dt-- to check whether any columns have prd_start_dt > prd_end_dt


		SELECT 
		*
		FROM silver.crm_prd_info

		WHERE prd_id IS NULL
		OR
		cat_id IS NULL
		OR
		prd_key IS NULL
		OR
		prd_nm IS NULL
		OR
		prd_cost IS NULL
		OR
		prd_line IS NULL
		OR
		prd_start_dt IS NULL
		OR
		prd_end_dt IS NULL;-- to check for any NULL VALUES in the silver.crm_prd_info

		SELECT
		DISTINCT cat_id
		FROM silver.crm_prd_info;-- to check for unique values in cat_id post insertion

		SELECT 
		DISTINCT
		prd_nm FROM silver.crm_prd_info;-- to check for unique values in prd_nm post insertion

		SELECT
		DISTINCT 
		prd_key
		FROM
		silver.crm_prd_info;-- to check for unique values in prd_key post insertion

	

		SELECT
		*
		FROM silver.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;-- to check for any NULLs or negative values in prd_cost

		PRINT'DATA VERIFICATION COMPLETED FOR silver.crm_prd_info'




		--Cleaning bronze.crm_sales_details before INSERSION


		-- Cleaning bronze.crm.crm_sales_details before inserting into silver.crm_sales_details



		PRINT'----------bronze.crm.crm_sales_details cleaning begins----------'

		SELECT
		TOP 1 *
		FROM bronze.crm_sales_details;-- to visualize the table bronze.crm.crm_sales_details

		SELECT
		COUNT(DISTINCT sls_ord_num) AS Actual_count,
		COUNT(*) AS Total_count
		FROM bronze.crm_sales_details;-- to check the row count vs the unique count


		SELECT
		*
		FROM bronze.crm_sales_details
		WHERE NULL IN ( sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)-- to chek for NULL in any of the data fields


		SELECT
		*
		FROM bronze.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price;-- to correct the sales data according to price and quantity

		SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_ord_num != TRIM(sls_ord_num)-- to check for any whitespaces in sls_ord_num
		OR
		sls_prd_key != TRIM(sls_prd_key)-- to check for any whitespaces in sls_prd_key
		OR
		sls_sales < 0 -- to check for negative values in sls_sales
		OR
		sls_quantity < 0 -- to check for negative values in sls_quantity
		OR
		sls_price < 0;-- to check for negative values in sls_price

		SELECT
		*
		FROM
		(

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN LEN(sls_order_dt) != 8 OR TRY_CAST(sls_order_dt AS INT) <=0 THEN NULL -- to check and ensure the date column alligned with the given structure and cast to date format
				ELSE TRY_CAST(sls_order_dt AS DATE) -- to check the date column alligned with the given structure and cast to date format
			END AS sls_order_dt,
			CASE
				WHEN LEN(sls_ship_dt) != 8 OR TRY_CAST(sls_ship_dt AS INT) <=0 THEN NULL -- to check amd emsure the date column alligned with the given structure and cast to date format
				ELSE TRY_CAST(sls_ship_dt AS DATE) -- to check the date column alligned with the given structure and cast to date format
			END AS sls_ship_dt,
			CASE
				WHEN LEN(sls_due_dt) != 8 OR TRY_CAST(sls_due_dt AS INT) <=0 THEN NULL -- to check  and ensure the date column alligned with the given structure and cast to date format
				ELSE TRY_CAST(sls_due_dt AS DATE) -- to check the date column alligned with the given structure and cast to date format
			END AS sls_due_dt,
			CASE
				WHEN sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) -- to check and ensure whether the sls_sales follows the design
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price != ABS(sls_price) THEN ABS(sls_price)-- to check and allign the sls_price as per the design
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details)T
		WHERE
		sls_price <0;

			/*

		Checks Included for bronze.crm_sales_details

			Unwanted white space in each column
			Duplicates in primary key column
			NULL Check in each column
			Data Standardization
			Data Normalization
			Data Integrity	Check

		Methods Used

			DISTINCT
			IS NULL
			TRIM
			UPPER
			CASE WHEN THEN
			ROW_NUMBER()
			REPLACE
			SUBSTRING
			ISNULL
			LEN
			TRY_CAST
			ABS

			*/

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.crm_sales_details BEGINS<<<<<<<<<<'



		TRUNCATE TABLE silver.crm_sales_details;-- truncating the table silver.crm_sales_details before insertion

		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN LEN(sls_order_dt) != 8 OR TRY_CAST(sls_order_dt AS INT) <=0 THEN NULL
				ELSE TRY_CAST(sls_order_dt AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN LEN(sls_ship_dt) != 8 OR TRY_CAST(sls_ship_dt AS INT) <=0 THEN NULL
				ELSE TRY_CAST(sls_ship_dt AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN LEN(sls_due_dt) != 8 OR TRY_CAST(sls_due_dt AS INT) <=0 THEN NULL
				ELSE TRY_CAST(sls_due_dt AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price != ABS(sls_price) THEN ABS(sls_price)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details-- inserting the data into the table silver.crm_sales_details

		PRINT'>>>>>>>>>>DATA INSERTION INTO silver.crm_sales_details COMPLETED<<<<<<<<<<'


		--DATA VERIFICATION AFTER CLEAN DATA INSERTION INTO silver.crm_prd_info

		PRINT'>>>>>>>>>>DATA CHECK POST INSERTION BEGINS<<<<<<<<<<'

		--VERIFICATION AFTER DATA INSERTION INTO silver.crm_sales_details

		SELECT 
		TOP 1 *
		FROM silver.crm_sales_details -- to visualize the table silver.crm_sales_details post insertion

		SELECT
		*
		FROM bronze.crm_sales_details
		WHERE LEN(sls_order_dt) !=8
		OR LEN(sls_ship_dt) != 8
		OR LEN(sls_due_dt) != 8 -- to check the allignment of all date columns as per the standard

		SELECT
		*
		FROM silver.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt; -- to check the data integrity of the date columns

		SELECT
		*
		FROM silver.crm_sales_details
		WHERE sls_sales <0 OR sls_price <0 OR sls_quantity <0;-- to check the data integrity of the sls_sales, sls_price, sls_quantity columns

		SELECT COUNT(*) AS row_count -- to check the total row count from silver.crm_sales_details

		FROM silver.crm_sales_details;


		PRINT'>>>>>DATA CLEANING FROM erp SOURCE SYSTEM BEGINS<<<<<'

		--SOURCE SYSTEM erp DATA cleaning and loading into the SILVER LAYER

		--Cleaning data from bronze.erp_cust_az12


		PRINT'-----DATA CLEANING FROM bronze.erp_cust_az12 BEGINS-----'

		SELECT
		TOP 1 *
		FROM bronze.erp_cust_az12; -- to visualize the table bronze.erp_cust_az12

		SELECT
		*
		FROM bronze.erp_cust_az12
		WHERE bdate > GETDATE(); -- to check the integrity of bdate


		SELECT 
		COUNT(cid) AS total_count,
		COUNT( DISTINCT cid) AS unique_count
		FROM bronze.erp_cust_az12; -- to check the row count vs the unique count

		SELECT 
		DISTINCT gen
		FROM bronze.erp_cust_az12; -- to check the unique gen details

				/*

		Checks Included for bronze.erp_cust_az12

			Unwanted white space in each column
			Duplicates in primary key column
			NULL Check in each column
			Data Standardization
			Data Normalization
			Data Integrity	Check

		Methods Used

			DISTINCT
			IS NULL
			TRIM
			UPPER
			CASE WHEN THEN
			ROW_NUMBER()
			REPLACE
			SUBSTRING
			ISNULL
			LEN
			TRY_CAST
			ABS
			COUNT

			*/

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.erp_cust_az12 BEGINS<<<<<<<<<<'

		--INSERTING CLEANED DATA INTO silver.erp_cust_az12

		TRUNCATE TABLE silver.erp_cust_az12 -- truncating the table silver.erp_cust_az12

		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)

		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid) )
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN TRIM(UPPER(gen)) = 'M' THEN 'Male'
				WHEN TRIM(UPPER(gen)) = 'F' THEN 'Female'
				WHEN LEN(TRIM(gen)) = 0 OR gen IS NULL THEN 'N/A' 
				ELSE gen
			END AS gen
		FROM bronze.erp_cust_az12 -- inserting the data into silver.erp_cust_az12

		PRINT'>>>>>DATA VERIFICATION POST INSERTING INTO silver.erp_cust_az12 BEGINS<<<<<<'

		--Data verification post insertion


		SELECT
		TOP 1*
		FROM silver.erp_cust_az12 -- to visualize the table post insertion

		SELECT
		*
		FROM silver.erp_cust_az12
		WHERE bdate IS NULL; -- to ckeck for any NULLS in bdate


		SELECT
		DISTINCT gen
		FROM silver.erp_cust_az12; -- to check the unique categories in gen


		PRINT'-----DATA CLEANING FROM bronze.erp_loc_a101 BEGINS-----'

		SELECT
		*
		FROM bronze.erp_loc_a101; -- to visualize the table structure

		SELECT COUNT(*)
		FROM bronze.erp_loc_a101; -- to check the total row count

		SELECT
		DISTINCT cntry
		FROM bronze.erp_loc_a101; -- to check the distinct country names 


		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.erp_loc_a101 BEGINS<<<<<<<<<<'


		-- Data Inserting into silver.erp_loc_a101

		--TRUNCATING TABLE silver.erp_loc_a101

		TRUNCATE TABLE silver.erp_loc_a101;

		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)

		SELECT
		REPLACE(cid,'-','') AS cid,
		CASE
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
		WHEN LEN(TRIM(cntry)) = 0 OR cntry IS NULL THEN 'N/A'
		ELSE cntry
		END AS cntry
		FROM bronze.erp_loc_a101 ;-- inserting data into silver.erp_loc_a101

		--Data Cheing after insrting into silver.erp_loc_a101

		PRINT'>>>>>DATA CHECK POST INSERTION BEGINS IN silver.erp_loc_a101'

		SELECT
		*
		FROM silver.crm_cust_info;-- to check the table structure of silver.crm_cust_info


		SELECT
		*
		FROM silver.erp_loc_a101
		WHERE cid IS NULL OR cntry IS NULL;-- to check for NULL if any


		SELECT cst_key
		FROM silver.crm_cust_info
		WHERE cst_key IN
		(SELECT cid FROM silver.erp_loc_a101)-- to check the cst_key and cid presence in the related tables

		PRINT'>>>>>>DATA CHECK COMPLETED FOR silver.crm_cust_info'

		PRINT'-----DATA CLEANING FROM bronze.erp_px_cat_g1v2 BEGINS-----'

		--DATA CLEANING FROM bronze.erp_px_cat_g1v2

		SELECT
		*
		FROM bronze.erp_px_cat_g1v2;-- TO VISUALIZE THE TABLE silver.crm_cust_info

		SELECT
		DISTINCT id
		FROM bronze.erp_px_cat_g1v2;-- to check the unique ids from silver.crm_cust_info

		SELECT
		DISTINCT cat
		FROM bronze.erp_px_cat_g1v2; -- to check unique category from silver.crm_cust_info

		SELECT
		DISTINCT subcat
		FROM bronze.erp_px_cat_g1v2; -- to check unique subcategory from silver.crm_cust_info

		SELECT
		DISTINCT maintenance
		FROM bronze.erp_px_cat_g1v2; -- to check unique maintenance from silver.crm_cust_info

		SELECT
		DISTINCT id
		FROM bronze.erp_px_cat_g1v2
		WHERE id != TRIM(id); -- to check for whitespaces in id

		SELECT
		DISTINCT cat
		FROM bronze.erp_px_cat_g1v2
		WHERE cat != TRIM(cat)-- to check for whitespaces in cat

		SELECT
		DISTINCT subcat
		FROM bronze.erp_px_cat_g1v2
		WHERE subcat != TRIM(subcat)-- to check for whitespaces in subcat

		SELECT
		DISTINCT maintenance
		FROM bronze.erp_px_cat_g1v2
		WHERE maintenance != TRIM(maintenance);-- to check for whitespaces in maintenance


		SELECT
		TOP 1 *
		FROM bronze.erp_px_cat_g1v2
		WHERE id IS NULL
		OR cat IS NULL
		OR subcat IS NULL
		OR maintenance IS NULL;-- to check for NULL values in the whole table 

		--INSERTING DATA INTO silver.erp_px_cat_g1v2

		PRINT'>>>>>>>>>>DATA INSERTION BEGINS<<<<<<<<<<'

		PRINT'>>>>>>>>>>TRUNCATION OF TABLE silver.erp_px_cat_g1v2 BEGINS<<<<<<<<<<'


		TRUNCATE TABLE silver.erp_px_cat_g1v2 -- truncating table silver.erp_px_cat_g1v2

		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		*
		FROM bronze.erp_px_cat_g1v2;-- inserting data into silver.erp_px_cat_g1v2

		--Data Verification After Inserting the Data into silver.erp_px_cat_g1v2

		PRINT'DATA VERIFICATION POST INSERTION BEGINS'

		SELECT 
		*
		FROM silver.erp_px_cat_g1v2;--to check the table structure


		SELECT
		*
		FROM silver.erp_px_cat_g1v2
		WHERE id IS NULL
		OR cat IS NULL
		OR subcat IS NULL
		OR maintenance IS NULL;-- to check for NULL values in silver.erp_px_cat_g1v2

		PRINT'>>>>>DATA CHECK COMPLETED FOR silver.erp_px_cat_g1v2<<<<<'
		SET @end_time = GETDATE()
		PRINT'IT TOOK'+ CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) +'seconds to finish silver layer execution'

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






