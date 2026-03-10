/*
====================================================================================
Stored Procedure - Load Silver Layer
====================================================================================
Script Purpose:
	This script performs the ETL (Extract, Transform, Load) process to populate data
	from bronze tables.
Actions:
	Truncates silver tables
	Inserts transformed and clansed data from bronze into silver tables
Parameters:
	None. This stored procedure does not accept any parameter or return any values
Usage Example:
	EXEC silver.load_silver
====================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_siler AS
BEGIN
	BEGIN TRY
		PRINT '======================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '======================================================================';

		DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.category';
		TRUNCATE TABLE silver.category;

		PRINT 'Inserting data into silver.category';
		INSERT INTO silver.category (
			category_id,
			category_name
		)
		SELECT
			categori_id,
			category_name
		FROM bronze.category
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.cities';
		TRUNCATE TABLE silver.cities

		PRINT 'Inserting data into silver.cities';
		INSERT INTO silver.cities (
			city_id,
			city_name,
			zip_code,
			country_id
		)
		SELECT
			city_id,
			city_name,
			zip_code,
			country_id
		FROM bronze.cities
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.countries';
		TRUNCATE TABLE silver.countries

		PRINT 'Inserting data into silver.countries';
		INSERT INTO silver.countries (
			country_id,
			country_name,
			country_code
		)
		SELECT
			*
		FROM bronze.countries
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.customers';
		TRUNCATE TABLE silver.customers

		PRINT 'Inserting data into silver.customers';
		INSERT INTO silver.customers (
			customer_id,
			full_name,
			city_id,
			address
		)
		SELECT
			customer_id,
			CASE WHEN middle_initial != 'NULL' THEN first_name + ' ' + middle_initial + '.' + ' ' + last_name
				 ELSE first_name + ' ' + last_name
			END AS full_name, -- Merge first_name, middle_innitial and last_name
			city_id,
			address
		FROM bronze.customers
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.employees';
		TRUNCATE TABLE silver.employees;

		PRINT 'Inserting data into silver.employees';
		INSERT INTO silver.employees (
			employee_id,
			full_name,
			birth_date,
			gender,
			city_id,
			hire_date
		)
		SELECT
			employee_id,
			first_name + ' ' + middle_initial + '.' + ' ' + last_name AS full_name, -- Merge first_name, middle_innitial and last_name
			TRY_CAST(birth_date AS DATE) AS birth_date,
			CASE WHEN gender = 'F' THEN 'Female'
				 WHEN gender = 'M' THEN 'Male'
				 ELSE 'n/a'
			END AS gender, -- Replace gender values with more friendly names
			city_id,
			TRY_CAST(hire_date AS DATE) AS hire_date
		FROM bronze.employees
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.products';
		TRUNCATE TABLE silver.products;

		PRINT 'Inserting data into silver.products';
		INSERT INTO silver.products (
			product_id,
			product_name,
			price,
			category_id,
			class,
			modify_date,
			resistant,
			is_allergic,
			vitality_days
		)
		SELECT
			product_id,
			/*
			Matching of misplaced values with their respective fields
			eg. part of product_name placed in price and category
			It seems that during extraction of the table some information was pushed forward to the
			next field.
			Relevant commands have been used to find the values that have been misplaced and
			brought to their correct fields.
			CAUTION:
				vitality days may be inaccurate for products whose preceeding information
				had been misplaced since it is the last field.
				However it does not contain very important information
				and therefore it can as well be ignored during analysis
			*/
			CASE WHEN product_name LIKE '%"%' AND price LIKE '%"%'
					THEN REPLACE(product_name + ' ' + price, '"', '')
				 WHEN product_name LIKE '%"%' AND category_id LIKE '%"%'
					THEN REPLACE(product_name + ' ' + price + ' ' + category_id, '"', '')
				 ELSE product_name
			END AS product_name,
			CASE WHEN price NOT LIKE '%[0-9.]%' OR price LIKE '%"%'
			THEN ROUND((CASE WHEN category_id LIKE '%[0-9.]%'
						THEN category_id
						ELSE class
					END), 0)
				 ELSE ROUND(price, 0)
			END AS price,
			CASE WHEN category_id NOT LIKE '%[0-9]%' 
					OR  category_id LIKE '%.%'
				 THEN (CASE WHEN class LIKE '%.%'
							THEN modify_date
							ELSE class
					   END)	 
				 ELSE category_id
			END AS category_id,
			CASE WHEN class LIKE '%[0-9]%' AND modify_date NOT LIKE '%[0-9]%' THEN modify_date
				 WHEN class LIKE '%[0-9]%' AND resistant = 'Low'
										OR resistant = 'Medium'
							OR resistant = 'High'
				 THEN resistant
				 ELSE class
			END AS class,
			CASE WHEN modify_date LIKE '%[^0-9 -:.]%' AND resistant LIKE '%[0-9 -:.]%' THEN resistant
				 WHEN modify_date LIKE '%[0-9]%' AND is_allergic LIKE '%[0-9 -:.]%' THEN is_allergic
				 ELSE TRY_CAST(modify_date AS DATE)
			END AS modify_date,
			CASE WHEN TRIM(resistant) NOT LIKE '%Durable%'
					AND TRIM(resistant) NOT LIKE '%Weak%'
					AND TRIM(resistant) NOT LIKE '%Unknown%'
				 THEN (CASE WHEN TRIM(is_allergic) = 'Durable'
								OR TRIM(is_allergic) = 'Weak'
								OR TRIM(is_allergic) = 'Unknown'
							THEN TRIM(is_allergic)
							ELSE (CASE WHEN vitality_days LIKE '%Dur%' THEN 'Durable'
									   WHEN vitality_days LIKE '%Weak%' THEN 'Weak'
									   WHEN vitality_days LIKE '%Unknown%' THEN 'Unknown'
								  END)
						END)
				 ELSE resistant
			END AS resistant,
			CASE WHEN TRIM(is_allergic) != 'True'
					OR TRIM(is_allergic) != 'False'
					OR TRIM(is_allergic) != 'Unknown'
				 THEN (CASE WHEN vitality_days LIKE '%True%' THEN 'True'
							WHEN vitality_days LIKE '%False%' THEN 'False'
							WHEN vitality_days LIKE '%Unknown%' THEN 'Unknown'
							ELSE is_allergic
						END)
				 ELSE is_allergic
			END AS is_allergic,
			CASE WHEN vitality_days LIKE '%[A-Z]%' THEN ROUND(TRY_CAST(SUBSTRING(vitality_days,
															PATINDEX('%[0-9]%', vitality_days),
															LEN(vitality_days)) AS FLOAT), 0)
				 ELSE vitality_days
			END AS vitality_days
		FROM bronze.products
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table silver.sales';
		TRUNCATE TABLE silver.sales;

		PRINT 'Inserting data into silver.sales';
		INSERT INTO silver.sales (
			sales_id,
			sales_person,
			customer_id,
			product_id,
			quantity,
			discount,
			total_price,
			sales_date,
			transaction_number
		)
		SELECT
			sales_id,
			sales_person,
			customer_id,
			product_id,
			quantity,
			discount,
			total_price,
			TRY_CAST(sales_date AS DATE) AS sales_date,
			transactio_number
		FROM bronze.sales
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>---------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT 'Total load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
	END TRY

	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH
END