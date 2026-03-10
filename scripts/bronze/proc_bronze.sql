/*
========================================================================================================
Stored procedure -> Load Bronze (Source Bronze)
========================================================================================================
Script Purpose:
	This script loads data into bronze schema from external csv files
	It performs the following actions:
		- Truncates the tables before inserting the data
		- Uses BULK INSERT command to insert the data from external csv files
Parameters:
			None. This stored procedure does not accept any parameters or return any values.
Usage Example:
			EXEC bronze.load_bronze
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '==========================================================================================';
		PRINT 'Loading Bronze Layer'
		PRINT '==========================================================================================';

		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.sales';
		TRUNCATE TABLE bronze.sales;

		PRINT 'Inserting data into bronze.sales';
		BULK INSERT bronze.sales
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\sales.csv'
		WITH (
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.category';
		TRUNCATE TABLE bronze.category;

		PRINT 'Inserting data into bronze.category';
		BULK INSERT bronze.category
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\categories.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.countries';
		TRUNCATE TABLE bronze.countries;

		PRINT 'Inserting data into bronze.countries';
		BULK INSERT bronze.countries
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\countries.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.cities';
		TRUNCATE TABLE bronze.cities;

		PRINT 'Inserting data into bronze.cities';
		BULK INSERT bronze.cities
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\cities.csv'
		WITH (
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.customers';
		TRUNCATE TABLE bronze.customers;

		PRINT 'Inserting data into bronze.customers';
		BULK INSERT bronze.customers
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\customers.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.employees';
		TRUNCATE TABLE bronze.employees;

		PRINT 'Inserting data into bronze.employees';
		BULK INSERT bronze.employees
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\employees.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.products';
		TRUNCATE TABLE bronze.products;

		PRINT 'Inserting data into bronze.products';
		BULK INSERT bronze.products
		FROM 'C:\Users\Dell\Documents\self_practice\datasets\grocery_sales\products.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT 'Total load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
	END TRY

	BEGIN CATCH
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH
END