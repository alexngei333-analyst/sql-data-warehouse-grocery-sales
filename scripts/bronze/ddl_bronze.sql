/*
===============================================================================
DDL Script: Create Bronze tables
===============================================================================
Script Purpose:
	This scropt creates in the bronze layer dropping existing ones with the same
	naming in the database
	Run this script to redefine the DDL structure of the bronze schema
*/

IF OBJECT_ID('bronze.sales', 'U') IS NOT NULL
DROP TABLE bronze.sales
GO

CREATE TABLE bronze.sales (
	sales_id			NVARCHAR(50),
	sales_person		NVARCHAR(50),
	customer_id			NVARCHAR(50),
	product_id			NVARCHAR(50),
	quantity			NVARCHAR(50),
	discount			NVARCHAR(50),
	total_price			NVARCHAR(50),
	sales_date			NVARCHAR(50),
	transactio_number	NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.category', 'U') IS NOT NULL
DROP TABLE bronze.category
GO

CREATE TABLE bronze.category (
	categori_id			NVARCHAR(50),
	category_name		NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.cities', 'U') IS NOT NULL
DROP TABLE bronze.cities
GO

CREATE TABLE bronze.cities (
	city_id			NVARCHAR(50),
	city_name		NVARCHAR(50),
	zip_code			NVARCHAR(50),
	country_id			NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.countries', 'U') IS NOT NULL
DROP TABLE bronze.countries
GO

CREATE TABLE bronze.countries (
	country_id			NVARCHAR(50),
	country_name		NVARCHAR(50),
	country_code		NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL
DROP TABLE bronze.customers
GO

CREATE TABLE bronze.customers (
	customer_id			NVARCHAR(50),
	first_name			NVARCHAR(50),
	middle_initial		NVARCHAR(50),
	last_name			NVARCHAR(50),
	city_id				NVARCHAR(50),
	address				NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.employees', 'U') IS NOT NULL
DROP TABLE bronze.employees
GO

CREATE TABLE bronze.employees (
	employee_id			NVARCHAR(50),
	first_name			NVARCHAR(50),
	middle_initial		NVARCHAR(50),
	last_name			NVARCHAR(50),
	birth_date			NVARCHAR(50),
	gender				NVARCHAR(50),
	city_id				NVARCHAR(50),
	hire_date			NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
DROP TABLE bronze.products
GO

CREATE TABLE bronze.products (
	product_id			NVARCHAR(50),
	product_name		NVARCHAR(50),
	price				NVARCHAR(50),
	category_id			NVARCHAR(50),
	class				NVARCHAR(50),
	modify_date			NVARCHAR(50),
	resistant			NVARCHAR(50),
	is_allergic			NVARCHAR(50),
	vitality_days		NVARCHAR(50)
);
GO
