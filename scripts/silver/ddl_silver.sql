/*
===============================================================================
DDL Script: Create silver tables
===============================================================================
Script Purpose:
	This scropt creates in the silver layer dropping existing ones with the same
	naming in the database
	Run this script to redefine the DDL structure of the silver schema
*/

IF OBJECT_ID('silver.sales', 'U') IS NOT NULL
DROP TABLE silver.sales
GO

CREATE TABLE silver.sales (
	sales_id			INT,
	sales_person		INT,
	customer_id			INT,
	product_id			INT,
	quantity			INT,
	discount			FLOAT,
	total_price			FLOAT,
	sales_date			DATE,
	transaction_number	NVARCHAR(50),
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.category', 'U') IS NOT NULL
DROP TABLE silver.category
GO

CREATE TABLE silver.category (
	category_id			SMALLINT,
	category_name		NVARCHAR(50),
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.cities', 'U') IS NOT NULL
DROP TABLE silver.cities
GO

CREATE TABLE silver.cities (
	city_id			SMALLINT,
	city_name		NVARCHAR(50),
	zip_code		INT,
	country_id		SMALLINT,
	gs_create_date	DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.countries', 'U') IS NOT NULL
DROP TABLE silver.countries
GO

CREATE TABLE silver.countries (
	country_id			INT,
	country_name		NVARCHAR(50),
	country_code		NVARCHAR(50),
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.customers', 'U') IS NOT NULL
DROP TABLE silver.customers
GO

CREATE TABLE silver.customers (
	customer_id			NVARCHAR(50),
	full_name			NVARCHAR(50),
	city_id				NVARCHAR(50),
	address				NVARCHAR(50),
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.employees', 'U') IS NOT NULL
DROP TABLE silver.employees
GO

CREATE TABLE silver.employees (
	employee_id			SMALLINT,
	full_name			NVARCHAR(50),
	birth_date			DATE,
	gender				NVARCHAR(10),
	city_id				SMALLINT,
	hire_date			DATE,
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.products', 'U') IS NOT NULL
DROP TABLE silver.products
GO

CREATE TABLE silver.products (
	product_id			INT,
	product_name		NVARCHAR(50),
	price				FLOAT,
	category_id			INT,
	class				NVARCHAR(50),
	modify_date			DATE,
	resistant			NVARCHAR(50),
	is_allergic			NVARCHAR(50),
	vitality_days		INT,
	gs_create_date		DATETIME2 DEFAULT GETDATE()
);
GO
