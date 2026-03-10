/*
=========================================================================================
Create 'GrocerySales' database and schemas
=========================================================================================
Script purpose:
	This script creates 'GrocerySales' database after checking if it already exists
	If it exists it is dropped and recreated again.
	In addition it creates three schemas; bronze, silver, gold
WARNING:
	This script deletes 'GrocerySales' database if it exists and all the data
	Proceed with caution and ensure you have proper backups before running this script
=========================================================================================
*/

USE MASTER;
GO

IF EXISTS(SELECT 1 FROM sys.databases WHERE NAME = 'GrocerySales')
BEGIN
	ALTER DATABASE GrocerySales SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE GrocerySales
END;
GO

CREATE DATABASE GrocerySales;
GO

USE GrocerySales;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
