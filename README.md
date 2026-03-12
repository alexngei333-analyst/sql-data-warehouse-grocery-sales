# Grocery Sales SQL Data Warehouse

## Project Overview
This project demonstrates how to design and build a modern SQL data warehouse using the Medallion Architecture (Bronze, Silver, Gold).

The goal is to transform raw grocery sales data into a clean analytical model for business insights.

## Dataset Attribution

**Dataset:** Grocery Sales Database

**Creator:** -

**Source:** https://www.kaggle.com/datasets/andrexibiza/grocery-sales-dataset?select=sales.csv

**Licence:** CCO: Public Domain

## Architecture
Bronze → Silver → Gold data pipeline

## Tools
- SQL Server Data Tools (SSDT)
- SQL Server Management Studio(SSMS)
- Power BI

## Technologies
- SQL
- Data Warehousing
- Star Schema Modeling
- ETL Pipeline Design

## Data Pipeline
1. Raw data loaded into Bronze schema
2. Data cleaned and standardized in Silver schema
3. Business-ready analytical model created in Gold schema

## Data Model
Star schema with fact_sales and dimension tables.

## Analytics
Example business questions:
- Sales by category
- Customer purchase behavior
- Employee sales performance

