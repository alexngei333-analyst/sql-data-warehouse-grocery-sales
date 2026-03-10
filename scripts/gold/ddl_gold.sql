/*
================================================================================
DDL Script: Create Gold Views
================================================================================
Script Purpose:
	This script create views for gold layer in the data warehouse.
	The gold layer consists of final dimensions and fact tables (Star Schema).

	Each view performs transformations and combines data from silver tables to
	produce clean, enriched and business-ready dataset.
*/

-- ==============================================================================;
-- Create Dimension: gold.dim_products;
-- ==============================================================================;

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key, -- Surrogate key
	p.product_id,
	p.product_name,
	c.category_id,
	c.category_name,
	p.price,
	p.class,
	p.modify_date,
	p.resistant,
	p.is_allergic,
	p.vitality_days
FROM silver.products p
LEFT JOIN silver.category c
ON		  p.category_id = c.category_id
GO

-- ==============================================================================;
-- Create Dimension: gold.dim_customers;
-- ==============================================================================;

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key, -- Surrogate key
	customer_id,
	full_name,
	ci.city_id,
	ci.city_name,
	ci.zip_code,
	ci.country_id
FROM silver.customers cu
LEFT JOIN silver.cities ci
ON		  cu.city_id = ci.city_id;
GO

-- ==============================================================================;
-- Create Dimension: gold.dim_employees;
-- ==============================================================================;

IF OBJECT_ID('gold.dim_employees', 'V')IS NOT NULL
	DROP VIEW gold.dim_employees;
GO

CREATE VIEW gold.dim_employees AS
SELECT
	ROW_NUMBER() OVER (ORDER BY hire_date) AS employee_key, -- Surrogate key
	em.employee_id,
	em.full_name,
	em.birth_date,
	em.gender,
	em.hire_date,
	ci.city_id,
	ci.city_name,
	ci.zip_code,
	ci.country_id
FROM silver.employees em
LEFT JOIN silver.cities ci
ON		  em.city_id = ci.city_id;
GO

-- ==============================================================================;
-- Create Fact Table: gold.fact_sales;
-- ==============================================================================;

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sa.sales_id,
	em.employee_key,
	cu.customer_key,
	pr.product_key,
	sa.quantity,
	sa.discount,
	sa.total_price,
	sa.sales_date,
	sa.transaction_number
FROM silver.sales sa
LEFT JOIN gold.dim_customers cu
ON		  sa.customer_id = cu.customer_id
LEFT JOIN gold.dim_products pr
ON		  sa.product_id = pr.product_id
LEFT JOIN gold.dim_employees em
ON		  sa.sales_person = em.employee_id;
GO