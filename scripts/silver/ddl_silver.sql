/*
===================================================================
DDl script: Create Silver tables
===================================================================
Script Purpose: 
    This script creates tables in silver schema , dropping existing 
    tables ,if they already exist.
    Run this script to re-define the structure of 'bronze' tables
===================================================================
*/

use DataWarehouse;
if object_id ('silver.crm_cust_info','U') is not null
	drop table silver.crm_cust_info;
-- Customer info DDL script
create table silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date Datetime2 default getdate()
)

if object_id ('silver.crm_prd_info','U') is not null
	drop table silver.crm_prd_info;
--prd_info DDL script
create table silver.crm_prd_info(
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date Datetime2 default getdate()
);

if object_id ('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
-- sls_ord_num ddl script
create table silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt Date,
sls_ship_dt Date,
sls_due_dt Date,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date Datetime2 default getdate()
)

if object_id ('silver.erp_cust_az12','U') is not null
	drop table silver.erp_cust_az12;
--cust_az12 ddl script
create table silver.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date Datetime2 default getdate()
)

if object_id ('silver.erp_loc_a101','U') is not null
	drop table silver.erp_loc_a101;
----loc_a101 ddl script
create table silver.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date Datetime2 default getdate()
);

if object_id ('silver.erp_px_cat_g1v2','U') is not null
	drop table silver.erp_px_cat_g1v2;
-- px_cat_g1v2
create table silver.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date Datetime2 default getdate()
)

