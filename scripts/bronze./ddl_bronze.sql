/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id 				VARCHAR(50),
    cst_key 			VARCHAR(50),
    cst_firstname 		VARCHAR(50),
    cst_lastname 		VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr 			VARCHAR(50),
    cst_create_date 	VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id     VARCHAR(50),
	prd_key    VARCHAR(50),
	prd_nm     VARCHAR(50),
	prd_cost   VARCHAR(50),
	pdr_line   VARCHAR(50),
	pdr_start  VARCHAR(50),
	pdr_end_dt VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  VARCHAR(50),
    sls_order_dt VARCHAR(50),
    sls_ship_dt  VARCHAR(50),
    sls_due_dt   VARCHAR(50),
    sls_sales    VARCHAR(50),
    sls_quantity VARCHAR(50),
    sls_price    VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid   VARCHAR(50),
    bdate VARCHAR(50),
    gen   VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
    id   		VARCHAR(50),
    cat 		VARCHAR(50),
    subcat 		VARCHAR(50),
    maintenance VARCHAR(50)
);
