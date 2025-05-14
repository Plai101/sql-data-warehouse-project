/*
=====================================================================
Script Purpose:
    This script is used to **load raw source data into the Bronze Layer**
    of the Data Warehouse. It covers both CRM and ERP data sources.

    The process includes:
    - Truncating existing tables in the `bronze` schema to ensure clean loads
    - Loading new CSV data using `LOAD DATA INFILE` for each table
=====================================================================
*/

# =============================================================
# Load CRM Tables into Bronze
# =============================================================

TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

# =============================================================
# Load ERP Tables into Bronze
# =============================================================

TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE 'C:/Users/admin/Desktop/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
