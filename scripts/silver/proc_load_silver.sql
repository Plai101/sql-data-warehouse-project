/*
=====================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.

    The process includes:
    - Truncating existing tables in the `silver` schema to ensure clean loads
    - Inserts transformed and cleansed data from Bronze into Silver tables
=====================================================================
*/

# =============================================================
# Load CRM Tables into Silver
# =============================================================

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
	 ELSE 'n/a'
END cst_gndr,
STR_TO_DATE(TRIM(REPLACE(REPLACE(REPLACE(cst_create_date, '\r', ''), '\n', ''), '\t', '')), '%Y-%m-%d')
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
	FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL AND cst_id != ''
)t WHERE flag_last = 1;
 
 
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
prd_nm,
IFNULL(NULLIF(TRIM(prd_cost), ''), 0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS prd_end_dt
FROM bronze.crm_prd_info;


TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
WITH cleaned_data AS (
  SELECT 
    sls_ord_num,
    sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price,
    CASE 
      WHEN sls_price IS NULL OR sls_price <= 0 
      THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_price
    END AS fixed_price
  FROM bronze.crm_sales_details
  WHERE 
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
)
 
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
       ELSE CAST(sls_order_dt AS DATE)
  END AS sls_order_dt,
  CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
       ELSE CAST(sls_ship_dt AS DATE)
  END AS sls_ship_dt,
  CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
       ELSE CAST(sls_due_dt AS DATE)
  END AS sls_due_dt,
 CASE 
    WHEN sls_sales IS NULL 
      OR sls_sales <= 0 
      OR sls_sales != sls_quantity * ABS(fixed_price)
    THEN sls_quantity * ABS(fixed_price)
    ELSE sls_sales
  END AS sls_sales,
  sls_quantity,
  fixed_price AS sls_price
FROM cleaned_data;

# =============================================================
# Load ERP Tables into Silver
# =============================================================

TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
	 ELSE bdate
END AS bdate,
CASE 
  WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, '\r', ''), '\n', ''), '\t', ''))) IN ('F', 'FEMALE') THEN 'Female'
  WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, '\r', ''), '\n', ''), '\t', ''))) IN ('M', 'MALE') THEN 'Male'
  ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;


TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE 
    WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
    WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) = 'DE' THEN 'Germany'
    WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
  END AS cntry
FROM bronze.erp_loc_a101;


TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT 
id,
cat,
subcat,
TRIM(REPLACE(maintenance, CHAR(13), '')) AS maintenance
FROM bronze.erp_px_cat_g1v2;
