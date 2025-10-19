/*
==================================================================

Quality Cheks 

==================================================================

Script Purpose :
    This script perfoms various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during the checks. 

==================================================================
*/

USE DataWarehouse;
 GO 
/*
 ==================================================================

Checking 'silver.crm_cust_info'

 ==================================================================
*/
   
 SELECT * FROM bronze.crm_cust_info;

-- Check if there is some duplicates or Null
SELECT cst_id, count (*) from bronze.crm_cust_info
GROUP BY cst_id having COUNT(*) > 1 OR cst_id is NULL;


-- try to find duplicates and keep the newest Row 
SELECT * FROM bronze.crm_cust_info WHERE cst_id = 29466;

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM bronze.crm_cust_info WHERE cst_id = 29466;

-- Select all duplicates and those having old status 
select * FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM bronze.crm_cust_info)tmp
where flag_last != 1;


select * FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)tmp
where flag_last = 1;

-- check unwanted spaces
-- Expecation : No result 

SELECT cst_firstname FROM bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

SELECT cst_key FROM bronze.crm_cust_info
where cst_key != TRIM(cst_key);
 
-- Data Standarizaion & Consistency
-- Gender
SELECT DISTINCT cst_gndr from bronze.crm_cust_info;

-- Marital Status

SELECT DISTINCT cst_maeriel_status FROM bronze.crm_cust_info;

-- TRUNCATE TABLE silver.crm_cust_info;

------------------------------------------------------------------------------------------------------
-- check the quality of silver

--    1-if there is any duplicates or NULL.

SELECT cst_id, count (*) from silver.crm_cust_info
GROUP BY cst_id having COUNT(*) > 1 OR cst_id is NULL;

-- 2- if there is any spaces
SELECT cst_firstname FROM silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

SELECT cst_key FROM silver.crm_cust_info
where cst_key != TRIM(cst_key);

SELECT * FROM silver.crm_cust_info;

/*
 ==================================================================

Checking 'silver.crm_prd_info'

 ==================================================================

*/


USE DataWarehouse;

-- Check for Nulls or Duplicates in Primary Key
-- Exopectation : No Result 

-- select prd_id, COUNT(*) from bronze.crm_prd_info GROUP BY prd_id
-- Having COUNT(*) > 1 OR prd_id is NULL;

select prd_id, COUNT(*) from silver.crm_prd_info GROUP BY prd_id
Having COUNT(*) > 1 OR prd_id is NULL;

-- Data Standardization & Consistency

--SELECT distinct prd_line 
--FROM bronze.crm_prd_info;

SELECT distinct prd_line 
FROM silver.crm_prd_info;

-- Check For Invalid Date Orders
--SELECT * FROM bronze.crm_prd_info
--WHERE prd_end_dt < prd_star_dt;

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_star_dt;

SELECT prd_id,
prd_key,
prd_nm,
prd_star_dt,
prd_end_dt,
LEAD(prd_star_dt) OVER (PARTITION BY prd_key ORDER BY prd_star_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');


/*
 ==================================================================

Checking 'silver.crm_sales_details'

 ==================================================================
*/



-- check for Invalid Dates

-- Order dates
SELECT sls_order_dt From bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

SELECT NULLIF(sls_order_dt,0) sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Shipping Date
SELECT sls_ship_dt From bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8;

-- Due Dates 
SELECT sls_due_dt From bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

-- Check for Invalid Date Orders 

SELECT * from bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--------------------------------------------------------

-- Check Data Consistency: Between Sales, Quantity and Price 
--> Sales = Quantity * Price 
--> Values must not be NULL, Zero or Negative.
-- If there is an issues, we shouldn't go and fix or transform it on my own
-- i usually go and talk to experts.


------------Solution--------------------------
-- we shouldn't go and fix it 
-- Solution 1: Data issues will be fixed direct in Source system.
-- Solution 2 : Live with it, if you can't, Data will be fixed in Data Warehouse.
-- Rules if you decide to fix it:
-- 1- If Sales is Negative, Zero or NULL, derive it using Quanntity and Price.
-- 2- If Price is Zero or NULL, calculate it using Sales and Quantity.
-- 3- If Price is Negative, conver it to a positive Value. 



SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0  OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales

END AS sls_sales,

CASE WHEN sls_price <= 0 OR sls_price IS NULL
THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price 
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

/*
 ==================================================================

Checking 'silver.erp_cust_az12'

 ==================================================================
*/

SELECT * FROM silver.erp_cust_az12;

SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
     ELSE cid
END cid,
bdate,
CASE WHEN bdate > GETDATE() THEN NULL 
     ELSE bdate
END bdate,
gen
FROM silver.erp_cust_az12;

select * FROM silver.crm_cust_info;

-- Identify Out-of-Range Dates

SELECT DISTINCT bdate FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE() 
ORDER by bdate DESC ;

-- Data Standardizaion & Consistency 


SELECT DISTINCT gen FROM silver.erp_cust_az12;

SELECT distinct gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
     ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;

SELECT DISTINCT gen,
    CASE
        WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '')) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '')) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS cleaned_gen
FROM silver.erp_cust_az12;

/*
 ==================================================================

Checking 'silver.erp_loc_a101'

 ==================================================================
*/


SELECT * FROM bronze.erp_loc_a101;

SELECT * FROM silver.crm_cust_info;

SELECT cid,
REPLACE(cid, '-', '') FROM bronze.erp_loc_a101;

--Data Standardization & Consistency

SELECT DISTINCT cntry,
CASE 
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) IN ('DE','Germany') THEN 'Germany'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) IN ('US','USA','United States') THEN 'United States'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = '' OR cntry IS NULL THEN 'n/a'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('United Kingdom') THEN 'United Kingdom'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('France') THEN 'France'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('Australia') THEN 'Australia'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('Canada') THEN 'Canada'
    ELSE TRIM(cntry)
END AS cleaned_cntry
FROM bronze.erp_loc_a101;

/*
 ==================================================================

Checking 'silver.erp_px_cat_g1v2'

 ==================================================================
*/


SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info;



-- Check for Unwanted spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
-- We do that, because we had some (\r and /n) in maintenance column. 

SELECT DISTINCT
CASE 
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) = ('No') THEN 'No'
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) = ('Yes') THEN 'Yes'
    ELSE TRIM(maintenance)
END AS maintenance
from bronze.erp_px_cat_g1v2;



