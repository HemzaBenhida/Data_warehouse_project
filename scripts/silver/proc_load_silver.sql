USE DataWarehouse;
GO
/*
================================================================

Stored Procedure: Load Silver Layer (Bronze -> Silver)

================================================================

Script Purpose : 
    This stored procedure performs the ETL (Extract, Transform, Load) process 
    to populate the 'silver' shema tables from the 'bronze' shema.

Actions Performed :
    - Truncate Silver tables.
    - Inserts transformed and cleansed data from Bronze into silver tables.

Parameters :
    None.
    This stored procedire does not acceptt any parameters or return any values.

Usage Examples :
    EXEC silver.load_silver
*/
-- Create stored procedure

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
    SET @batch_start_time = GETDATE();
    print '================================';
    print 'Load silver Layer';
    print '================================';

    print '================================';
    print 'Load CRM Tables';
    print '================================';

    -- Loading silver.crm_cust_info
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting  Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_maeriel_status,
        cst_gndr,
        cst_create_date
        )
    select cst_id,
    cst_key,
    TRIM (cst_firstname) AS cst_firstname,
    TRIM (cst_lastname) AS cst_lastname,

    CASE WHEN UPPER(TRIM(cst_maeriel_status))= 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_maeriel_status))= 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_maeriel_status,

    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date
    FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
    FROM bronze.crm_cust_info where cst_id IS NOT NULL)tmp
    where flag_last = 1;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

    -- Loading silver.crm_prd_info 
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info(
     prd_id,
     cat_id,
     prd_key,
     prd_nm,
     prd_cost,
     prd_line,
     prd_star_dt,
     prd_end_dt
)
SELECT prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Oher Sales'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
CAST (prd_star_dt AS DATE) AS prd_star_dt,
CAST (LEAD(prd_star_dt) OVER (PARTITION BY prd_key ORDER BY prd_star_dt )-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

-- Loading silver.crm_sales_details
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting  Data Into: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt)!= 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS varchar)AS DATE )

END AS sls_order_dt,

CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt)!= 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS varchar)AS DATE )

END AS sls_ship_dt,

CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt)!= 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS varchar)AS DATE )

END AS sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0  OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales

END AS sls_sales,
sls_quantity,

CASE WHEN sls_price <= 0 OR sls_price IS NULL
THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

-- Loading silver.erp_cust_az12
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting  Data Into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12(
     cid,
     bdate,
     gen
)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
     ELSE cid
END AS cid,

CASE WHEN bdate > GETDATE() THEN NULL 
     ELSE bdate
END AS bdate,

CASE
     WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '')) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '')) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

--Loading silver.erp_loc_a101
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting  Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101(
    cid,
    cntry
)
SELECT REPLACE(cid,'-','') AS cid,

CASE 
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) IN ('DE','Germany') THEN 'Germany'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) IN ('US','USA','United States') THEN 'United States'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = '' OR cntry IS NULL THEN 'n/a'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('United Kingdom') THEN 'United Kingdom'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('France') THEN 'France'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('Australia') THEN 'Australia'
    WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')) = ('Canada') THEN 'Canada'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

-- Loading silver.erp_px_cat_g1v2
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting  Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
id,
cat,
subcat,
CASE 
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) = ('No') THEN 'No'
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) = ('Yes') THEN 'Yes'
    ELSE TRIM(maintenance)
END AS maintenance
FROM bronze.erp_px_cat_g1v2;

SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF (SECOND, @start_time, @end_time)AS NVARCHAR)+ 'seconds';

SET @batch_end_time = GETDATE();
PRINT('Loading Silver Layer is completed ')
PRINT '>> Toal Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';
    END TRY 
    BEGIN CATCH

    PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
    PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE'+ CAST (ERROR_NUMBER()AS NVARCHAR);
    PRINT 'ERROR MESSAGE'+ CAST (ERROR_STATE()AS NVARCHAR);
    END CATCH

END 

