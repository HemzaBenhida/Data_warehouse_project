  
USE DataWarehouse;
GO

-- Create stored procedure


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY

print '================================';
print 'Load Bronze Layer';
print '================================';

SET @batch_start_time = GETDATE();


print 'Load CRM Tables';
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from '/tmp/datasets/source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';

-- INSERT CSV File into Table bronze.crm_prd_info

SET @start_time = GETDATE();
TRUNCATE TABLE brinze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
from '/tmp/datasets/source_crm/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';

-- INSERT CSV File into Table bronze.crm_sales_details
SET @start_time = GETDATE();

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM '/tmp/datasets/source_crm/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';



print 'Load ERP Tables';
-- INSERTT CSV File into Table bronze.erp_cust_az12
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12 
FROM '/tmp/datasets/source_erp/CUST_AZ12.csv'
WITH (
     FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';


-- INSERT CSV File into Table bronze.erp_loc_a101
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_loc_az101;
BULK INSERT bronze.erp_loc_a101 
FROM '/tmp/datasets/source_erp/LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';


-- INSERT CSV File into Table bronze.erp_px_cat_g1v2 

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/tmp/datasets/source_erp/PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';

SET @batch_end_time = GETDATE();
PRINT('Loading Bronze Layer is completed ')
PRINT '>> Toal Load duration:' + CAST (DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds';
    END TRY 
    BEGIN CATCH

    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE'+ CAST (ERROR_NUMBER()AS NVARCHAR);
    PRINT 'ERROR MESSAGE'+ CAST (ERROR_STATE()AS NVARCHAR);
    END CATCH

END 

