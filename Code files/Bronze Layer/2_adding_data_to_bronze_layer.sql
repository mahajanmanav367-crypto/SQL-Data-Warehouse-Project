USE datawarehouse;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	BEGIN TRY
	PRINT'============================================';
	PRINT'Loading data to bronze layer';
	PRINT'============================================';

	PRINT'                                             ';
	PRINT'---------------------------------------------';
	PRINT'Loading CRM data';
	PRINT'---------------------------------------------';

	PRINT'Truncating bronze.crm_cust_info table';
	PRINT'Loading new data to bronze.crm_cust_info table';

	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- Path of our csv file 
	WITH ( FIRSTROW = 2, -- Telling sql that data starts from row 2
		FIELDTERMINATOR = ',', -- Define the separator of your file
		TABLOCK
	);

	PRINT'Truncating bronze.crm_prd_info table';
	PRINT'Loading new data to bronze.crm_prd_info table';
	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT'Truncating bronze.crm_sales_details table';
	PRINT'Loading new data to bronze.crm_sales_details table';
	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT'---------------------------------------------';
	PRINT'Loading ERP data';
	PRINT'---------------------------------------------';

	PRINT'Truncating bronze.erp_cust_az12 table';
	PRINT'Loading new data to bronze.erp_cust_az12 table';
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT'Truncating bronze.erp_loc_a101 table';
	PRINT'Loading new data to bronze.erp_loc_a101 table';
	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT'Truncating bronze.erp_px_cat_g1v2 table';
	PRINT'Loading new data to bronze.erp_px_cat_g1v2 table';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\mahaj\OneDrive\Desktop\DIF\DE\SQL\Resources\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	END TRY
	BEGIN CATCH 

		PRINT'----------------------------------------------';
		PRINT'Error encountered while running';
		PRINT' Error messsage:- ' + ERROR_MESSAGE();
		PRINT'----------------------------------------------';

	END CATCH 

END

EXEC bronze.load_bronze;

