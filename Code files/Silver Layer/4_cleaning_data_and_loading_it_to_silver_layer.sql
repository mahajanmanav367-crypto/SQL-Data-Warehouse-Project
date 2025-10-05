USE datawarehouse;

CREATE SCHEMA silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	
	PRINT'Truncating silve.crm_cust_info table';
	TRUNCATE TABLE silver.crm_cust_info ;
	PRINT'Loading new data to silver.crm_cust_info table';
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE UPPER(TRIM(cst_marital_status))
		WHEN 'S' THEN 'Single'
		WHEN 'M' THEN 'Married'
		ELSE 'N/A'
		END cst_marital_status,
		CASE UPPER(TRIM(cst_gndr))
		WHEN 'M' THEN 'Male'
		WHEN 'F' THEN 'Female'
		ELSE 'N/A'
		END cst_gndr,
		cst_create_date
	FROM (
		SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rn
		FROM bronze.crm_cust_info
		)t
	WHERE rn = 1 AND cst_id IS NOT NULL; 


	-------------------------------------------------------------------------------------------
	PRINT'Truncating table silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT'Inserting records in silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id, 
		cat_id,
		prd_key	,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	SELECT prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, 
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	TRIM(prd_nm) prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE),
	CAST(LEAD(prd_start_dt,1,NULL) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS new_end_dt
	FROM bronze.crm_prd_info

	-----------------------------------------------------------------

	PRINT'Truncating table silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT'Inserting records in silver.crm_prd_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key	,
		sls_cust_id	,
		sls_order_dt ,
		sls_ship_dt	,
		sls_due_dt	,
		sls_sales	,
		sls_quantity,
		sls_price 
	)
	SELECT 
		TRIM(sls_ord_num) AS sls_ord_num,
		TRIM(sls_prd_key) AS sls_prd_key,
		sls_cust_id,
		CASE 
		WHEN LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
		WHEN LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
		WHEN LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * sls_price 
		ELSE sls_sales
		END AS sls_sales,
		CASE 
		WHEN sls_quantity IS NULL OR sls_quantity <=0 THEN sls_sales/ NULLIF(sls_price,0)
		ELSE sls_quantity
		END sls_quantity,
		CASE 
		WHEN sls_price <0 THEN ABS(sls_price)
		WHEN sls_price = 0 THEN sls_sales / sls_quantity
		ELSE sls_price
		END sls_price

	FROM bronze.crm_sales_details


	-----------------------------------------------------------------------

	PRINT'Truncating table silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT'Inserting records in silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END AS cid,
		CASE
		WHEN bdate >= GETDATE() THEN NULL
		ELSE bdate
		END AS bdate,
		CASE 
		WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN  ('F','FEMALE') THEN 'Female'
		ELSE 'N/A'
		END AS gen
	FROM bronze.erp_cust_az12


	-----------------------------------------------------------------------------
	PRINT'Truncating table silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT'Inserting records in silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT REPLACE(cid,'-','') AS cid,
		CASE 
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
		ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101


	--------------------------------------------------------------------------

	PRINT'Truncating table silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT'Inserting records in silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2

END


EXEC silver.load_silver;