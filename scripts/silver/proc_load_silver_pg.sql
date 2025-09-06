/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from 'bronze' schema. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `BULK INSERT` command to load data from 'bronze' schema to 'silver' tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
	--silver.crm_cust_info
	truncate table silver.crm_cust_info;
	insert into silver.crm_cust_info
	select
		cst_id,
		cst_key,
		lower(trim(cst_firstname)) as cst_firstname,
		lower(trim(cst_lastname)) as cst_lastname,
		Case
			when upper(trim(cst_marital_status)) = 'S' then 'Single'
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		End as cst_marital_status,
		Case
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			else 'n/a'
		End as cst_gndr,
		cst_create_date
	from (
		select
		*,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
	)
	where flag_last = 1;
	
	--silver.crm_prd_info
	truncate table silver.crm_prd_info;
	insert into silver.crm_prd_info
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,        -- Extract product key
		prd_nm,
		COALESCE(prd_cost, 0)AS prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' 
			AS DATE
		) AS prd_end_dt -- Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info;
	
	--silver.crm_sales_details
	truncate table silver.crm_sales_details;
	insert into silver.crm_sales_details
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price  -- Derive price if original value is invalid
		END AS sls_price
	FROM bronze.crm_sales_details;
	
	--silver.erp_cust_az12
	truncate table silver.erp_cust_az12;
	insert into silver.erp_cust_az12
	select
	case 
		when cid like 'NAS%' then substring(cid,4,length(cid)) 
		else cid
	end as cid,
	case
		when bdate > now() then null
		else bdate
	end as bdate,
	case
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		else 'n/a'
	end as gen
	from bronze.erp_cust_az12;
	
	--silver.erp_loc_a101
	truncate table silver.erp_loc_a101;
	insert into silver.erp_loc_a101
	select 
		replace(cid,'-','') as cid_1,
		case 
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
		end as cntry
	from bronze.erp_loc_a101;
	
	--silver.erp_px_cat_g1v2
	truncate table silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2
	select 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2;
END;
$$;