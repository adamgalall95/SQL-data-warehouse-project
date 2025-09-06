/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

drop view if exists gold.dim_customers;
create view gold.dim_customers as(
	select 
		row_number() over(order by cc.cst_id) as customer_key,
		cc.cst_id as customer_id,
		cc.cst_key as customer_number,
		cc.cst_firstname as first_name,
		cc.cst_lastname as last_name,
		cc.cst_marital_status as marital_status,
		case 
			when cc.cst_gndr != 'n/a' then cc.cst_gndr
			when ec.gen is not null then ec.gen
			else 'n/a'
		end as gender,
		el.cntry as country,
		ec.bdate as birth_date,
		cc.cst_create_date as create_date
	from silver.crm_cust_info as cc
	left join silver.erp_cust_az12 as ec
	on cc.cst_key = ec.cid
	left join silver.erp_loc_a101 as el
	on cc.cst_key = el.cid
);

drop view if exists gold.dim_products;
create view gold.dim_products as (
	SELECT
	    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
	    pn.prd_id       AS product_id,
	    pn.prd_key      AS product_number,
	    pn.prd_nm       AS product_name,
	    pn.cat_id       AS category_id,
	    pc.cat          AS category,
	    pc.subcat       AS subcategory,
	    pc.maintenance  AS maintenance,
	    pn.prd_cost     AS cost,
	    pn.prd_line     AS product_line,
	    pn.prd_start_dt AS start_date
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	    ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL
);

drop view if exists gold.fact_sales;
	create view gold.fact_sales as (
	SELECT
	    sd.sls_ord_num  AS order_number,
	    pr.product_key  AS product_key,
	    cu.customer_key AS customer_key,
	    sd.sls_order_dt AS order_date,
	    sd.sls_ship_dt  AS shipping_date,
	    sd.sls_due_dt   AS due_date,
	    sd.sls_sales    AS sales_amount,
	    sd.sls_quantity AS quantity,
	    sd.sls_price    AS price
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
	    ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers cu
	    ON sd.sls_cust_id = cu.customer_id
);

select * from gold.fact_sales f
left join gold.dim_customers dc
on f.customer_key = dc.customer_key
left join gold.dim_products dp
on f.product_key = dp.product_key
where f.customer_key is null or f.product_key is null




