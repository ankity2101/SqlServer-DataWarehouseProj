/*
===================================================================
Stored Procedure : Load Silver Layer (Bronze -> Silver)
===================================================================
Script Purpose: 
    This Stored procedure performs the ETL process to populate the 
    'silver' schema tables from the 'bronze' schema.

Actions Performed:
    -TruncateSilver Tables
    -Insert transformed data from bronze to silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return
    any values.

Usage Example:
    exec silver.load_silver
===================================================================
*/


create or alter procedure silver.load_silver as 
begin 
		begin try
		declare @silver_batch_start DATE, @silver_batch_end DATE;
		set @silver_batch_start =GETDATE()
			print('==============================')
			print('Loading the Silver layer')
			print('==============================')

			print('------------------------------')
			print('Loading CRM tables')
			print('------------------------------')

			declare @start_time DATE,@end_time Date;
			set @start_time =GETDATE()
	
			print('>>Truncating table: silver.crm_cust_info ' )
			truncate table silver.crm_cust_info;

			print('>> Inserting data into : silver.crm_cust_info')
			insert into silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			select cst_id, cst_key, trim(cst_firstname) cst_firstname ,
				trim(cst_lastname) cst_lastname,
			case 
				when upper(trim(cst_marital_status)) ='S' then 'Single'
				 when upper(trim(cst_marital_status)) = 'M' then 'Married'
				 else 'N/A'
			end cst_marital_status,
			case 
				when upper(trim(cst_gndr)) ='F' then 'Female'
				 when upper(trim(cst_gndr)) ='M' then 'Male'
				 else 'N/A'
			end cst_gndr,
			cst_create_date from (
				select *, row_number() 
				over(partition by cst_id 
				order by cst_create_date desc)
				flag from bronze.crm_cust_info where cst_id is not null
			) t where flag  =1 

			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')
			-----
			set @start_time =GETDATE()
			print('>>Truncating table: silver.crm_prd_info ' )
			truncate table silver.crm_prd_info;
			print('>> Inserting data into : silver.crm_prd_info')
			insert into silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)


			SELECT [prd_id]
				  ,replace(substring(prd_key,1,5),'-','_') cat_id
				  ,substring(prd_key,7,len(prd_key)) prd_key
				  ,[prd_nm]
				  ,isnull(prd_cost,0) prd_cost,
				  case upper(trim(prd_line))
					  when 'M' then 'Mountain'
					  when 'R' then 'Road'
					  when 'S' then 'Other Sales'
					  when 'T' then 'Touring'
					  else 'N/A'
				  end as prd_line
				  ,cast(prd_start_dt as DATE) as prd_start_dt
				  ,cast(lead(prd_start_dt) 
				  over(partition by prd_key 
				  order by prd_start_dt )-1 as DATE) prd_end_dt
			  FROM [bronze].[crm_prd_info]


			 set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')
			  ------------
			  set @start_time =GETDATE()

			print('>>Truncating table: silver.crm_sales_details ' )
			truncate table silver.crm_sales_details;
			print('>> Inserting data into : silver.crm_sales_details')

			insert into silver.crm_sales_details(
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

			SELECT [sls_ord_num]
				  ,[sls_prd_key]
				  ,[sls_cust_id]
				  ,case 
					when sls_order_dt <=0 or len(sls_order_dt) !=8 then null
					else cast(cast(sls_order_dt as VARCHAR) as date)
				  end as sls_order_dt,

				  case when sls_order_dt <=0 or len(sls_order_dt) !=8 then null
				  else cast(cast(sls_ship_dt as VARCHAR) as date)
				  end as sls_ship_dt,

				  case when sls_due_dt <=0 or len(sls_due_dt) !=8 then null
				  else cast(cast(sls_due_dt as VARCHAR) as date)
				  end as sls_due_dt

				  ,case when sls_sales is null or sls_sales <=0 or sls_sales !=sls_quantity* abs(sls_price)
						then sls_quantity * abs(sls_price)
					else sls_sales
					end as sls_sales--recalculate sales if original value is misssing or incorrect

				  ,[sls_quantity]
				  ,case when sls_price is null or sls_price <=0
						then sls_sales /nullif(sls_quantity,0)
					else sls_price
					end as sls_price --derive price if original value is invalid
		
			  FROM [DataWarehouse].[bronze].[crm_sales_details]

			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')


			  ------

			 set @start_time =GETDATE()
			print('>>Truncating table: silver.erp_cust_az12 ' )
			truncate table silver.erp_cust_az12;
			print('>> Inserting data into : silver.erp_cust_az12')
			insert into silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)
			select 
				case when cid like '%NAS%' then substring(cid,4,len(cid))
					else cid
				end as cid,
				case when bdate> GETDATE() then null
					else bdate
				end as bdate,
				case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
					when upper(trim(gen)) in ('M','MALE') then 'Male'
					else 'N/A'
				end as gen
			from bronze.erp_cust_az12;

			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

			-------

			set @start_time =GETDATE()
			print('>>Truncating table: silver.erp_loc_a101 ' )
			truncate table silver.erp_loc_a101;
			print('>> Inserting data into : silver.erp_loc_a101')
			insert into silver.erp_loc_a101(
				cid,
				cntry
			)

			  select replace(cid,'-','') cid,
			  case 
					when trim(cntry)='DE' then 'Germany'
					when trim(cntry) in('US','USA') then 'United States'
					when trim(cntry)='' or cntry is null then 'N/A'
					else trim(cntry)
				end as cntry --normalize and handle missing or blank country codes 
			  from bronze.erp_loc_a101;
			  set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')
			 ------

			 set @start_time =GETDATE()
			print('>>Truncating table: silver.erp_px_cat_g1v2 ' )
			truncate table silver.erp_px_cat_g1v2;
			 print('>> Inserting data into : silver.erp_px_cat_g1v2')
			 insert into silver.erp_px_cat_g1v2(
				 id,
				cat,
				subcat,
				maintenance)

			select 
				id,
				cat, 
				subcat,
				maintenance 
			from bronze.erp_px_cat_g1v2;
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

		set @silver_batch_end= GETDATE()
		print('------------------------------')
		print('----Loading Silver layer is Completed!!----')
		print '>>Load duration for whole batch '+
		cast(datediff(second,@silver_batch_start,@silver_batch_end) as nvarchar) +' seconds';
		print('------------------------------')
	end try

		begin catch
		print('==============================================')
		print('ERROR OCCURED DURING LOADING SILVER LAYER')
		print('Error message:'+ Error_message())
		print('==============================================')
		
	end catch
end


