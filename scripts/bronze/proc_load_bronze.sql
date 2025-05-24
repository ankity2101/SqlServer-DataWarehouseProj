/*
============================================================
Stored Procedure: Load bronze layer (Source -> Bronze)
============================================================
Script purpose:
  This stored procedure load data into the bronze schema from external csv files.
  It performs following action:
  - Truncate the bronze table before loading the data 
  - Use the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None. This stored procedure does not accept any parameters or return any value

Usage example:
  EXEC bronze.load_bronze;
============================================================
*/

create or alter procedure bronze.load_bronze as
begin
	begin try
		declare @bronze_batch_start DATE, @bronze_batch_end DATE;
		set @bronze_batch_start =GETDATE()
			print('==============================')
			print('Loading the Bronze layer')
			print('==============================')

			print('------------------------------')
			print('Loading CRM tables')
			print('------------------------------')

			declare @start_time DATE,@end_time Date;
			set @start_time =GETDATE()
			print('>>Truncating table: bronze.crm_cust_info ' )
			truncate table bronze.crm_cust_info;

			print('>>Inserting into table: bronze.crm_cust_info ')
			bulk insert bronze.crm_cust_info
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')
			--select * from bronze.crm_cust_info;
			---------------------------------------------------------------------------

			set @start_time =GETDATE()
			print('>>Truncating table: bronze.crm_prd_info')
			truncate table bronze.crm_prd_info;

			print('>>Inserting into table: bronze.crm_prd_info ')
			bulk insert bronze.crm_prd_info
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

			--select * from bronze.crm_prd_info;
			----------------------------------------------------------------------------------

			set @start_time =GETDATE()
			print('>>Truncating table: bronze.crm_sales_details')
			truncate table bronze.crm_sales_details;
			print('>>Inserting into table: bronze.crm_sales_details')
			bulk insert bronze.crm_sales_details
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

			--select * from bronze.crm_sales_details;
			---------------------------------------------------------------------------------------
			---------------------------------------------------------------------------------------

			print('------------------------------')
			print('Loading ERP tables')
			print('------------------------------')

			set @start_time =GETDATE()

			print('>>Truncating table: bronze.erp_cust_az12')
			truncate table bronze.erp_cust_az12;
			print('>>Inserting into table: bronze.erp_cust_az12 ')
			bulk insert bronze.erp_cust_az12
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

			----------------------------------------------------------------------------

			--select * from bronze.erp_cust_az12;
			set @start_time =GETDATE()

			print('>>Truncating table: bronze.erp_loc_a101')
			truncate table bronze.erp_loc_a101;
			print('>>Inserting into table: bronze.erp_loc_a101')
			bulk insert bronze.erp_loc_a101
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

			------------------------------------------------------------------
			set @start_time =GETDATE()
			print('>>Truncating Tbale: bronze.erp_px_cat_g1v2')
			truncate table bronze.erp_px_cat_g1v2;
			print('>>Inserting into table: bronze.erp_px_cat_g1v2')
			bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\ankit\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			with (
				firstrow =2,
				fieldterminator=',',
				tablock
			);
			set @end_time =GETDATE()
			print('------------------------------')
			print '>>Load duration '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
			print('------------------------------')

		set @bronze_batch_end= GETDATE()
		print('------------------------------')
		print('----Loading Bronze layer is Completed!!----')
		print '>>Load duration for whole batch '+ cast(datediff(second,@bronze_batch_start,@bronze_batch_end) as nvarchar) +' seconds';
		print('------------------------------')
	end try

		begin catch
		print('==============================================')
		print('ERROR OCCURED DURING LOADING BRONZE LAYER')
		print('Error message:'+ Error_message())
		print('==============================================')
		
	end catch
end
