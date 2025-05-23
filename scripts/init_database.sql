/*
============================
create database and schemas
============================
Script purpose :
here we are create a new Database named "DataWarehouse" after checking if it aready exists.
If the database exists , it will be dropped and recreated. Additionally, the script set up
three schemas within the database - 'bronze', 'silver' and 'gold'.

WARNING: Running this script will drop the entire 'Datawarehouse' database if exists.

*/

use master;
GO

--drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name= 'DataWarehouse')
begin 
	alter database Datawarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
GO

--create 'DataWarehouse' database
create database DataWarehouse;
GO

use DataWarehouse;
GO

 --create schemas
 create schema bronze;
 GO

 create schema silver;
 GO

 create schema gold;
 GO
