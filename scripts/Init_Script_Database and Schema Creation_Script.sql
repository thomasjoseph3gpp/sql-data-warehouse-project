
/*
===================================================

Create Database and Schemas

===================================================

Script Purpose:
	This script creates a new database named DataWarehouse and corresponging Schemas after checking whether it exists or not.
	If already exists, it will drop the existing database and create a new one.
	Since no historization needed, this can be considered as a best practice.
	As we follow Medalion Architecture the created schemas are bronze , silver and gold

Warning

	Running this script will drop the entire database.
	As we set rollback immediate, it will rollback all the ongoing changes in the database.
	As we set to single user, all other users will be automatically logged out from this.
	So need to ensure proper backups if needed.

	*/




-- Create Database DataWarehouse

USE master;-- to switch to master to create database


IF EXISTS ( SELECT 1 FROM sys.databases WHERE  name = 'DataWarehouse') -- to check whether database named DataWarehouse esists or not

BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse; -- to drop the database DataWarehouse if exists
END

GO
CREATE DATABASE DataWarehouse;

GO
USE DataWarehouse;

--To create the necessary Schemas ( bronze, silver and gold)
GO
CREATE SCHEMA bronze;

GO
CREATE SCHEMA silver;

GO
CREATE SCHEMA gold;

GO



