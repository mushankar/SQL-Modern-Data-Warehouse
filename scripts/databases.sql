/*
=================================
CREATE DATABASE AND SCHEMAS
=================================

Script Purpose: 
        This script creates a new database named "Datawarehouse" after checking if it already exists. If it already exists it drops the entire database and
recreates it. And three schemas are setup after the database is created as follows {gold, silver & bronze}

Warning: 
        Running this script will drop or delete the existing data within warehouse "Datawarehouse" if it exists.
*/

USE master;
GO

--Drop and recreate database 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER with ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Create database
CREATE DATABASE Datawarehouse;
USE Datawarehouse;

CREATE Schema bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
