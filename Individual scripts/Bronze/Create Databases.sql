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
