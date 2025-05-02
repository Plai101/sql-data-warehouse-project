/*
=============================================================
Script Purpose:
    This script drops the existing 'DataWarehouse' database (if it exists)
    and recreates it from scratch. After that, it simulates schema separation
    by creating three logical groupings using table name prefixes or separate organization: 
    'bronze', 'silver', and 'gold'. These will be used to represent different layers 
    of data processing in a data warehouse pipeline.

WARNING:
    Executing this script will permanently delete the 'DataWarehouse' database 
    and all its data. Ensure proper backups exist before proceeding.
=============================================================
*/

-- Drop the database if it exists
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
