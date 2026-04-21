-- Explore All objects in the Datebase
SELECT * FROM INFORMATION_SCHEMA.TABLES

-- Explore all columns in the Datebase
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'