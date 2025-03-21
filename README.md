# TaxiETLApp
ETL project for importing taxi trip data from CSV into SQL Server

How to assume if the program is used on much larger data files?
To handle a 10GB CSV file, I would optimize the program by using StreamReader for streaming data to avoid loading the entire file into memory and process it in chunks; for insertion into SQL Server, I would use SqlBulkCopy with an optimized batch size (e.g., 10,000 rows) to reduce overhead; I would handle duplicate removal directly in SQL using ROW_NUMBER() for efficient processing of large datasets; I would also add indexes on frequently queried columns (e.g., tpep_pickup_datetime) and consider parallel processing with Task Parallel Library (TPL) to speed up data analysis and insertion.

Number of rows: 29889
