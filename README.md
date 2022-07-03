# bigquery-datacomparison
Stored procedure, table definition and exampe to compare the data in one table against another.

This stored procedure does the following:

-  Accepts the following input parameters:
    -  source_table (STRING) -- supplied in format dataset.tablename
    -  target_table (STRING) -- supplied in format dataset.tablename
    -  key_col (STRING) -- this is either the name of a column to serve as a comparison key across both tables, or a comma-delimited list of columns for compound keys.  Project and/or dataset names are not added

-  Queries INFORMATION_SCHEMA to return a list of non-key columns to iterate through and compare between both tables.
Iterates through the list of columns, finding rows in one table but not the other, or rows with different values in equivalent columns when joined across the keys.
-  Munges the results into summary information.
-  Creates a table in the source table's schema if not exists and stores the results of the comparison on the table, for future reference.



Please see complete documentation at:  https://www.bigqueryblog.com/post/bigquery-data-compare-stored-procedure

-------------------
## example usage:
-------------------
```
DECLARE my_source_table STRING;
DECLARE my_target_table STRING;
DECLARE my_key_column STRING;

SET my_source_table = 'misc.dim_car';
SET my_target_table = 'misc.dim_car2';
SET my_key_column = 'car_key';

--Perform comparison
CALL misc.p_bq_data_compare(my_source_table, my_target_table, my_key_column);

--Get results from the latest run
SELECT *
FROM misc.bq_data_compare_results
WHERE time_compared = (SELECT MAX(time_compared) FROM misc.bq_data_compare_results);


--Get sumary metrics
WITH latest_results AS (
  SELECT *
  FROM misc.bq_data_compare_results
  WHERE time_compared = (SELECT MAX(time_compared) FROM misc.bq_data_compare_results)
)


SELECT 
  (SELECT CAST(COUNT(1) AS FLOAT64) FROM latest_results WHERE tbl = my_source_table AND evaluation LIKE 'not in%')
  / (SELECT COUNT(1) FROM misc.dim_car) pct_missing_new_table
, (SELECT CAST(COUNT(1) AS FLOAT64) FROM latest_results WHERE tbl = my_target_table AND evaluation LIKE 'not in%')
  / (SELECT COUNT(1) FROM misc.dim_car) pct_extra_rows_new_table
, (SELECT CAST(COUNT(DISTINCT key_val) AS FLOAT64) FROM latest_results WHERE evaluation NOT LIKE 'not in%')
  / (SELECT COUNT(1) FROM misc.dim_car) pct_rows_non_matching;
```
