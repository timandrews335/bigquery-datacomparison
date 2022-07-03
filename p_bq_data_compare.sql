CREATE OR REPLACE PROCEDURE <<mydataset>>.p_bq_data_compare(source_table STRING, target_table STRING, key_col STRING)
BEGIN

/*
Author:  Tim Andrews / 2022-07-03
https://github.com/timandrews335/bigquery-datacomparison
*/


DECLARE current_col INT64;
DECLARE create_sql STRING;
DECLARE loop_sql STRING;
DECLARE source_schema STRING;
DECLARE source_table_small STRING;
DECLARE current_colname STRING;
DECLARE info_schema_where_clause STRING; --If the user passed in a comma-delimited set of values for the PK, then put single quotes around it.
DECLARE key_col_to_except_by STRING; --If the user has pased in a compound key, we need to concatenate it into a single column for comparision.
DECLARE create_final_tables STRING;
DECLARE comparison_run TIMESTAMP; --Unique identifier for this comparison exercise


SET source_schema = SPLIT(source_table, '.')[offset(0)];
SET source_table_small = SPLIT(source_table, '.')[offset(1)];
SET current_col = 1;
SET comparison_run = CURRENT_TIMESTAMP();

--If the user passed in a comma-delimited set of values for the PK, then put single quotes around it.
--If the user has pased in a compound key, we need to concatenate it into a single column for comparision.
IF ARRAY_LENGTH(SPLIT(key_col, ',')) > 1 THEN
  SET info_schema_where_clause = CONCAT("'", key_col, "'");
  SET info_schema_where_clause = REPLACE(info_schema_where_clause, ",", "','");
  SET key_col_to_except_by = CONCAT('CONCAT(CAST( ', ARRAY_TO_STRING(SPLIT(key_col, ','), ' AS STRING),"--", CAST('), ' AS STRING)) AS key_col');
ELSE
  SET info_schema_where_clause = CONCAT("'", key_col, "'");
  SET key_col_to_except_by = CONCAT('CAST(', key_col, ' AS STRING) AS key_col');
END IF;


--Build up a script to obtain the columns in the source table that we need to compare against the target table.  This will yield a temp table as a list to iterate through, performing comparisions while we join both tables on the key_col.
SET create_sql = CONCAT("CREATE OR REPLACE TEMPORARY TABLE cols_to_process AS SELECT column_name, ordinal_position, 0 as processed  FROM ", source_schema, ".INFORMATION_SCHEMA.COLUMNS WHERE table_name = '", source_table_small, "' AND column_name NOT IN ( ", info_schema_where_clause  , ") ORDER BY ordinal_position;");

EXECUTE IMMEDIATE create_sql;

--Temp table to hold our comparison differences
CREATE TEMPORARY TABLE diffs (key_val STRING, col_val STRING, tbl STRING, col STRING);

--Iterate through our list performing comparisons
WHILE (SELECT COUNT(1) FROM cols_to_process WHERE processed = 0) > 0 DO
  SET current_colname = (SELECT column_name FROM cols_to_process WHERE ordinal_position = current_col);
  --In source, not in target
  SET loop_sql = CONCAT("INSERT INTO diffs SELECT s.*, '", source_table, "' AS tbl, '", current_colname, "' AS col FROM (\n");
  SET loop_sql = CONCAT(loop_sql, "SELECT ", key_col_to_except_by, ", CAST(", current_colname, " AS STRING) AS col_val FROM ", source_table, " src EXCEPT DISTINCT ", "SELECT ", key_col_to_except_by, ", CAST(", current_colname, " AS STRING) AS col_val FROM ", target_table, " dst ");
  SET loop_sql = CONCAT(loop_sql, "\n) s");
  --In target, not in source
  SET loop_sql = CONCAT(loop_sql, "\n");
  SET loop_sql = CONCAT(loop_sql, "UNION ALL\n");
  SET loop_sql = CONCAT(loop_sql, "SELECT t.*, '", target_table, "' AS tbl, '", current_colname, "' AS col  FROM (\n");
  SET loop_sql = CONCAT(loop_sql, "SELECT ", key_col_to_except_by, ", CAST(", current_colname, " AS STRING) AS col_val FROM ", target_table, " dst EXCEPT DISTINCT ", "SELECT ", key_col_to_except_by, ", CAST(", current_colname, " AS STRING) AS col_val FROM ", source_table, " src"); 
  SET loop_sql = CONCAT(loop_sql, "\n) t");
  SET loop_sql = CONCAT(loop_sql, ";");
  IF(loop_sql IS NOT NULL) THEN
    EXECUTE IMMEDIATE loop_sql;
    --SELECT loop_sql;  --Use this to debug the main SQL statement that is doing the comparison
  END IF;
  UPDATE cols_to_process SET processed = 1 WHERE ordinal_position = current_col;
  SET current_col = current_col + 1;
END WHILE;

/* The final results need to be organized into three types, with one row per mis-match:
1.  Rows which are in the source table, but not the target table
2.  Rows which are in the target table, but not in the source table
3.  One observation for each column, for each row in the source table that has a different value for that column

These results will be stored in a final, permanent table that will be created if does not exist, in the source schema.
*/

SET create_final_tables = CONCAT("CREATE TABLE IF NOT EXISTS ", source_schema, ".bq_data_compare_results(time_compared TIMESTAMP, key_val STRING, col_val STRING, tbl STRING, col STRING, evaluation STRING, other_col_val STRING);");
EXECUTE IMMEDIATE create_final_tables;
SET create_final_tables = CONCAT("INSERT INTO  ", source_schema, ".bq_data_compare_results SELECT CAST('", CAST(comparison_run AS STRING) , "' AS TIMESTAMP), * FROM results2;");


CREATE OR REPLACE TEMPORARY TABLE results AS 
  SELECT 
  a.* 
  ,CASE
    WHEN b.key_val IS NULL THEN 'not in other table'
    ELSE 'value is different'
  END AS evaluation
  FROM diffs a 
  LEFT OUTER JOIN diffs b
    ON a.key_val = b.key_val
    AND a.col = b.col
    AND a.tbl <> b.tbl
  ORDER BY 1, 4, 3
;

CREATE OR REPLACE TEMPORARY TABLE results2 AS 
SELECT DISTINCT key_val, CAST(NULL AS STRING) AS col_val, tbl, CAST(NULL AS STRING) AS col, CONCAT('not in ', target_table) AS evaluation, CAST(NULL AS STRING) AS other_col_val  FROM results WHERE evaluation = 'not in other table' AND tbl = source_table
UNION ALL
SELECT DISTINCT key_val, CAST(NULL AS STRING) AS col_val, tbl, CAST(NULL AS STRING) AS col, CONCAT('not in ', source_table) AS evaluation, CAST(NULL AS STRING) AS other_col_val  FROM results WHERE evaluation = 'not in other table' AND tbl = target_table
UNION ALL
SELECT a.key_val, a.col_val, a.tbl, a.col, a.evaluation, CAST(b.col_val AS STRING) AS other_col_val 
FROM results a
INNER JOIN results b
  ON a.col = b.col
  AND a.tbl <> b.tbl
  AND a.tbl = source_table
  AND a.key_val = b.key_val
;

EXECUTE IMMEDIATE create_final_tables;

END
