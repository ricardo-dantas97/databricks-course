-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
LIMIT 50;

-- COMMAND ----------

DESC f1_processed.drivers;

-- COMMAND ----------

-- Window functions
WITH tbl AS (

  SELECT
    nationality
    , name
    , FLOOR(DATEDIFF(CURRENT_TIMESTAMP(), dob) / 365) AS age
    , ROW_NUMBER() OVER(PARTITION BY nationality ORDER BY dob DESC) AS row_num
  FROM f1_processed.drivers

)

  SELECT *
  FROM tbl
  WHERE row_num <= 5
