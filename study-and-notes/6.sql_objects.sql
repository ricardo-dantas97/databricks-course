-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create demo database
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

-- More info
DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- Show tables in specific database
SHOW TABLES IN demo;

-- COMMAND ----------

-- Change a database
USE demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed tables

-- COMMAND ----------

-- MAGIC %run "../formula1-project/utils/configs"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- Creating managed table using SQL
CREATE TABLE race_results_sql AS
  SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

-- When we drop a managed table, data is droped on DBFS as well
DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # We specify the location where data will be saved
-- MAGIC race_results.write.format('parquet') \
-- MAGIC     .option('path', f'{presentation_folder_path}/race_results_ext_py') \
-- MAGIC     .saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql_test (
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap STRING,
  race_time STRING,
  points STRING,
  position STRING,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/rddatabricks/presentation/race_results_ext_sql_test"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

drop table demo.race_results_ext_sql_test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())
-- MAGIC print(presentation_folder_path)

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql_test
SELECT *
FROM demo.race_results_ext_py
WHERE race_year = 2020

-- COMMAND ----------

SELECT COUNT(*)
FROM demo.race_results_ext_sql_test
