-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/rddatabricks/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path '/mnt/rddatabricks/raw/races.csv', header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path '/mnt/rddatabricks/raw/constructors.json')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path '/mnt/rddatabricks/raw/drivers.json')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid  INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  miliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS (path '/mnt/rddatabricks/raw/results.json')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stop table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS (path '/mnt/rddatabricks/raw/pit_stops.json', multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create table for list of files

-- COMMAND ----------

-- Create lap times table
DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path '/mnt/rddatabricks/raw/lap_times')

-- COMMAND ----------

-- Create qualifying table
DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING json
OPTIONS (path '/mnt/rddatabricks/raw/qualifying', multiLine true)
