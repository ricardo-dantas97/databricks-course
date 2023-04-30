-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vw_dominant_drivers AS
SELECT
  driver_name
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
  , RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  driver_name
  , race_year
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY 
  driver_name
  , race_year
ORDER BY
  race_year DESC
  , avg_points DESC

-- COMMAND ----------

SELECT
  driver_name
  , race_year
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY 
  driver_name
  , race_year
ORDER BY
  race_year DESC
  , avg_points DESC

-- COMMAND ----------

SELECT
  driver_name
  , race_year
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY 
  driver_name
  , race_year
ORDER BY
  race_year DESC
  , avg_points DESC
