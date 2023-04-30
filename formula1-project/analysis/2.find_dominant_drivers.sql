-- Databricks notebook source
SELECT
  driver_name
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  driver_name
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year >= 2011
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_points DESC
