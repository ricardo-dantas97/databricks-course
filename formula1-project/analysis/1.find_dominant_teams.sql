-- Databricks notebook source
SELECT
  team_name
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(*) > 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  team_name
  , SUM(calculated_points) AS total_points
  , COUNT(*) AS total_races
  , ROUND(AVG(calculated_points), 2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year >= 2011
GROUP BY team_name
HAVING COUNT(*) > 100
ORDER BY avg_points DESC
