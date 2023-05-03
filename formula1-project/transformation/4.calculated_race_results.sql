-- Databricks notebook source
CREATE TABLE f1_presentation.calculated_race_results
USING parquet AS
  SELECT
    rc.race_year
    , c.name AS team_name
    , d.name AS driver_name
    , r.position
    , r.points AS original_points
    , CASE
        WHEN r.position = 1 THEN 10
        WHEN r.position = 2 THEN 9
        WHEN r.position = 3 THEN 8
        WHEN r.position = 4 THEN 7
        WHEN r.position = 5 THEN 6
        WHEN r.position = 6 THEN 5
        WHEN r.position = 7 THEN 4
        WHEN r.position = 8 THEN 3
        WHEN r.position = 9 THEN 2
        WHEN r.position = 10 THEN 1
        ELSE 0
    END AS calculated_points
  FROM f1_processed.results AS r
  JOIN f1_processed.drivers AS d ON d.driver_id = r.driver_id
  JOIN f1_processed.constructors AS c ON c.constructor_id = r.constructor_id
  JOIN f1_processed.races AS rc ON r.race_id = rc.race_id 
  WHERE r.position <= 10
