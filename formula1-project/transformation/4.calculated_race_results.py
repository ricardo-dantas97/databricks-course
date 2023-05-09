# Databricks notebook source
dbutils.widgets.text('p_file_date', '')
file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results (
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT,
        position INT,
        original_points INT,
        calculated_points INT,
        created_date TIMESTAMP,
        updated_date TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW race_results_updated AS
    SELECT
        rc.race_year
        , c.name AS team_name
        , d.driver_id AS driver_id
        , d.name AS driver_name
        , rc.race_id
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
    AND r.file_date = '{file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updated upd
# MAGIC ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET  tgt.position = upd.position,
# MAGIC               tgt.calculated_points = upd.calculated_points,
# MAGIC               tgt.original_points = upd.original_points,
# MAGIC               tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (race_year, team_name, driver_id, driver_name, race_id, position, original_points, calculated_points, created_date)
# MAGIC   VALUES (race_year, team_name, driver_id, driver_name, race_id, position, original_points, calculated_points, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.calculated_race_results
