# Databricks notebook source
# MAGIC %run "../utils/configs"

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')
races_df = spark.read.parquet(f'{processed_folder_path}/races')
circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')
drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers')
constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
             .select(
                races_df.race_id,
                races_df.race_year,
                races_df.name.alias('race_name'),
                races_df.race_timestamp.alias('race_date'),
                circuits_df.location.alias('circuit_location')
             )

# COMMAND ----------

df = results_df.join(df, results_df.race_id == df.race_id) \
               .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
               .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
               .select(
                    df.race_year,
                    df.race_name,
                    df.race_date,
                    df.circuit_location,
                    results_df.grid,
                    results_df.fastest_lap,
                    results_df.time.alias('race_time'),
                    results_df.points,
                    results_df.position,
                    drivers_df.name.alias('driver_name'),
                    drivers_df.number.alias('driver_number'),
                    drivers_df.nationality,
                    constructors_df.name.alias('team')
                )

# COMMAND ----------

df = add_created_date(df)

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
