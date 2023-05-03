# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

def add_ingestion_date(df):
    df = df.withColumn('ingestion_date', current_timestamp())
    return df


def add_data_source(df, value):
    df = df.withColumn('data_source', lit(value))
    return df


def add_created_date(df):
    df = df.withColumn('created_date', current_timestamp())
    return df


def add_file_date(df, file_date):
    df = df.withColumn('file_date', lit(file_date))
    return df


def re_arrange_partition_column(input_df, partition_column):
    # Spark expects the partition column to be the last one in the dataframe
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df


def overwrite_partition(input_df, db_name, tbl_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{tbl_name}')):
        output_df.write.mode('overwrite').insertInto(f'{db_name}.{tbl_name}')
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f'{db_name}.{tbl_name}')


def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                           .distinct() \
                           .collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list
