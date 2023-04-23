# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

def add_ingestion_date(df):
    df = df.withColumn('ingestion_date', current_timestamp())
    return df


def add_data_source(df, value):
    df = df.withColumn('data_source', lit(value))
    return df
