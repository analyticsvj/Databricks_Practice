from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, MapType
import logging

logger = logging.getLogger("PETFileLoader")

def write_config(spark: SparkSession, dataset_id: str, spec_data: dict, param_data: dict, db_name: str, last_process_map: dict, overwrite: bool) -> bool:
    """Write configuration data to the Delta table."""
    try:
        config_row = {
            "dataset_id": dataset_id,
            "dataset_name": spec_data.get("Dataset Name"),
            "source_database_name": db_name,
            "load_type": param_data.get("Load Type"),
            "is_enabled": True,
            "pet_dataset_id": param_data.get("Pet Dataset ID"),
            "incremental_timestamp_method": param_data.get("Incremental Timestamp Method"),
            "incremental_timestamp_field": param_data.get("Incremental Timestamp Field"),
            "incremental_header_to_tables_link_field": param_data.get("Incremental Header To Tables  Link Field"),
            "schedule": param_data.get("Schedule"),
            "week_days": param_data.get("Week Days"),
            "dates_of_month": param_data.get("Dates Of Month"),
            "dataset_owner": param_data.get("Dataset Owner"),
            "modified_date": datetime.now(),
            "dataset_modified_by_user": os.getenv("USER", "system"),
            "last_run_time": None,
            "next_run_due_time": None,
            "last_process_datetime": last_process_map,
            "comments": param_data.get("Comments")
        }

        config_schema = StructType([
            StructField("dataset_id", StringType(), True),
            StructField("dataset_name", StringType(), True),
            StructField("source_database_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("is_enabled", BooleanType(), True),
            StructField("pet_dataset_id", StringType(), True),
            StructField("incremental_timestamp_method", StringType(), True),
            StructField("incremental_timestamp_field", StringType(), True),
            StructField("incremental_header_to_tables_link_field", StringType(), True),
            StructField("schedule", StringType(), True),
            StructField("week_days", StringType(), True),
            StructField("dates_of_month", StringType(), True),
            StructField("dataset_owner", StringType(), True),
            StructField("modified_date", TimestampType(), True),
            StructField("dataset_modified_by_user", StringType(), True),
            StructField("last_run_time", TimestampType(), True),
            StructField("next_run_due_time", TimestampType(), True),
            StructField("last_process_datetime", MapType(StringType(), TimestampType()), True),
            StructField("comments", StringType(), True)
        ])

        config_df = spark.createDataFrame([Row(**config_row)], schema=config_schema)
        config_df.write.format("delta").mode("overwrite" if overwrite else "append").saveAsTable("pet.ctrl_dataset_config_vj")
        logger.info("Configuration data written successfully.")
        return True
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_config_vj: {e}")
        raise

def write_input_fields(spark: SparkSession, input_fields_df: pd.DataFrame, dataset_id: str) -> bool:
    """Write input fields to the Delta table."""
    try:
        df = input_fields_df.copy()
        df["dataset_id"] = dataset_id
        df.rename(columns={"Table Name": "table_name", "Column Name": "field_name"}, inplace=True)
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").saveAsTable("pet.ctrl_dataset_input_fields_vj")
        logger.info("Input fields written successfully.")
        return True
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_input_fields_vj: {e}")
        raise

def write_config_table(spark: SparkSession, df: DataFrame, schema: StructType, overwrite: bool) -> None:
    """Write configuration data to the Delta table."""
    try:
        df_to_write = spark.createDataFrame(df.rdd, schema=schema)
        df_to_write.write.format("delta").mode("overwrite" if overwrite else "append").saveAsTable("pet.ctrl_dataset_config_vj")
        logger.info("Configuration data written successfully.")
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_config_vj: {e}")
        raise

def write_input_fields_table(spark: SparkSession, df: DataFrame, dataset_id: str) -> None:
    """Write input fields to the Delta table."""
    try:
        df = df.copy()
        df["dataset_id"] = dataset_id
        df.rename(columns={"Table Name": "table_name", "Column Name": "field_name"}, inplace=True)
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").saveAsTable("pet.ctrl_dataset_input_fields_vj")
        logger.info("Input fields written successfully.")
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_input_fields_vj: {e}")
        raise
