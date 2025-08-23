from __future__ import annotations

import io
import os
# import re
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Any, Union

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
# from pyspark.sql import Row, DataFrame
# from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    MapType,
)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("PETFileLoader")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ------------------------------------------------------------------------------
# Option C helpers: read Excel to memory (bytes -> pandas.ExcelFile)
# ------------------------------------------------------------------------------
def _split_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri.lower().startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    path = s3_uri[5:]
    first = path.find("/")
    if first < 0:
        raise ValueError(f"Bad S3 URI (missing key): {s3_uri}")
    bucket = path[:first]
    key = path[first + 1 :]
    return bucket, key


def fetch_xlsx_bytes(uri: str) -> bytes:
    """
    Return the Excel file contents as bytes.

    Supports:
      - s3://bucket/key.xlsx   (via boto3)
      - dbfs:/path/file.xlsx   (via open on /dbfs)
      - /dbfs/... or /local path
    """
    if uri.lower().startswith("s3://"):
        import boto3

        bucket, key = _split_s3_uri(uri)
        logger.info(f"PETFileLoader:Fetching bytes from s3://{bucket}/{key}")
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    # DBFS convenience: allow dbfs:/ and /dbfs/
    if uri.lower().startswith("dbfs:/"):
        path = "/dbfs/" + uri[6:]
    else:
        path = uri

    with open(path, "rb") as f:
        data = f.read()
    logger.info(f"PETFileLoader:Fetched {len(data)} bytes from {uri}")
    return data


def open_excel(blob: bytes) -> pd.ExcelFile:
    """
    Create a pandas.ExcelFile from in-memory bytes using openpyxl.
    """
    bio = io.BytesIO(blob)
    return pd.ExcelFile(bio, engine="openpyxl")

# COMMAND ----------


import os
from datetime import datetime
from typing import Optional, Dict

import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType, MapType
)

# logger should already exist in your module:
# logger = logging.getLogger("PETFileLoader")


# ---------- small utilities ----------
def _p(d: Dict[str, str], key: str) -> Optional[str]:
    """Case/space-insensitive getter for Params keys."""
    want = key.lower().replace(" ", "")
    for k, v in d.items():
        if str(k).lower().replace(" ", "") == want:
            return None if v is None else str(v).strip()
    return None

def _to_bool(s: Optional[str]) -> bool:
    return str(s).strip().lower() in {"true", "1", "yes", "y"} if s is not None else False


# ---------- CONFIG WRITER ----------
def write_config(
    spark: SparkSession,
    dataset_id: str,
    spec_data: dict,
    param_data: dict,
    db_name: str,
    last_process_map: dict,
    overwrite: bool,
    is_data_provisioning: bool = False
) -> bool:
    """
    Write to pet.ctrl_dataset_config; merge last_process_datetime if dataset_id+stage exists.
    """
    try:
        stage = _p(param_data, "Stage") or param_data.get("Stage")

        # 1) existing row (dataset_id + stage)
        existing_df = spark.sql(
            f"""
            SELECT * FROM pet.ctrl_dataset_config
            WHERE dataset_id = '{dataset_id}'
              AND stage = '{stage}'
            """
        )
        merged_last_process_map = {}
        if existing_df.count() > 0:
            existing_record = existing_df.collect()[0].asDict()
            existing_last = existing_record.get("last_process_datetime") or {}
            merged_last_process_map.update(existing_last)
            merged_last_process_map.update(last_process_map)
        else:
            merged_last_process_map = last_process_map

        # 2) map values (all from sheets)
        dataset_name = spec_data.get("Dataset Name") or dataset_id
        is_enabled = _to_bool(_p(param_data, "Is Enabled"))
        pet_dataset_id = _p(param_data, "Pet Dataset ID")
        dataset_owner = _p(param_data, "Dataset Owner")
        comments = _p(param_data, "Comments")
        
        # Skip these fields for data provisioning
        if is_data_provisioning:
            load_type = None
            method_norm = None
            inc_ts_field_raw = None
            header_link = None
            schedule = None
            week_days = None
            dates_of_month = None
        else:
            load_type = (_p(param_data, "Load Type") or "").upper()
            method_raw = _p(param_data, "Incremental Timestamp Method") or ""
            method_norm = method_raw.upper().replace("-", "_")
            inc_ts_field_raw = _p(param_data, "Incremental Timestamp Field")
            
            # tolerate both single- and double-space variants
            header_link = _p(param_data, "Incremental Header To Tables Link Field")
            if not header_link:
                header_link = _p(param_data, "Incremental Header To Tables  Link Field")

            schedule = _p(param_data, "Schedule")
            week_days = _p(param_data, "Week Days")
            dates_of_month = _p(param_data, "Dates Of Month")

        try:
            dataset_modified_by_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
        except Exception:
            dataset_modified_by_user = os.getenv("USER", "system")

        config_row = {
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            "source_database_name": db_name,
            "load_type": load_type,
            "is_enabled": is_enabled,
            "pet_dataset_id": pet_dataset_id,
            "incremental_timestamp_method": method_norm,
            "incremental_timestamp_field": inc_ts_field_raw,
            "incremental_header_to_tables_link_field": header_link,
            "schedule": schedule,
            "week_days": week_days,
            "dates_of_month": dates_of_month,
            "dataset_owner": dataset_owner,
            "modified_date": datetime.now(),
            "dataset_modified_by_user": dataset_modified_by_user,
            "last_run_time": None,
            "next_run_due_time": None,
            "last_process_datetime": merged_last_process_map,
            "stage": stage,
            "comments": comments,
        }


        config_schema = StructType(
            [
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
                StructField("stage", StringType(), True),
                StructField("comments", StringType(), True),
            ]
        )

        config_df = spark.createDataFrame([Row(**config_row)], schema=config_schema)
        config_df.createOrReplaceTempView("tmp_config")

        # 3) write path
        if overwrite:
            # delete + insert this dataset+stage
            spark.sql(
                f"""
                DELETE FROM pet.ctrl_dataset_config
                WHERE dataset_id = '{dataset_id}'
                  AND stage = '{stage}'
                """
            )
            spark.sql("INSERT INTO pet.ctrl_dataset_config SELECT * FROM tmp_config")
        else:
            # MERGE (upsert) to preserve other columns/rows
            spark.sql(f"""
                MERGE INTO pet.ctrl_dataset_config AS t
                USING tmp_config AS s
                ON  t.dataset_id = s.dataset_id AND t.stage = s.stage
                WHEN MATCHED THEN UPDATE SET
                      t.dataset_name                              = s.dataset_name,
                      t.source_database_name                      = s.source_database_name,
                      t.load_type                                 = s.load_type,
                      t.is_enabled                                = s.is_enabled,
                      t.pet_dataset_id                            = s.pet_dataset_id,
                      t.incremental_timestamp_method              = s.incremental_timestamp_method,
                      t.incremental_timestamp_field               = s.incremental_timestamp_field,
                      t.incremental_header_to_tables_link_field   = s.incremental_header_to_tables_link_field,
                      t.schedule                                  = s.schedule,
                      t.week_days                                 = s.week_days,
                      t.dates_of_month                            = s.dates_of_month,
                      t.dataset_owner                             = s.dataset_owner,
                      t.modified_date                             = s.modified_date,
                      t.dataset_modified_by_user                  = s.dataset_modified_by_user,
                      t.last_run_time                             = s.last_run_time,
                      t.next_run_due_time                         = s.next_run_due_time,
                      t.last_process_datetime                     = s.last_process_datetime,
                      t.comments                                  = s.comments
                WHEN NOT MATCHED THEN INSERT *
            """)

        spark.catalog.dropTempView("tmp_config")
        logger.info(f"Configuration written for dataset {dataset_id} (stage={stage}, overwrite={overwrite}).")
        return True

    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_config: {e}")
        raise


# COMMAND ----------

def write_input_fields_table(spark: SparkSession, df: pd.DataFrame, dataset_id: str, stage: str) -> None:
    """
    Write Input Schema rows to pet.ctrl_dataset_input_fields:
      (dataset_id, database_name, table_name, field_name, comments, stage)
    """
    try:
        data = df.copy()
        data["dataset_id"] = dataset_id
        data.rename(
            columns={
                "Database Name": "database_name",
                "Table Name": "table_name",
                "Column Name": "field_name",
            },
            inplace=True,
        )
        data["comments"] = None
        data["stage"] = stage

        spark_df = spark.createDataFrame(
            data[["dataset_id", "database_name", "table_name", "field_name", "comments", "stage"]]
        )
        spark_df.createOrReplaceTempView("tmp_input_fields")

        # delete + insert only this dataset+stage
        spark.sql(
            f"""
            DELETE FROM pet.ctrl_dataset_input_fields
            WHERE dataset_id = '{dataset_id}'
              AND stage = '{stage}'
            """
        )
        spark.sql(
            """
            INSERT INTO pet.ctrl_dataset_input_fields
              (dataset_id, database_name, table_name, field_name, comments, stage)
            SELECT dataset_id, database_name, table_name, field_name, comments, stage
            FROM tmp_input_fields
            """
        )

        spark.catalog.dropTempView("tmp_input_fields")
        logger.info(f"Input Schema written for dataset {dataset_id} (stage={stage}).")
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_input_fields: {e}")
        raise


# COMMAND ----------

# ------------------------------------------------------------------------------
# Loader class
# ------------------------------------------------------------------------------
class PETFileLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def LoadConfigurationsForDataset(
        self,
        dataset_id: str,
        source_file_path: str,
        stage: str,
        overwrite: bool = False,
    ) -> dict:
        results = {
            "dataset_id": dataset_id,
            "status": "FAILED",
            "errors": [],
            "config_written": False,
            "input_fields_written": False,
        }

        try:
            # 1) File sanity
            validate_file_format(source_file_path)

            # 2) Fetch into memory
            blob = fetch_xlsx_bytes(source_file_path)
            xls = open_excel(blob)

            # 3) Required sheets
            validate_required_sheets(xls.sheet_names)

            # 4) Spec (placeholder)
            try:
                spec_df = pd.read_excel(
                    xls,
                    sheet_name="Specification",
                    dtype=str,
                    keep_default_na=False,
                    engine="openpyxl"
                )
                spec_data = {}
                for k, v in zip(spec_df.iloc[:, 0], spec_df.iloc[:, 1]):
                    if k is None:
                        continue
                    key = str(k).strip()
                    if not key:
                        continue
                    val = None if v is None or str(v).strip() == "" else str(v).strip()
                    spec_data[key] = val
            except Exception:
                # Fallback: keep previous behaviour if the sheet is absent
                spec_data = {"Dataset Name": dataset_id}

            # 5) DPS-CDP Params -> dict
            param_data: Dict[str, Optional[str]] = {}
            param_df = pd.read_excel(
                xls,
                sheet_name="DPS-CDP Params",
                dtype=str,
                keep_default_na=False,
                engine="openpyxl"
            )
            for k, v in zip(param_df.iloc[:, 0], param_df.iloc[:, 1]):
                if k is None:
                    continue
                key = str(k).strip()
                if not key:
                    continue
                val = None if v is None else str(v).strip()
                if val == "":
                    val = None
                param_data[key] = val

            # 6) Enabled?
            validate_dataset_enabled(param_data, dataset_id)

            # 7) Database name
            db_name = validate_database_name(self.spark, param_data)

            # 8) Input Schema (raw) for candidate tables
            raw_input_df = pd.read_excel(
                xls,
                sheet_name="Input Schema",
                dtype=str,
                keep_default_na=False,
                engine="openpyxl"
            )
            if "Table Name" in raw_input_df.columns:
                candidate_tables = (
                    raw_input_df["Table Name"]
                    .dropna().astype(str).str.strip()
                    .replace("", pd.NA).dropna().unique().tolist()
                )
            else:
                candidate_tables = (
                    raw_input_df.iloc[:, 1]
                    .dropna().astype(str).str.strip()
                    .replace("", pd.NA).dropna().unique().tolist()
                )

            # 9) Validate Input Schema fully (per-row DB/table/column).
            cleaned_input_fields_df, unique_db_tables = validate_input_fields(
                self.spark, raw_input_df, db_name
            )

            # 10) DPS-CDP params (stage + load-type/method checks)
            validate_dps_cdp_params(
                self.spark, param_data, db_name, stage, input_schema_tables=unique_db_tables
            )

            # 11-12) Skip incremental processing for Data-Provisioning stage
            is_data_provisioning = stage.upper() == "DATA-PROVISIONING"

            
            if not is_data_provisioning:
                
                # Normalize/validate Incremental Timestamp Field
                normalized_ts = validate_and_normalize_incremental_timestamp_field(
                    self.spark, db_name, param_data.get("Incremental Timestamp Field"), candidate_tables=candidate_tables
                )
                param_data["Incremental Timestamp Field"] = normalized_ts
                
                # Generate last_process_map
                last_process_map = get_last_process_map(
                    self.spark, dataset_id,stage, param_data.get("Start Process From Date"), cleaned_input_fields_df
                )
            else:
                last_process_map = {}

            # 13) Write config
            config_written = write_config(
                self.spark,
                dataset_id,
                spec_data,
                param_data,
                db_name,
                last_process_map,
                overwrite,
                is_data_provisioning
            )
            results["config_written"] = bool(config_written)

            # 14) Write Input Schema
            stage_value = param_data.get("Stage") or stage
            write_input_fields_table(
                self.spark,
                cleaned_input_fields_df,
                dataset_id,
                stage_value
            )
            results["input_fields_written"] = True

            results["status"] = "SUCCESS"
            return results

        except Exception as e:
            logger.exception(f"Error during config load: {e}")
            results["errors"].append(str(e))
            return results

# ------------------------------------------------------------------------------
# Databricks widgets entrypoint
# ------------------------------------------------------------------------------
if "dbutils" in locals():
    dbutils.widgets.text("dataset_id", "")
    dbutils.widgets.text("source_file_path", "")
    dbutils.widgets.dropdown("stage", "Data-Provisioning", ["Data-Provisioning", "Domain-0-Transformation"])
    dbutils.widgets.dropdown("overwrite", "False", ["True", "False"])

    spark = SparkSession.builder.getOrCreate()
    dataset_id = dbutils.widgets.get("dataset_id")
    source_file_path = dbutils.widgets.get("source_file_path")
    stage = dbutils.widgets.get("stage")
    overwrite = dbutils.widgets.get("overwrite") == "True"

    loader = PETFileLoader(spark)
    result = loader.LoadConfigurationsForDataset(dataset_id, source_file_path, stage, overwrite)

    print(result)
    dbutils.notebook.exit(result)
