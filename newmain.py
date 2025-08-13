# Databricks notebook source
from __future__ import annotations

import io
import os
import re
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Any, Union

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import col
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

# ------------------------------------------------------------------------------
# Metastore / table / column helpers
#  - canonical DB resolver fixes case/casing mismatches (DIDS vs dids)
#  - nested path resolution with manual walk + Spark analyzer fallback
# ------------------------------------------------------------------------------
def _resolve_db(spark: SparkSession, db: str) -> str:
    """
    Return canonical metastore schema name with exact casing.
    Supports 'schema' or 'catalog.schema'. If not found, return input.
    """
    parts = [p for p in db.split(".") if p]
    if not parts:
        return db

    # Databricks SHOW DATABASES lists schemas in current catalog
    try:
        dbs = [r.databaseName for r in spark.sql("SHOW DATABASES").collect()]
    except Exception:
        dbs = []

    if len(parts) == 1:
        schema = parts[0]
        return next((d for d in dbs if d.lower() == schema.lower()), schema)

    if len(parts) >= 2:
        catalog, schema = parts[0], parts[1]
        canon_schema = next((d for d in dbs if d.lower() == schema.lower()), schema)
        return f"{catalog}.{canon_schema}"


def _q(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _qualify(spark: SparkSession, db: str, tbl: str) -> str:
    canon_db = _resolve_db(spark, db)
    parts = [p for p in canon_db.split(".") if p]
    if len(parts) == 1:
        return f"{_q(parts[0])}.{_q(tbl)}"
    if len(parts) == 2:
        return f"{_q(parts[0])}.{_q(parts[1])}.{_q(tbl)}"
    return ".".join([_q(p) for p in parts] + [_q(tbl)])


def _show_tables_in(spark: SparkSession, db: str, like: str):
    canon_db = _resolve_db(spark, db)
    parts = [p for p in canon_db.split(".") if p]
    like_ = like.replace("`", "``")
    if len(parts) == 1:
        return spark.sql(f"SHOW TABLES IN {_q(parts[0])} LIKE '{like_}'")
    elif len(parts) == 2:
        return spark.sql(f"SHOW TABLES IN {_q(parts[0])}.{_q(parts[1])} LIKE '{like_}'")
    return None


def _table_exists(spark: SparkSession, db: str, tbl: str) -> bool:
    try:
        df = _show_tables_in(spark, db, tbl)
        if df is not None:
            return df.limit(1).count() > 0
    except Exception:
        pass
    # Fallback: listTables in the schema part
    try:
        canon_db = _resolve_db(spark, db)
        schema = canon_db.split(".")[-1]
        return any(t.name.lower() == tbl.lower() for t in spark.catalog.listTables(schema))
    except Exception:
        return False


def _canonicalize_path(schema: StructType, path: str) -> Optional[str]:
    """
    Return the exact-cased dotted path found in schema (case-insensitive).
    E.g., 'meta.event_received_ts' -> 'META.EVENT_RECEIVED_TS'
    """
    parts = [p.strip() for p in path.split(".") if p.strip()]
    cur = schema
    resolved: List[str] = []
    for p in parts:
        if not isinstance(cur, StructType):
            return None
        fld = next((f for f in cur.fields if f.name.lower() == p.lower()), None)
        if fld is None:
            return None
        resolved.append(fld.name)
        cur = fld.dataType
    return ".".join(resolved)


def _get_schema_debug_info(schema: StructType, prefix: str = "") -> List[str]:
    """
    Get a list of all available column paths in the schema for debugging.
    """
    columns = []
    for field in schema.fields:
        current_path = f"{prefix}.{field.name}" if prefix else field.name
        columns.append(current_path)
        
        # If it's a struct, recursively get nested fields
        if hasattr(field.dataType, 'fields'):  # StructType
            nested = _get_schema_debug_info(field.dataType, current_path)
            columns.extend(nested)
    
    return columns


def _quote_path_for_select(path: str) -> str:
    # `META`.`EVENT_RECEIVED_TS`
    return "`.`".join([p.replace("`", "``") for p in path.split(".") if p])


def check_table_and_column_exist(spark: SparkSession, db_name: str, table_name: str, column_name: str) -> bool:
    try:
        # Check if table exists first
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
            if not tables_df.filter(col("tableName") == table_name).count():
                logger.warning(f"Table '{table_name}' not found in database '{db_name}'")
                return False
        except Exception as e:
            logger.error(f"Failed to list tables in database '{db_name}': {e}")
            raise ValueError(f"Cannot access database '{db_name}': {e}")
        
        # Get table schema with better error handling
        try:
            full_table_name = f"{db_name}.{table_name}"
            logger.info(f"Accessing table schema for: {full_table_name}")
            df_schema = spark.table(full_table_name).schema
            logger.info(f"Successfully retrieved schema for {full_table_name}")
        except Exception as e:
            error_msg = str(e)
            if "table" in error_msg.lower() and ("not found" in error_msg.lower() or "does not exist" in error_msg.lower()):
                logger.warning(f"Table '{full_table_name}' does not exist or is not accessible")
                return False
            elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                raise ValueError(f"Permission denied accessing table '{full_table_name}': {e}")
            elif "o" in error_msg and ".table" in error_msg:
                # Handle Spark internal errors like "o337.table"
                raise ValueError(f"Spark internal error accessing table '{full_table_name}'. This may indicate table corruption, permission issues, or Delta table problems: {e}")
            else:
                raise ValueError(f"Failed to access table schema for '{full_table_name}': {e}")
        
        
        # Check for direct column match first (case-insensitive)
        for field in df_schema.fields:
            if field.name.lower() == column_name.lower():
                return True
        
        # Check for struct field (e.g., meta.event_time)
        if "." in column_name:
            parts = column_name.split(".", 1)  # Split only on first dot
            struct_name = parts[0].lower()
            nested_field = parts[1].lower()
            
            # Find the struct field (case-insensitive)
            for field in df_schema.fields:
                if field.name.lower() == struct_name:
                    # Check if it's a struct type and contains the nested field
                    if hasattr(field.dataType, 'fields'):  # StructType
                        found_nested = any(nested_f.name.lower() == nested_field for nested_f in field.dataType.fields)
                        if found_nested:
                            return True
                    break
        
        return False
    except Exception as e:
        logger.exception(f"Table/Column check failed: {e}")
        raise

# ------------------------------------------------------------------------------
# Validators
# ------------------------------------------------------------------------------
def validate_required_sheets(sheetnames: list) -> bool:
    required_sheets = {"Specification", "Input Schema", "DPS-CDP Params"}
    missing = required_sheets - set(sheetnames)
    if missing:
        raise ValueError(f"Missing required sheets: {missing}")
    return True


def validate_schedule_rules(param_data: dict) -> bool:
    schedule = param_data.get("Schedule")
    week_days = param_data.get("Week Days")
    dates_of_month = param_data.get("Dates Of Month")

    if schedule not in ("DAILY", "WEEKLY", "MONTHLY"):
        raise ValueError(f"Invalid schedule '{schedule}'. Must be DAILY, WEEKLY, or MONTHLY.")

    if schedule == "DAILY":
        if week_days or dates_of_month:
            raise ValueError("For DAILY schedule, 'Week Days' and 'Dates Of Month' must be empty.")
    elif schedule == "WEEKLY":
        if not week_days:
            raise ValueError("For WEEKLY schedule, 'Week Days' must not be empty.")
        if dates_of_month:
            raise ValueError("For WEEKLY schedule, 'Dates Of Month' must be empty.")
    elif schedule == "MONTHLY":
        if not dates_of_month:
            raise ValueError("For MONTHLY schedule, 'Dates Of Month' must not be empty.")
        if week_days:
            raise ValueError("For MONTHLY schedule, 'Week Days' must be empty.")

    logger.info(f"Schedule validation passed for '{schedule}'")
    return True


def validate_dps_cdp_params(spark: SparkSession, param_data: dict, db_name: str, expected_stage: str, 
                          input_schema_tables: Optional[List[Tuple[str, str]]] = None) -> bool:
    load_type = (param_data.get("Load Type") or "").upper()

    # Stage validation
    stage = (param_data.get("Stage") or "").upper()
    if not stage:
        raise ValueError("Stage is missing in DPS-CDP Params sheet.")
    if stage != expected_stage.upper():
        raise ValueError(f"Stage mismatch: expected '{expected_stage}', found '{stage}'")
    if stage not in {"DOMAIN-0-TRANSFORMATION", "DATA-PROVISIONING"}:
        raise ValueError("Stage must be 'DOMAIN-0-TRANSFORMATION' or 'DATA-PROVISIONING'")

    if load_type not in {"INCREMENTAL", "FULL"}:
        raise ValueError("Load Type must be 'INCREMENTAL' or 'FULL'")

    if load_type == "INCREMENTAL":
        if not param_data.get("Incremental Timestamp Field"):
            raise ValueError("Incremental Timestamp Field must not be empty for INCREMENTAL load")
        validate_schedule_rules(param_data)
    else:
        # FULL should not have incremental/schedule params
        if (
            param_data.get("Incremental Timestamp Field")
            or param_data.get("Incremental Header To Tables  Link Field")
            or param_data.get("Schedule")
            or param_data.get("Week Days")
            or param_data.get("Dates Of Month")
        ):
            raise ValueError("For FULL load, incremental fields and schedule parameters must be empty")

    if not param_data.get("Pet Dataset ID"):
        raise ValueError("Pet Dataset ID is required")

    method = (param_data.get("Incremental Timestamp Method") or "").upper()
    if method not in {"TIMESTAMP_HEADER_TABLE", "TIMESTAMP_DATA_TABLE"}:
        raise ValueError("Incremental Timestamp Method must be 'TIMESTAMP_HEADER_TABLE' or 'TIMESTAMP_DATA_TABLE'")

    if method == "TIMESTAMP_HEADER_TABLE":
        value = param_data.get("Incremental Header To Tables  Link Field")
        if not value:
            raise ValueError("Incremental Header To Tables Link Field is required for TIMESTAMP_HEADER_TABLE method")
        
        field_name = value.strip()
        
        # Search for the field in Input Schema tables only
        found = False
        found_location = None
        for schema_db, schema_table in input_schema_tables:
            if check_table_and_column_exist(spark, schema_db, schema_table, field_name):
                found = True
                found_location = f"{schema_db}.{schema_table}"
                break
        
        if not found:
            available_tables = [f"{db}.{tbl}" for db, tbl in input_schema_tables]
            raise ValueError(f"TIMESTAMP_HEADER_TABLE field '{field_name}' not found in any Input Schema tables. "
                           f"Searched tables: {available_tables}")
        else:
            logger.info(f"TIMESTAMP_HEADER_TABLE field '{field_name}' found in {found_location}")

    return True


def check_database_exists(spark: SparkSession, db_name: str) -> bool:
    canon = _resolve_db(spark, db_name)
    return canon.lower() in [row.databaseName.lower() for row in spark.sql("SHOW DATABASES").collect()]


def validate_incremental_timestamp_field(spark: SparkSession, db_name: str, field_spec: Optional[str]) -> None:
    if not field_spec or "." not in field_spec.strip():
        raise ValueError(
            "Incremental Timestamp Field must include the table, e.g. 'table.col' or 'table.struct.inner'."
        )
    table, column_path = field_spec.strip().split(".", 1)
    table = table.strip()
    column_path = column_path.strip()

    ok = check_table_and_column_exist(spark, db_name, table, column_path)
    if not ok:
        raise ValueError(f"Not found → {db_name}.{table}.{column_path}")


def get_last_process_map(
    spark: SparkSession, dataset_id: str, start_process_date: Optional[str], input_fields_df: pd.DataFrame
) -> dict:
    try:
        existing_map = {}
        existing_df = spark.sql(
            f"SELECT last_process_datetime FROM pet.ctrl_dataset_config WHERE dataset_id = '{dataset_id}'"
        )
        if existing_df.count() > 0:
            existing_map = existing_df.collect()[0].asDict().get("last_process_datetime") or {}

        tables = input_fields_df["Table Name"].dropna().unique()
        merged_map = dict(existing_map)

        if start_process_date:
            try:
                init_date = datetime.strptime(start_process_date.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                init_date = datetime.strptime(start_process_date.strip(), "%Y-%m-%d")

            for table in tables:
                if table not in merged_map:
                    merged_map[table] = init_date
        else:
            if not existing_map:
                raise ValueError(f"'Start Process From Date' is required for first-time run of dataset {dataset_id}")

        return merged_map
    except Exception as e:
        logger.exception(f"Failed to compute last_process_map for dataset {dataset_id}: {e}")
        raise


def validate_external_id(spec_data: dict, dataset_id: str) -> None:
    if (spec_data.get("ExternalID") or "").upper() != dataset_id.upper():
        raise ValueError("Dataset ID mismatch between param and file")


def validate_dataset_name(spec_data: dict, dataset_id: str) -> None:
    if (spec_data.get("Dataset Name") or "").upper() != dataset_id.upper():
        raise ValueError("Dataset Name mismatch between param and file")


def validate_file_format(source_file_path: str) -> bool:
    if not source_file_path.lower().endswith((".xlsx", ".xls")):
        raise ValueError(f"Invalid file format. Expected Excel file (.xlsx or .xls), got: {source_file_path}")
    return True


def validate_dataset_enabled(param_data: dict, dataset_id: str) -> None:
    val = (param_data.get("Is Enabled") or "TRUE").strip().upper()
    if val == "FALSE":
        raise Exception(f"Dataset {dataset_id} is disabled (Is Enabled = FALSE). Skipping load.")


def validate_database_name(spark: SparkSession, param_data: dict) -> str:
    db_name = param_data.get("Database Name")
    if not db_name:
        raise ValueError("Database Name missing")
    if not check_database_exists(spark, db_name):
        raise ValueError(f"Database '{db_name}' not found")
    return db_name


def validate_input_fields(spark: SparkSession, input_fields_df: pd.DataFrame, default_db_name: str) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """
    Validates the 'Input Schema' sheet which contains:
      - 'Database Name'
      - 'Table Name'
      - 'Column Name'
    For each row:
      * if Database Name is blank, falls back to default_db_name (from DPS-CDP Params)
      * validates database/table/column (supports struct access via dot notation)
    Returns:
      - cleaned DataFrame with exactly those three columns
      - list of unique (database_name, table_name) tuples for later use
    """
    if input_fields_df is None or input_fields_df.empty:
        raise ValueError("Input Schema sheet is empty.")

    # Only keep first 3 columns; drop fully empty rows
    df = input_fields_df.iloc[:, :3].copy().dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = {"Database Name", "Table Name", "Column Name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input Schema is missing required columns: {missing}")

    # Normalize cells
    df["Database Name"] = df["Database Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")
    df["Table Name"] = df["Table Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")
    df["Column Name"] = df["Column Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")

    cleaned_rows = []
    unique_db_tables = set()  # Track unique (database, table) combinations
    
    for idx, row in df.iterrows():
        # Fallback to DPS-CDP 'Database Name' if blank
        db_name = row["Database Name"] or (default_db_name or "")
        table = row["Table Name"]
        column = row["Column Name"]

        row_no = idx + 2  # header is row 1 in Excel
        if not db_name:
            raise ValueError(f"[Input Schema] Row {row_no}: 'Database Name' is empty and no default provided.")
        if not table:
            raise ValueError(f"[Input Schema] Row {row_no}: 'Table Name' is empty.")
        if not column:
            raise ValueError(f"[Input Schema] Row {row_no}: 'Column Name' is empty.")

        if not check_database_exists(spark, db_name):
            raise ValueError(f"[Input Schema] Row {row_no}: Database '{db_name}' not found.")

        if not check_table_and_column_exist(spark, db_name, table, column):
            raise ValueError(f"[Input Schema] Row {row_no}: Not found → {db_name}.{table}.{column}")

        cleaned_rows.append({"Database Name": db_name, "Table Name": table, "Column Name": column})
        unique_db_tables.add((db_name, table))  # Store unique database/table combination

    cleaned_df = pd.DataFrame(cleaned_rows, columns=["Database Name", "Table Name", "Column Name"])
    unique_db_tables_list = list(unique_db_tables)
    
    logger.info(f"Found {len(unique_db_tables_list)} unique database/table combinations: {unique_db_tables_list}")
    
    return cleaned_df, unique_db_tables_list


# ------------------------------------------------------------------------------
# Writers
# ------------------------------------------------------------------------------
def write_config(
    spark: SparkSession,
    dataset_id: str,
    spec_data: dict,
    param_data: dict,
    db_name: str,
    last_process_map: dict,
    overwrite: bool,
) -> bool:
    """
    Write to pet.ctrl_dataset_config; merge last_process_datetime if dataset_id+stage exists.
    """
    try:
        stage = param_data.get("Stage")

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

        # 2) row payload
        config_row = {
            "dataset_id": dataset_id,
            "dataset_name": spec_data.get("Dataset Name"),
            "source_database_name": db_name,
            "load_type": param_data.get("Load Type"),
            "is_enabled": True,
            "pet_dataset_id": param_data.get("Pet Dataset ID"),
            "incremental_timestamp_method": param_data.get("Incremental Timestamp Method"),
            "incremental_timestamp_field": param_data.get("Incremental Timestamp Field"),
            "incremental_header_to_tables_link_field": param_data.get(
                "Incremental Header To Tables  Link Field"
            ),
            "schedule": param_data.get("Schedule"),
            "week_days": param_data.get("Week Days"),
            "dates_of_month": param_data.get("Dates Of Month"),
            "dataset_owner": param_data.get("Dataset Owner"),
            "modified_date": datetime.now(),
            "dataset_modified_by_user": os.getenv("USER", "system"),
            "last_run_time": None,
            "next_run_due_time": None,
            "last_process_datetime": merged_last_process_map,
            "stage": stage,
            "comments": param_data.get("Comments"),
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

        # 3) delete + insert only the dataset_id + stage
        spark.sql(
            f"""
            DELETE FROM pet.ctrl_dataset_config
            WHERE dataset_id = '{dataset_id}'
              AND stage = '{stage}'
            """
        )
        spark.sql("INSERT INTO pet.ctrl_dataset_config SELECT * FROM tmp_config")

        logger.info(f"Configuration written for dataset {dataset_id} (stage={stage}).")
        return True

    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_config: {e}")
        raise


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

        logger.info(f"Input Schema written for dataset {dataset_id} (stage={stage}).")
    except Exception as e:
        logger.exception(f"Failed to write to pet.ctrl_dataset_input_fields: {e}")
        raise


# ------------------------------------------------------------------------------
# Incremental Timestamp normalizer (accepts unqualified form)
# ------------------------------------------------------------------------------
def validate_and_normalize_incremental_timestamp_field(
    spark: SparkSession,
    db_name: str,
    field_spec: Optional[str],
    candidate_tables: Optional[Iterable[str]] = None,
) -> str:
    """
    Validate incremental timestamp field by searching ALL tables in the database.
    This matches the original working logic from validators.py.
    """
    if not field_spec or not str(field_spec).strip():
        raise ValueError("Incremental Timestamp Field is missing")

    fs = str(field_spec).strip()
    
    # Always search ALL tables for the field (like the original working logic)
    try:
        tables = spark.sql(f"SHOW TABLES IN {db_name}").collect()
    except Exception as e:
        raise ValueError(f"Could not access database {db_name}: {e}")
    
    found = False
    scanned_tables = []
    found_table = None
    
    for table_row in tables:
        table_name = table_row.tableName
        scanned_tables.append(table_name)
        
        if check_table_and_column_exist(spark, db_name, table_name, fs):
            found = True
            found_table = table_name
            break
    
    if not found:
        raise ValueError(f"Not found → {db_name}.{fs} Scanned tables: {scanned_tables}")
    
    # Return in table.field format
    return f"{found_table}.{fs}"

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
            spec_data = {"Dataset Name": dataset_id}

            # 5) DPS-CDP Params -> dict
            param_data: Dict[str, Optional[str]] = {}
            param_df = pd.read_excel(
                xls, sheet_name="DPS-CDP Params", dtype=str, keep_default_na=False, engine="openpyxl"
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
                xls, sheet_name="Input Schema", dtype=str, keep_default_na=False, engine="openpyxl"
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

            # 9) Validate Input Schema fully (per-row DB/table/column) and collect unique DB/table combinations
            cleaned_input_fields_df, unique_db_tables = validate_input_fields(self.spark, raw_input_df, db_name)

            # 10) DPS-CDP params (stage + load-type/method checks) with optimized table scope
            validate_dps_cdp_params(self.spark, param_data, db_name, stage, input_schema_tables=unique_db_tables)

            # 11) Normalize/validate Incremental Timestamp Field
            normalized_ts = validate_and_normalize_incremental_timestamp_field(
                self.spark, db_name, param_data.get("Incremental Timestamp Field"), candidate_tables=candidate_tables
            )
            param_data["Incremental Timestamp Field"] = normalized_ts

            # 12) last_process_map
            last_process_map = get_last_process_map(
                self.spark, dataset_id, param_data.get("Start Process From Date"), cleaned_input_fields_df
            )

            # 13) Write config
            config_written = write_config(
                self.spark, dataset_id, spec_data, param_data, db_name, last_process_map, overwrite
            )
            results["config_written"] = bool(config_written)

            # 14) Write Input Schema
            stage_value = param_data.get("Stage") or stage
            write_input_fields_table(self.spark, cleaned_input_fields_df, dataset_id, stage_value)
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

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
loader = PETFileLoader(spark)

loader.LoadConfigurationsForDataset(
    dataset_id="DPS_DIDS",
    source_file_path="s3://nhsd-dspp-core-ref-pet-target/config/DIDS_Dataset_Spec_Pass1.xlsx", 
  stage="Domain-0-Transformation",
     # Must be accessible in DBFS
    overwrite=True
)
