# Databricks notebook source
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from pyspark.sql import SparkSession

__all__ = [
    "validate_required_sheets",
    "validate_schedule_rules",
    "validate_file_format",
    "validate_dataset_enabled",
    "validate_external_id",
    "validate_dataset_name",
    "validate_database_name",
    "validate_input_fields",
    "validate_dps_cdp_params",
    "validate_and_normalize_incremental_timestamp_field",
    "get_last_process_map",
]

logger = logging.getLogger(__name__)


# ---------------------------
# Utilities
# ---------------------------

def _getp(d: Dict[str, object], key: str) -> Optional[str]:
    """
    Case/space-insensitive getter for workbook 'Params' keys.
    Returns a stripped string or None.
    """
    want = key.lower().replace(" ", "")
    for k, v in (d or {}).items():
        if str(k).lower().replace(" ", "") == want:
            return None if v is None else str(v).strip()
    return None


# ---------------------------
# Basic validators
# ---------------------------

def validate_required_sheets(sheetnames: list) -> bool:
    required_sheets = {"Specification", "Input Schema", "DPS-CDP Params"}
    missing = required_sheets - set(sheetnames or [])
    if missing:
        raise ValueError(f"Missing required sheets: {missing}")
    return True


def validate_schedule_rules(param_data: dict) -> bool:
    schedule = _getp(param_data, "Schedule")
    week_days = _getp(param_data, "Week Days")
    dates_of_month = _getp(param_data, "Dates Of Month")

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


def validate_file_format(source_file_path: str) -> bool:
    if not source_file_path.lower().endswith((".xlsx", ".xls")):
        raise ValueError(f"Invalid file format. Expected Excel file (.xlsx or .xls), got: {source_file_path}")
    return True


def validate_dataset_enabled(param_data: dict, dataset_id: str) -> None:
    val = (_getp(param_data, "Is Enabled") or "TRUE").upper()
    if val == "FALSE":
        raise Exception(f"Dataset {dataset_id} is disabled (Is Enabled = FALSE). Skipping load.")


def validate_external_id(spec_data: dict, dataset_id: str) -> None:
    if ((_getp(spec_data, "ExternalID") or "").upper()) != dataset_id.upper():
        raise ValueError("Dataset ID mismatch between param and file")


def validate_dataset_name(spec_data: dict, dataset_id: str) -> None:
    if ((_getp(spec_data, "Dataset Name") or "").upper()) != dataset_id.upper():
        raise ValueError("Dataset Name mismatch between param and file")


def validate_database_name(spark: SparkSession, param_data: dict) -> str:
    db_name = _getp(param_data, "Database Name")
    if not db_name:
        raise ValueError("Database Name missing")
    if not check_database_exists(spark, db_name):
        raise ValueError(f"Database '{db_name}' not found")
    return db_name


# ---------------------------
# Input Schema validation
# ---------------------------

def validate_input_fields(
    spark: SparkSession,
    input_fields_df: pd.DataFrame,
    default_db_name: str
) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """
    Validates 'Input Schema' (Database Name, Table Name, Column Name).
    - Blank Database Name falls back to DPS-CDP default_db_name
    - Validates database/table/column (supports struct with dot-notation)
    Returns:
      - cleaned DataFrame with the 3 columns
      - list of unique (db, table) tuples
    """
    if input_fields_df is None or input_fields_df.empty:
        raise ValueError("Input Schema sheet is empty.")

    df = input_fields_df.iloc[:, :3].copy().dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = {"Database Name", "Table Name", "Column Name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input Schema is missing required columns: {missing}")

    df["Database Name"] = df["Database Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")
    df["Table Name"]   = df["Table Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")
    df["Column Name"]  = df["Column Name"].apply(lambda x: str(x).strip() if pd.notnull(x) else "")

    cleaned_rows: List[dict] = []
    unique_db_tables: set = set()

    for idx, row in df.iterrows():
        db_name = row["Database Name"] or (default_db_name or "")
        table   = row["Table Name"]
        column  = row["Column Name"]
        row_no  = idx + 2  # excel header = 1

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
        unique_db_tables.add((db_name, table))

    cleaned_df = pd.DataFrame(cleaned_rows, columns=["Database Name", "Table Name", "Column Name"])
    return cleaned_df, list(unique_db_tables)


# ---------------------------
# DPS/CDP params validation
# ---------------------------

def validate_dps_cdp_params(
    spark: SparkSession,
    param_data: dict,
    db_name: str,
    expected_stage: str,
    input_schema_tables: Optional[List[Tuple[str, str]]] = None,
) -> bool:
    """
    Validate DPS-CDP Params sheet.

    - Ensures Stage, Load Type, Pet Dataset ID.
    - For INCREMENTAL: enforces schedule rules and verifies the timestamp field(s).
    - Supports table-qualified fields ('table.col' or 'table.struct.col') and struct paths ('a.b').
    """

    # ---------- helpers ----------
    def _list_tables_in_db() -> List[str]:
        rows = spark.sql(f"SHOW TABLES IN hive_metastore.{db_name}").collect()
        return [r.tableName for r in rows]

    def _validate_field_on_tables(field_spec: str, searched: List[str]) -> bool:
        """
        field_spec may be:
          - 'field' or 'struct.field'  → scan input_schema_tables
          - 'table.field' or 'table.struct.field' → validate on that table in db_name
        """
        if not field_spec:
            return False

        # table-qualified?
        if "." in field_spec:
            first, remainder = field_spec.split(".", 1)
            try:
                db_tables = _list_tables_in_db()
            except Exception as e:
                raise ValueError(f"Could not access database {db_name}: {e}")
            if any(t.lower() == first.lower() for t in db_tables):
                searched.append(f"{db_name}.{first}")
                return check_table_and_column_exist(spark, db_name, first, remainder)

        # not table-qualified → scan declared input schema tables
        for schema_db, schema_table in (input_schema_tables or []):
            searched.append(f"{schema_db}.{schema_table}")
            if check_table_and_column_exist(spark, schema_db, schema_table, field_spec):
                return True
        return False

    # ---------- required basics ----------
    stage = (_getp(param_data, "Stage") or "").upper()
    if not stage:
        raise ValueError("Stage is missing in DPS-CDP Params.")
    if stage != (expected_stage or "").upper():
        raise ValueError(f"Stage mismatch: expected '{expected_stage}', found '{stage}'")
    if stage not in {"DOMAIN-0-TRANSFORMATION", "DATA-PROVISIONING"}:
        raise ValueError("Stage must be 'Domain-0-Transformation' or 'Data-Provisioning'")

    load_type = (_getp(param_data, "Load Type") or "").upper()
    if load_type not in {"INCREMENTAL", "FULL"}:
        raise ValueError("Load Type must be 'INCREMENTAL' or 'FULL'")

    if not _getp(param_data, "Pet Dataset ID"):
        raise ValueError("Pet Dataset ID is required")

    # ---------- schedule + incremental fields ----------
    inc_ts_field = _getp(param_data, "Incremental Timestamp Field") or ""
    schedule       = _getp(param_data, "Schedule")
    week_days      = _getp(param_data, "Week Days")
    dates_of_month = _getp(param_data, "Dates Of Month")

    if load_type == "INCREMENTAL":
        if not inc_ts_field:
            raise ValueError("Incremental Timestamp Field must not be empty for INCREMENTAL load")
        validate_schedule_rules({
            "Schedule": schedule or None,
            "Week Days": week_days or None,
            "Dates Of Month": dates_of_month or None,
        })
    else:
        # FULL: incremental/schedule fields must be empty
        if inc_ts_field or week_days or dates_of_month or schedule:
            raise ValueError("For FULL load, incremental and schedule fields must be empty")

    # ---------- timestamp method ----------
    method = (_getp(param_data, "Incremental Timestamp Method") or "").upper().replace("-", "_")
    if method not in {"TIMESTAMP_HEADER_TABLE", "TIMESTAMP_DATA_TABLE"}:
        raise ValueError(
            "Incremental Timestamp Method must be 'TIMESTAMP_HEADER_TABLE' or 'TIMESTAMP_DATA_TABLE'"
        )

    if method == "TIMESTAMP_HEADER_TABLE":
        # accept both single- and double-space variants
        header_link = _getp(param_data, "Incremental Header To Tables Link Field") \
                      or _getp(param_data, "Incremental Header To Tables  Link Field")
        if not header_link:
            raise ValueError("Incremental Header To Tables Link Field is required for TIMESTAMP_HEADER_TABLE")

        searched: List[str] = []
        if not _validate_field_on_tables(header_link, searched):
            raise ValueError(
                f"TIMESTAMP_HEADER_TABLE field '{header_link}' not found. Searched tables: {searched}"
            )
    else:  # TIMESTAMP_DATA_TABLE
        searched: List[str] = []
        if not _validate_field_on_tables(inc_ts_field, searched):
            raise ValueError(
                f"Incremental Timestamp Field '{inc_ts_field}' not found. Searched tables: {searched}"
            )

    return True


# ---------------------------
# Incremental TS field (canonical: struct.field if nested, else field)
# ---------------------------

def validate_and_normalize_incremental_timestamp_field(
    spark: SparkSession,
    db_name: str,
    field_spec: Optional[str],
    candidate_tables: Optional[Iterable[str]] = None,
) -> str:
    """
    Validate the Incremental Timestamp Field and return a canonical value:
      • If the real column is nested → return 'struct.field'
      • If it's a top-level column   → return 'field'
    Never includes a table prefix in the return value.

    Accepts any of:
      - "field"
      - "struct.field"
      - "table.field"
      - "table.struct.field"
    """
    if not field_spec or not str(field_spec).strip():
        raise ValueError("Incremental Timestamp Field is missing")

    spec = str(field_spec).strip()
    searched: List[str] = []

    # tables to search
    try:
        if candidate_tables:
            tables = list(candidate_tables)
        else:
            rows = spark.sql(f"SHOW TABLES IN hive_metastore.{db_name}").collect()
            tables = [r.tableName for r in rows]
    except Exception as e:
        raise ValueError(f"Could not access database {db_name}: {e}")

    # helpers
    def _schema_for(table: str):
        try:
            return spark.table(f"{db_name}.{table}").schema
        except Exception:
            return None

    def _has_top_level(schema, col: str) -> bool:
        col_l = col.lower()
        return any(f.name.lower() == col_l for f in (schema.fields if schema else []))

    def _structs_with_leaf(schema, leaf: str) -> List[str]:
        hits: List[str] = []
        if not schema:
            return hits
        leaf_l = leaf.lower()
        for f in schema.fields:
            dt = getattr(f, "dataType", None)
            if hasattr(dt, "fields"):  # StructType
                if any(nf.name.lower() == leaf_l for nf in dt.fields):
                    hits.append(f.name)
        return hits

    def _has_struct_path(schema, path: str) -> bool:
        if "." not in path or not schema:
            return False
        struct, leaf = path.split(".", 1)
        s_l, l_l = struct.lower(), leaf.lower()
        for f in schema.fields:
            if f.name.lower() == s_l and hasattr(f.dataType, "fields"):
                return any(nf.name.lower() == l_l for nf in f.dataType.fields)
        return False

    # Case A: table-qualified in spec → validate only on that table
    if "." in spec:
        first, remainder = spec.split(".", 1)
        match = next((t for t in tables if t.lower() == first.lower()), None)
        if match:
            searched.append(f"{db_name}.{match}")
            sch = _schema_for(match)
            if "." in remainder:
                # table.struct.field
                if _has_struct_path(sch, remainder):
                    return remainder  # keep struct.field
            else:
                # table.field
                if _has_top_level(sch, remainder):
                    return remainder  # plain field
                structs = _structs_with_leaf(sch, remainder)
                if len(structs) == 1:
                    return f"{structs[0]}.{remainder}"  # resolve to struct.field
                if len(structs) > 1:
                    options = [f"{s}.{remainder}" for s in structs]
                    raise ValueError(
                        f"Ambiguous leaf '{remainder}' in {db_name}.{match}; "
                        f"candidates: {options}. Please specify one in the sheet."
                    )
            raise ValueError(f"Not found → {db_name}.{match}.{remainder}")

    # Case B: not table-qualified (spec is 'field' or 'struct.field')
    if "." in spec:
        # struct.field → confirm it exists in at least one table
        struct_path = spec
        for t in tables:
            searched.append(f"{db_name}.{t}")
            if _has_struct_path(_schema_for(t), struct_path):
                return struct_path
        raise ValueError(f"Not found → {db_name}.{struct_path}. Scanned tables: {searched}")

    # leaf-only → try top-level first across all tables
    top_hits: List[str] = []
    nested_hits: List[str] = []  # candidate struct paths

    for t in tables:
        searched.append(f"{db_name}.{t}")
        sch = _schema_for(t)
        if _has_top_level(sch, spec):
            top_hits.append(t)
        else:
            nested_hits.extend([f"{s}.{spec}" for s in _structs_with_leaf(sch, spec)])

    if top_hits:
        return spec  # plain field exists somewhere → keep as field

    nested_paths = sorted(set(nested_hits))
    if len(nested_paths) == 1:
        return nested_paths[0]  # single struct path → use 'struct.field'
    if len(nested_paths) > 1:
        raise ValueError(
            f"Ambiguous leaf '{spec}' across tables in {db_name}; "
            f"candidates: {nested_paths}. Please specify one in the sheet."
        )

    raise ValueError(f"Not found → {db_name}.{spec}. Scanned tables: {searched}")


# ---------------------------
# Last-process datetime map
# ---------------------------

def get_last_process_map(
    spark: SparkSession,
    dataset_id: str,
    stage: str,
    start_process_date: Optional[str],
    input_fields_df: pd.DataFrame
) -> dict:
    """
    Merge existing last_process_datetime map with initial dates for tables that
    don't have an entry yet (when 'Start Process From Date' is provided).
    The lookup is dataset_id + stage.
    """
    try:
        existing_map: Dict[str, datetime] = {}
        existing_df = spark.sql(
            f"""
            SELECT last_process_datetime
            FROM pet.ctrl_dataset_config
            WHERE dataset_id = '{dataset_id}' AND stage = '{stage}'
            """
        )
        if existing_df.count() > 0:
            existing_map = existing_df.collect()[0].asDict().get("last_process_datetime") or {}

        tables = input_fields_df["Table Name"].dropna().unique()
        merged_map = dict(existing_map)

        if start_process_date:
            # support "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DD"
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
