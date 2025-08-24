# pet_ctrl_objects_ddl.py
# Non-UC (hive_metastore) & DBR 10.4-safe preflight/bootstrap using your helpers.
from typing import List, Dict, Optional
import re
from pyspark.sql import SparkSession
from dsp.common.spark_helpers import create_asset, database_exists, table_exists

# -----------------------
# Internal helper methods
# -----------------------

def _ensure_database(spark: SparkSession, db_name: str, asset_group: str) -> None:
    """Ensure DB exists at external storage via your helper (prevents DBFS perms issues)."""
    if not database_exists(spark, db_name):
        print(f"[preflight] creating database {db_name} via create_asset('{asset_group}', '{db_name}', force_bucket=True)")
        create_asset(spark, asset_group, db_name, force_bucket=True)

def _sql_ident(s: str) -> str:
    s = str(s)
    return s if (s.startswith("`") and s.endswith("`")) else f"`{s.replace('`','``')}`"

def _escape_sql_literal(s: object) -> str:
    """Escape single quotes for SQL literal usage."""
    return str(s).replace("'", "''")

def _col_ddl(cols: List[Dict]) -> str:
    """
    DBR 10.4-safe column DDL.
    columns: [{name: str, type: str, nullable: bool=True, comment: str?}, ...]
    NOTE: 'type' can include full SQL (e.g., "BIGINT GENERATED ALWAYS AS IDENTITY (...)").
    """
    parts = []
    for c in cols:
        name = _sql_ident(c["name"])
        ctype = c.get("type", "STRING").strip()
        line = f"{name} {ctype}"
        if c.get("nullable") is False:
            line += " NOT NULL"
        if c.get("comment"):
            comment_txt = _escape_sql_literal(c["comment"])  # pre-escape to avoid backslashes in f-string
            line += f" COMMENT '{comment_txt}'"
        parts.append(line)
    return ",\n        ".join(parts)

def _props(props: Optional[Dict[str, str]]) -> str:
    """Keep properties minimal for 10.4 compatibility."""
    if not props:
        return ""
    rows = []
    for k, v in props.items():
        k_esc = _escape_sql_literal(k)  # pre-escape
        v_esc = _escape_sql_literal(v)
        rows.append(f"'{k_esc}' = '{v_esc}'")
    return "TBLPROPERTIES (\n      " + ",\n      ".join(rows) + "\n    )"

def _has_identity(cols: List[Dict]) -> bool:
    for c in cols:
        t = (c.get("type") or "").upper()
        if "GENERATED" in t and "IDENTITY" in t:
            return True
    return False

_IDENTITY_CLAUSE_RE = re.compile(r"\s+GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*\([^)]*\)", re.IGNORECASE)

def _strip_identity_from_type(t: str) -> str:
    """Remove 'GENERATED ALWAYS AS IDENTITY (...)' from a type string."""
    return _IDENTITY_CLAUSE_RE.sub("", t).strip()

def _strip_identity_from_columns(cols: List[Dict]) -> List[Dict]:
    """Return a deep-ish copy of columns with identity clause removed from any 'type' fields."""
    out = []
    for c in cols:
        c2 = dict(c)
        if "type" in c2 and isinstance(c2["type"], str):
            c2["type"] = _strip_identity_from_type(c2["type"])
        out.append(c2)
    return out

def _create_table_sql(db: str, tbl: str, columns: List[Dict], partition_by: Optional[List[str]], tblproperties: Optional[Dict[str, str]]) -> str:
    part = ""
    if partition_by:
        part_cols = ", ".join(_sql_ident(p) for p in partition_by)
        part = f"\n    PARTITIONED BY ({part_cols})"
    ddl = f"""
    CREATE TABLE {db}.{tbl} (
        {_col_ddl(columns)}
    )
    USING DELTA{part}
    {_props(tblproperties)}
    """
    return ddl

def _create_if_missing(
    spark: SparkSession,
    db: str,
    tbl: str,
    columns: List[Dict],
    partition_by: Optional[List[str]] = None,
    tblproperties: Optional[Dict[str, str]] = None,
) -> None:
    """Create table only if missing (idempotent). Tries identity; falls back if unsupported."""
    full = f"{db}.{tbl}"
    if table_exists(spark, full):
        print(f"[preflight] {full} exists")
        return

    # Attempt with identity (if present).
    ddl_with_identity = _create_table_sql(db, tbl, columns, partition_by, tblproperties)
    print(f"[preflight] creating {full}")
    print(ddl_with_identity)
    try:
        spark.sql(ddl_with_identity)
        return
    except Exception as e:
        # If identity not supported, retry without identity clause
        if _has_identity(columns):
            print(f"[preflight][warning] Identity columns may not be supported on this runtime/catalog. "
                  f"Retrying creation of {full} without IDENTITY. Error was: {e}")
            cols_no_ident = _strip_identity_from_columns(columns)
            ddl_no_identity = _create_table_sql(db, tbl, cols_no_ident, partition_by, tblproperties)
            print(ddl_no_identity)
            spark.sql(ddl_no_identity)
        else:
            # Reraise if no identity present; it's some other error
            raise

def _describe_location(spark: SparkSession, full: str) -> str:
    """Return the storage location of a Delta table (forces metadata resolution)."""
    try:
        return spark.sql(f"DESCRIBE DETAIL {full}").select("location").first()[0]
    except Exception as e:
        raise RuntimeError(f"[preflight] Unable to DESCRIBE DETAIL {full}: {e}")

# -----------------------
# Public API
# -----------------------

def ensure_db_tables_and_verify_locations(
    spark: SparkSession,
    *,
    db_name: str,
    asset_group: str,
    tables: List[Dict],
    workspace_bucket_hint: Optional[str] = "databricks",  # substring to detect workspace bucket
) -> None:
    """
    Ensure the database & Delta tables exist at external storage and verify each table location.
    'tables' is a list of dicts: {"name", "columns", "partition_by"?, "tblproperties"?}
    """
    _ensure_database(spark, db_name, asset_group)

    for t in tables:
        _create_if_missing(
            spark,
            db=db_name,
            tbl=t["name"],
            columns=t["columns"],
            partition_by=t.get("partition_by"),
            tblproperties=t.get("tblproperties"),
        )
        full = f"{db_name}.{t['name']}"
        loc = _describe_location(spark, full)
        print(f"[preflight] {full} location: {loc}")
        # If the location looks like DBFS/workspace bucket, warn clearly
        if loc.startswith("dbfs:/") or (workspace_bucket_hint and workspace_bucket_hint in loc):
            raise RuntimeError(
                f"[preflight] {full} is stored at '{loc}', which is likely the workspace bucket/DBFS. "
                "Your cluster role usually cannot read that, causing the DeltaLog error. "
                "Drop & re-create the table so it lives under the external database location created by create_asset()."
            )

def drop_and_recreate_tables(
    spark: SparkSession,
    *,
    db_name: str,
    tables: List[Dict],
) -> None:
    """Use if a table is on DBFS/workspace bucket; re-creates under the DB's external location."""
    for t in tables:
        full = f"{db_name}.{t['name']}"
        print(f"[preflight] dropping {full}")
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        _create_if_missing(
            spark,
            db=db_name,
            tbl=t["name"],
            columns=t["columns"],
            partition_by=t.get("partition_by"),
            tblproperties=t.get("tblproperties"),
        )

# -----------------------
# Example usage (edit to your needs)
# -----------------------
if __name__ == "__main__":
    spark = spark

    DB_NAME = "pet"            # pass your database name
    ASSET_GROUP = "curated" # the asset group your create_asset(...) expects

    PET_TABLES = [
        {
            "name": "ctrl_dataset_config",
            "columns": [
                {"name": "dataset_id", "type": "STRING", "nullable": False, "comment": "Lookup from config."},
                {"name": "dataset_name", "type": "STRING", "nullable": False},
                {"name": "source_database_name", "type": "STRING", "nullable": False},
                {"name": "load_type", "type": "STRING"},
                {"name": "is_enabled", "type": "BOOLEAN"},
                {"name": "pet_dataset_id", "type": "STRING", "nullable": False},
                {"name": "incremental_timestamp_method", "type": "STRING"},
                {"name": "incremental_timestamp_field", "type": "STRING"},
                {"name": "incremental_header_to_tables_link_field", "type": "STRING"},
                {"name": "schedule", "type": "STRING"},
                {"name": "week_days", "type": "STRING"},
                {"name": "dates_of_month", "type": "STRING"},
                {"name": "dataset_owner", "type": "STRING", "nullable": False},
                {"name": "modified_date", "type": "TIMESTAMP", "nullable": False},
                {"name": "dataset_modified_by_user", "type": "STRING", "nullable": False},
                {"name": "last_run_time", "type": "TIMESTAMP"},
                {"name": "next_run_due_time", "type": "TIMESTAMP"},
                {"name": "last_process_datetime", "type": "MAP<STRING, TIMESTAMP>"},
                {"name": "stage", "type": "STRING", "nullable": False},
                {"name": "comments", "type": "STRING"},
            ],
            "tblproperties": {"description": "Dataset-level control metadata"},
        },
        {
            "name": "ctrl_dataset_input_fields",
            "columns": [
                {
                    "name": "field_id",
                    "type": "BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)",
                    "comment": "Autogenerated identity column"
                },
                {"name": "dataset_id", "type": "STRING", "nullable": False},
                {"name": "database_name", "type": "STRING", "nullable": False},
                {"name": "table_name", "type": "STRING", "nullable": False},
                {"name": "field_name", "type": "STRING", "nullable": False},
                {"name": "stage", "type": "STRING", "nullable": False},
                {"name": "comments", "type": "STRING"},
            ],
            "tblproperties": {"description": "Expected input fields by dataset and table"},
        },
        {
            "name": "log_dataset_process",
            "columns": [
                {"name": "process_load_id", "type": "STRING", "comment": "Process ID (populate in code if needed)"},
                {"name": "dataset_id", "type": "STRING"},
                {"name": "source_database_name", "type": "STRING"},
                {"name": "target_file_name", "type": "STRING"},
                {"name": "status", "type": "STRING"},
                {"name": "load_type", "type": "STRING"},
                {"name": "start_timestamp", "type": "TIMESTAMP"},
                {"name": "end_timestamp", "type": "TIMESTAMP"},
                {"name": "last_process_timestamp", "type": "TIMESTAMP"},
                {"name": "job_run_duration", "type": "STRING"},
                {"name": "error_message", "type": "STRING"},
                {"name": "record_count", "type": "BIGINT"},
            ],
            "partition_by": ["dataset_id"],
            "tblproperties": {"description": "Per-run process log for dataset loads",
        "delta.appendOnly": "true"},
        },
        {
            "name": "log_dataset_event",
            "columns": [
                {"name": "event_load_id", "type": "STRING", "comment": "Event ID (populate in code if needed)"},
                {"name": "process_load_id", "type": "STRING"},
                {"name": "dataset_id", "type": "STRING"},
                {"name": "event_name", "type": "STRING"},
                {"name": "event_duration", "type": "INT"},
                {"name": "event_start_datetime", "type": "TIMESTAMP"},
                {"name": "event_end_datetime", "type": "TIMESTAMP"},
                {"name": "event_status", "type": "STRING"},
                {"name": "event_message", "type": "STRING"},
                {"name": "record_count", "type": "BIGINT"},
            ],
            "partition_by": ["dataset_id"],
            "tblproperties": {"description": "Event-level metrics within a process run",
        "delta.appendOnly": "true"},
        },
    ]

    # 1) Ensure + verify (fails fast if a table is on DBFS/workspace bucket)
    ensure_db_tables_and_verify_locations(
        spark,
        db_name=DB_NAME,
        asset_group=ASSET_GROUP,
        tables=PET_TABLES,
    )

    # If the call above raised a "workspace bucket" warning, run this once:
    # drop_and_recreate_tables(spark, db_name=DB_NAME, tables=PET_TABLES)
    # then call ensure_db_tables_and_verify_locations(...) again.

    # Quick verification
    spark.sql(f"SHOW TABLES IN {DB_NAME}").show(truncate=False)
