# Databricks notebook source
from __future__ import annotations

import logging
from typing import List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

__all__ = [
    "check_database_exists",
    "check_table_and_column_exist",
]

logger = logging.getLogger(__name__)

# ---------------------------
# Internal helpers
# ---------------------------

def _resolve_db(spark: SparkSession, db: str) -> str:
    """
    Return canonical metastore schema name with exact casing.
    Supports 'schema' or 'catalog.schema'. If not found, returns input.
    """
    parts = [p for p in (db or "").split(".") if p]
    if not parts:
        return db

    try:
        dbs = [r.databaseName for r in spark.sql("SHOW DATABASES").collect()]
    except Exception:
        dbs = []

    if len(parts) == 1:
        schema = parts[0]
        return next((d for d in dbs if d.lower() == schema.lower()), schema)

    # catalog.schema
    catalog, schema = parts[0], parts[1]
    canon_schema = next((d for d in dbs if d.lower() == schema.lower()), schema)
    return f"{catalog}.{canon_schema}"

def _q(name: str) -> str:
    return f"`{str(name).replace('`', '``')}`"

def _show_tables_in(spark: SparkSession, db: str, like: str):
    canon_db = _resolve_db(spark, db)
    parts = [p for p in canon_db.split(".") if p]
    like_ = like.replace("`", "``")
    if len(parts) == 1:
        return spark.sql(f"SHOW TABLES IN hive_metastore.{_q(parts[0])} LIKE '{like_}'")
    if len(parts) == 2:
        return spark.sql(f"SHOW TABLES IN hive_metastore.{_q(parts[0])}.{_q(parts[1])} LIKE '{like_}'")
    return None

def _canonicalize_path(schema: StructType, path: str) -> Optional[str]:
    """Return exact-cased dotted path found in schema (case-insensitive)."""
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

# ---------------------------
# Public API
# ---------------------------
def check_database_exists(spark, db_name: str) -> bool:
    return db_name.lower() in [r.databaseName.lower() for r in spark.sql("SHOW DATABASES").collect()]
    
def check_table_and_column_exist(spark: SparkSession, db_name: str, table_name: str, column_name: str) -> bool:
    """
    Returns True if (db.table).column exists. Supports struct path 'struct.field'.
    Raises ValueError with a helpful message on permission/access errors.
    """
    try:
        # Table exists?
        try:
            tables_df = spark.sql(f"SHOW TABLES IN hive_metastore.{db_name}")
            if not tables_df.filter(col("tableName") == table_name).count():
                logger.warning(f"Table '{table_name}' not found in database '{db_name}'")
                return False
        except Exception as e:
            logger.error(f"Failed to list tables in database '{db_name}': {e}")
            raise ValueError(f"Cannot access database '{db_name}': {e}")

        # Load schema
        try:
            full = f"{db_name}.{table_name}"
            logger.info(f"Accessing table schema for: {full}")
            df_schema = spark.table(full).schema
        except Exception as e:
            em = str(e).lower()
            if ("table" in em) and ("not found" in em or "does not exist" in em):
                logger.warning(f"Table '{full}' does not exist or is not accessible")
                return False
            if "permission" in em or "access" in em:
                raise ValueError(f"Permission denied accessing table '{full}': {e}")
            if "o" in em and ".table" in em:  # e.g., "o337.table"
                raise ValueError(
                    f"Spark internal error accessing table '{full}'. "
                    f"This may indicate corruption, permission issues, or Delta table problems: {e}"
                )
            raise ValueError(f"Failed to access table schema for '{full}': {e}")

        # Direct column?
        if any(f.name.lower() == column_name.lower() for f in df_schema.fields):
            return True

        # Struct path?
        if "." in column_name:
            struct_name, nested_field = column_name.split(".", 1)
            for f in df_schema.fields:
                if f.name.lower() == struct_name.lower() and hasattr(f.dataType, "fields"):
                    return any(nf.name.lower() == nested_field.lower() for nf in f.dataType.fields)

        return False

    except Exception as e:
        logger.exception(f"Table/Column check failed: {e}")
        raise