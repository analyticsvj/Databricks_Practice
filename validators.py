from typing import List, Dict, Optional
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger("PETFileLoader")

# def validate_required_sheets(sheetnames: List[str]) -> bool:
#     required_sheets = {"Specification", "Input Fields", "DPS-CDP Params"}
#     missing = required_sheets - set(sheetnames)
#     if missing:
#         raise ValueError(f"Missing required sheets: {missing}")
#     return True

def validate_required_sheets(sheetnames: list) -> bool:
    required_sheets = {"Specification", "Input Fields", "DPS-CDP Params"}
    missing = required_sheets - set(sheetnames)
    if missing:
        raise ValueError(f"Missing required sheets: {missing}")
    return True

def validate_specification_format(spec_sheet) -> Dict[str, str]:
    expected_keys = [
        "Source Tenant", "Dataset Name", "Data Sharing Agreement (DSA)", "Purpose",
        "Privacy Domain - Encryption Key", "Destination Tenant", "ExternalID",
        "Allow Missing Table", "Priority", "Policy"
    ]
    spec_rows = list(spec_sheet.iter_rows(min_row=1, max_row=10, max_col=2, values_only=True))
    if len(spec_rows) < 10:
        raise ValueError(f"Specification sheet must contain 10 rows, found {len(spec_rows)}.")
    output = {}
    for i, expected_key in enumerate(expected_keys):
        actual_key, actual_value = spec_rows[i]
        if actual_key != expected_key:
            raise ValueError(f"Row {i+1} mismatch: expected '{expected_key}', found '{actual_key}'")
        if not actual_value:
            raise ValueError(f"Missing value for '{expected_key}' at row {i+1}")
        output[actual_key] = str(actual_value).strip()
    return output

def check_database_exists(spark: SparkSession, db_name: str) -> bool:
    return db_name.lower() in [row.databaseName.lower() for row in spark.sql("SHOW DATABASES").collect()]

def check_table_and_column_exist(spark: SparkSession, db_name: str, table_name: str, column_name: str) -> bool:
    try:
        # Check if table exists
        tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
        if not tables_df.filter(col("tableName") == table_name).count():
            print(f"DEBUG: Table '{table_name}' not found in database '{db_name}'")
            return False
        
        # Get table schema
        df_schema = spark.table(f"{db_name}.{table_name}").schema
        print(f"DEBUG: Checking for column '{column_name}' in table '{db_name}.{table_name}'")
        print(f"DEBUG: Available fields: {[f.name for f in df_schema.fields]}")
        
        # Check for direct column match first (case-insensitive)
        for field in df_schema.fields:
            if field.name.lower() == column_name.lower():
                print(f"DEBUG: Found direct match: '{field.name}' matches '{column_name}'")
                return True
        
        # Check for struct field (e.g., meta.event_time)
        if "." in column_name:
            parts = column_name.split(".", 1)  # Split only on first dot
            struct_name = parts[0].lower()
            nested_field = parts[1].lower()
            print(f"DEBUG: Looking for struct field: struct='{struct_name}', nested='{nested_field}'")
            
            # Find the struct field (case-insensitive)
            for field in df_schema.fields:
                if field.name.lower() == struct_name:
                    print(f"DEBUG: Found struct field '{field.name}', type: {field.dataType}")
                    # Check if it's a struct type and contains the nested field
                    if hasattr(field.dataType, 'fields'):  # StructType
                        nested_fields = [nf.name for nf in field.dataType.fields]
                        print(f"DEBUG: Nested fields in '{field.name}': {nested_fields}")
                        found_nested = any(nested_f.name.lower() == nested_field for nested_f in field.dataType.fields)
                        if found_nested:
                            print("DEBUG: Found nested field match!")
                            return True
                    else:
                        print(f"DEBUG: Field '{field.name}' is not a struct type")
                    break
        
        print(f"DEBUG: No match found for '{column_name}' in table '{db_name}.{table_name}'")
        return False
    except Exception as e:
        print(f"DEBUG: Table/Column check failed: {e}")
        logger.exception(f"Table/Column check failed: {e}")
        raise

def validate_incremental_timestamp_field(spark: SparkSession, db_name: str, field: Optional[str]) -> None:
    if not field:
        raise ValueError("Incremental Timestamp Field is missing")
    
    parts = field.split(".")
    if len(parts) == 1:
        # Single field name - check across all tables in the database
        field_name = parts[0]
        tables = spark.sql(f"SHOW TABLES IN {db_name}").collect()
        
        found = False
        for table_row in tables:
            table_name = table_row.tableName
            if check_table_and_column_exist(spark, db_name, table_name, field_name):
                found = True
                break
        
        if not found:
            raise ValueError(f"Incremental Timestamp Field '{field_name}' not found in any table in database '{db_name}'")
            
    elif len(parts) >= 2:
        # Format: table.column or table.struct.field (e.g., "mets.event_time")
        table_name = parts[0]
        column_name = ".".join(parts[1:])  # Join remaining parts for struct fields
        
        if not check_table_and_column_exist(spark, db_name, table_name, column_name):
            raise ValueError(f"Incremental Timestamp Field not found: {field}")
    else:
        raise ValueError(f"Invalid format for Incremental Timestamp Field: {field}")

def get_last_process_map(spark: SparkSession, dataset_id: str, start_process_from_date_str: Optional[str]) -> Optional[Dict[str, datetime]]:
    try:
        df_existing = spark.sql(
            f"SELECT last_process_datetime FROM pet.ctrl_dataset_config_vj WHERE dataset_id = '{dataset_id}'"
        )
        if df_existing.count() > 0:
            current_map = df_existing.collect()[0]["last_process_datetime"]
            if current_map and ("initialise" in current_map or dataset_id.lower() in current_map):
                return current_map

        if not start_process_from_date_str:
            return None

        try:
            start_process_from_date = datetime.strptime(start_process_from_date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_process_from_date = datetime.strptime(start_process_from_date_str, "%Y-%m-%d")

        return {"initialise": start_process_from_date}
    except Exception as e:
        logger.exception(f"Error checking last_process_datetime: {e}")
        raise
