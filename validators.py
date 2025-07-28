from typing import List, Dict, Optional
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger("PETFileLoader")

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

    # Start validate DPS-CDP Params

def validate_dps_cdp_params(self, param_data: dict, db_name: str):
    load_type = param_data["load_type"].upper()
    schedule = param_data.get("schedule")
    week_days = param_data.get("week_days")
    dates_of_month = param_data.get("dates_of_month")

    if load_type not in {"INCREMENTAL", "FULL"}:
        raise ValueError("load_type must be 'INCREMENTAL' or 'FULL'")

    if load_type == "INCREMENTAL":
        if not param_data["incremental_timestamp_field"]:
            raise ValueError("incremental_timestamp_field must not be empty for INCREMENTAL load")
        if schedule and schedule.upper() == "DAILY":
            if dates_of_month:
                raise ValueError("dates_of_month must be empty for DAILY schedule")
            if week_days:
                raise ValueError("week_days must be empty for DAILY schedule")
        elif schedule and schedule.upper() == "WEEKLY":
            if not week_days:
                raise ValueError("week_days is required for WEEKLY schedule")
            if dates_of_month:
                raise ValueError("dates_of_month must be empty for WEEKLY schedule")
        elif schedule and schedule.upper() == "MONTHLY":
            if week_days:
                raise ValueError("week_days must be empty for MONTHLY schedule")
            if not dates_of_month:
                raise ValueError("dates_of_month must not be empty for MONTHLY schedule")
    else:
        if param_data["incremental_timestamp_field"] or param_data.get("incremental_header_to_tables_link_field") or schedule or week_days or dates_of_month:
            raise ValueError("For FULL load, incremental_timestamp_field, incremental_header_to_tables_link_field, schedule, week_days, and dates_of_month must be empty")

    if not param_data["pet_dataset_id"]:
        raise ValueError("pet_dataset_id is required")

    method = param_data["incremental_timestamp_method"]
    if method.upper() not in {"TIMESTAMP_HEADER_TABLE", "TIMESTAMP_DATA_TABLE"}:
        raise ValueError("incremental_timestamp_method must be 'timestamp_header_table' or 'timestamp_data_table'")

    if method.lower() == "timestamp_header_table":
        value = param_data.get("incremental_header_to_tables_link_field")
        if not value or "." not in value:
            raise ValueError("incremental_header_to_tables_link_field must be in '<table>.<field>' format")
        table, field = value.split(".", 1)
        table_exists = self.spark.sql(f"SHOW TABLES IN {db_name}").filter(f"tableName = '{table}'").count() > 0
        if not table_exists:
            raise ValueError(f"Table '{table}' not found in database '{db_name}'")
        schema_fields = [f.name for f in self.spark.table(f"{db_name}.{table}").schema.fields]
        if field not in schema_fields:
            raise ValueError(f"Field '{field}' not found in table '{db_name}.{table}'")

    if schedule and schedule.capitalize() not in {"Daily", "Weekly", "Monthly"}:
        raise ValueError("schedule must be one of: Daily, Weekly, Monthly")

    if week_days:
        valid_days = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
        given_days = {day.strip().capitalize() for day in week_days.split(",")}
        invalid = given_days - valid_days
        if invalid:
            raise ValueError(f"Invalid week_days: {invalid}")

    if dates_of_month:
        values = dates_of_month.split(",")
        for val in values:
            if not val.strip().isdigit() or not (1 <= int(val.strip()) <= 28):
                raise ValueError(f"dates_of_month must be between 1 and 28. Invalid: {val.strip()}")

    return True
# End validate DPS-CDP Params

def check_database_exists(spark: SparkSession, db_name: str) -> bool:
    return db_name.lower() in [row.databaseName.lower() for row in spark.sql("SHOW DATABASES").collect()]

def check_table_and_column_exist(spark: SparkSession, db_name: str, table_name: str, column_name: str) -> bool:
    try:
        # Check if table exists
        tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
        if not tables_df.filter(col("tableName") == table_name).count():
            return False
        
        # Get table schema
        df_schema = spark.table(f"{db_name}.{table_name}").schema
        
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

def validate_incremental_timestamp_field(spark: SparkSession, db_name: str, field: Optional[str]) -> None:
    if not field:
        raise ValueError("Incremental Timestamp Field is missing")
    
    # Always search all tables for the field (whether single or multi-part)
    tables = spark.sql(f"SHOW TABLES IN {db_name}").collect()
    
    found = False
    for table_row in tables:
        table_name = table_row.tableName
        if check_table_and_column_exist(spark, db_name, table_name, field):
            found = True
            break
    
    if not found:
        raise ValueError(f"Incremental Timestamp Field not found: {field}")

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

def validate_external_id(spec_data: dict, dataset_id: str) -> None:
    """Validate that ExternalID in specification matches the dataset ID."""
    if spec_data.get("ExternalID", "").upper() != dataset_id.upper():
        raise ValueError("Dataset ID mismatch between param and file")

def validate_dataset_enabled(param_data: dict, dataset_id: str) -> None:
    """Check if dataset is enabled for processing."""
    if param_data.get("Is Enabled", "TRUE").strip().upper() == "FALSE":
        raise Exception(f"Dataset {dataset_id} is disabled (Is Enabled = FALSE). Skipping load.")

def validate_database_name(spark: SparkSession, param_data: dict) -> str:
    """Validate database name exists and return it."""
    db_name = param_data.get("Database Name")
    if not db_name:
        raise ValueError("Database Name missing")
    if not check_database_exists(spark, db_name):
        raise ValueError(f"Database '{db_name}' not found")
    return db_name

def validate_input_fields(spark: SparkSession, input_fields_df, db_name: str) -> None:
    """Validate that all input fields exist in the database tables."""
    # Clean up the dataframe
    input_fields_df = input_fields_df.iloc[:, :2].dropna(how="all")
    input_fields_df.columns = [col.strip() for col in input_fields_df.columns]
    
    for _, row in input_fields_df.iterrows():
        table = str(row["Table Name"]).strip()
        column = str(row["Column Name"]).strip()
        if not check_table_and_column_exist(spark, db_name, table, column):
            raise ValueError(f"Table or column not found: {db_name}.{table}.{column}")
