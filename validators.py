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

def validate_dps_cdp_params(spark: SparkSession, param_data: dict, db_name: str) -> bool:
    load_type = param_data.get("Load Type", "").upper()
    schedule = param_data.get("Schedule")
    week_days = param_data.get("Week Days")
    dates_of_month = param_data.get("Dates Of Month")

    if load_type not in {"INCREMENTAL", "FULL"}:
        raise ValueError("Load Type must be 'INCREMENTAL' or 'FULL'")

    if load_type == "INCREMENTAL":
        if not param_data.get("Incremental Timestamp Field"):
            raise ValueError("Incremental Timestamp Field must not be empty for INCREMENTAL load")
        if schedule and schedule.upper() == "DAILY":
            if dates_of_month:
                raise ValueError("Dates Of Month must be empty for DAILY schedule")
            if week_days:
                raise ValueError("Week Days must be empty for DAILY schedule")
        elif schedule and schedule.upper() == "WEEKLY":
            if not week_days:
                raise ValueError("Week Days is required for WEEKLY schedule")
            if dates_of_month:
                raise ValueError("Dates Of Month must be empty for WEEKLY schedule")
        elif schedule and schedule.upper() == "MONTHLY":
            if week_days:
                raise ValueError("Week Days must be empty for MONTHLY schedule")
            if not dates_of_month:
                raise ValueError("Dates Of Month must not be empty for MONTHLY schedule")
    else:
        if (param_data.get("Incremental Timestamp Field") or 
            param_data.get("Incremental Header To Tables  Link Field") or 
            schedule or week_days or dates_of_month):
            raise ValueError("For FULL load, incremental fields and schedule parameters must be empty")

    if not param_data.get("Pet Dataset ID"):
        raise ValueError("Pet Dataset ID is required")

    method = param_data.get("Incremental Timestamp Method", "")
    if method.upper() not in {"TIMESTAMP_HEADER_TABLE", "TIMESTAMP_DATA_TABLE"}:
        raise ValueError("Incremental Timestamp Method must be 'TIMESTAMP_HEADER_TABLE' or 'TIMESTAMP_DATA_TABLE'")

    if method.upper() == "TIMESTAMP_HEADER_TABLE":
        value = param_data.get("Incremental Header To Tables  Link Field")
        if not value or "." not in value:
            raise ValueError("Incremental Header To Tables Link Field must be in '<table>.<field>' format")
        table, field = value.split(".", 1)
        if not check_table_and_column_exist(spark, db_name, table, field):
            raise ValueError(f"Table or field not found: {db_name}.{table}.{field}")

    if schedule and schedule.capitalize() not in {"Daily", "Weekly", "Monthly"}:
        raise ValueError("Schedule must be one of: Daily, Weekly, Monthly")

    if week_days:
        valid_days = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
        given_days = {day.strip().capitalize() for day in week_days.split(",")}
        invalid = given_days - valid_days
        if invalid:
            raise ValueError(f"Invalid Week Days: {invalid}")

    if dates_of_month:
        values = dates_of_month.split(",")
        for val in values:
            if not val.strip().isdigit() or not (1 <= int(val.strip()) <= 28):
                raise ValueError(f"Dates Of Month must be between 1 and 28. Invalid: {val.strip()}")

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

def get_last_process_map(spark: SparkSession, dataset_id: str, start_process_from_date_str: Optional[str], cleaned_input_fields_df) -> Optional[Dict[str, datetime]]:
    try:
        df_existing = spark.sql(
            f"SELECT last_process_datetime FROM pet.ctrl_dataset_config_vj WHERE dataset_id = '{dataset_id}'"
        )
        if df_existing.count() > 0:
            current_map = df_existing.collect()[0]["last_process_datetime"]
            if current_map:
                return current_map

        if not start_process_from_date_str:
            return None

        try:
            start_process_from_date = datetime.strptime(start_process_from_date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_process_from_date = datetime.strptime(start_process_from_date_str, "%Y-%m-%d")

        # If last_process_datetime is null, create a map with all unique table names
        unique_tables = cleaned_input_fields_df["Table Name"].unique()
        table_timestamp_map = {}
        for table in unique_tables:
            table_timestamp_map[str(table).strip()] = start_process_from_date
        
        return table_timestamp_map
    except Exception as e:
        logger.exception(f"Error checking last_process_datetime: {e}")
        raise

def validate_external_id(spec_data: dict, dataset_id: str) -> None:
    """Validate that ExternalID in specification matches the dataset ID."""
    if spec_data.get("ExternalID", "").upper() != dataset_id.upper():
        raise ValueError("Dataset ID mismatch between param and file")

def validate_dataset_name(spec_data: dict, dataset_id: str) -> None:
    """Validate that Dataset Name in specification matches the dataset ID."""
    if spec_data.get("Dataset Name", "").upper() != dataset_id.upper():
        raise ValueError("Dataset Name mismatch between param and file")

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

def validate_input_fields(
    spark: SparkSession,
    input_fields_df: pd.DataFrame,
    default_db_name: str
) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """
    Validates 'Input Schema' (Database Name, Table Name, Column Name).
    - Blank Database Name falls back to DPS-CDP default_db_name
    - Validates database/table/column (supports struct with dot-notation)
    - Preserves the original order from the Excel file
    Returns:
      - cleaned DataFrame with the 3 columns in original order
      - list of unique (db, table) tuples
    """
    if input_fields_df is None or input_fields_df.empty:
        raise ValueError("Input Schema sheet is empty.")

    # Get first 3 columns and clean up
    df = input_fields_df.iloc[:, :3].copy()
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = {"Database Name", "Table Name", "Column Name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input Schema is missing required columns: {missing}")

    # Clean string values
    df["Database Name"] = df["Database Name"].apply(lambda x: str(x).strip() if pd.notnull(x) and str(x).strip() else "")
    df["Table Name"]   = df["Table Name"].apply(lambda x: str(x).strip() if pd.notnull(x) and str(x).strip() else "")
    df["Column Name"]  = df["Column Name"].apply(lambda x: str(x).strip() if pd.notnull(x) and str(x).strip() else "")

    # Remove completely empty rows but preserve original position information
    # Add original position tracking before removing empty rows
    df = df.reset_index(drop=True)  # Reset index to get clean 0,1,2,3... indices
    df['_original_position'] = df.index  # Track original position
    
    # Remove rows where all three key fields are empty
    mask = (df["Database Name"] != "") | (df["Table Name"] != "") | (df["Column Name"] != "")
    df = df[mask]
    
    if df.empty:
        raise ValueError("Input Schema sheet has no valid data rows.")

    # Process each row in order and collect validated results
    validated_rows = []
    unique_db_tables: set = set()

    for position, (idx, row) in enumerate(df.iterrows()):
        db_name = row["Database Name"] or (default_db_name or "")
        table   = row["Table Name"]
        column  = row["Column Name"]
        original_pos = row['_original_position']
        row_no  = original_pos + 2  # excel header = row 1, so data starts at row 2

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

        # Store validated row in order
        validated_rows.append({
            "Database Name": db_name,
            "Table Name": table, 
            "Column Name": column,
            "_original_position": original_pos
        })
        
        # Track unique combinations for validation
        unique_db_tables.add((db_name, table))

    # Create result DataFrame preserving original order
    if not validated_rows:
        raise ValueError("No valid rows found in Input Schema.")
    
    # Sort by original position to maintain order, then remove position column
    validated_df = pd.DataFrame(validated_rows)
    validated_df = validated_df.sort_values('_original_position').reset_index(drop=True)
    cleaned_df = validated_df[["Database Name", "Table Name", "Column Name"]]
    
    return cleaned_df, list(unique_db_tables)
