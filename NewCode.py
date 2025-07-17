class pet_domain0_transformations_config:

    def __init__(self, spark):
        self.spark = spark

    def download_to_tmp(self, file_path):
        # Implement this method if not already present
        # Example: download file to /tmp and return path
        pass

    def validate_required_sheets(self, sheetnames: list) -> bool:
        required = {"Specification", "Input Fields", "DPS-CDP Params"}
        missing = required - set(sheetnames)
        if missing:
            raise ValueError(f"Missing sheets: {missing}")
        return True

    def parse_specification(self, sheet, dataset_id_param):
        dataset_name = str(sheet["B2"].value).strip()
        dataset_id_file = str(sheet["B7"].value).strip().upper()
        if dataset_id_file != dataset_id_param.upper():
            raise ValueError(f"Mismatch: parameter '{dataset_id_param}' vs file '{dataset_id_file}'")
        return dataset_name

    def extract_dps_cdp_params(self, sheet) -> dict:
        params = {
            key.strip().lower(): str(value).strip() if value else None
            for key, value in (row for row in sheet.iter_rows(min_row=2, max_row=15, max_col=2, values_only=True))
            if key
        }
        if not params.get("database_name"):
            raise ValueError("Missing 'database_name' in DPS-CDP Params")
        return params

    def validate_database(self, db_name):
        existing = [db.databaseName.lower() for db in self.spark.sql("SHOW DATABASES").collect()]
        if db_name.lower() not in existing:
            raise ValueError(f"Hive database '{db_name}' not found")
        return True

    def read_input_fields(self, local_path: str) -> pd.DataFrame:
        df = pd.read_excel(local_path, sheet_name="Input Fields", engine="openpyxl").iloc[:, :2].dropna(how="all")
        df.columns = [c.strip() for c in df.columns]
        return df

    def validate_input_fields(self, df: pd.DataFrame, db_name: str):
        tables = [row["tableName"] for row in self.spark.sql(f"SHOW TABLES IN {db_name}").collect()]
        for _, row in df.iterrows():
            tbl = row["Table Name"].strip()
            coln = row["Column Name"].strip()
            if tbl not in tables:
                raise ValueError(f"Table '{tbl}' not found in database '{db_name}'")
            full_tbl = f"{db_name}.{tbl}"
            schema = [f.name for f in self.spark.table(full_tbl).schema.fields]
            if coln not in schema:
                raise ValueError(f"Column '{coln}' not found in table '{tbl}'")
        return True

    def check_dataset_exists(self, dataset_id: str) -> bool:
        count = self.spark.sql(f"SELECT * FROM PET.CTRL_dataset_config WHERE dataset_id = '{dataset_id}'").count()
        return count > 0

    def remove_existing_dataset(self, dataset_id: str):
        self.spark.sql(f"DELETE FROM PET.CTRL_dataset_config WHERE dataset_id = '{dataset_id}'")
        self.spark.sql(f"DELETE FROM PET.CTRL_dataset_input_fields WHERE dataset_id = '{dataset_id}'")

    def write_dataset_config(self, dataset_id: str, dataset_name: str, param_data: dict):
        now = datetime.now()
        row_obj = Row(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            source_database_name=param_data["database_name"],
            load_type=param_data["load_type"],
            is_enabled=True,
            pet_dataset_id=param_data["pet_dataset_id"],
            incremental_timestamp_method=param_data["incremental_timestamp_method"],
            incremental_timestamp_field=param_data["incremental_timestamp_field"],
            incremental_header_to_tables_link_field=param_data.get("incremental_header_to_tables_link_field"),
            schedule=param_data.get("schedule"),
            week_days=param_data.get("week_days"),
            dates_of_month=param_data.get("dates_of_month"),
            dataset_owner=param_data["dataset_owner"],
            modified_date=now,
            dataset_modified_by_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
            comments=param_data.get("comments")
        )

        schema = StructType([
            StructField("dataset_id", StringType(), False),
            StructField("dataset_name", StringType(), False),
            StructField("source_database_name", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("is_enabled", BooleanType(), False),
            StructField("pet_dataset_id", StringType(), False),
            StructField("incremental_timestamp_method", StringType(), True),
            StructField("incremental_timestamp_field", StringType(), True),
            StructField("incremental_header_to_tables_link_field", StringType(), True),
            StructField("schedule", StringType(), True),
            StructField("week_days", StringType(), True),
            StructField("dates_of_month", StringType(), True),
            StructField("dataset_owner", StringType(), False),
            StructField("modified_date", TimestampType(), False),
            StructField("dataset_modified_by_user", StringType(), False),
            StructField("comments", StringType(), True)
        ])

        self.spark.createDataFrame([row_obj], schema).write.mode("append").saveAsTable("PET.CTRL_dataset_config")

    def write_input_fields(self, dataset_id: str, df: pd.DataFrame) -> int:
        rows = [
            Row(dataset_id=dataset_id, table_name=r["Table Name"].strip(), field_name=r["Column Name"].strip())
            for _, r in df.iterrows()
        ]

        schema = StructType([
            StructField("dataset_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("field_name", StringType(), False)
        ])

        self.spark.createDataFrame(rows, schema).coalesce(1).write.mode("append").saveAsTable("PET.CTRL_dataset_input_fields")
        return len(rows)
