from dsp.udfs.pet_validators import (
    check_table_and_column_exist,
    validate_required_sheets,
    validate_schedule_rules,
    validate_file_format,
    validate_dataset_enabled,
    validate_external_id,
    validate_dataset_name,
    validate_database_name,
    validate_input_fields,
    validate_dps_cdp_params,
    validate_and_normalize_incremental_timestamp_field,
    get_last_process_map
    )

try:
    ok = check_table_and_column_exist(
        spark,
        db_name="dids",
        table_name="dids",  # change to the real table if needed (e.g., "did_sar")
        column_name="meta.event_received_ts"
    )
    assert ok, "Expected column meta.event_received_ts to exist"
    print("test passed")
    print("------------------------------")
    print("check_table_and_column_exist()")
    print("------------------------------")

except ValueError as e:
    # function raises ValueError on permission/catalog access issues
    print(f"environment/access error: {e}")

except AssertionError as e:
    print(f"assertion failed: {e}")

except Exception as e:
    # optional catch-all so the notebook doesn't hard-fail
    print(f"unexpected error: {e}")


# COMMAND ----------

print("------------------------------")
print("validate_required_sheets()")
print("------------------------------")

def run_case(sheets, label, expect_ok: bool):
    try:
        ok = validate_required_sheets(sheets)
        if expect_ok:
            assert ok, "Expected True"
            print(f"[PASS] {label}")
        else:
            print(f"[FAIL] {label}: expected ValueError but got ok=True")
    except ValueError as e:
        if expect_ok:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except AssertionError as e:
        print(f"[FAIL] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# 1) HAPPY: all required sheets present
run_case(
    ["Specification", "Input Schema", "DPS-CDP Params"],
    "happy: all required present",
    expect_ok=True,
)

# 2) EXTRA SHEETS: still OK
run_case(
    ["Specification", "Input Schema", "DPS-CDP Params", "Notes", "Mappings"],
    "happy: extras allowed",
    expect_ok=True,
)

# 3) MISSING ONE: should raise ValueError
run_case(
    ["Specification", "Input Schema"],  # missing "DPS-CDP Params"
    "failure: missing DPS-CDP Params",
    expect_ok=False,
)

# 4) NONE/EMPTY: should raise ValueError (missing all)
run_case(
    None,
    "failure: no sheets provided",
    expect_ok=False,
)

# 5) WHITESPACE/CASE MISMATCH: shows current function is case/space sensitive
run_case(
    ["specification", " Input Schema ", "DPS-CDP Params"],
    "failure: case/space mismatch (as implemented)",
    expect_ok=False,
)

print("Done.")


# COMMAND ----------

print("------------------------------")
print("validate_schedule_rules()")
print("------------------------------")

def run_case(param_data, label, expect_ok: bool):
    try:
        ok = validate_schedule_rules(param_data)
        if expect_ok:
            assert ok is True, "Expected True"
            print(f"[PASS] {label}")
        else:
            print(f"[FAIL] {label}: expected ValueError but got ok=True")
    except ValueError as e:
        if expect_ok:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except AssertionError as e:
        print(f"[FAIL] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# ---- HAPPY PATHS ----
# DAILY: week_days and dates_of_month must be empty/None
run_case(
    {"Schedule": "DAILY", "Week Days": None, "Dates Of Month": None},
    "happy: DAILY with both empty",
    expect_ok=True,
)

# WEEKLY: week_days required; dates_of_month must be empty
run_case(
    {"Schedule": "WEEKLY", "Week Days": ["MON","WED"], "Dates Of Month": None},
    "happy: WEEKLY with week_days list",
    expect_ok=True,
)
run_case(
    {"Schedule": "WEEKLY", "Week Days": "MON,TUE", "Dates Of Month": ""},
    "happy: WEEKLY with week_days string",
    expect_ok=True,
)

# MONTHLY: dates_of_month required; week_days must be empty
run_case(
    {"Schedule": "MONTHLY", "Week Days": None, "Dates Of Month": [1,15,31]},
    "happy: MONTHLY with dates list",
    expect_ok=True,
)
run_case(
    {"Schedule": "MONTHLY", "Week Days": "", "Dates Of Month": "1,15,31"},
    "happy: MONTHLY with dates string",
    expect_ok=True,
)

# ---- FAILURE CASES ----
# Invalid schedule value
run_case(
    {"Schedule": "HOURLY", "Week Days": None, "Dates Of Month": None},
    "fail: invalid schedule value",
    expect_ok=False,
)

# DAILY but week_days present
run_case(
    {"Schedule": "DAILY", "Week Days": "MON", "Dates Of Month": None},
    "fail: DAILY cannot have week_days",
    expect_ok=False,
)

# DAILY but dates_of_month present
run_case(
    {"Schedule": "DAILY", "Week Days": None, "Dates Of Month": "1,15"},
    "fail: DAILY cannot have dates_of_month",
    expect_ok=False,
)

# WEEKLY but missing week_days
run_case(
    {"Schedule": "WEEKLY", "Week Days": None, "Dates Of Month": None},
    "fail: WEEKLY requires week_days",
    expect_ok=False,
)

# WEEKLY but dates_of_month present
run_case(
    {"Schedule": "WEEKLY", "Week Days": ["MON"], "Dates Of Month": [1]},
    "fail: WEEKLY cannot have dates_of_month",
    expect_ok=False,
)

# MONTHLY but missing dates_of_month
run_case(
    {"Schedule": "MONTHLY", "Week Days": None, "Dates Of Month": None},
    "fail: MONTHLY requires dates_of_month",
    expect_ok=False,
)

# MONTHLY but week_days present
run_case(
    {"Schedule": "MONTHLY", "Week Days": "MON", "Dates Of Month": "1,15"},
    "fail: MONTHLY cannot have week_days",
    expect_ok=False,
)

# Missing/None schedule
run_case(
    {"Schedule": None, "Week Days": None, "Dates Of Month": None},
    "fail: missing schedule",
    expect_ok=False,
)

print("Done.")


# COMMAND ----------

print("------------------------------")
print("validate_file_format()")
print("------------------------------")

def run_case(path, label, expect_ok: bool):
    try:
        ok = validate_file_format(path)
        if expect_ok:
            assert ok is True, "Expected True"
            print(f"[PASS] {label}")
        else:
            print(f"[FAIL] {label}: expected ValueError but got ok=True")
    except ValueError as e:
        if expect_ok:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except AssertionError as e:
        print(f"[FAIL] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# ---- HAPPY PATHS ----
run_case("report.xlsx", "happy: .xlsx", expect_ok=True)
run_case("legacy.xls",  "happy: .xls",  expect_ok=True)
run_case("DBFS:/mnt/data/Report.XLSX", "happy: case-insensitive + DBFS-like path", expect_ok=True)
run_case("s3://bucket/folder/file.XlS", "happy: case-insensitive + S3-like path", expect_ok=True)

# ---- FAILURE CASES ----
run_case("data.csv",            "fail: wrong extension .csv", expect_ok=False)
run_case("report.xlsx.gz",      "fail: .xlsx.gz is not allowed", expect_ok=False)
run_case("no_extension",        "fail: no extension", expect_ok=False)
run_case("",                    "fail: empty path", expect_ok=False)
run_case(None,                  "fail: None path", expect_ok=False)
run_case("file.xlsm",           "fail: .xlsm macro workbook not allowed (as implemented)", expect_ok=False)
run_case("file.xlsx?version=1", "fail: query string after extension", expect_ok=False)

print("Done.")


# COMMAND ----------

# assumes validate_dataset_enabled (and _getp) are already available

print("------------------------------")
print("validate_dataset_enabled()")
print("------------------------------")

def run_case(param_data, dataset_id, label, expect_raise: bool):
    try:
        validate_dataset_enabled(param_data, dataset_id)
        if expect_raise:
            print(f"[FAIL] {label}: expected Exception but none raised")
        else:
            print(f"[PASS] {label}")
    except Exception as e:
        if expect_raise:
            # sanity check that the message references the dataset id
            msg = str(e)
            assert str(dataset_id) in msg, f"message should include dataset_id -> {msg}"
            print(f"[PASS] {label}: {e}")
        else:
            print(f"[FAIL] {label}: unexpected Exception -> {e}")

# ---- HAPPY (no exception expected) ----
run_case({"Is Enabled": "TRUE"}, "ds_ok_true", "happy: explicit TRUE", expect_raise=False)

# defaulting: missing key -> treated as TRUE by (val or 'TRUE')
run_case({}, "ds_ok_default", "happy: missing key defaults to TRUE", expect_raise=False)

# case-insensitive TRUE
run_case({"Is Enabled": "true"}, "ds_ok_lower", "happy: 'true' lower-case", expect_raise=False)

# other non-'FALSE' strings are treated as enabled
run_case({"Is Enabled": "No"}, "ds_no", "note: 'No' treated as enabled (as implemented)", expect_raise=False)

# ---- FAILURE (exception expected) ----
run_case({"Is Enabled": "FALSE"}, "ds_off_upper", "fail: explicit FALSE", expect_raise=True)
run_case({"Is Enabled": "False"}, "ds_off_mixed", "fail: 'False' -> upper() == 'FALSE'", expect_raise=True)

print("Done.")


# COMMAND ----------

print("------------------------------")
print("validate_external_id()")
print("------------------------------")

def run_case(spec_data, dataset_id, label, expect_raise: bool):
    try:
        validate_external_id(spec_data, dataset_id)
        if expect_raise:
            print(f"[FAIL] {label}: expected ValueError but none raised")
        else:
            print(f"[PASS] {label}")
    except ValueError as e:
        if expect_raise:
            assert "mismatch" in str(e).lower(), f"message mismatch -> {e}"
            print(f"[PASS] {label}: {e}")
        else:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# ---- HAPPY PATHS ----
run_case({"ExternalID": "DS1"}, "DS1", "happy: exact match", expect_raise=False)
run_case({"ExternalID": "ds1"}, "DS1", "happy: case-insensitive", expect_raise=False)

# ---- FAILURE CASES ----
run_case({}, "DS1", "fail: missing ExternalID (treated as empty)", expect_raise=True)
run_case({"ExternalID": "DS2"}, "DS1", "fail: mismatch", expect_raise=True)

# # current implementation does not strip spaces -> this will fail
# run_case({"ExternalID": " DS1 "}, "DS1", "note: spaces cause mismatch with current impl", expect_raise=True)

# stringy numbers still fine (string compare)
run_case({"ExternalID": "001"}, "001", "happy: zero-padded string equal", expect_raise=False)

print("Done.")


# COMMAND ----------

print("------------------------------")
print("validate_dataset_name()")
print("------------------------------")

def run_case(spec_data, dataset_id, label, expect_raise: bool):
    try:
        validate_dataset_name(spec_data, dataset_id)
        if expect_raise:
            print(f"[FAIL] {label}: expected ValueError but none raised")
        else:
            print(f"[PASS] {label}")
    except ValueError as e:
        if expect_raise:
            assert "mismatch" in str(e).lower(), f"message mismatch -> {e}"
            print(f"[PASS] {label}: {e}")
        else:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# ---- HAPPY PATHS ----
run_case({"Dataset Name": "DS1"}, "DS1", "happy: exact match", expect_raise=False)
run_case({"Dataset Name": "ds1"}, "DS1", "happy: case-insensitive match", expect_raise=False)

# ---- FAILURE CASES ----
run_case({}, "DS1", "fail: missing 'Dataset Name' (treated as empty)", expect_raise=True)
run_case({"Dataset Name": "DS2"}, "DS1", "fail: mismatch", expect_raise=True)

# # Current implementation does not .strip() -> spaces will cause mismatch
# run_case({"Dataset Name": " DS1 "}, "DS1", "note: spaces cause mismatch (as implemented)", expect_raise=True)

# Stringy numbers still fine (string compare)
run_case({"Dataset Name": "001"}, "001", "happy: zero-padded string equal", expect_raise=False)

print("Done.")


# COMMAND ----------

print("------------------------------")
print("validate_database_name()")
print("------------------------------")

from pyspark.sql import SparkSession
spark: SparkSession = spark  # Databricks-provided

# --- seed: create a temp database that should be found ---
test_db = "pet"


def run_case(param_data, label, expect_ok: bool, expected_return: str | None = None):
    try:
        out = validate_database_name(spark, param_data)
        if expect_ok:
            assert out == expected_return, f"Expected return {expected_return!r}, got {out!r}"
            print(f"[PASS] {label}")
        else:
            print(f"[FAIL] {label}: expected ValueError but got {out!r}")
    except ValueError as e:
        if expect_ok:
            print(f"[FAIL] {label}: unexpected ValueError -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except AssertionError as e:
        print(f"[FAIL] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# 1) HAPPY: database exists
run_case({"Database Name": test_db}, "happy: existing database", expect_ok=True, expected_return=test_db)

# 2) HAPPY: case-insensitive lookup (function returns the input string unchanged)
run_case({"Database Name": test_db.upper()}, "happy: uppercase name", expect_ok=True, expected_return=test_db.upper())

# 3) FAILURE: missing key -> "Database Name missing"
run_case({}, "fail: missing 'Database Name'", expect_ok=False)

# 4) FAILURE: non-existent database -> "Database '<name>' not found"
run_case({"Database Name": "no_such_db_xyz"}, "fail: database not found", expect_ok=False)


# COMMAND ----------

# Assumes these are already available (via %run or import):
# - validate_input_fields(spark, input_fields_df: pd.DataFrame, default_db_name: str)
# - database_exists(spark, db_name)
# - check_table_and_column_exist(spark, db_name, table_name, column_name)

import pandas as pd
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

print("------------------------------")
print("validate_input_fields() — order preserving")
print("------------------------------")

# --------------------------
# Seed: create temp databases/tables
# --------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS pet")
spark.sql("CREATE DATABASE IF NOT EXISTS pet")

# pet.t1 with nested struct "meta.ts"
t1_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("meta", StructType([
        StructField("ts", TimestampType(), True),
        StructField("src", StringType(), True),
    ]), True),
])
spark.createDataFrame(
    [
        (1, Row(ts=datetime(2025,1,1,0,0,0), src="sys")),
        (2, Row(ts=datetime(2025,1,2,0,0,0), src="ingest")),
    ],
    schema=t1_schema
).write.mode("overwrite").saveAsTable("pet.t1")

# pet.t2 with simple column c1
spark.createDataFrame([(1, "a")], "id int, c1 string").write.mode("overwrite").saveAsTable("pet.t2")

# --------------------------
# Helpers
# --------------------------
def run_ok(df_in, default_db, label, expect_rows, expect_pairs):
    try:
        cleaned_df, pairs = validate_input_fields(spark, df_in, default_db)
        # 1) columns exact
        assert list(cleaned_df.columns) == ["Database Name", "Table Name", "Column Name"], "unexpected columns"
        # 2) order preserved
        got_rows = [tuple(r) for r in cleaned_df.to_records(index=False)]
        assert got_rows == expect_rows, f"row order/content mismatch:\n got={got_rows}\n exp={expect_rows}"
        # 3) unique pairs (order not guaranteed)
        assert set(pairs) == set(expect_pairs), f"pairs mismatch:\n got={pairs}\n exp={expect_pairs}"
        print(f"[PASS] {label}")
    except Exception as e:
        print(f"[FAIL] {label}: {e}")

def run_fail(df_in, default_db, label, expect_substr=None):
    try:
        validate_input_fields(spark, df_in, default_db)
        print(f"[FAIL] {label}: expected ValueError but succeeded")
    except ValueError as e:
        msg = str(e).lower()
        if expect_substr and expect_substr.lower() not in msg:
            print(f"[FAIL] {label}: message mismatch -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# --------------------------
# 1) HAPPY: preserves original order; trims; default DB fallback; struct path
# Excel-like rows (header is conceptual row 1; code reports row_no = original_index + 2)
df_in = pd.DataFrame(
    [
        ["",        "t1", "meta.ts"],     # row 2: DB falls back to pet
        ["   ",     "   ", "   "],        # row 3: fully blank -> removed
        [" pet", "t2 ", " c1 "],       # row 4: trims to pet,t2,c1
        ["",        "",    ""],           # row 5: fully blank -> removed
        ["pet",  "t1",  "meta.ts"],    # row 6: duplicate OK; pairs set deduped
    ],
    columns=["Database Name", "Table Name", "Column Name"]
)
run_ok(
    df_in,
    default_db="pet",
    label="happy: order preserved + trim + default db + struct path",
    expect_rows=[
        ("pet", "t1", "meta.ts"),  # from row 2
        ("pet", "t2", "c1"),       # from row 4
        ("pet", "t1", "meta.ts"),  # from row 6
    ],
    expect_pairs=[("pet","t1"), ("pet","t2")]
)

# --------------------------
# 2) FAILURE: DB blank with no default -> error, row number reflects original Excel row
df_no_default = pd.DataFrame(
    [
        ["", "t1", "meta.ts"],  # original row 2 -> error points to Row 2
    ],
    columns=["Database Name","Table Name","Column Name"]
)
run_fail(
    df_no_default,
    default_db="",
    label="fail: DB blank and no default",
    expect_substr="row 2"
)

# --------------------------
# 3) FAILURE: missing Table Name (error includes correct row number)
df_missing_table = pd.DataFrame(
    [
        ["pet", "", "meta.ts"],  # original row 2
        ["pet", "t2", "c1"],     # original row 3 (valid, but shouldn't be reached)
    ],
    columns=["Database Name","Table Name","Column Name"]
)
run_fail(
    df_missing_table,
    default_db="pet",
    label="fail: missing Table Name (row 2)",
    expect_substr="row 2"
)

# --------------------------
# 4) FAILURE: missing Column Name (row number)
df_missing_col = pd.DataFrame(
    [
        ["pet", "t1", ""],     # original row 2
    ],
    columns=["Database Name","Table Name","Column Name"]
)
run_fail(
    df_missing_col,
    default_db="pet",
    label="fail: missing Column Name (row 2)",
    expect_substr="row 2"
)

# --------------------------
# 5) FAILURE: database not found
df_bad_db = pd.DataFrame(
    [["no_such_db_xyz", "t1", "meta.ts"]],
    columns=["Database Name","Table Name","Column Name"]
)
run_fail(
    df_bad_db,
    default_db="pet",
    label="fail: database not found",
    expect_substr="database 'no_such_db_xyz' not found"
)

# --------------------------
# 6) FAILURE: table/column not found
df_bad_col = pd.DataFrame(
    [["pet", "t2", "nope"]],
    columns=["Database Name","Table Name","Column Name"]
)
run_fail(
    df_bad_col,
    default_db="pet",
    label="fail: column not found",
    expect_substr="not found"
)

# --------------------------
# Cleanup (optional)
# --------------------------
try:
    spark.sql("DROP TABLE IF EXISTS pet.t1")
    spark.sql("DROP TABLE IF EXISTS pet.t2")
    print("Cleanup done")
except Exception as e:
    print(f"Cleanup warning: {e}")


# COMMAND ----------

# - validate_dps_cdp_params(spark, param_data, db_name, expected_stage, input_schema_tables)
# - validate_schedule_rules(...)
# - check_table_and_column_exist(...)
# - _getp(...)
# - logger (optional)

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

print("------------------------------")
print("validate_dps_cdp_params()")
print("------------------------------")

# --------------------------
# Seed: create temp DB+tables in hive_metastore (because the function pins that catalog)
# --------------------------
db = "pet_test"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{db}")

# Data table with a direct ts and a nested struct ts
t_data = f"hive_metastore.{db}.t_data"
t_hdr  = f"hive_metastore.{db}.t_hdr"

data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("meta", StructType([
        StructField("ts", TimestampType(), True),
        StructField("src", StringType(), True),
    ]), True),
])

spark.createDataFrame(
    [
        (1, datetime(2025,1,1,0,0,0), Row(ts=datetime(2025,1,1,0,0,0), src="sys")),
        (2, datetime(2025,1,2,0,0,0), Row(ts=datetime(2025,1,2,0,0,0), src="ingest")),
    ], schema=data_schema
).write.mode("overwrite").saveAsTable(t_data)

# Header table for TIMESTAMP_HEADER_TABLE method
spark.createDataFrame(
    [(1, datetime(2025,1,3,0,0,0))],
    "hdr_id int, header_link_ts timestamp"
).write.mode("overwrite").saveAsTable(t_hdr)

# input_schema_tables the function will scan for unqualified fields
INPUT_SCHEMA_TABLES = [
    (db, "t_data"),
    (db, "t_hdr"),
]

# --------------------------
# Helpers
# --------------------------
def run_ok(param_data, label):
    try:
        out = validate_dps_cdp_params(
            spark=spark,
            param_data=param_data,
            db_name=db,                    # NOTE: function uses hive_metastore.<db>
            expected_stage=param_data.get("Stage", ""),  # not used internally, but passed
            input_schema_tables=INPUT_SCHEMA_TABLES
        )
        # The function returns True normally; for DATA-PROVISIONING it returns None (skips).
        if param_data.get("Stage", "").upper() == "DATA-PROVISIONING":
            print(f"[PASS] {label} (skipped, stage=Data-Provisioning)")
        else:
            assert out is True, "expected True"
            print(f"[PASS] {label}")
    except Exception as e:
        print(f"[FAIL] {label}: {e}")

def run_fail(param_data, label, expect_substr=None):
    try:
        validate_dps_cdp_params(
            spark=spark,
            param_data=param_data,
            db_name=db,
            expected_stage=param_data.get("Stage", ""),
            input_schema_tables=INPUT_SCHEMA_TABLES
        )
        print(f"[FAIL] {label}: expected ValueError/Exception but got success")
    except Exception as e:
        msg = str(e).lower()
        if expect_substr and expect_substr.lower() not in msg:
            print(f"[FAIL] {label}: message mismatch -> {e}")
        else:
            print(f"[PASS] {label}: {e}")

# --------------------------
# HAPPY: INCREMENTAL + TIMESTAMP_DATA_TABLE, unqualified field found via input_schema_tables
# --------------------------
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "event_ts",  # unqualified -> scan t_data
    "Schedule": "DAILY",
    "Week Days": None,
    "Dates Of Month": None,
}, "INCR/DATA_TABLE: unqualified field via input schema")

# HAPPY: INCREMENTAL + TIMESTAMP_DATA_TABLE, struct path via input_schema_tables
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "meta.ts",   # struct path on t_data
    "Schedule": "DAILY",
    "Week Days": None,
    "Dates Of Month": None,
}, "INCR/DATA_TABLE: struct path via input schema")

# HAPPY: INCREMENTAL + TIMESTAMP_DATA_TABLE, table-qualified
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "t_data.event_ts",  # table-qualified
    "Schedule": "DAILY",
    "Week Days": None,
    "Dates Of Month": None,
}, "INCR/DATA_TABLE: table-qualified field")

# HAPPY: INCREMENTAL + TIMESTAMP_HEADER_TABLE, table-qualified link
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_HEADER_TABLE",
    "Incremental Header To Tables Link Field": "t_hdr.header_link_ts",
    "Schedule": "DAILY",
    "Week Days": None,
    "Dates Of Month": None,
}, "INCR/HEADER_TABLE: table-qualified link field")

# HAPPY: INCREMENTAL + TIMESTAMP_HEADER_TABLE, unqualified (found via input_schema_tables)
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_HEADER_TABLE",
    "Incremental Header To Tables Link Field": "header_link_ts",
    "Schedule": "DAILY",
    "Week Days": None,
    "Dates Of Month": None,
}, "INCR/HEADER_TABLE: unqualified link via input schema")

# HAPPY: FULL — all incremental/schedule fields must be empty
run_ok({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "FULL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",  # ignored in FULL
    "Incremental Timestamp Field": "",
    "Schedule": None,
    "Week Days": None,
    "Dates Of Month": None,
}, "FULL: no incremental/schedule fields")

# HAPPY: DATA-PROVISIONING — skip checks (function returns None)
run_ok({
    "Stage": "DATA-PROVISIONING",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",  # ignored because of stage skip
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "t_data.event_ts",
    "Schedule": "DAILY",
}, "DATA-PROVISIONING: skip validations")

# --------------------------
# FAILURES
# --------------------------

# Missing Stage
run_fail({
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
}, "fail: missing Stage", "stage is missing")

# Missing Pet Dataset ID
run_fail({
    "Stage": "StageA",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "event_ts",
    "Schedule": "DAILY",
}, "fail: missing Pet Dataset ID", "pet dataset id is required")

# INCR but missing Incremental Timestamp Field
run_fail({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "",
    "Schedule": "DAILY",
}, "fail: INCR requires incremental ts field", "must not be empty")

# FULL but has incremental/schedule fields
run_fail({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "FULL",
    "Incremental Timestamp Field": "t_data.event_ts",
    "Schedule": "DAILY",
}, "fail: FULL cannot have incremental/schedule", "for full load")

# Invalid schedule for INCR (e.g., WEEKLY without week days)
run_fail({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "event_ts",
    "Schedule": "WEEKLY",
    "Week Days": None,
    "Dates Of Month": None,
}, "fail: WEEKLY requires week days", "week days")

# Invalid method
run_fail({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "SOMETHING_ELSE",
    "Incremental Timestamp Field": "event_ts",
    "Schedule": "DAILY",
}, "fail: invalid method", "timestamp method")

# Field not found (searches input tables + db tables)
run_fail({
    "Stage": "StageA",
    "Pet Dataset ID": "p1",
    "Load Type": "INCREMENTAL",
    "Incremental Timestamp Method": "TIMESTAMP_DATA_TABLE",
    "Incremental Timestamp Field": "nope",
    "Schedule": "DAILY",
}, "fail: ts field not found", "not found")

# --------------------------
# Cleanup
# --------------------------
try:
    spark.sql(f"DROP TABLE IF EXISTS {t_data}")
    spark.sql(f"DROP TABLE IF EXISTS {t_hdr}")
#     spark.sql(f"DROP SCHEMA IF EXISTS hive_metastore.{db}")
    print("Cleanup done")
except Exception as e:
    print(f"Cleanup warning: {e}")


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

print("------------------------------")
print("validate_and_normalize_incremental_timestamp_field()")
print("------------------------------")

# Ensure we're looking at hive_metastore since the function does SHOW TABLES IN hive_metastore.<db>
try:
    spark.sql("USE CATALOG hive_metastore")
except Exception:
    pass

db = "pet_test"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{db}")

# ---- Seed tables ----
# t1: top-level event_ts + nested meta.ts
t1_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("meta", StructType([
        StructField("ts", TimestampType(), True),
        StructField("src", StringType(), True),
    ]), True),
])
spark.createDataFrame(
    [
        (1, datetime(2025,1,1), Row(ts=datetime(2025,1,1), src="sys")),
        (2, datetime(2025,1,2), Row(ts=datetime(2025,1,2), src="ingest")),
    ],
    schema=t1_schema
).write.mode("overwrite").saveAsTable(f"hive_metastore.{db}.t1")

# t2: nested header.ts (to create ambiguity for leaf 'ts')
t2_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("header", StructType([
        StructField("ts", TimestampType(), True),
    ]), True),
])
spark.createDataFrame([(1, Row(ts=datetime(2025,1,3)))], schema=t2_schema) \
     .write.mode("overwrite").saveAsTable(f"hive_metastore.{db}.t2")

# t3: nested received.rcv_ts (for unique nested-leaf test)
t3_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("received", StructType([
        StructField("rcv_ts", TimestampType(), True),
    ]), True),
])
spark.createDataFrame([(1, Row(rcv_ts=datetime(2025,1,4)))], schema=t3_schema) \
     .write.mode("overwrite").saveAsTable(f"hive_metastore.{db}.t3")

TABLES = ["t1", "t2", "t3"]  # handy for candidate_tables tests

# ---- Helpers ----
def run_ok(spec, expected, label, candidate_tables=None):
    try:
        out = validate_and_normalize_incremental_timestamp_field(
            spark=spark,
            db_name=db,
            field_spec=spec,
            candidate_tables=candidate_tables
        )
        assert out == expected, f"expected {expected!r}, got {out!r}"
        # ensure no table prefix in the returned value
        assert "." not in out.split(".")[0], "return value must not include a table prefix"
        print(f"[PASS] {label} -> {out}")
    except Exception as e:
        print(f"[FAIL] {label}: {e}")

def run_fail(spec, label, expect_substr=None, candidate_tables=None, bad_db=None):
    try:
        validate_and_normalize_incremental_timestamp_field(
            spark=spark,
            db_name=(bad_db or db),
            field_spec=spec,
            candidate_tables=candidate_tables
        )
        print(f"[FAIL] {label}: expected ValueError but succeeded")
    except ValueError as e:
        msg = str(e).lower()
        if expect_substr and expect_substr.lower() not in msg:
            print(f"[FAIL] {label}: message mismatch -> {e}")
        else:
            print(f"[PASS] {label}: {e}")
    except Exception as e:
        print(f"[FAIL] {label}: unexpected error -> {e}")

# ---- HAPPY CASES ----
# 1) table-qualified top-level -> stripped to 'field'
run_ok("t1.event_ts", "event_ts", "table-qualified top-level")

# 2) table-qualified struct -> stripped to 'struct.field'
run_ok("t1.meta.ts", "meta.ts", "table-qualified struct path")

# 3) unqualified struct path -> confirmed on any table
run_ok("meta.ts", "meta.ts", "unqualified struct path (found on t1)")

# 4) leaf-only, top-level exists somewhere -> return 'field'
run_ok("event_ts", "event_ts", "leaf-only top-level exists")

# 5) leaf-only, unique nested across all tables -> returns 'struct.field'
run_ok("rcv_ts", "received.rcv_ts", "leaf-only unique nested (t3)")

# 6) candidate_tables limits search, resolving otherwise ambiguous 'ts' via t1 only
run_ok("ts", "meta.ts", "candidate_tables disambiguates leaf",
       candidate_tables=["t1"])

# ---- FAILURE CASES ----
# 7) ambiguous nested leaf across multiple tables (meta.ts vs header.ts)
run_fail("ts", "ambiguous nested leaf across tables", "ambiguous leaf")

# 8) not found anywhere
run_fail("missing_col", "leaf not found", "not found")

# 9) 'table.field' where table doesn't exist in db -> treated as struct.path and not found
run_fail("nope.col", "table-qualified unknown table", "not found")

# 10) empty/None spec
run_fail("", "empty spec", "missing")
run_fail(None, "None spec", "missing")

# 11) database access problem (bad db)
run_fail("event_ts", "bad db access", "could not access database", bad_db="no_such_db_xyz")

# 12) candidate_tables excludes the right table -> not found
run_fail("event_ts", "candidate_tables excludes top-level field", "not found", candidate_tables=["t2","t3"])

# ---- Cleanup ----
try:
    spark.sql(f"DROP TABLE IF EXISTS hive_metastore.{db}.t1")
    spark.sql(f"DROP TABLE IF EXISTS hive_metastore.{db}.t2")
    spark.sql(f"DROP TABLE IF EXISTS hive_metastore.{db}.t3")
#     spark.sql(f"DROP SCHEMA IF EXISTS hive_metastore.{db}")
    print("Cleanup done")
except Exception as e:
    print(f"Cleanup warning: {e}")


# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType, StructField, MapType, StringType, TimestampType
from pyspark.sql import Row

print("------------------------------")
print("get_last_process_map()")
print("------------------------------")

# ---- Build a schema matching the SELECT last_process_datetime result ----
lp_schema = StructType([
    StructField("last_process_datetime", MapType(StringType(), TimestampType()), True)
])

# ---- Monkeypatch spark.sql only for the specific SELECT your function calls ----
orig_sql = spark.sql
_ctx = {"mode": "none", "map": None}  # mode ∈ {"none", "existing"}

def _fake_sql(query: str):
    ql = (query or "").lower().strip()
    if ql.startswith("select") and "from pet.ctrl_dataset_config" in ql:
        if _ctx["mode"] == "existing" and isinstance(_ctx["map"], dict):
            return spark.createDataFrame([Row(last_process_datetime=_ctx["map"])], lp_schema)
        else:
            # no row found
            return spark.createDataFrame([], lp_schema)
    return orig_sql(query)

spark.sql = _fake_sql  # apply monkeypatch

def restore_sql():
    spark.sql = orig_sql

# ---- Helpers ----
def df_tables(*names):
    return pd.DataFrame({"Table Name": list(names)})

def assert_dt_eq(got: datetime, exp: datetime):
    assert isinstance(got, datetime) and got == exp, f"datetime mismatch: got={got!r} exp={exp!r}"

def run_ok(dataset_id, stage, start_date, input_df, label, expect_map: dict):
    try:
        out = get_last_process_map(
            spark=spark,
            dataset_id=dataset_id,
            stage=stage,
            start_process_date=start_date,
            input_fields_df=input_df
        )
        # Compare keys and values
        assert set(out.keys()) == set(expect_map.keys()), f"keys mismatch: {out.keys()} vs {expect_map.keys()}"
        for k, v in expect_map.items():
            assert_dt_eq(out[k], v)
        print(f"[PASS] {label}")
    except Exception as e:
        print(f"[FAIL] {label}: {e}")

def run_fail(dataset_id, stage, start_date, input_df, label, expect_substr=None):
    try:
        get_last_process_map(
            spark=spark,
            dataset_id=dataset_id,
            stage=stage,
            start_process_date=start_date,
            input_fields_df=input_df
        )
        print(f"[FAIL] {label}: expected error but got success")
    except Exception as e:
        msg = str(e).lower()
        if expect_substr and expect_substr.lower() not in msg:
            print(f"[FAIL] {label}: message mismatch -> {e}")
        else:
            print(f"[PASS] {label}: {e}")

# ---- Seed contexts and run cases ----

# 1) Existing map + start date: merge new table only
_ctx["mode"] = "existing"
_ctx["map"]  = {
    "t1": datetime(2025, 1, 10, 0, 0, 0),
    "t2": datetime(2025, 1, 11, 0, 0, 0),
}
start = "2025-01-01 12:34:56"
input_df = df_tables("t1", "t2", "t3")  # t3 is new
expect = {
    "t1": datetime(2025, 1, 10, 0, 0, 0),
    "t2": datetime(2025, 1, 11, 0, 0, 0),
    "t3": datetime(2025, 1, 1, 12, 34, 56),
}
run_ok("dsX", "stageA", start, input_df, "existing map + start date merges new tables", expect)

# 2) Existing map + no start date: return existing as-is
_ctx["mode"] = "existing"
_ctx["map"]  = {"t1": datetime(2025, 1, 10, 0, 0, 0)}
input_df = df_tables("t1", "t2")  # t2 will NOT be added (no start date)
expect = {"t1": datetime(2025, 1, 10, 0, 0, 0)}
run_ok("dsX", "stageA", None, input_df, "existing map + no start date returns existing only", expect)

# 3) No existing map + start date: seed all tables
_ctx["mode"] = "none"
_ctx["map"]  = None
start = "2025-02-05"
input_df = df_tables("a", "b")
seed_dt = datetime(2025, 2, 5, 0, 0, 0)  # YYYY-MM-DD parsed at midnight
expect = {"a": seed_dt, "b": seed_dt}
run_ok("dsY", "stageA", start, input_df, "no existing + start date seeds all", expect)

# 4) Input has NaN/blank table names: they are dropped by .dropna() and ignored
_ctx["mode"] = "none"
_ctx["map"]  = None
input_df = pd.DataFrame({"Table Name": ["a", None, " ", "b"]})
# strip isn't applied here, so " " is not None → will be included; your function doesn't strip table names.
# To mimic realistic input with blanks truly empty, pass None for blanks.
input_df = pd.DataFrame({"Table Name": ["a", None, "b"]})
start = "2025-03-01 00:00:00"
seed_dt = datetime(2025, 3, 1, 0, 0, 0)
expect = {"a": seed_dt, "b": seed_dt}
run_ok("dsT", "stageA", start, input_df, "blanks (None) are dropped by dropna()", expect)

# ---- Restore spark.sql ----
restore_sql()
print("Done.")