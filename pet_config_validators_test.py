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
