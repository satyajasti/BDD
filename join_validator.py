import re
import pandas as pd
import snowflake.connector
from snowflake_connection import get_snowflake_connection  # Assuming you have this

# Function to check if a table physically exists in Snowflake
def check_table_exists(conn, database, schema, table_name):
    query = f"""
    SELECT COUNT(*)
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}'
    AND TABLE_NAME = '{table_name}'
    """
    cur = conn.cursor()
    cur.execute(query)
    exists = cur.fetchone()[0] > 0
    cur.close()
    return exists

# Function to run Anti-Join check
def run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{l} = b.{r}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT COUNT(*)
    FROM {left_table} a
    LEFT JOIN {right_table} b
    ON {join_condition}
    WHERE b.{right_keys[0]} IS NULL
    """
    cur = conn.cursor()
    cur.execute(query)
    missing_count = cur.fetchone()[0]
    cur.close()
    return missing_count

# Function to run Join Explosion check
def run_join_multiplicity_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{l} = b.{r}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT a.*
        FROM {left_table} a
        JOIN {right_table} b
        ON {join_condition}
    )
    """
    cur = conn.cursor()
    cur.execute(query)
    joined_count = cur.fetchone()[0]

    # Original count
    cur.execute(f"SELECT COUNT(*) FROM {left_table}")
    original_count = cur.fetchone()[0]
    cur.close()

    explosion = joined_count - original_count
    return explosion

# Function to check for NULL keys
def run_null_key_validation(conn, left_table, left_keys):
    null_conditions = ' OR '.join([f"{key} IS NULL" for key in left_keys])
    query = f"SELECT COUNT(*) FROM {left_table} WHERE {null_conditions}"
    cur = conn.cursor()
    cur.execute(query)
    null_count = cur.fetchone()[0]
    cur.close()
    return null_count

# Function to pull Spot-Check Samples
def run_spot_check_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{l} = b.{r}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT a.*, b.*
    FROM {left_table} a
    JOIN {right_table} b
    ON {join_condition}
    LIMIT 20
    """
    df = pd.read_sql(query, conn)
    return df

# Master Runner Function
def validate_joins_from_list(joins_list, conn):
    results = []
    for join in joins_list:
        left_table = join['left_table']
        right_table = join['right_table']
        left_keys = join['left_keys']
        right_keys = join['right_keys']
        join_type = join['join_type']

        # Only validate INNER JOIN for now
        if join_type != 'INNER JOIN':
            results.append({**join, "Validation_Status": "Skipped - Non-Inner Join"})
            continue

        # Check table existence
        try:
            left_db, left_schema, left_table_name = left_table.split(".")
            right_db, right_schema, right_table_name = right_table.split(".")
        except ValueError:
            results.append({**join, "Validation_Status": "Skipped - Table name invalid format"})
            continue

        left_exists = check_table_exists(conn, left_db, left_schema, left_table_name)
        right_exists = check_table_exists(conn, right_db, right_schema, right_table_name)

        if not (left_exists and right_exists):
            results.append({**join, "Validation_Status": "Skipped - Temp/Derived Table (Not Found)"})
            continue

        # If tables exist, perform validations
        try:
            missing_records = run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys)
            explosion_records = run_join_multiplicity_validation(conn, left_table, right_table, left_keys, right_keys)
            null_keys = run_null_key_validation(conn, left_table, left_keys)
            spot_check_df = run_spot_check_validation(conn, left_table, right_table, left_keys, right_keys)

            # You can save spot_check_df separately if needed

            validation_result = {
                **join,
                "Missing_Records": missing_records,
                "Join_Explosion": explosion_records,
                "Null_Join_Keys": null_keys,
                "Validation_Status": "Validated"
            }
            results.append(validation_result)

        except Exception as e:
            results.append({**join, "Validation_Status": f"Failed - {str(e)}"})

    return pd.DataFrame(results)

if __name__ == "__main__":
    conn, *_ = get_snowflake_connection("config.json")

    # Example joins_list loaded from sql_parser output
    joins_list = [
        {
            "left_table": "TEMP_DB.TEMP_SCHEMA.tableA",
            "right_table": "TEMP_DB.TEMP_SCHEMA.tableB",
            "left_keys": ["id"],
            "right_keys": ["id"],
            "join_type": "INNER JOIN"
        }
    ]

    result_df = validate_joins_from_list(joins_list, conn)
    result_df.to_excel("output/join_validation_results.xlsx", index=False)

    conn.close()
    print("✅ Validation completed and Excel saved!")
