import os
import pandas as pd
from snowflake_connection import get_snowflake_connection
import re

def smart_load_joins(input_file):
    df = pd.read_excel(input_file)

    # Standardize column names to match expected keys
    df.rename(columns={
        'Left_Table': 'left_table',
        'Right_Table': 'right_table',
        'Left_Join_Columns': 'left_keys',
        'Right_Join_Columns': 'right_keys',
        'Join_Type': 'join_type',
        'Join Keys Left': 'left_keys',
        'Join Keys Right': 'right_keys'
    }, inplace=True)

    # Auto-split keys into lists
    if 'left_keys' in df.columns:
        df['left_keys'] = df['left_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])
    if 'right_keys' in df.columns:
        df['right_keys'] = df['right_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])

    return df.to_dict(orient='records')

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

def run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{l} = b.{r}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT COUNT(*)
    FROM {left_table} a
    LEFT JOIN {right_table} b
    ON {join_condition}
    WHERE b.{right_keys[0]} IS NULL
    """
    print(f"\n📄 Anti-Join Query:\n{query}")
    cur = conn.cursor()
    cur.execute(query)
    missing_count = cur.fetchone()[0]
    cur.close()
    return missing_count

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
    print(f"\n📄 Join Multiplicity Query:\n{query}")
    cur = conn.cursor()
    cur.execute(query)
    joined_count = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {left_table}")
    original_count = cur.fetchone()[0]
    cur.close()

    explosion = joined_count - original_count
    return explosion

def run_null_key_validation(conn, left_table, left_keys):
    null_conditions = ' OR '.join([f"{key} IS NULL" for key in left_keys])
    query = f"SELECT COUNT(*) FROM {left_table} WHERE {null_conditions}"
    print(f"\n📄 Null Key Query:\n{query}")
    cur = conn.cursor()
    cur.execute(query)
    null_count = cur.fetchone()[0]
    cur.close()
    return null_count

def run_spot_check_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{l} = b.{r}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT a.*, b.*
    FROM {left_table} a
    JOIN {right_table} b
    ON {join_condition}
    LIMIT 20
    """
    print(f"\n📄 Spot-Check Query:\n{query}")
    df = pd.read_sql(query, conn)
    return df

def validate_joins_from_list(joins_list, conn):
    results = []
    for join in joins_list:
        left_table = join['left_table']
        right_table = join['right_table']
        left_keys = join['left_keys']
        right_keys = join['right_keys']
        join_type = join['join_type']

        print(f"\n🔄 Validating Join: {left_table} {join_type} {right_table} ON {', '.join(left_keys)}")

        if join_type != 'INNER JOIN':
            results.append({**join, "Validation_Status": "Skipped - Non-Inner Join"})
            print("⚠️ Skipped - Non-Inner Join")
            continue

        try:
            left_db, left_schema, left_table_name = left_table.split(".")
            right_db, right_schema, right_table_name = right_table.split(".")
        except ValueError:
            results.append({**join, "Validation_Status": "Skipped - Table name invalid format"})
            print("⚠️ Skipped - Table name invalid format")
            continue

        left_exists = check_table_exists(conn, left_db, left_schema, left_table_name)
        right_exists = check_table_exists(conn, right_db, right_schema, right_table_name)

        if not (left_exists and right_exists):
            results.append({**join, "Validation_Status": "Skipped - Temp/Derived Table (Not Found)"})
            print("⚠️ Skipped - Temp/Derived Table Not Found")
            continue

        try:
            missing_records = run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys)
            explosion_records = run_join_multiplicity_validation(conn, left_table, right_table, left_keys, right_keys)
            null_keys = run_null_key_validation(conn, left_table, left_keys)
            spot_check_df = run_spot_check_validation(conn, left_table, right_table, left_keys, right_keys)

            validation_result = {
                **join,
                "Missing_Records": missing_records,
                "Join_Explosion": explosion_records,
                "Null_Join_Keys": null_keys,
                "Validation_Status": "Validated"
            }
            results.append(validation_result)

            print("✅ Validation Success")

        except Exception as e:
            results.append({**join, "Validation_Status": f"Failed - {str(e)}"})
            print(f"❌ Validation Failed: {e}")

    return pd.DataFrame(results)

if __name__ == "__main__":
    input_file = "output/smart_parsed_joins.xlsx"
    output_file = "output/join_validation_results.xlsx"

    print(f"\n🔵 Reading Input Excel: {input_file}")
    print(f"🔵 Output will be saved at: {output_file}")

    conn, *_ = get_snowflake_connection("config.json")

    joins_list = smart_load_joins(input_file)

    result_df = validate_joins_from_list(joins_list, conn)

    os.makedirs("output", exist_ok=True)
    result_df.to_excel(output_file, index=False)

    conn.close()

    print("\n✅ Validation Completed Successfully!")
    print(f"📦 Results saved at: {output_file}")
