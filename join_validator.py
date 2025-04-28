import os
import pandas as pd
from snowflake_connection import get_snowflake_connection
import re

def smart_load_joins(input_file):
    df = pd.read_excel(input_file)

    # Normalize all column names (lowercase, no spaces)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Rename normalized columns to expected names
    df.rename(columns={
        'left_table': 'left_table',
        'right_table': 'right_table',
        'left_join_columns': 'left_keys',
        'right_join_columns': 'right_keys',
        'join_keys_left': 'left_keys',
        'join_keys_right': 'right_keys',
        'join_type': 'join_type'
    }, inplace=True)

    # Auto-split keys into lists
    if 'left_keys' in df.columns:
        df['left_keys'] = df['left_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])
    if 'right_keys' in df.columns:
        df['right_keys'] = df['right_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])

    return df.to_dict(orient='records')

def clean_dataframe(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    return df

def smart_split_table(full_table_name):
    parts = full_table_name.strip().split('.')
    parts = [p.strip() for p in parts if p.strip() != '']
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    else:
        raise ValueError(f"Invalid table name format: {full_table_name}")

def safe_column(colname):
    if any(c in colname for c in (' ', '-', '#', '$')) or not colname.islower():
        return f'"{colname}"'
    else:
        return colname

def check_table_exists(conn, database, schema, table_name):
    schema = schema.upper()
    table_name = table_name.upper()
    query = f"""
    SELECT COUNT(*)
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}'
    AND TABLE_NAME = '{table_name}'
    """
    print(f"\n🔍 Checking Table Existence with Query:\n{query}")
    cur = conn.cursor()
    cur.execute(query)
    exists = cur.fetchone()[0] > 0
    cur.close()
    return exists

def run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{safe_column(l)} = b.{safe_column(r)}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT COUNT(*)
    FROM {left_table} a
    LEFT JOIN {right_table} b
    ON {join_condition}
    WHERE b.{safe_column(right_keys[0])} IS NULL
    """
    return query

def run_join_multiplicity_validation(left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{safe_column(l)} = b.{safe_column(r)}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT a.*
        FROM {left_table} a
        JOIN {right_table} b
        ON {join_condition}
    )
    """
    return query

def run_null_key_validation(left_table, left_keys):
    null_conditions = ' OR '.join([f"{safe_column(key)} IS NULL" for key in left_keys])
    query = f"SELECT COUNT(*) FROM {left_table} WHERE {null_conditions}"
    return query

def run_spot_check_validation(left_table, right_table, left_keys, right_keys):
    join_condition = ' AND '.join([f"a.{safe_column(l)} = b.{safe_column(r)}" for l, r in zip(left_keys, right_keys)])
    query = f"""
    SELECT a.*, b.*
    FROM {left_table} a
    JOIN {right_table} b
    ON {join_condition}
    LIMIT 20
    """
    return query

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
            left_db, left_schema, left_table_name = smart_split_table(left_table)
            right_db, right_schema, right_table_name = smart_split_table(right_table)
        except ValueError as e:
            results.append({**join, "Validation_Status": f"Skipped - {e}"})
            print(f"⚠️ Skipped - {e}")
            continue

        left_exists = check_table_exists(conn, left_db, left_schema, left_table_name)
        right_exists = check_table_exists(conn, right_db, right_schema, right_table_name)

        if not (left_exists and right_exists):
            results.append({**join, "Validation_Status": "Skipped - Temp/Derived Table (Not Found)"})
            print("⚠️ Skipped - Temp/Derived Table Not Found")
            continue

        try:
            anti_join_query = run_anti_join_validation(conn, left_table, right_table, left_keys, right_keys)
            multiplicity_query = run_join_multiplicity_validation(left_table, right_table, left_keys, right_keys)
            null_query = run_null_key_validation(left_table, left_keys)
            spot_check_query = run_spot_check_validation(left_table, right_table, left_keys, right_keys)

            cur = conn.cursor()

            print(f"\n📄 Executing Anti-Join Query:\n{anti_join_query}")
            cur.execute(anti_join_query)
            missing_count = cur.fetchone()[0]

            print(f"\n📄 Executing Multiplicity Query:\n{multiplicity_query}")
            cur.execute(multiplicity_query)
            joined_count = cur.fetchone()[0]

            cur.execute(f"SELECT COUNT(*) FROM {left_table}")
            original_count = cur.fetchone()[0]
            explosion = joined_count - original_count

            print(f"\n📄 Executing Null Key Query:\n{null_query}")
            cur.execute(null_query)
            null_count = cur.fetchone()[0]

            cur.close()

            validation_result = {
                **join,
                "Anti_Join_Query": anti_join_query,
                "Multiplicity_Query": multiplicity_query,
                "Null_Key_Query": null_query,
                "Spot_Check_Query": spot_check_query,
                "Missing_Records": missing_count,
                "Join_Explosion": explosion,
                "Null_Join_Keys": null_count,
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
    result_df = clean_dataframe(result_df)
    result_df.to_excel(output_file, index=False)

    conn.close()

    print("\n✅ Validation Completed Successfully!")
    print(f"📦 Results saved at: {output_file}")
