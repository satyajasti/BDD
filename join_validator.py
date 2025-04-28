import pandas as pd
import snowflake.connector
from snowflake_connection import get_snowflake_connection
import os

def clean_dataframe(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    return df

def safe_full_column(alias, colname):
    if any(c in colname for c in (' ', '-', '#', '$')) or not colname.islower():
        return f'{alias}."{colname}"'
    else:
        return f'{alias}.{colname}'

def smart_load_joins(input_file):
    df = pd.read_excel(input_file)
    
    # Normalize all column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Rename if needed (safe)
    df.rename(columns={
        'left_table': 'left_table',
        'right_table': 'right_table',
        'left_join_columns': 'left_keys',
        'right_join_columns': 'right_keys',
        'join_keys_left': 'left_keys',
        'join_keys_right': 'right_keys',
        'join_type': 'join_type'
    }, inplace=True)
    
    # Auto split keys
    if 'left_keys' in df.columns:
        df['left_keys'] = df['left_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])
    if 'right_keys' in df.columns:
        df['right_keys'] = df['right_keys'].apply(lambda x: str(x).split(', ') if pd.notnull(x) else [])
    
    return df.to_dict(orient='records')

def check_table_exists(conn, database, schema, table_name):
    query = f"""
    SELECT COUNT(*)
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema.upper()}'
      AND TABLE_NAME = '{table_name.upper()}'
    """
    print(f"\n🔍 Checking Table Existence with Query:\n{query}")

    cur = conn.cursor()
    cur.execute(query)
    exists = cur.fetchone()[0] > 0
    cur.close()
    return exists

def validate_joins_from_list(joins_list, conn):
    results = []
    
    for join in joins_list:
        left_table = join['left_table']
        right_table = join['right_table']
        left_keys = join['left_keys']
        right_keys = join['right_keys']
        join_type = join['join_type']

        try:
            print(f"\n🔄 Validating Join: {left_table} {join_type} {right_table} ON {', '.join(left_keys)}")

            left_db, left_schema, left_table_name = left_table.split('.')
            right_db, right_schema, right_table_name = right_table.split('.')

            if not (check_table_exists(conn, left_db, left_schema, left_table_name) and
                    check_table_exists(conn, right_db, right_schema, right_table_name)):
                print(f"⚠️ Skipped Join: One of the tables doesn't exist.")
                results.append({
                    **join,
                    "Validation_Status": "Skipped - Table Missing"
                })
                continue

            # Safe ON condition
            join_condition = ' AND '.join([
                f"{safe_full_column('a', l)} = {safe_full_column('b', r)}"
                for l, r in zip(left_keys, right_keys)
            ])

            # 1. Anti-Join Query (with Subquery)
            select_columns = ', '.join([safe_full_column('a', l) for l in left_keys])
            where_null_condition = ' AND '.join([
                f"{safe_full_column('b', r)} IS NULL" for r in right_keys
            ])

            anti_join_query = f"""
            SELECT COUNT(*)
            FROM (
                SELECT {select_columns}
                FROM {left_table} a
                LEFT JOIN {right_table} b
                  ON {join_condition}
                WHERE {where_null_condition}
            )
            """
            print(f"\n📄 Anti-Join Query:\n{anti_join_query}")
            missing_records = execute_count_query(conn, anti_join_query)

            # 2. Multiplicity Check
            multiplicity_query = f"""
            SELECT SUM(multi_count - 1)
            FROM (
                SELECT COUNT(*) AS multi_count
                FROM {left_table} a
                JOIN {right_table} b
                  ON {join_condition}
                GROUP BY {', '.join([safe_full_column('a', k) for k in left_keys])}
            )
            """
            print(f"\n📄 Multiplicity Query:\n{multiplicity_query}")
            explosion_records = execute_sum_query(conn, multiplicity_query)

            # 3. Null Key Check
            null_query = f"""
            SELECT COUNT(*)
            FROM {left_table}
            WHERE {" OR ".join([f"{safe_full_column('', k)} IS NULL" for k in left_keys])}
            """
            print(f"\n📄 Null Key Check Query:\n{null_query}")
            null_keys = execute_count_query(conn, null_query)

            # 4. Spot Check Query
            spot_check_query = f"""
            SELECT *
            FROM {left_table} a
            JOIN {right_table} b
              ON {join_condition}
            LIMIT 10
            """
            print(f"\n📄 Spot Check Query:\n{spot_check_query}")

            validation_result = {
                **join,
                "Anti_Join_Query": anti_join_query,
                "Multiplicity_Query": multiplicity_query,
                "Null_Key_Query": null_query,
                "Spot_Check_Query": spot_check_query,
                "Missing_Records": missing_records,
                "Join_Explosion": explosion_records,
                "Null_Join_Keys": null_keys,
                "Validation_Status": "Validated"
            }
            results.append(validation_result)

        except Exception as e:
            print(f"⚠️ Error during join validation: {e}")
            results.append({
                **join,
                "Validation_Status": f"Error - {str(e)}"
            })
    
    return pd.DataFrame(results)

def execute_count_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    count = cur.fetchone()[0]
    cur.close()
    return count

def execute_sum_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    cur.close()
    if result and result[0] is not None:
        return int(result[0])
    return 0

if __name__ == "__main__":
    input_file = "output/smart_parsed_joins.xlsx"
    output_file = "output/join_validation_results.xlsx"

    print(f"\n🔵 Reading Input Excel: {input_file}")
    print(f"🔵 Output will be saved at: {output_file}")

    conn, *_ = get_snowflake_connection("config.json")

    joins_list = smart_load_joins(input_file)

    result_df = validate_joins_from_list(joins_list, conn)
    result_df = clean_dataframe(result_df)
    result_df.to_excel(output_file, index=False)

    print("\n✅ Validation completed successfully!")
    print(f"📦 Results saved at: {output_file}")
