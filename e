import pandas as pd
import json
import os
import argparse
from snowflake_connectivity import get_snowflake_connection

CONFIG_FILE = "config.json"

def load_config_excel_path():
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
    excel_path = config["active_excel"]
    database = config["secondary_db"]["database"]
    return os.path.abspath(excel_path), database

def find_schema(conn, database, table):
    cur = conn.cursor()
    try:
        query = f"SELECT table_schema FROM {database}.information_schema.tables WHERE table_name = '{table.upper()}'"
        print(f" Running SQL: {query}")
        cur.execute(query)
        result = cur.fetchone()
        return result[0] if result else "NOT_FOUND"
    except Exception as e:
        print(f" Error checking table '{table}': {e}")
        return "NOT_FOUND"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input sheet from cmrcl validation")
    parser.add_argument("--sheet", required=True, help="Output sheet name for edl results")
    args = parser.parse_args()

    excel_path, edl_db = load_config_excel_path()

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f" Excel file not found at: {excel_path}")

    cmrcl_df = pd.read_excel(excel_path, sheet_name=args.input)
    conn, _, _ = get_snowflake_connection(CONFIG_FILE)

    results = []
    for _, row in cmrcl_df.iterrows():
        table = row["Table"]
        param = row["Parameter"]
        cmrcl_status = row["Status"]
        cmrcl_schema = row["Verified_Schema"]
        cmrcl_db = row["database"]

        if cmrcl_status == " FOUND":
            results.append({
                "Parameter": param,
                "Table": table,
                "database": cmrcl_db,
                "Verified_Schema": cmrcl_schema,
                "Status": cmrcl_status
            })
        else:
            schema = find_schema(conn, edl_db, table)
            status = " FOUND" if schema != "NOT_FOUND" else " NOT FOUND"
            results.append({
                "Parameter": param,
                "Table": table,
                "database": edl_db,
                "Verified_Schema": schema,
                "Status": status
            })

    result_df = pd.DataFrame(results)

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        result_df.to_excel(writer, sheet_name=args.sheet, index=False)

    print(f" Sheet '{args.sheet}' written to {excel_path}")

if __name__ == "__main__":
    main()
