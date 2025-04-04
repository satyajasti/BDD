import pandas as pd
import os
import time
import logging
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from snowflake_connection import get_snowflake_connection

logging.basicConfig(filename="compare_tables.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_comparison_config(excel_path):
    df = pd.read_excel(excel_path)
    expected_cols = [
        "Source_Database", "Source_Schema", "Source_Table",
        "Target_Database", "Target_Schema", "Target_Table", "Compare_Columns"
    ]
    if list(df.columns) != expected_cols:
        raise ValueError(f" Excel columns do not match expected headers: {expected_cols}")
    logging.info("Comparison config loaded successfully.")
    return df

def write_df_to_sheet(wb, sheet_name, df):
    logging.info(f" Writing to sheet: {sheet_name} (Rows: {len(df)})")
    ws = wb.create_sheet(title=sheet_name)
    for row in dataframe_to_rows(df, index=False, header=True):
        if any(row):
            ws.append(row)

def fetch_query_result(query, conn):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns).astype(str)
    finally:
        cursor.close()

def run_table_comparison(config_file, output_dir="reports/table_comparisons"):
    os.makedirs(output_dir, exist_ok=True)
    config_df = read_comparison_config(config_file)

    conn, _, _, _, _, _ = get_snowflake_connection("config.json")

    summary_rows = []

    for index, row in config_df.iterrows():
        src_db = row['Source_Database'].strip()
        src_table = row['Source_Table'].strip()
        src_schema = row['Source_Schema'].strip()
        tgt_db = row['Target_Database'].strip()
        tgt_table = row['Target_Table'].strip()
        tgt_schema = row['Target_Schema'].strip()
        columns = [col.strip() for col in row['Compare_Columns'].split(',')]

        logging.info(f" Comparing [{index+1}/{len(config_df)}]: {src_db}.{src_schema}.{src_table} vs {tgt_db}.{tgt_schema}.{tgt_table}")

        try:
            src_query = f"SELECT {', '.join(columns)} FROM {src_db}.{src_schema}.{src_table}"
            logging.info(f" Source Query: {src_query}")
            src_df = fetch_query_result(src_query, conn)
            logging.info(f" Source rows fetched: {len(src_df)}")

            tgt_query = f"SELECT {', '.join(columns)} FROM {tgt_db}.{tgt_schema}.{tgt_table}"
            logging.info(f" Target Query: {tgt_query}")
            tgt_df = fetch_query_result(tgt_query, conn)
            logging.info(f" Target rows fetched: {len(tgt_df)}")

            # Compare DataFrames using merge
            merged = src_df.merge(tgt_df, how='outer', on=columns, indicator=True)
            only_in_source = merged[merged['_merge'] == 'left_only'][columns].head(10)
            only_in_target = merged[merged['_merge'] == 'right_only'][columns].head(10)
            mismatched_df = pd.DataFrame(columns=columns)  # Skipped mismatch by counts for performance

            output_file = os.path.join(output_dir, f"{src_table}_vs_{tgt_table}.xlsx")
            wb = Workbook()
            wb.remove(wb.active)

            write_df_to_sheet(wb, "only_in_source", only_in_source)
            write_df_to_sheet(wb, "only_in_target", only_in_target)
            write_df_to_sheet(wb, "mismatches", mismatched_df)

            wb.save(output_file)
            logging.info(f" Comparison Excel saved: {output_file}")

            summary_rows.append({
                "Source_Database": src_db,
                "Target_Database": tgt_db,
                "Source_Table": src_table,
                "Target_Table": tgt_table,
                "Only_in_Source": len(only_in_source),
                "Only_in_Target": len(only_in_target),
                "Mismatches": 0,
                "Status": " Match" if len(only_in_source) == 0 and len(only_in_target) == 0 else " Diff"
            })

        except Exception as e:
            logging.error(f" Error comparing tables {src_table} and {tgt_table}: {e}")
            summary_rows.append({
                "Source_Database": src_db,
                "Target_Database": tgt_db,
                "Source_Table": src_table,
                "Target_Table": tgt_table,
                "Only_in_Source": "-",
                "Only_in_Target": "-",
                "Mismatches": "-",
                "Status": f"Error: {str(e)}"
            })

    conn.close()
    logging.info(" Snowflake connection closed.")

    summary_df = pd.DataFrame(summary_rows)
    summary_path = os.path.join(output_dir, "comparison_summary.xlsx")
    summary_df.to_excel(summary_path, index=False)
    logging.info(f" Comparison summary written to: {summary_path}")

if __name__ == "__main__":
    run_table_comparison("compare_config.xlsx")
