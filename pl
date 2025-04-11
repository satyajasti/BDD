import re
import json
import sqlglot
import pandas as pd

# ---- CONFIGURATION ----
PARAMS_FILE = "parameters.json"
SQL_FILE = "query.sql"
ENV = "dev"
OUTPUT_EXCEL = "extracted_schema_table.xlsx"

def load_parameters(path):
    with open(path, "r") as f:
        return json.load(f)

def replace_placeholders(sql, params):
    keys = re.findall(r"\$\{([^}]+)\}", sql)
    for key in set(keys):
        resolved = params.get(key, "TO_BE_RESOLVED").replace("${env}", ENV)
        sql = sql.replace(f"${{{key}}}", resolved)
    return sql

def loosen_sql(sql):
    # Replace any ON (CASE...) = ... with ON 1=1 to avoid parsing errors
    return re.sub(r"ON\s*\(\s*CASE.*?END\s*\)", "ON 1=1", sql, flags=re.DOTALL)

def extract_tables(sql):
    parsed = sqlglot.parse_one(sql)
    tables = parsed.find_all(sqlglot.exp.Table)
    table_rows = []

    for t in tables:
        schema = t.args.get("db") or "TO_BE_RESOLVED"
        table = t.name or "UNKNOWN"
        table_rows.append({"tbl_schema": schema, "Table": table})
    return pd.DataFrame(table_rows)

def main():
    print(" Loading parameters from JSON...")
    all_params = load_parameters(PARAMS_FILE)
    param_map = {k: v for k, v in all_params.items() if isinstance(v, str)}

    print(" Reading query.sql...")
    with open(SQL_FILE, "r") as f:
        raw_sql = f.read()

    print(" Replacing placeholders...")
    cleaned_sql = replace_placeholders(raw_sql, param_map)

    print(" Loosening complex JOINs for parsing...")
    loosened_sql = loosen_sql(cleaned_sql)

    print(" Parsing tables using sqlglot...")
    df = extract_tables(loosened_sql)

    print(f" Saving to {OUTPUT_EXCEL}")
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(" Done.")

if __name__ == "__main__":
    main()
