import re
import json
import pandas as pd

# ---- CONFIG ----
PARAMS_FILE = "parameters.json"
SQL_FILE = "query.sql"
ENV = "dev"
OUTPUT_EXCEL = "regex_extracted_schema_table.xlsx"

def load_parameters(path):
    with open(path, "r") as f:
        return json.load(f)

def replace_placeholders(sql, params):
    keys = re.findall(r"\$\{([^}]+)\}", sql)
    for key in set(keys):
        resolved = params.get(key, "TO_BE_RESOLVED").replace("${env}", ENV)
        sql = sql.replace(f"${{{key}}}", resolved)
    return sql

def extract_tables_by_regex(sql):
    pattern = re.compile(r"(?:from|join)\s+([a-zA-Z0-9_\$]+)\.([a-zA-Z0-9_\$]+)", re.IGNORECASE)
    matches = pattern.findall(sql)
    return [{"tbl_schema": schema, "Table": table} for schema, table in matches]

def main():
    print(" Loading parameters...")
    all_params = load_parameters(PARAMS_FILE)
    param_map = {k: v for k, v in all_params.items() if isinstance(v, str)}

    print(" Reading query.sql...")
    with open(SQL_FILE, "r") as f:
        raw_sql = f.read()

    print(" Replacing placeholders...")
    resolved_sql = replace_placeholders(raw_sql, param_map)

    print(" Extracting schema.table with regex...")
    extracted = extract_tables_by_regex(resolved_sql)

    df = pd.DataFrame(extracted).drop_duplicates()
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(f" Tables saved to '{OUTPUT_EXCEL}'")

if __name__ == "__main__":
    main()
