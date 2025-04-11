import json
import sqlglot
import pandas as pd
import re

def load_parameters(json_path):
    with open(json_path, "r") as f:
        return json.load(f)

def replace_query_parameters(query_text, parameters, env="dev"):
    placeholders = re.findall(r"\$\{([^}]+)\}", query_text)
    for key in set(placeholders):
        if key in parameters:
            resolved = parameters[key].replace("${env}", env)
        else:
            # If not found in JSON, use TO_BE_RESOLVED as schema
            resolved = "TO_BE_RESOLVED"
        query_text = query_text.replace(f"${{{key}}}", resolved)
    return query_text

def extract_schema_table(sql_text):
    parsed = sqlglot.parse_one(sql_text)
    tables = parsed.find_all(sqlglot.exp.Table)
    rows = []

    for t in tables:
        schema = t.args.get("db") or "TO_BE_RESOLVED"
        table = t.name or "UNKNOWN"
        rows.append({"tbl_schema": schema, "Table": table})

    df = pd.DataFrame(rows)
    df.to_excel("extracted_schema_table.xlsx", index=False)
    print(" Extracted schema-table mapping saved to 'extracted_schema_table.xlsx'")

def main():
    json_path = "parameters.json"
    sql_path = "query.sql"
    env = "dev"

    print(" Loading parameters...")
    parameters = load_parameters(json_path)

    print(" Reading query.sql...")
    with open(sql_path, "r") as f:
        raw_sql = f.read()

    print(" Replacing placeholders (fallback to TO_BE_RESOLVED)...")
    resolved_sql = replace_query_parameters(raw_sql, parameters, env)

    print(" Parsing query with sqlglot...")
    extract_schema_table(resolved_sql)

if __name__ == "__main__":
    main()
