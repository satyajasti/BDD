import sqlglot
import pandas as pd

def extract_schema_table(sql_text):
    parsed = sqlglot.parse_one(sql_text)
    tables = parsed.find_all(sqlglot.exp.Table)
    rows = []

    for t in tables:
        schema = t.args.get("db") or "Unknown"
        table = t.name
        rows.append({"tbl_schema": schema, "Table": table})

    df = pd.DataFrame(rows)
    df.to_excel("extracted_schema_table.xlsx", index=False)
    print(" Table-format schema extraction saved to 'extracted_schema_table.xlsx'")

def main():
    input_sql_file = "query.sql"
    with open(input_sql_file, "r") as file:
        sql_text = file.read()
    extract_schema_table(sql_text)

if __name__ == "__main__":
    main()
