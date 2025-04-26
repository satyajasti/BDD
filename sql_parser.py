import re
import pandas as pd
import os

def extract_joins_from_sql(sql_text):
    """
    Extracts all JOINs (INNER JOINs validated, others logged) from the given SQL text.
    Returns a list of dictionaries: left_table, right_table, left_keys, right_keys, join_type
    """
    # Clean SQL: remove line breaks and multiple spaces
    sql_text = re.sub(r'\s+', ' ', sql_text, flags=re.MULTILINE)

    # Pattern to find JOINs
    join_pattern = re.compile(
        r'(?i)FROM\s+([\w\.]+)(?:\s+\w+)?\s+'  # FROM left_table [alias]
        r'(INNER|LEFT|RIGHT)?\s*JOIN\s+([\w\.]+)(?:\s+\w+)?\s+ON\s+([^;]+?)(?=(?:\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bJOIN\b|$))'
    )

    join_matches = join_pattern.findall(sql_text)
    parsed_joins = []

    for match in join_matches:
        left_table = match[0]
        join_type = match[1].upper() if match[1] else 'INNER JOIN'  # Default to INNER JOIN
        right_table = match[2]
        on_condition = match[3]

        # Extract individual join columns
        left_keys = []
        right_keys = []

        # Find all colA = colB pairs in ON condition
        join_condition_pattern = re.compile(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', re.IGNORECASE)
        condition_matches = join_condition_pattern.findall(on_condition)

        for cond in condition_matches:
            left_alias_or_table, left_col, right_alias_or_table, right_col = cond
            left_keys.append(left_col)
            right_keys.append(right_col)

        parsed_joins.append({
            "left_table": left_table,
            "right_table": right_table,
            "left_keys": left_keys,
            "right_keys": right_keys,
            "join_type": join_type
        })

    return parsed_joins


def save_joins_to_excel(joins_list, output_path):
    """
    Save extracted joins to an Excel file for review.
    """
    records = []
    for join in joins_list:
        records.append({
            'Left_Table': join['left_table'],
            'Right_Table': join['right_table'],
            'Left_Join_Columns': ', '.join(join['left_keys']),
            'Right_Join_Columns': ', '.join(join['right_keys']),
            'Join_Type': join['join_type'],
            'Validation_Status': 'Pending'  # Will update later
        })

    df = pd.DataFrame(records)
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_excel(output_path, sheet_name='Parsed_Joins_Summary', index=False)
    print(f'✅ Parsed joins saved to: {output_path}')


if __name__ == "__main__":
    # Example usage
    sql_file_path = "input/your_query.sql"  # Put your SQL here
    output_excel_path = "output/parsed_joins_summary.xlsx"

    with open(sql_file_path, 'r') as file:
        sql_text = file.read()

    joins = extract_joins_from_sql(sql_text)
    save_joins_to_excel(joins, output_excel_path)
