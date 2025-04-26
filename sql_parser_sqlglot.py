import sqlglot
import pandas as pd
import os

def extract_joins_using_sqlglot(sql_text):
    """
    Parses SQL using sqlglot and extracts all JOINs (INNER, LEFT, RIGHT, FULL, etc.)
    Returns a list of dictionaries.
    """
    parsed = sqlglot.parse_one(sql_text)
    joins_info = []

    # Find all JOIN nodes in the AST
    for join in parsed.find_all(sqlglot.exp.Join):
        join_type = (join.args.get('kind') or 'INNER').upper()  # default to INNER JOIN if kind missing
        left = join.this  # Left side of join
        right = join.expression  # Right side of join
        on_condition = join.args.get('on')

        # Try extracting table names
        left_table = left.sql() if left else 'UNKNOWN_LEFT'
        right_table = right.sql() if right else 'UNKNOWN_RIGHT'
        join_condition = on_condition.sql() if on_condition else 'UNKNOWN'

        joins_info.append({
            'Left_Table': left_table,
            'Right_Table': right_table,
            'Join_Type': join_type,
            'Join_Condition': join_condition
        })

    return joins_info


def save_joins_to_excel(joins_list, output_path):
    """
    Save extracted joins into Excel for review.
    """
    df = pd.DataFrame(joins_list)
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_excel(output_path, index=False)
    print(f'✅ Parsed joins saved to: {output_path}')


if __name__ == "__main__":
    sql_file_path = "input/your_query.sql"
    output_excel_path = "output/parsed_joins_sqlglot.xlsx"

    # Read SQL
    with open(sql_file_path, 'r') as f:
        sql_text = f.read()

    # Extract joins
    joins = extract_joins_using_sqlglot(sql_text)

    # Save to Excel
    save_joins_to_excel(joins, output_excel_path)

    # Print extracted joins
    for join in joins:
        print(join)
