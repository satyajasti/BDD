import sqlglot
import pandas as pd
import os

def extract_joins_using_sqlglot(sql_text):
    """
    Parses SQL using sqlglot and extracts all JOINs (INNER, LEFT, RIGHT, FULL, etc.)
    Resolves aliases to real table names.
    Returns a list of dictionaries.
    """
    parsed = sqlglot.parse_one(sql_text)
    joins_info = []
    alias_to_table = {}

    # First pass: find alias mappings from FROM and WITH clauses
    for from_exp in parsed.find_all(sqlglot.exp.From):
        for table in from_exp.find_all(sqlglot.exp.Table):
            if table.alias:
                alias_to_table[table.alias] = table.this.sql()
            else:
                alias_to_table[table.sql()] = table.sql()

    for with_exp in parsed.find_all(sqlglot.exp.With):
        for cte in with_exp.expressions:
            if isinstance(cte, sqlglot.exp.CTE):
                alias_to_table[cte.alias_or_name] = cte.this.sql()

    # Second pass: find JOINs
    for join in parsed.find_all(sqlglot.exp.Join):
        join_type = (join.args.get('kind') or 'INNER').upper()
        left = join.this
        right = join.expression
        on_condition = join.args.get('on')

        # Left Alias and Real Table
        left_alias = left.alias_or_name if left and hasattr(left, 'alias_or_name') else (left.sql() if left else 'UNKNOWN_LEFT')
        left_real_table = alias_to_table.get(left_alias, left_alias)

        # Right Alias and Real Table
        if right:
            if isinstance(right, sqlglot.exp.Table):
                right_alias = right.alias_or_name if right.alias_or_name else right.name
                right_real_table = alias_to_table.get(right_alias, right.sql())
            elif isinstance(right, sqlglot.exp.Alias):
                right_alias = right.alias_or_name if right.alias_or_name else right.this.sql()
                right_real_table = alias_to_table.get(right_alias, right.this.sql())
            elif isinstance(right, sqlglot.exp.Subquery):
                right_alias = right.alias_or_name if right.alias_or_name else 'SUBQUERY'
                right_real_table = f"SUBQUERY_{right_alias}" if right_alias else 'SUBQUERY'
            else:
                right_alias = 'UNKNOWN_RIGHT'
                right_real_table = 'UNKNOWN_RIGHT'
        else:
            right_alias = 'UNKNOWN_RIGHT'
            right_real_table = 'UNKNOWN_RIGHT'

        join_condition = on_condition.sql() if on_condition else 'UNKNOWN'

        joins_info.append({
            'Left_Alias': left_alias,
            'Left_Real_Table': left_real_table,
            'Right_Alias': right_alias,
            'Right_Real_Table': right_real_table,
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
