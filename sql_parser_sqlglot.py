import os
import re
import pandas as pd
import sqlparse
from sql_cleaner_advanced import advanced_clean_sql

def smart_extract_joins(sql_text):
    sql_text = re.sub(r'\s+', ' ', sql_text.strip())
    tokens = sql_text.split(' ')

    joins = []
    current_left_alias = None
    current_left_table = None
    alias_mapping = {}

    i = 0
    while i < len(tokens):
        token = tokens[i].upper()

        if token == 'FROM' and i + 2 < len(tokens):
            table = tokens[i + 1]
            possible_alias = tokens[i + 2]
            if possible_alias.upper() not in ('INNER', 'LEFT', 'RIGHT', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'OUTER'):
                alias_mapping[possible_alias] = table
                current_left_alias = possible_alias
                current_left_table = table
                i += 2
            else:
                current_left_alias = None
                current_left_table = table
                i += 1

        elif token in ('JOIN', 'INNER', 'LEFT', 'RIGHT'):
            join_type = 'INNER JOIN'
            if token == 'LEFT':
                if i + 2 < len(tokens) and tokens[i + 1].upper() == 'OUTER' and tokens[i + 2].upper() == 'JOIN':
                    join_type = 'LEFT OUTER JOIN'
                    i += 2
                elif i + 1 < len(tokens) and tokens[i + 1].upper() == 'JOIN':
                    join_type = 'LEFT JOIN'
                    i += 1
            elif token == 'RIGHT':
                if i + 1 < len(tokens) and tokens[i + 1].upper() == 'JOIN':
                    join_type = 'RIGHT JOIN'
                    i += 1
            elif token == 'INNER':
                if i + 1 < len(tokens) and tokens[i + 1].upper() == 'JOIN':
                    join_type = 'INNER JOIN'
                    i += 1

            if i + 1 < len(tokens):
                right_table = tokens[i + 1]

                # Skip joins with subqueries
                if right_table.startswith('(') or right_table.upper().startswith('SELECT'):
                    i += 1
                    continue

                right_alias = None
                if i + 2 < len(tokens):
                    possible_alias = tokens[i + 2]
                    if possible_alias.upper() not in ('ON', 'INNER', 'LEFT', 'RIGHT', 'JOIN', 'WHERE', 'GROUP', 'ORDER', 'OUTER'):
                        right_alias = possible_alias
                        alias_mapping[right_alias] = right_table
                        i += 1

                join_keys_left = []
                join_keys_right = []

                while i < len(tokens) and tokens[i].upper() != 'ON':
                    i += 1
                if i < len(tokens) and tokens[i].upper() == 'ON':
                    i += 1
                    condition = ''
                    while i < len(tokens) and tokens[i].upper() not in ('INNER', 'LEFT', 'RIGHT', 'JOIN', 'WHERE', 'GROUP', 'ORDER', 'OUTER'):
                        condition += tokens[i] + ' '
                        i += 1
                    join_matches = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', condition, re.IGNORECASE)
                    for match in join_matches:
                        left_alias, left_col, right_alias2, right_col = match
                        join_keys_left.append(left_col)
                        join_keys_right.append(right_col)

                joins.append({
                    'Left_Table': alias_mapping.get(current_left_alias, 'Derived/Temp'),
                    'Left_Alias': current_left_alias if current_left_alias not in (None, '.') else None,
                    'Right_Table': right_table,
                    'Right_Alias': right_alias if right_alias not in (None, '.') else None,
                    'Join_Type': join_type,
                    'Join_Keys_Left': ', '.join(join_keys_left),
                    'Join_Keys_Right': ', '.join(join_keys_right)
                })

                current_left_alias = right_alias
                current_left_table = right_table
            else:
                i += 1
        else:
            i += 1

    return joins

if __name__ == "__main__":
    input_raw_sql = "input/raw_query.sql"
    output_clean_sql = "input/cleaned_query.sql"
    output_joins_excel = "output/smart_parsed_joins.xlsx"

    os.makedirs("input", exist_ok=True)
    os.makedirs("output", exist_ok=True)

    # Step 1: Clean SQL
    with open(input_raw_sql, 'r') as f:
        raw_sql = f.read()

    cleaned_sql = advanced_clean_sql(raw_sql)

    with open(output_clean_sql, 'w') as f:
        f.write(cleaned_sql)

    print(f" Advanced cleaned SQL saved to {output_clean_sql}")

    # Step 2: Parse joins
    joins = smart_extract_joins(cleaned_sql)

    df = pd.DataFrame(joins)
    df.to_excel(output_joins_excel, index=False)

    print(f" Smart joins extracted and saved to {output_joins_excel}")
    print(df)
