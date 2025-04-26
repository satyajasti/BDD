import re
import pandas as pd

def clean_condition_functions(condition):
    # Remove TRIM(), CAST(), UPPER(), LOWER(), etc.
    condition = re.sub(r'TRIM\((.*?)\)', r'\1', condition, flags=re.IGNORECASE)
    condition = re.sub(r'CAST\((.*?)\s+AS\s+.*?\)', r'\1', condition, flags=re.IGNORECASE)
    condition = re.sub(r'UPPER\((.*?)\)', r'\1', condition, flags=re.IGNORECASE)
    condition = re.sub(r'LOWER\((.*?)\)', r'\1', condition, flags=re.IGNORECASE)
    return condition

def smart_extract_joins(sql_text):
    sql_text = re.sub(r'\s+', ' ', sql_text.strip())  # Flatten spaces
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

            if is_valid_table_name(table):
                if is_valid_alias(possible_alias):
                    alias_mapping[possible_alias] = table
                    current_left_alias = possible_alias
                    current_left_table = table
                    i += 2
                else:
                    current_left_alias = None
                    current_left_table = table
                    i += 1
            else:
                i += 1

        elif token in ('JOIN', 'INNER', 'LEFT', 'RIGHT'):
            join_type = 'INNER JOIN'
            if token != 'JOIN':
                join_type = token + ' JOIN'
                i += 1

            if i + 1 < len(tokens):
                right_table = tokens[i + 1]
                right_alias = None

                if is_valid_table_name(right_table):
                    if i + 2 < len(tokens) and is_valid_alias(tokens[i + 2]):
                        right_alias = tokens[i + 2]
                        alias_mapping[right_alias] = right_table
                        i += 2
                    else:
                        i += 1

                    join_keys_left = []
                    join_keys_right = []

                    while i < len(tokens) and tokens[i].upper() != 'ON':
                        i += 1
                    if i < len(tokens) and tokens[i].upper() == 'ON':
                        i += 1
                        condition = ''
                        while i < len(tokens) and tokens[i].upper() not in ('INNER', 'LEFT', 'RIGHT', 'JOIN', 'WHERE', 'GROUP', 'ORDER'):
                            condition += tokens[i] + ' '
                            i += 1

                        condition = clean_condition_functions(condition)
                        join_matches = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', condition, re.IGNORECASE)
                        for match in join_matches:
                            left_alias, left_col, right_alias2, right_col = match
                            join_keys_left.append(left_col)
                            join_keys_right.append(right_col)

                    joins.append({
                        'Left_Table': alias_mapping.get(current_left_alias, 'Derived/Temp'),
                        'Left_Alias': current_left_alias,
                        'Right_Table': right_table,
                        'Right_Alias': right_alias,
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
        else:
            i += 1

    return joins

def is_valid_table_name(name):
    return name.count('.') >= 1 and not name.upper() in ('JOIN', 'SELECT', '(', ')')

def is_valid_alias(name):
    return not name.upper() in ('JOIN', 'ON', 'INNER', 'LEFT', 'RIGHT', 'SELECT', 'WHERE', 'GROUP', 'ORDER', 'BY', 'FROM')

if __name__ == "__main__":
    with open("input/your_query.sql", 'r') as f:
        sql_text = f.read()

    joins = smart_extract_joins(sql_text)

    df = pd.DataFrame(joins)
    df.to_excel("output/smart_parsed_joins.xlsx", index=False)

    print("✅ Smart join parsing completed!")
    print(df)
