import re
import pandas as pd

def smart_extract_joins(sql_text):
    """
    Smarter SQL JOIN extractor: walks through SQL, handles aliases, detects JOINs properly.
    """
    sql_text = re.sub(r'\s+', ' ', sql_text.strip())  # Flatten spaces
    tokens = sql_text.split(' ')

    joins = []
    current_left_alias = None
    current_left_table = None
    alias_mapping = {}

    i = 0
    while i < len(tokens):
        token = tokens[i].upper()

        # Detect FROM <table> <alias>
        if token == 'FROM' and i + 2 < len(tokens):
            table = tokens[i + 1]
            possible_alias = tokens[i + 2]

            if possible_alias.upper() not in ('INNER', 'LEFT', 'RIGHT', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER'):
                alias_mapping[possible_alias] = table
                current_left_alias = possible_alias
                current_left_table = table
                i += 2
            else:
                current_left_alias = None
                current_left_table = table
                i += 1

        # Detect JOIN
        elif token in ('JOIN', 'INNER', 'LEFT', 'RIGHT'):
            join_type = 'INNER JOIN'  # default
            if token != 'JOIN':
                join_type = token + ' JOIN'
                i += 1  # skip "JOIN"

            if i + 2 < len(tokens):
                right_table = tokens[i + 1]
                possible_alias = tokens[i + 2]

                right_alias = None
                if possible_alias.upper() not in ('ON', 'INNER', 'LEFT', 'RIGHT', 'JOIN', 'WHERE', 'GROUP', 'ORDER'):
                    right_alias = possible_alias
                    alias_mapping[right_alias] = right_table
                    i += 1  # skip alias

                # Now look for ON condition
                join_keys_left = []
                join_keys_right = []

                # Find ON
                while i < len(tokens) and tokens[i].upper() != 'ON':
                    i += 1
                if i < len(tokens) and tokens[i].upper() == 'ON':
                    i += 1  # move to condition
                    # Start capturing condition
                    condition = ''
                    while i < len(tokens) and tokens[i].upper() not in ('INNER', 'LEFT', 'RIGHT', 'JOIN', 'WHERE', 'GROUP', 'ORDER'):
                        condition += tokens[i] + ' '
                        i += 1
                    # Parse condition for join keys
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

                # After JOIN, the right side becomes the new left
                current_left_alias = right_alias
                current_left_table = right_table
            else:
                i += 1
        else:
            i += 1

    return joins

if __name__ == "__main__":
    # Example usage
    with open("input/your_query.sql", 'r') as f:
        sql_text = f.read()

    joins = smart_extract_joins(sql_text)

    df = pd.DataFrame(joins)
    df.to_excel("output/smart_parsed_joins.xlsx", index=False)

    print("Smart join parsing completed!")
    print(df)
