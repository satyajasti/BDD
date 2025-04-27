import sqlparse
from sqlparse.tokens import Keyword, Name, Comment

def advanced_clean_sql(sql_text):
    """
    Cleans SQL:
    - Removes comments
    - Normalizes spaces
    - Uppercases keywords
    - Lowercases identifiers (optional)
    - Re-formats nicely
    """
    parsed = sqlparse.parse(sql_text)[0]
    cleaned_tokens = []

    for token in parsed.flatten():
        if token.ttype in Comment:
            continue  # Skip comments
        elif token.ttype in Keyword:
            cleaned_tokens.append(str(token).upper())  # Make SQL keywords UPPERCASE
        elif token.ttype in Name:
            cleaned_tokens.append(str(token))  # Optional: .lower() if you want identifiers lowercase
        elif not token.is_whitespace:
            cleaned_tokens.append(str(token))

    # Join everything back
    cleaned_sql = ' '.join(cleaned_tokens)

    # Final formatting
    formatted_sql = sqlparse.format(
        cleaned_sql,
        keyword_case='upper',
        identifier_case='lower',  # Optional: all table/column names lowercase
        strip_comments=True,
        reindent=True,  # <-- Makes SELECT, FROM, JOIN, etc. go on new lines
        indent_width=2
    )

    return formatted_sql

if __name__ == "__main__":
    input_raw_sql = "input/raw_query.sql"
    output_clean_sql = "input/cleaned_query.sql"

    with open(input_raw_sql, 'r') as f:
        raw_sql = f.read()

    cleaned_sql = advanced_clean_sql(raw_sql)

    with open(output_clean_sql, 'w') as f:
        f.write(cleaned_sql)

    print(f"✅ Advanced cleaned SQL saved to {output_clean_sql}")
