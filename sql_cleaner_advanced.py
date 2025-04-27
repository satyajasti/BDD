import sqlparse
import re
from sqlparse.tokens import Keyword, Name, Comment, Punctuation

def advanced_clean_sql(sql_text):
    """
    Cleans SQL:
    - Removes comments
    - Normalizes spaces
    - Uppercases keywords
    - Lowercases identifiers
    - Fixes dots (no space around '.')
    - Re-indents nicely
    """
    parsed = sqlparse.parse(sql_text)[0]
    cleaned_tokens = []

    prev_token = None
    for token in parsed.flatten():
        if token.ttype in Comment:
            continue  # Skip comments

        token_text = str(token)

        if token.ttype in Keyword:
            token_text = token_text.upper()
        elif token.ttype in Name:
            token_text = token_text.lower()

        if prev_token:
            if prev_token.value == '.' or token.value == '.':
                # No space around dots
                cleaned_tokens.append(token_text)
            else:
                cleaned_tokens.append(' ' + token_text)
        else:
            cleaned_tokens.append(token_text)

        prev_token = token

    cleaned_sql = ''.join(cleaned_tokens)

    # Final formatting with sqlparse
    formatted_sql = sqlparse.format(
        cleaned_sql,
        keyword_case='upper',
        identifier_case='lower',
        strip_comments=True,
        reindent=True,
        indent_width=2
    )

    # Extra space normalization
    formatted_sql = re.sub(r'\s+', ' ', formatted_sql).strip()

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
