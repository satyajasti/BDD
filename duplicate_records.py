import snowflake.connector
import pandas as pd
from snowflake_connection import get_snowflake_connection

# Function to retrieve column names from the specified table
def get_columns_from_table(conn, schema_name, table_name):
    query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
        AND TABLE_NAME = '{table_name}'
    """
    cur = conn.cursor()
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    return columns

# Function to check for duplicate records in each column and add the result to a summary
def check_duplicate_records(conn, schema_name, table_name, columns):
    cur = conn.cursor()
    duplicate_summary = []

    for column in columns:
        query = f"""
            SELECT {column}, COUNT(*) AS cnt
            FROM {schema_name}.{table_name}
            GROUP BY {column}
            HAVING COUNT(*) > 1
        """
        cur.execute(query)
        duplicates = cur.fetchall()

        if duplicates:
            # Record that duplicates were found
            duplicate_summary.append({
                'column_name': column,
                'Duplicate': 'Yes'
            })
            # Print the result to the console
            print(f"Duplicate records found for column {column}")
        else:
            # No duplicates found
            duplicate_summary.append({
                'column_name': column,
                'Duplicate': 'No'
            })
            # Print the result to the console
            print(f"No duplicate found for column {column}")

    cur.close()
    return duplicate_summary

# Main function to run the process and write results to Excel
def main():
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Get all column names from the table
    columns = get_columns_from_table(conn, schema_name, table_name)

    # Check for duplicate records in each column and summarize results
    duplicate_summary = check_duplicate_records(conn, schema_name, table_name, columns)

    # Output file for the results
    output_file = 'duplicate_records_summary.xlsx'

    # Convert the summary to a DataFrame and write it to a single sheet
    df_summary = pd.DataFrame(duplicate_summary)
    
    # Write the DataFrame to the Excel file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Duplicate_Summary', index=False)

    print(f"\nDuplicate records validation summary written to {output_file}")

    # Close the Snowflake connection
    conn.close()

if __name__ == "__main__":
    main()
