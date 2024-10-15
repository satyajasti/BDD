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

# Function to check for duplicate records in each column
def check_duplicate_records(conn, schema_name, table_name, columns, writer):
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
            # Record that duplicates were found and get the count
            total_duplicates = sum([row[1] for row in duplicates])
            duplicate_summary.append({
                'column_name': column,
                'Duplicate': 'Yes',
                'Count': total_duplicates
            })

            # Convert the result to a DataFrame and limit the data to 5 rows
            df_duplicates = pd.DataFrame(duplicates, columns=[column, 'Count']).head(5)

            # Print the duplicates to the console
            print(f"\nDuplicate records found for column {column}:")
            print(df_duplicates)

            # Write the duplicates to Excel
            sheet_name = f'{column}_Duplicates'
            df_duplicates.to_excel(writer, sheet_name=sheet_name, startrow=1, startcol=1, index=False)

            # Write the column name in A1
            writer.sheets[sheet_name].cell(row=1, column=1).value = f"Column: {column}"
        else:
            # No duplicates found
            duplicate_summary.append({
                'column_name': column,
                'Duplicate': 'No',
                'Count': 0
            })
            print(f"No duplicate found for column {column}")

    cur.close()
    return duplicate_summary

# Main function to run the process and write results to Excel
def main():
    # Get connection, schema, and table name from JSON config
    conn, database_name, schema_name, table_name = get_snowflake_connection('config.json')

    # Get all column names from the table
    columns = get_columns_from_table(conn, schema_name, table_name)

    # Output file for the results
    output_file = 'duplicate_records_validation.xlsx'

    # Create an Excel writer
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Validate duplicate records for each column
        duplicate_summary = check_duplicate_records(conn, schema_name, table_name, columns, writer)

        # Write summary of duplicates to a separate sheet
        df_summary = pd.DataFrame(duplicate_summary)
        df_summary.to_excel(writer, sheet_name='Duplicate_Summary', index=False)

    print(f"\nDuplicate records validation results written to {output_file}")

    # Close the Snowflake connection
    conn.close()

if __name__ == "__main__":
    main()
