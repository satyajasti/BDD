import json
import re
import logging
import argparse
import os
from snowflake_connectivity import get_snowflake_connection  # Ensure this file is in your project
from snowflake.connector import ProgrammingError

# Configure logging
logging.basicConfig(filename="sql_validation.log",
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Load parameters from JSON for the specified environment
def load_parameters(json_file_path, env):
    try:
        with open(json_file_path, "r") as file:
            all_params = json.load(file)
        
        if env in all_params:
            return all_params[env]
        else:
            logging.error(f"Environment '{env}' not found in the parameters file.")
            raise ValueError(f"Environment '{env}' not found in parameters.")
    except Exception as e:
        logging.error(f"Error loading parameters: {e}")
        raise

# Replace placeholders in SQL query with actual parameters
def parameterize_sql(sql_file_path, parameters):
    try:
        with open(sql_file_path, "r") as file:
            sql_query = file.read()

        # Replace parameters (assuming parameters are in ${param_name} format)
        for key, value in parameters.items():
            sql_query = re.sub(rf"\${{{key}}}", str(value), sql_query)

        return sql_query
    except Exception as e:
        logging.error(f"Error parameterizing SQL: {e}")
        raise

# Execute the SQL query in Snowflake
def execute_sql_query(conn, query):
    try:
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        logging.info(f"Query executed successfully: {query}")
        return results
    except ProgrammingError as e:
        logging.error(f"SQL Execution Error: {e}")
        raise
    finally:
        cur.close()

# Main function
def main():
    parser = argparse.ArgumentParser(description="Execute SQL query with environment-based parameters")
    parser.add_argument("--env", required=True, help="Specify the environment (e.g., dev, qa, prod)")
    args = parser.parse_args()
    env = args.env

    json_path = "config.json"  # Ensure correct path
    sql_file_path = "query.sql"  # Ensure correct path

    try:
        # Load parameters for the specified environment
        parameters = load_parameters(json_path, env)

        # Generate final SQL query
        final_query = parameterize_sql(sql_file_path, parameters)

        # Print final query for debugging
        print("Final SQL Query:\n", final_query)

        # Establish Snowflake connection
        conn, database, schema = get_snowflake_connection(json_path)

        # Execute query and fetch results
        results = execute_sql_query(conn, final_query)

        # Print and log results
        print("Query Results:\n", results)
        logging.info(f"Query Results: {results}")

    except Exception as e:
        logging.error(f"Error in SQL execution: {e}")
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()
