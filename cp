import argparse
import logging
from parameter_loader import load_parameters
from query_parser import replace_query_parameters
from excel_writer import create_resolution_report

# Setup logging
logging.basicConfig(filename="query_resolution.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    parser = argparse.ArgumentParser(description="Resolve SQL parameters from JSON and output mappings.")
    parser.add_argument("--env", required=True, help="Environment prefix (e.g., dev, qa, prod)")
    parser.add_argument("--json", default="parameters.json", help="Path to JSON parameter file")
    parser.add_argument("--sql", default="query.sql", help="Path to SQL file with placeholders")
    args = parser.parse_args()

    logging.info("Starting query resolution process")
    parameters = load_parameters(args.json)

    # Replace ${env} in parameters
    for k, v in parameters.items():
        if isinstance(v, str):
            parameters[k] = v.replace("${env}", args.env)

    with open(args.sql, "r") as file:
        query_text = file.read()

    final_query, used_keys = replace_query_parameters(query_text, parameters)

    with open("resolved_query.sql", "w") as out_file:
        out_file.write(final_query)
    logging.info("Query resolved and saved to resolved_query.sql")

    create_resolution_report(parameters, used_keys, output_excel='query_parameter_mapping.xlsx')
    logging.info("Excel mapping report saved to query_parameter_mapping.xlsx")

    print(" Query and Excel report generated successfully.")

if __name__ == "__main__":
    main()
