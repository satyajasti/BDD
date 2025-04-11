import pandas as pd

def create_resolution_report(parameters, used_keys, output_excel='query_parameter_mapping.xlsx'):
    rows = []
    for key in used_keys:
        mapping = parameters.get(key)
        if mapping:
            parts = mapping.replace("${env}.", "").split(".")
            if len(parts) == 3:
                db, schema, _ = parts
                status = "Mapped from JSON"
            else:
                db = schema = "Unknown"
                status = "Malformed mapping"
        else:
            db = schema = "Unknown"
            status = "No match in JSON"

        rows.append({
            "Param Key": key,
            "Database": db,
            "Schema": schema,
            "Status": status
        })

    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)
    print(f"Mapping report saved to '{output_excel}'")
