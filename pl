import json

def load_parameters(json_path):
    with open(json_path, "r") as f:
        return json.load(f)




        -------------------------
        
        import re

def replace_query_parameters(query_text, parameters):
    placeholders = re.findall(r"\$\{([^}]+)\}", query_text)
    for key in set(placeholders):
        if key in parameters:
            query_text = query_text.replace(f"${{{key}}}", parameters[key])
    return query_text, placeholders
