import json

def load_parameters(json_path):
    with open(json_path, "r") as f:
        return json.load(f)
