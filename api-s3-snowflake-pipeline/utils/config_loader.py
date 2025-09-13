import yaml

def load_config(path: str = "config/credentials.yaml") -> dict:
    """
    load YAML config file and return as dictionary
    """
    with open(path, "r") as file:
        return yaml.safe_load(file)