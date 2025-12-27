# app/config/settings.py
import yaml

def load_config(config_path="app/config/config.yaml"):
    """Load configuration settings from a YAML file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Config file not found at {config_path}, using default values.")
        return {}
    except yaml.YAMLError as e:
        print(f"Error reading config file: {e}, using default values.")
        return {}

config = load_config()