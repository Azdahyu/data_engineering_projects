import pandas as pd
import numpy as np
import logging
import os
import yaml

# Load config.yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Setup logging from config
logging.basicConfig(
    level=getattr(logging, config["logging"]["level"].upper(), logging.INFO),
    format=config["logging"]["format"],
    datefmt=config["logging"]["datefmt"]
)

def extract(file):
    try:
        logging.info(f'Starting extraction from {file}')
        df = pd.read_excel(file, engine='openpyxl')
        logging.info('Extraction complete!')
        return df
    except Exception as e:
        logging.error(f'Error extracting from {file}: {e}', exc_info=True)
        return None

def transform(df, transformations):
    try:
        logging.info('Starting transformation...')
        if "rename_columns" in transformations:
            df = df.rename(columns=transformations["rename_columns"])
        logging.info('Transformation complete!')
        return df
    except Exception as e:
        logging.error(f'Transformation failed: {e}', exc_info=True)
        return None

def load(df, file_path):
    try:
        logging.info(f'Preparing to load to {file_path}')
        
        # If file exists, delete it first
        if os.path.exists(file_path):
            logging.warning(f'{file_path} already exists. Deleting old file...')
            os.remove(file_path)
        
        df.to_csv(file_path, index=False)
        logging.info('Loading complete!')
    except Exception as e:
        logging.error(f'Loading to {file_path} failed: {e}', exc_info=True)
        return None


if __name__ == "__main__":
    raw_data_path = config["paths"]["raw_data"]
    output_data_path = config["paths"]["output_data"]
    transformations = config.get("transformations", {})

    raw_data = extract(raw_data_path)
    if raw_data is not None:
        transformed_data = transform(raw_data, transformations)
        if transformed_data is not None:
            load(transformed_data, output_data_path)
