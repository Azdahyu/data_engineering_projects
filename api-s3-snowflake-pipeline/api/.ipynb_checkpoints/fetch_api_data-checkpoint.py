import requests
import pandas as pd
from utils.config_loader import load_config


def fetch_carts_data(api_url: str) -> pd.DataFrame:
    """
    Fetch carts data from an API and return it as a pandas DataFrame.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    carts_df = pd.json_normalize(
        data,
        record_path=["products"],
        meta=["id", "userId", "date"]
    )
    return carts_df


def fetch_products_data(api_url: str) -> pd.DataFrame:
    """
    Fetch products data from an API and return it as a pandas DataFrame.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    products_df = pd.json_normalize(data)
    return products_df


def merge_carts_products(carts_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge carts and products data on productId.
    """
    merged_df = carts_df.merge(
        products_df,
        left_on="productId",
        right_on="id",
        suffixes=("_cart", "_product")
    ).drop(columns=["id_product"])
    
    # Rename nested JSON fields for Snowflake compatibility
    merged_df.columns = merged_df.columns.str.replace(r"\.", "_", regex=True)
    
    return merged_df

if __name__ == "__main__":
    # Load config (expects carts_url and products_url)
    config = load_config()

    carts_url = config["api"]["carts_url"]
    products_url = config["api"]["products_url"]

    # Fetch data
    carts_df = fetch_carts_data(carts_url)
    products_df = fetch_products_data(products_url)

    # Merge
    merged_df = merge_carts_products(carts_df, products_df)

    print(merged_df.head())