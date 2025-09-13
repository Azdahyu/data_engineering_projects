import io
import boto3
import pandas as pd
from utils.config_loader import load_config


def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Upload a pandas DataFrame to S3 as a CSV file without saving locally.
    Relies on AWS credentials from environment variables or ~/.aws/credentials.
    """
    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Create S3 client (credentials are loaded automatically)
    s3_client = boto3.client("s3")

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )

    print(f"Uploaded DataFrame to s3://{bucket}/{key}")


if __name__ == "__main__":
    from api.fetch_api_data import fetch_carts_data, fetch_products_data, merge_carts_products

    config = load_config()
    carts_url = config["api"]["carts_url"]
    products_url = config["api"]["products_url"]

    carts_df = fetch_carts_data(carts_url)
    products_df = fetch_products_data(products_url)
    merged_df = merge_carts_products(carts_df, products_df)

    bucket_name = config["aws"]["s3_bucket"]
    object_key = "raw_data/merged_data.csv"
    upload_df_to_s3(merged_df, bucket_name, object_key)
