-- Create S3 storage integration

CREATE STORAGE INTEGRATION apii_s3_snowflake_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::351516300644:role/api-s3-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://api-s3-snowflake-bucket/raw_data/');