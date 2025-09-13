-- Set the role and warehouse

USE ROLE accountadmin;
CREATE OR REPLACE WAREHOUSE apii_s3_snowflake_pipeline_wh 
  WITH WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60;
USE WAREHOUSE apii_s3_snowflake_pipeline_wh;

-- Create database and schema

CREATE OR REPLACE DATABASE apii_s3_snowflake_pipeline_db;
USE DATABASE apii_s3_snowflake_pipeline_db;
CREATE OR REPLACE SCHEMA apii_s3_snowflake_pipeline_schema;
USE SCHEMA apii_s3_snowflake_pipeline_schema;

-- Create external stage

USE DATABASE apii_s3_snowflake_pipeline_db;
CREATE OR REPLACE STAGE apii_s3_snowflake_stage
  STORAGE_INTEGRATION = apii_s3_snowflake_int
  URL = 's3://api-s3-snowflake-bucket/raw_data/';

-- Create raw landing table

CREATE OR REPLACE TABLE raw_sales_data (
    productId INT,
    quantity INT,
    id_cart VARCHAR,
    userId VARCHAR,
    date VARCHAR,
    title VARCHAR,
    price FLOAT,
    description VARCHAR,
    category VARCHAR,
    image VARCHAR,
    rating_rate FLOAT,
    rating_count INT
);

-- Load raw data from S3 into Snowflake

COPY INTO raw_sales_data
FROM @apii_s3_snowflake_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- Create cleaned data table

CREATE OR REPLACE TABLE clean_sales_data AS
SELECT
    productId,
    quantity,
    id_cart,
    userId,
    TO_TIMESTAMP_NTZ(date) AS date,
    INITCAP(title) AS title,
    price,
    description,
    INITCAP(category) AS category,
    image,
    rating_rate,
    rating_count
FROM raw_sales_data;

-- Create Dimension Tables

-- Product Dimension
CREATE OR REPLACE TABLE DimProduct AS
SELECT
    ROW_NUMBER() OVER (ORDER BY productId) AS product_key,
    productId,
    title,
    description,
    image
FROM clean_sales_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY productId ORDER BY date DESC) = 1;

-- User Dimension
CREATE OR REPLACE TABLE DimUser AS
SELECT
    ROW_NUMBER() OVER (ORDER BY userId) AS user_key,
    userId
FROM clean_sales_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY userId ORDER BY date DESC) = 1;

-- Category Dimension
CREATE OR REPLACE TABLE DimCategory AS
SELECT
    ROW_NUMBER() OVER (ORDER BY category) AS category_key,
    category
FROM clean_sales_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY category) = 1;

-- Date Dimension
CREATE OR REPLACE TABLE DimDate AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_key,
    date,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day,
    DAYOFWEEK(date) AS weekday
FROM (
    SELECT DISTINCT TO_TIMESTAMP_NTZ(date) AS date 
    FROM clean_sales_data
);

-- Create Fact Table

CREATE OR REPLACE TABLE FactSales AS
SELECT
    dp.product_key,
    du.user_key,
    dc.category_key,
    dd.date_key,
    s.productId,
    s.quantity,
    s.price,
    s.rating_rate,
    s.rating_count
FROM clean_sales_data s
JOIN DimProduct dp   ON s.productId = dp.productId
JOIN DimUser du      ON s.userId = du.userId
JOIN DimCategory dc  ON s.category = dc.category
JOIN DimDate dd      ON TO_TIMESTAMP_NTZ(s.date) = dd.date;

-- Create Stream to track new data

CREATE OR REPLACE STREAM sales_data_stream ON TABLE raw_sales_data;

-- Create Automated Task for Star Schema Transformation

CREATE OR REPLACE TASK transform_star_schema
WAREHOUSE = apii_s3_snowflake_pipeline_wh
SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
-- Step 1: Insert new cleaned data
INSERT INTO clean_sales_data
SELECT
    productId,
    quantity,
    id_cart,
    userId,
    TO_TIMESTAMP_NTZ(date),
    INITCAP(title),
    price,
    description,
    INITCAP(category),
    image,
    rating_rate,
    rating_count
FROM sales_data_stream
WHERE METADATA$ACTION = 'INSERT';

-- Step 2: Merge into DimProduct
MERGE INTO DimProduct dp
USING (
    SELECT DISTINCT productId, title, description, image
    FROM clean_sales_data
) src
ON dp.productId = src.productId
WHEN NOT MATCHED THEN
    INSERT (product_key, productId, title, description, image)
    VALUES (DEFAULT, src.productId, src.title, src.description, src.image);

-- Step 3: Merge into DimUser
MERGE INTO DimUser du
USING (
    SELECT DISTINCT userId
    FROM clean_sales_data
) src
ON du.userId = src.userId
WHEN NOT MATCHED THEN
    INSERT (user_key, userId)
    VALUES (DEFAULT, src.userId);

-- Step 4: Merge into DimCategory
MERGE INTO DimCategory dc
USING (
    SELECT DISTINCT category
    FROM clean_sales_data
) src
ON dc.category = src.category
WHEN NOT MATCHED THEN
    INSERT (category_key, category)
    VALUES (DEFAULT, src.category);

-- Step 5: Merge into DimDate
MERGE INTO DimDate dd
USING (
    SELECT DISTINCT TO_TIMESTAMP_NTZ(date) AS date
    FROM clean_sales_data
) src
ON dd.date = src.date
WHEN NOT MATCHED THEN
    INSERT (date_key, date, year, month, day, weekday)
    VALUES (
        DEFAULT,
        src.date,
        YEAR(src.date),
        MONTH(src.date),
        DAY(src.date),
        DAYOFWEEK(src.date)
    );

-- Step 6: Insert into Fact Table
INSERT INTO FactSales
SELECT
    dp.product_key,
    du.user_key,
    dc.category_key,
    dd.date_key,
    s.productId,
    s.quantity,
    s.price,
    s.rating_rate,
    s.rating_count
FROM clean_sales_data s
JOIN DimProduct dp   ON s.productId = dp.productId
JOIN DimUser du      ON s.userId = du.userId
JOIN DimCategory dc  ON s.category = dc.category
JOIN DimDate dd      ON TO_TIMESTAMP_NTZ(s.date) = dd.date;
