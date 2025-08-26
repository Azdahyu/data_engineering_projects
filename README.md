# API → S3 → Snowflake Data Pipeline

## **Overview**
This project builds a **data pipeline** that:
1. Collects data from a public API and converts it into a Pandas DataFrame.
2. Uploads the data to **AWS S3**.
3. Uses **Snowflake storage integration + external stage** to load data directly from S3.
4. Tracks new/changed data using **Snowflake Streams**.
5. Applies transformations automatically using **Snowflake Tasks**.
6. Stores the data in a **star schema model** for analytics.

---

## **Project Structure**
├── api  
    └── fetch_api_data.py
├── s3  
    └── upload_to_s3.py
├── snowflake  
    └── create_storage_integration.sql # One-time setup  
    └── s3_snowflake.sql # Main ETL pipeline  
├── README.md

---

## **Tech Stack**
- **Python 3.8+** → `boto3`, `pandas`
- **AWS S3** → stores raw API data
- **Snowflake** → warehouse, streams, tasks, transformations
- **SnowSQL** → CLI for running Snowflake SQL scripts
- **Git & GitHub** → version control

---

## **Setup Instructions**

### **1. Configure SnowSQL (Optional but Recommended)**
To avoid typing your password every time, set up a config file:

**Linux / MacOS:**  
```bash
nano ~/.snowsql/config
```

**Windows (PowerShell):**
```bash
notepad %USERPROFILE%\.snowsql\config
```
```ini
[connections.my_snowflake]
accountname = <account_identifier>
username = <your_username>
password = <your_password>
warehouse = <default_warehouse>
role = SYSADMIN
```
Then run commands like:
```bash
snowsql -c my_snowflake -f snowflake/s3_snowflake.sql
```

### **2. Setup AWS Credentials**
Make sure you’ve configured your AWS CLI with credentials that allow S3 PutObject:
```bash
aws configure
```

### **3. Execution Order**
**Step 1 → Run One-Time Setup (Storage Integration)**
```bash
snowsql -a <account_identifier> -u <username> -f snowflake/create_storage_integration.sql
```
- Grants Snowflake access to S3.
- This only needs to be done once.

**Step 2 → Run Main ETL Pipeline**
```bash
snowsql -a <account_identifier> -u <username> -f snowflake/s3_snowflake.sql
```
This script:  
Creates the warehouse, database, and schema.  
Creates the external stage linked to your S3 bucket.  
Sets up streams and tasks for change tracking.  
Transforms and loads data into the star schema.  

**Step 3 → Fetch & Upload Data**
```bash
python -m api.fetch_api_data
python -m s3.upload_to_s3
```

**Step 4 → Verify Data in Snowflake**
```bash
SELECT * FROM FactSales LIMIT 10;
```
### **Workflow Summary**
| Step | Action                      | Script                           | Frequency   |
| ---- | --------------------------- | -------------------------------- | ----------- |
| 1    | Fetch API data              | `api/fetch_api_data.py`          | As needed   |
| 2    | Upload data to S3           | `s3/upload_to_s3.py`             | As needed   |
| 3    | Set up S3 → Snowflake trust | `create_storage_integration.sql` | **Once**    |
| 4    | Build stage, tables, tasks  | `s3_snowflake.sql`               | Re-runnable |
| 5    | Auto-load + transform data  | Snowflake Tasks                  | Automatic   |



