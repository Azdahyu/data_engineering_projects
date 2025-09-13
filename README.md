# ETL Pipeline: Mode of Transportation Data

This project implements a simple **ETL (Extract–Transform–Load)** pipeline using Python, Pandas, and logging.  
It extracts raw data from an Excel file, transforms the column names, and loads the result into a CSV file.

---

## Project Structure
├── transport_etl_pipeline.py # Main ETL pipeline script.  
├── config.yaml # Configuration file for paths, logging, and transformations.  
├── README.md # Project documentation

---

## Requirements
- Python 3.8+
- pandas
- numpy
- openpyxl
- pyyaml (for reading config)

Install dependencies:
```bash
pip install pandas numpy openpyxl pyyaml
```

# How to run
1. Update the file paths in config.yaml
2. Run the pipeline
```bash
python transport_etl_pipeline.py
```
3. Check the output CSV at the location specified in config.yaml.

Logging configuration is defined in config.yaml.
By default, logs include timestamps, log level, and messages.

Example log output:  
2025-08-18 10:30:12 - INFO - Starting extraction from C:/Users/HP/Tina/Projects/mode_of_transportation.xlsx  
2025-08-18 10:30:13 - INFO - Extraction complete!  
2025-08-18 10:30:14 - INFO - Transformation complete!  
2025-08-18 10:30:15 - INFO - Loading complete!

Currently, the following column renaming is applied:  

reportyear → report_year  
geotype → geo_type  
geotypevalue → geo_type_value  
geoname → geo_name  

You can modify config.yaml to adjust transformations as needed.

Author: Tina
