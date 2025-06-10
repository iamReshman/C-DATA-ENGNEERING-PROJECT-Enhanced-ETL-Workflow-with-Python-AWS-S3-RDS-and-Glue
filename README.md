# C-DATA-ENGNEERING-PROJECT-Enhanced-ETL-Workflow-with-Python-AWS-S3-RDS-and-Glue
# Cloud-Based ETL Pipeline with AWS

## Project Overview
This project demonstrates a scalable ETL pipeline using AWS services:
- **S3** for data storage
- **RDS** for relational data persistence
- Optional: **AWS Glue** for automation

## Steps Performed
1. Extract CSV, JSON, XML files.
2. Transform data: Unit conversion, cleaning.
3. Load:
   - Transformed data to S3
   - Final dataset into AWS RDS
4. Logging: Logs saved locally and optionally to S3

## Tech Stack
- Python (pandas, sqlalchemy, boto3)
- AWS S3, RDS, Glue
- SQL Workbench for verification

## To Run
```bash
pip install -r requirements.txt
python etl_pipeline.py
