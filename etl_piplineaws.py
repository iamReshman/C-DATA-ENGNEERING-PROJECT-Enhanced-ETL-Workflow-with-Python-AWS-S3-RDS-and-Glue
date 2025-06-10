# === Required Libraries ===
import os
import logging
import boto3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import time

# === Configuration ===
AWS_REGION = 'ap-south-1'
S3_BUCKET = 'my-etl-bucket-unique123456'
RDS_IDENTIFIER = 'etl-mysql-db'
RDS_USERNAME = 'admin'
RDS_PASSWORD = 'strongpassword'
RDS_DB_NAME = 'etl_database'
GLUE_DATABASE = 'etl_glue_db'
GLUE_CRAWLER = 'etl_glue_crawler'
INSTANCE_CLASS = 'db.t3.micro'

# === Logging Setup ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename='logs/etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === Connect to AWS Services ===
s3 = boto3.client('s3', region_name=AWS_REGION)
rds = boto3.client('rds', region_name=AWS_REGION)
glue = boto3.client('glue', region_name=AWS_REGION)
iam = boto3.client('iam', region_name=AWS_REGION)

# === S3 Bucket Creation ===
def create_s3_bucket():
    try:
        s3.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
        )
        logging.info(f"S3 bucket '{S3_BUCKET}' created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logging.info(f"S3 bucket '{S3_BUCKET}' already exists.")

# === RDS Instance Creation ===
def create_rds_instance():
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=RDS_IDENTIFIER,
            AllocatedStorage=20,
            DBInstanceClass=INSTANCE_CLASS,
            Engine='mysql',
            MasterUsername=RDS_USERNAME,
            MasterUserPassword=RDS_PASSWORD,
            DBName=RDS_DB_NAME,
            PubliclyAccessible=True
        )
        logging.info("RDS instance creation started.")
        rds.get_waiter('db_instance_available').wait(DBInstanceIdentifier=RDS_IDENTIFIER)
        logging.info("RDS instance is now available.")
    except rds.exceptions.DBInstanceAlreadyExistsFault:
        logging.info("RDS instance already exists.")

# === Get RDS Endpoint ===
def get_rds_endpoint():
    response = rds.describe_db_instances(DBInstanceIdentifier=RDS_IDENTIFIER)
    return response['DBInstances'][0]['Endpoint']['Address']

# === Create IAM Role for Glue ===
def create_glue_service_role(iam_client):
    role_name = 'AWSGlueServiceRole'
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description="Glue ETL Role"
        )
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
        logging.info(f"IAM role '{role_name}' created and policies attached.")
    except iam_client.exceptions.EntityAlreadyExistsException:
        logging.info(f"IAM role '{role_name}' already exists.")

def create_glue_resources():
    try:
        glue.create_database(DatabaseInput={'Name': GLUE_DATABASE})
        logging.info(f"Glue database '{GLUE_DATABASE}' created.")
    except glue.exceptions.AlreadyExistsException:
        logging.info(f"Glue database '{GLUE_DATABASE}' already exists.")

    role_arn = iam.get_role(RoleName='AWSGlueServiceRole')['Role']['Arn']
    try:
        glue.create_crawler(
            Name=GLUE_CRAWLER,
            Role=role_arn,
            DatabaseName=GLUE_DATABASE,
            Targets={'S3Targets': [{'Path': f's3://{S3_BUCKET}/'}]},
            TablePrefix='etl_'
        )
        glue.start_crawler(Name=GLUE_CRAWLER)
        logging.info("Glue crawler created and started.")
    except glue.exceptions.AlreadyExistsException:
        logging.info("Glue crawler already exists.")

# === Extract Functions ===
def extract_csv(path):
    return pd.read_csv(path)

def extract_json(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    records = [json.loads(line.strip()) for line in lines if line.strip()]
    return pd.json_normalize(records)

def extract_xml(path):
    tree = ET.parse(path)
    root = tree.getroot()
    records = [{child.tag: child.text for child in person} for person in root]
    return pd.DataFrame(records)

# === Transform Function ===
def transform_data(df):
    df.rename(columns={
        'height': 'height_inches',
        'weight': 'weight_pounds'
    }, inplace=True)
    if 'height_inches' in df.columns:
        df['height_meters'] = pd.to_numeric(df['height_inches'], errors='coerce') * 0.0254
    if 'weight_pounds' in df.columns:
        df['weight_kg'] = pd.to_numeric(df['weight_pounds'], errors='coerce') * 0.453592
    return df

# === Upload to S3 ===
def upload_to_s3(file_path, s3_key):
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    logging.info(f"Uploaded {file_path} to S3 as {s3_key}")

# === Load to RDS ===
def load_to_rds(df, endpoint):
    engine = create_engine(f'mysql+pymysql://{RDS_USERNAME}:{RDS_PASSWORD}@{endpoint}/{RDS_DB_NAME}')
    df.to_sql('etl_table', con=engine, if_exists='replace', index=False)
    logging.info("Data loaded into RDS successfully.")

# === Main Function ===
def main():
    try:
        logging.info("ETL pipeline started.")
        create_s3_bucket()
        create_rds_instance()
        rds_endpoint = get_rds_endpoint()
        create_glue_service_role(iam)
        create_glue_resources()

        # Extract from all files
        df_csv1 = extract_csv('data/source1.csv')
        df_csv2 = extract_csv('data/source2.csv')
        df_csv3 = extract_csv('data/source3.csv')

        df_json1 = extract_json('data/source1.json')
        df_json2 = extract_json('data/source2.json')
        df_json3 = extract_json('data/source3.json')

        df_xml1 = extract_xml('data/source1.xml')
        df_xml2 = extract_xml('data/source2.xml')
        df_xml3 = extract_xml('data/source3.xml')

        # Combine and transform
        full_df = pd.concat([
            df_csv1, df_csv2, df_csv3,
            df_json1, df_json2, df_json3,
            df_xml1, df_xml2, df_xml3
        ], ignore_index=True)

        transformed_df = transform_data(full_df)

        # Save and upload
        transformed_df.to_csv('transformed_data.csv', index=False)
        upload_to_s3('transformed_data.csv', 'transformed_data.csv')

        # Load to RDS
        load_to_rds(transformed_df, rds_endpoint)

        logging.info("ETL pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")

if __name__ == '__main__':
    main()
