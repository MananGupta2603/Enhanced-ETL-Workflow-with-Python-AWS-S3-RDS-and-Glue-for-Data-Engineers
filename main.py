import os
import boto3
import pandas as pd
import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# AWS Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = "us-east-1"
S3_BUCKET = "my-etl-project-bucket"
RDS_HOST = "your-rds-endpoint"
RDS_USER = "your-rds-username"
RDS_PASS = "your-rds-password"
RDS_DB = "etl_db"

# Initialize AWS Clients
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)

def upload_to_s3(file_name, bucket, object_name=None):
    """Uploads a file to an S3 bucket."""
    try:
        if object_name is None:
            object_name = file_name
        s3_client.upload_file(file_name, bucket, object_name)
        logging.info(f"Uploaded {file_name} to {bucket}/{object_name}")
    except NoCredentialsError:
        logging.error("Credentials not available.")

def download_from_s3(bucket, object_name, file_name):
    """Downloads a file from an S3 bucket."""
    try:
        s3_client.download_file(bucket, object_name, file_name)
        logging.info(f"Downloaded {object_name} from {bucket}")
    except NoCredentialsError:
        logging.error("Credentials not available.")

# Data Extraction Functions
def extract_csv(file_path):
    """Extracts data from a CSV file."""
    return pd.read_csv(file_path)

def extract_json(file_path):
    """Extracts data from a JSON file."""
    with open(file_path, 'r') as file:
        return pd.json_normalize(json.load(file))

def extract_xml(file_path):
    """Extracts data from an XML file."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for child in root:
        data.append({elem.tag: elem.text for elem in child})
    return pd.DataFrame(data)

# Transformation Function
def transform_data(df):
    """Performs data transformation, including unit conversion."""
    df['height_m'] = df['height_in'] * 0.0254  # Convert inches to meters
    df['weight_kg'] = df['weight_lb'] * 0.453592  # Convert pounds to kg
    df.drop(columns=['height_in', 'weight_lb'], inplace=True)
    return df

# Load Data to RDS
def load_to_rds(df, table_name):
    """Loads transformed data into an AWS RDS database."""
    engine = create_engine(f"mysql+pymysql://{RDS_USER}:{RDS_PASS}@{RDS_HOST}/{RDS_DB}")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Loaded data into {table_name} table in RDS")

# Main ETL Process
def main():
    logging.info("Starting ETL process")
    
    # Step 1: Extract Data
    csv_data = extract_csv('data.csv')
    json_data = extract_json('data.json')
    xml_data = extract_xml('data.xml')
    
    # Step 2: Combine Data
    combined_df = pd.concat([csv_data, json_data, xml_data], ignore_index=True)
    
    # Step 3: Transform Data
    transformed_df = transform_data(combined_df)
    
    # Step 4: Save Transformed Data
    transformed_df.to_csv('transformed_data.csv', index=False)
    upload_to_s3('transformed_data.csv', S3_BUCKET, 'transformed_data.csv')
    
    # Step 5: Load Data to RDS
    load_to_rds(transformed_df, 'transformed_table')
    
    logging.info("ETL process completed successfully")

if __name__ == "__main__":
    main()
