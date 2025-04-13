import boto3
import pandas as pd
import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import pymysql
from botocore.exceptions import NoCredentialsError
import Aws_Credential
import glob

# Configure logging
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# AWS Configuration
AWS_ACCESS_KEY = Aws_Credential.AWS_ACCESS_KEY
AWS_SECRET_KEY = Aws_Credential.AWS_SECRET_KEY
AWS_REGION = Aws_Credential.AWS_REGION
S3_BUCKET = Aws_Credential.S3_BUCKET
RDS_HOST = Aws_Credential.RDS_HOST
RDS_USER = Aws_Credential.RDS_USER
RDS_PASS = Aws_Credential.RDS_PASS
RDS_DB = Aws_Credential.RDS_DB

# Initialize AWS Clients
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)

def setup_logging():
    
    # Open the log file in append mode and add a separator for each new run
    with open('etl.log', 'a') as log_file:
        log_file.write("\n\n--- New ETL Process Run ---\n")

    # Configure logging settings
    logging.basicConfig(
        level=logging.INFO,
        filename='etl.log',
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'  # Append mode
    )

def upload_to_s3(file_name, bucket, object_name=None):

    try:
        if object_name is None:
            object_name = file_name
        s3_client.upload_file(file_name, bucket, object_name)
        logging.info(f"Uploaded {file_name} to {bucket}/{object_name}")
    except NoCredentialsError:
        logging.error("Credentials not available.")

def download_from_s3(bucket, object_name, file_name):
   
    try:
        s3_client.download_file(bucket, object_name, file_name)
        logging.info(f"Downloaded {object_name} from {bucket}")
    except NoCredentialsError:
        logging.error("Credentials not available.")

# Data Extraction Functions
def extract_csv(folder_path):
   
  
    csv_files = glob.glob(f"{folder_path}/*.csv")  
    dataframes = [pd.read_csv(file) for file in csv_files]  
    combined_data = pd.concat(dataframes, ignore_index=True)  
    logging.info(f"Data Extract From CSV")
    return combined_data
    

def extract_json(folder_path):
   
    json_files = glob.glob(f"{folder_path}/*.json")  
    dataframes = [pd.read_json(file, lines=True) for file in json_files]  
    combined_data = pd.concat(dataframes, ignore_index=True) 
    logging.info(f"Data Extract From Json")
    return combined_data


def extract_xml(folder_path):
    # print("Hi xml")
    xml_files = glob.glob(f"{folder_path}/*.xml")  
    all_data = []

    for file in xml_files:
        tree = ET.parse(file)
        root = tree.getroot()

        # Parse XML into a list of dictionaries
        for person in root.findall('person'):
            data = {
                'name': person.find('name').text,
                'height': float(person.find('height').text),
                'weight': float(person.find('weight').text),
            }
            all_data.append(data)

    # Convert list of dictionaries into a DataFrame
    combined_data = pd.DataFrame(all_data)
    logging.info(f"Data Extract From XML")
    return combined_data


def transform_data(df):
    df['height_m'] = (df['height'] * 0.0254).round(2)  # Convert inches to meters
    df['weight_kg'] = (df['weight'] * 0.453592).round(2)  # Convert pounds to kg
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        df = df.drop_duplicates()
    
    df=df.reset_index(drop=True)
    logging.info(f"Transformed the data")
    return df


def load_to_rds(df, table_name):
    engine = create_engine(f"mysql+pymysql://{RDS_USER}:{RDS_PASS}@{RDS_HOST}/{RDS_DB}")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Loaded data into {table_name} table in RDS")

# Main ETL Process
def main():
    setup_logging()
    logging.info("Starting ETL process")
    
    csv_data = extract_csv('data/')
    json_data = extract_json('data/')
    xml_data = extract_xml('data/')
    

    combined_df = pd.concat([csv_data, json_data, xml_data], ignore_index=True)
    

    combined_df.to_csv('raw_data.csv', index=False)
    upload_to_s3('raw_data.csv', S3_BUCKET, 'raw_data.csv')
    
    transformed_df = transform_data(combined_df)
    
    transformed_df.to_csv('transformed_data.csv', index=False)
    upload_to_s3('transformed_data.csv', S3_BUCKET, 'transformed_data.csv')
    
    load_to_rds(transformed_df, 'transformed_table')
    
    logging.info("ETL process completed successfully")

if __name__ == "__main__":
    main()
