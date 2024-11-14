# main.py

from bs4 import BeautifulSoup
import requests
import polars as pl
import boto3

import sys
import os
import shutil
import time
from datetime import datetime

# Logging
import logging

# Log file directory
log_directory = 'logs'
log_file = f'{log_directory}/app_{datetime.now().date()}.log'  

# Create the log directory if it does not exist
os.makedirs(log_directory, exist_ok=True)

# Check if the log file exists
if os.path.exists(log_file):
    os.remove(log_file)  # Delete the file
    print(f"{log_file} has been deleted.")
else:
    print(f"{log_file} does not exist.")

# Setup the logging file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Importing the modules
from src.data.html_scrape import check_same_state, update_state_s3, link_extraction
from src.data.aggregation import process_taxi_data, upload_to_s3, refresh_glue_crawler

# AWS Details
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION_NAME = "ap-southeast-1"

# Create S3 client
logging.info("Creating AWS S3 client.")
s3_bucket_name = 'nyc-taxi-time-series-analysis'
s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME
)

# AWS Glue client
logging.info("Creating AWS Glue client.")
crawler_name = "nyc-taxi-time-series-2-crawler"
glue_client = boto3.client(
    service_name='glue',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME
)


#--------- Main Part ---------#
def main(year, month, links_dict, s3_bucket_name, crawler_name, s3_client, glue_client):
    logging.info("Creating the monthly aggregates directory to store the aggregated data.")
    data_dir = "../data/monthly_aggregates"
    os.makedirs(data_dir, exist_ok=True)


    # Iterate over the ride types
    for ride_type in links_dict[year][month].keys():
        logging.info(f"Preprocessing {ride_type} for {month} {year}")
        data_url = links_dict[year][month][ride_type]

        # Constructs the filenames and directories
        ride_type_dir = f"{data_dir}/{year}/{month}/{ride_type}"
        filename = f"{ride_type_dir}/{ride_type}_{month}_{year}.parquet"
        aws_dir = f"agg_data/{year}/{month}/{ride_type}/{ride_type}_{month}_{year}.parquet"

        # Local directory to place the parquet file
        os.makedirs(ride_type_dir, exist_ok=True)

        # Preprocessing step
        process_taxi_data(data_url, filename, ride_type)
        logging.info(f"Preprocessing {ride_type} done.")

        # Upload to S3
        logging.info(f"Uploading {ride_type} to AWS S3")
        upload_to_s3(s3_client, filename, s3_bucket_name, aws_dir)
        logging.info(f"Upload to AWS S3 successful.")

        # Delete the local file (to prevent the container from getting bigger)
        try:
            shutil.rmtree(ride_type_dir)
            print(f"'{ride_type_dir}' deleted successfully.")
        except OSError as e:
            print(f"Error in deleting the local parquet file: {e.strerror}")

        # Time delay
        time.sleep(5)
                    
    # Refresh AWS Glue crawler
    logging.info(f"Refreshing AWS Glue crawler.")
    refresh_glue_crawler(glue_client, crawler_name)
    logging.info(f"AWS Glue crawler done re-running.")


if __name__ == "__main__":
    # Check the current month
    logging.info("Checking if the data is up to date.")
    state, soup, month, year = check_same_state(s3_client, s3_bucket_name)

    if not state:
        logging.info(f"{month} {year} is missing.")
        
        # Link extraction
        logging.info("Extracting links from the website.")
        links_dict = link_extraction(soup)
        
        # We extract the data from the links
        logging.info(f"Extracting and processing data for {month} {year}.")
        main(year, month, links_dict, s3_bucket_name, crawler_name, s3_client, glue_client)
        logging.info(f"Extraction and processing done.")

        # Update the state file in S3
        logging.info("Updating state file in S3.")
        update_state_s3(s3_client, s3_bucket_name, month, year)
        logging.info("Successfully updated state file.")
    
    else:
        logging.info("Data is up to date.")
    
    logging.info("DONE")

