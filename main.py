# main.py

from bs4 import BeautifulSoup
import requests
import polars as pl

import sys
import os
import shutil
import time
from datetime import datetime
from typing import List, Dict

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
from src.data.html_scrape import get_html, link_extraction
from src.data.aggregation import process_taxi_data, combine_all

#--------- Main Part ---------#
def main(year: int, target_months: List[str], links_dict: Dict[str, str],):
    logging.info("Creating the monthly_aggregates and concatenated directories to store the aggregated and concatenated data, respecitvely.")
    os.makedirs("../data/monthly_aggregates", exist_ok=True)
    os.makedirs(f"../data/monthly_aggregates/{year}", exist_ok=True)
    os.makedirs(f"../data/concatenated/{year}", exist_ok=True)

    # URL to the data of the specific year
    data = links_dict[year]

    # Traverse the months and get each ride type
    for month, rides in data.items():
        if month in target_months:
            
            logging.info(f"Processing {month} {year} data.")
            os.makedirs(f"../data/monthly_aggregates/{year}/{month}", exist_ok=True) # Create the directory for the month

            # Web scraping part
            for ride_type, url in rides.items():                
                logging.info(f"Aggregating {ride_type}. Data from {url}.")
                process_taxi_data(year, month, ride_type, url)
                logging.info(f"{ride_type} done.")
                logging.info(f"Waiting 120s before processing the next ride type")
                time.sleep(120)
                
    # Concatenate all dataframes into a single file
    os.makedirs(f"../data/concatenated/{year}", exist_ok=True) # Create the directory for the month
    combine_all(year)

if __name__ == "__main__":
    # Check the current month\
    logging.info("Extracting HTML.")
    soup = get_html()
    
    # Link extraction
    logging.info("Extracting links from the website.")
    links_dict = link_extraction(soup)

    # The year for data scraping
    year = int(sys.argv[1] ) # user-defined
    logging.info(f"Extracting {year} data.")

    # Months to consider
    target_months  = [#'January', 
                      #'February', 
                      'March', 
                        #'April', 'May', 'June', 
                      # 'July', 'August', 'September', 'October', 'November', 'December'
                     ]

    # Run the scraping and aggregations
    main(year, target_months, links_dict)
    
    logging.info("DONE")

