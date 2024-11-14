# extraction_aggregation.py

import requests
import time
import polars as pl



def process_taxi_data(url, filename, ride_type, max_retries=10, delay=2):
    """Downloading, transforming, and exporting taxi data based on ride type."""
    # Check if the URL is available
    try:
        response = requests.head(url)
        if response.status_code != 200:
            raise RuntimeError(f"URL not reachable: {url} (Status code: {response.status_code})")
            
    except requests.RequestException as e:
        raise RuntimeError(f"Error checking the URL {url}: {e}")
        
    # Read the parquet file from the URL
    # There may be times when there are runtime error so we need to additional attempts.
    for attempt in range(max_retries):
        try:
            df = pl.read_parquet(url)
            break  # Exit loop if successful
            
        except Exception as e:
            if attempt < max_retries - 1:  # Check if retries are left
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            
            else:
                raise RuntimeError(f"Error reading the parquet file from {url} after {max_retries} attempts: {e}")

    
    # Determine the timestamp column based on the ride type
    if ride_type == "Yellow Taxi Trip Records":
        timestamp_col = 'tpep_pickup_datetime'
        
    elif ride_type == "Green Taxi Trip Records":
        timestamp_col = 'lpep_pickup_datetime'
        
    elif ride_type == "For-Hire Vehicle Trip Records":
        timestamp_col = 'pickup_datetime'
        df = df.rename({"PUlocationID": "PULocationID", "DOlocationID": "DOLocationID"})  # Rename for consistency
        
    elif ride_type == "High Volume For-Hire Vehicle Trip Records":
        timestamp_col = 'request_datetime'
        
    else:
        raise ValueError("Invalid ride type specified.")

    
    # Assigning the ride type and making new columns for date and hour
    try:
        df = df.with_columns(
            pl.lit(ride_type).alias("ride_type"),
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.truncate('1h').alias('timestamp_hour'),
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.date().alias('txn_date'),
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.hour().alias('txn_hour')
        )
        
        # Performing aggregations to compute for the number of transactions
        df = (
            df
            .group_by(["txn_date", "txn_hour", "timestamp_hour", "ride_type", "PULocationID", "DOLocationID"])
            .agg(pl.len().alias("num_txns"))
        )

        # Hard-code the schema
        df = (
            df.with_columns([
                pl.col('txn_date').cast(pl.Date),
                pl.col('txn_hour').cast(pl.Int32),
                pl.col('timestamp_hour').cast(pl.Datetime),
                pl.col('ride_type').cast(pl.String),
                pl.col('PULocationID').cast(pl.Int32),
                pl.col('DOLocationID').cast(pl.Int32),
                pl.col('num_txns').cast(pl.Int32)
            ])
        )
        
        # Export the dataframe to a parquet file
        df.write_parquet(filename)

    
    except Exception as e:
        raise RuntimeError(f"Error processing data for {ride_type}: {e}.")



def upload_to_s3(s3_client, local_file, s3_bucket_name, aws_dir):
    """Creates a copy of the aggregated file to AWS S3."""
    try:
        s3_client.upload_file(local_file, s3_bucket_name, aws_dir)

    except Exception as e:
        raise RuntimeError(f"Error upploading {local_file} to S3: {str(e)}")



def refresh_glue_crawler(glue_client, crawler_name):
    """Re-runs the glue crawler each time a year's worth of data is added."""
    # Stop the crawler just in case it is running
    try:
        glue_client.stop_crawler(Name=crawler_name)
        
    except glue_client.exceptions.CrawlerNotRunningException:
        pass

    # Start the crawler
    try:
        glue_client.start_crawler(Name=crawler_name)
        
    except Exception as e:
        raise RuntimeError(f"Error refreshing {crawler_name}: {str(e)}")




