# extraction_aggregation.py

import requests
import time
import os
import polars as pl


def process_taxi_data(year: int, month: str, ride_type: str, url: str, max_retries:int =20, delay:int=30):
    """Downloading, transforming, and exporting taxi data based on ride type."""
    # Read the location mapping
    try:
        df_loc_dict = pl.read_csv(
            "data dictionary/taxi_zone_lookup.csv", 
            columns=["LocationID", "Borough"], 
            schema_overrides={'LocationID': pl.Int32, 'Borough': pl.Utf8}
        )
        
    except Exception as e:
        raise RuntimeError(f"Error in reading location ID mapping CSV file: {e}.")
    
    
    # Check if the URL is available
    try:
        response = requests.head(url)
        if response.status_code != 200:
            raise RuntimeError(f"URL not reachable: {url} (Status code: {response.status_code})")
            
    except requests.RequestException as e:
        raise RuntimeError(f"Error checking the URL {url}: {e}")

    
    # Each ride type has different timestamp column names
    if ride_type == "Yellow Taxi Trip Records":
        timestamp_col = 'tpep_pickup_datetime'

    elif ride_type == "Green Taxi Trip Records":
        timestamp_col = 'lpep_pickup_datetime'

    elif ride_type == "For-Hire Vehicle Trip Records":
        timestamp_col = 'pickup_datetime'

    elif ride_type == "High Volume For-Hire Vehicle Trip Records":
        timestamp_col = 'request_datetime'

    # Schema of the dataframes. We fix the schema for consistency
    schema_overrides = {
        timestamp_col: pl.Datetime, 
        'PULocationID': pl.Int32,
        'DOLocationID': pl.Int32,
        'total_amount': pl.Float64
    }
    
    # Read the parquet files from the URL
    # There may be times when there are runtime error so we need to have additional attempts.
    for attempt in range(max_retries):
        try:
            # Checks the ride type
            if ride_type == "Yellow Taxi Trip Records":
                # Read the parquet file
                df = pl.read_parquet(
                    url, 
                    columns=["tpep_pickup_datetime", "PULocationID", "DOLocationID", "payment_type", "total_amount"]
                )
                df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in schema_overrides.items()]) # modifying the data types
                df = df.with_columns(pl.lit(ride_type).alias("ride_type")) # Ride type column = Yellow Taxi
                df = df.filter(pl.col("payment_type") != 6) # Filter out Voided transactions
                df = df.drop("payment_type")
                break  # Exit loop if successful

            
            elif ride_type == "Green Taxi Trip Records":
                df = pl.read_parquet(
                    url,
                    columns=[timestamp_col, "PULocationID", "DOLocationID", "payment_type", "total_amount"]
                )
                df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in schema_overrides.items()])
                df = df.with_columns(pl.lit(ride_type).alias("ride_type")) # Ride type column = Green Taxi
                df = df.filter(pl.col("payment_type") != 6) # Filter out Voided transactions
                df = df.drop("payment_type")
                break  # Exit loop if successful

            
            elif ride_type == "For-Hire Vehicle Trip Records":
                df = pl.read_parquet(
                    url,
                    columns=[timestamp_col, "PUlocationID", "DOlocationID"]
                )
                df = df.rename({"PUlocationID": "PULocationID", "DOlocationID": "DOLocationID"}) # Rename loc id cols
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("total_amount")) # Add a blank column
                df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in schema_overrides.items()]) # Fix the schema
                df = df.with_columns(pl.lit(ride_type).alias("ride_type")) # Ride type column
                break  # Exit loop if successful

            
            elif ride_type == "High Volume For-Hire Vehicle Trip Records":
                # Ride hailing brand mapping
                hvfhs_mapping = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}
                
                # Read the parquet file
                df = pl.read_parquet(
                    url,
                    columns=["hvfhs_license_num", timestamp_col, "PULocationID", "DOLocationID", 
                             "base_passenger_fare", "tolls", "bcf", "sales_tax", 
                             "congestion_surcharge", "airport_fee", "tips"]
                )

                # Create the total_amount column based on the other columns
                df = df.with_columns([
                    (pl.col("base_passenger_fare") + pl.col("tolls") + pl.col("bcf") + 
                     pl.col("sales_tax") + pl.col("congestion_surcharge") + pl.col("airport_fee") + pl.col("tips"))
                    .alias("total_amount")
                ])

                # Map the license num to uber, lyft, etc
                df = df.with_columns([
                    pl.col("hvfhs_license_num").replace(hvfhs_mapping).alias("ride_type")
                ])

                # Drop unnecessary columns
                df = df.drop(["hvfhs_license_num", "base_passenger_fare", "tolls", "bcf", 
                              "sales_tax", "congestion_surcharge", "airport_fee", "tips"])

                # Change the data types
                df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in schema_overrides.items()]) # modifying the data types
                break  # Exit loop if successful

        # In case of error, try again
        except Exception as e:
            if attempt < max_retries - 1:  # Check if retries are left
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            
            else:
                raise RuntimeError(f"Error reading the parquet file from {url} after {max_retries} attempts: {e}")

    # Additional preprocessing
    try:
        # Mapping the pickup and dropoff boroughs
        ## Pickup Borough
        df = (
            df
            .join(df_loc_dict[["Borough", "LocationID"]], 
                  how="left", 
                  left_on="PULocationID", right_on="LocationID")
            .rename({"Borough": "PUBorough"})
        )
        
        ## Dropoff Borough
        df = (
            df
            .join(df_loc_dict[["Borough", "LocationID"]], 
                  how="left", 
                  left_on="DOLocationID", right_on="LocationID")
            .rename({"Borough": "DOBorough"})
        )

        # Making new time and date columns
        df = df.with_columns(
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.truncate('1h').alias('timestamp_hour'),
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.date().alias('txn_date'),
            pl.col(timestamp_col).cast(pl.Datetime("ns")).dt.hour().alias('txn_hour')
        )
        
        # Performing aggregations to compute for the number of transactions
        df = (
             df
            .group_by(["txn_date", "txn_hour", "timestamp_hour", "ride_type", "PUBorough", "DOBorough", "PULocationID", "DOLocationID"])
            .agg([
                pl.len().alias("num_txns"),
                pl.col("total_amount").mean().alias("total_amount")
            ])
        )
        
        # Arranging the columns
        df = df.select([
            'txn_date', 'txn_hour', 'timestamp_hour',
            "PULocationID", 'PUBorough', 
            "DOLocationID", 'DOBorough',
            'ride_type',
            'num_txns','total_amount'
        ])

        # Hard-code the schema
        df = (
            df.with_columns([
                pl.col('txn_date').cast(pl.Date),
                pl.col('txn_hour').cast(pl.Int32),
                pl.col('timestamp_hour').cast(pl.Datetime),
                pl.col('PULocationID').cast(pl.Int32),
                pl.col('PUBorough').cast(pl.String),
                pl.col('DOLocationID').cast(pl.Int32),
                pl.col('DOBorough').cast(pl.String),
                pl.col('ride_type').cast(pl.String),
                pl.col('num_txns').cast(pl.Int32)
            ])
        )

        # Keep only the transactions within the month
        df = df.filter(pl.col("txn_date").dt.strftime("%B").str.to_lowercase() == month.lower())
        
    except Exception as e:
        raise RuntimeError(f"Error processing data for {ride_type}: {e}.")

    # Export the dataframe to a parquet file. Use gzip compression to save disk space
    try:
        # Filename of the parquet file
        filename = f"../data/monthly_aggregates/{year}/{month}/{ride_type.replace(' Trip Records', '')} - {month} {year}.parquet.gz"
        df.write_parquet(filename, compression="gzip")
        
    except Exception as e:
        raise RuntimeError(f"Error in writing parquet file: {e}.")



def combine_all(year):
    """Combines all parquet files in ..data/aggregates/ into a single parquet file."""
    # Check if the directory exists
    try:
        directory = rf'../data/monthly_aggregates/{year}/'
        months = os.listdir(directory)

    except Exception as e:
        raise RuntimeError(f"Error scanning {directory}: {e}.")

    # Get the parquet files
    try:
        parquet_files = []
        for month in months:
            file_dir = os.path.join(directory, month)
            parquet_file_in_dir = [os.path.normpath(os.path.join(file_dir, f)) for f in os.listdir(file_dir) if f.endswith('.parquet.gz')]
            parquet_files.extend(parquet_file_in_dir)

    except Exception as e:
        raise RuntimeError(f"Error finding the parquet files in {directory}: {e}.")


    # Read all the parquet files and store each in dfs
    dfs = []
    for file in parquet_files:
        df = pl.read_parquet(file)
        dfs.append(df)
    
    # Concatenate all parquet files
    df = pl.concat(dfs)

    # Export to a single parquet file
    concatenated_dir = rf"../data/concatenated/{year}"
    parquet_filename = rf"{year} Taxi and Ride Hailing Records.parquet.gz"
    final_pq_file_dir = os.path.normpath(os.path.join(concatenated_dir, parquet_filename))
    df.write_parquet(final_pq_file_dir, compression="gzip")
