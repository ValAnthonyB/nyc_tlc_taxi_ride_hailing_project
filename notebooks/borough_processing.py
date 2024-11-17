import os
import polars as pl
import datetime

directory = rf'../../data/concatenated/'
years = os.listdir(directory)

parquet_files = []
for year in years:
    file_dir = os.path.join(directory, year)

    # Get the parquet files
    parquet_file_in_dir = [os.path.normpath(os.path.join(file_dir, f)) for f in os.listdir(file_dir) if f.endswith('.parquet.gz')]
    parquet_files.extend(parquet_file_in_dir)

for file in parquet_files:
    print(f"Processing {file}")
    # Read the file
    df = pl.read_parquet(file)

    # Get the year 
    year = file.split('\\')[-2] # For filename purposes

    # Iterate over the boroughs
    for borough in ["EWR", "Brooklyn", "Manhattan", "Staten Island", "Queens", "Bronx"]:
        print(f"\tProcessing {borough} for year {year}.")
        
        # Initial aggregation
        print(f"\t\tDoing initial aggregation.")
        df_agg = (
            df
            .filter(pl.col("PUBorough") == borough) 
            .filter(pl.col("txn_date").dt.year() == 2020) # Ensure that the data is only from 2020
            .group_by([
                'txn_date','txn_hour',
                'timestamp_hour',
                'PUBorough','ride_type'
            ])
            .agg([
                pl.col("num_txns").sum().alias("num_txns"),
                pl.col("total_amount").sum().alias("total_amount"),
            ])
        )

        # # Pivot the ride types
        # print(f"\t\tPivot the ride types.")
        

        # Export the dataframe
        print(f"\t\tExport the dataframe in parquet format.\n")
        parquet_filename = f"..\\..\\data\\processed by borough\\{borough}\\{borough} TLC Records - {year}.parquet.gz"
        df_agg.write_parquet(parquet_filename, compression="gzip")

processed_path = f"..\\..\\data\\processed by borough"
main_directories = [f"{processed_path}\\{d}" for d in os.listdir(processed_path) if os.path.isdir(os.path.join(processed_path, d))]

dfs = []
for main_dir in main_directories:
    # Get the borough name from the directory
    borough = main_dir.split('\\')[-1]
    parquet_files = [rf"{main_dir}\{f}" for f in os.listdir(main_dir) if f.endswith('.parquet.gz')]

    # Loop through the parquet files
    for filename in parquet_files:
        df = pl.read_parquet(filename)
        df_agg = (
            df
            .pivot(
                values=["num_txns", "total_amount"],  # Specify two value columns
                index=["txn_date", "txn_hour", "timestamp_hour"],            # Index column
                on="ride_type",              # Columns to pivot on
                maintain_order=True
            )
            .sort('timestamp_hour', nulls_last=True)
        )
        print(df_agg.columns)
        dfs.append(df_agg)
        
    # Concatenate the dataframes
    df = pl.concat(dfs)
    df.write_parquet(f".\\..\\data\\final processed\\{borough} - all.parquet.gz", compression="gzip")
    
print("DONE")