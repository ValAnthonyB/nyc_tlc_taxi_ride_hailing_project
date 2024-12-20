# NYC Taxi Trip Data Analysis and Demand Forecasting

Project Overview

This project automates data ingestion from the NYC TLC website and utilizes time series forecasting to analyze and predict ride demand trends. By forecasting future demand, we aim to support better resource allocation and improve service efficiency.

Key objectives include:

    1. Data Ingestion: Automating the scraping and storage of data to build a robust dataset of past trip records.
    2. Demand Forecasting: Using historical data to train time series models, providing accurate predictions of future ride demand.

## Data Source

The data is sourced from the NYC Taxi & Limousine Commission's Trip Record Data ([NYC Taxi & Limousine Commission's Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)), which provides hourly transactions of NYC taxi and ride-hailing services. To scrape data for a particular year, just use
```python
python scraper <input year here>
```


## Time Series Models

We use the following algorithms to make time series models on each New York City borough. 
* Naive forecast (last week, moving average)
* ARIMA
* XGBoost/LightGBM
* FB Prophet

We then evaluate each of the models using MAE, RMSE, R^2, and MAPE.