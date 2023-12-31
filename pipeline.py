from datetime import datetime, timedelta
from dagster import op, In, Out, graph
from typing import List
import yfinance as yf
import pandas as pd

@op(ins={"ticker": In(dagster_type=str)}, out=Out(pd.DataFrame))

def download_data(context, ticker: str) -> pd.DataFrame:
    # Calculate start and end dates for the download
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=1)
    
    # Download the data for the ticker
    data = yf.download(ticker, start=start_date, end=end_date, interval="1m")
    
    # Filter to yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date()
    data = data.loc[data.index.date == yesterday]
    
    # Add column for ticker symbol
    data['Ticker'] = ticker
    
    return data

@op(out=Out(str))
def get_netflix() -> str:
    return "NFLX"

@op(out=Out(str))
def get_disney() -> str:
    return "DIS"

@op(ins={"data": In(dagster_type=pd.DataFrame)}, out=Out(pd.DataFrame))
def validate_data(context, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        context.log.error(f"Data invalid for ticker: {data.iloc[0]['Ticker']}")
        return data
    else:
        context.log.error(f"Data valid for ticker: {data.iloc[0]['Ticker']}")
        return data
    
@op(ins={"data": In(dagster_type=pd.DataFrame)}, out=Out(pd.DataFrame))
def clean_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Remove "Adj Close" column from data
    data.drop("Adj Close", axis=1, inplace=True)
    
    # Return the updated data frame
    return data

@op(ins={"data": In(dagster_type=pd.DataFrame)}, out=Out(pd.DataFrame))

def transform_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Columns needed to determine rolling VWAP
    data['Typical Price'] = (data['High'] + data['Low'] + data['Close']) / 3
    data['Cumulative TPV'] = data['Typical Price'] * data['Volume']
    data['Cumulative Volume'] = data['Volume'].cumsum()
    data['Rolling TPV'] = data['Cumulative TPV'].rolling('15min', min_periods=1).sum()
    data['Rolling Volume'] = data['Volume'].rolling('15min', min_periods=1).sum()
    
    # Add the rolling VWAP to the data frame
    data['VWAP'] = data['Rolling TPV'] / data['Rolling Volume']
    
    # Remove unneeded temporary columns
    data.drop(['Typical Price',
               'Rolling Volume',
               'Cumulative TPV',
               'Cumulative Volume',
               'Rolling TPV'], axis=1, inplace=True)
    
    # Calculate the cumulative dollar value of all trades
    dollar_value = (data['Close'] * data['Volume']).cumsum()
    
    # Add the dollar value column to the data data frame
    data['DollarValue'] = dollar_value
    
    # Return the udpated data frame
    return data

@op(ins={"dfs": In(dagster_type=List[pd.DataFrame])})
def write_to_csv(context, dfs):
    # Combine different datasets
    data = pd.concat(dfs, ignore_index=False).sort_values("Datetime")
    
    # Get the daily date of the data
    filepath = f"./{str(data.index[0].date())}.csv"
    
    # Write the transformed data to a CSV file
    data.to_csv(filepath)
    
    # Log a message to confirm that the data has been written to the file
    context.log.info(f"Data written to file: {filepath}")

##### Building the pipeline #####
    
@graph
def collection_pipeline(ticker: str):
    # Get data for ticker
    data = download_data(ticker)
    
    # Ensure data is valid and clean
    data = clean_data(validate_data(data))
    
    # Enrich data
    data = transform_data(data)
    
    return data

@graph
def running_pipeline():
    data_nflx = collection_pipeline(get_netflix())
    data_dis = collection_pipeline(get_disney())
    
    # Save the combined datasets to one location
    write_to_csv([data_nflx, data_dis])
    
#### Jobs ####
    
pipeline_job = running_pipeline.to_job(
    "both_tickers", description="Combined job for Netflix and Disney market data collection and processing."
)

