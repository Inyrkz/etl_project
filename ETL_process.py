'''Data Engineering - ETL Process

Objectives
 
*   Run the ETL process
*   Extract bank and market cap data from the JSON file `bank_market_cap.json`
*   Transform the market cap currency using the exchange rate data
*   Load the transformed data into a seperate CSV
'''

# Imports
import glob
import pandas as pd
from datetime import datetime


def extract_from_json(file_to_process):
    '''Function to read JSON file
    
    Parameter
    ----------
    file_to_process: str. Name of the JSON data.
    
    Returns
    --------
    dataframe: Pandas DataFrame. DataFrame of the JSON file.
    '''
    dataframe = pd.read_json(file_to_process)
    return dataframe


# Invoke the Extract Function
columns=['Name','Market Cap (US$ Billion)']

def extract():
    '''Function to extract JSON file in the directory and
    load them into a Pandas DataFrame'''

    # Check for all JSON files in the directory
    for json_file in glob.glob('*.json'):
        # Read each file
        # In this case, there is only one JSON file.
        df = extract_from_json(json_file)    
        # filter the DataFrame to show the necessary columns
        df = df[columns]
        
    return df

# check
df = extract()
print(df.head())

# Load the file exchange_rates.csv as a dataframe and find the exchange rate for British pounds with the symbol
# GBP, store it in the variable exchange_rate.

csv_df = pd.read_csv('exchange_rates.csv', index_col=0)
exchange_rate = csv_df.loc['GBP'].values[0]
print(exchange_rate)


# Transform
# Using exchange_rate and the `exchange_rates.csv` file find the exchange rate of USD to GBP. 
# Write a transform function that
# 1.  Changes the `Market Cap (US$ Billion)` column from USD to GBP
# 2.  Rounds the Market Cap (US$ Billion)\` column to 3 decimal places
# 3.  Rename `Market Cap (US$ Billion)` to `Market Cap (GBP$ Billion)` 

def transform(exchange_rate, data):
    '''Transform the market cap data to a different currency rate. In this instance, 
    from dollars (USD) to pounds (GBP)
  
    Parameters
    -----------
    exchange_rate: float, the exchange rate value that will be used for conversion.
                    Exchange rate for GBP
    data: Pandas DataFrame, data with the column 'Market Cap (US$ Billion)' 
                    that will be transformed to 'Market Cap (GBP$ Billion)'
    Returns
    -------
    data: Pandas DataFrame, transformed DataFrame
    '''
    data['Market Cap (US$ Billion)'] = data['Market Cap (US$ Billion)'] * exchange_rate
    data['Market Cap (US$ Billion)'] = round(data['Market Cap (US$ Billion)'], 3)
    data.rename({'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'}, inplace=True)
    return data    

# Load
# Create a function that takes a dataframe and load it to a csv named `bank_market_cap_gbp.csv`. 
# Make sure to set `index` to `False`.

def load(transformed_data, filename='bank_market_cap_gbp.csv'):
    '''Function that takes transformed data and loads it to a CSV file
    
    Parameters
    ----------
    transformed_data: Pandas DataFrame, this is the data to be stored into CSV file
    filename: str, name of the csv file. Default value is bank_market_cap_gbp.csv
    '''
    # Remove index from the csv file
    transformed_data.to_csv(filename, index=False)   


# Logging Function 

def log(message):
    '''Function to log the time of each of the ETL process
     and store the log data in a file'''
    # define the date and time format
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    # get current time
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('log_file.txt', 'a') as file:
        file.write(message + ", " + timestamp + '\n')  


# Running the ETL Process

log("ETL Job Started")

log("Extract Phase Started")
# Call the extract() function
df = extract()
# Print the rows here
print(df.head())
log("Extract Phase Ended")

log("Transform Phase Started")
# Call the transform() function
transformed_df = transform(exchange_rate, df)
print(transformed_df.head())
log("Transform Phase Ended")

log("Load Phase Started")
load(transformed_df)
log("Load Phase Ended")

log("ETL Job Ended")
