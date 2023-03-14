'''Data Engineering - Extract API Data

Extract data from the Exchange Rate Data API

Objectives
*   Collect exchange rate data using an API
*   Store the data as a CSV
''' 

# Importing libraries here.
import requests
import pandas as pd
import json


# Extract Data Using an API
# Rememeber to replace XXXXXXX with the actual API key
url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=XXXXXXX"
r = requests.get(url)
print(r.content)

# Save as DataFrame
# Using the data gathered turn it into a `pandas` dataframe. 
# The dataframe should have the `Currency` as the index and `Rate` as their columns. 

# Parse data to JSON
data = json.loads(r.content)
# Turn the data into a dataframe
df = pd.DataFrame(data)

# Drop unnescessary columns
# df.drop(['success', 'timestamp', 'base', 'date'], axis=1, inplace=True)
df = df[['rates']]
df.head()


# Load the Data
# Using the dataframe save it as a CSV names `exchange_rates_1.csv`.
df.to_csv('exchange_rates_1.csv')
