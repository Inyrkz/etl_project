'''Data Engineering - Webscraping
Use webscraping to get bank information
'''

# Import libraries.
from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd


# Extract Data Using Web Scraping
# Gather the contents of the webpage in text format using the `requests` library 
# and assign it to the variable.

url = "https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks"
r = requests.get(url)
# Gather website data in text format
html_data = r.content

# Scraping the Data
soup = BeautifulSoup(html_data, 'html5lib')

# Load the data from the `By market capitalization` table into a pandas dataframe. 
# The dataframe should have the bank `Name` and `Market Cap (US$ Billion)` as column names.
#  Using the empty dataframe `data` and the given loop extract the necessary data from each row 
# and append it to the empty dataframe.

data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])

for row in soup.find_all('tbody')[2].find_all('tr'):
    col = row.find_all('td')
    if len(col) != 0:
        col1 = str(col[-2].find_all('a')[-1]).strip('</a>').split('>')[-1]
        col2 = str(col[-1]).strip('</td>').strip('\n')
        # store as a tuple
        sample = (col1, col2)
        # append to dataframe
        data = data.append(pd.Series(sample, index=data.columns), ignore_index=True)

# view dataframe
data.head()

# Load the `pandas` dataframe created above into a JSON named `bank_market_cap.json` 
data.to_json('bank_market_cap.json')
