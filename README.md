## Extract, Transform & Load (ETL)

**Scenario**
In my latest project, I assumed the role of a data engineer working for an international financial analysis company. My hypothetical company tracks stock prices, commodities, forex rates, inflation rates.  
My job was to extract financial data from various sources like websites, APIs and files provided by various financial analysis firms. After I collected the data, I extracted the data of interest to my hypothetical company and transformed it based on the requirements given to me. After the transformation was completed, I loaded that data into a file/database.

**Steps**
- I extracted the exchange rate data from the Exchange Rates Data API.
- I extracted the market capitalization data from Wikipedia by scraping the website using BeautifulSoup.
- I wrote a script to perfrom ETL (Extract, Transform, Load) operation on the data.
- During the transform stage, I converted the market cap(US billions) to market cap(GBP billions).
- I also created a log to track the time the extract, transform and load jobs started and ended.

The API extraction notebook is `API_data_extraction.ipynb`.
The web scraping notebook is `webscraping_wikipedia.ipynb`.
The ETL job notebook is here `ETL_process.ipynb`.
