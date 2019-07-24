# FinancePythonTest
Creating a Python app to take perform some actions with YahooFinance and quandl data.

This project was performed as part of a selection process, so take into account that the time for its development was limited.

To explain how it works:
    1. First, we get the Stock Names we want to obtain data for from a Wikipedia List via a Scraper.
    2. Afterwards, we query the Yahoo and Quandl API to obtain all the data we need (if a stock doesn't have data, it will be printed to a error file).
    3. This data will be cleaned and prepared in order to perform some operations on it. It will be persisted with the joblib library just in case we want to re-use the same data in future runs of the application.