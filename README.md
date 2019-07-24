# FinancePythonTest
Creating a Python app to take perform some actions with YahooFinance and quandl data.

This project was performed as part of a selection process, so take into account that the time for its development was limited.

To explain how it works:
    1. First, we get the Stock Names we want to obtain data for from a Wikipedia List via a Scraper.
    2. Afterwards, we query the Yahoo and Quandl API to obtain all the data we need (if a stock doesn't have data, it will be printed to a error file).
    3. This data will be cleaned and prepared in order to perform some operations on it. It will be persisted with the joblib library just in case we want to re-use the same data in future runs of the application.
    4. Once we have the data downloaded and concatenated into a single pandas DataFrame, we begin doing the desired calculations.
    5. First, we do the basic calculations, which consist of return, high-low and turnover, for every stock through every desired day. We save them into 3 different dataframes for each source of data (yahoo or quandl).
    6. Once we have the basic calculations dataframes, we can start calculating more important data: mean and variance. The mean is important because it shows the average per day for every previous calculation if you take into account every stock.
    7. Then we compare the data between yahoo and quandl for each of the basic calculations.
    8. Last of all, we repeat every calculation but treating the data in order to have it "consolidated". This means, we take the lowest value for every variable from yahoo or quandl.