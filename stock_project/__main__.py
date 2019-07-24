from stock_project.src.preparation.StocksNameScraper import StockNameScraper
from stock_project.src.preparation.DataIngestionAPI import DataIngestion
from stock_project.src.preparation.PersistenceAPI import PersistenceAPI
from stock_project.src.processing.DataProcessing import DataProcessing

from pathlib import Path
from multiprocessing import Process
import pandas as pd
import quandl

def main(args=None):
    """The main routine."""

if __name__ == "__main__":
    main()

    # We get the path of the data directory (we need to use it several times)
    data_dir = (Path(__file__) / "../data").resolve()

    # We set the period of dates we want to make the calculations for
    start_date = '2017-01-01'
    end_date = '2018-01-01'

    # If first time running program, we scrape the web to get names. If not, we take the persisted data
    if not ((data_dir / 'stock_names.joblib').resolve()).is_file():
        stock_names = StockNameScraper.obtain_stock_names()
    
    else:
        stock_names = PersistenceAPI.get_stock_data((data_dir / 'stock_names.joblib').resolve())

    # Using stock names, and the previously set dates, we download the data for yahoo and quandl
    p1 = Process(target = DataIngestion.download_data_yahoo(stock_names, start_date, end_date))
    p2 = Process(target = DataIngestion.download_data_quandl(stock_names, '2017-01-01', '2018-01-01'))
    p1.start()
    p2.start()

    # We retrieve the data for Yahoo and Quandl
    filename_yahoo = "data_yahoo_" + start_date +"_to_" + end_date + ".joblib"
    yahoo_data = PersistenceAPI.get_stock_data((data_dir / 'yahoo' / filename_yahoo).resolve())

    filename_quandl = "data_quandl_" + start_date +"_to_" + end_date + ".joblib"
    quandl_data = PersistenceAPI.get_stock_data((data_dir / 'quandl' / filename_quandl).resolve())

    # Then, we perform the basic calculations and create the CSV files
    yahoo_return, yahoo_high_low, yahoo_turnover = DataProcessing.perform_basic_calculations(yahoo_data, "yahoo")
    quandl_return, quandl_high_low, quandl_turnover = DataProcessing.perform_basic_calculations(quandl_data, "quandl")
    
    # Now we create the mean CSV files
    DataProcessing.calculate_mean(yahoo_return, yahoo_high_low, yahoo_turnover, "yahoo")
    DataProcessing.calculate_mean(quandl_return, quandl_high_low, quandl_turnover, "quandl")

    # For the sample variance calculations, we want to specify the dates we want the data for
    DataProcessing.calculate_variance(yahoo_return, yahoo_high_low, yahoo_turnover, "yahoo",
     start_date=' 2017-02-11', end_date='2017-11-08')
    
    DataProcessing.calculate_variance(quandl_return, quandl_high_low, quandl_turnover, "quandl",
     start_date=' 2017-02-11', end_date='2017-11-08')

    # Same for the std deviation calculations
    DataProcessing.make_comparison((yahoo_return, yahoo_high_low, yahoo_turnover), 
                            (quandl_return, quandl_high_low, quandl_turnover), start_date='2017-08-17', end_date='2017-12-05')