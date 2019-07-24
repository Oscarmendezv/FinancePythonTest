from stock_project.src.preparation.PersistenceAPI import PersistenceAPI
from stock_project.src.processing.DataProcessing import DataProcessing
from pathlib import Path

import yfinance as yf
import pandas as pd
import quandl

class DataIngestion:

    save_dir = Path(__file__).parent.parent
    persistenceApi = PersistenceAPI()

    @classmethod
    def download_data_yahoo(cls, stock_names, start_date, end_date):
        # We set the paths to persist the data
        data_filename = "data_yahoo_" + start_date +"_to_" + end_date + ".joblib"
        full_data_path = (cls.save_dir / "../data/yahoo/" / data_filename).resolve()

        # We check if we already have the data persisted for the specified dates
        if not full_data_path.is_file():
            yahoo_stock_data = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])

            # We obtain the data from yahoo, clean it and concatenate it all together into a pandas DataFrame
            for name in stock_names:
                share = yf.Ticker(name)
                try:
                    data = share.history(start=start_date, end=end_date)
                    data = DataProcessing.clean_data(data)
                    data['Ticker'] = name
                    yahoo_stock_data =  pd.concat([yahoo_stock_data, data], ignore_index=True)
                        
                except ValueError:
                    cls.persistenceApi.write_to_error_file(name, "yahoo")

            # We persist the data
            PersistenceAPI.persist_stock_data(yahoo_stock_data, full_data_path)
                    

    @classmethod
    def download_data_quandl(cls, stock_names, start_date, end_date):
        # We set the paths to persist the data
        data_filename = "data_quandl_" + start_date +"_to_" + end_date + ".joblib"
        full_data_path = (cls.save_dir / "../data/quandl" / data_filename).resolve()

        # We check if we already have the data persisted for the specified dates
        if not full_data_path.is_file():
            quandl_stock_data = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])

            for name in stock_names:
                query_name = "WIKI/" + name

                try:
                    data = quandl.Dataset(query_name).data(params={ 'start_date':start_date, 'end_date':end_date}).to_pandas()
                    data = DataProcessing.clean_data(data)
                    data['Ticker'] = name
                    quandl_stock_data =  pd.concat([quandl_stock_data, data], ignore_index=True)

                except:
                    cls.persistenceApi.write_to_error_file(name, "quandl")

            # We persist the data
            PersistenceAPI.persist_stock_data(quandl_stock_data, full_data_path)

