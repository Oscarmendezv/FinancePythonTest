from pathlib import Path
import joblib
import csv

class  PersistenceAPI:

    def __init__(self):
        pass

    @staticmethod
    def persist_stock_data(data, path):
        """ This function takes data (preferably a dataframe) and uses joblib to persist it """

        joblib.dump(data, path)


    @staticmethod
    def get_stock_data(filepath):
        """ This function uses joblib to retrieve the persisted data """

        return joblib.load(filepath)


    def write_to_error_file(self, stock, raised_by):
        """ This function writes the stocks that couldn't be downloaded.

        Parameters
        ----------
        stock: str
            Indicates the name of the stock that couldn't be downloaded
        raised_by: str
            Indicates from which source we couldn't download the stock data (yahoo or quandl)
        """

        base_path = Path(__file__).parent.parent
        filename = raised_by + "_stock_errors.csv"
        file_path = (base_path / "../data/error/" / filename).resolve()

        with open(file_path, 'a+') as stock_errors:
            print(stock, file=stock_errors)