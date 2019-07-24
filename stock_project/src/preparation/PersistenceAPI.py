from pathlib import Path
import joblib
import csv

class  PersistenceAPI:

    def __init__(self):
        pass

    @staticmethod
    def persist_stock_data(data, path):
        joblib.dump(data, path)

    @staticmethod
    def get_stock_data(filepath):
        return joblib.load(filepath)

    def write_to_error_file(self, stock, raised_by):
        base_path = Path(__file__).parent.parent
        filename = raised_by + "_stock_errors.csv"
        file_path = (base_path / "../data/error/" / filename).resolve()

        with open(file_path, 'a+') as stock_errors:
            print(stock, file=stock_errors)