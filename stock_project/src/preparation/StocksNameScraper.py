from stock_project.src.preparation.PersistenceAPI import PersistenceAPI

from bs4 import BeautifulSoup
import requests
import csv
from pathlib import Path

class StockNameScraper:

    @staticmethod
    def obtain_stock_names():
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        stock_names = []
        response = requests.get(url, timeout=5)
        content = BeautifulSoup(response.content, "html.parser")

        # We get stock_names from the web page
        for stock in content.findAll('a', attrs={"class": "external text"}):
            if(len(stock.text)<=5):
                stock_names.append(stock.text)

        # We persist the Stock Names
        save_dir = Path(__file__).parent.parent
        filename = (save_dir / "../data/stock_names.joblib").resolve()
        PersistenceAPI.persist_stock_data(stock_names, filename)

        return stock_names