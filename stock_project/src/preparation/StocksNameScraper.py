from bs4 import BeautifulSoup
import requests


class StockNameScraper:

    @staticmethod
    def obtainStockNames():
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        stock_names = []
        response = requests.get(url, timeout=5)
        content = BeautifulSoup(response.content, "html.parser")

        for stock in content.findAll('a', attrs={"class": "external text"}):
            if(len(stock.text.encode('utf-8'))<=5):
                stock_names.append(stock.text.encode('utf-8'))

        return stock_names