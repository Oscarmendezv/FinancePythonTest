from stock_project.src.processing.DataOutput import DataOutput
import pandas as pd

class DataProcessing:

    output = DataOutput()

    @staticmethod
    def clean_data(data):
        data = data.iloc[:, :5]
        data.reset_index(level=0, inplace=True)
        return data

    @classmethod
    def perform_basic_calculations(cls, data, source, start_date=None, end_date=None):

        if start_date != None and end_date != None:
            data = data.loc[start_date:end_date]
    
        # We perform calculations of return and output it into CSV
        data_return = DataProcessing.calculate_return(data)
        cls.output.output_to_csv(data_return, "return", source=source)

        # We perform calculations of high_low and output it into CSV
        data_high_low = DataProcessing.calculate_high_low(data)
        cls.output.output_to_csv(data_high_low, "high_low", source=source)

        # We perform calculations of turnover and output it into CSV
        data_turnover = DataProcessing.calculate_turnover(data)
        cls.output.output_to_csv(data_turnover, "turnover", source=source)

        return (data_return, data_high_low, data_turnover)

    @staticmethod
    def calculate_return(data):
        data['return'] = (data['Close'] / data['Open']) -1
        data = data.pivot_table(values='return', index='Date', columns='Ticker')
        return data
    
    @staticmethod
    def calculate_high_low(data):
        data['high_low'] = (data['High'] / data['Low']) -1
        data = data.pivot_table(values='high_low', index='Date', columns='Ticker')
        return data
    
    @staticmethod
    def calculate_turnover(data):
        data['turnover'] = pd.to_numeric(data['Close'] * data['Volume'])
        data = data.pivot_table(values='turnover', index='Date', columns='Ticker')
        return data

    @classmethod
    def calculate_mean(cls, data_return, data_high_low, data_turnover, source, start_date=None, end_date=None):

        # We create a new column with the mean for every row in every data set
        data_return = cls.mean_calculation(data_return, "return")
        data = data_return['return']

        data_high_low = cls.mean_calculation(data_high_low, "high_low")
        data = pd.concat([data, data_high_low['high_low']], axis=1)

        data_turnover = cls.mean_calculation(data_turnover, "turnover")
        data = pd.concat([data, data_turnover['turnover']], axis=1)

        if start_date != None and end_date != None:
            arr = end_date.split("-")
            final_date = arr[0] + '-' + arr[1] + '-' + str(int(arr[2]) -1)
            # We get the dates we want
            data = data.loc[start_date:final_date]

        # We output the CSV
        cls.output.output_to_csv(data, "mean", source=source)

    @staticmethod
    def mean_calculation(data, name):
        data[name] = data.mean(axis=1)
        return data

    @classmethod
    def calculate_variance(cls, data_return, data_high_low, data_turnover, source, start_date=None, end_date=None):

        # We create a new column with the mean for every row in every data set
        data_return = cls.variance_calculation(data_return, 'return')
        data = data_return['return']
        
        data_high_low = cls.variance_calculation(data_high_low, 'high_low')
        data = pd.concat([data, data_high_low['high_low']], axis=1)

        data_turnover = cls.variance_calculation(data_turnover, 'turnover')
        data = pd.concat([data, data_turnover['turnover']], axis=1)

        if start_date != None and end_date != None:
            arr = end_date.split("-")
            final_date = arr[0] + '-' + arr[1] + '-' + str(int(arr[2]) -1)
            # We get the dates we want
            data = data.loc[start_date:final_date]

        # We output the CSV
        cls.output.output_to_csv(data, "variance", source=source)

    @staticmethod
    def variance_calculation(data, name):
        data[name] = data.var(axis=1)
        return data

    @classmethod
    def make_comparison(cls, data: tuple, data_to_compare: tuple, start_date=None, end_date=None):
        yahoo_return, yahoo_high_low, yahoo_turnover = data
        quandl_return, quandl_high_low, quandl_turnover = data_to_compare

        # We calculate the deviation for the return
        compared_return = cls.calculate_deviation(yahoo_return, quandl_return, 'return')
        data = compared_return['return']

        # We calculate the deviation for the return
        compared_high_low = cls.calculate_deviation(yahoo_high_low, quandl_high_low, 'high_low')
        data = pd.concat([data, compared_high_low['high_low']], axis=1)

        # We calculate the deviation for the return
        compared_turnover = cls.calculate_deviation(yahoo_turnover, quandl_turnover, 'turnover')
        data = pd.concat([data, compared_turnover['turnover']], axis=1)

        if start_date != None and end_date != None:
            arr = end_date.split("-")
            final_date = arr[0] + '-' + arr[1] + '-' + str(int(arr[2]) -1)
            # We get the dates we want
            data = data.loc[start_date:final_date]

        # We output the CSV
        cls.output.output_to_csv(data, "yahoo_quandl__comparison")

    @staticmethod    
    def calculate_deviation(data, data_to_compare, name):
        compared_values = data - data_to_compare 
        compared_values[name] = compared_values.std(axis=1)

        return compared_values

    @classmethod
    def consolidated_calculations(cls):
        None
