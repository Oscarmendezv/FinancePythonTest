from stock_project.src.processing.DataOutput import DataOutput
import pandas as pd
import numpy as np

class DataProcessing:

    output = DataOutput()

    @staticmethod
    def clean_data(data):
        """ Simple method that takes a DataFrame and takes the columns needed for the calculations."""

        data = data.iloc[:, :5]
        data.reset_index(level=0, inplace=True)
        return data


    @classmethod
    def perform_basic_calculations(cls, data, source, start_date=None, end_date=None):
        """ A function that performs three basic calculations (return, High-low and Turnover) on a DataFrame.
        
        Parameters
        ----------
        data: pandas.DataFrame
                A DataFrame with the stock information needed to perform calculations (High, Low, Open, Close and Volume)
        source: str
                Source from where the data comes (yahoo or quandl)
        start_date (optional=True): str in YYYY-MM-DD format
                Beginning day from which you want data
        end_date (optional=True): str in YYYY-MM-DD format
                Date until which you want data
         """

        if start_date != None and end_date != None:
            data = cls.apply_timerange(data, start_date, end_date)
    
        # We perform calculations of return and output it into CSV
        data_return = DataProcessing.calculate_return(data)
        cls.output.output_to_csv(data_return, source + "__return")

        # We perform calculations of high_low and output it into CSV
        data_high_low = DataProcessing.calculate_high_low(data)
        cls.output.output_to_csv(data_high_low, source + "__high_low")

        # We perform calculations of turnover and output it into CSV
        data_turnover = DataProcessing.calculate_turnover(data)
        cls.output.output_to_csv(data_turnover, source + "__turnover")

        return (data_return, data_high_low, data_turnover)


    @staticmethod
    def calculate_return(data):
        """ This method takes a dataframe with stock data and calculates the return of them for every considered day """

        data['return'] = (data['Close'] / data['Open']) -1
        data = data.pivot_table(values='return', index='Date', columns='Ticker')
        return data
    

    @staticmethod
    def calculate_high_low(data):
        """ This method takes a dataframe with stock data and calculates the high-low difference of them for every considered day """

        data['high_low'] = (data['High'] / data['Low']) -1
        data = data.pivot_table(values='high_low', index='Date', columns='Ticker')
        return data
    

    @staticmethod
    def calculate_turnover(data):
        """ This method takes a dataframe with stock data and calculates the turnover of them for every considered day """

        data['turnover'] = pd.to_numeric(data['Close'] * data['Volume'])
        data = data.pivot_table(values='turnover', index='Date', columns='Ticker')
        return data


    @classmethod
    def calculate_mean(cls, data_return, data_high_low, data_turnover, source=None, start_date=None, end_date=None):
        """ This method takes the return, high-low and turnover data and makes the necessary calculations to end up with
        a daily mean value of them for the whole stocks considered.

        Parameters
        ----------
        data_return, data_high_low, data_turnover: pandas.DataFrame
            The three needed DataFrames to make the mean calculation
        source (optional=True): str
            The source from which the data has been retrieved (yahoo or quandl)
        start_date (optional=True): str in YYYY-MM-DD format
                Beginning day from which you want data
        end_date (optional=True): str in YYYY-MM-DD format
                Date until which you want data
        """

        # We create a new column with the mean for every row in every data set
        data_return = cls.mean_calculation(data_return, "return")
        data = data_return['return']
        data_return = data_return.drop('return', axis=1)

        data_high_low = cls.mean_calculation(data_high_low, "high_low")
        data = pd.concat([data, data_high_low['high_low']], axis=1)
        data_high_low = data_high_low.drop('high_low', axis=1)

        data_turnover = cls.mean_calculation(data_turnover, "turnover")
        data = pd.concat([data, data_turnover['turnover']], axis=1)
        data_turnover = data_turnover.drop('turnover', axis=1)

        if start_date != None and end_date != None:
            data = cls.apply_timerange(data, start_date, end_date)

        # We output the CSV
        if source != None:
            cls.output.output_to_csv(data, source + '__mean')
        else:
            cls.output.output_to_csv(data, 'consolidated__mean')


    @staticmethod
    def mean_calculation(data, name):
        """ This method takes a dataframe of previously treated stock data and calculates the mean 
        per row. The parameter name should indicate the type of data we are performing the mean for (return, high-low or turnover).
        """

        data[name] = data.mean(axis=1)
        return data


    @classmethod
    def calculate_variance(cls, data_return, data_high_low, data_turnover, source=None, start_date=None, end_date=None):
        """ This method takes the return, high-low and turnover data and makes the necessary calculations to end up with
        a daily variance value of them for the whole stocks considered.

        Parameters
        ----------
        data_return, data_high_low, data_turnover: pandas.DataFrame
            The three needed DataFrames to make the mean calculation
        source (optional=True): str
            The source from which the data has been retrieved (yahoo or quandl)
        start_date (optional=True): str in YYYY-MM-DD format
                Beginning day from which you want data
        end_date (optional=True): str in YYYY-MM-DD format
                Date until which you want data
        """

        # We create a new column with the var for every row in every data set
        data_return = cls.variance_calculation(data_return, 'return')
        data = data_return['return']
        
        data_high_low = cls.variance_calculation(data_high_low, 'high_low')
        data = pd.concat([data, data_high_low['high_low']], axis=1)

        data_turnover = cls.variance_calculation(data_turnover, 'turnover')
        data = pd.concat([data, data_turnover['turnover']], axis=1)

        if start_date != None and end_date != None:
            data = cls.apply_timerange(data, start_date, end_date)

        # We output the CSV
        if source != None:
            cls.output.output_to_csv(data, source + '__variance')
        else:
            cls.output.output_to_csv(data, 'consolidated__variance')


    @staticmethod
    def variance_calculation(data, name):
        """ This method takes a dataframe of previously treated stock data and calculates the variance 
        per row. The parameter name should indicate the type of data we are performing the mean for (return, high-low or turnover).
        """

        data[name] = data.var(axis=1)
        return data


    @classmethod
    def make_comparison(cls, data_yahoo: tuple, data_quandl: tuple, start_date=None, end_date=None, compare_consolidated_to=None):
        """ Takes the return, high-low and turnover data from both sources (yahoo and quanld) and compares their results daily.

        Parameters 
        ----------
        data_yahoo: tuple
            Tuple containing the three required DataFrames return, high-low and turnover from yahoo
        data_quandl: tuple
            Tuple containing the three required DataFrames return, high-low and turnover from yahoo
        start_date (optional=True): str in YYYY-MM-DD format
            Beginning day from which you want data
        end_date (optional=True): str in YYYY-MM-DD format
            Date until which you want data
        compare_consolidated_to (optional=True): str
            When comparing consolidated data, this parameter should indicate to which source we are comparing it (yahoo or quandl)
        """

        data_return, data_high_low, data_turnover = data_yahoo
        data_compare_return, data_compare_high_low, data_compare_turnover = data_quandl

        # We calculate the deviation for the return
        compared_return = cls.calculate_deviation(data_return, data_compare_return, 'return')
        data = compared_return['return']

        # We calculate the deviation for the return
        compared_high_low = cls.calculate_deviation(data_high_low, data_compare_high_low, 'high_low')
        data = pd.concat([data, compared_high_low['high_low']], axis=1)

        # We calculate the deviation for the return
        compared_turnover = cls.calculate_deviation(data_turnover, data_compare_turnover, 'turnover')
        data = pd.concat([data, compared_turnover['turnover']], axis=1)

        if start_date != None and end_date != None:
            data = cls.apply_timerange(data, start_date, end_date)

        # We output the CSV
        if compare_consolidated_to != None:
            cls.output.output_to_csv(data, 'consolidated_' + compare_consolidated_to + '__comparison')
        else:
            cls.output.output_to_csv(data, 'yahoo_quandl__comparison')


    @staticmethod    
    def calculate_deviation(data, data_to_compare, name):
        """ Simple method that takes two dataframes and calculates the std of their difference row per row. The name should indicate
        for what data we are calculating the std (return, high-low or turnover).
        """

        compared_values = data - data_to_compare 
        compared_values[name] = compared_values.std(axis=1)

        return compared_values


    @classmethod
    def consolidate_dataframes(cls, stock_name, data, data_to_compare):
        """ This method consolidates stock data. Given two stock dataframes, it takes the lowest value in every variable for each stock.

        Parameters
        ----------
        stock_name: list
            Whole list of stock_names being considered. Used to avoid inconsistencies (every source could've downloaded different stocks)
        data: pandas.DataFrame
            First dataframe to consider for the consolidation of data.
        data_to_compare: pandas.DataFrame
            Second dataframe to consider for the consolidation of data.
        """

        # We eliminate unnecessary columns
        data = data.iloc[:, :-1]
        data.sort_index(inplace=True)
        data_to_compare = data_to_compare.iloc[:, :-1]
        data.sort_index(inplace=True)

        # We merge the data so we can process the consolidation (to use a common index)
        combined_data = pd.merge(data, data_to_compare, left_index=True, right_index=True, how='outer')
        # We set the new 'Date' index to a newly created DataFrame
        consolidated_data = pd.DataFrame(index = combined_data.index.copy())

        # We iterate through every stock name to check from where to gather the data (if available)
        for name in stock_name:
            name_data = name +'_x'
            name_data_to_compare = name + '_y'
            if name in data and name in data_to_compare:
                consolidated_data[name] = np.where(np.logical_or(combined_data[name_data].notna(), 
                                                combined_data[name_data_to_compare].notna()) 
                                                & combined_data[name_data].abs() < combined_data[name_data_to_compare].abs(),
                                                combined_data[name_data], combined_data[name_data_to_compare])
            
            elif name in data:
                consolidated_data[name] = combined_data[name]
            
            elif name in data_to_compare:
                consolidated_data[name] = combined_data[name]
                
        return consolidated_data


    @staticmethod
    def apply_timerange(data, start_date, end_date):
        """ This method is used to standarize the way of filtering stock data between a start (included) and end date (not included) """

        # As end date shouldn't be included, we take a day off it
        arr = end_date.split("-")
        final_date = arr[0] + '-' + arr[1] + '-' + str(int(arr[2]) -1)
        # We filter for the desired dates
        data = data.loc[start_date:final_date]

        return data