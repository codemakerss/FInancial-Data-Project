from alpha_vantage.timeseries import TimeSeries
from numpy.lib.index_tricks import _diag_indices_from
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine
from urllib.parse import quote  
from datetime import date
import mysql.connector
import pymysql
import pandas as pd
import pandas as pd
import requests
import calendar
import xlwt
import csv 
class Alpha_VantageAPI(object):

    def __init__(self, apikey : str):
        self.apikey = apikey

    # Get Stock Information 
    # daily stock price 
    def GetDailyStockPrice(self, stock_id : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey ) 
        #data, meta_data = ts.get_daily_adjusted(stock_id, outputsize='full')
        data, meta_data = ts.get_daily(stock_id, outputsize='full')
        symbol_df = pd.DataFrame.from_dict( data, orient = 'index' )
        symbol_df = symbol_df.apply(pd.to_numeric)
        symbol_df.index = pd.to_datetime( symbol_df.index )
        #symbol_df.columns = [ 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amt','split_coef' ]
        symbol_df.columns = [ 'open', 'high', 'low', 'close', 'volume' ]
        symbol_df = symbol_df.sort_index( ascending = False )
        #symbol_df = symbol_df.drop('split_coef', axis = 1)
        symbol_df = symbol_df.rename_axis('datetime').reset_index()
        col_name = symbol_df.columns.tolist()
        col_name.insert(0,'symbol')
        symbol_df = symbol_df.reindex(columns=col_name)
        symbol_df['symbol'] = stock_id

        return symbol_df

    # weekly stock price
    def GetWeeklyStockPrice(self, stock_id : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey )
        data, meta_data = ts.get_weekly_adjusted(stock_id)
        symbol_df = pd.DataFrame.from_dict( data, orient = 'index' )
        symbol_df = symbol_df.apply(pd.to_numeric)
        symbol_df.index = pd.to_datetime( symbol_df.index )
        symbol_df.columns = [ 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amt' ]
        symbol_df = symbol_df.sort_index( ascending = False )
        symbol_df = symbol_df.rename_axis('datetime').reset_index()
        col_name = symbol_df.columns.tolist()
        col_name.insert(0,'symbol')
        symbol_df = symbol_df.reindex(columns=col_name)
        symbol_df['symbol'] = stock_id

        return symbol_df

    # monthly stock price
    def GetMonthlyStockPrice(self, stock_id : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey )
        data, meta_data = ts.get_monthly_adjusted(stock_id)
        symbol_df = pd.DataFrame.from_dict( data, orient = 'index' )
        symbol_df = symbol_df.apply(pd.to_numeric)
        symbol_df.index = pd.to_datetime( symbol_df.index )
        symbol_df.columns = [ 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amt' ]
        symbol_df = symbol_df.sort_index( ascending = False )
        symbol_df = symbol_df.rename_axis('datetime').reset_index()
        col_name = symbol_df.columns.tolist()
        col_name.insert(0,'symbol')
        symbol_df = symbol_df.reindex(columns=col_name)
        symbol_df['symbol'] = stock_id

        return symbol_df

    # intraday stock price - most recent 1 to 2 months data
    def GetIntradayStockPrice(self, stock_id : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey )
        data, meta_data = ts.get_intraday( stock_id, interval = '5min', outputsize = 'full')
        symbol_df = pd.DataFrame.from_dict( data, orient = 'index' )
        symbol_df = symbol_df.apply(pd.to_numeric)
        symbol_df.index = pd.to_datetime( symbol_df.index )
        symbol_df.columns = [ 'open', 'high', 'low', 'close', 'volume']
        symbol_df = symbol_df.sort_index( ascending = False )
        symbol_df = symbol_df.rename_axis('datetime').reset_index()
        col_name = symbol_df.columns.tolist()
        col_name.insert(0,'symbol')
        symbol_df = symbol_df.reindex(columns=col_name)
        symbol_df['symbol'] = stock_id

        return symbol_df
    
    # Get more stocks price - no more than 5 stocks due to API call limits 
    def GetMultiStockPrice(self, stocks_id : list) -> DataFrame:
        #apikey = "BF4TLBIGC0D0F8RY"
        df = pd.DataFrame()
        function_use = input('Choose the stock price time interval you want (daily, weekly, monthly, intraday) : ')
        if function_use.lower() == 'daily':
            for stock in stocks_id:
                df = df.append(self.GetDailyStockPrice(stock))
        elif function_use.lower() == 'weekly':
            for stock in stocks_id:
                df = df.append(self.GetWeeklyStockPrice(stock))
        elif function_use.lower() == 'monthly':
            for stock in stocks_id:
                df = df.append(self.GetMonthlyStockPrice(stock))
        elif function_use.lower() == 'intraday':
            for stock in stocks_id:
                df = df.append(self.GetIntradayStockPrice(stock))
        else:
            print('We do not have this function to use')
        # Check DataFrame empty 
        if df.empty:
            return None
        else:
            return df

    # Company Information 
    # Currency, GrossProfit in last 5 years - from 2016/12/31 to 2020/12/31, Total Revenue, NetIncome
    def GetIncomeStatement(self, stock_id : str) -> DataFrame:
        base_url = 'https://www.alphavantage.co/query?'
        df = pd.DataFrame()
        df_new = pd.DataFrame()
        params = {'function': 'INCOME_STATEMENT', 'symbol': stock_id, 'apikey': self.apikey}
        response = requests.get(base_url, params=params)
        data = response.json() # dict
        data_annual = data['annualReports']
        
        for dict in data_annual:
            df = df.append(pd.DataFrame([dict]))
        df_new = df.loc[:,['fiscalDateEnding','reportedCurrency','grossProfit', 'totalRevenue', 'netIncome']]
        
        col_name = df_new.columns.tolist()
        col_name.insert(0,'Symbol')
        df_new = df_new.reindex(columns=col_name)
        df_new['Symbol'] = stock_id

        return df_new

    # Symbol, Name, Exchange, Country, Sector, Industry, Fiscal year end, 52 Week high, 52 Week low, 50DayMovingAverage, 200DayMovingAverage, 
    def GetCompanyOverview(self, stock_id : str) -> DataFrame:
        base_url = 'https://www.alphavantage.co/query?'
        df_new = pd.DataFrame()
        params = {'function': 'OVERVIEW', 'symbol': stock_id, 'apikey': self.apikey}
        response = requests.get(base_url, params=params)
        data = response.json() # dict

        df = pd.DataFrame([data])
        df_new = df.loc[:,['Symbol', 'Name','Exchange','Country', 'Sector', 'Industry', 'FiscalYearEnd', '52WeekHigh', '52WeekLow','50DayMovingAverage', '200DayMovingAverage']]
        
        return df_new
    
    # Symbol, Name, Exchange, AssetType, IPO Date, Delisting Date, Status
    # This is the old version of function
    def GetListingDelistingStatus_bkp(self) -> DataFrame:
        CSV_URL ='https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=' + self.apikey
        data_lst = []
        with requests.Session() as s:
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
        
        for row in my_list:
            data_lst.append(row)
        df = pd.DataFrame(columns=data_lst[0], data = data_lst[1:])

        return df 

    # Symbol, Name, Exchange, AssetType, IPO Date, Delisting Date, Status
    # This is the new version of function
    def GetListingDelistingStatus(self) -> DataFrame:
        CSV_URL ='https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=' + self.apikey
        r = requests.get(CSV_URL)
        decoded_content = r.content.decode('utf-8')

        df = pd.DataFrame()
        for i in decoded_content.splitlines():
            data_list = i.split(',')
            df = df.append(pd.DataFrame(data_list).T, ignore_index=True)
        df = df.rename(columns=df.iloc[0])
        df = df.drop(df.index[0])
        df.loc[(df["delistingDate"] == "null"), "delistingDate"] = "1970-01-01"
        return df 

    # Symbol, Name, Type, Region, MarketOpen, MarketClose, Timezone, Currency, MatchScore 
    def GetSearchEndpoint(self, find_stock : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey )
        data = ts.get_symbol_search(find_stock)
        data = data[0]

        df = pd.DataFrame()
        for dict in data:
            df = df.append(pd.DataFrame([dict]))
        df.columns = ['Symbol', 'Name', 'Type', 'Region', 'MarketOpen', 'MarketClose', 'Timezone', 'Currency', 'MatchScore']
        return df 
   

    # Find IPO companies in the next three months
    # 'symbol', 'name', 'ipoDate', 'priceRangeLow', 'priceRangeHigh', 'currency', 'exchange'
    # This is the old version
    def FindIPOCalender_bkp(self) -> DataFrame:
        CSV_URL = 'https://www.alphavantage.co/query?function=IPO_CALENDAR&apikey=' + self.apikey
        data_lst = []
        with requests.Session() as s:
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
                
        for row in my_list:
            data_lst.append(row)
        df = pd.DataFrame(columns=data_lst[0], data = data_lst[1:])

        return df
    
    # 'symbol', 'name', 'ipoDate', 'priceRangeLow', 'priceRangeHigh', 'currency', 'exchange'
    # This is the new version
    def FindIPOCalender(self) -> DataFrame:
        CSV_URL = 'https://www.alphavantage.co/query?function=IPO_CALENDAR&apikey=' + self.apikey
        r = requests.get(CSV_URL)
        decoded_content = r.content.decode('utf-8')

        df = pd.DataFrame()
        for i in decoded_content.splitlines():
            data_list = i.split(',')
            df = df.append(pd.DataFrame(data_list).T, ignore_index=True)
        df = df.rename(columns=df.iloc[0])
        df = df.drop(df.index[0])

        return df
    
    # CSV file - Filter data 
    def CSV_Output(self, stock_id : str) -> DataFrame:
        workbook = xlwt.Workbook()
        workbook.add_sheet('Daily Price')
        workbook.add_sheet('Weekly Price')
        workbook.add_sheet('Monthly Price')
        workbook.add_sheet('Intraday Price')
        workbook.add_sheet('Income Statement Annual Reports')
        workbook.add_sheet('Company Overview')
        workbook.add_sheet('Search Endpoint Results')
        workbook.add_sheet('US ListingDelisting Status')
        workbook.add_sheet('IPO Calender')
        workbook.save('Filter_Data.xlsx')
        writer = pd.ExcelWriter('Filter_Data.xlsx', engine='xlsxwriter')
        self.GetDailyStockPrice(stock_id).to_excel(writer, sheet_name='Daily Price')
        self.GetWeeklyStockPrice(stock_id).to_excel(writer, sheet_name='Weekly Price')
        self.GetMonthlyStockPrice(stock_id).to_excel(writer, sheet_name='Monthly Price')
        self.GetIntradayStockPrice(stock_id).to_excel(writer, sheet_name='Intraday Price')
        self.GetIncomeStatement(stock_id).to_excel(writer, sheet_name='Income Statement Annual Reports')
        self.GetCompanyOverview(stock_id).to_excel(writer, sheet_name='Company Overview')
        self.GetSearchEndpoint(stock_id).to_excel(writer, sheet_name='Search Endpoint Results')
        self.GetListingDelistingStatus().to_excel(writer, sheet_name='US ListingDelisting Status')
        self.FindIPOCalender().to_excel(writer, sheet_name='IPO Calender')
        writer.save()

    # CSV file - Original data
    def GetDailyStockPrice_Original(self, stock_id : str) -> DataFrame:
        ts = TimeSeries( key = self.apikey )
        data, meta_data = ts.get_daily_adjusted(stock_id, outputsize='full')
        symbol_df = pd.DataFrame.from_dict( data, orient = 'index' )
        symbol_df = symbol_df.apply(pd.to_numeric)
        symbol_df.index = pd.to_datetime( symbol_df.index )
        symbol_df.columns = [ 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amt','split_coef' ]
        symbol_df = symbol_df.sort_index( ascending = False )
        symbol_df = symbol_df.rename_axis('Datetime').reset_index()
        col_name = symbol_df.columns.tolist()
        col_name.insert(0,'Symbol')
        symbol_df = symbol_df.reindex(columns=col_name)
        symbol_df['Symbol'] = stock_id
        
        return symbol_df

    def GetIncomeStatement_Original(self, stock_id : str) -> DataFrame:
        base_url = 'https://www.alphavantage.co/query?'
        df_annual = pd.DataFrame()
        df_quarterly = pd.DataFrame()
        params = {'function': 'INCOME_STATEMENT', 'symbol': stock_id, 'apikey': self.apikey}
        response = requests.get(base_url, params=params)
        data = response.json() # dict
        data_annual = data['annualReports']
        data_quarterly = data['quarterlyReports']
        
        for dict_1 in data_annual:
            df_annual = df_annual.append(pd.DataFrame([dict_1]))
        
        col_name = df_annual.columns.tolist()
        col_name.insert(0,'Symbol')
        df_annual = df_annual.reindex(columns=col_name)
        df_annual['Symbol'] = stock_id
        
        for dict_2 in data_quarterly:
            df_quarterly = df_quarterly.append(pd.DataFrame([dict_2]))
        
        col_name = df_quarterly.columns.tolist()
        col_name.insert(0,'Symbol')
        df_quarterly = df_quarterly.reindex(columns=col_name)
        df_quarterly['Symbol'] = stock_id

        return df_annual, df_quarterly

    def GetCompanyOverview_Original(self, stock_id : str) -> DataFrame:
        base_url = 'https://www.alphavantage.co/query?'
        df_new = pd.DataFrame()
        params = {'function': 'OVERVIEW', 'symbol': stock_id, 'apikey': self.apikey}
        response = requests.get(base_url, params=params)
        data = response.json() # dict

        df = pd.DataFrame([data])
        
        return df

    # Company overview combine with the IPO date information in the listing&delisting data 
    def Database_Center(self, stock_id : str) -> DataFrame:
        df_income_statement = self.GetListingDelistingStatus()
        df_company_overview = self.GetCompanyOverview_Original(stock_id)
        df_company_overview = df_company_overview.loc[:,['Symbol', 'AssetType', 'Name', 'Exchange','Country', 'Sector', 'Industry']]
        df_company_IPO_date = df_income_statement.loc[df_income_statement['symbol'] == stock_id]

        df_company_overview['IpoDate'] = str(df_company_IPO_date['ipoDate'].values[0])
        df_company_overview['DelistingDate'] = str(df_company_IPO_date['delistingDate'].values[0])
        df_company_overview.loc[(df_company_overview["DelistingDate"] == "null"), "DelistingDate"] = "1970-01-01"
        df_company_overview['Status'] = str(df_company_IPO_date['status'].values[0])

        return df_company_overview

    def CSV_Output_Original(self, stock_id : str) -> DataFrame:
        workbook = xlwt.Workbook()
        workbook.add_sheet('Daily Price')
        workbook.add_sheet('Weekly Price')
        workbook.add_sheet('Monthly Price')
        workbook.add_sheet('Intraday Price')
        workbook.add_sheet('Income Statement Annual')
        workbook.add_sheet('Income Statement Quarterly')
        workbook.add_sheet('Company Overview')
        workbook.add_sheet('Search Endpoint Results')
        workbook.add_sheet('US ListingDelisting Status')
        workbook.add_sheet('IPO Calender')
        workbook.save('Original_Data.xlsx')
        df = self.GetIncomeStatement_Original(stock_id)
        writer = pd.ExcelWriter('Original_Data.xlsx', engine='xlsxwriter')
        self.GetDailyStockPrice_Original(stock_id).to_excel(writer, sheet_name='Daily Price')
        self.GetWeeklyStockPrice(stock_id).to_excel(writer, sheet_name='Weekly Price')
        self.GetMonthlyStockPrice(stock_id).to_excel(writer, sheet_name='Monthly Price')
        self.GetIntradayStockPrice(stock_id).to_excel(writer, sheet_name='Intraday Price')
        df[0].to_excel(writer, sheet_name='Income Statement Annual')
        df[1].to_excel(writer, sheet_name='Income Statement Quarterly')
        self.GetCompanyOverview_Original(stock_id).to_excel(writer, sheet_name='Company Overview')
        self.GetSearchEndpoint(stock_id).to_excel(writer, sheet_name='Search Endpoint Results')
        self.GetListingDelistingStatus().to_excel(writer, sheet_name='US ListingDelisting Status')
        self.FindIPOCalender().to_excel(writer, sheet_name='IPO Calender')
        writer.save()
    
# 只针对当前项目，可替换
class Info_Collected(object):
    def __init__(self, apikey : str, api_port : classmethod) -> None:
        self.apikey = apikey 
        self.api_port = api_port

    def Get_Stock_Price(self, stock_id:str) -> DataFrame:
        # Stock price 
        df_daily = self.api_port.GetDailyStockPrice(stock_id)
        df_weekly = self.api_port.GetWeeklyStockPrice(stock_id)
        df_monthly = self.api_port.GetMonthlyStockPrice(stock_id)
        df_intraday = self.api_port.GetIntradayStockPrice(stock_id)

        return df_daily, df_weekly, df_monthly, df_intraday
    
    def Get_Stock_Status(self, stock_id:str) -> DataFrame:
        # listing and delisting status
        df_status = self.api_port.GetListingDelistingStatus()

        return df_status

    def Get_IPO_Calender(self) -> DataFrame:
        # IPO list in next three months
        df_ipo = self.api_port.FindIPOCalender()

        return df_ipo
    
    def Search_Endpoint_Backup(self, stock_id:str) -> DataFrame:
        # Search endpoint data backup
        df_search = self.api_port.GetSearchEndpoint(stock_id)

        return df_search
        

    def Database_Center(self, stock_id : str) -> DataFrame:
        # Company overview combine with the IPO date information in the listing&delisting data 
        df_income_statement = self.api_port.GetListingDelistingStatus()
        df_company_overview = self.api_port.GetCompanyOverview_Original(stock_id)
        df_company_overview = df_company_overview.loc[:,['Symbol', 'AssetType', 'Name', 'Exchange','Country', 'Sector', 'Industry']]
        df_company_IPO_date = df_income_statement.loc[df_income_statement['symbol'] == stock_id]

        df_company_overview['IpoDate'] = str(df_company_IPO_date['ipoDate'].values[0])
        df_company_overview['DelistingDate'] = str(df_company_IPO_date['delistingDate'].values[0])
        df_company_overview.loc[(df_company_overview["DelistingDate"] == "null"), "DelistingDate"] = "1970-01-01"
        df_company_overview['Status'] = str(df_company_IPO_date['status'].values[0])
        
        return df_company_overview

    