from typing import DefaultDict
from alpha_vantage.timeseries import TimeSeries
from mysql.connector.errors import Error
from numpy.lib.arraysetops import _unique1d
from pandas.core import api
from pandas.core.algorithms import unique
from pandas.core.arrays import boolean
from pandas.core.frame import DataFrame
from pandas.io.formats.format import Datetime64Formatter
from pandas.io.pytables import duplicate_doc
from sqlalchemy.exc import ObjectNotExecutableError 
from API_Construct import Alpha_VantageAPI
from API_Construct import Info_Collected 
from datetime import date, datetime, timedelta
import mysql.connector
import pandas as pd
import calendar
import datetime
import pymysql
import time 
import os 

class MySQL_Connection(object):
    def __init__(self, apikey : str, host : str, user : str, password :str, database : str) -> None:
        self.apikey = apikey
        self.host = host
        self.user = user 
        self.password = password
        self.database = database
    
    # Connect to the database
    def connect_database(self) -> mysql:
        try:
            db = mysql.connector.connect(host = self.host, user = self.user, password = self.password)
            sql = 'USE ' + self.database
            cursor = db.cursor()
            cursor.execute(sql)
            print(self.database + ' database has already been connected! ')
            return db 
        except:
            db = mysql.connector.connect(host = self.host, user = self.user, password = self.password)
            sql = 'CREATE DATABASE ' + self.database
            cursor = db.cursor()
            cursor.execute(sql)
            print(self.database + ' database has already been successfully added in to MySQL! ')
            return db 
    
# 数据操作层面
class Data_to_SQL(object):
    # Call mysql connection and API here 
    def __init__(self, mysql_connection : classmethod) -> None:
        self.mysql_connection = mysql_connection.connect_database()
        #self.API_port = API_port(apikey1)
        #self.Info = Info(apikey1, self.API_port)

    # define table datatypes 
    def table_datatypes(self, table_name : str):
        if table_name == 'daily':
            table_datatype = '(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(255) NOT NULL, datetime DATE, open DECIMAL(10,5), high DECIMAL(10,5), low DECIMAL(10,5), close DECIMAL(10,5), volume int(11))'
        elif table_name == 'weekly':
            table_datatype = '(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(255) NOT NULL, datetime DATE, open DECIMAL(10,5), high DECIMAL(10,5), low DECIMAL(10,5), close DECIMAL(10,5), adjusted_close DECIMAL(10,5), volume int(11), dividend_amt DECIMAL(10,5))'
        elif table_name == 'monthly':
            table_datatype = '(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(255) NOT NULL, datetime DATE, open DECIMAL(10,5), high DECIMAL(10,5), low DECIMAL(10,5), close DECIMAL(10,5), adjusted_close DECIMAL(10,5), volume int(11), dividend_amt DECIMAL(10,5))'
        elif table_name == 'intraday':
            table_datatype = '(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(255) NOT NULL, datetime DATETIME, open DECIMAL(10,5), high DECIMAL(10,5), low DECIMAL(10,5), close DECIMAL(10,5), volume INT(11))'
        elif table_name == 'company_info':
            table_datatype = '(Symbol VARCHAR(225) NOT NULL, AssetType VARCHAR(225), Name VARCHAR(225), Exchange VARCHAR(225), Country VARCHAR(225), Sector VARCHAR(225), Industry VARCHAR(225), IpoDate DATE, DelistingDate DATE NULL, Status VARCHAR(225), PRIMARY KEY (symbol))'   
        elif table_name == 'search':
            table_datatype = '(Symbol VARCHAR(225) NOT NULL PRIMARY KEY, Name VARCHAR(225), Type VARCHAR(225), Region VARCHAR(225), MarketOpen TIME, MarketClose TIME, Timezone VARCHAR(225), Currency VARCHAR(225), MatchScore DECIMAL(7,5))' 
        elif table_name == 'listinganddelisting':
            table_datatype = '(symbol VARCHAR(225) NOT NULL, name VARCHAR(225), exchange VARCHAR(225), assetType VARCHAR(225), ipoDate DATE, delistingDate DATE NULL DEFAULT NULL, status VARCHAR(225), PRIMARY KEY (symbol))'  
        elif table_name == 'IPOCalender':
            table_datatype = '(symbol VARCHAR(225) NOT NULL PRIMARY KEY, name VARCHAR(225), ipoDate DATE, priceRangeLow DECIMAL(10,4), priceRangeHigh DECIMAL(10,4), currency VARCHAR(225), exchange VARCHAR(225))'
        else:
            print('No data support! ')
        
        return table_datatype
    
    def create_table(self, table_name : list) -> str:
        cursor = self.mysql_connection.cursor()
        for name in table_name:
            try:
                order = self.table_datatypes(name)
                sql = 'CREATE TABLE ' + name + ' '+ order
                cursor.execute(sql)
                self.mysql_connection.commit()
                print(name + ' table has already been successfully added in to MySQL! ')  
            except Error as e:
                print(e)
        self.mysql_connection.close()  
    
    # small function to get time data
    def splitter(self, time_data : timedelta) -> str:
        time_data = str(time_data).split(' ')[-1:][0]
        return time_data

    def find_original_data(self, db_table : str) -> DataFrame:
        """return database tables dataframe

        Fetch all column names with their relative data and 
        transform them into types of dataframe.

        Parameters
        -----------
        db_table : str
            Choose the table you want to make to the dataframe 

        """
        try:
            cursor = self.mysql_connection.cursor()
            sql = "SELECT * FROM " + db_table
            cursor.execute(sql)

            name = []
            for col in cursor.description:
                name.append(col[0])

            data = cursor.fetchall()
            data_list = [list(i) for i in data]

            df = pd.DataFrame(data=data_list, columns=name)
            
            # format dataframe types 
            # change data type of mysql dataframe to the same type of API data
            # stock price format
            lst = ["daily", "weekly", "monthly", "intraday"]
            if db_table in lst:
                df = df.sort_values(by="datetime", ascending=False)          
                df = df.drop(columns=['id'])
                df = df.round(2)
                df["open"] = df["open"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                df["close"] = df["close"].astype(float)
                df["datetime"] = pd.to_datetime(df["datetime"].astype(str).str.strip(), format='%Y-%m-%d')

                if db_table == "weekly" or db_table == "monthly":
                    df["adjusted_close"] = df["adjusted_close"].astype(float)
                    df["dividend_amt"] = df["dividend_amt"].astype(float)
                
                df = df.sort_values(by = ["datetime"], ascending = False)
                df = df.reset_index(drop=True)

            elif db_table == "search":
                    df["MarketOpen"] = df["MarketOpen"].apply(self.splitter)
                    df["MarketOpen"] = pd.to_datetime(df["MarketOpen"],format='%H:%M:%S')
                    df["MarketOpen"] =df["MarketOpen"].dt.strftime('%H:%M').astype(object)
                    df["MarketClose"] = df["MarketClose"].apply(self.splitter)
                    df["MarketClose"] = pd.to_datetime(df["MarketClose"],format='%H:%M:%S')
                    df["MarketClose"] =df["MarketClose"].dt.strftime('%H:%M').astype(object)  
            elif db_table == "listinganddelisting":  
                    df["ipoDate"] = pd.to_datetime(df["ipoDate"].astype(str).str.strip(), format='%Y-%m-%d')
                    df["delistingDate"] = pd.to_datetime(df["delistingDate"].astype(str).str.strip(), format='%Y-%m-%d')   
            elif db_table == "company_info":
                    df["IpoDate"] = pd.to_datetime(df["IpoDate"].astype(str).str.strip(), format='%Y-%m-%d')
                    df["DelistingDate"] = pd.to_datetime(df["DelistingDate"].astype(str).str.strip(), format='%Y-%m-%d')   


            #self.mysql_connection.close()
            return df 
        except Error as e:
            print(e)
            

    def find_max_date(self, table_name : str, symbol : str) -> str:
        """return max datetime and sock symbol

        Go through mysql database and choose the table we want
        to insert as well as find the max date. 

        Parameters
        -----------
        table_name : str 
            Choose Table we want to insert data in
        symbol : str 
            Choose the stock name 
        """
        price = ["daily", "weekly", "monthly","intraday"]
        info = ["company_info", "search", "listinganddelisting", "IPOCalender"]
        
        if table_name in price:
            df_database_original = self.find_original_data(table_name)
            df_symbol_date = df_database_original["datetime"][df_database_original["symbol"] == symbol]
            df_max_date = df_symbol_date.max()
            return df_max_date

        else:
            print("table cannot be found ! ")

    def check_symbol_exists(self, table_name : str, symbol : str) -> boolean:
        """return boolean value of whether symbol name exists

        Find all symbol name in the database tables and check
        if the symbol has already exists in the tables.

        Parameters
        -----------
        table_name : str 
            Choose Table we want to insert data in
        symbol : str 
            Choose the stock name 
        
        Raises
        ----------
        Error
            If table name not exists, it will 
            raise error message
        """
        try:
            df_table = self.find_original_data(table_name)
            
            if table_name == "company_info" or table_name == "search":
                df_new_table = df_table["Symbol"].drop_duplicates()
            else:
                df_new_table = df_table["symbol"].drop_duplicates()

            symbol_list = list(df_new_table)
            if symbol in symbol_list:
                return True
            else:
                return False
        except Error as e:
            raise e 

    def find_unique_search(self, API_data : DataFrame) -> DataFrame:
        """return all search symbols that are not in the database 

        Use check_symbol function to determine all unique symbols.

        Parameters
        -----------
        API_data : DataFrame
            Search data from API port with a DataFrame format 
        """
        try: 
            # duplicate_data used for double checking
            list_search = API_data["Symbol"].to_list()
            unique_data = pd.DataFrame()
            duplicate_data = pd.DataFrame()
            
            for name in list_search:
                symbol_check = self.check_symbol_exists("search", name)
                if symbol_check:
                    duplicate_data = duplicate_data.append(API_data[API_data["Symbol"] == name])
                else:
                     unique_data = unique_data.append(API_data[API_data["Symbol"] == name])

            return unique_data

        except Error as e:
            raise e

    def find_unique_api_listinganddelisting(self, API_data : DataFrame) -> DataFrame:
        """return all symbols that are not in the database 

        Exclude all intersection data and database data to 
        get the unique data from API. 

        Parameters
        -----------
        API_data : DataFrame
            Data from API port with a DataFrame format 
        """
        try:
            df_mysql_data = self.find_original_data("listinganddelisting")
            if len(df_mysql_data.drop_duplicates()) == len(df_mysql_data):

                # get all data from API and mysql data and change type of date value
                API_data["ipoDate"] = pd.to_datetime(API_data["ipoDate"].astype(str).str.strip(), format='%Y-%m-%d')
                API_data["delistingDate"] = pd.to_datetime(API_data["delistingDate"].astype(str).str.strip(), format='%Y-%m-%d')
                all_df = pd.concat([df_mysql_data,API_data])
                all_df = all_df.drop_duplicates()

                # find unique data from API 
                unique_api = pd.concat([all_df, df_mysql_data]).drop_duplicates(keep=False)
                unique_sql = pd.concat([all_df, API_data]).drop_duplicates(keep=False)

                # delete original data 
                cursor = self.mysql_connection.cursor()
                for index,row in unique_sql.iterrows(): 
                    sql = """DELETE FROM {} WHERE symbol='{}' and name='{}';""".format("listinganddelisting", row.symbol, row["name"])
                    cursor.execute(sql) 
                    self.mysql_connection.commit()

                return unique_api
            else:
                print("There is some duplicate values in listinganddelisting data, please do manually checking ! ")
        except Error as e:
            print(e)

    def check_symbol_IPO_exists(self, IPO_data : DataFrame) -> DataFrame:
        """return updated symbol names with DataFrame type

        Check whether API IPO calender data has already 
        in the database IPO table and make a symbol list
        that has symbol names not in the database.

        Parameters
        -----------
        table_name : str 
            Choose Table we want to insert data in
        symbol : str 
            Choose the stock name 
        
        Raises
        ----------
        Error
            If table name not exists, it will 
            raise error message or any other 
            error message
        """
        try:
            df_new_table = IPO_data["symbol"].drop_duplicates()
            symbol_list = list(df_new_table)
            df_exists = pd.DataFrame()
            for symbol in symbol_list:
                if self.check_symbol_exists('IPOCalender',symbol) == False:
                    df_exists = df_exists.append(IPO_data[IPO_data["symbol"] == symbol])
            return df_exists
        except Error as e:
            raise e 

    # insert data into the database tables
    def insert_data(self, data : DataFrame, table_name : str, symbol : str) -> str:
        try:
            cursor = self.mysql_connection.cursor()
            lst1 = ["daily"]
            lst = ["weekly", "monthly"]
            if table_name in lst1:
                # 检查表单里是否有公司股价，如果有则赋予data值为最新的股价数据
                if self.check_symbol_exists(table_name, symbol):
                    max_date = self.find_max_date(table_name, symbol)
                    data = data[data.datetime > str(max_date)]
                    #data = data[data.Datetime == "2021-12-14"]
                for index,row in data.iterrows():  
                    row.datetime = row.datetime.strftime("%Y-%m-%d")
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, volume)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    self.mysql_connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")
            elif table_name in lst:
                # 检查表单里是否有公司股价，如果有则赋予data值为最新的股价数据
                if self.check_symbol_exists(table_name, symbol):
                    max_date = self.find_max_date(table_name, symbol)
                    data = data[data.datetime > str(max_date)]
                for index,row in data.iterrows():  
                    row.datetime = row.datetime.strftime("%Y-%m-%d")
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, adjusted_close, volume, dividend_amt)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    self.mysql_connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")
            elif table_name == "intraday":
                # 检查表单里是否有公司股价，如果有则赋予data值为最新的股价数据
                if self.check_symbol_exists(table_name, symbol):
                    max_date = self.find_max_date(table_name, symbol)
                    data = data[data.datetime > str(max_date)]
                for index,row in data.iterrows():
                    #row.Datetime = row.Datetime.strftime("%Y-%m-%d")
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, volume)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    self.mysql_connection.commit()
                print(symbol+ " data has already been updated into the " + table_name + " table! ")
            # use Info_collected classmethod in the API file
            elif table_name == "company_info":
                # 公司在表单里不存在的时候加入
                if self.check_symbol_exists(table_name, symbol) == False:
                    for index,row in data.iterrows():
                        sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                        cursor.execute(sql)
                        self.mysql_connection.commit()
                    print(symbol + " data has already been updated into the " + table_name + " table! ")
                else:
                    print(symbol + " data has already exsits ! ")
            elif table_name == "search":
                # 查询数据备份
                df_unique = self.find_unique_search(data)
                if len(df_unique):
                    for index,row in data.iterrows():
                        sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                        cursor.execute(sql)
                        self.mysql_connection.commit()
                    print(symbol + " data has already been updated into the " + table_name + " table! ")
                else:
                    print(symbol + " data has already exsits ! ")
            elif table_name == "listinganddelisting":
                # check delisting stock 
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    self.mysql_connection.commit()
                print(table_name + " data has already been updated into the " + table_name + " table! ")
            elif table_name == "IPOCalender":
                # check IPO stocks in next three months
                new_data = self.check_symbol_IPO_exists(data)
                if len(new_data):
                    for index,row in new_data.iterrows():
                        sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                        cursor.execute(sql)
                        self.mysql_connection.commit()
                    print("IPO data has already been updated into the " + table_name + " table! ")
                else:
                    print("IPO data has already exsits ! ")
            self.mysql_connection.close()
        except Error as e:
             print(e)

    # test

    
   
        
   

        
        

        

        

        