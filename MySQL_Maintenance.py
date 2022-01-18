from time import strftime
from mysql.connector.errors import Error, custom_error_exception
from numpy.core.arrayprint import _array_repr_dispatcher
from numpy.core.numeric import full
from numpy.lib.shape_base import column_stack
from API_Data_Retrieve import *
from MySQLConnect import * 
import mysql.connector
import pandas as pd
import pymysql
import datetime 
import calendar
import os 


class MySQL_Maintenance(object):
    """
    This class is for MySQL database maintenance 
    ...

    Attributes 
    ----------
    mysql_connection : str
        build connection with database

    Methods
    ----------
    database_backup(dbname)
        Prints bakup successfully after finished 
    

    
    """
    def __init__(self, api_data : classmethod, mysql_connection : mysql, data_to_sql : classmethod) -> None:
        """
        Parameters
        ----------
        mysql_connection : mysql
            connection to mysql 
        data_to_sql : classmethod
            use classmethod         
        """
        self.api_data = api_data
        self.mysql_connection = mysql_connection
        self.data_to_sql = data_to_sql(self.mysql_connection)
        
        
        
    
    def database_backup(self, dbname : str) -> str:
        """prints database successfully backup 

        If the argument prints the statement, then 
        the database has been successfully backup.

        Parameters 
        ----------
        dbname  : str
            The database name you want to backup 
        
        Raises
        ----------
        Error
            If database cannot be backup, it will 
            raise error message
        
        """
        try:
            db_backup = os.system("/usr/local/mysql/bin/mysqldump -u -p --databases " + dbname + " > your directory"+ dbname + "_bkp_" + str(datetime.datetime.now().date()) + ".sql")
            print(db_backup)
            print("database has successfully been backup to your directory")
        except Error as e:
            print(e)
    
    def database_check(self, dbname : str) -> str:
        """prints database check resuls

        The results will show each table check results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check 
        """
        mysql_check = os.system("/usr/local/mysql/bin/mysqlcheck -u -p --databases " + dbname)
        print("Check Table Results : ")
        print(mysql_check)

    def database_optimize(self, dbname : str) -> str:
        """prints database optimize resuls

        The results will show each table optimize results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check 

        
        MySQL5.7 version perfer using alter table table_name engine=innodb to do mysql dump
        """
        db_optimize = "/usr/local/mysql/bin/mysqlcheck --optimize -u -p --databases " + dbname
        print("MySQL optimize results : successfully ")
        #print(db_optimize)

    def database_analyze(self, dbname : str) -> str:
        """prints database analyze resuls

        The results will show each table analyze results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check
        """
        db_analyze = "/usr/local/mysql/bin/mysqlcheck --analyze -u -p --databases " + dbname
        print("MySQL analyze results : ")
        print(db_analyze)
    
    def delete_data(self, table_name : str, symbol_name : str, val : str) -> str:
        """prints database analyze resuls

        The results will show each table analyze results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check
        """
        try:
            connection = self.mysql_connection.connect_database()
            cursor = connection.cursor()
            # """SELECT * FROM `users` WHERE `username` '{}' AND `password` '{}'""".format(username,password))
            # delete from daily where symbol="IBM" and datetime="2021-12-14" and id > 0;
            #sql = "DELETE FROM {}" + table_name + " WHERE symbol={}" + symbol_name + " and datetime={}" + date_val + " and id > 0;"
            stock_price_list = ["daily", "weekly", "monthly","intraday"]
            if table_name in stock_price_list:
                sql = """DELETE FROM {} WHERE symbol='{}' and datetime='{}' and id > 0;""".format(table_name,symbol_name, val)
            #["", "", , "IPOCalender"]
            elif table_name == "company_info" or table_name == "search":
                sql = """DELETE FROM {} WHERE Symbol='{}' and Name='{}';""".format(table_name,symbol_name, val)
            elif table_name == "listinganddelisting":
                sql = """DELETE FROM {} WHERE symbol='{}' and name='{}';""".format(table_name,symbol_name, val)
            else:
                print("Table name does not exist ! ")
            cursor.execute(sql) 
            connection.commit()
            connection.close()
            print("Error data has already been deleted from " + table_name)
        except Error as e:
            raise e
    
    def update_data(self, data : DataFrame, table_name : str, symbol : str) -> str:
        try:
            connection = self.mysql_connection.connect_database()
            cursor = connection.cursor()
            lst1 = ["daily"]
            lst = ["weekly", "monthly"]
            if table_name in lst1:
                for index,row in data.iterrows():  
                    row.datetime = row.datetime.strftime("%Y-%m-%d")
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, volume)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")

            elif table_name in lst:
                for index,row in data.iterrows():  
                    row.datetime = row.datetime.strftime("%Y-%m-%d")
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, adjusted_close, volume, dividend_amt)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")

            elif table_name == "intraday":
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + "(symbol, datetime, open, high, low, close, volume)" + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(symbol+ " data has already been updated into the " + table_name + " table! ")

            # use Info_collected classmethod in the API file
            elif table_name == "company_info":
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")
                
            elif table_name == "search":
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(symbol + " data has already been updated into the " + table_name + " table! ")
                
            elif table_name == "listinganddelisting":
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print(table_name + " data has already been updated into the " + table_name + " table! ")

            elif table_name == "IPOCalender":
                for index,row in data.iterrows():
                    sql = "INSERT INTO " + table_name + " VALUES" + str(tuple(row))
                    cursor.execute(sql)
                    connection.commit()
                print("IPO data has already been updated into the " + table_name + " table! ")

            connection.close()
        except Error as e:
             raise e
    def api_data_reconstruction(self, table_name : str, api_data : DataFrame, mysql_data : DataFrame, symbol : str) -> DataFrame:
        """return new api data DataFrame 

        The results will show only weekly or monthly data
        by excluding other dates 

        Parameters 
        ----------
        table_name : str
            Choose table name 
        api_data  : DataFrame
            Must be weekly or monthly stock price data 
        mysql_data : DataFrame
            Must be weekly or monthly stock price data 
        symbol : str 
            Choose the stock name 
        """
        try:
            #api_data["datetime"] = pd.to_datetime(api_data["datetime"].astype(str).str.strip(), format='%Y-%m-%d')
            #api_data["datetime"] = api_data["datetime"].dt.strftime("%Y-%#m-%d")
            date_val = api_data["datetime"]
        
            # delete all data that are not at Friday date 
            if table_name == "weekly":
                for date_data in date_val:
                    if date_data.weekday() not in range(2,5):
                        index = api_data[api_data["datetime"] == date_data].index
                        api_data = api_data.drop(index)

                api_data = api_data.sort_values(by = ["datetime"], ascending = False)
                api_data = api_data.reset_index(drop=True)
                return api_data
            elif table_name == "monthly":
                for date_data in date_val:
                    # last_day_of_date = str(date_data.year) + "-" + '{:02}'.format(date_data.month) + "-" + str()
                    # date_data = str(date_data.year) + "-" + '{:02}'.format(date_data.month) + "-" + str(date_data.day)
                    last_day = calendar.monthrange(date_data.year, date_data.month)[1]
                    if date_data.day not in range(last_day - 4, last_day + 1):
                        index = api_data[api_data["datetime"] == date_data].index
                        api_data.drop(index, inplace=True)
                
                api_data = api_data.sort_values(by = ["datetime"], ascending = False)
                api_data = api_data.reset_index(drop=True)
                return api_data
            else:
                print("Table name not exists, please check table name ! ")
            
        except Error as e:
            raise e 
            
    def stock_maintenance(self, table_name : str) -> str:
        """prints stock data maintenance results

        The results will show each stock table maintenance results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check
        """
        #["daily", "weekly", "monthly", "intraday"]
        t1 = time.time()
        try:
            df_origin = self.data_to_sql.find_original_data(table_name)
            if len(df_origin) == len(df_origin.drop_duplicates(subset=["symbol", "datetime"])):
                
                df_symbol = df_origin["symbol"].drop_duplicates()
                symbol_list = list(df_symbol)

                for name in symbol_list:
                    # mysql data
                    df_symbol = df_origin[df_origin["symbol"] == name]
                    df_symbol = df_symbol.sort_values(by = ["datetime"], ascending = False)
                    df_symbol = df_symbol.reset_index(drop=True)

                    # api data 
                    if table_name == "daily":
                        df_api = self.api_data.GetDailyStockPrice(str(name))
                    elif table_name == "weekly":
                        df_api = self.api_data.GetWeeklyStockPrice(str(name))
                        #df_api = self.api_data_reconstruction("weekly", df_api, name)
                    elif table_name == "monthly":
                        df_api = self.api_data.GetMonthlyStockPrice(str(name))
                        #df_api = self.api_data_reconstruction("monthly", df_api, name)
                    elif table_name == "intraday":
                        df_api = self.api_data.GetIntradayStockPrice(str(name))
                    elif table_name == "company_info":
                        df_api = self.api_data.GetDailyStockPrice_Original(str(name))
                    elif table_name == "listinganddelisting":
                        df_api = self.api_data.GetListingDelistingStatus()

                    # compare with api data with mysql database table data 
                    df_check = df_symbol.equals(df_api)
                    if df_check == False:   
                        # find row needs to be changed
                        df_all = pd.concat([df_symbol, df_api]).drop_duplicates()
                        df_name_price_unique = pd.concat([df_all, df_api]).drop_duplicates(keep = False)
                        df_api_unique = pd.concat([df_all, df_symbol]).drop_duplicates(keep = False)
                        
                        #print(df_name_price_unique)
                        #print(df_api_unique)
                        
                        # delete original value and update with new one
                        check = 0
                        if df_name_price_unique.isnull().values.any() == False:
                            for datetime_val in df_name_price_unique["datetime"]:
                                self.delete_data(table_name, name, datetime_val)
                                check = check + 1
                        if df_api_unique.isnull().values.any() == False:
                                self.update_data(df_api_unique,table_name,name)
                                check = check + 1
                        # check whether updating or not
                        if check:
                            print(name + " data in " + table_name + " has been updated ! ")
                        else:
                            print(name + " data in " + table_name + " have updating problems !")

                    else:
                        print(name + " stock data accurate ! ")
                
                print(table_name + " stock data maintenance complete ! ")
                                    
            else:
                print("There are some duplicate values in the original " + table_name + " data")
                print("Please check data manually ! ")

        except Error as e:
            raise e
        t2 = time.time()
        print("time cost : ", t2-t1)

        
        #print("MySQL analyze results : ")
    def company_info_maintenance(self, table_name : str) -> str:
        """prints company_info data maintenance results

        The results will show company_info maintenance results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check
        """
        #["daily", "weekly", "monthly", "intraday"]
        t1 = time.time()
        try:
            df_origin = self.data_to_sql.find_original_data(table_name)
            # print(df_origin)
            if len(df_origin) == len(df_origin.drop_duplicates()):
                
                df_symbol = df_origin["Symbol"].drop_duplicates()
                symbol_list = list(df_symbol)

                for name in symbol_list:
                    # mysql data
                    df_symbol = df_origin[df_origin["Symbol"] == name]
                    df_symbol = df_symbol.reset_index(drop=True)

                    # api data 
                    if table_name == "company_info":
                        df_api = self.api_data.Database_Center(str(name))
                        # df_api_bkp = self.api_data.Database_Center(str(name))
                        df_api["IpoDate"] = pd.to_datetime(df_api["IpoDate"].astype(str).str.strip(), format='%Y-%m-%d')
                        df_api["DelistingDate"] = pd.to_datetime(df_api["DelistingDate"].astype(str).str.strip(), format='%Y-%m-%d') 

                    # print(df_api.dtypes)
                    # print(df_symbol.dtypes)

                    # compare with api data with mysql database table data 

                    df_check = df_symbol.equals(df_api)
                    if df_check == False:   
                        # find row needs to be changed
                        df_all = pd.concat([df_symbol, df_api]).drop_duplicates()
                        df_name_price_unique = pd.concat([df_all, df_api]).drop_duplicates(keep = False)
                        df_api_unique = pd.concat([df_all, df_symbol]).drop_duplicates(keep = False)
                        
                        # delete original value and update with new one
                        check = 0
                        if df_name_price_unique.isnull().values.any() == False:
                            for full_name in df_name_price_unique["Name"]:
                                self.delete_data(table_name, name, full_name)
                                check = check + 1
                        if df_api_unique.isnull().values.any() == False:
                                self.update_data(df_api_unique,table_name,name)
                                check = check + 1
                        # check whether updating or not
                        if check:
                            print(name + " data in " + table_name + " has been updated ! ")
                        else:
                            print(name + " data in " + table_name + " have updating problems !")

                    else:
                        print(name + " stock data accurate ! ")
                
                print(table_name + " stock data maintenance complete ! ")
                                    
            else:
                print("There are some duplicate values in the original " + table_name + " data")
                print("Please check data manually ! ")

        except Error as e:
            raise e
        t2 = time.time()
        print("time cost : ", t2-t1)

    def listinganddelisting_maintenance(self, table_name : str) -> str:
        """prints listinganddelisting data maintenance results

        The results will show listinganddelisting maintenance results 

        Parameters 
        ----------
        dbname  : str
            The database name you want to check
        """
        #["daily", "weekly", "monthly", "intraday"]
        t1 = time.time()
        try:
            df_origin = self.data_to_sql.find_original_data(table_name)
            df_origin = df_origin.reset_index(drop=True)
            # print(df_origin)
            if len(df_origin) == len(df_origin.drop_duplicates()):
                # api data  
                if table_name == "listinganddelisting":
                    df_api = self.api_data.GetListingDelistingStatus()
                    df_api = df_api.reset_index(drop=True)
                    df_api["ipoDate"] = pd.to_datetime(df_api["ipoDate"].astype(str).str.strip(), format='%Y-%m-%d')
                    df_api["delistingDate"] = pd.to_datetime(df_api["delistingDate"].astype(str).str.strip(), format='%Y-%m-%d')
                
                #print(df_origin)
                #print(df_api)
                
                df_check = df_origin.equals(df_api)
                if df_check == False:   
                    # find row needs to be changed
                    df_all = pd.concat([df_origin, df_api]).drop_duplicates()
                    df_sql_unique = pd.concat([df_all, df_api]).drop_duplicates(keep = False)
                    df_api_unique = pd.concat([df_all, df_origin]).drop_duplicates(keep = False)
                
                    # delete original value and update with new one
                    check = 0
                    if df_sql_unique.isnull().values.any() == False:
                        for full_name in df_sql_unique["name"]:
                            name = df_origin["symbol"][df_origin["name"] == full_name]
                            self.delete_data(table_name, name.values[0], full_name)
                            check = check + 1
                    if df_api_unique.isnull().values.any() == False:
                            self.update_data(df_api_unique,table_name,"")
                            check = check + 1
                    # check whether updating or not
                    if check:
                        print(table_name + " has been updated ! ")
                    else:
                        print(table_name + " have updating problems !")
                       
                else:
                    print(table_name+ " data accurate ! ")
                
                print(table_name + " stock data maintenance complete ! ")
                                    
            else:
                print("There are some duplicate values in the original " + table_name + " data")
                print("Please check data manually ! ")

        except Error as e:
            raise e
        t2 = time.time()
        print("time cost : ", t2-t1)

    def data_restore_table(self, dbname : str, table_name : str) -> str:
        
        pass


if __name__ == "__main__": 

    # use for double check for specific problem 
    # if df_name_price["symbol"].equals(df_api["symbol"]):
    #     print("symbol")
    # if df_name_price["datetime"].equals(df_api["datetime"]):
    #     print("datetime")
    # if df_name_price["open"].equals(df_api["open"]):
    #     print("open")
    # if df_name_price["high"].equals(df_api["high"]):
    #     print("high")
    # if df_name_price["low"].equals(df_api["low"]):
    #     print("low")
    # if df_name_price["close"].equals(df_api["close"]):
    #     print("close")
    # if df_name_price["volume"].equals(df_api["volume"]):
    #     print("volume")
    pass
