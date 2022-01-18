from datetime import timedelta
from os import PRIO_PGRP
from API_Data_Retrieve import *
from MySQLConnect import * 
from MySQL_Maintenance import * 
import API_Data_Retrieve as data_mysql 

apikey = ""
#host="", user="", password="", database=""

# connect to mysql database 
# if choose database that is not in your mysql database, this will automatic create a new database 
#Alpha_VantageAPI = Alpha_VantageAPI(apikey5)
#Info_Collected = Info_Collected(apikey1, Alpha_VantageAPI)

# MySQL data update
#MySQL_Connection = MySQL_Connection(apikey,host="",user="",password="",database="")
#Data_to_SQL = Data_to_SQL(MySQL_Connection,Alpha_VantageAPI,Info_Collected)
#Data_to_SQL = Data_to_SQL(MySQL_Connection)
names = ["daily", "weekly", "monthly","intraday", "company_info", "search", "listinganddelisting", "IPOCalender"]
#Data_to_SQL.create_table(names)
# df = Data_to_SQL.data_to_sql_execute("IBM")
# print(df)



# Mysql maintenance
Alpha_VantageAPI = Alpha_VantageAPI(apikey)
MySQL_Connection = MySQL_Connection(apikey,host="",user="",password="",database="")
MySQL_Maintenance = MySQL_Maintenance(Alpha_VantageAPI, MySQL_Connection, Data_to_SQL)
# MySQL_Maintenance.delete_data("daily","IBM","2021-12-10")
print(MySQL_Maintenance.database_backup("Your Database"))


# print(MySQL_Maintenance.delete_data("listinganddelisting","AA","Alcoa Corp"))


