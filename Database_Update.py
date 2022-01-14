from API_Data_Retrieve import *
from MySQLConnect import * 
apikey = ""
class Insert_Data(object):
    def __init__(self, API_Port : classmethod, Data_SQL : classmethod) -> None:
        """                    Frequency
        stock daily price    : update everyday (Mon - Fri)
        stock weeky price    : update every week (Sun)
        stock monthly price  : update at the end of the week day
        stock intraday price : update everyday (Mon - Fri)

        company overview     : update when inserting new symbols
        search               : update when stock price updated
        listing and delisting status : update everyday (Mon - Fri)
        IPO calender         : update everyday (Mon - Fri)
        """
        self.API_Port = API_Port
        self.Data_SQL = Data_SQL

    # Stock price 
    def insert_daily_price(self, stock_id:str) -> DataFrame:
        df_daily = self.API_Port.GetDailyStockPrice(stock_id)
        daily = self.Data_SQL.insert_data(df_daily, "daily", stock_id)
        return daily

    def insert_weekly_price(self, stock_id:str) -> DataFrame:
        df_weekly = self.API_Port.GetWeeklyStockPrice(stock_id)
        weekly = self.Data_SQL.insert_data(df_weekly, "weekly", stock_id)
        return weekly

    def insert_monthly_price(self, stock_id:str) -> DataFrame:
        df_monthly = self.API_Port.GetMonthlyStockPrice(stock_id)
        monthly = self.Data_SQL.insert_data(df_monthly, "monthly", stock_id)
        return monthly

    def insert_intraday_price(self, stock_id:str) -> DataFrame:
        df_intraday = self.API_Port.GetIntradayStockPrice(stock_id)
        intraday = self.Data_SQL.insert_data(df_intraday, "intraday", stock_id)
        return intraday

    # listing and delisting status
    def insert_stock_listingdelistingstatus(self) -> DataFrame:
        df_status = self.API_Port.GetListingDelistingStatus()
        df_unique_api = self.Data_SQL.find_unique_api_listinganddelisting(df_status)
        listinganddelisting = self.Data_SQL.insert_data(df_unique_api, "listinganddelisting", "")
        return listinganddelisting
        
    # IPO list in next three months
    def insert_ipo_calender(self) -> DataFrame:
        df_ipo = self.API_Port.FindIPOCalender()
        ipo = self.Data_SQL.insert_data(df_ipo, "IPOCalender", "")
        return ipo

    # Search endpoint data backup
    def insert_search_endpoint(self, stock_id:str) -> DataFrame:
        df_search = self.API_Port.GetSearchEndpoint(stock_id)
        search = self.Data_SQL.insert_data(df_search, 'search', stock_id)
        return search
        
    def insert_company_info(self, stock_id : str) -> DataFrame:
        df_company_info = self.API_Port.Database_Center(stock_id)
        info = Data_to_SQL.insert_data(df_company_info, "company_info", stock_id)
        return info 

if __name__ == "__main__": 
    Alpha_VantageAPI = Alpha_VantageAPI(apikey)
    MySQL_Connection = MySQL_Connection(apikey,'localhost','root','Dhy9904191asd@@','FinanceDataReal')
    Data_to_SQL = Data_to_SQL(MySQL_Connection)
    Insert_Data = Insert_Data(Alpha_VantageAPI,Data_to_SQL)
    names = ["daily", "weekly", "monthly","intraday", "company_info", "search", "listinganddelisting", "IPOCalender"]
    #print(Insert_Data.insert_stock_listingdelistingstatus())
    #print(Alpha_VantageAPI.GetListingDelistingStatus())
    df = Data_to_SQL.find_original_data("listinganddelisting")
    name = df["symbol"][df["name"] == "Agilent Technologies Inc"]
    print(type(name.values[0]))

   

