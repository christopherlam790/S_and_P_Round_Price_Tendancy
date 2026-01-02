import yfinance as yf
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime


"""
Return Data from ticker
"""
def get_data(ticker, period="max", interval="1d", start="2007-01-01"):
    asset = yf.Ticker(ticker=ticker)
    
    return asset.history(period=period, interval=interval, start=start)

"""
Clean data of unnecassary cols
"""  
def clean_data(df):
    
    df.drop(columns=["Dividends", "Stock Splits"],inplace=True,errors="ignore")
    
    return True

"""
Add day of the week to each entry
"""
def add_day_of_week_col(df):
    
    def get_weekday_from_yyyymmdd(date_string):
        date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S%z')
        weekday_name = date_object.strftime('%A')
        
        return weekday_name
    
    for date in df.index:
        print(date)
        
    
    return True


"""
Validate data has all appropriet cols
"""
def validate_clean_data():
    return
    
    

    
    
def download_data_as_postgressql(df, ticker):
    
    engine = create_engine(
        f"postgresql+psycopg2://{os.environ['PG_USER']}:"
        f"{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:"
        f"{os.environ['PG_PORT']}/"
        f"{os.environ['PG_DB']}",
        pool_pre_ping=True
    )

    # df.to_sql(f'{ticker}_table', engine)
   
    return True

def verify_download():
    
    return True

"""
==========================================
Testing Section
"""
if __name__ == "__main__":
    
    df = get_data("^SPX")
    clean_data(df)
    add_day_of_week_col(df)
    
    


    
    

    