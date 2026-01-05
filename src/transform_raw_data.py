"""
RAW DATA -> CLEAN DATA
"""


import yfinance as yf
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime

from download_raw_data import get_raw_df_from_sql



"""
Add day of the week to each entry
@return df
"""
def add_day_of_week_col(df):

    def get_weekday_from_yyyymmdd(date_string):
        date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S%z')
        weekday_name = date_object.strftime('%A')
        
        return weekday_name
    
    
    weekday = []
    for date in df.index:
        weekday.append(get_weekday_from_yyyymmdd(str(date)))
        
    df["Weekday"] = weekday
    
    return df

"""
Round to 2 decimal places via standard rounding
"""
def add_true_rounded_close(df):
    
    df["true_rounded_close"] = df["close"].round(2)
    
    return df

"""
Round to nearest dollar via standard rounding
"""
def add_true_dollar_rounded_close(df):
    df["true_dollar_rounded_close"] = df["close"].round(0)
    
    return df

"""
Floored value of close
"""
def add_floor_rounded_close(df):
    df["floor_rounded_close"] = df["close"].apply(np.floor)
    
    return df

"""
Ceiling value of close
"""
def add_ceil_rounded_close(df):
    df["ceil_rounded_close"] = df["close"].apply(np.ceil)
    
    return df 
    
"""
Check if dollar rounded is true multiple of 5
"""
def add_is_dollar_rounded_true_multiple_of_5(df):
    
    df["is_dollar_rounded_true_multiple_of_5"] = [1 if close % 5 == 0 else 0 for close in df["true_dollar_rounded_close"]]
    
    return df

"""
Check if dollar rounded is true multiple of 2.5
"""
def add_is_dollar_rounded_true_multiple_of_2_point_5(df):
    
    df["is_dollar_rounded_true_multiple_of_2_point_5"] = [1 if close % 2.5 == 0 else 0 for close in df["true_dollar_rounded_close"]]
    
    return df


def add_is_dollar_rounded_tolerance_1_multiple_of_5(df):
    
    df["is_dollar_rounded_tolerance_1_multiple_of_5"] = [1 if close % 5 in [0, 1, 4] else 0 for close in df["true_dollar_rounded_close"]]
    
    return df



def transform_raw_date(ticker):
    df = get_raw_df_from_sql(ticker=ticker) 
    df = add_day_of_week_col(df=df)
    
    df = add_true_rounded_close(df=df)
    df = add_true_dollar_rounded_close(df=df)
    df = add_floor_rounded_close(df=df)
    df = add_ceil_rounded_close(df=df)
    
    df = add_is_dollar_rounded_true_multiple_of_5(df=df)
    df = add_is_dollar_rounded_true_multiple_of_2_point_5(df=df)
    df = add_is_dollar_rounded_tolerance_1_multiple_of_5(df=df)
    
    print(df[["close","true_dollar_rounded_close", "is_dollar_rounded_tolerance_1_multiple_of_5"]]) 
    


"""
=======
TESTING AREA
"""
if __name__ == "__main__":
    
    
    transform_raw_date("^SPX")
    print("TESTING COMPLETE")