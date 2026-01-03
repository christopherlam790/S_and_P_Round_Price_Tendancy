"""
PREP CLEAN DATA
"""


import yfinance as yf
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime



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
