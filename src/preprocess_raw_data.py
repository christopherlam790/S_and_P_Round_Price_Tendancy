"""
EXTRACT RAW DATA -> MAKE CLEAN DATA
"""
import yfinance as yf
import numpy as np
import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
)

"""
Return Data from ticker
@return df
"""
def get_data(ticker, period="max", interval="1d", start="2007-01-01"):
    asset = yf.Ticker(ticker=ticker)
    
    return asset.history(period=period, interval=interval, start=start)


"""
Clean data of unnecassary cols
@return df
"""  
def clean_extra_cols(df):
    
    df.drop(columns=["Dividends", "Stock Splits", "Capital Gains"],inplace=True,errors="ignore")
    
    return df



"""
Validate data has all appropriet cols
"""
def validate_clean_data(df):  
    
    assert df.shape[1] == 5, "Wrong df shape; Expecting 5 cols"
        
    expected_cols = {"Open", "High", "Low", "Close", "Volume"}
    assert expected_cols.issubset(df.columns), "Wrong cols; Expecting: `Open`, `High`, `Low`, `Close`, & `Volume`"
    
    assert is_datetime64_any_dtype(df.index), "Index of wrong type; Expecting datetime64"
            
    assert df["Open"].dtype == "float64", "Col `Open` of wrong type; Expecting float64"
    assert df["High"].dtype == "float64", "Col `High` of wrong type; Expecting float64"
    assert df["Low"].dtype == "float64", "Col `Low` of wrong type; Expecting float64"
    assert df["Close"].dtype == "float64", "Col `Close` of wrong type; Expecting float64"
    assert df["Volume"].dtype == "int64", "Col `Volume` of wrong type; Expecting float64"




    return True
    
    
"""
Overall call for preprocessing
"""
def preprocess_data(symbol):
    
    df = get_data(symbol)
    clean_extra_cols(df)
    validate_clean_data(df)
    
    return df
    



"""
==============================
Testing Area
"""
if __name__ == "__main__":
    
    df = preprocess_data("SPY")
    
    print(df)
    
    
    
    
    print("Testing Completed")
    

