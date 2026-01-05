"""
PGSQL CLEAN DATA -> PYTHON DF
"""



import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv


"""
Pull ticker's raw data, if it exists, from PGSQL db
"""
def get_raw_df_from_sql(ticker):
    
    try:
        load_dotenv()

        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
    except:
        raise Exception("No connection; Check creds")

    try:
        
        """
        Convert ticker for SQL format
        """
        def sql_ticker_converter(ticker):
            
            return ticker.replace("^", "")
        
        ticker = sql_ticker_converter(ticker=ticker)
        
        query = f"""
        SELECT date, open, high, low, close, volume, weekday,
        true_rounded_close,
        true_dollar_rounded_close,
        true_point_5_rounded_close,
        true_point_25_rounded_close,
        floor_rounded_close,
        ceil_rounded_close,
        is_dollar_rounded_true_multiple_of_5,
        is_dollar_rounded_true_multiple_of_10,
        is_point_5_rounded_true_multiple_of_1,
        is_point_25_rounded_true_multiple_of_1,
        is_dollar_rounded_tolerance_1_multiple_of_5,
        is_dollar_rounded_tolerance_1_multiple_of_10
        FROM {ticker}_daily_prices_clean
        ORDER BY date;
        """

        df = pd.read_sql(query, conn)
        conn.close()

        df = df.set_index("date")

        return df
    except:
        raise Exception("Failed to pull data; Check df integrity & schema")


"""
=======
TEST AREA
"""
if __name__ == "__main__":
    df = get_raw_df_from_sql("SPY")
    
    print(df)