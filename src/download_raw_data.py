"""
PGSQL RAW DATA -> PYTHON DF
"""



import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv


"""
Pull ticker's raw data, if it exists, from PGSQL db
"""
def get_df_from_sql(ticker):
    
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
        query = f"""
        SELECT date, open, high, low, close, volume
        FROM {ticker}_daily_prices_raw
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
    df = get_df_from_sql("SPY")
    
    print(df.columns)