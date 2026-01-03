"""
TAKE PREPROCESSED CLEAN DATA -> INGESTION INTO POSGRESQL
"""


import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()


import preprocess_raw_data


    
def prep_df_for_sql(df):

    df.index.name = "date"
    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    
    return df

    
def download_data_as_postgressql(df, ticker):
    
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    
    curr = conn.cursor()
    
    # do things
    
    curr.execute(f"""
    CREATE TABLE IF NOT EXISTS {ticker}_daily_prices (
        date TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        PRIMARY KEY (Date)
    );
    """)
    
    
    records = df.itertuples(index=False, name=None)

    insert_sql = f"""
    INSERT INTO {ticker}_daily_prices (date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (date) DO NOTHING;
    """

    execute_batch(curr, insert_sql, records, page_size=1000)
    
    conn.commit()
    curr.close()
    conn.close()

    return True

def verify_download():
    
    return True





"""
Download raw data into PostgreSQL
"""
def download_raw_data(ticker):
    
    df = preprocess_raw_data.preprocess_data(symbol=ticker)
    
    df_sql = prep_df_for_sql(df=df)
    
    """
    Convert ticker for SQL format
    """
    def sql_ticker_converter(ticker):
        
        return ticker.replace("^", "")
        
        
    
    download_data_as_postgressql(df=df_sql, ticker=sql_ticker_converter(ticker))   
    
    return



"""
==========================================
Testing Section
"""
if __name__ == "__main__":
    
    
    download_raw_data("SPY")
    
    print("TESTING COMPLETE")
    
    


    
    

    