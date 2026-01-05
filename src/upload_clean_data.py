"""
TAKE TRANSFORMED CLEAN DATA -> INGESTION INTO POSGRESQL
"""


import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()


import transform_raw_data as transform_raw_data

    
def prep_df_for_sql(df):

    df.index.name = "date"
    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    
    return df

    
def upload_data_as_postgressql(df, ticker):
    
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
    CREATE TABLE IF NOT EXISTS {ticker}_daily_prices_clean (
        date TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        
        weekday TEXT,

        true_rounded_close DOUBLE PRECISION,
        true_dollar_rounded_close DOUBLE PRECISION,
        true_point_5_rounded_close DOUBLE PRECISION,
        true_point_25_rounded_close DOUBLE PRECISION,
        floor_rounded_close DOUBLE PRECISION,
        ceil_rounded_close DOUBLE PRECISION,

        is_dollar_rounded_true_multiple_of_5 SMALLINT,
        is_dollar_rounded_true_multiple_of_10 SMALLINT,
        is_point_5_rounded_true_multiple_of_1 SMALLINT,
        is_point_25_rounded_true_multiple_of_1 SMALLINT,
        is_dollar_rounded_tolerance_1_multiple_of_5 SMALLINT,
        is_dollar_rounded_tolerance_1_multiple_of_10 SMALLINT,
        
        
        PRIMARY KEY (Date)
    );
    """)
    
    
    records = df.itertuples(index=False, name=None)

    insert_sql = f"""
    INSERT INTO {ticker}_daily_prices_clean (
        date,
        open, high, low, close, volume,
        weekday,
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
    )
    VALUES (
        %s, %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (date) DO NOTHING;
    """

    execute_batch(curr, insert_sql, records, page_size=1000)
    
    conn.commit()
    curr.close()
    conn.close()

    return True


"""
Upload raw data into PostgreSQL
"""
def upload_raw_data(ticker):
    
    df = transform_raw_data.transform_raw_data(ticker=ticker)
    
    df_sql = prep_df_for_sql(df=df)
    
    """
    Convert ticker for SQL format
    """
    def sql_ticker_converter(ticker):
        
        return ticker.replace("^", "")
        
        
    upload_data_as_postgressql(df=df_sql, ticker=sql_ticker_converter(ticker))   
    
    return



"""
==========================================
Testing Section
"""
if __name__ == "__main__":
    
    
    upload_raw_data("^XSP")
    
    print("TESTING COMPLETE")
    
    


    
    

    