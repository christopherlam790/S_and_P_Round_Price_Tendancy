"""
TAKE ANALYZED CLEAN DATA -> INGESTION INTO POSGRESQL
"""


import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()


import analyze_clean_data as analyze_clean_data

    
def prep_df_for_sql(df):

    df.index.name = "index"
    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    
    return df

    
def upload_data_as_postgressql(df, ticker, pval_threshold, wilson_threshold):
    
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
    CREATE TABLE IF NOT EXISTS {ticker}_analyzed_data_pval_{pval_threshold}_wilson_{wilson_threshold} (
        index SMALLINT,
        day TEXT,
        feature TEXT,
        p_hat DOUBLE PRECISION,
        p0 DOUBLE PRECISION,
        delta DOUBLE PRECISION,
        n_obs BIGINT,
        
        z DOUBLE PRECISION,
        p_value DOUBLE PRECISION,
        ci_low DOUBLE PRECISION,
        ci_high DOUBLE PRECISION,
        is_significant BOOLEAN,
        
        
        ticker TEXT,
        pval_threshold DOUBLE PRECISION,
        wilson_threshold DOUBLE PRECISION,
        
        
        PRIMARY KEY (index)
    );
    """)
    
        
    cols = [
        "index",
        "day",
        "feature",
        "p_hat",
        "p0",
        "delta",
        "n_obs",
        "z",
        "p_value",
        "ci_low",
        "ci_high",
        "is_significant",
        "ticker",
        "pval_threshold",
        "wilson_threshold",
    ]

    records = [
        tuple(row[c] for c in cols)
        for _, row in df.iterrows()
    ]

    insert_sql = f"""
    INSERT INTO {ticker}_analyzed_data_pval_{pval_threshold}_wilson_{wilson_threshold} (
        index,
        day,
        feature,
        p_hat,
        p0,
        delta,
        n_obs,
        z,
        p_value,
        ci_low,
        ci_high,
        is_significant,
        ticker,
        pval_threshold,
        wilson_threshold
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (index) DO NOTHING;
    """

    execute_batch(curr, insert_sql, records, page_size=1000)
    
    conn.commit()
    curr.close()
    conn.close()

    return True


"""
Upload raw data into PostgreSQL
"""
def upload_raw_data(ticker, pval_threhsold=0.05, wilson_threshold=0.05):
    
    df = analyze_clean_data.get_combined_final_stats(ticker=ticker, pval_threshold=pval_threhsold, wilson_threshold=wilson_threshold)
    
    df_sql = prep_df_for_sql(df=df)
    
    """
    Convert ticker for SQL format
    """
    def sql_ticker_converter(ticker):
        
        return ticker.replace("^", "")
    
    def sql_threshold_converter(threshold):
        return str(threshold).replace(".","_")
    
    
    print(df_sql)
        
        
    upload_data_as_postgressql(df=df_sql, ticker=sql_ticker_converter(ticker), 
                               pval_threshold=sql_threshold_converter(pval_threhsold), 
                               wilson_threshold=sql_threshold_converter(wilson_threshold))
    
    return



"""
==========================================
Testing Section
"""
if __name__ == "__main__":
    
    
    upload_raw_data("SPY", pval_threhsold=0.05, wilson_threshold=0.05)
    
    print("TESTING COMPLETE")
    
    


    
    

    