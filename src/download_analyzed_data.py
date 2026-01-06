"""
PGSQL ANALYZED DATA -> PYTHON DF
"""



import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv


"""
Pull ticker's clean data, if it exists, from PGSQL db
"""
def get_clean_df_from_sql(ticker, pval_threshold, wilson_threshold):
    
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
       
        def sql_threshold_converter(threshold):
            return str(threshold).replace(".","_")
       
        
        ticker = sql_ticker_converter(ticker=ticker)
        pval_threshold = sql_threshold_converter(threshold=pval_threshold)
        wilson_threshold = sql_threshold_converter(threshold=wilson_threshold)
        
        query = f"""
        SELECT index,
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
        FROM {ticker}_analyzed_data_pval_{pval_threshold}_wilson_{wilson_threshold}
        ORDER BY index;
        """

        df = pd.read_sql(query, conn)
        conn.close()

        df = df.set_index("index")

        return df
    except:
        raise Exception("Failed to pull data; Check df integrity & schema")


"""
=======
TEST AREA
"""
if __name__ == "__main__":
    df = get_clean_df_from_sql("^XSP",pval_threshold=0.2, wilson_threshold=0.2)
    
    print(df)