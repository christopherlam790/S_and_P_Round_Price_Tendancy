"""
CLEAN PGSQL DATA -> ANALYZED RESULTS DF
"""

from scipy.stats import norm
import numpy as np
import pandas as pd
from download_clean_data import get_clean_df_from_sql
from statsmodels.stats.proportion import proportion_confint

"""
=========
Helpers for prep
"""

"""
Gets percentage of occurences where dollar rounded is true mult of 5, depending on day of week
"""
def get_is_dollar_rounded_true_multiple_of_5_prob(df, weekday = "absolute"):
    
    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_dollar_rounded_true_multiple_of_5"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_dollar_rounded_true_multiple_of_5"].mean(), denom]
    
 


"""
Gets percentage of occurences where dollar rounded is true mult of 10
"""
def get_is_dollar_rounded_true_multiple_of_10_prob(df, weekday = "absolute"):
   
    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_dollar_rounded_true_multiple_of_10"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_dollar_rounded_true_multiple_of_10"].mean(), denom]
   
    


"""
Gets percentage of occurences where .5 rounded is true mult of 1
"""
def get_is_point_5_rounded_true_multiple_of_1_prob(df, weekday = "absolute"):

    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_point_5_rounded_true_multiple_of_1"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_point_5_rounded_true_multiple_of_1"].mean(), denom]




"""
Gets percentage of occurences where .25 rounded is true mult of 1
"""
def get_is_point_25_rounded_true_multiple_of_1_prob(df, weekday = "absolute"):
    
    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_point_25_rounded_true_multiple_of_1"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_point_25_rounded_true_multiple_of_1"].mean(), denom]


"""
Gets percentage of occurences where dollar rounded is tolerance 1 mult of 5
"""
def get_is_dollar_rounded_tolerance_1_multiple_of_5_prob(df, weekday = "absolute"):
    
    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_dollar_rounded_tolerance_1_multiple_of_5"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_dollar_rounded_tolerance_1_multiple_of_5"].mean(), denom]



"""
Gets percentage of occurences where dollar rounded is tolerance 1 mult of 10
"""
def get_is_dollar_rounded_tolerance_1_multiple_of_10_prob(df, weekday = "absolute"):
     
    if weekday == "absolute":
        denom = len(df)
        if denom == 0:
            return 0.0
        return [df["is_dollar_rounded_tolerance_1_multiple_of_10"].mean(), denom]
    
    df_w = df[df["weekday"] == weekday]
    denom = len(df_w)
    if denom == 0:
        return 0.0

    return [df_w["is_dollar_rounded_tolerance_1_multiple_of_10"].mean(), denom]

"""
======
Prep & Aggregation
"""

def analyze_clean_data(ticker, weekday= "absolute"):
    
    df = get_clean_df_from_sql(ticker=ticker)
    
    df_analyzed = pd.DataFrame({
        "day": [weekday],
        
        "dol_round_true_mult_5_prob": [get_is_dollar_rounded_true_multiple_of_5_prob(df=df, weekday= weekday)[0]],
        "dol_round_true_mult_5_n_obs": [get_is_dollar_rounded_true_multiple_of_5_prob(df=df, weekday= weekday)[1]],
 
        "dol_round_true_mult_10_prob":  [get_is_dollar_rounded_true_multiple_of_10_prob(df=df, weekday= weekday)[0]],
        "dol_round_true_mult_10_n_obs":  [get_is_dollar_rounded_true_multiple_of_10_prob(df=df, weekday= weekday)[1]],
        
        
        "point_5_round_true_mult_1_prob": [get_is_point_5_rounded_true_multiple_of_1_prob(df=df, weekday= weekday)[0]],
        "point_5_round_true_mult_1_n_obs": [get_is_point_5_rounded_true_multiple_of_1_prob(df=df, weekday= weekday)[1]],
        
        
        "point_25_round_true_mult_1_prob": [get_is_point_25_rounded_true_multiple_of_1_prob(df=df, weekday= weekday)[0]],
        "point_25_round_true_mult_1_n_obs": [get_is_point_25_rounded_true_multiple_of_1_prob(df=df, weekday= weekday)[1]],
        
        
        "dol_round_tol_1_mult_5_prob": [get_is_dollar_rounded_tolerance_1_multiple_of_5_prob(df=df, weekday= weekday)[0]],
        "dol_round_tol_1_mult_5_n_obs": [get_is_dollar_rounded_tolerance_1_multiple_of_5_prob(df=df, weekday= weekday)[1]],
        
        
        "dol_round_tol_1_mult_10_prob": [get_is_dollar_rounded_tolerance_1_multiple_of_10_prob(df=df, weekday= weekday)[0]],
        "dol_round_tol_1_mult_10_n_obs": [get_is_dollar_rounded_tolerance_1_multiple_of_10_prob(df=df, weekday= weekday)[1]]
    })
    
    return df_analyzed
    
def analyze_all_clean_data(ticker):
    df = pd.DataFrame()
    
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "absolute"]:
        df = pd.concat([df,analyze_clean_data(ticker, weekday=day)], ignore_index=True)
                
    return df 

"""
======
Statistical Significance
"""


def get_EXPECTED_P():
    
    
    EXPECTED_P = {
    "dol_round_true_mult_5": 0.20,
    "dol_round_true_mult_10": 0.10,
    "point_5_round_true_mult_1": 0.50,
    "point_25_round_true_mult_1": 0.25,
    "dol_round_tol_1_mult_5": 0.60,
    "dol_round_tol_1_mult_10": 0.30,
    }


    return EXPECTED_P


def proportion_z_test(p_hat, p0, n):
    se = np.sqrt(p0 * (1 - p0) / n)
    z = (p_hat - p0) / se
    p_value = 2 * (1 - norm.cdf(abs(z)))  # two-sided
    return z, p_value



def wilson_ci(successes, n, alpha=0.05):
    return proportion_confint(
        count=successes,
        nobs=n,
        alpha=alpha,
        method="wilson"
    )


def get_stat_sig(df, pval_threshold=0.05):
    
    results = []

    for _, row in df.iterrows():
        day = row["day"]

        for feature, p0 in get_EXPECTED_P().items():
            prob_col = f"{feature}_prob"
            n_col = f"{feature}_n_obs"

            p_hat = row[prob_col]
            n = row[n_col]

            z, pval = proportion_z_test(p_hat, p0, n)
            ci_low, ci_high = wilson_ci(p_hat, n)

            results.append({
                "day": day,
                "feature": feature,
                "p_hat": p_hat,
                "p0": p0,
                "delta": p_hat - p0,
                "n_obs": n,
                "z": z,
                "p_value": pval,
                "ci_low": ci_low,
                "ci_high": ci_high,
                f"significant_{pval_threshold * 100}pct": pval < pval_threshold
            })

    results_df = pd.DataFrame(results)
    
    
    return results_df

if __name__ == "__main__":
    
    df = analyze_all_clean_data("^SPX")
    
    pval_threshold = 0.1
    
    stat_df = get_stat_sig(df, pval_threshold=pval_threshold)
    
    print(stat_df[(stat_df["delta"] > 0) & (stat_df[f"significant_{pval_threshold * 100}pct"] == True) ])
    
    
    print("TESTING COMPLETE")
    
