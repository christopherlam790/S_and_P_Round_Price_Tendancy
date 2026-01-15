# S-P-OPEX-Analysis
Analysis of S&amp;P behaviors surrounding major and minor OPEX days. Analyzes the tendency for S&amp;P 500 assets (SPY, ^SPX, ^XSP) to display price pinning behaviors near round values (e.g. values ending in 5) to a statistically significant level beyond random chance.

For more details on this project, see the associated paper:
https://christopherlam-portfolio-website.vercel.app/papers/SPY%20Round%20Price%20Attraction%20Analysis/S&P%20Round%20Price%20Pinning%20Tendency%20.pdf

---

# Dataset Info

The analysis is conducted on data spanning from 2023-03-01 to 2026-01-09 for each of the applicable assets (SPY, ^SPX, ^XSP). Data is seperated into 3 distinct stages:
- Raw
- Cleaned
- Aalyzed

## Raw Data
Raw data refers to the data extracted directly from Python's yfinance. Fields include:
- Date
- Open
- High
- Low
- Close
- Volume

## Cleaned Data
Cleaned data refers the cleaned version of the raw data. Fields are the same as 'Raw Data', and further include:
- Weekday
- True_rounded_close
- True_dollar_rounded_close
- True_point_5_rounded_close
- True_point_25_rounded_close
- Floor_rounded_close
- Ceil_rounded_close
- Is_dollar_rounded_true_multiple_of_5
- Is_dollar_rounded_true_multiple_of_10
- Is_point_5_rounded_true_multiple_of_1
- Is_point_25_rounded_true_multiple_of_1
- Is_dollar_rounded_tolerance_1_multiple_of_5
- Is_dollar_rounded_tolerance_1_multiple_of_10

## Analyzed Data
Analyzed verson of the cleaned data. Includes information regarding to hypothesis testing of the underlying asset's frequency to display price pinning. Fields include:
- Index
- Day
- Feature
- P_hat
- P0
- Delta
- N_obs
- Z
- P_value
- Ci_low
- Ci_high
- Is_significant
- Ticker
- Pval_threshold
- Wilson_threshold

# Methods of Statistical Analysis
Analysis of the 'Analyzed Data' dataset uses a combination of differnt Wilson Confidence Interval coupled with a various p-values:
- (0.05, 0.05)
- (0.1, 0.1)
- (0.2, 0.1)
- (0.2, 0.2)

Each combination above is mapped to each of the studied assets (SPY, ^SPX, ^XSP), each of which forms 3 types of figures:
- Point Esimate vs Null w/ Wilson CI
- Delta Heatmap
- Effects Size vs Statistical Significance

