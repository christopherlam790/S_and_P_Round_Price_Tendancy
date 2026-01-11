"""
PGSQL Analyzed Data -> Visualization of Results
"""


from download_analyzed_data import get_clean_df_from_sql 
import matplotlib.pyplot as plt
import numpy as np


def generate_point_estimate_vs_null_with_ci(ticker, pval_threshold, wilson_threshold):

    df_plot = get_clean_df_from_sql(ticker=ticker, pval_threshold=pval_threshold, wilson_threshold=wilson_threshold)
 
    days = df_plot["day"].unique()

    for day in days:
        d = df_plot[df_plot["day"] == day]
                
        plt.figure()

        x = range(len(d))

        plt.errorbar(
            x=x,
            y=d["p_hat"],
            yerr=[d["p_hat"] - d["ci_low"], d["ci_high"] - d["p_hat"]],
            fmt="o",
            capsize=4
        )

        # plot per-feature nulls
        plt.scatter(
            x=x,
            y=d["p0"],
            marker="_",
            s=400,
            c="red"
            
        )        
        
        plt.xticks(range(len(d)), d["feature"], rotation=45, ha="right")
        plt.ylabel("Probability")
        plt.title(f"{ticker} {day}: p̂ vs Null({pval_threshold}) with Wilson CI({wilson_threshold})")
        plt.tight_layout()
        
        plt.savefig(f"{ticker} {day}: p̂ vs Null({pval_threshold}) with Wilson CI({wilson_threshold}).jpg")
        plt.show()
        
    return

def generate_delta_heatmap(ticker, pval_threshold, wilson_threshold):
    
    df = get_clean_df_from_sql(ticker=ticker, pval_threshold=pval_threshold, wilson_threshold=wilson_threshold)

    
    pivot = (
    df.pivot(index="feature", columns="day", values="delta")
    )

    plt.figure()
    plt.imshow(pivot.values)
    plt.colorbar(label="p̂ − p₀")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45)
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.title(f"{ticker} Delta Heatmap by Day and Feature\n pval_threshold: {pval_threshold} & wilson_threshold: {wilson_threshold}")
    plt.tight_layout()
    
    plt.savefig(f"{ticker} Delta Heatmap by Day and Feature\n pval_threshold: {pval_threshold} & wilson_threshold: {wilson_threshold}.jpg")
    plt.show()

    
    return
    
    
def generate_diagnostic_plot(ticker, pval_threshold, wilson_threshold):

    d = get_clean_df_from_sql(ticker=ticker, pval_threshold=pval_threshold, wilson_threshold=wilson_threshold)

    plt.figure()
    plt.scatter(d["delta"], -np.log10(d["p_value"]))
    plt.axhline(-np.log10(pval_threshol), linestyle="--")
    plt.xlabel("p̂ − p₀")
    plt.ylabel("-log10(p-value)")
    plt.title(f"{ticker} Effect Size vs Statistical Significance \n {pval_threshold} & wilson_threshold: {wilson_threshold}")
    plt.tight_layout()
    
    plt.savefig(f"{ticker} Effect Size vs Statistical Significance \n {pval_threshold} & wilson_threshold: {wilson_threshold}.jpg")
    plt.show()


if __name__ == "__main__":  
    
    ticker = "^XSP"
    pval_threshol = 0.1
    wilson_threshold = 0.1
    
    # generate_point_estimate_vs_null_with_ci(ticker=ticker, pval_threshold=pval_threshol, wilson_threshold=wilson_threshold)
    # generate_delta_heatmap(ticker=ticker, pval_threshold=pval_threshol, wilson_threshold=wilson_threshold)  
    generate_diagnostic_plot(ticker=ticker, pval_threshold=pval_threshol, wilson_threshold=wilson_threshold)
    
