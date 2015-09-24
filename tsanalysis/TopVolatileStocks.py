import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import numpy as np
import seaborn as sns

datetimeidx = pd.date_range('8/22/2015', periods=5, freq='D')
volatile_tickers = [('one', np.array([0.1, 0.2, 0.3, 1.0, 1.1])),
                    ('two', np.array([0.9, 0.6, 2.1, 1.1, 1.4])),
                    ('three', np.array([0.8, 0.5, 0.1, 1.2, 1.5]))
                    ]
ticker_vol_aug_24 = [('one', 1.0),
                    ('two', 2.0),
                    ('three',  3.0),
                     ('four', 1.0),
                    ]

def plot_volatility_over_time(ticker_df):
    """Plots volatility of list of stocks over time. Not a great plot when
    there are many stocks.
    Parameters
    ----------
        ticker_df - dataframe of stock time series where columns are stock
        prices and the rows are observations at a specific time.
    Returns
    -------
    Displays a plot of volatility over time.
    """
    ticker_df.plot()
    print("Printing volatility over time plot.")
    plt.savefig("img/vol_over_time.png")
    plt.close()

def plot_joint_dist(ticker_df):
    g = sns.PairGrid(ticker_df)
    g.map(plt.scatter)
    print("Printing joint distribution")
    sns.plt.savefig('img/joint-dist')
    sns.plt.close()

def plot_kde_smoothed_joint_dist(ticker_vol):
    grid = sns.PairGrid(ticker_vol)
    grid.map_diag(sns.kdeplot)
    grid.map_offdiag(sns.kdeplot, cmap="Blues_d", n_levels=6)
    print("Printing kde smoothed plot.")
    sns.plt.savefig('img/kde-smoothed-pair-plot')
    sns.plt.close()

def top_10(ticker_vol_pairs):
    """Returns the top ten ticker symbols by volatility."""
    sorted_by_vol = sorted(ticker_vol_pairs, key=lambda x: x[1], reverse=True)
    ticker_symbols = zip(*sorted_by_vol[:10])[0]
    return ticker_symbols

if __name__ == "__main__":
    ticker_df = pd.DataFrame.from_items(volatile_tickers)
    indexed_ticker_df = ticker_df.set_index(datetimeidx)
    plot_volatility_over_time(indexed_ticker_df)
    plot_joint_dist(indexed_ticker_df)
    plot_kde_smoothed_joint_dist(indexed_ticker_df)

