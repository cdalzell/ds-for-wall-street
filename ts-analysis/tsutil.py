import numpy as np
from sparkts.timeseriesrdd import time_series_rdd_from_pandas_series_rdd

def count_nans(vec):
    return np.count_nonzero(np.isnan(vec))

def lead_and_lag(lead, lag, series):
    """Given a series, return a 2D array with lead and lag terms.
    
    The returned array with have (lead + lag + 1) columns and (len(series) - lead - lag) rows.
    
    Parameters
    ----------
        lead - The number of lead terms to include in the result.
        lag - The number of lag terms to include in the result.
        series - The series to lead and lag.
    """
    series = np.transpose(series)
    mat = np.zeros([len(series) - lead - lag, lead + lag + 1])
    mat[:,0] = series[lag:-lead]
    mat[:,lead] = series[lag+lead:]
    for i in xrange(1, lead):
        mat[:,i] = series[lag+i:-lead+i]
    for i in xrange(1, lag + 1):
        mat[:,i+lead] = series[lag-i:-lead-i]
    return mat
    
def sample_daily(tsrdd, how):
    """Accepts a TimeSeriesRDD with granularity finer than daily and rolls it up.
    
    Parameters
    ----------
        tsrdd - A TimeSeriesRDD.
        how - The sampling method.
    
    Returns
    -------
        A condensed TimeSeriesRDD.
    """
    return time_series_rdd_from_pandas_series_rdd( \
        ticker_tsrdd.to_pandas_series_rdd() \
            .mapValues(lambda x: x.resample('D', how=how)))


