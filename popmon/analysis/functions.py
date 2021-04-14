# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import as_strided
from scipy import linalg, stats
from scipy.stats import linregress, norm

from ..analysis.hist_numpy import (
    check_similar_hists,
    get_consistent_numpy_2dgrids,
    get_consistent_numpy_entries,
    set_2dgrid,
)
from ..hist.hist_utils import COMMON_HIST_TYPES, is_numeric
from ..stats.numpy import probability_distribution_mean_covariance


def pull(row, suffix_mean="_mean", suffix_std="_std", cols=None):
    """Calculate normalized residual (pull) for list of cols

    Function can be used by ApplyFunc module.

    :param pd.Series row: row to apply pull function to
    :param list cols: list of cols to calculate pull of
    :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
    :param str suffix_std: suffix of std. std column = metric + suffix_std
    """
    x = pd.Series()
    if cols is None or len(cols) == 0:
        # if no columns are given, find colums for which pulls can be calculated.
        # e.g. to calculate x_pull, need to have [x, x_mean, x_std] present. If so, put x in cols.
        cols = []
        for m in row.index.to_list()[:]:
            if m not in cols:
                required = [m, m + suffix_mean, m + suffix_std]
                if all(r in row for r in required):
                    cols.append(m)
    for m in cols:
        x[m] = np.nan
        required = [m, m + suffix_mean, m + suffix_std]
        if not all(r in row for r in required):
            continue
        if any(pd.isnull(row[required])):
            continue
        if row[m + suffix_std] == 0.0:
            continue
        x[m] = (row[m] - row[m + suffix_mean]) / row[m + suffix_std]
    return x


def expanding_mean(df, shift=1):
    """Calculate expanding mean of all numeric columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int shift: size of shift. default is 1.
    :return: df with expanding means of columns
    """
    return df.shift(shift).expanding().mean()


def expanding_std(df, shift=1):
    """Calculate expanding std of all numeric columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int shift: size of shift. default is 1.
    :return: df with expanding std of columns
    """
    return df.shift(shift).expanding().std()


def expanding_apply(df, func, shift=1, *args, **kwargs):
    """Calculate expanding apply() to all columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param func: function to be applied
    :param int shift: size of shift. default is 1.
    :param args: args passed on to function
    :param kwargs: kwargs passed on to function
    :return: df with expanding results of function applied to all columns
    """
    return df.shift(shift).expanding().apply(func, args=args, **kwargs)


def rolling_std(df, window, shift=1):
    """Calculate rolling std of all numeric columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int shift: size of shift. default is 1.
    :param int window: size of rolling window.
    :return: df with rolling std of columns
    """
    return df.shift(shift).rolling(window).std()


def rolling_mean(df, window, shift=1):
    """Calculate rolling mean of all numeric columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int shift: size of shift. default is 1.
    :param int window: size of rolling window.
    :return: df with rolling mean of columns
    """
    return df.shift(shift).rolling(window).mean()


def rolling_apply(df, window, func, shift=1, *args, **kwargs):
    """Calculate rolling apply() to all columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int window: size of rolling window.
    :param func: function to be applied
    :param int shift: size of shift. default is 1.
    :param args: args passed on to function
    :param kwargs: kwargs passed on to function
    :return: df with rolling results of function applied to all columns
    """
    # raw=False already use Future setting
    return df.shift(shift).rolling(window).apply(func, raw=False, args=args, **kwargs)


def rolling_lr(df, window, index=0, shift=0):
    """Calculate rolling scipy lin_regress() to all columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int window: size of rolling window.
    :param int index: index of lin_regress results to return. default is 0.
    :param int shift: size of shift. default is 0.
    :return: df with rolling results of lin_regress() function applied to all columns
    """
    # raw=True suppresses Future warning
    return (
        df.shift(shift)
        .rolling(window)
        .apply(lambda x: linregress(np.arange(len(x)), x)[index], raw=True)
    )


def rolling_lr_zscore(df, window, shift=0):
    """Calculate rolling z-score of scipy lin_regress() to all columns of a pandas dataframe

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe
    :param int window: size of rolling window.
    :param int shift: size of shift. default is 0.
    :return: df with rolling z-score results of lin_regress() function applied to all columns
    """
    # MB 20200420: turn original df.rolling off, it doesn't accept timestamps.
    # raw=True suppresses Future warning
    # return df.shift(shift).rolling(window).apply(func, raw=True)
    def func(x):
        y = pd.Series(index=x.index, dtype=float)
        for name in x.index:
            try:
                xv = x[name].astype(float)
                y[name] = norm.ppf(linregress(np.arange(len(xv)), xv)[3])
            except Exception:
                y[name] = np.nan
        return y

    return roll(df, window=window, shift=shift).apply(func, axis=1)


def roll(df, window, shift=1):
    """Implementation of rolling window that can handle non-numerical columns such as histograms

    :param pd.DataFrame df: input dataframe to apply rolling function to.
    :param int window: size of rolling window
    :param int shift: shift of dataframe, default is 1 (optional)
    """
    assert shift >= 0
    assert isinstance(
        df, (pd.DataFrame, pd.Series)
    ), "input should be a dataframe or series"

    cols = df.columns if isinstance(df, pd.DataFrame) else [df.name]
    x = df.values
    # apply shift
    x = x[:-shift] if shift > 0 else x

    # apply windowing, use numpy's as_strided function to step through x and create sub-arrays
    if isinstance(df, pd.DataFrame):
        d0, d1 = x.shape
        s0, s1 = x.strides
        arr = as_strided(x, (d0 - (window - 1), window, d1), (s0, s0, s1))
    elif isinstance(df, pd.Series):
        hopsize = 1
        nrows = ((x.size - window) // hopsize) + 1
        if nrows < 0:
            nrows = 0
        n = x.strides[0]
        arr = as_strided(x, shape=(nrows, window), strides=(hopsize * n, n))

    # fill up missing values b/c off window & shift with Nones
    arr_shape = list(arr.shape)
    arr_shape[0] = len(df.index) - len(arr)
    arr_shape = tuple(arr_shape)
    n_fill = len(cols) * window * (len(df.index) - len(arr))
    fill_value = np.array([[None] * n_fill]).reshape(arr_shape)
    arr = np.append(fill_value, arr, axis=0)

    # reshape to new data frame
    def reshape(vs, i):
        return vs if len(vs.shape) == 1 else vs[:, i]

    d = [{c: reshape(vals, i) for i, c in enumerate(cols)} for vals in arr]
    rolled_df = pd.DataFrame(data=d, index=df.index)

    return rolled_df


def expand(df, shift=1):
    """Implementation of expanding window that can handle non-numerical values such as histograms

    Split up input array into expanding sub-arrays

    :param pd.DataFrame df: input dataframe to apply rolling function to.
    :param int shift: shift of dataframe, default is 1 (optional)
    :param fillvalue: default value to fill dataframe in case shift > 0 (optional)
    """
    assert shift >= 0
    assert isinstance(
        df, (pd.DataFrame, pd.Series)
    ), "input should be a dataframe or series"

    cols = df.columns if isinstance(df, pd.DataFrame) else [df.name]
    x = df.values
    arr = [x[: max(i + 1 - shift, 0)] for i in range(x.shape[0])]

    # fill up missing values b/c off shift with Nones
    fill_value = np.array([[None] * len(cols)])
    for i in range(shift):
        arr[i] = fill_value

    # reshape to new data frame
    def reshape(vs, i):
        return vs if len(vs.shape) == 1 else vs[:, i]

    d = [{c: reshape(vals, i) for i, c in enumerate(cols)} for vals in arr]
    expanded_df = pd.DataFrame(data=d, index=df.index)

    return expanded_df


def expanding_hist(df, shift=1, *args, **kwargs):
    """Apply expanding histogram sum

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe with column of histograms
    :param int shift: shift of dataframe, default is 1 (optional)
    :param args: args passed on to hist_sum function
    :param kwargs: kwargs passed on to hist_sum function
    :return: dataframe with expanding hist_sum results
    """
    return expand(df, shift=shift).apply(hist_sum, axis=1, args=args, **kwargs)


def rolling_hist(df, window, shift=1, *args, **kwargs):
    """Apply rolling histogram sum

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe with column of histograms
    :param int window: size of rolling window
    :param int shift: shift of dataframe, default is 1 (optional)
    :param args: args passed on to hist_sum function
    :param kwargs: kwargs passed on to hist_sum function
    :return: dataframe with rolling hist_sum results
    """
    return roll(df, window=window, shift=shift).apply(
        hist_sum, axis=1, args=args, **kwargs
    )


def hist_sum(x, hist_name=""):
    """Return sum of histograms

    Usage: df['hists'].apply(hist_sum) ; series.apply(hist_sum)

    :param pd.Series x: pandas series to extract histogram list from.
    :param str hist_name: name of column to extract histograms from. needs to be set with axis=1 (optional)
    :return: sum histogram
    """
    assert isinstance(x, pd.Series)
    if len(hist_name) > 0 and hist_name in x:
        hist_list = x[hist_name]
    else:
        hist_list = x.values
        if len(hist_name) == 0:
            hist_name = "histogram"

    if len(hist_list) == 0:
        raise ValueError("List of input histograms is empty.")

    # initialize
    o = pd.Series()
    o[hist_name] = None

    # basic checks
    all_hist = all([isinstance(hist, COMMON_HIST_TYPES) for hist in hist_list])
    if not all_hist:
        return o

    similar = check_similar_hists(hist_list)
    if not similar:
        return o

    # MB FIX: h_sum not initialized correctly in a sum by histogrammar for sparselybin (origin); below it is.
    # h_sum = np.sum([hist for hist in hist_list])

    h_sum = hist_list[0].zero()
    for hist in hist_list:
        h_sum += hist
    o[hist_name] = h_sum
    return o


def roll_norm_hist_mean_cov(df, window, shift=1, *args, **kwargs):
    """Apply rolling normalized_hist_mean_cov function

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe with column of histograms
    :param int window: size of rolling window
    :param int shift: shift of dataframe, default is 1 (optional)
    :param args: args passed on to hist_sum function
    :param kwargs: kwargs passed on to hist_sum function
    :return: dataframe with rolling normalized_hist_mean_cov results
    """
    return roll(df, window=window, shift=shift).apply(
        normalized_hist_mean_cov, axis=1, args=args, **kwargs
    )


def expand_norm_hist_mean_cov(df, shift=1, *args, **kwargs):
    """Apply expanding normalized_hist_mean_cov function

    Function can be used by ApplyFunc module.

    :param pd.DataFrame df: input pandas dataframe with column of histograms
    :param int shift: shift of dataframe, default is 1 (optional)
    :param args: args passed on to hist_sum function
    :param kwargs: kwargs passed on to hist_sum function
    :return: dataframe with expanding normalized_hist_mean_cov results
    """
    return expand(df, shift=shift).apply(
        normalized_hist_mean_cov, axis=1, args=args, **kwargs
    )


def normalized_hist_mean_cov(x, hist_name=""):
    """Mean normalized histogram and its covariance of list of input histograms

    Usage: df['hists'].apply(normalized_hist_mean_cov) ; series.apply(normalized_hist_mean_cov)

    :param pd.Series x: pandas series to extract histogram list from.
    :param str hist_name: name of column to extract histograms from. needs to be set with axis=1 (optional)
    :return: mean normalized histogram, covariance probability matrix
    """
    assert isinstance(x, pd.Series)
    if len(hist_name) > 0 and hist_name in x:
        hist_list = x[hist_name]
    else:
        hist_list = x.values
        if len(hist_name) == 0:
            hist_name = "histogram"

    if len(hist_list) == 0:
        raise ValueError("List of input histograms is empty.")

    # initialize
    o = pd.Series()
    o[hist_name + "_mean"] = None
    o[hist_name + "_cov"] = None
    o[hist_name + "_binning"] = None

    # basic checks
    all_hist = all([isinstance(hist, COMMON_HIST_TYPES) for hist in hist_list])
    if not all_hist:
        return o
    similar = check_similar_hists(hist_list)
    if not similar:
        return o

    # get entries as numpy arrays
    if hist_list[0].n_dim == 1:
        entries_list, binning = get_consistent_numpy_entries(
            hist_list, get_bin_labels=True
        )
        entries_list = np.array(entries_list, dtype=np.float)
    else:
        entries_list, xkeys, ykeys = get_consistent_numpy_2dgrids(
            hist_list, get_bin_labels=True
        )
        entries_list = np.array([h.flatten() for h in entries_list], dtype=np.float)
        binning = (xkeys, ykeys)

    # calculation of mean normalized histogram and its covariance matrix
    (
        normalized_hist_mean,
        normalized_hist_covariance,
    ) = probability_distribution_mean_covariance(entries_list)

    o[hist_name + "_mean"] = normalized_hist_mean
    o[hist_name + "_cov"] = normalized_hist_covariance
    o[hist_name + "_binning"] = binning
    return o


def relative_chi_squared(
    row,
    hist_name="histogram",
    suffix_mean="_mean",
    suffix_cov="_cov",
    suffix_binning="_binning",
):
    """Calculate chi squared of normalized histogram with pre-calculated mean normalized histogram

    :param pd.Series row: row to apply chi_squared function to.
    :param str hist_name: name of column to extract histograms from. default is 'histogram' (optional)
    :param str suffix_mean: suffix of mean. mean column = hist_name + suffix_mean (optional)
    :param str suffix_std: suffix of std. std column = hist_name + suffix_std (optional)
    :param str suffix_binning: suffix of binning. binning column = hist_name + suffix_binning (optional)
    """
    x = pd.Series()
    x["chi2"] = np.nan
    x["naive_pvalue"] = np.nan
    x["naive_zscore"] = np.nan
    x["max_res"] = np.nan

    required = [
        hist_name,
        hist_name + suffix_mean,
        hist_name + suffix_cov,
        hist_name + suffix_binning,
    ]
    if not all(r in row for r in required):
        return x

    hist = row[hist_name]
    norm_mean = row[hist_name + suffix_mean]
    cov = row[hist_name + suffix_cov]
    binning = row[hist_name + suffix_binning]

    # basic checks
    if not isinstance(hist, COMMON_HIST_TYPES):
        return x
    if any([ho is None for ho in [norm_mean, cov, binning]]):
        return x
    if len(cov.shape) != 2 or len(norm_mean.shape) != 1:
        return x

    variance = np.diagonal(cov)

    # get entries as numpy arrays
    if hist.n_dim == 1:
        entries = (
            hist.bin_entries(xvalues=binning)
            if is_numeric(hist)
            else hist.bin_entries(labels=binning)
        )
    else:
        assert len(binning) == 2
        entries = set_2dgrid(hist, binning[0], binning[1])
        entries = entries.flatten()

    # calculation of mean normalized histogram and its covariance matrix of input histogram
    single_norm, _ = probability_distribution_mean_covariance([entries])

    if (
        np.linalg.cond(cov) < 0.1 / np.finfo(cov.dtype).eps
        and np.abs(np.linalg.det(cov)) > np.finfo(cov.dtype).eps
    ):
        # check if covariance matrix is invertible
        # see: https://stackoverflow.com/questions/13249108/efficient-pythonic-check-for-singular-matrix
        # We try to use the precision matrix (inverse covariance matrix) for the chi-squared calculation
        pm = linalg.inv(cov)
        chi_squared = np.dot(
            (norm_mean - single_norm), np.dot(pm, (norm_mean - single_norm))
        )
        if chi_squared <= 0:
            chi_squared = np.finfo(np.float).eps
    else:
        # If a covariance matrix is singular we fall back on using variances
        chi_squared = np.sum(
            (norm_mean - single_norm) ** 2 / (variance + np.finfo(np.float).eps)
        )

    # pvalue and zvalue based on naive number of degrees of freedom
    ndof = len(entries) - 1
    p_value = stats.chi2.sf(chi_squared, ndof)
    z_score = -stats.norm.ppf(p_value)
    # scenario where pvalue is too small to evaluate Z
    # use Chernoff approximation for p-value upper bound
    # see: https://en.wikipedia.org/wiki/Chi-squared_distribution
    if p_value == 0:
        z = chi_squared / ndof
        u = -np.log(2 * np.pi) - ndof * np.log(z) + ndof * (z - 1)
        z_score = np.sqrt(u - np.log(u))

    max_resid = np.max(
        np.abs((norm_mean - single_norm) / np.sqrt(variance + np.finfo(np.float).eps))
    )

    x["chi2"] = chi_squared
    x["naive_pvalue"] = p_value
    x["naive_zscore"] = z_score
    x["max_res"] = max_resid
    return x
