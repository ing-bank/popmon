# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
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


import copy
import logging

import histogrammar
import numpy as np
import pandas as pd

from ...hist.filling.pandas_histogrammar import PandasHistogrammar
from ...hist.filling.spark_histogrammar import SparkHistogrammar
from ...hist.filling.utils import check_dtype

logger = logging.getLogger()


def make_histograms(
    df,
    features=None,
    binning="auto",
    bin_specs=None,
    time_axis="",
    time_width=None,
    time_offset=0,
    var_dtype=None,
    ret_specs=False,
    nbins_1d=40,
    nbins_2d=20,
    nbins_3d=10,
    max_nunique=500,
):
    """Create histograms from pandas or spark dataframe.

    :param df: input pandas or spark dataframe to create histograms of.
    :param list features: columns to pick up from input data. (default is all features)
        For multi-dimensional histograms, separate the column names with a ":". An example features list is:

        .. code-block:: python

            features = ['x', 'date', 'date:x', 'date:y', 'date:x:y']

    :param str binning: default binning to revert to in case bin_specs not supplied. options are:
        "unit" or "auto", default is "auto". When using "auto", semi-clever binning is automatically done.
    :param dict bin_specs: dictionaries used for rebinning numeric or timestamp features. An example bin_specs
        dictionary is:

        .. code-block:: python

            bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                         'y': {'num': 10, 'low': 0.0, 'high': 2.0},
                         'x:y': [{}, {'num': 5, 'low': 0.0, 'high': 1.0}]}

        In the bin specs for x:y, x is not provided (here) and reverts to the 1-dim setting. The 'bin_width',
        'bin_offset' notation makes an open-ended histogram (for that feature) with given bin width and offset.
        The notation 'num', 'low', 'high' gives a fixed range histogram from 'low' to 'high' with 'num'
        number of bins.
    :param str time_axis: name of datetime feature, used as time axis, eg 'date'. if True, will be guessed.
        If time_axis is set, if no features given, features becomes: ['date:x', 'date:y', 'date:z'] etc.
    :param time_width: bin width of time_axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1w', 3600e9 (number of ns),
                      anything understood by pd.Timedelta(time_width).value

    :param time_offset: bin offset of time_axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1-1-2020', 0 (number of ns since 1-1-1970),
                      anything parsed by pd.Timestamp(time_offset).value

    :param dict var_dtype: dictionary with specified datatype per feature (optional)
    :param bool ret_specs: if true, also return features, bin_specs, var_dtype, time_axis used for filling histograms.
    :param int nbins_1d: auto-binning number of bins for 1d histograms. default is 40.
    :param int nbins_2d: auto-binning number of bins for 2d histograms. default is 20.
    :param int nbins_3d: auto-binning number of bins for 3d histograms. default is 10.
    :param int max_nunique: auto-binning threshold for unique categorical values. default is 500.
    :return: dict of created histogrammar histograms
    """
    # basic checks on presence of time_axis
    if (not isinstance(time_axis, (str, bool))) or (
        isinstance(time_axis, bool) and not time_axis
    ):
        raise TypeError("time_axis needs to be a string, or a bool set to True")
    if (
        isinstance(time_axis, str)
        and len(time_axis) > 0
        and time_axis not in df.columns
    ):
        raise ValueError(f'time_axis "{time_axis}" not found in columns of dataframe.')
    if isinstance(time_axis, bool):
        time_axes = get_time_axes(df)
        num = len(time_axes)
        if num == 1:
            time_axis = time_axes[0]
            logger.info(f'Time-axis automatically set to "{time_axis}"')
        elif num == 0:
            raise RuntimeError(
                "No obvious time-axes found. Cannot generate stability report."
            )
        else:
            raise RuntimeError(
                f"Found {num} time-axes: {time_axes}. Set *one* time_axis manually!"
            )

    # if time_axis present, interpret time_width and time_offset
    if (
        isinstance(time_axis, str)
        and len(time_axis) > 0
        and isinstance(time_width, (str, int, float))
        and isinstance(time_offset, (str, int, float))
    ):
        if not isinstance(bin_specs, (type(None), dict)):
            raise RuntimeError("bin_specs object is not a dictionary")
        bin_specs = copy.copy(bin_specs) if isinstance(bin_specs, dict) else {}
        if time_axis in bin_specs:
            raise RuntimeError(
                f'time-axis "{time_axis}" already found in binning specifications.'
            )
        # convert time width and offset to nanoseconds
        time_specs = {
            "bin_width": float(pd.Timedelta(time_width).value),
            "bin_offset": float(pd.Timestamp(time_offset).value),
        }
        bin_specs[time_axis] = time_specs

    cls = PandasHistogrammar if isinstance(df, pd.DataFrame) else SparkHistogrammar
    hist_filler = cls(
        features=features,
        binning=binning,
        bin_specs=bin_specs,
        time_axis=time_axis,
        var_dtype=var_dtype,
        nbins_1d=nbins_1d,
        nbins_2d=nbins_2d,
        nbins_3d=nbins_3d,
        max_nunique=max_nunique,
    )
    hists = hist_filler.get_histograms(df)

    if ret_specs:
        features, binning, var_dtype, time_axis = hist_filler.get_features_specs()
        return hists, features, binning, time_axis, var_dtype

    return hists


def get_data_type(df, col):
    """Get data type of a column of pandas or spark dataframe.

    :param df: input data frame (pandas or spark)
    :param str col: column
    """
    if col not in df.columns:
        raise KeyError(f'Column "{col:s}" not in input dataframe.')
    dt = dict(df.dtypes)[col]

    if hasattr(dt, "type"):
        # convert pandas types, such as pd.Int64, into numpy types
        dt = type(dt.type())

    try:
        # spark conversions to numpy or python equivalent
        if dt == "string":
            dt = "str"
        elif dt == "timestamp":
            dt = np.datetime64
        elif dt == "boolean":
            dt = bool
        elif dt == "bigint":
            dt = np.int64
    except TypeError:
        pass

    return np.dtype(dt)


def get_time_axes(df):
    """Return all time-axis columns of a dataframe

    :param df: input dataframe (pandas or spark)
    :return: list of time-axis columns
    """
    return [
        c
        for c in df.columns
        if np.issubdtype(check_dtype(get_data_type(df, c)), np.datetime64)
    ]


def has_one_time_axis(df):
    """Return boolean if one time-axis column in dataframe

    :param df: input dataframe (pandas or spark)
    :return: boolean if one time-axis column
    """
    dt_cols = get_time_axes(df)
    return len(dt_cols) == 1


def get_one_time_axis(df):
    """Return time-axis if one time-axis column in dataframe

    :param df: input dataframe (pandas or spark)
    :return: one time-axis column, else empty string
    """
    dt_cols = get_time_axes(df)
    return dt_cols[0] if len(dt_cols) == 1 else ""


def _get_bin_specs(h):
    """Get histogram bin specifications

    :param h: input histogrammar histogram
    :return: list with bin_specs of all dimensions of the histogram
    :rtype: list
    """
    bin_specs = []
    if isinstance(h, histogrammar.Count):
        return bin_specs

    if isinstance(h, histogrammar.Categorize):
        bin_specs.append({})
    elif isinstance(h, histogrammar.Bin):
        bin_specs.append(dict(num=h.num, low=h.low, high=h.high))
    elif isinstance(h, histogrammar.SparselyBin):
        bin_specs.append(dict(bin_width=h.binWidth, bin_offset=h.origin))

    # histogram may have a sub-histogram. Extract it and recurse
    if hasattr(h, "bins"):
        hist = list(h.bins.values())[0] if h.bins else histogrammar.Count()
    elif hasattr(h, "values"):
        hist = h.values[0] if h.values else histogrammar.Count()
    else:
        hist = histogrammar.Count()
    return bin_specs + _get_bin_specs(hist)


def _match_first_key(skip_first_axis=None, feature=""):
    """Helper function to match and remove skip_first_axis from feature

    :param skip_first_axis: True or string. if set, ignore first axis of input histogram(s)
    :param feature: input feature
    :return: match and (rest of) feature
    """
    assert isinstance(feature, str)
    karr = feature.split(":")
    begin = karr[0]
    rest_key = ":".join(karr[1:])
    if isinstance(skip_first_axis, bool):
        return skip_first_axis, rest_key if skip_first_axis else feature
    elif isinstance(skip_first_axis, str) and len(skip_first_axis) > 0:
        match = begin == skip_first_axis
        return match, rest_key if match else feature
    return False, feature


def get_bin_specs(hd, skip_first_axis=False):
    """Get histogram bin specifications

    :param hd: input histogrammar histogram (or dict of input histograms)
    :param skip_first_axis: bool or string of first axis. if set, ignore first axis of input histogram(s)
    :return: list (or dict with lists) with bin_specs of all dimensions of the histogram
    :rtype: list (or dict)
    """
    if isinstance(hd, dict):
        bin_specs = {}
        for key, h in hd.items():
            bs = _get_bin_specs(h)
            match, rest_key = _match_first_key(skip_first_axis, key)
            bs = bs[1:] if match else bs
            bs = bs[0] if len(bs) == 1 else bs
            bin_specs[rest_key] = bs
    else:
        bs = _get_bin_specs(hd)
        match, _ = _match_first_key(skip_first_axis)
        bs = bs[1:] if match else bs
        bs = bs[0] if len(bs) == 1 else bs
        bin_specs = bs
    return bin_specs
