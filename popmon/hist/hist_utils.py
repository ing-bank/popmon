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


import histogrammar
import numpy as np
import pandas as pd
from histogrammar.util import get_hist_props

COMMON_HIST_TYPES = (
    histogrammar.Categorize,
    histogrammar.Bin,
    histogrammar.SparselyBin,
    histogrammar.specialized.CategorizeHistogramMethods,
    histogrammar.specialized.HistogramMethods,
    histogrammar.specialized.SparselyHistogramMethods,
    histogrammar.specialized.CategorizeHistogramMethods,
    histogrammar.specialized.TwoDimensionallyHistogramMethods,
    histogrammar.specialized.SparselyTwoDimensionallyHistogramMethods,
)

HG_FACTORY = histogrammar.Factory()


def sum_entries(hist, default=True):
    """Recursively get sum of entries of histogram

    Sometimes hist.entries gives zero as answer? This function always works though.

    :param hist: input histogrammar histogram
    :param bool default: if false, do not use default HG method for evaluating entries, but exclude nans, of, uf.
    :return: total sum of entries of histogram
    :rtype: int
    """
    if default:
        entries = hist.entries
        if entries > 0:
            return entries

    # double check number of entries, sometimes not well set
    if hasattr(hist, "bins"):
        # loop over all counters and integrate over y (=j)
        return sum(sum_entries(bi) for bi in hist.bins.values())
    elif hasattr(hist, "values"):
        # loop over all counters and integrate over y (=j)
        return sum(sum_entries(bi) for bi in hist.values)
    elif hasattr(hist, "entries"):
        # only count histogrammar.Count() objects
        return hist.entries
    else:
        raise TypeError(
            "histogram should have attribute 'bins', 'values' or 'entries'."
        )


def project_on_x(hist):
    """Project n-dim histogram onto x-axis

    :param hist: input histogrammar histogram
    :return: on x-axis projected histogram (1d)
    """
    # basic check: projecting on itself
    if hasattr(hist, "n_dim") and hist.n_dim <= 1:
        return hist
    # basic checks on contents
    if hasattr(hist, "bins"):
        if len(hist.bins) == 0:
            return hist
    elif hasattr(hist, "values"):
        if len(hist.values) == 0:
            return hist
    else:
        return hist

    # make empty clone
    # note: cannot do: h_x = hist.zero(), b/c it copies n-dim structure, which screws up hist.toJsonString()
    if isinstance(hist, histogrammar.Bin):
        h_x = histogrammar.Bin(
            num=hist.num,
            low=hist.low,
            high=hist.high,
            quantity=hist.quantity,
        )
    elif isinstance(hist, histogrammar.SparselyBin):
        h_x = histogrammar.SparselyBin(
            binWidth=hist.binWidth,
            origin=hist.origin,
            quantity=hist.quantity,
        )
    elif isinstance(hist, histogrammar.Categorize):
        h_x = histogrammar.Categorize(quantity=hist.quantity)
    else:
        raise TypeError("Unknown histogram type. cannot get zero copy.")

    if hasattr(hist, "bins"):
        for key, bi in hist.bins.items():
            h_x.bins[key] = histogrammar.Count.ed(sum_entries(bi))
    elif hasattr(hist, "values"):
        for i, bi in enumerate(hist.values):
            h_x.values[i] = histogrammar.Count.ed(sum_entries(bi))

    return h_x


def sum_over_x(hist):
    """Integrate histogram over first dimension

    :param hist: input histogrammar histogram
    :return: integrated histogram
    """
    # basic check: nothing to do?
    if hasattr(hist, "n_dim") and hist.n_dim == 0:
        return hist
    if hasattr(hist, "n_dim") and hist.n_dim == 1:
        return histogrammar.Count.ed(sum_entries(hist))

    # n_dim >= 2 from now on
    # basic checks on contents
    if hasattr(hist, "bins"):
        if len(hist.bins) == 0:
            return hist
    elif hasattr(hist, "values"):
        if len(hist.values) == 0:
            return hist
    else:
        return hist

    # n_dim >= 2 and we have contents; here we sum over it.
    h_proj = None
    if hasattr(hist, "bins"):
        h_proj = list(hist.bins.values())[0].zero()
        # loop over all counters and integrate over x (=i)
        for bi in hist.bins.values():
            h_proj += bi
    elif hasattr(hist, "values"):
        h_proj = hist.values[0].zero()
        # loop over all counters and integrate
        for bi in hist.values:
            h_proj += bi

    return h_proj


def project_split2dhist_on_axis(splitdict, axis="x"):
    """Project a split 2d-histogram onto one axis

    Project a 2d hist that's been split with function split_hist_along_first_dimension
    onto x or y axis.

    :param dict splitdict: input split histogram to be projected.
    :param str axis: name of axis to project on, should be x or y. default is x.

    :return: sorted dictionary of sub-histograms, with as keys the x-axis name and bin-number
    :rtype: SortedDict
    """
    if not isinstance(splitdict, dict):
        raise TypeError(f"splitdict: {type(splitdict)}, type should be a dictionary.")
    if axis not in ["x", "y"]:
        raise ValueError(f"axis: {axis}, can only be x or y.")

    hdict = {
        key: project_on_x(hxy) if axis == "x" else sum_over_x(hxy)
        for key, hxy in splitdict.items()
    }

    return hdict


def get_histogram(hist_obj):
    """
    Parse input and convert to histogrammar object

    :param hist_obj: input histogrammar object. Can also be a corresponding json object or str.
    :return: histogrammar histogram
    """
    hist = None
    if isinstance(hist_obj, COMMON_HIST_TYPES):
        hist = hist_obj
    elif isinstance(hist_obj, str):
        hist = HG_FACTORY.fromJsonString(hist_obj)
    elif isinstance(hist_obj, dict):
        hist = HG_FACTORY.fromJson(hist_obj)
    if hist is None:
        raise ValueError("Please provide histogram object as input.")
    return hist


def is_timestamp(hist):
    props = get_hist_props(hist)
    return props["is_ts"]


def is_numeric(hist):
    props = get_hist_props(hist)
    return props["is_num"]


def sparse_bin_centers_x(hist):
    """Get x-axis bin centers of sparse histogram"""
    keys = sorted(hist.bins.keys())
    if hist.minBin is None or hist.maxBin is None:
        # number of bins is set to 1.
        centers = np.array([hist.origin + 0.5 * hist.binWidth])
    else:
        centers = np.array([hist.origin + (i + 0.5) * hist.binWidth for i in keys])

    values = [hist.bins[key] for key in keys]
    return centers, values


def get_bin_centers(hist):
    """Get bin centers or labels of histogram"""
    if isinstance(hist, histogrammar.Bin):  # Bin
        centers, values = hist.bin_centers(), hist.values
    elif isinstance(hist, histogrammar.SparselyBin):
        centers, values = sparse_bin_centers_x(hist)
    else:  # categorize
        centers, values = hist.bin_labels(), hist.values
    return centers, values


def split_hist_along_first_dimension(
    hist,
    xname="x",
    yname="y",
    short_keys=True,
    convert_time_index=True,
    filter_empty_split_hists=True,
):
    """Split (multi-dimensional) hist into sub-hists along x-axis

    Function to split a (multi-dimensional) histogram into sub-histograms
    along the first dimension encountered.

    :param str xname: name of x-axis. default is x.
    :param str yname: name of y-axis. default is y.
    :param bool short_keys: if false, use long descriptive dict keys.
    :param bool convert_time_index: if first dimension is a datetime, convert to pandas timestamp. default is true.
    :param bool filter_empty_split_hists: filter out empty sub-histograms after splitting. default is True.
    :returns: sorted dictionary of sub-histograms, with as keys the x-axis name and bin-number
    :rtype: SortedDict
    """
    hdict = {}

    # nothing special to do
    if hist.n_dim == 0:
        hdict["dummy"] = hist
        return hdict

    centers, values = get_bin_centers(hist)

    # MB 20191004: this happens rarely, but, in Histogrammar, if a multi-dim histogram contains *only*
    #   nans, overflows, or underflows for x, its sub-dimensional histograms (y, z, etc) do not get filled
    #   and/or are created. For sparselybin histograms this screws up the event-count, and evaluation of n-dim and
    #   datatype, so that the comparison of split-histograms along the x-axis gives inconsistent histograms.
    #   In this step we filter out any such empty sub-histograms, to ensure that
    #   all left-over sub-histograms are consistent with each other.
    if filter_empty_split_hists:
        centers, values = _filter_empty_split_hists(centers, values)

    for name, val in zip(centers, values):
        name = _edit_name(hist, name, xname, yname, convert_time_index, short_keys)
        hdict[name] = val

    return hdict


def _filter_empty_split_hists(centers, values):
    """Filter empty split histograms from input centers and values

    :param list centers: input center values list
    :param list values: input values list
    :return: filtered centers and values lists
    """
    cc = []
    vv = []
    for c, v in zip(centers, values):
        # ignore nan, overflow and underflow counters in total event count
        entries = sum_entries(v, default=False)
        if entries > 0:
            cc.append(c)
            vv.append(v)
    return cc, vv


def _edit_name(hist, axis_name, xname, yname, convert_time_index, short_keys):
    if convert_time_index and is_timestamp(hist):
        axis_name = pd.Timestamp(axis_name)
    if not short_keys:
        axis_name = f"{xname}={axis_name}"
        if hist.n_dim >= 2:
            axis_name = f"{yname}[{axis_name}]"
    return axis_name
