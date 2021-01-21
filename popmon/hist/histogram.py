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


import numpy as np
import pandas as pd

from ..hist.patched_histogrammer import COMMON_HIST_TYPES, histogrammar

HG_FACTORY = histogrammar.Factory()


def sum_entries(hist_data, default=True):
    """Recursively get sum of entries of histogram

    Sometimes hist.entries gives zero as answer? This function always works though.

    :param hist_data: input histogrammar histogram
    :param bool default: if false, do not use default HG method for evaluating entries, but exclude nans, of, uf.
    :return: total sum of entries of histogram
    :rtype: int
    """
    if default:
        entries = hist_data.entries
        if entries > 0:
            return entries

    # double check number of entries, sometimes not well set
    sume = 0
    if hasattr(hist_data, "bins"):
        # loop over all counters and integrate over y (=j)
        for i in hist_data.bins:
            bi = hist_data.bins[i]
            sume += sum_entries(bi)
    elif hasattr(hist_data, "values"):
        # loop over all counters and integrate over y (=j)
        for i, bi in enumerate(hist_data.values):
            sume += sum_entries(bi)
    elif hasattr(hist_data, "entries"):
        # only count histogrammar.Count() objects
        sume += hist_data.entries
    return sume


def project_on_x(hist_data):
    """Project n-dim histogram onto x-axis

    :param hist_data: input histogrammar histogram
    :return: on x-axis projected histogram (1d)
    """
    # basic check: projecting on itself
    if hasattr(hist_data, "n_dim") and hist_data.n_dim <= 1:
        return hist_data
    # basic checks on contents
    if hasattr(hist_data, "bins"):
        if len(hist_data.bins) == 0:
            return hist_data
    elif hasattr(hist_data, "values"):
        if len(hist_data.values) == 0:
            return hist_data
    else:
        return hist_data

    # make empty clone
    # note: cannot do: h_x = hist.zero(), b/c it copies n-dim structure, which screws up hist.toJsonString()
    if isinstance(hist_data, histogrammar.Bin):
        h_x = histogrammar.Bin(
            num=hist_data.num,
            low=hist_data.low,
            high=hist_data.high,
            quantity=hist_data.quantity,
        )
    elif isinstance(hist_data, histogrammar.SparselyBin):
        h_x = histogrammar.SparselyBin(
            binWidth=hist_data.binWidth,
            origin=hist_data.origin,
            quantity=hist_data.quantity,
        )
    elif isinstance(hist_data, histogrammar.Categorize):
        h_x = histogrammar.Categorize(quantity=hist_data.quantity)
    else:
        raise RuntimeError("unknown historgram type. cannot get zero copy.")

    if hasattr(hist_data, "bins"):
        for key, bi in hist_data.bins.items():
            h_x.bins[key] = histogrammar.Count.ed(sum_entries(bi))
    elif hasattr(hist_data, "values"):
        for i, bi in enumerate(hist_data.values):
            h_x.values[i] = histogrammar.Count.ed(sum_entries(bi))

    return h_x


def sum_over_x(hist_data):
    """Integrate histogram over first dimension

    :param hist_data: input histogrammar histogram
    :return: integrated histogram
    """
    # basic check: nothing to do?
    if hasattr(hist_data, "n_dim") and hist_data.n_dim == 0:
        return hist_data
    if hasattr(hist_data, "n_dim") and hist_data.n_dim == 1:
        return histogrammar.Count.ed(sum_entries(hist_data))

    # n_dim >= 2 from now on
    # basic checks on contents
    if hasattr(hist_data, "bins"):
        if len(hist_data.bins) == 0:
            return hist_data
    elif hasattr(hist_data, "values"):
        if len(hist_data.values) == 0:
            return hist_data
    else:
        return hist_data

    # n_dim >= 2 and we have contents; here we sum over it.
    h_proj = None
    if hasattr(hist_data, "bins"):
        h_proj = list(hist_data.bins.values())[0].zero()
        # loop over all counters and integrate over x (=i)
        for bi in hist_data.bins.values():
            h_proj += bi
    elif hasattr(hist_data, "values"):
        h_proj = hist_data.values[0].zero()
        # loop over all counters and integrate
        for bi in hist_data.values:
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
        raise TypeError(
            "splitdict: {wt}, type should be a dictionary.".format(wt=type(splitdict))
        )
    if axis not in ["x", "y"]:
        raise ValueError(f"axis: {axis}, can only be x or y.")

    hdict = dict()

    for key, hxy in splitdict.items():
        h = project_on_x(hxy) if axis == "x" else sum_over_x(hxy)
        hdict[key] = h

    return hdict


class HistogramContainer:
    """Wrapper class around histogrammar histograms with several utility functions."""

    def __init__(self, hist_obj):
        """Initialization

        :param hist_obj: input histogrammar object. Can also be a corresponding json object or str.
        """
        self.hist = None
        if isinstance(hist_obj, HistogramContainer):
            self.hist = hist_obj.hist
        elif isinstance(hist_obj, COMMON_HIST_TYPES):
            self.hist = hist_obj
        elif isinstance(hist_obj, str):
            self.hist = HG_FACTORY.fromJsonString(hist_obj)
        elif isinstance(hist_obj, dict):
            self.hist = HG_FACTORY.fromJson(hist_obj)
        if self.hist is None:
            raise ValueError(
                "Please provide histogram or histogram container as input."
            )

        self.is_list = isinstance(self.hist.datatype, list)
        var_type = self.hist.datatype if not self.is_list else self.hist.datatype[0]
        self.npdtype = np.dtype(var_type)

        # determine data-type categories
        self.is_int = np.issubdtype(self.npdtype, np.integer)
        self.is_ts = np.issubdtype(self.npdtype, np.datetime64)
        self.is_num = self.is_ts or np.issubdtype(self.npdtype, np.number)
        self.n_dim = self.hist.n_dim
        self.entries = self.hist.entries

    def __repr__(self):
        return f"HistogramContainer(dtype={self.npdtype}, n_dims={self.n_dim})"

    def __str__(self):
        return repr(self)

    def _edit_name(self, axis_name, xname, yname, convert_time_index, short_keys):
        if convert_time_index and self.is_ts:
            axis_name = pd.Timestamp(axis_name)
        if not short_keys:
            axis_name = f"{xname}={axis_name}"
            if self.n_dim >= 2:
                axis_name = f"{yname}[{axis_name}]"
        return axis_name

    def sparse_bin_centers_x(self):
        """Get x-axis bin centers of sparse histogram"""
        keys = sorted(self.hist.bins.keys())
        if self.hist.minBin is None or self.hist.maxBin is None:
            # number of bins is set to 1.
            centers = np.array([self.hist.origin + 0.5 * self.hist.binWidth])
        else:
            centers = np.array(
                [self.hist.origin + (i + 0.5) * self.hist.binWidth for i in keys]
            )

        values = [self.hist.bins[key] for key in keys]
        return centers, values

    def get_bin_centers(self):
        """Get bin centers or labels of histogram"""
        if isinstance(self.hist, histogrammar.Bin):  # Bin
            centers, values = self.hist.bin_centers(), self.hist.values
        elif isinstance(self.hist, histogrammar.SparselyBin):
            centers, values = self.sparse_bin_centers_x()
        else:  # categorize
            centers, values = self.hist.bin_labels(), self.hist.values
        return centers, values

    def split_hist_along_first_dimension(
        self,
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
        hdict = dict()

        # nothing special to do
        if self.n_dim == 0:
            hdict["dummy"] = self.hist
            return hdict

        centers, values = self.get_bin_centers()

        # MB 20191004: this happens rarely, but, in Histogrammar, if a multi-dim histogram contains *only*
        #   nans, overflows, or underflows for x, its sub-dimensional histograms (y, z, etc) do not get filled
        #   and/or are created. For sparselybin histograms this screws up the event-count, and evaluation of n-dim and
        #   datatype, so that the comparison of split-histograms along the x-axis gives inconsistent histograms.
        #   In this step we filter out any such empty sub-histograms, to ensure that
        #   all left-over sub-histograms are consistent with each other.
        if filter_empty_split_hists:
            centers, values = self._filter_empty_split_hists(centers, values)

        for name, val in zip(centers, values):
            name = self._edit_name(name, xname, yname, convert_time_index, short_keys)
            hdict[name] = val

        return hdict

    def _filter_empty_split_hists(self, centers, values):
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


def get_hist_props(hist):
    """Get histogram datatype properties.

    :param hist: input histogram
    :returns dict: Column properties
    """
    hist = hist.hist if isinstance(hist, HistogramContainer) else hist

    var_type = (
        hist.datatype if not isinstance(hist.datatype, list) else hist.datatype[0]
    )
    npdtype = np.dtype(var_type)

    # determine data-type categories
    is_int = isinstance(npdtype.type(), np.integer)
    is_ts = isinstance(npdtype.type(), np.datetime64)
    is_num = is_ts or isinstance(npdtype.type(), np.number)
    is_bool = isinstance(npdtype.type(), np.bool_)

    return dict(
        dtype=npdtype, is_num=is_num, is_int=is_int, is_ts=is_ts, is_bool=is_bool
    )


def dumper(obj):
    """Utility function to convert objects to json

    From: https://stackoverflow.com/questions/3768895/how-to-make-a-class-json-serializable
    E.g. use to convert dict of histogrammar objects to json

    Use as:

    .. code-block:: python

        js = json.dumps(hists, default=dumper)
        with open(filename, 'w') as f:
            json.dump(hists, f, default=dumper)

    :param obj: input object
    :return: output json object
    """
    if hasattr(obj, "toJSON"):
        return obj.toJSON()
    elif hasattr(obj, "toJson"):
        return obj.toJson()
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    else:
        raise RuntimeError(f"Do not know how to serialize object type {type(obj)}")
