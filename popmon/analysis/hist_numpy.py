# Copyright (c) 2022 ING Wholesale Banking Advanced Analytics
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


import warnings

import histogrammar
import numpy as np
from histogrammar.util import get_hist_props

from ..hist.hist_utils import get_bin_centers, is_numeric
from ..stats.numpy import quantile

used_hist_types = (histogrammar.Bin, histogrammar.SparselyBin, histogrammar.Categorize)


def prepare_ndgrid(hist, n_dim):
    """Get lists of all unique combinations of keys

    Used as input by get_ndgrid(hist).

    :param hist: input histogrammar histogram
    :return: n sorted lists of keys
    """

    if hist.n_dim < n_dim:
        warnings.warn(
            f"Input histogram only has {hist.n_dim} dimensions (<{n_dim}). Returning empty lists."
        )
        return [[] for _ in range(n_dim)]

    def get_hist_keys(h):
        if hasattr(h, "bins"):
            return set(h.bins.keys())
        elif hasattr(h, "values"):
            return set(range(len(h.values)))
        else:
            raise TypeError()

    # SparselyBin or Categorize
    def keys_recursive(hist, hist_keys, idx):
        hist_keys[idx] |= get_hist_keys(hist)
        if (idx + 1) < len(hist_keys):
            if hasattr(hist, "bins"):
                for h in hist.bins.values():
                    hist_keys = keys_recursive(h, hist_keys, idx + 1)
            elif hasattr(hist, "values"):
                for h in hist.values:
                    hist_keys = keys_recursive(h, hist_keys, idx + 1)
            else:
                raise TypeError()
        return hist_keys

    keys = [set() for _ in range(n_dim)]
    keys = keys_recursive(hist, keys, 0)
    keys = [sorted(k) for k in keys]
    return keys


def prepare_2dgrid(hist):
    """Get lists of all unique x and y keys

    Used as input by get_2dgrid(hist).

    :param hist: input histogrammar histogram
    :return: two sorted lists of unique x and y keys
    """
    return prepare_ndgrid(hist, n_dim=2)


def set_ndgrid(hist, keys, n_dim):
    """Set n-d grid of first n dimensions of input histogram

    Used as input by get_ndgrid(hist).

    :param hist: input histogrammar histogram
    :param list keys: list with unique keys per dim
    :return: filled nd numpy grid
    """
    shape = [len(k) for k in reversed(keys)]
    grid = np.zeros(shape)

    if hist.n_dim < n_dim:
        warnings.warn(
            f"Input histogram only has {hist.n_dim} dimensions (<{n_dim}). Returning empty grid."
        )
        return grid

    def flatten(histogram, keys, grid, dim=0, prefix=None):
        if prefix is None:
            prefix = []

        if len(keys) == len(prefix):
            grid[tuple(prefix)] = histogram.entries
        else:
            if hasattr(histogram, "bins"):
                for k, h in histogram.bins.items():
                    if k not in keys[dim]:
                        continue
                    i = keys[dim].index(k)
                    flatten(h, keys, grid, dim + 1, [i] + prefix)
            elif hasattr(histogram, "values"):
                for i, h in enumerate(histogram.values):
                    flatten(h, keys, grid, dim + 1, [i] + prefix)
            else:
                raise TypeError()

    flatten(hist, keys, grid)
    return grid


def set_2dgrid(hist, keys):
    """Set 2d grid of first two dimenstions of input histogram

    Used as input by get_2dgrid(hist).

    :param hist: input histogrammar histogram
    :param list xkeys: list with unique x keys
    :param list ykeys: list with unique y keys
    :return: filled 2d numpy grid
    """
    return set_ndgrid(hist, keys, n_dim=2)


def get_ndgrid(hist, get_bin_labels=False, n_dim=2):
    """Get filled n-d grid of first n dimensions of input histogram

    :param hist: input histogrammar histogram
    :return: grid of first n dimenstions of input histogram
    """
    if hist.n_dim < n_dim:
        warnings.warn(
            f"Input histogram only has {hist.n_dim} dimensions (<{n_dim}). Returning empty grid."
        )
        return np.zeros(tuple([0] * n_dim))

    keys = prepare_ndgrid(hist, n_dim)
    grid = set_ndgrid(hist, keys, n_dim)

    if get_bin_labels:
        return grid, keys

    return grid


def get_2dgrid(hist, get_bin_labels=False):
    """Get filled x,y grid of first two dimensions of input histogram

    :param hist: input histogrammar histogram
    :return: x,y grid of first two dimenstions of input histogram
    """
    return get_ndgrid(hist, get_bin_labels, n_dim=2)


def get_consistent_numpy_ndgrids(hist_list=[], get_bin_labels=False, dim=3):
    """Get list of consistent x,y grids of first n dimensions of (sparse) input histograms

    :param list hist_list: list of input histogrammar histograms
    :param bool get_bin_labels: if true, return x-keys and y-keys describing binnings of 2d-grid.
    :param int dim: number of dimension (>= 3)
    :return: list of consistent x,y grids of first two dimensions of each input histogram in list
    """
    # --- basic checks
    if len(hist_list) == 0:
        raise ValueError("Input histogram list has zero length.")
    if hist_list[0].n_dim < dim:
        raise ValueError(
            f"Input histogram only has {hist_list[0].n_dim} dimensions (<{dim}). Cannot compute {dim}d-grid."
        )
    assert_similar_hists(hist_list)

    keys = [set() for _ in range(dim)]
    for hist in hist_list:
        hist_keys = prepare_ndgrid(hist, n_dim=dim)
        for i, h_keys in enumerate(hist_keys):
            keys[i] |= set(h_keys)
    keys = [sorted(k) for k in keys]

    gridnd_list = [set_ndgrid(hist, keys, n_dim=dim) for hist in hist_list]

    if get_bin_labels:
        return gridnd_list, keys

    return gridnd_list


def get_consistent_numpy_2dgrids(hist_list=[], get_bin_labels=False):
    """Get list of consistent x,y grids of first two dimensions of (sparse) input histograms

    :param list hist_list: list of input histogrammar histograms
    :param bool get_bin_labels: if true, return x-keys and y-keys describing binnings of 2d-grid.
    :return: list of consistent x,y grids of first two dimensions of each input histogram in list
    """
    return get_consistent_numpy_ndgrids(hist_list, get_bin_labels, dim=2)


def get_consistent_numpy_1dhists(hist_list, get_bin_labels=False, crop_range=False):
    """Get list of consistent numpy hists for list of sparse (or bin) input histograms

    Works for sparse and bin histograms.
    Note: for sparse histograms, all potential bins between low and high are picked up (also unfilled).

    Note: a numpy histogram is a union of lists of bin_edges and number of entries
    This gives the full range of bin_centers, including zeros, which is not robust against (extreme) outliers.
    Ideally, use this for plotting of multiple histograms only.

    :param list hist_list: list of input histogram objects
    :param bool get_bin_labels: return bin labels as well, default is false.
    :param bool crop_range: return a trimmed version of the histogram, between 5-95% quantiles.
    :return: list of consistent 1d numpy hists for list of sparse input histograms
    """
    # --- basic checks
    if len(hist_list) == 0:
        raise ValueError("Input histogram list has zero length.")
    assert_similar_hists(hist_list)

    low_arr = [hist.low for hist in hist_list if hist.low is not None]
    high_arr = [hist.high for hist in hist_list if hist.high is not None]

    low = min(low_arr) if len(low_arr) > 0 else None
    high = max(high_arr) if len(high_arr) > 0 else None
    # low == None and/or high == None can only happen when all input hists are empty.

    if crop_range:
        # crop_range option crops a histogram to reasonable range, e.g. for plotting, giving nice plots.
        # in particular this protects against outliers that distort the view on the core part of the distribution
        # range is quantiles 5-95% + 5% on both sides
        q05_arr = []
        q95_arr = []
        for hist in hist_list:
            bin_centers, values = get_bin_centers(hist)
            bin_entries = np.array([v.entries for v in values])
            qs = quantile(bin_centers, [0.05, 0.95], bin_entries)
            q05_arr.append(qs[0])
            q95_arr.append(qs[1])
        q05 = min(q05_arr) if len(q05_arr) > 0 else np.nan
        q95 = max(q95_arr) if len(q95_arr) > 0 else np.nan
        delta = q95 - q05
        var_min = q05 - (0.06 / 0.9) * delta
        var_max = q95 + (0.06 / 0.9) * delta
        if 0.0 < var_min < 0.2 * delta:
            var_min = 0.0
        elif -0.2 * delta < var_max < 0.0:
            var_max = 0.0
        if not np.isnan(var_min) and low is not None and var_min > low:
            low = var_min
        if not np.isnan(var_max) and high is not None and var_max < high:
            high = var_max

    # if one of the input histograms is sparse and empty, copy the bin-edges and bin-centers
    # from a filled histogram, and use empty bin-entries array
    # MB 20220601: note this gives the full range of bin_centers, which is not robust against (extreme) outliers
    #              get_consistent_numpy_entries() ignores all empty bins.
    bin_edges = [0.0, 1.0]
    bin_centers = [0.5]
    null_entries = [0.0]
    if low is not None and high is not None:
        for hist in hist_list:
            if hist.low is not None and hist.high is not None:
                bin_edges = hist.bin_edges(low, high)
                bin_centers = hist.bin_centers(low, high)
                null_entries = np.zeros(len(bin_centers))
                break

    nphist_list = []
    for hist in hist_list:
        bin_entries = (
            null_entries
            if (hist.low is None and hist.high is None)
            else hist.bin_entries(low, high)
        )
        nphist_list.append((bin_entries, bin_edges))

    if get_bin_labels:
        return nphist_list, bin_centers
    else:
        return nphist_list


def get_consistent_numpy_entries(hist_list, get_bin_labels=False):
    """Get list of consistent numpy bin_entries for list of 1d input histograms

    Works for categorize, sparse and bin histograms.
    Note: for sparse histograms, *only* the filled bins are picked up.
    (this is not the case when calling get_consistent_numpy_1dhists(), which takes all bins b/n low and high.)

    :param list hist_list: list of input histogrammar histograms
    :return: list of consistent 1d numpy arrays with bin_entries for list of input histograms
    """
    # --- basic checks
    if len(hist_list) == 0:
        raise ValueError("Input histogram list has zero length.")
    assert_similar_hists(hist_list)

    # datatype check
    v0 = is_numeric(hist_list[0])
    constant = all(is_numeric(hist) == v0 for hist in hist_list[1:])
    all_num = constant and v0
    all_cat = constant and not v0
    if not (all_num or all_cat):
        raise TypeError(
            "Input histograms are mixture of Bin/SparselyBin and Categorize types."
        )

    # union of all labels encountered
    labels = set()
    for hist in hist_list:
        bin_labels = get_bin_centers(hist)[0]
        labels = labels.union(bin_labels)
    labels = sorted(labels)

    # collect list of consistent bin_entries
    if all_num:
        kwargs = {"xvalues": labels}
    else:
        # PATCH: deal with boolean labels, which get bin_labels() returns as strings
        cat_labels = labels
        props = get_hist_props(hist_list[0])
        if props["is_bool"]:
            cat_labels = [lab == "True" for lab in cat_labels]
        kwargs = {"labels": cat_labels}

    entries_list = [hist.bin_entries(**kwargs) for hist in hist_list]

    if get_bin_labels:
        return entries_list, labels
    else:
        return entries_list


def get_contentType(hist):
    """Get content type of bins of histogram

    :param hist: input histogram
    :return: string describing content type
    """
    if isinstance(hist, histogrammar.Count):
        return "Count"
    elif isinstance(hist, histogrammar.Bin):
        return "Bin"
    elif isinstance(hist, histogrammar.SparselyBin):
        return "SparselyBin"
    elif isinstance(hist, histogrammar.Categorize):
        return "Categorize"
    return "Count"


def check_similar_hists(hist_list, check_type=True, assert_type=used_hist_types):
    """Check consistent list of input histograms

    Check that type and dimension of all histograms in input list are the same.

    :param list hist_list: list of input histogram objects to check on consistency
    :param bool check_type: if true, also check type consistency of histograms (besides n-dim and datatype).
    :return: bool indicating if lists are similar
    """
    if len(hist_list) < 1:
        return True
    for hist in hist_list:
        if not isinstance(hist, assert_type):
            raise TypeError(
                "Input histogram type {htype} not of {htypes}.".format(
                    htype=type(hist), htypes=assert_type
                )
            )
    # perform similarity checks on:
    # - number of dimensions
    # - histogram type
    # - datatypes
    # - Bin attributes
    # - SparselyBin attributes
    # - all above for sub-histograms in case of n-dim > 1

    # Check generic attributes - filled histograms only
    n_d = [hist.n_dim for hist in hist_list]
    if not n_d.count(n_d[0]) == len(n_d):
        warnings.warn("Input histograms have inconsistent dimensions.")
        return False
    dts = [hist.datatype for hist in hist_list]
    if not dts.count(dts[0]) == len(dts):
        warnings.warn(f"Input histograms have inconsistent datatypes: {dts}")
        return False
    # Check generic attributes
    if check_type:
        # E.g. histogrammar.specialized.CategorizeHistogramMethods and
        # histogrammar.primitives.categorize.Categorize are both of type hg.Categorize
        # Make this consistent first.
        types = [get_contentType(hist) for hist in hist_list]
        if not types.count(types[0]) == len(types):
            warnings.warn(
                "Input histograms have inconsistent class types: {types}".format(
                    types=types
                )
            )
            return False

    # Check Bin attributes
    if isinstance(hist_list[0], histogrammar.Bin):
        nums = [hist.num for hist in hist_list]
        if not nums.count(nums[0]) == len(nums):
            warnings.warn(
                "Input Bin histograms have inconsistent num attributes: {types}".format(
                    types=nums
                )
            )
            return False
        lows = [hist.low for hist in hist_list]
        if not lows.count(lows[0]) == len(lows):
            warnings.warn(
                "Input Bin histograms have inconsistent low attributes: {types}".format(
                    types=lows
                )
            )
            return False
        highs = [hist.high for hist in hist_list]
        if not highs.count(highs[0]) == len(highs):
            warnings.warn(
                "Input histograms have inconsistent high attributes: {types}".format(
                    types=highs
                )
            )
            return False

    # Check SparselyBin attributes
    if isinstance(hist_list[0], histogrammar.SparselyBin):
        origins = [hist.origin for hist in hist_list]
        if not origins.count(origins[0]) == len(origins):
            warnings.warn(
                "Input SparselyBin histograms have inconsistent origin attributes: {types}".format(
                    types=origins
                )
            )
            return False
        bws = [hist.binWidth for hist in hist_list]
        if not bws.count(bws[0]) == len(bws):
            warnings.warn(
                "Input SparselyBin histograms have inconsistent binWidth attributes: {types}".format(
                    types=bws
                )
            )
            return False

    # Check sub-histogram attributes
    if n_d[0] > 1:
        sub_hist_list = []
        # Categorize and SparselyBin
        if hasattr(hist_list[0], "bins"):
            for hist in hist_list:
                kys = list(hist.bins.keys())
                if len(kys) > 0:
                    sub_hist_list.append(hist.bins[kys[0]])
        # Bin
        elif hasattr(hist_list[0], "values"):
            for hist in hist_list:
                if hist.num > 0:
                    sub_hist_list.append(hist.values[0])
        # iterate down
        if not check_similar_hists(sub_hist_list):
            return False

    return True


def assert_similar_hists(hist_list, check_type=True, assert_type=used_hist_types):
    """Assert consistent list of input histograms

    Assert that type and dimension of all histograms in input list are the same.

    :param list hist_list: list of input histogram objects to check on consistency
    :param bool assert_type: if true, also assert type consistency of histograms (besides n-dim and datatype).
    """
    similar = check_similar_hists(
        hist_list, check_type=check_type, assert_type=assert_type
    )
    if not similar:
        raise ValueError("Input histograms are not all similar.")


def check_same_hists(hist1, hist2):
    """Check if two hists are the same

    :param hist1: input histogram 1
    :param hist2: input histogram 2
    :return: boolean, true if two histograms are the same
    """
    same = check_similar_hists([hist1, hist2])
    same &= hist1.entries == hist2.entries
    same &= hist1.n_bins == hist2.n_bins
    same &= hist1.quantity.name == hist2.quantity.name
    return same
