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


import warnings

import histogrammar
import numpy as np
from histogrammar.util import get_hist_props

from ..hist.hist_utils import is_numeric

used_hist_types = (histogrammar.Bin, histogrammar.SparselyBin, histogrammar.Categorize)


def prepare_2dgrid(hist):
    """Get lists of all unique x and y keys

    Used as input by get_2dgrid(hist).

    :param hist: input histogrammar histogram
    :return: two comma-separated lists of unique x and y keys
    """
    if hist.n_dim < 2:
        warnings.warn(
            "Input histogram only has {n} dimensions (<2). Returning empty lists.".format(
                n=hist.n_dim
            )
        )
        return [], []

    xkeys = set()
    ykeys = set()
    # SparselyBin or Categorize
    if hasattr(hist, "bins"):
        xkeys = xkeys.union(hist.bins.keys())
        for h in hist.bins.values():
            if hasattr(h, "bins"):
                ykeys = ykeys.union(h.bins.keys())
            elif hasattr(h, "values"):
                ykeys = ykeys.union(range(len(h.values)))
    # Bin
    elif hasattr(hist, "values"):
        xkeys = xkeys.union(range(len(hist.values)))
        for h in hist.values:
            if hasattr(h, "bins"):
                ykeys = ykeys.union(h.bins.keys())
            elif hasattr(h, "values"):
                ykeys = ykeys.union(range(len(h.values)))
    return sorted(xkeys), sorted(ykeys)


def set_2dgrid(hist, xkeys, ykeys):
    """Set 2d grid of first two dimenstions of input histogram

    Used as input by get_2dgrid(hist).

    :param hist: input histogrammar histogram
    :param list xkeys: list with unique x keys
    :param list ykeys: list with unique y keys
    :return: filled 2d numpy grid
    """
    grid = np.zeros((len(ykeys), len(xkeys)))

    if hist.n_dim < 2:
        warnings.warn(
            "Input histogram only has {n} dimensions (<2). Returning original grid.".format(
                n=hist.n_dim
            )
        )
        return grid

    # SparselyBin or Categorize
    if hasattr(hist, "bins"):
        for k, h in hist.bins.items():
            if k not in xkeys:
                continue
            i = xkeys.index(k)
            if hasattr(h, "bins"):
                for ll, g in h.bins.items():
                    if ll not in ykeys:
                        continue
                    j = ykeys.index(ll)
                    grid[j, i] = g.entries  # sum_entries(g)
            elif hasattr(h, "values"):
                for j, g in enumerate(h.values):
                    grid[j, i] = g.entries
    # Bin
    elif hasattr(hist, "values"):
        for i, h in enumerate(hist.values):
            if hasattr(h, "bins"):
                for ll, g in h.bins.items():
                    if ll not in ykeys:
                        continue
                    j = ykeys.index(ll)
                    grid[j, i] = g.entries
            elif hasattr(h, "values"):
                for j, g in enumerate(h.values):
                    grid[j, i] = g.entries
    return grid


def get_2dgrid(hist, get_bin_labels=False):
    """Get filled x,y grid of first two dimensions of input histogram

    :param hist: input histogrammar histogram
    :return: x,y grid of first two dimenstions of input histogram
    """
    import numpy as np

    if hist.n_dim < 2:
        warnings.warn(
            "Input histogram only has {n} dimensions (<2). Returning empty grid.".format(
                n=hist.n_dim
            )
        )
        return np.zeros((0, 0))

    xkeys, ykeys = prepare_2dgrid(hist)
    grid = set_2dgrid(hist, xkeys, ykeys)

    if get_bin_labels:
        return grid, xkeys, ykeys

    return grid


def get_consistent_numpy_2dgrids(hist_list=[], get_bin_labels=False):
    """Get list of consistent x,y grids of first two dimensions of (sparse) input histograms

    :param list hist_list: list of input histogrammar histograms
    :param bool get_bin_labels: if true, return x-keys and y-keys describing binnings of 2d-grid.
    :return: list of consistent x,y grids of first two dimensions of each input histogram in list
    """
    # --- basic checks
    if len(hist_list) == 0:
        raise ValueError("Input histogram list has zero length.")
    assert_similar_hists(hist_list)

    xkeys = set()
    ykeys = set()
    for hist in hist_list:
        if hist.n_dim < 2:
            raise ValueError(
                "Input histogram only has {n} dimensions (<2). Cannot compute 2d-grid.".format(
                    n=hist.n_dim
                )
            )
        x, y = prepare_2dgrid(hist)
        xkeys = xkeys.union(x)
        ykeys = ykeys.union(y)
    xkeys = sorted(xkeys)
    ykeys = sorted(ykeys)

    grid2d_list = []
    for hist in hist_list:
        grid2d_list.append(set_2dgrid(hist, xkeys, ykeys))

    if get_bin_labels:
        return grid2d_list, xkeys, ykeys

    return grid2d_list


def get_consistent_numpy_1dhists(hist_list, get_bin_labels=False):
    """Get list of consistent numpy hists for list of sparse input histograms

    Note: a numpy histogram is a union of lists of bin_edges and number of entries

    :param list hist_list: list of input histogram objects
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

    # if one of the input histograms is sparse and empty, copy the bin-edges and bin-centers
    # from a filled histogram, and use empty bin-entries array
    bin_edges = [0.0, 1.0]
    bin_centers = [0.5]
    null_entries = [0.0]
    if low is not None and high is not None:
        for hist in hist_list:
            if hist.low is not None and hist.high is not None:
                bin_edges = hist.bin_edges(low, high)
                bin_centers = hist.bin_centers(low, high)
                null_entries = [0] * len(bin_centers)
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

    :param list hist_list: list of input histogrammar histograms
    :return: list of consistent 1d numpy arrays with bin_entries for list of input histograms
    """
    # --- basic checks
    if len(hist_list) == 0:
        raise ValueError("Input histogram list has zero length.")
    assert_similar_hists(hist_list)

    # datatype check
    is_num_arr = []
    for hist in hist_list:
        is_num_arr.append(is_numeric(hist))
    all_num = all(is_num_arr)
    all_cat = not any(is_num_arr)
    if not (all_num or all_cat):
        raise TypeError(
            "Input histograms are mixture of Bin/SparselyBin and Categorize types."
        )

    # union of all labels encountered
    labels = set()
    for hist in hist_list:
        bin_labels = hist.bin_centers() if all_num else hist.bin_labels()
        labels = labels.union(bin_labels)
    labels = sorted(labels)

    # PATCH: deal with boolean labels, which get bin_labels() returns as strings
    cat_labels = labels
    props = get_hist_props(hist_list[0])
    if props["is_bool"]:
        cat_labels = [lab == "True" for lab in cat_labels]

    # collect list of consistent bin_entries
    entries_list = []
    for hist in hist_list:
        entries = (
            hist.bin_entries(xvalues=labels)
            if all_num
            else hist.bin_entries(labels=cat_labels)
        )
        entries_list.append(entries)

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
