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
from scipy.stats import norm, pearsonr

from ...analysis.apply_func import ApplyFunc
from ...analysis.functions import (
    expand_norm_hist_mean_cov,
    expanding_hist,
    hist_sum,
    normalized_hist_mean_cov,
    relative_chi_squared,
    roll_norm_hist_mean_cov,
    rolling_hist,
)
from ...analysis.hist_numpy import (
    check_similar_hists,
    get_consistent_numpy_1dhists,
    get_consistent_numpy_2dgrids,
    get_consistent_numpy_entries,
)
from ...base import Pipeline
from ...hist.hist_utils import COMMON_HIST_TYPES, is_numeric
from ...stats.numpy import googl_test, ks_prob, ks_test, uu_chi2


def hist_compare(row, hist_name1="", hist_name2="", max_res_bound=7.0):
    """Function to compare two histograms

    Apply statistical tests to compare two input histograms, such as:
    Chi2, KS, Pearson, max probability difference.
    For categorical histograms, also check for unknown labels.

    :param pd.Series row: row to apply compare function to
    :param str hist_name1: name of histogram one to compare
    :param str hist_name2: name of histogram two to compare
    :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
        Default is 7.0.
    :return: pandas Series with popular comparison metrics.
    """
    x = pd.Series()
    x["ks"] = np.nan
    x["ks_zscore"] = np.nan
    x["ks_pvalue"] = np.nan
    x["pearson"] = np.nan
    x["chi2"] = np.nan
    x["chi2_norm"] = np.nan
    x["chi2_zscore"] = np.nan
    x["chi2_pvalue"] = np.nan
    x["chi2_max_residual"] = np.nan
    x["chi2_spike_count"] = np.nan
    x["max_prob_diff"] = np.nan
    unknown_labels = np.nan
    x["unknown_labels"] = unknown_labels

    # basic name checks
    cols = row.index.to_list()
    if len(hist_name1) == 0 or len(hist_name2) == 0 and len(cols) == 2:
        hist_name1 = cols[0]
        hist_name2 = cols[1]
    if not all([name in cols for name in [hist_name1, hist_name2]]):
        raise ValueError("Need to provide two histogram column names.")

    # basic histogram checks
    hist1 = row[hist_name1]
    hist2 = row[hist_name2]
    if not all([isinstance(hist, COMMON_HIST_TYPES) for hist in [hist1, hist2]]):
        return x
    if not check_similar_hists([hist1, hist2]):
        return x

    # compare
    is_num = is_numeric(hist1)
    if hist1.n_dim == 1:
        if is_num:
            numpy_1dhists = get_consistent_numpy_1dhists([hist1, hist2])
            entries_list = [nphist[0] for nphist in numpy_1dhists]
            # KS-test only properly defined for (ordered) 1D interval variables
            ks_testscore = ks_test(*entries_list)
            x["ks"] = ks_testscore
            ks_pvalue = ks_prob(ks_testscore)
            x["ks_pvalue"] = ks_pvalue
            x["ks_zscore"] = -norm.ppf(ks_pvalue)
        else:  # categorical
            entries_list = get_consistent_numpy_entries([hist1, hist2])
            # check consistency of bin_labels
            labels1 = hist1.bin_labels()
            labels2 = hist2.bin_labels()
            subset = set(labels1) <= set(labels2)
            unknown_labels = int(not subset)
    elif hist1.n_dim == 2:
        numpy_2dgrids = get_consistent_numpy_2dgrids([hist1, hist2])
        entries_list = [entry.flatten() for entry in numpy_2dgrids]

    # calculate pearson coefficient
    pearson, pvalue = (np.nan, np.nan)
    if len(entries_list[0]) >= 2:
        same0 = all(entries_list[0] == entries_list[0][0])
        same1 = all(entries_list[1] == entries_list[1][0])
        if not same0 and not same1:
            # this avoids std==0, and thereby avoid runtime warnings
            pearson, pvalue = pearsonr(*entries_list)

    chi2, chi2_norm, zscore, pvalue, res = uu_chi2(*entries_list)

    x["pearson"] = pearson
    x["chi2"] = chi2
    x["chi2_norm"] = chi2_norm
    x["chi2_zscore"] = zscore
    x["chi2_pvalue"] = pvalue
    x["chi2_max_residual"] = max(list(map(abs, res)))
    x["chi2_spike_count"] = sum(abs(r) > max_res_bound for r in res)
    x["max_prob_diff"] = googl_test(*entries_list)
    x["unknown_labels"] = unknown_labels
    return x


class HistComparer(Pipeline):
    """Base pipeline to compare histogram to previous rolling histograms"""

    def __init__(
        self,
        func_hist_collector,
        read_key,
        store_key,
        assign_to_key=None,
        hist_col="histogram",
        suffix="comp",
        max_res_bound=7.0,
        *args,
        **kwargs,
    ):
        """Initialize an instance of RollingHistComparer.

        :param func_hist_collector: histogram collection function
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str assign_to_key: key of the input data to assign function applied-output to. (optional)
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'roll' -> column = 'histogram_roll'
        :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
                                    Default is 7.0.
        :param args: (tuple, optional): residual args passed on to func_mean and func_std
        :param kwargs: (dict, optional): residual kwargs passed on to func_mean and func_std
        """
        super().__init__(modules=[])

        if assign_to_key is None:
            assign_to_key = read_key

        # make reference histogram(s)
        hist_collector = ApplyFunc(apply_to_key=read_key, assign_to_key=assign_to_key)
        hist_collector.add_apply_func(
            func=func_hist_collector, entire=True, suffix=suffix, *args, **kwargs
        )
        # do histogram comparison
        hist_comparer = ApplyFunc(
            apply_to_key=assign_to_key,
            assign_to_key=store_key,
            apply_funcs=[
                {
                    "func": hist_compare,
                    "hist_name1": hist_col,
                    "hist_name2": hist_col + "_" + suffix,
                    "prefix": suffix,
                    "axis": 1,
                    "max_res_bound": max_res_bound,
                }
            ],
        )
        self.modules = [hist_collector, hist_comparer]


class RollingHistComparer(HistComparer):
    """Compare histogram to previous rolling histograms"""

    def __init__(
        self,
        read_key,
        store_key,
        window,
        shift=1,
        hist_col="histogram",
        suffix="roll",
        max_res_bound=7.0,
    ):
        """Initialize an instance of RollingHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int window: size of rolling window
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'roll' -> column = 'histogram_roll'
        :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
                                    Default is 7.0.
        """
        super().__init__(
            rolling_hist,
            read_key,
            store_key,
            read_key,
            hist_col,
            suffix,
            max_res_bound,
            window=window,
            shift=shift,
            hist_name=hist_col,
        )
        self.read_key = read_key
        self.window = window

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with rolling sum of {self.window} previous histogram(s).'
        )
        return super().transform(datastore)


class PreviousHistComparer(RollingHistComparer):
    """Compare histogram to previous histograms"""

    def __init__(
        self,
        read_key,
        store_key,
        hist_col="histogram",
        suffix="prev1",
        max_res_bound=7.0,
    ):
        """Initialize an instance of PreviousHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'prev' -> column = 'histogram_prev'
        :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
                                    Default is 7.0.
        """
        super().__init__(
            read_key,
            store_key,
            window=1,
            shift=1,
            hist_col=hist_col,
            suffix=suffix,
            max_res_bound=max_res_bound,
        )


class ExpandingHistComparer(HistComparer):
    """Compare histogram to previous expanding histograms"""

    def __init__(
        self,
        read_key,
        store_key,
        shift=1,
        hist_col="histogram",
        suffix="expanding",
        max_res_bound=7.0,
    ):
        """Initialize an instance of ExpandingHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'expanding' -> column = 'histogram_expanding'
        :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
                                    Default is 7.0.
        """
        super().__init__(
            expanding_hist,
            read_key,
            store_key,
            read_key,
            hist_col,
            suffix,
            max_res_bound,
            shift=shift,
            hist_name=hist_col,
        )
        self.read_key = read_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with expanding sum of past histograms.'
        )
        return super().transform(datastore)


class ReferenceHistComparer(HistComparer):
    """Compare histogram to reference histograms"""

    def __init__(
        self,
        reference_key,
        assign_to_key,
        store_key,
        hist_col="histogram",
        suffix="ref",
        max_res_bound=7.0,
    ):
        """Initialize an instance of ReferenceHistComparer.

        :param str reference_key: key of input data to read from data store
        :param str assign_to_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'ref' -> column = 'histogram_ref'
        :param float max_res_bound: count number of normalized residuals with (absolute) value greater than X.
                                    Default is 7.0.
        """
        super().__init__(
            hist_sum,
            reference_key,
            store_key,
            assign_to_key,
            hist_col,
            suffix,
            max_res_bound,
            metrics=[hist_col],
        )
        self.reference_key = reference_key
        self.assign_to_key = assign_to_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.assign_to_key}" with reference "{self.reference_key}"'
        )
        return super().transform(datastore)


class NormHistComparer(Pipeline):
    """Base pipeline to compare histogram to normalized histograms"""

    def __init__(
        self,
        func_hist_collector,
        read_key,
        store_key,
        assign_to_key=None,
        hist_col="histogram",
        *args,
        **kwargs,
    ):
        """Initialize an instance of NormHistComparer.

        :param func_hist_collector: histogram collection function
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str assign_to_key: key of the input data to assign function applied-output to. (optional)
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param args: (tuple, optional): residual args passed on to func_hist_collector
        :param kwargs: (dict, optional): residual kwargs passed on to func_hist_collector
        """
        super().__init__(modules=[])

        if assign_to_key is None:
            assign_to_key = read_key

        # make reference histogram(s)
        hist_collector = ApplyFunc(apply_to_key=read_key, assign_to_key=assign_to_key)
        hist_collector.add_apply_func(
            func=func_hist_collector, hist_name=hist_col, suffix="", *args, **kwargs
        )

        # do histogram comparison
        hist_comparer = ApplyFunc(
            apply_to_key=assign_to_key,
            assign_to_key=store_key,
            apply_funcs=[
                {
                    "func": relative_chi_squared,
                    "hist_name": hist_col,
                    "suffix": "",
                    "axis": 1,
                }
            ],
        )

        self.modules = [hist_collector, hist_comparer]


class RollingNormHistComparer(NormHistComparer):
    """Compare histogram to previous rolling normalized histograms"""

    def __init__(self, read_key, store_key, window, shift=1, hist_col="histogram"):
        """Initialize an instance of RollingNormHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int window: size of rolling window
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        """
        if window < 2:
            raise ValueError("Need window size of 2 or greater.")
        super().__init__(
            roll_norm_hist_mean_cov,
            read_key,
            store_key,
            read_key,
            hist_col,
            window=window,
            shift=shift,
            entire=True,
        )
        self.read_key = read_key
        self.window = window

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with relative mean of {self.window} previous histogram(s).'
        )
        return super().transform(datastore)


class ExpandingNormHistComparer(NormHistComparer):
    """Compare histogram to previous expanding normalized histograms"""

    def __init__(self, read_key, store_key, shift=1, hist_col="histogram"):
        """Initialize an instance of ExpandingNormHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        """
        super().__init__(
            expand_norm_hist_mean_cov,
            read_key,
            store_key,
            read_key,
            hist_col,
            shift=shift,
            entire=True,
        )
        self.read_key = read_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with normalized mean of expanding past histograms.'
        )
        return super().transform(datastore)


class ReferenceNormHistComparer(NormHistComparer):
    """Compare histogram to reference normalized histograms"""

    def __init__(self, reference_key, assign_to_key, store_key, hist_col="histogram"):
        """Initialize an instance of ReferenceNormHistComparer.

        :param str reference_key: key of input data to read from data store
        :param str assign_to_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        """
        super().__init__(
            normalized_hist_mean_cov, reference_key, store_key, assign_to_key, hist_col
        )
        self.reference_key = reference_key
        self.assign_to_key = assign_to_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.assign_to_key}" with normalized reference "{self.reference_key}"'
        )
        return super().transform(datastore)
