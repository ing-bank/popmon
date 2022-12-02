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
from typing import Any, Callable, Dict, Optional

import numpy as np
import pandas as pd

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
    get_consistent_numpy_entries,
    get_consistent_numpy_ndgrids,
)
from ...base import Pipeline
from ...hist.hist_utils import COMMON_HIST_TYPES, is_numeric


def hist_compare(row, hist_name1="", hist_name2=""):
    """Function to compare two histograms

    Apply statistical tests to compare two input histograms, such as:
    Chi2, KS, Pearson, max probability difference.
    For categorical histograms, also check for unknown labels.

    :param pd.Series row: row to apply compare function to
    :param str hist_name1: name of histogram one to compare
    :param str hist_name2: name of histogram two to compare
    :return: pandas Series with popular comparison metrics.
    """
    from popmon.analysis.comparison import Comparisons

    x = {key: np.nan for key in Comparisons.get_keys()}

    # basic name checks
    cols = row.index.to_list()
    if len(hist_name1) == 0 or len(hist_name2) == 0 and len(cols) == 2:
        hist_name1 = cols[0]
        hist_name2 = cols[1]
    if not all(name in cols for name in [hist_name1, hist_name2]):
        raise ValueError("Need to provide two histogram column names.")

    # basic histogram checks
    hist1 = row[hist_name1]
    hist2 = row[hist_name2]
    if not all(
        isinstance(hist, COMMON_HIST_TYPES) for hist in [hist1, hist2]
    ) or not check_similar_hists([hist1, hist2]):
        return pd.Series(x)

    # compare
    if hist1.n_dim == 1:
        entries_list = get_consistent_numpy_entries([hist1, hist2])
        if is_numeric(hist1):
            htype = "num"
            args = entries_list
        else:
            htype = "cat"
            args = [hist1, hist2]

        x.update(Comparisons.run(args, dim=1, htype=htype))
        x.update(Comparisons.run(entries_list, dim=1, htype="all"))
    else:
        numpy_ndgrids = get_consistent_numpy_ndgrids([hist1, hist2], dim=hist1.n_dim)
        entries_list = [entry.flatten() for entry in numpy_ndgrids]

        x.update(Comparisons.run(entries_list, dim=(2,), htype="all"))

    x.update(Comparisons.run(entries_list, dim=-1, htype="all"))

    if len(set(x.keys()) - set(Comparisons.get_keys())) > 0:
        raise ValueError("Could not compute full comparison")

    return pd.Series(x)


class GenericHistComparer(Pipeline):
    def __init__(
        self,
        store_key: str,
        hist_col: str,
        prefix: str,
        assign_to_key: Optional[str] = None,
        left_read_key: Optional[str] = None,
        right_read_key: Optional[str] = None,
        func_left: Optional[Callable] = None,
        func_right: Optional[Callable] = None,
        suffix1: Optional[str] = None,
        suffix2: Optional[str] = None,
        left_kwargs: Optional[Dict[Any, Any]] = None,
        right_kwargs: Optional[Dict[Any, Any]] = None,
        *args,
        **kwargs,
    ):
        # TODO: add reference type to datastore (along the lines of datastore["references"] =
        #  {"ref": "Self Reference", "prev1": Rolling Reference (window=1, shift=1)"})

        if assign_to_key is None:
            raise ValueError("Ambiguous assign to key")
        #     assign_to_key = read_key

        if left_kwargs is None:
            left_kwargs = {}

        if right_kwargs is None:
            right_kwargs = {}

        modules = []

        # make left reference histograms
        hist_name1 = hist_col
        if func_left is not None and suffix1 is not None:
            hist_name1 = f"{hist_name1}_{suffix1}"
            hist_collector1 = ApplyFunc(
                apply_to_key=left_read_key,
                assign_to_key=assign_to_key,
            )
            hist_collector1.add_apply_func(
                func=func_left,
                entire=True,
                suffix=suffix1,
                hist_name=hist_col,
                **left_kwargs,
            )
            modules.append(hist_collector1)

        # right left reference histograms
        hist_name2 = hist_col
        if func_right is not None and suffix2 is not None:
            hist_name2 = f"{hist_name2}_{suffix2}"
            hist_collector2 = ApplyFunc(
                apply_to_key=right_read_key,
                assign_to_key=assign_to_key,
            )
            hist_collector2.add_apply_func(
                func=func_right,
                entire=True,
                suffix=suffix2,
                hist_name=hist_col,
                **right_kwargs,
            )
            modules.append(hist_collector2)

        # do histogram comparison
        hist_comparer = ApplyFunc(
            apply_to_key=assign_to_key,
            assign_to_key=store_key,
            apply_funcs=[
                {
                    "func": hist_compare,
                    "hist_name1": hist_name1,
                    "hist_name2": hist_name2,
                    "prefix": prefix,
                    "axis": 1,
                }
            ],
        )
        modules.append(hist_comparer)

        super().__init__(modules)


class HistComparer(GenericHistComparer):
    """Base pipeline to compare histogram to previous rolling histograms"""

    def __init__(
        self,
        func_hist_collector: Callable,
        read_key: str,
        store_key: str,
        hist_col: str,
        suffix: str,
        assign_to_key: Optional[str] = None,
        **kwargs,
    ):
        """Initialize an instance of RollingHistComparer.

        :param func_hist_collector: histogram collection function
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str assign_to_key: key of the input data to assign function applied-output to. (optional)
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'roll' -> column = 'histogram_roll'
        :param args: (tuple, optional): residual args passed on to func_mean and func_std
        :param kwargs: (dict, optional): residual kwargs passed on to func_mean and func_std
        """
        super().__init__(
            store_key,
            right_read_key=read_key,
            hist_col=hist_col,
            assign_to_key=assign_to_key,
            func_right=func_hist_collector,
            right_kwargs=kwargs,
            suffix2=suffix,
            prefix=suffix,
        )


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
    ):
        """Initialize an instance of RollingHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int window: size of rolling window
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'roll' -> column = 'histogram_roll'
        """
        super().__init__(
            rolling_hist,
            read_key,
            store_key,
            assign_to_key=read_key,
            hist_col=hist_col,
            suffix=suffix,
            window=window,
            shift=shift,
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
    ):
        """Initialize an instance of PreviousHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'prev' -> column = 'histogram_prev'
        """
        super().__init__(
            read_key,
            store_key,
            window=1,
            shift=1,
            hist_col=hist_col,
            suffix=suffix,
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
    ):
        """Initialize an instance of ExpandingHistComparer.

        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param int shift: shift of rolling window. default is 1.
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'expanding' -> column = 'histogram_expanding'
        """
        super().__init__(
            expanding_hist,
            read_key,
            store_key,
            assign_to_key=read_key,
            hist_col=hist_col,
            suffix=suffix,
            shift=shift,
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
    ):
        """Initialize an instance of ReferenceHistComparer.

        :param str reference_key: key of input data to read from data store
        :param str assign_to_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str hist_col: column/key in input df/dict that contains the histogram. default is 'histogram'
        :param str suffix: column/key of rolling histogram. default is 'ref' -> column = 'histogram_ref'
        """
        super().__init__(
            hist_sum,
            reference_key,
            store_key,
            assign_to_key=assign_to_key,
            hist_col=hist_col,
            suffix=suffix,
            metrics=[hist_col],
        )
        self.reference_key = reference_key
        self.assign_to_key = assign_to_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.assign_to_key}" with reference "{self.reference_key}"'
        )
        return super().transform(datastore)


class RollingInputFixedReference(GenericHistComparer):
    def __init__(
        self,
        read_key,
        reference_key,
        store_key,
        assign_to_key=None,
        window=1,
        shift=1,
        hist_col="histogram",
        suffix1="roll",
        suffix2="ref",
        prefix="rollref",
    ):
        super().__init__(
            read_key,  # left read key
            store_key,
            # right_read_key = reference_key,
            hist_col=hist_col,
            assign_to_key=assign_to_key,
        )


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

        super().__init__(modules=[hist_collector, hist_comparer])


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
