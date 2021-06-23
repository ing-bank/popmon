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

from ...analysis.apply_func import ApplyFunc
from ...analysis.functions import (
    expanding_mean,
    expanding_std,
    pull,
    rolling_mean,
    rolling_std,
)
from ...base import Pipeline
from ...stats.numpy import mad


class PullCalculator(Pipeline):
    """Base module for pull calculation, based on mean and standard deviation calculation

    Steps as performed by ApplyFunc modules:

    - calculate standard deviation seen in a metric.
    - calculate mean seen in a metric.
    - calculate pull of a metric as: pull = (metric - mean) / std.

    The pull is then stored as a new column.
    """

    def __init__(
        self,
        func_mean,
        func_std,
        apply_to_key,
        assign_to_key=None,
        store_key=None,
        suffix_mean="_mean",
        suffix_std="_std",
        suffix_pull="_pull",
        features=None,
        *args,
        **kwargs,
    ):
        """Initialize an instance of HistComparer.

        :param str func_mean: applied-function to calculate mean of profiled statistics
        :param str func_std: applied-function to calculate std of profiled statistics
        :param str apply_to_key: key of the input data to apply funcs to.
        :param str assign_to_key: key of the input data to assign function applied-output to. (optional)
        :param str store_key: key of the output data to store in the datastore (optional)
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean. default is ``_mean``.
        :param str suffix_std: suffix of std. std column = metric + suffix_std. default is ``_std``.
        :param str suffix_pull: suffix of pull. pull column = metric + suffix_pull. default is ``_pull``.
        :param list features: list of features to calculate pull for. default is all.
        :param args: (tuple, optional): residual args passed on to func_mean and func_std
        :param kwargs: (dict, optional): residual kwargs passed on to func_mean and func_std
        """
        super().__init__(modules=[])

        if assign_to_key is None:
            assign_to_key = apply_to_key

        calc_mean_std = ApplyFunc(
            apply_to_key=apply_to_key, assign_to_key=assign_to_key, features=features
        )
        calc_mean_std.add_apply_func(
            func_std, suffix=suffix_std, entire=True, *args, **kwargs
        )
        calc_mean_std.add_apply_func(
            func_mean, suffix=suffix_mean, entire=True, *args, **kwargs
        )

        calc_pull = ApplyFunc(
            apply_to_key=assign_to_key, assign_to_key=store_key, features=features
        )
        calc_pull.add_apply_func(
            pull,
            suffix=suffix_pull,
            axis=1,
            suffix_mean=suffix_mean,
            suffix_std=suffix_std,
        )

        self.modules = [calc_mean_std, calc_pull]


class RollingPullCalculator(PullCalculator):
    """Pull calculation based on rolling mean and standard deviations"""

    def __init__(
        self,
        read_key,
        window,
        shift=1,
        features=None,
        store_key=None,
        suffix_mean="_roll_mean",
        suffix_std="_roll_std",
        suffix_pull="_roll_pull",
        *args,
        **kwargs,
    ):
        """Initialize an instance of HistComparer.

        :param str read_key: key of input data to read from data store
        :param int window: size of rolling window
        :param int shift: shift of the window, default is 1.
        :param list features: list of features to calculate pull for.
        :param str store_key: key of the output data to store in the datastore (optional)
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        :param str suffix_pull: suffix of pull. pull column = metric + suffix_pull
        :param args: (tuple, optional): residual args passed on to mean and std functions
        :param kwargs: (dict, optional): residual kwargs passed on to mean and std functions
        """
        kws = {"window": window, "shift": shift}
        kws.update(kwargs)
        super().__init__(
            rolling_mean,
            rolling_std,
            read_key,
            read_key,
            store_key,
            suffix_mean,
            suffix_std,
            suffix_pull,
            features,
            *args,
            **kws,
        )
        self.read_key = read_key
        self.window = window

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with rolling mean/std of {self.window} past values.'
        )
        return super().transform(datastore)


class ExpandingPullCalculator(PullCalculator):
    """Pull calculation based on expanding mean and standard deviations"""

    def __init__(
        self,
        read_key,
        shift=1,
        features=None,
        store_key=None,
        suffix_mean="_exp_mean",
        suffix_std="_exp_std",
        suffix_pull="_exp_pull",
        *args,
        **kwargs,
    ):
        """Initialize an instance of HistComparer.

        :param str read_key: key of input data to read from data store
        :param int shift: shift of the window, default is 1.
        :param list features: list of features to calculate pull for. (optional)
        :param str store_key: key of the output data to store in the datastore (optional)
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        :param str suffix_pull: suffix of pull. pull column = metric + suffix_pull
        :param args: (tuple, optional): residual args passed on to mean and std functions
        :param kwargs: (dict, optional): residual kwargs passed on to mean and std functions
        """
        kws = {"shift": shift}
        kws.update(kwargs)
        super().__init__(
            expanding_mean,
            expanding_std,
            read_key,
            read_key,
            store_key,
            suffix_mean,
            suffix_std,
            suffix_pull,
            features,
            *args,
            **kws,
        )
        self.read_key = read_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.read_key}" with expanding mean/std of past values.'
        )
        return super().transform(datastore)


class ReferencePullCalculator(PullCalculator):
    """Pull calculation based on reference mean and standard deviations"""

    def __init__(
        self,
        reference_key,
        assign_to_key,
        store_key=None,
        features=None,
        suffix_mean="_ref_mean",
        suffix_std="_ref_std",
        suffix_pull="_ref_pull",
        *args,
        **kwargs,
    ):
        """Initialize an instance of HistComparer.

        :param str reference_key: key of input data to read from data store
        :param str assign_to_key: key of output data to store in data store
        :param str store_key: key of the output data to store in the datastore (optional)
        :param list features: list of features to calculate pull for. (optional)
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        :param str suffix_pull: suffix of pull. pull column = metric + suffix_pull
        :param args: (tuple, optional): residual args passed on to mean and std functions
        :param kwargs: (dict, optional): residual kwargs passed on to mean and std functions
        """
        super().__init__(
            np.mean,
            np.std,
            reference_key,
            assign_to_key,
            store_key,
            suffix_mean,
            suffix_std,
            suffix_pull,
            features,
            *args,
            **kwargs,
        )
        self.reference_key = reference_key
        self.assign_to_key = assign_to_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.assign_to_key}" with reference "{self.reference_key}"'
        )
        return super().transform(datastore)


class RefMedianMadPullCalculator(PullCalculator):
    """Pull calculation based on reference median and mad"""

    def __init__(
        self,
        reference_key,
        assign_to_key,
        store_key=None,
        features=None,
        suffix_mean="_ref_mean",
        suffix_std="_ref_std",
        suffix_pull="_ref_pull",
        *args,
        **kwargs,
    ):
        """Initialize an instance of HistComparer.

        :param str reference_key: key of input data to read from data store
        :param str assign_to_key: key of output data to store in data store
        :param str store_key: key of the output data to store in the datastore (optional)
        :param list features: list of features to calculate pull for.
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        :param str suffix_pull: suffix of pull. pull column = metric + suffix_pull
        :param args: (tuple, optional): residual args passed on to mean and std functions
        :param kwargs: (dict, optional): residual kwargs passed on to mean and std functions
        """

        def df_median(df):
            if isinstance(df, pd.DataFrame):
                df = df.select_dtypes([np.number]).dropna(axis=1, how="all")
            return df.median()

        super().__init__(
            df_median,
            mad,
            reference_key,
            assign_to_key,
            store_key,
            suffix_mean,
            suffix_std,
            suffix_pull,
            features,
            *args,
            **kwargs,
        )
        self.reference_key = reference_key
        self.assign_to_key = assign_to_key

    def transform(self, datastore):
        self.logger.info(
            f'Comparing "{self.assign_to_key}" with median/mad of reference "{self.reference_key}"'
        )
        return super().transform(datastore)
