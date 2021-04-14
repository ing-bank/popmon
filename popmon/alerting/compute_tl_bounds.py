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


import collections
import copy
import fnmatch
import uuid
from collections import defaultdict

import numpy as np
import pandas as pd

from ..analysis.apply_func import ApplyFunc
from ..base import Module, Pipeline


def traffic_light_summary(row, cols=None, prefix=""):
    """Make a summary of traffic light alerts present in the dataframe

    Count number of green, yellow, red traffic lights and worst value.

    Evaluate with df.apply(traffic_light_summary, axis=1)

    :param pd.Series row: row to calculate traffic light summary of.
    :param list cols: list of cols to calculate traffic light summary of (optional)
    :param str prefix: prefix of traffic light columns, in case cols is empty. default is ``"tl_"``
    """
    x = pd.Series(
        {"worst": np.nan, "n_red": np.nan, "n_yellow": np.nan, "n_green": np.nan}
    )

    if cols is None or len(cols) == 0:
        # if no columns are given, find traffic light columns for which summary is made.
        cols = (
            [m for m in row.index.to_list() if m.startswith(prefix)]
            if prefix
            else row.index.to_list()
        )
    if len(cols) == 0:
        return x

    traffic_lights = np.array([row[c] for c in cols])
    x["worst"] = np.max(traffic_lights)
    x["n_red"] = (traffic_lights == 2).sum()
    x["n_yellow"] = (traffic_lights == 1).sum()
    x["n_green"] = (traffic_lights == 0).sum()
    return x


def traffic_light(value, red_high, yellow_high, yellow_low=0, red_low=0):
    """Get corresponding traffic light given a value and traffic light bounds.

    :param float value: value to check
    :param float red_high: higher bound of red traffic light
    :param float yellow_high: higher bound of yellow traffic light
    :param float yellow_low: lower bound of yellow traffic light (optional)
    :param float red_low: lower bound of red traffic light (optional)

    :return: corresponding traffic light based on the value and traffic light bounds
    """
    assert (
        np.diff([red_high, yellow_high, yellow_low, red_low]) > 0
    ).sum() == 0, "Traffic lights not sorted!"

    if value < red_low or value >= red_high:
        return 2
    elif value < yellow_low or value >= yellow_high:
        return 1
    return 0


def collect_traffic_light_bounds(monitoring_rules):
    """Collect traffic light boundaries

    Splits generic metrics and metrics which belong to particular features
    and puts them in separate objects.

    :param dict monitoring_rules: dictionary of defined monitoring rules (bounds)

    :return: dict metrics_per_feature (containing features with corresponding metrics),
             list metrics: list of generic metrics
    """
    metrics_per_feature = defaultdict(list)
    metrics = []
    for pattern in monitoring_rules.keys():
        psplit = pattern.split(":")
        feature = ":".join(psplit[:-1])
        metric = psplit[-1]
        (metrics_per_feature[feature] if feature else metrics).append(metric)

    return metrics_per_feature, metrics


class ComputeTLBounds(Module):
    """Expand traffic light boundaries over all features and metrics

    The module extracts the monitoring rules for the traffic light
    bounds for specific features/metrics and if those are not found -
    monitoring rules are extracted from the wildcard rules which are
    meant to be generic. Then bounds can be stored as either raw
    values or as directly calculated values on the statistics of the data.
    """

    def __init__(
        self,
        read_key,
        monitoring_rules=None,
        store_key="",
        features=None,
        ignore_features=None,
        apply_funcs_key="",
        func=None,
        metrics_wide=False,
        prefix="traffic_light_",
        suffix="",
        entire=False,
        **kwargs,
    ):
        """Initialize an instance of TafficLightBounds module.

        :param str read_key: key of input data to read from datastore
        :param str store_key: key of output data to store in datastore (optional)
        :param dict monitoring_rules: dict of traffic light bounds => key is feature:metric and value is bounds
        :param list features: features of data frames to pick up from input data (optional)
        :param list ignore_features: list of features to ignore (optional)
        :param str apply_funcs_key: key of to-be-applied traffic light functions in data to store (optional)
        :param func: traffic light function to apply (optional)
        :param bool metrics_wide: if true, select wide set of metrics from columns. default is false (optional)
        :param str prefix: prefix for traffic light variables. default is ``tl_`` (optional)
        :param str suffix: suffix for traffic light variables. default is '' (optional)
        :param bool entire: if True, apply function to the entire feature's dataframe of metrics (optional)
        :param kwargs: residual kwargs are passed as kwargs to 'func' (dict, optional)
        """

        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.monitoring_rules = monitoring_rules or {}
        self.features = features or []
        self.ignore_features = ignore_features or []
        self.traffic_lights = {}
        self.traffic_light_funcs = []
        self.apply_funcs_key = apply_funcs_key
        self.traffic_light_func = func if func is not None else traffic_light
        self.metrics_wide = metrics_wide
        self.prefix = prefix
        self.suffix = suffix
        self.entire = entire
        self.kwargs = copy.copy(kwargs)

        # check inputs
        if not isinstance(self.traffic_light_func, collections.Callable):
            raise TypeError("supplied function must be callable object")

    def _set_traffic_lights(self, feature, cols, pattern, rule_name):
        process_cols = fnmatch.filter(cols, pattern)

        for pcol in process_cols:
            name = feature + ":" + pcol
            if name not in self.traffic_lights:
                bounds = self.monitoring_rules[eval(rule_name)]
                self.traffic_lights[name] = bounds
                metrics = (
                    [pcol]
                    if not self.metrics_wide
                    else [c for c in cols if c.startswith(pcol.split("_")[0])]
                )
                self.traffic_light_funcs.append(
                    {
                        "func": self.traffic_light_func,
                        "features": [feature],
                        "metrics": metrics,
                        "args": tuple(bounds),
                        "prefix": self.prefix,
                        "suffix": self.suffix,
                        "entire": self.entire,
                        "kwargs": self.kwargs,
                    }
                )

    def transform(self, datastore):
        # fetch and check input data
        test_data = self.get_datastore_object(datastore, self.read_key, dtype=dict)

        # determine all possible features, used for the comparison below
        features = self.get_features(test_data.keys())

        pkeys, nkeys = collect_traffic_light_bounds(self.monitoring_rules)

        # loop over features to apply monitoring business rules to
        for feature in features[:]:
            # basic checks if feature object is filled correctly
            test_df = self.get_datastore_object(test_data, feature, dtype=pd.DataFrame)

            # --- 1. tl bounds explicitly defined for a particular feature
            if feature in pkeys:
                explicit_cols = [
                    pcol for pcol in pkeys[feature] if pcol in test_df.columns
                ]
                implicit_cols = set(pkeys[feature]) - set(explicit_cols)

                # --- A1. tl bounds explicitly defined for a particular feature/profile combo
                self._set_traffic_lights(
                    feature, explicit_cols, pattern="*", rule_name="name"
                )

                # --- B1. tl bounds implicitly defined for particular feature
                #         see if a wildcard match can be found.
                for pattern in implicit_cols:
                    self._set_traffic_lights(
                        feature,
                        test_df.columns,
                        pattern,
                        rule_name="feature + ':' + pattern",
                    )
            # --- 2. tl bounds not explicitly defined for a particular feature,
            #        see if a wildcard match can be found.
            for pattern in nkeys:
                self._set_traffic_lights(
                    feature, test_df.columns, pattern, rule_name="pattern"
                )

        # storage
        if self.store_key:
            datastore[self.store_key] = self.traffic_lights
        if self.apply_funcs_key:
            datastore[self.apply_funcs_key] = self.traffic_light_funcs

        return datastore


def pull_bounds(
    row,
    red_high,
    yellow_high,
    yellow_low=0,
    red_low=0,
    suffix_mean="_mean",
    suffix_std="_std",
    cols=None,
):
    """Calculate traffic light pull bounds for list of cols

    Function can be used with ApplyFunc module.

    :param pd.Series row: row to apply this function to.
    :param float red_high: higher bound of red traffic light
    :param float yellow_high: higher bound of yellow traffic light
    :param float yellow_low: lower bound of yellow traffic light (optional)
    :param float red_low: lower bound of red traffic light (optional)
    :param str suffix_mean: suffix of mean column. default is '_mean' (optional)
    :param str suffix_std: suffix of std column. default is '_std' (optional)
    :param list cols: list of cols to calculate bounds of (optional)
    """
    assert (
        np.diff([red_high, yellow_high, yellow_low, red_low]) > 0
    ).sum() == 0, "Traffic lights not sorted!"

    if cols is None or len(cols) == 0:
        # if no columns are given, find columns for which pulls can be calculated.
        # e.g. to calculate x_pull, need to have [x, x_mean, x_std] present. If so, put x in cols.
        cols = []
        for m in row.index.to_list():
            if m not in cols:
                required = [m, m + suffix_mean, m + suffix_std]
                if all(r in row for r in required):
                    cols.append(m)
    else:
        for m in cols:
            required = [m + suffix_mean, m + suffix_std]
            assert all(r in row for r in required)

    x = pd.Series()
    for m in cols:
        x[m + "_red_high"] = np.nan
        x[m + "_yellow_high"] = np.nan
        x[m + "_yellow_low"] = np.nan
        x[m + "_red_low"] = np.nan
        required = [m + suffix_mean, m + suffix_std]
        if any(pd.isnull(row[required])):
            continue
        x[m + "_red_high"] = row[m + suffix_mean] + row[m + suffix_std] * red_high
        x[m + "_yellow_high"] = row[m + suffix_mean] + row[m + suffix_std] * yellow_high
        x[m + "_yellow_low"] = row[m + suffix_mean] + row[m + suffix_std] * yellow_low
        x[m + "_red_low"] = row[m + suffix_mean] + row[m + suffix_std] * red_low
    return x


def df_single_op_pull_bounds(
    df,
    red_high,
    yellow_high,
    yellow_low=0,
    red_low=0,
    suffix_mean="_mean",
    suffix_std="_std",
    cols=None,
):
    """Calculate traffic light pull bounds for list of cols on first row only

    Function can be used with ApplyFunc module.

    :param pd.DataFrame df: df to apply this function to
    :param float red_high: higher bound of red traffic light
    :param float yellow_high: higher bound of yellow traffic light
    :param float yellow_low: lower bound of yellow traffic light (optional)
    :param float red_low: lower bound of red traffic light (optional)
    :param str suffix_mean: suffix of mean column. default is '_mean' (optional)
    :param str suffix_std: suffix of std column. default is '_std' (optional)
    :param list cols: list of cols to calculate bounds of (optional)
    """
    if len(df.index) == 0:
        raise ValueError("input df has zero length")
    row = df.iloc[0]
    return pull_bounds(
        row, red_high, yellow_high, yellow_low, red_low, suffix_mean, suffix_std, cols
    )


class DynamicBounds(Pipeline):
    """Calculate dynamic traffic light bounds based on pull thresholds and dynamic mean and std.deviation."""

    def __init__(
        self, read_key, rules, store_key="", suffix_mean="_mean", suffix_std="_std"
    ):
        """Initialize an instance of DynamicTrafficLightBounds.

        :param str read_key: key of input data to read from data store, only used to extract feature list.
        :param dict rules: dict of traffic light bounds => key is feature:metric and value is bounds
        :param str store_key: key of dynamic traffic light boundaries to store in data store, e.g. used for plotting.
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        """
        super().__init__(modules=[])
        self.read_key = read_key

        apply_funcs_key = str(uuid.uuid4())

        expand_bounds = ComputeTLBounds(
            read_key=read_key,
            monitoring_rules=rules,
            apply_funcs_key=apply_funcs_key,
            func=pull_bounds,
            metrics_wide=True,
            axis=1,
            suffix_std=suffix_std,
            suffix_mean=suffix_mean,
        )
        calc_bounds = ApplyFunc(
            apply_to_key=read_key,
            assign_to_key=store_key,
            apply_funcs_key=apply_funcs_key,
        )

        self.modules = [expand_bounds, calc_bounds]

    def transform(self, datastore):
        self.logger.info(f'Calculating dynamic bounds for "{self.read_key}"')
        return super().transform(datastore)


class StaticBounds(Pipeline):
    """Calculate static traffic light bounds based on pull thresholds and static mean and std.deviation."""

    def __init__(
        self, read_key, rules, store_key="", suffix_mean="_mean", suffix_std="_std"
    ):
        """Initialize an instance of StaticBounds.

        :param str read_key: key of input data to read from data store, only used to extract feature list.
        :param dict rules: dict of traffic light bounds => key is feature:metric and value is bounds
        :param str store_key: key of dynamic traffic light boundaries to store in data store, e.g. used for plotting.
        :param str suffix_mean: suffix of mean. mean column = metric + suffix_mean
        :param str suffix_std: suffix of std. std column = metric + suffix_std
        """
        super().__init__(modules=[])
        self.read_key = read_key

        apply_funcs_key = str(uuid.uuid4())

        expand_bounds = ComputeTLBounds(
            read_key=read_key,
            monitoring_rules=rules,
            apply_funcs_key=apply_funcs_key,
            func=df_single_op_pull_bounds,
            entire=True,
            metrics_wide=True,
            suffix_std=suffix_std,
            suffix_mean=suffix_mean,
        )
        calc_bounds = ApplyFunc(
            apply_to_key=read_key,
            assign_to_key=store_key,
            apply_funcs_key=apply_funcs_key,
        )

        self.modules = [expand_bounds, calc_bounds]

    def transform(self, datastore):
        self.logger.info(f'Calculating static bounds for "{self.read_key}"')
        return super().transform(datastore)


class TrafficLightAlerts(Pipeline):
    """Evaluate the traffic light alerts

    Steps:

    - Generate static traffic light bounds by expanding the wildcarded monitoring rules
    - Apply them to profiled test statistics data
    """

    def __init__(self, read_key, store_key, rules, expanded_rules_key=""):
        """Initialize an instance of TrafficLightBounds.

        :param str read_key: key of input data to read from data store, only used to extract feature list.
        :param str store_key: results of traffic light bounds after applied to input data, to store in data store.
        :param dict rules: input dict of wildcard traffic light bounds => key is feature:metric
                                      and value is bounds
        :param str expanded_rules_key: store key of expanded monitoring rules to store in data store,
                                                  eg. these can be used for plotting. (optional)
        """
        super().__init__(modules=[])
        self.read_key = read_key

        apply_funcs_key = str(uuid.uuid4())

        # generate static traffic light bounds by expanding the wildcarded monitoring rules
        expand_bounds = ComputeTLBounds(
            read_key=read_key,
            store_key=expanded_rules_key,
            prefix="",
            apply_funcs_key=apply_funcs_key,
            monitoring_rules=rules,
        )
        # Apply them to profiled test statistics data
        apply_bounds = ApplyFunc(
            apply_to_key=read_key,
            assign_to_key=store_key,
            apply_funcs_key=apply_funcs_key,
        )

        self.modules = [expand_bounds, apply_bounds]

    def transform(self, datastore):
        self.logger.info(f'Calculating traffic light alerts for "{self.read_key}"')
        return super().transform(datastore)
