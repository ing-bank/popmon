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
import multiprocessing
import warnings

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from ..base import Module


class ApplyFunc(Module):
    """This module applies functions to specified feature and metrics.

    Extra parameters (kwargs) can be passed to the apply function.
    """

    def __init__(
        self,
        apply_to_key,
        store_key="",
        assign_to_key="",
        apply_funcs_key="",
        features=None,
        apply_funcs=None,
        metrics=None,
        msg="",
    ):
        """Initialize an instance of ApplyFunc.

        :param str apply_to_key: key of the input data to apply funcs to.
        :param str assign_to_key: key of the input data to assign function applied-output to. (optional)
        :param str store_key: key of the output data to store in the datastore (optional)
        :param str apply_funcs_key: key of to-be-applied functions in data to store (optional)
        :param list features: list of features to pick up from input data and apply funcs to (optional)
        :param list metrics: list of metrics to apply funcs to (optional)
        :param str msg: message to print out at start of transform function. (optional)
        :param list apply_funcs: functions to apply (list of dicts):

          - 'func': function to apply
          - 'suffix' (string, optional): suffix added to each metric. default is function name.
          - 'prefix' (string, optional): prefix added to each metric.
          - 'features' (list, optional): features the function is applied to. Overwrites features above
          - 'metrics' (list, optional): metrics the function is applied to. Overwrites metrics above
          - 'entire' (boolean, optional): apply function to the entire feature's dataframe of metrics?
          - 'args' (tuple, optional): args for 'func'
          - 'kwargs' (dict, optional): kwargs for 'func'

        """
        super().__init__()
        self.apply_to_key = apply_to_key
        self.assign_to_key = self.apply_to_key if not assign_to_key else assign_to_key
        self.store_key = self.assign_to_key if not store_key else store_key
        self.apply_funcs_key = apply_funcs_key
        self.features = features or []
        self.metrics = metrics or []
        self.msg = msg
        self.apply_funcs = []
        # import applied functions
        apply_funcs = apply_funcs or []
        for af in apply_funcs:
            self.add_apply_func(**af)

    def add_apply_func(
        self,
        func,
        suffix=None,
        prefix=None,
        metrics=[],
        features=[],
        entire=None,
        *args,
        **kwargs,
    ):
        """Add function to be applied to dataframe.

        Can call this function after module instantiation to add new functions.

        :param func: function to apply
        :param suffix: (string, optional) suffix added to each metric. default is function name.
        :param prefix: (string, optional) prefix added to each metric.
        :param features: (list, optional) features the function is applied to. Overwrites features above
        :param metrics: (list, optional) metrics the function is applied to. Overwrites metrics above
        :param entire: (boolean, optional) apply function to the entire feature's dataframe of metrics?
        :param args: (tuple, optional) args for 'func'
        :param kwargs: (dict, optional) kwargs for 'func'
        """
        # check inputs
        if not callable(func):
            raise TypeError("functions in ApplyFunc must be callable objects")
        if suffix is not None and not isinstance(suffix, str):
            raise TypeError("prefix, and suffix in ApplyFunc must be strings or None.")
        if prefix is not None and not isinstance(prefix, str):
            raise TypeError("prefix, and suffix in ApplyFunc must be strings or None.")
        if not isinstance(metrics, list) or not isinstance(features, list):
            raise TypeError("metrics and features must be lists of strings.")

        # add function
        self.apply_funcs.append(
            {
                "features": features,
                "metrics": metrics,
                "func": func,
                "entire": entire,
                "suffix": suffix,
                "prefix": prefix,
                "args": args,
                "kwargs": kwargs,
            }
        )

    def transform(self, datastore):
        """
        Apply functions to specified feature and metrics

        Each feature/metric combination is treated as a pandas series

        :param datastore: input datastore
        :return: updated datastore
        :rtype: dict
        """
        if self.msg:
            self.logger.info(self.msg)

        apply_to_data = self.get_datastore_object(
            datastore, self.apply_to_key, dtype=dict
        )
        assign_to_data = self.get_datastore_object(
            datastore, self.assign_to_key, dtype=dict, default={}
        )

        if self.apply_funcs_key:
            apply_funcs = self.get_datastore_object(
                datastore, self.apply_funcs_key, dtype=list
            )
            self.apply_funcs += apply_funcs

        features = self.get_features(apply_to_data.keys())

        num_cores = multiprocessing.cpu_count()
        same_key = self.assign_to_key == self.apply_to_key

        res = Parallel(n_jobs=num_cores)(
            delayed(apply_func_array)(
                feature=feature,
                metrics=self.metrics,
                apply_to_df=self.get_datastore_object(
                    apply_to_data, feature, dtype=pd.DataFrame
                ),
                assign_to_df=None
                if same_key
                else self.get_datastore_object(
                    assign_to_data, feature, dtype=pd.DataFrame, default=pd.DataFrame()
                ),
                apply_funcs=self.apply_funcs,
                same_key=same_key,
            )
            for feature in features
        )
        new_metrics = {r[0]: r[1] for r in res}

        # storage
        datastore[self.store_key] = new_metrics
        return datastore


def apply_func_array(
    feature, metrics, apply_to_df, assign_to_df, apply_funcs, same_key
):
    """Apply list of functions to dataframe

    Split off for parallellization reasons

    :param str feature: feature currently looping over
    :param list metrics: list of selected metrics to apply functions to
    :param apply_to_df: pandas data frame that function in arr is applied to
    :param assign_to_df: pandas data frame the output of function is assigned to
    :param apply_funcs: list of functions to apply to
    :param same_key: if True, merge apply_to_df and assign_to_df before returning assign_to_df
    :return: untion of feature and assign_to_df
    """
    if not isinstance(apply_to_df, pd.DataFrame):
        raise TypeError(
            f'apply_to_df of feature "{feature}" is not a pandas dataframe.'
        )

    if same_key or (len(assign_to_df.index) == 0 and len(assign_to_df.columns) == 0):
        assign_to_df = pd.DataFrame(index=apply_to_df.index)

    for arr in apply_funcs:
        obj = apply_func(feature, metrics, apply_to_df, arr)
        if len(obj) == 0:
            # no metrics were found in apply_to_df
            continue
        for new_metric, o in obj.items():
            if isinstance(o, pd.Series):
                if len(assign_to_df.index) == len(o) and all(
                    assign_to_df.index == o.index
                ):
                    assign_to_df[new_metric] = o
                else:
                    warnings.warn(
                        f"{feature}:{new_metric}: df_out and object have inconsistent lengths."
                    )
            else:
                # o is number or object, assign to every element of new column
                assign_to_df[new_metric] = [o] * len(assign_to_df.index)
    if same_key:
        assign_to_df = pd.concat([apply_to_df, assign_to_df], axis=1)
    return feature, assign_to_df


def apply_func(feature, selected_metrics, df, arr):
    """Apply function to dataframe

    :param str feature: feature currently looping over
    :param list selected_metrics: list of selected metrics to apply to
    :param df: pandas data frame that function in arr is applied to
    :param dict arr: dictionary containing the function to be applied to pandas dataframe.
    :return: dictionary with outputs of applied-to metric pd.Series
    """
    # basic checks of feature
    if "features" in arr and len(arr["features"]) > 0:
        if feature not in arr["features"]:
            return {}

    # get func input
    keys = list(arr.keys())

    assert "func" in keys, "function input is insufficient."
    func = arr["func"]

    if "prefix" not in keys or arr["prefix"] is None:
        arr["prefix"] = ""
    if len(arr["prefix"]) > 0 and not arr["prefix"].endswith("_"):
        arr["prefix"] = arr["prefix"] + "_"
    if "suffix" not in keys or arr["suffix"] is None:
        arr["suffix"] = func.__name__ if len(arr["prefix"]) == 0 else ""
    if len(arr["suffix"]) > 0 and not arr["suffix"].startswith("_"):
        arr["suffix"] = "_" + arr["suffix"]
    suffix = arr["suffix"]
    prefix = arr["prefix"]

    args = ()
    kwargs = {}
    if "kwargs" in keys:
        kwargs = arr["kwargs"]
    if "args" in keys:
        args = arr["args"]

    # apply func
    if len(selected_metrics) > 0 or ("metrics" in keys and len(arr["metrics"]) > 0):
        metrics = (
            arr["metrics"]
            if ("metrics" in keys and len(arr["metrics"]) > 0)
            else selected_metrics
        )
        metrics = [m for m in metrics if m in df.columns]
        # assert all(m in df.columns for m in metrics)
        if len(metrics) == 0:
            return {}
        df = df[metrics] if len(metrics) >= 2 else df[metrics[0]]

    if (
        "entire" in arr
        and arr["entire"] is not None
        and arr["entire"] is not False
        and arr["entire"] != 0
    ):
        obj = func(df, *args, **kwargs)
    else:
        obj = df.apply(func, args=args, **kwargs)

    # convert object to dict format
    if not isinstance(
        obj, (pd.Series, pd.DataFrame, list, tuple, np.ndarray)
    ) and isinstance(df, pd.Series):
        obj = {df.name: obj}
    elif not isinstance(
        obj, (pd.Series, pd.DataFrame, list, tuple, np.ndarray)
    ) and isinstance(df, pd.DataFrame):
        obj = {"_".join(df.columns): obj}
    elif (
        isinstance(obj, (list, tuple, np.ndarray))
        and isinstance(df, pd.DataFrame)
        and len(df.columns) == len(obj)
    ):
        obj = {c: o for c, o in zip(df.columns, obj)}
    elif (
        isinstance(obj, (list, tuple, np.ndarray))
        and isinstance(df, pd.Series)
        and len(df.index) == len(obj)
    ):
        obj = {df.name: pd.Series(data=obj, index=df.index)}
    elif (
        isinstance(obj, (list, tuple, np.ndarray))
        and isinstance(df, pd.DataFrame)
        and len(df.index) == len(obj)
    ):
        obj = {"_".join(df.columns): pd.Series(data=obj, index=df.index)}
    elif (
        isinstance(obj, pd.Series)
        and isinstance(df, pd.Series)
        and len(obj) == len(df)
        and all(obj.index == df.index)
    ):
        obj = {df.name: obj}
    elif (
        isinstance(obj, pd.Series)
        and isinstance(df, pd.DataFrame)
        and len(obj) == len(df)
        and all(obj.index == df.index)
    ):
        obj = {"_".join(df.columns): obj}
    elif (
        isinstance(obj, pd.DataFrame)
        and len(obj.columns) == 1
        and len(obj.index) != len(df.index)
    ):
        # e.g. output of normalized_hist_mean_cov: a dataframe with one column, actually a series
        obj = obj[obj.columns[0]].to_dict()
    elif (
        isinstance(obj, pd.DataFrame)
        and len(obj.columns) == 1
        and len(obj.index) == len(df.index)
        and (obj.index != df.index).any()
    ):
        # e.g. output of normalized_hist_mean_cov: a dataframe with one column, actually a series
        obj = obj[obj.columns[0]].to_dict()
    elif isinstance(obj, pd.Series):
        # e.g. output of np.mean of np.std: results in one number per column when applied to a dataframe
        obj = obj.to_dict()
    elif isinstance(obj, pd.DataFrame):
        # when apply to one column returns another column
        obj = {c: obj[c] for c in obj.columns}
    assert isinstance(obj, dict)

    # add prefix and suffix to dict keys
    keys = list(obj.keys())
    for k in keys:
        obj[prefix + k + suffix] = obj.pop(k)

    return obj
