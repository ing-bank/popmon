# Copyright (c) 2023 ING Analytics Wholesale Banking
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
from __future__ import annotations

from math import floor

import pandas as pd
from typing_extensions import Literal


def _split_dataset_pandas(
    dataset,
    split_type: Literal["n_instances", "fraction", "condition"],
    split: int | float | pd.Series | str,
):
    if split_type in ["n_instances", "fraction"]:
        if split_type == "fraction":
            n = dataset.shape[0]
            split = floor(split * n)
        reference = dataset.iloc[:split]
        df = dataset.iloc[split:]
    else:
        reference = dataset[split]
        df = dataset[~split]

    if reference.empty:
        raise ValueError("Reference is empty. Please adjust the `split` argument")
    if df.empty:
        raise ValueError(
            "Returned dataframe is empty. Please adjust the `split` argument"
        )

    return reference, df


def _split_dataset_spark(
    dataset,
    split_type: Literal["n_instances", "fraction", "condition"],
    split: int | float | pd.Series | str,
    time_axis: str,
    partition_cols: list[str] | None = None,
    persist: bool = True,
):
    """
    Split a dataset into a reference and remaining part based on split params using PySpark
    (see `split_dataset`)

    :param split_type: meaning of split parameter
    :param split: split parameter
    :param time_axis: time axis
    :param partition_cols: cols to partition by (for performance)
    :param persist: persist or not, enabled by default since we are checking for empty dataframes
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.window import Window

    if split_type in ["n_instances", "fraction"]:
        data_win = Window.orderBy(time_axis)
        if partition_cols is not None:
            data_win = data_win.partitionBy(partition_cols)

        if split_type == "n_instances":
            dt_rank = dataset.withColumn("tmpcol", F.row_number().over(data_win))
        else:
            dt_rank = dataset.withColumn("tmpcol", F.percent_rank().over(data_win))

        reference = dt_rank.filter(F.col("tmpcol") <= split).drop("tmpcol")
        df = dt_rank.filter(F.col("tmpcol") > split).drop("tmpcol")
    else:
        reference = dataset.filter(split)
        df = dataset.filter(f"NOT ({split})")

    if persist:
        reference.persist()
        df.persist()

    if len(reference.head(1)) == 0:
        raise ValueError("Reference is empty. Please adjust the `split` argument")
    if len(df.head(1)) == 0:
        raise ValueError(
            "Returned dataframe is empty. Please adjust the `split` argument"
        )

    return reference, df


def split_dataset(dataset, split: int | float | pd.Series | str, time_axis: str):
    """Split a dataset into a reference and remaining part based on split params.

    :param pd.Dataset|pyspark.sql.Dataset dataset: dataset as input
    :param Any split: split details, meaning depends on the type:
        if integer, then the reference will be the first ``split`` instances
        if float, then ``split`` will be used as ration (e.g. 0.5 returns a 50/50 split)
        otherwise, the ``split`` are interpreted as condition, where the records for which the condition is
        true are considered the reference, and the other records the remaining dataset.
    :param time_axis: the time axis
    :return: tuple of reference, dataset
    """

    if isinstance(split, int):
        split_type = "n_instances"
        if split <= 0:
            raise ValueError("Number of instances should be greater than 0")
    elif isinstance(split, float):
        if split >= 1.0 or split <= 0:
            raise ValueError("Fraction should be 0 > fraction > 1")
        split_type = "fraction"
    elif isinstance(split, (pd.Series, str)):
        split_type = "condition"
    else:
        raise TypeError("`split` should be int, float, str or pd.Series")

    if isinstance(dataset, pd.DataFrame):
        reference, df = _split_dataset_pandas(dataset, split_type, split)
    else:
        reference, df = _split_dataset_spark(dataset, split_type, split, time_axis)

    return reference, df
