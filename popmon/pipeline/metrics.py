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


import logging

import pandas as pd
from histogrammar.dfinterface.make_histograms import (
    get_bin_specs,
    get_time_axes,
    make_histograms,
)

from ..config import Settings
from ..pipeline.metrics_pipelines import create_metrics_pipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s]: %(message)s"
)
logger = logging.getLogger()


def stability_metrics(
    hists,
    settings: Settings,
    reference_type="self",
    reference=None,
    time_axis="",
    features=None,
    **kwargs,
):
    """Create a data stability monitoring datastore for given dict of input histograms.

    :param dict hists: input histograms to be profiled and monitored over time.
    :param reference_type: type or reference used for comparisons. Options [self, external, rolling, expanding].
        default is 'self'.
    :param reference: histograms used as reference. default is None
    :param str time_axis: name of datetime feature, used as time axis, eg 'date'. auto-guessed when not provided.
    :param list features: histograms to pick up from the 'hists' dictionary (default is all keys)
    :param kwargs: residual keyword arguments passed on to report pipeline.
    :return: dict with results of metrics pipeline
    """

    if not isinstance(hists, dict):
        raise TypeError("hists should be a dict of histogrammar histograms.")

    if (isinstance(time_axis, str) and len(time_axis) == 0) or (
        isinstance(time_axis, bool) and time_axis
    ):
        # auto guess the time_axis: find the most frequent first column name in the histograms list
        first_cols = [k.split(":")[0] for k in list(hists.keys())]
        time_axis = max(set(first_cols), key=first_cols.count)

    if reference_type == "external" and "ref_hists_key" not in kwargs:
        kwargs["ref_hists_key"] = "ref_hists"

    pipeline = create_metrics_pipeline(
        settings=settings,
        reference_type=reference_type,
        reference=reference,
        hists_key="hists",
        time_axis=time_axis,
        features=features,
        **kwargs,
    )

    datastore = {"hists": hists}
    if reference_type == "external":
        datastore["ref_hists"] = reference

    return pipeline.transform(datastore)


def df_stability_metrics(
    df,
    time_axis,
    settings: Settings = None,
    features=None,
    binning="auto",
    bin_specs=None,
    time_width=None,
    time_offset=0,
    var_dtype=None,
    reference_type="self",
    reference=None,
    **kwargs,
):
    """Create a data stability monitoring html datastore for given pandas or spark dataframe.

    :param df: input pandas/spark dataframe to be profiled and monitored over time.
    :param str time_axis: name of datetime feature, used as time axis, eg 'date'. if True, will be auto-guessed.
        If time_axis is set or found, and if no features provided, features becomes: ['date:x', 'date:y', 'date:z'] etc.
    :param list features: columns to pick up from input data. (default is all features).
        For multi-dimensional histograms, separate the column names with a ':'. Example features list is:

        .. code-block:: python

            features = ["x", "date", "date:x", "date:y", "date:x:y"]

    :param str binning: default binning to revert to in case bin_specs not supplied. options are:
        "unit" or "auto", default is "auto". When using "auto", semi-clever binning is automatically done.
    :param dict bin_specs: dictionaries used for rebinning numeric or timestamp features.
        An example bin_specs dictionary is:

        .. code-block:: python

            bin_specs = {
                "x": {"bin_width": 1, "bin_offset": 0},
                "y": {"num": 10, "low": 0.0, "high": 2.0},
                "x:y": [{}, {"num": 5, "low": 0.0, "high": 1.0}],
            }

        In the bin specs for x:y, x is not provided (here) and reverts to the 1-dim setting.
        The 'bin_width', 'bin_offset' notation makes an open-ended histogram (for that feature) with given bin width
        and offset. The notation 'num', 'low', 'high' gives a fixed range histogram from 'low' to 'high' with 'num'
        number of bins.
    :param time_width: bin width of time axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1w', 3600e9 (number of ns),
                      anything understood by pd.Timedelta(time_width).value

    :param time_offset: bin offset of time axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1-1-2020', 0 (number of ns since 1-1-1970),
                      anything parsed by pd.Timestamp(time_offset).value

    :param dict var_dtype: dictionary with specified datatype per feature. auto-guessed when not provided.
    :param reference_type: type or reference used for comparisons. Options [self, external, rolling, expanding].
        default is 'self'.
    :param reference: reference dataframe or histograms. default is None
    :param kwargs: residual keyword arguments, passed on to stability_report()
    :return: dict with results of metrics pipeline
    """
    if settings is None:
        settings = Settings()

    # basic checks on presence of time_axis
    if not (isinstance(time_axis, str) and len(time_axis) > 0) and not (
        isinstance(time_axis, bool) and time_axis
    ):
        raise TypeError("time_axis needs to be a filled string or set to True")
    if isinstance(time_axis, str) and time_axis not in df.columns:
        raise ValueError(f'time_axis  "{time_axis}" not found in columns of dataframe.')
    if reference is not None and not isinstance(reference, dict):
        if isinstance(time_axis, str) and time_axis not in reference.columns:
            raise ValueError(
                f'time_axis  "{time_axis}" not found in columns of reference dataframe.'
            )
    if isinstance(time_axis, bool):
        time_axes = get_time_axes(df)
        num = len(time_axes)
        if num == 1:
            time_axis = time_axes[0]
            logger.info(f'Time-axis automatically set to "{time_axis}"')
        elif num == 0:
            raise ValueError(
                "No obvious time-axes found. Cannot generate stability report."
            )
        else:
            raise ValueError(
                f"Found {num} time-axes: {time_axes}. Set *one* time_axis manually!"
            )
    if features is not None:
        # by now time_axis is defined. ensure that all histograms start with it.
        if not isinstance(features, list):
            raise TypeError(
                "features should be list of columns (or combos) to pick up from input data."
            )
        features = [
            c if c.startswith(time_axis) else f"{time_axis}:{c}" for c in features
        ]

    # interpret time_width and time_offset
    if isinstance(time_width, (str, int, float)) and isinstance(
        time_offset, (str, int, float)
    ):
        if bin_specs is None:
            bin_specs = {}
        elif not isinstance(bin_specs, dict):
            raise TypeError("bin_specs object is not a dictionary")

        if time_axis in bin_specs:
            raise ValueError(
                f'time-axis "{time_axis}" already found in binning specifications.'
            )
        # convert time width and offset to nanoseconds
        time_specs = {
            "bin_width": float(pd.Timedelta(time_width).value),
            "bin_offset": float(pd.Timestamp(time_offset).value),
        }
        bin_specs[time_axis] = time_specs

    if reference is not None:
        reference_type = "external"
        if isinstance(reference, dict):
            # 1. reference is dict of histograms
            # extract features and bin_specs from reference histograms
            reference_hists = reference
            features = list(reference_hists.keys())
            bin_specs = get_bin_specs(reference_hists)
        else:
            # 2. reference is pandas or spark dataframe
            # generate histograms and return updated features, bin_specs, time_axis, etc.
            (
                reference_hists,
                features,
                bin_specs,
                time_axis,
                var_dtype,
            ) = make_histograms(
                reference,
                features,
                binning,
                bin_specs,
                time_axis,
                var_dtype,
                ret_specs=True,
            )
        kwargs["reference_hists"] = reference_hists

    # use the same features, bin_specs, time_axis, etc as for reference hists
    hists = make_histograms(
        df,
        features=features,
        binning=binning,
        bin_specs=bin_specs,
        time_axis=time_axis,
        var_dtype=var_dtype,
    )

    # generate data stability report
    return stability_metrics(
        hists,
        settings=settings,
        reference_type=reference_type,
        time_axis=time_axis,
        features=features,
        **kwargs,
    )
