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


import multiprocessing

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

from ..base import Module
from ..config import get_stat_description
from ..utils import filter_metrics, short_date
from ..visualization.utils import _prune, plot_bars_b64
from .traffic_light_section_generator import _plot_metrics


class AlertSectionGenerator(Module):
    """This module takes the time-series data of already computed statistics, plots the data and
    combines all the plots into a list which is stored together with the section name in a dictionary
    which later will be used for the report generation.
    """

    def __init__(
        self,
        read_key,
        store_key,
        section_name,
        features=None,
        ignore_features=None,
        last_n=0,
        skip_first_n=0,
        skip_last_n=0,
        static_bounds=None,
        dynamic_bounds=None,
        prefix="traffic_light_",
        suffices=["_red_high", "_yellow_high", "_yellow_low", "_red_low"],
        ignore_stat_endswith=None,
        skip_empty_plots=True,
        description="",
        show_stats=None,
    ):
        """Initialize an instance of SectionGenerator.

        :param str read_key: key of input data to read from the datastore and use for plotting
        :param str store_key: key for output data to be stored in the datastore
        :param str section_name: key of output data to store in the datastore
        :param list features: list of features to pick up from input data (optional)
        :param list ignore_features: ignore list of features, if present (optional)
        :param int last_n: plot statistic data for last 'n' periods (optional)
        :param int skip_first_n: when plotting data skip first 'n' periods. last_n takes precedence (optional)
        :param int skip_last_n: in plot skip last 'n' periods. last_n takes precedence (optional)
        :param str static_bounds: key to static traffic light bounds key in datastore (optional)
        :param str dynamic_bounds: key to dynamic traffic light bounds key in datastore (optional)
        :param str prefix: dynamic traffic light prefix. default is ``'traffic_light_'`` (optional)
        :param str suffices: dynamic traffic light suffices. (optional)
        :param list ignore_stat_endswith: ignore stats ending with any of list of suffices. (optional)
        :param bool skip_empty_plots: if false, also show empty plots in report with only nans or zeroes (optional)
        :param str description: description of the section. default is empty (optional)
        :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.features = features or []
        self.ignore_features = ignore_features or []
        self.section_name = section_name
        self.last_n = last_n
        self.skip_first_n = skip_first_n
        self.skip_last_n = skip_last_n
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds
        self.prefix = prefix
        self.suffices = suffices
        self.ignore_stat_endswith = ignore_stat_endswith or []
        self.skip_empty_plots = skip_empty_plots
        self.description = description
        self.show_stats = show_stats
        self.plot_overview = True
        self.plot_metrics = False

    def transform(self, datastore):
        data_obj = self.get_datastore_object(datastore, self.read_key, dtype=dict)

        static_bounds = self.get_datastore_object(
            datastore, self.static_bounds, dtype=dict, default={}
        )
        dynamic_bounds = self.get_datastore_object(
            datastore, self.dynamic_bounds, dtype=dict, default={}
        )

        features = self.get_features(data_obj.keys())
        features_w_metrics = []

        num_cores = multiprocessing.cpu_count()

        self.logger.info(
            f'Generating section "{self.section_name}". skip empty plots: {self.skip_empty_plots}'
        )

        for feature in tqdm(features, ncols=100):
            df = data_obj.get(feature, pd.DataFrame())
            fdbounds = dynamic_bounds.get(feature, pd.DataFrame(index=df.index))

            assert all(df.index == fdbounds.index)

            # prepare date labels
            df.drop(
                columns=["histogram", "reference_histogram"],
                inplace=True,
                errors="ignore",
            )
            dates = [short_date(str(date)) for date in df.index.tolist()]

            metrics = filter_metrics(
                df.columns, self.ignore_stat_endswith, self.show_stats
            )

            plots = []
            if self.plot_overview:
                plots.append(
                    _plot_metrics(
                        feature,
                        [m for m in metrics if not m.endswith("worst")],
                        dates,
                        df,
                        0,
                        0,
                        0,
                        0,
                        style="alerts",
                    )
                )
            if self.plot_metrics:
                plots += Parallel(n_jobs=num_cores)(
                    delayed(_plot_metric)(
                        feature,
                        metric,
                        dates,
                        df[metric],
                        static_bounds,
                        fdbounds,
                        self.prefix,
                        self.suffices,
                        self.last_n,
                        self.skip_first_n,
                        self.skip_last_n,
                        self.skip_empty_plots,
                    )
                    for metric in metrics
                )
            # filter out potential empty plots (from skip empty plots)
            if self.skip_empty_plots:
                plots = [e for e in plots if len(e["plot"])]

            features_w_metrics.append(
                {"name": feature, "plots": sorted(plots, key=lambda plot: plot["name"])}
            )

        params = {
            "section_title": self.section_name,
            "section_description": self.description,
            "features": features_w_metrics,
        }

        if self.store_key in datastore:
            datastore[self.store_key].append(params)
        else:
            datastore[self.store_key] = [params]

        return datastore


def _plot_metric(
    feature,
    metric,
    dates,
    values,
    static_bounds,
    fdbounds,
    prefix,
    suffices,
    last_n,
    skip_first_n,
    skip_last_n,
    skip_empty,
):
    """Split off plot histogram generation to allow for parallel processing"""
    # pick up static traffic light boundaries
    name = feature + ":" + metric
    sbounds = static_bounds.get(name, ())
    # pick up dynamic traffic light boundaries
    names = [prefix + metric + suffix for suffix in suffices]
    dbounds = tuple(
        _prune(fdbounds[n].tolist(), last_n, skip_first_n, skip_last_n)
        for n in names
        if n in fdbounds.columns
    )
    # choose dynamic bounds if present
    bounds = dbounds if len(dbounds) > 0 else sbounds
    # prune dates and values
    dates = _prune(dates, last_n, skip_first_n, skip_last_n)
    values = _prune(values, last_n, skip_first_n, skip_last_n)

    # make plot. note: slow!
    plot = plot_bars_b64(
        data=np.array(values),
        labels=dates,
        ylim=True,
        bounds=bounds,
        skip_empty=skip_empty,
    )

    return {"name": metric, "description": get_stat_description(metric), "plot": plot}
