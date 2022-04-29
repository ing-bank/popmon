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


from typing import Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from ..base import Module
from ..config import get_stat_description
from ..utils import filter_metrics, parallel, short_date
from ..visualization.utils import (
    _prune,
    plot_traffic_lights_alerts_b64,
    plot_traffic_lights_b64,
    plot_traffic_lights_overview,
)


class TrafficLightSectionGenerator(Module):
    """This module takes the time-series data of already computed statistics, plots the data and
    combines all the plots into a list which is stored together with the section name in a dictionary
    which later will be used for the report generation.
    """

    _input_keys = ("read_key", "dynamic_bounds", "store_key")
    _output_keys = ("store_key",)

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
        plot_overview=True,
        plot_metrics=False,
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
        :param bool plot_overview: heatmap overview of traffic lights (features x time)
        :param bool plot_metrics: individual plot per feature
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.section_name = section_name
        self.last_n = last_n
        self.skip_first_n = skip_first_n
        self.skip_last_n = skip_last_n
        self.prefix = prefix
        self.suffices = suffices
        self.ignore_stat_endswith = ignore_stat_endswith or []
        self.skip_empty_plots = skip_empty_plots
        self.description = description
        self.show_stats = show_stats
        self.plot_overview = plot_overview
        self.plot_metrics = plot_metrics

    def get_description(self):
        return self.section_name

    def transform(
        self,
        data_obj: dict,
        dynamic_bounds: Optional[dict] = None,
        sections: Optional[list] = None,
    ):
        assert isinstance(data_obj, dict)
        if dynamic_bounds is None:
            dynamic_bounds = {}
        assert isinstance(dynamic_bounds, dict)
        if sections is None:
            sections = []
        assert isinstance(sections, list)

        features = self.get_features(list(data_obj.keys()))
        features_w_metrics = []

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
                        metrics,
                        dates,
                        df,
                        self.last_n,
                        self.skip_first_n,
                        self.skip_last_n,
                        self.skip_empty_plots,
                    )
                )

            if self.plot_metrics:
                args = [
                    (
                        metric,
                        dates,
                        df[metric],
                        self.last_n,
                        self.skip_first_n,
                        self.skip_last_n,
                        self.skip_empty_plots,
                    )
                    for metric in metrics
                ]
                plots += parallel(_plot_metric, args)

            # filter out potential empty plots (from skip empty plots)
            if self.skip_empty_plots:
                plots = [e for e in plots if len(e["plot"])]
            features_w_metrics.append(
                {"name": feature, "plots": sorted(plots, key=lambda plot: plot["name"])}
            )

        sections.append(
            {
                "section_title": self.section_name,
                "section_description": self.description,
                "features": features_w_metrics,
            }
        )
        return sections


def _plot_metric(metric, dates, values, last_n, skip_first_n, skip_last_n, skip_empty):
    """Split off plot histogram generation to allow for parallel processing"""

    # prune dates and values
    dates = _prune(dates, last_n, skip_first_n, skip_last_n)
    values = _prune(values, last_n, skip_first_n, skip_last_n)

    # make plot. note: slow!
    plot = plot_traffic_lights_b64(
        data=np.array(values), labels=dates, skip_empty=skip_empty
    )

    return {"name": metric, "description": get_stat_description(metric), "plot": plot}


def _plot_metrics(
    feature,
    metrics,
    dates,
    df,
    last_n,
    skip_first_n,
    skip_last_n,
    skip_empty,
    style="heatmap",
):
    # prune dates and values
    dates = _prune(dates, last_n, skip_first_n, skip_last_n)

    values = []
    nonempty_metrics = []
    for metric in metrics:
        value = _prune(df[metric], last_n, skip_first_n, skip_last_n)

        if not skip_empty or np.sum(value) > 0:
            values.append(value)
            nonempty_metrics.append(metric)

    if len(values) > 0:
        values = np.stack(values)

        # make plot. note: slow!
        if style == "heatmap":
            plot = plot_traffic_lights_overview(
                feature, values, metrics=nonempty_metrics, labels=dates
            )
        elif style == "alerts":
            plot = plot_traffic_lights_alerts_b64(
                feature,
                values,
                metrics=nonempty_metrics,
                labels=dates,
            )
        else:
            raise ValueError("style must be either 'heatmap' or 'alerts'")
    else:
        plot = ""

    return {"name": "Overview", "description": "", "plot": plot, "full_width": True}
