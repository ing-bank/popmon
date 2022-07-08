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
from ..config import Report
from ..resources import templates_env
from ..utils import filter_metrics
from ..visualization.utils import _prune


class OverviewSectionGenerator(Module):
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
        settings: Report,
        features=None,
        ignore_features=None,
        static_bounds=None,
        dynamic_bounds=None,
        prefix="traffic_light_",
        suffices=["_red_high", "_yellow_high", "_yellow_low", "_red_low"],
        ignore_stat_endswith=None,
    ):
        """Initialize an instance of SectionGenerator.

        :param str read_key: key of input data to read from the datastore and use for plotting
        :param str store_key: key for output data to be stored in the datastore
        :param list features: list of features to pick up from input data (optional)
        :param list ignore_features: ignore list of features, if present (optional)
        :param str static_bounds: key to static traffic light bounds key in datastore (optional)
        :param str dynamic_bounds: key to dynamic traffic light bounds key in datastore (optional)
        :param str prefix: dynamic traffic light prefix. default is ``'traffic_light_'`` (optional)
        :param str suffices: dynamic traffic light suffices. (optional)
        :param list ignore_stat_endswith: ignore stats ending with any of list of suffices. (optional)
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.prefix = prefix
        self.suffices = suffices
        self.ignore_stat_endswith = ignore_stat_endswith or []

        self.last_n = settings.last_n
        self.skip_first_n = settings.skip_first_n
        self.skip_last_n = settings.skip_last_n
        self.skip_empty_plots = settings.skip_empty_plots
        self.show_stats = settings.show_stats if not settings.extended_report else None
        self.section_name = settings.section.overview.name
        self.description = settings.section.overview.description

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

        self.logger.info(
            f'Generating section "{self.section_name}". skip empty plots: {self.skip_empty_plots}'
        )

        values = {}
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

            metrics = filter_metrics(
                df.columns, self.ignore_stat_endswith, self.show_stats
            )

            values[feature] = _get_metrics(
                metrics,
                df,
                self.last_n,
                self.skip_first_n,
                self.skip_last_n,
                self.skip_empty_plots,
            )

        plots = [_plot_metrics(values)]

        # filter out potential empty plots (from skip empty plots)
        if self.skip_empty_plots:
            plots = [e for e in plots if len(e["plot"])]

        plots = sorted(plots, key=lambda plot: plot["name"])

        sections.append(
            {
                "section_title": self.section_name,
                "section_description": self.description,
                "plots": plots,
            }
        )
        return sections


def _plot_metrics(
    values,
):
    # sort features by n_red, n_yellow, n_green
    values = {
        k: v
        for k, v in sorted(
            values.items(),
            key=lambda x: (
                x[1][2] / x[1]["total"] if x[1]["total"] > 0 else 0,
                x[1][1] / x[1]["total"] if x[1]["total"] > 0 else 0,
                x[1][0] / x[1]["total"] if x[1]["total"] > 0 else 0,
            ),
            reverse=True,
        )
    }

    plot = templates_env(
        "aggregated-overview.html",
        values=values,
    )

    return {
        "name": "Alert frequency per Feature",
        "type": "alert",
        "description": "",
        "plot": plot,
        "full_width": True,
    }


def _get_metrics(
    metrics,
    df,
    last_n,
    skip_first_n,
    skip_last_n,
    skip_empty,
):
    values = []
    nonempty_metrics = []
    for metric in metrics:
        value = _prune(df[metric], last_n, skip_first_n, skip_last_n)

        if not skip_empty or np.sum(value) > 0:
            values.append(value)
            nonempty_metrics.append(metric)

    empty = {0: 0, 1: 0, 2: 0}
    if len(values) > 0:
        values = np.stack(values)

        keys, counts = np.unique(values, return_counts=True)
        counts = dict(zip(keys, counts))
        empty.update(counts)
    empty["total"] = empty[0] + empty[1] + empty[2]
    empty["n_zero"] = sum(empty[i] == 0 for i in range(3))
    return empty
