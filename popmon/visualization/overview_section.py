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

from datetime import datetime

import numpy as np
import pandas as pd
from tqdm import tqdm

from popmon.base import Module
from popmon.config import Report
from popmon.resources import templates_env
from popmon.utils import filter_metrics
from popmon.version import version as __version__
from popmon.visualization.utils import _prune, get_reproduction_table, get_summary_table


class OverviewSectionGenerator(Module):
    """This module takes the time-series data of already computed statistics, plots the data and
    combines all the plots into a list which is stored together with the section name in a dictionary
    which later will be used for the report generation.
    """

    _input_keys = ("read_key", "dynamic_bounds", "store_key", "start_time", "end_time")
    _output_keys = ("store_key",)

    def __init__(
        self,
        read_key,
        store_key,
        settings: Report,
        reference_type,
        time_axis,
        bin_specs,
        features=None,
        ignore_features=None,
        static_bounds=None,
        dynamic_bounds=None,
        prefix: str = "traffic_light_",
        suffices=None,
        ignore_stat_endswith=None,
    ) -> None:
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
        self.start_time = "start_time"
        self.end_time = "end_time"
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.prefix = prefix
        self.suffices = suffices or [
            "_red_high",
            "_yellow_high",
            "_yellow_low",
            "_red_low",
        ]
        self.ignore_stat_endswith = ignore_stat_endswith or []
        self.reference_type = reference_type
        self.time_axis = time_axis
        self.bin_specs = bin_specs

        self.last_n = settings.last_n
        self.skip_first_n = settings.skip_first_n
        self.skip_last_n = settings.skip_last_n
        self.show_stats = settings.show_stats if not settings.extended_report else None
        self.section_name = settings.section.overview.name
        self.description = settings.section.overview.description

    def get_description(self):
        return self.section_name

    def transform(
        self,
        data_obj: dict,
        dynamic_bounds: dict | None = None,
        sections: list | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ):
        assert isinstance(data_obj, dict)
        if dynamic_bounds is None:
            dynamic_bounds = {}
        assert isinstance(dynamic_bounds, dict)
        if sections is None:
            sections = []
        assert isinstance(sections, list)

        features = self.get_features(list(data_obj.keys()))

        self.logger.info(f'Generating section "{self.section_name}"')
        time_windows = 0
        values = {}
        offset = ""
        max_timestamp = ""
        for feature in tqdm(features, ncols=100):
            df = data_obj.get(feature, pd.DataFrame())
            time_windows = len(df.index)
            offset = df.index.min()
            max_timestamp = df.index.max()

            fdbounds = dynamic_bounds.get(feature, pd.DataFrame(index=df.index))
            assert all(df.index == fdbounds.index)

            # prepare date labels
            df = df.drop(
                columns=["histogram", "reference_histogram"],
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
            )

        # Dataset summary table and Analysis Details  table
        tables = []
        bin_width = (
            self.bin_specs[self.time_axis]["bin_width"]
            if self.time_axis in self.bin_specs
            else 0
        )

        if (
            self.time_axis in self.bin_specs
            and self.bin_specs[self.time_axis]["bin_offset"] > 0
        ):
            offset = datetime.utcfromtimestamp(
                self.bin_specs[self.time_axis]["bin_offset"] // 1e9
            )
        tables.append(
            get_summary_table(
                len(features),
                time_windows,
                self.time_axis,
                self.reference_type,
                bin_width,
                offset,
                max_timestamp,
            )
        )

        tables.append(get_reproduction_table(start_time, end_time, __version__))

        # overview plots
        plots = [_plot_metrics(values)]
        # filter out potential empty plots (from skip empty plots)
        plots = [e for e in plots if len(e["plot"])]
        plots = sorted(plots, key=lambda plot: plot["name"])

        plots = tables + plots

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
    values = dict(
        sorted(
            values.items(),
            key=lambda x: (
                x[1][2] / x[1]["total"] if x[1]["total"] > 0 else 0,
                x[1][1] / x[1]["total"] if x[1]["total"] > 0 else 0,
                x[1][0] / x[1]["total"] if x[1]["total"] > 0 else 0,
            ),
            reverse=True,
        )
    )

    plot = templates_env(
        "aggregated-overview.html",
        values=values,
    )

    return {
        "name": "Alerts",
        "type": "alert",
        "description": "",
        "plot": plot,
        "layout": "",
        "full_width": True,
    }


def _get_metrics(
    metrics,
    df,
    last_n,
    skip_first_n,
    skip_last_n,
):
    values = [
        _prune(df[metric], last_n, skip_first_n, skip_last_n) for metric in metrics
    ]

    empty = {0: 0, 1: 0, 2: 0}
    if len(values) > 0:
        values = np.stack(values)

        keys, counts = np.unique(values, return_counts=True)
        counts = dict(zip(keys, counts))
        empty.update(counts)
    empty["total"] = empty[0] + empty[1] + empty[2]
    empty["n_zero"] = sum(empty[i] == 0 for i in range(3))
    return empty
