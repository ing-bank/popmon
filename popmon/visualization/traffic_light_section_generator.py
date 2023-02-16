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

import numpy as np
import pandas as pd
from tqdm import tqdm

from popmon.base import Module
from popmon.config import Report
from popmon.utils import filter_metrics, short_date
from popmon.visualization.utils import (
    _prune,
    plot_traffic_lights_alerts_aggregate,
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
        settings: Report,
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
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.last_n = settings.last_n
        self.skip_first_n = settings.skip_first_n
        self.skip_last_n = settings.skip_last_n
        self.prefix = prefix
        self.suffices = suffices or [
            "_red_high",
            "_yellow_high",
            "_yellow_low",
            "_red_low",
        ]
        self.ignore_stat_endswith = ignore_stat_endswith or []
        self.show_stats = settings.show_stats if not settings.extended_report else None

        self.section_name = settings.section.traffic_lights.name
        self.description = settings.section.traffic_lights.description
        self.tl_colors = settings.tl_colors

    def get_description(self):
        return self.section_name

    def transform(
        self,
        data_obj: dict,
        dynamic_bounds: dict | None = None,
        sections: list | None = None,
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

        self.logger.info(f'Generating section "{self.section_name}"')

        for feature in tqdm(features, ncols=100):
            df = data_obj.get(feature, pd.DataFrame())
            fdbounds = dynamic_bounds.get(feature, pd.DataFrame(index=df.index))

            assert all(df.index == fdbounds.index)

            # prepare date labels
            df = df.drop(
                columns=["histogram", "reference_histogram"],
                errors="ignore",
            )
            dates = [short_date(date) for date in df.index.tolist()]

            metrics = sorted(
                filter_metrics(df.columns, self.ignore_stat_endswith, self.show_stats)
            )

            plots = [
                _plot_metrics(
                    feature,
                    metrics,
                    dates,
                    df,
                    self.last_n,
                    self.skip_first_n,
                    self.skip_last_n,
                    tl_colors=self.tl_colors,
                )
            ]

            # filter out potential empty plots (from skip empty plots)
            plots = [e for e in plots if len(e["plot"])]

            features_w_metrics.append(
                {
                    "name": feature,
                    "plot_type_layouts": {"traffic_lights": ""},
                    "plots": sorted(plots, key=lambda plot: plot["name"]),
                }
            )

        sections.append(
            {
                "section_title": self.section_name,
                "section_description": self.description,
                "features": features_w_metrics,
            }
        )
        return sections


def _plot_metrics(
    feature,
    metrics,
    dates,
    df,
    last_n,
    skip_first_n,
    skip_last_n,
    tl_colors,
    style: str = "heatmap",
):
    # prune dates and values
    dates = _prune(dates, last_n, skip_first_n, skip_last_n)

    values = [
        _prune(df[metric], last_n, skip_first_n, skip_last_n) for metric in metrics
    ]

    if len(values) > 0:
        values = np.stack(values)

        if style == "heatmap":
            plot = plot_traffic_lights_overview(
                feature, values, metrics=metrics, labels=dates
            )
        elif style == "alerts":
            plot = plot_traffic_lights_alerts_aggregate(
                feature,
                values,
                metrics=metrics,
                labels=dates,
                tl_colors=tl_colors,
            )
        else:
            raise ValueError("style must be either 'heatmap' or 'alerts'")
    else:
        plot = ""

    return {
        "name": "Overview",
        "type": "traffic_light",
        "description": "",
        "plot": plot,
        "full_width": True,
    }
