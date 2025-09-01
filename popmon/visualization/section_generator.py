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

from collections import defaultdict

import numpy as np
import pandas as pd
from tqdm import tqdm

from popmon.analysis.comparison import Comparisons
from popmon.analysis.profiling import Profiles
from popmon.base import Module
from popmon.config import Report
from popmon.utils import filter_metrics, parallel, short_date
from popmon.visualization.utils import _prune, plot_bars

profiles = Profiles.get_descriptions()

comparisons = Comparisons.get_descriptions()


group_titles = {
    "prev1": "Previous Reference",
    "rolling": "Rolling Reference",
    "self": "Self-Reference",
    "ref": "External Reference",
    "expanding": "Expanding Reference",
}
references = {
    "ref": "the reference data",
    "roll": "a rolling window",
    "prev1": "the preceding time slot",
    "expanding": "all preceding time slots",
}
group_descriptions = {
    key: f"Comparing each time slot to {value}." for key, value in references.items()
}


def get_stat_description(name: str) -> str:
    """Gets the description of a statistic.

    :param str name: the name of the statistic.

    :returns str: the description of the statistic. If not found, returns an empty string
    """
    if not isinstance(name, str):
        raise TypeError("Statistic's name should be a string.")

    if name in profiles:
        return profiles[name]

    if name in "mean_trend10_zscore":
        return "Significance of (rolling) trend in means of features"

    head, *tails = name.split("_")
    tail = "_".join(tails)

    if tail in comparisons and head in references:
        return comparisons[tail]

    return ""


class SectionGenerator(Module):
    """This module takes the time-series data of already computed statistics, plots the data and
    combines all the plots into a list which is stored together with the section name in a dictionary
    which later will be used for the report generation.
    """

    _input_keys = ("read_key", "static_bounds", "dynamic_bounds", "store_key")
    _output_keys = ("store_key",)

    def __init__(
        self,
        read_key,
        store_key,
        section_name,
        settings: Report,
        features=None,
        ignore_features=None,
        static_bounds=None,
        dynamic_bounds=None,
        prefix: str = "traffic_light_",
        suffices=None,
        ignore_stat_endswith=None,
        description: str = "",
    ) -> None:
        """Initialize an instance of SectionGenerator.

        :param str read_key: key of input data to read from the datastore and use for plotting
        :param str store_key: key for output data to be stored in the datastore
        :param str section_name: key of output data to store in the datastore
        :param list features: list of features to pick up from input data (optional)
        :param list ignore_features: ignore list of features, if present (optional)
        :param str static_bounds: key to static traffic light bounds key in datastore (optional)
        :param str dynamic_bounds: key to dynamic traffic light bounds key in datastore (optional)
        :param str prefix: dynamic traffic light prefix. default is ``'traffic_light_'`` (optional)
        :param str suffices: dynamic traffic light suffices. (optional)
        :param list ignore_stat_endswith: ignore stats ending with any of list of suffices. (optional)
        :param str description: description of the section. default is empty (optional)
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.dynamic_bounds = dynamic_bounds
        self.static_bounds = static_bounds

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.section_name = section_name
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
        self.description = description
        self.show_stats = settings.show_stats if not settings.extended_report else None
        self.primary_color = settings.primary_color
        self.tl_colors = settings.tl_colors

    def get_description(self):
        return self.section_name

    def transform(
        self,
        data_obj: dict,
        static_bounds: dict | None = None,
        dynamic_bounds: dict | None = None,
        sections: list | None = None,
    ):
        if static_bounds is None:
            static_bounds = {}
        if dynamic_bounds is None:
            dynamic_bounds = {}
        if sections is None:
            sections = []

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
            dates = np.array([short_date(date) for date in df.index.tolist()])

            metrics = filter_metrics(
                df.columns, self.ignore_stat_endswith, self.show_stats
            )

            args = [
                (
                    feature,
                    metric,
                    dates,
                    df[metric].values,
                    static_bounds,
                    fdbounds,
                    self.prefix,
                    self.suffices,
                    self.last_n,
                    self.skip_first_n,
                    self.skip_last_n,
                    self.primary_color,
                    self.tl_colors,
                )
                for metric in metrics
            ]
            plots = parallel(_plot_metric, args)

            # filter out potential empty plots (from skip empty plots)
            plots = [e for e in plots if len(e["plot"])]

            layouts = ""
            if len(plots) > 0:
                layouts = plots[0]["layout"]
                if "shapes" in layouts:
                    del layouts["shapes"]
                if "range" in layouts.get("yaxis", {}):
                    del layouts["yaxis"]["range"]

            # Group comparisons in Comparison section
            if self.read_key == "comparisons":
                grouped_metrics = defaultdict(list)
                for plot in plots:
                    prefix = plot["name"].split("_")[0]
                    if prefix not in references:
                        prefix = "Others"
                    grouped_metrics[prefix].append(plot)

                features_w_metrics.append(
                    {
                        "name": feature,
                        "plot_type_layouts": {"barplot": layouts},
                        "plots": grouped_metrics,
                        "titles": group_titles,
                        "descriptions": group_descriptions,
                    }
                )
            else:
                features_w_metrics.append(
                    {
                        "name": feature,
                        "plot_type_layouts": {"barplot": layouts},
                        "plots": plots,
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
    primary_color,
    zline_color,
):
    """Split off plot histogram generation to allow for parallel processing"""
    # pick up static traffic light boundaries
    name = f"{feature}:{metric}"
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
    plot = plot_bars(
        data=values,
        labels=dates,
        ylim=True,
        bounds=bounds,
        primary_color=primary_color,
        tl_colors=zline_color,
        metric=metric,
    )

    if not isinstance(plot, dict):
        return {
            "name": metric,
            "type": "barplot",
            "description": get_stat_description(metric),
            "plot": plot,
            "layout": plot,
        }

    return {
        "name": metric,
        "type": "barplot",
        "description": get_stat_description(metric),
        "plot": plot["data"],
        "shapes": plot["layout"]["shapes"] if "shapes" in plot["layout"] else "",
        "yaxis_range": (
            [
                "null" if r is None else r
                for r in plot["layout"].get("yaxis", {}).get("range")
            ]
            if "range" in plot["layout"].get("yaxis", {})
            else ""
        ),
        "layout": plot["layout"],
    }
