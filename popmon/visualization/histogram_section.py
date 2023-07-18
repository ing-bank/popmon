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
from histogrammar.util import get_hist_props
from tqdm import tqdm

from popmon.analysis.hist_numpy import (
    assert_similar_hists,
    get_consistent_numpy_1dhists,
    get_consistent_numpy_entries,
)
from popmon.base import Module
from popmon.config import HistogramSectionModel
from popmon.utils import parallel, short_date
from popmon.visualization.utils import (
    histogram_basic_checks,
    plot_heatmap,
    plot_histogram_overlay,
)


class HistogramSection(Module):
    """This module plots histograms of all selected features for the last 'n' periods."""

    _input_keys = ("read_key", "store_key")
    _output_keys = ("store_key",)

    def __init__(
        self,
        read_key,
        store_key,
        reference_type: str,
        settings: HistogramSectionModel,
        features=None,
        ignore_features=None,
        hist_names=None,
        hist_name_starts_with: str = "histogram",
    ) -> None:
        """Initialize an instance of SectionGenerator.

        :param str read_key: key of input data to read from the datastore and use for plotting
        :param str store_key: key for output data to be stored in the datastore
        :param list features: list of features to pick up from input data (optional)
        :param list ignore_features: ignore list of features, if present (optional)
        :param list hist_names: list of histogram names to plot
        :param str hist_name_starts_with: find histograms in case hist_names is empty. default is histogram.
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.reference_type = reference_type

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.hist_names = hist_names or []
        self.hist_name_starts_with = hist_name_starts_with

        # section specific
        self.section_name = settings.name
        self.descriptions = settings.descriptions
        self.description = settings.description
        self.hist_names = settings.hist_names
        self.hist_names_formatted = settings.hist_names_formatted
        self.plot_hist_n = settings.plot_hist_n
        self.top_n = settings.top_n
        self.n_choices = settings.inspector_histogram_choices
        self.cmap = settings.cmap

    def get_description(self):
        return self.section_name

    def transform(self, data_obj: dict, sections: list | None = None):
        if sections is None:
            sections = []

        features = self.get_features(list(data_obj.keys()))
        features_w_metrics = []

        # Treat these as static
        is_static_reference = self.reference_type in ["self", "external"]

        self.logger.info(f'Generating section "{self.section_name}".')

        for feature in tqdm(features, ncols=100):
            df = data_obj.get(feature, pd.DataFrame())
            last_n = (
                len(df.index)
                if (len(df.index) < self.plot_hist_n or self.plot_hist_n == 0)
                else self.plot_hist_n
            )
            hist_names = [hn for hn in self.hist_names if hn in df.columns]
            if len(hist_names) == 0 and len(self.hist_name_starts_with) > 0:
                # if no columns are given, find histogram columns.
                hist_names = [
                    c for c in df.columns if c.startswith(self.hist_name_starts_with)
                ]
            if len(hist_names) == 0:
                self.logger.debug(
                    f"for feature {feature} no histograms found. skipping."
                )
                continue

            # heatmap plot for each metric
            dates = [short_date(date) for date in df.index[:]]
            hists = [
                df[hist_names].iloc[-i].values
                for i in reversed(range(1, len(dates) + 1))
            ]

            # compute heatmaps
            heatmaps = _plot_heatmap(
                feature,
                dates,
                [h[0] for h in hists],
                self.top_n,
                self.cmap,
                self.hist_names,
                self.hist_names_formatted,
                self.descriptions,
            )

            # get base64 encoded plot for each metric; do parallel processing to speed up.
            dates = [short_date(date) for date in df.index[-last_n:]]
            hists = [
                df[hist_names].iloc[-i].values for i in reversed(range(1, last_n + 1))
            ]

            args = [
                (feature, dates[i], hists[i], hist_names, self.top_n)
                for i in range(last_n)
            ]

            # get histograms for each timestamp
            plots = parallel(_plot_histograms, args)

            plot_type_layouts = {}

            # filter out potential empty plots
            plots = [e for e in plots if len(e)]
            plots = sorted(plots, key=lambda plot: plot["date"])

            # basic checks for histograms
            histogram_basic_checks(plots)

            for plot in plots:
                for index in range(len(plot["hists"])):
                    if plot["hist_names"][index] == "histogram_prev1":
                        del plot["hist_names"][index]
                        del plot["hists"][index]
                        break

            # get histogram plots
            histogram = {}
            if len(plots) > 1:
                histogram = plot_histogram_overlay(
                    plots,
                    plots[0]["is_num"],
                    plots[0]["is_ts"],
                    is_static_reference,
                    top=self.top_n,
                    n_choices=self.n_choices,
                )

            if len(histogram) > 0:
                plot_type_layouts["histogram"] = histogram["layout"]
                histogram = [histogram]
            else:
                histogram = []

            # filter out potential empty heatmap plots, then prepend them to the sorted histograms
            hplots = [h for h in heatmaps if isinstance(h, dict) and len(h["plot"])]

            if len(hplots) > 0:
                plot_type_layouts["heatmap"] = hplots[0]["layout"]

            plots = hplots + histogram

            features_w_metrics.append(
                {
                    "name": feature,
                    "plot_type_layouts": plot_type_layouts,
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


def _plot_histograms(feature, date, hc_list, hist_names, top_n, max_nbins: int = 1000):
    """Split off plot histogram generation to allow for parallel processing

    :param str feature: feature
    :param str date: date of time slot
    :param list hc_list: histogram list
    :param list hist_names: names of histograms to show as labels
    :param int max_nbins: maximum number of histogram bins allowed for plot (default 1000)
    :return: dict with histograms for each timestamp
    """
    # basic checks
    if len(hc_list) != len(hist_names):
        raise ValueError(
            "histogram list and histograms names should have equal length."
        )
    # filter out Nones (e.g. can happen with empty rolling hist)
    none_hists = [i for i, hc in enumerate(hc_list) if hc is None]
    hc_list = [hc for i, hc in enumerate(hc_list) if i not in none_hists]
    hist_names = [hn for i, hn in enumerate(hist_names) if i not in none_hists]
    # more basic checks
    if len(hc_list) == 0:
        return {}
    assert_similar_hists(hc_list)

    # make plot. note: slow!
    if hc_list[0].n_dim == 1:
        if all(h.entries == 0 for h in hc_list):
            # triviality checks, skip all histograms empty
            return {}

        props = get_hist_props(hc_list[0])
        is_num = props["is_num"]
        is_ts = props["is_ts"]
        y_label = "Bin count" if len(hc_list) == 1 else "Bin probability"

        if is_num:
            numpy_1dhists = get_consistent_numpy_1dhists(hc_list, crop_range=True)
            entries_list = [nphist[0] for nphist in numpy_1dhists]
            bins = numpy_1dhists[0][1]  # bins = bin-edges
        else:
            # categorical
            entries_list, bins = get_consistent_numpy_entries(
                hc_list, get_bin_labels=True
            )  # bins = bin-labels

        # skip histograms with too many bins to plot (default more than 1000)
        if len(bins) > max_nbins:
            return {}

        # normalize histograms for plotting (comparison!) in case there is more than one.
        if len(hc_list) >= 2:
            entries_list = [
                el / hc.entries if hc.entries > 0 else el
                for el, hc in zip(entries_list, hc_list)
            ]
        # if categorical
        # get top_n categories for histogram
        if not is_num:
            if len(bins) > top_n:
                entries_list = np.stack(entries_list, axis=1)
                entries_list, bins = get_top_categories(entries_list, bins, top_n)
                entries_list = np.stack(entries_list, axis=1)
                entries_list = np.reshape(entries_list.ravel(), (-1, len(bins)))

        hists = [(el, bins) for el in entries_list]

    elif hc_list[0].n_dim == 2:
        return {}
    else:
        return {}

    return {
        "date": date,
        "hists": hists,
        "feature": feature,
        "hist_names": hist_names,
        "y_label": y_label,
        "is_num": is_num,
        "is_ts": is_ts,
    }


def _plot_heatmap(
    feature,
    date,
    hc_list,
    top_n,
    cmap,
    hist_names,
    hist_names_formatted,
    descriptions,
):
    # basic checks
    if len(hist_names) <= 0:
        # skip numeric heatmap
        return {"plot": ""}

    # filter out Nones (e.g. can happen with empty rolling hist)
    none_hists = [i for i, hc in enumerate(hc_list) if hc is None]
    hc_list = [hc for i, hc in enumerate(hc_list) if i not in none_hists]
    hist_names = [hn for i, hn in enumerate(hist_names) if i not in none_hists]
    # more basic checks
    if len(hc_list) == 0:
        return date, ""
    assert_similar_hists(hc_list)
    if hc_list[0].n_dim == 1:
        props = get_hist_props(hc_list[0])
        is_num = props["is_num"]
        is_ts = props["is_ts"]
        y_label = "Bin count" if len(hc_list) == 1 else "Bin probability"

        if is_num:
            # skip numeric heatmap
            return {"plot": ""}
        else:
            # categorical, retrieve values and bins
            entries_list, bins = get_consistent_numpy_entries(
                hc_list, get_bin_labels=True
            )  # bins = bin-labels

            entries_list = np.stack(entries_list, axis=1)

            # if cardinality of feature is more than 20
            if len(bins) > top_n:
                entries_list, bins = get_top_categories(entries_list, bins, top_n)

            # make 3 copies : 1st normal, 2nd for column normalized heatmap, 3rd for row normalized heatmap
            hists = []
            if "heatmap" in hist_names:
                hist_normal = entries_list.copy()
                hists.append(hist_normal)

            # normalize across column for a plot
            if "heatmap_column_normalized" in hist_names:
                hist_col = entries_list.copy()
                hist_col = np.stack(hist_col, axis=1)
                hist_col = hist_col.astype(float)
                for i in range(hist_col.shape[0]):
                    if hist_col[i].sum() > 0:
                        hist_col[i] = hist_col[i] / hist_col[i].sum()
                hist_col = np.stack(hist_col, axis=1)
                hists.append(hist_col)

            # normalize across row for a plot
            if "heatmap_row_normalized" in hist_names:
                hist_row = entries_list.copy()
                hist_row = hist_row.astype(float)
                for i in range(hist_row.shape[0]):
                    if hist_row[i].sum() > 0:
                        hist_row[i] = hist_row[i] / hist_row[i].sum()

                hists.append(hist_row)

        if len(bins) == 0:
            # skip empty histograms
            return {"plot": ""}

        args = [
            (hist, bins, date, feature, hist_name, y_label, is_num, is_ts, cmap)
            for hist, hist_name in zip(hists, hist_names)
        ]
        heatmaps = parallel(plot_heatmap, args)

        if isinstance(heatmaps, list):
            plot = [hist_lookup(heatmaps, hist_name) for hist_name in hist_names]
        elif isinstance(heatmaps, dict):
            plot = [heatmaps]

        plots = [
            {
                "name": hist_names_formatted[hist_name],
                "type": "heatmap",
                "description": descriptions[hist_name],
                "plot": pl["plot"],
                "layout": pl["layout"],
                "full_width": True,
            }
            for pl, hist_name in zip(plot, hist_names)
        ]

    elif hc_list[0].n_dim == 2:
        plots = {"plot": ""}
    else:
        plots = {"plot": ""}

    return plots


def get_top_categories(entries_list, bins, top_n):
    # get the top top_n rows
    row_sum = np.sum(entries_list, axis=1).ravel().tolist()
    sorted_index = np.argsort(row_sum).tolist()
    top_rows = entries_list[sorted_index[-top_n:], :]

    # aggregate all other rows
    bottom_rows = entries_list[sorted_index[:-top_n], :]
    bottom_row = np.sum(bottom_rows, axis=0).ravel().tolist()
    # append alll other aggregated row to top_rows
    top_rows = np.append(top_rows, [bottom_row], axis=0)

    # select the corresponding bins/labels
    bins = [bins[i] for i in sorted_index[-top_n:]]
    # add 'others' label
    bins.append("Others")

    return top_rows, bins


def hist_lookup(plot, hist_name):
    for pl in plot:
        if pl["name"] == hist_name:
            return pl
    return None
