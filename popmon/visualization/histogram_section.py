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

import pandas as pd
from histogrammar.util import get_hist_props
from joblib import Parallel, delayed
from tqdm import tqdm

from ..analysis.hist_numpy import (
    assert_similar_hists,
    get_consistent_numpy_1dhists,
    get_consistent_numpy_entries,
)
from ..base import Module
from ..config import get_stat_description
from ..utils import short_date
from ..visualization.utils import plot_overlay_1d_histogram_b64


class HistogramSection(Module):
    """This module plots histograms of all selected features for the last 'n' periods."""

    def __init__(
        self,
        read_key,
        store_key,
        section_name="Histograms",
        features=None,
        ignore_features=None,
        last_n=1,
        hist_names=None,
        hist_name_starts_with="histogram",
        description="",
    ):
        """Initialize an instance of SectionGenerator.

        :param str read_key: key of input data to read from the datastore and use for plotting
        :param str store_key: key for output data to be stored in the datastore
        :param str section_name: key of output data to store in the datastore
        :param list features: list of features to pick up from input data (optional)
        :param list ignore_features: ignore list of features, if present (optional)
        :param int last_n: plot histogram for last 'n' periods. default is 1 (optional)
        :param list hist_names: list of histogram names to plot
        :param str hist_name_starts_with: find histograms in case hist_names is empty. default is histogram.
        :param str description: description of the section. default is empty (optional)
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.features = features or []
        self.ignore_features = ignore_features or []
        self.section_name = section_name
        self.last_n = last_n if last_n >= 1 else 1
        self.hist_names = hist_names or []
        self.hist_name_starts_with = hist_name_starts_with
        self.description = description

    def transform(self, datastore):
        data_obj = self.get_datastore_object(datastore, self.read_key, dtype=dict)

        features = self.get_features(data_obj.keys())
        features_w_metrics = []

        num_cores = multiprocessing.cpu_count()

        self.logger.info(f'Generating section "{self.section_name}".')

        for feature in tqdm(features, ncols=100):
            df = data_obj.get(feature, pd.DataFrame())

            last_n = len(df.index) if len(df.index) < self.last_n else self.last_n
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

            # get base64 encoded plot for each metric; do parallel processing to speed up.
            dates = [short_date(str(date)) for date in df.index[-last_n:]]
            hists = [
                df[hist_names].iloc[-i].values for i in reversed(range(1, last_n + 1))
            ]

            plots = Parallel(n_jobs=num_cores)(
                delayed(_plot_histograms)(feature, dates[i], hists[i], hist_names)
                for i in range(last_n)
            )
            # filter out potential empty plots
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


def _plot_histograms(feature, date, hc_list, hist_names):
    """Split off plot histogram generation to allow for parallel processing

    :param str feature: feature
    :param str date: date of time slot
    :param list hc_list: histogram list
    :param list hist_names: names of histograms to show as labels
    :return: dict with plotted histogram
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
        return date, ""
    assert_similar_hists(hc_list)

    # make plot. note: slow!
    if hc_list[0].n_dim == 1:
        props = get_hist_props(hc_list[0])
        is_num = props["is_num"]
        is_ts = props["is_ts"]
        y_label = "Bin count" if len(hc_list) == 1 else "Bin probability"

        if is_num:
            numpy_1dhists = get_consistent_numpy_1dhists(hc_list)
            entries_list = [nphist[0] for nphist in numpy_1dhists]
            bins = numpy_1dhists[0][1]  # bins = bin-edges
        else:
            # categorical
            entries_list, bins = get_consistent_numpy_entries(
                hc_list, get_bin_labels=True
            )  # bins = bin-labels
        if len(bins) == 0:
            # skip empty histograms
            return date, ""

        # normalize histograms for plotting (comparison!) in case there is more than one.
        if len(hc_list) >= 2:
            entries_list = [
                el / hc.entries if hc.entries > 0 else el
                for el, hc in zip(entries_list, hc_list)
            ]
        hists = [(el, bins) for el in entries_list]
        plot = plot_overlay_1d_histogram_b64(
            hists, feature, hist_names, y_label, is_num, is_ts
        )
    elif hc_list[0].n_dim == 2:
        # grid2d_list, xkeys, ykeys = get_consistent_numpy_2dgrids(hc_list, get_bin_labels=True)
        plot = ""
    else:
        plot = ""

    return {"name": date, "description": get_stat_description(date), "plot": plot}
