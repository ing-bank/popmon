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


import logging
import math
from collections import defaultdict
from io import BytesIO, StringIO
from textwrap import shorten
from typing import List, Optional

import numpy as np
import pandas as pd
import pybase64
from ing_theme_matplotlib import mpl_style
from matplotlib import pyplot as plt
from matplotlib.colorbar import ColorbarBase
from matplotlib.colors import BoundaryNorm, ListedColormap
from mpl_toolkits.axes_grid1 import make_axes_locatable

from popmon.resources import templates_env

NUM_NS_DAY = 24 * 3600 * int(1e9)

logger = logging.getLogger()
mpl_style(dark=False, minor_ticks=False)


def plt_to_str(format="png"):
    """Outputting plot as a base64 encoded string or as svg image.

    :return: base64 encoded plot image or svg image
    :rtype:   str
    """

    if format == "png":
        tmpfile = BytesIO()

        plt.savefig(tmpfile, format="png")
        plt.close()

        return pybase64.b64encode(tmpfile.getvalue()).decode("utf-8")
    elif format == "svg":
        tmpfile = StringIO()

        plt.savefig(tmpfile, format="svg")
        plt.close()

        return tmpfile.getvalue().encode("utf-8")
    else:
        raise ValueError("Format should be png or svg.")


def plot_bars_b64(data, labels=None, bounds=None, ylim=False, skip_empty=True):
    """Plotting histogram data.

    :param numpy.ndarray data: bin values of a histogram
    :param list labels: common bin labels for all histograms. default is None.
    :param bounds: traffic light bounds (y-coordinates). default is None.
    :param bool ylim: place y-axis limits for zooming into the data. default is False.
    :param bool skip_empty: if false, also plot empty plots with only nans or only zeroes. default is True.
    :return: base64 encoded plot image
    :rtype: str
    """
    # basic checks first
    n = data.size  # number of bins
    if labels and len(labels) != n:
        raise ValueError("shape mismatch: x-axis labels do not match the data shape")

    # skip plot generation for empty datasets
    if skip_empty:
        n_data = len(data)
        n_zero = n_data - np.count_nonzero(data)
        n_nan = pd.isnull(data).sum()
        n_inf = np.sum([np.isinf(x) for x in data if isinstance(x, float)])
        if n_nan + n_zero + n_inf == n_data:
            logger.debug("skipping plot with empty data.")
            return ""

    fig, ax = plt.subplots()

    index = np.arange(n)
    width = (index[1] - index[0]) * 0.9 if n >= 2 else 1.0
    ax.bar(index, data, width=width, align="center")

    if labels:
        ax.set_xticks(index)
        ax.set_xticklabels(labels, fontdict={"rotation": "vertical"})
        granularity = math.ceil(len(labels) / 50)
        [
            l.set_visible(False)
            for (i, l) in enumerate(ax.xaxis.get_ticklabels())
            if i % granularity != 0
        ]

    # plot boundaries
    try:
        all_nan = (np.isnan(data)).all()
        max_value = np.nanmax(data) if not all_nan else np.nan
        min_value = np.nanmin(data) if not all_nan else np.nan

        if len(bounds) > 0:
            max_r, max_y, min_y, min_r = bounds
            y_max = max(
                max(max_r) if isinstance(max_r, (list, tuple)) else max_r, max_value
            )
            y_min = min(
                max(min_r) if isinstance(min_r, (list, tuple)) else min_r, min_value
            )
            spread = (y_max - y_min) / 20
            y_max += spread
            y_min -= spread

            if not isinstance(max_r, (list, tuple)):
                ax.axhline(y=max_r, xmin=0, xmax=1, color="r")
            else:
                ax.plot(index, max_r, color="r")
            if not isinstance(max_r, (list, tuple)):
                ax.axhline(y=max_y, xmin=0, xmax=1, color="y")
            else:
                ax.plot(index, max_y, color="y")
            if not isinstance(max_r, (list, tuple)):
                ax.axhline(y=min_y, xmin=0, xmax=1, color="y")
            else:
                ax.plot(index, min_y, color="y")
            if not isinstance(max_r, (list, tuple)):
                ax.axhline(y=min_r, xmin=0, xmax=1, color="r")
            else:
                ax.plot(index, min_r, color="r")
            if y_max > y_min:
                ax.set_ylim(y_min, y_max)
        elif ylim:
            spread = (max_value - min_value) / 20
            y_min = min_value - spread
            y_max = max_value + spread
            if y_max > y_min:
                ax.set_ylim(y_min, y_max)
    except Exception:
        pass

    ax.grid(True, linestyle=":")

    fig.tight_layout()
    return plt_to_str()


def render_traffic_lights_table(feature, data, metrics: List[str], labels: List[str]):
    colors = {}
    color_map = ["green", "yellow", "red"]
    for c1, metric in enumerate(metrics):
        colors[metric] = {}
        for c2, label in enumerate(labels):
            colors[metric][label] = [color_map[data[c1][c2]]]

    return templates_env(
        "table.html",
        feature=feature,
        metrics=metrics,
        labels=labels,
        data=colors,
        links=True,
    )


def plot_traffic_lights_overview(feature, data, metrics=None, labels=None):
    return render_traffic_lights_table(feature, data, metrics, labels)


def render_alert_aggregate_table(feature, data, metrics: List[str], labels: List[str]):
    colors = {}
    for c1, metric in enumerate(metrics):
        colors[metric] = {}
        row_max = np.max(data[c1])
        for c2, label in enumerate(labels):
            a = data[c1][c2] / row_max
            if metric.endswith("green"):
                rgba = (0, 128, 0, a)
            elif metric.endswith("yellow"):
                rgba = (255, 255, 0, a)
            else:
                rgba = (255, 0, 0, a)
            rgba = (str(v) for v in rgba)
            colors[metric][label] = (rgba, data[c1][c2])

    return templates_env(
        "table.html",
        feature=feature,
        metrics=metrics,
        labels=labels,
        data=colors,
        links=False,
    )


def plot_traffic_lights_alerts_b64(feature, data, metrics=None, labels=None):
    assert data.shape[0] == 3

    # Reorder metrics if needed
    pos_green = metrics.index("n_green")
    pos_yellow = metrics.index("n_yellow")
    pos_red = metrics.index("n_red")

    if [pos_green, pos_yellow, pos_red] != [0, 1, 2]:
        data[[0, 1, 2]] = data[[pos_green, pos_yellow, pos_red]]

    metrics = ["# green", "# yellow", "# red"]

    return render_alert_aggregate_table(feature, data.astype(int), metrics, labels)


def plot_traffic_lights_b64(data, labels=None, skip_empty=True):
    """Plotting histogram data.

    :param np.array data: bin values of a histogram
    :param labels: common bin labels for all histograms (optional)
    :param bool skip_empty: if true, skip empty plots with only nans or only zeroes (optional)

    :return: base64 encoded plot image
    :rtype:   string
    """
    # basic checks first
    n = data.size  # number of bins
    if labels and len(labels) != n:
        raise ValueError("shape mismatch: x-axis labels do not match the data shape")

    # skip plot generation for empty datasets
    if skip_empty:
        n_data = len(data)
        n_zero = n_data - np.count_nonzero(data)
        n_nan = pd.isnull(data).sum()
        n_inf = np.sum([np.isinf(x) for x in data if isinstance(x, float)])
        if n_nan + n_zero + n_inf == n_data:
            logger.debug("skipping plot with empty data.")
            return ""

    fig, ax = plt.subplots()

    ax.yaxis.grid(True)
    ax.xaxis.grid(False)

    colors = ["green", "yellow", "red"]
    ones = np.ones(n)

    index = np.arange(n)

    for i, color in enumerate(colors):
        mask = data == i
        ax.bar(
            index[mask],
            ones[mask],
            width=1,
            align="center",
            color=color,
            alpha=0.8,
            edgecolor="black",
        )

    ax.set_yticks([])

    if labels:
        ax.set_xticks(index)
        ax.set_xticklabels(labels, fontdict={"rotation": "vertical"})
        granularity = math.ceil(len(labels) / 50)
        [
            l.set_visible(False)
            for (i, l) in enumerate(ax.xaxis.get_ticklabels())
            if i % granularity != 0
        ]

    fig.tight_layout()

    return plt_to_str()


def grouped_bar_chart_b64(data, labels, legend):
    """Plotting grouped histogram data.

    :param numpy.ndarray data: bin values of histograms
    :param list labels: common bin labels for all histograms
    :param list legend: corresponding names of histograms we want to represent
    :return: base64 encoded plot image (grouped bar chart)
    :rtype: str
    """
    n = data.shape[0]  # number of histograms
    b = data.shape[1]  # number of bins per histogram

    if len(labels) != b:
        raise ValueError("shape mismatch: x-axis labels do not match the data shape")

    if len(legend) != n:
        raise ValueError(
            "shape mismatch: the number of data entry lists does not match the legend shape"
        )

    x = np.arange(b)
    max_width = 0.9
    width = max_width / n

    fig, ax = plt.subplots()
    offset = (1 - n) * width / 2
    for label, row in zip(legend, data):
        ax.bar(x + offset, row, width, label=label)
        offset += width

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontdict={"rotation": "vertical"})
    ax.legend()

    fig.tight_layout()

    return plt_to_str()


def plot_overlay_1d_histogram_b64(
    hists,
    x_label,
    hist_names=[],
    y_label=None,
    is_num=True,
    is_ts=False,
    top=20,
    width_in=None,
    xlim=None,
):
    """Create and plot (overlapping) histogram(s) of column values.

    Copyright Eskapade:
    Kindly taken from Eskapade package and then modified. Reference link:
    https://github.com/KaveIO/Eskapade/blob/master/python/eskapade/visualization/vis_utils.py#L397
    License: https://github.com/KaveIO/Eskapade-Core/blob/master/LICENSE
    Modifications copyright ING WBAA.

    :param list hists: list of input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param list hist_names: list of histogram names. default is [].
    :param str y_label: Label for histogram y-axis. default is None.
    :param bool is_num: True if observable to plot is numeric. default is True.
    :param bool is_ts: True if observable to plot is a timestamp. default is False.
    :param int top: only print the top 20 characters of x-labels and y-labels. default is 20.
    :param float width_in: the width of the bars of the histogram in percentage (0-1). default is None.
    :param tuple xlim: set the x limits of the current axes. default is None.
    :return: base64 encoded plot image
    :rtype: str
    """
    # basic checks
    if len(hist_names) == 0:
        hist_names = [f"hist{i}" for i in range(len(hists))]

    if hist_names:
        if len(hists) != len(hist_names):
            raise ValueError("length of hist and hist_names are different")

    for hist in hists:
        if len(hist) != 2:
            raise ValueError("Cannot extract binning and values from input histogram")
        hist_values, hist_bins = hist
        if hist_values is None or not len(hist_values):
            raise ValueError("Histogram bin values have not been set.")
        if hist_bins is None or not len(hist_bins):
            raise ValueError("Histogram binning has not been set.")

        if is_num or is_ts:
            if len(hist_bins) != len(hist_values) + 1:
                raise ValueError(
                    "bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )
        else:
            if len(hist_bins) != len(hist_values):
                raise ValueError(
                    "labels and values have different array lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )

    y_label = str(y_label) if y_label is not None else "Bin count"

    plt.subplots(figsize=(9, 7))

    alpha = 1.0 / len(hists)

    if is_num or is_ts:
        # plot numeric and time stamps
        for i, (hist_values, hist_bins) in enumerate(hists):
            if is_ts:
                # difference in seconds
                be_tsv = [pd.Timestamp(ts).value for ts in hist_bins]
                width = np.diff(be_tsv)
                # pd.Timestamp(ts).value is in ns
                # matplotlib dates have base of 1 day
                width = width / NUM_NS_DAY
            elif width_in:
                width = width_in
            else:
                width = np.diff(hist_bins)

            # plot histogram
            plt.bar(
                hist_bins[:-1],
                hist_values,
                width=width,
                alpha=alpha,
                label=hist_names[i],
            )

            # set x-axis properties
            if not xlim:
                xlim = min(hist_bins), max(hist_bins)
            plt.xlim(xlim)
            plt.xticks(fontsize=12, rotation=90 if is_ts else 0)
    else:
        # plot categories
        for i, (hist_values, hist_bins) in enumerate(hists):
            # plot histogram
            tick_pos = np.arange(len(hist_bins)) + 0.5
            plt.bar(tick_pos, hist_values, width=0.8, alpha=alpha, label=hist_names[i])

            short_labels = [
                shorten(lab, width=top, placeholder="...") for lab in hist_bins
            ]

            # set x-axis properties
            plt.xlim((0.0, float(len(hist_bins))))
            plt.xticks(tick_pos, short_labels, fontsize=12, rotation=90)

    # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(y_label, fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()
    plt.legend()

    return plt_to_str()


def _prune(values, last_n=0, skip_first_n=0, skip_last_n=0):
    """inline function to select first or last items of input list

    :param values: input list to select from
    :param int last_n: select last 'n' items of values. default is 0.
    :param int skip_first_n: skip first n items of values. default is 0. last_n takes precedence.
    :param int skip_last_n: in plot skip last 'n' periods. last_n takes precedence (optional)
    :return: list of selected values
    """
    if last_n > 0:
        return values[-last_n:]
    if skip_first_n > 0:
        values = values[skip_first_n:]
    if skip_last_n > 0:
        values = values[:-skip_last_n]
    return values


def plot_stacked_1d_histogram_b64(
    hists,
    x_label,
    hist_names=[],
    y_label: Optional[str] = None,
    is_num=True,
    is_ts=False,
    top=20,
    width_in=None,
    xlim=None,
):
    """Create and plot (overlapping) histogram(s) of column values.

    Copyright Eskapade:
    Kindly taken from Eskapade package and then modified. Reference link:
    https://github.com/KaveIO/Eskapade/blob/master/python/eskapade/visualization/vis_utils.py#L397
    License: https://github.com/KaveIO/Eskapade-Core/blob/master/LICENSE
    Modifications copyright ING WBAA.

    :param list hists: list of input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param list hist_names: list of histogram names. default is [].
    :param str y_label: Label for histogram y-axis. default is None.
    :param bool is_num: True if observable to plot is numeric. default is True.
    :param bool is_ts: True if observable to plot is a timestamp. default is False.
    :param int top: only print the top 20 characters of x-labels and y-labels. default is 20.
    :param float width_in: the width of the bars of the histogram in percentage (0-1). default is None.
    :param tuple xlim: set the x limits of the current axes. default is None.
    :return: base64 encoded plot image
    :rtype: str
    """
    # basic checks
    if len(hist_names) == 0:
        hist_names = [f"hist{i}" for i in range(len(hists))]

    if hist_names:
        if len(hists) != len(hist_names):
            raise ValueError("length of hist and hist_names are different")

    for hist in hists:
        if len(hist) != 2:
            raise ValueError("Cannot extract binning and values from input histogram")
        hist_values, hist_bins = hist
        if hist_values is None or not len(hist_values):
            raise ValueError("Histogram bin values have not been set.")
        if hist_bins is None or not len(hist_bins):
            raise ValueError("Histogram binning has not been set.")

        if is_num or is_ts:
            if len(hist_bins) != len(hist_values) + 1:
                raise ValueError(
                    "bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )
        else:
            if len(hist_bins) != len(hist_values):
                raise ValueError(
                    "labels and values have different array lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )

    y_label = str(y_label) if y_label is not None else "Bin count"

    fig, axs = plt.subplots(figsize=(14, 6))

    tick_pos = np.arange(len(hist_names)) + 0.5

    bars_pos = defaultdict(list)
    bars_val = defaultdict(list)

    if is_num or is_ts:
        # plot numeric and time stamps
        # bars_wid = defaultdict(list)

        for i, (hist_values, hist_bins) in enumerate(hists):
            # if is_ts:
            #     # difference in seconds
            #     be_tsv = [pd.Timestamp(ts).value for ts in hist_bins]
            #     width = np.diff(be_tsv)
            #     # pd.Timestamp(ts).value is in ns
            #     # matplotlib dates have base of 1 day
            #     width = width / NUM_NS_DAY
            # elif width_in:
            #     width = width_in
            # else:
            #     width = np.diff(hist_bins)

            for bin_name, bin_value in zip(hist_bins[:-1], hist_values):
                bars_pos[bin_name].append(tick_pos[i])
                bars_val[bin_name].append(bin_value)
                # bars_wid[bin_name].append(width)

    else:
        # plot categories
        for i, (hist_values, hist_bins) in enumerate(hists):
            # plot histogram
            for name, value in zip(hist_bins, hist_values):
                bars_pos[name].append(tick_pos[i])
                bars_val[name].append(value)

    # Threshold: only keep top-n
    if is_num or is_ts:
        min_probs = 0.001
        for ij, name in enumerate(list(bars_pos.keys())):
            prob = np.max(bars_val[name])
            if prob < min_probs:
                del bars_pos[name]
                del bars_val[name]
    else:
        top_n = 25
        if len(bars_pos) > top_n:
            probs = {}
            for ij, name in enumerate(list(bars_pos.keys())):
                probs[name] = np.max(bars_val[name])

            probs = {k: v for k, v in sorted(probs.items(), key=lambda item: item[1])}
            probs = dict(list(probs.items())[top_n:])
            for name in probs.keys():
                del bars_pos[name]
                del bars_val[name]

    b = None
    colors = plt.cm.nipy_spectral(np.linspace(0, 1, len(bars_pos)))
    for ij, name in enumerate(bars_pos.keys()):
        plt.bar(
            bars_pos[name],
            bars_val[name],
            bottom=b,
            width=0.8,
            label=str(name),
            color=colors[ij],
        )
        if b is None:
            b = bars_val[name]
        else:
            b = [v1 + v2 for v1, v2 in zip(b, bars_val[name])]

    # set x-axis properties
    plt.xlim((0.0, float(len(hist_names))))
    plt.xticks(tick_pos, hist_names, fontsize=12, rotation=90)

    # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(y_label, fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()
    if not (is_num or is_ts):
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    else:
        divider = make_axes_locatable(axs)
        cax = divider.append_axes("right", size="5%", pad=0.05)
        # FIXME: bins != edges/boundaries
        norm = BoundaryNorm(list(bars_pos.keys()), len(bars_pos.keys()))
        _ = ColorbarBase(
            cax, cmap=ListedColormap(colors), norm=norm, orientation="vertical"
        )

    fig.tight_layout()

    return plt_to_str()


def plot_heatmap_1d_histogram_b64(
    hists,
    x_label,
    hist_names=[],
    y_label: Optional[str] = None,
    is_num=True,
    is_ts=False,
    top=20,
    width_in=None,
    xlim=None,
):
    """Create and plot (overlapping) histogram(s) of column values.

    Copyright Eskapade:
    Kindly taken from Eskapade package and then modified. Reference link:
    https://github.com/KaveIO/Eskapade/blob/master/python/eskapade/visualization/vis_utils.py#L397
    License: https://github.com/KaveIO/Eskapade-Core/blob/master/LICENSE
    Modifications copyright ING WBAA.

    :param list hists: list of input numpy histogram = values, bin_edges
    :param str x_label: Label for histogram x-axis
    :param list hist_names: list of histogram names. default is [].
    :param str y_label: Label for histogram y-axis. default is None.
    :param bool is_num: True if observable to plot is numeric. default is True.
    :param bool is_ts: True if observable to plot is a timestamp. default is False.
    :param int top: only print the top 20 characters of x-labels and y-labels. default is 20.
    :param float width_in: the width of the bars of the histogram in percentage (0-1). default is None.
    :param tuple xlim: set the x limits of the current axes. default is None.
    :return: base64 encoded plot image
    :rtype: str
    """
    # basic checks
    if len(hist_names) == 0:
        hist_names = [f"hist{i}" for i in range(len(hists))]

    if hist_names:
        if len(hists) != len(hist_names):
            raise ValueError("length of hist and hist_names are different")

    for hist in hists:
        if len(hist) != 2:
            raise ValueError("Cannot extract binning and values from input histogram")
        hist_values, hist_bins = hist
        if hist_values is None or not len(hist_values):
            raise ValueError("Histogram bin values have not been set.")
        if hist_bins is None or not len(hist_bins):
            raise ValueError("Histogram binning has not been set.")

        if is_num or is_ts:
            if len(hist_bins) != len(hist_values) + 1:
                raise ValueError(
                    "bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )
        else:
            if len(hist_bins) != len(hist_values):
                raise ValueError(
                    "labels and values have different array lengths: {:d} vs {:d}. {}".format(
                        len(hist_bins), len(hist_values), x_label
                    )
                )

    fig, axs = plt.subplots()

    tick_pos = np.arange(len(hist_names))

    bars_pos = defaultdict(list)
    bars_val = defaultdict(list)

    if is_num or is_ts:
        # plot numeric and time stamps
        bars_wid = defaultdict(list)

        for i, (hist_values, hist_bins) in enumerate(hists):
            if is_ts:
                # difference in seconds
                be_tsv = [pd.Timestamp(ts).value for ts in hist_bins]
                width = np.diff(be_tsv)
                # pd.Timestamp(ts).value is in ns
                # matplotlib dates have base of 1 day
                width = width / NUM_NS_DAY
            elif width_in:
                width = width_in
            else:
                width = np.diff(hist_bins)

            for bin_name, bin_value in zip(hist_bins[:-1], hist_values):
                bars_pos[bin_name].append(tick_pos[i])
                bars_val[bin_name].append(bin_value)
                bars_wid[bin_name].append(width)

    else:
        # plot categories
        for i, (hist_values, hist_bins) in enumerate(hists):
            # plot histogram
            for name, value in zip(hist_bins, hist_values):
                bars_pos[name].append(tick_pos[i])
                bars_val[name].append(value)

    n_dates = len(hist_names)
    n_vals = len(bars_pos)

    shape = n_vals, n_dates
    data = np.zeros(shape)
    for ij, name in enumerate(bars_pos.keys()):
        for ji, val in enumerate(bars_val[name]):
            data[ij, ji] = val

    im = plt.imshow(
        data,
        interpolation="none",
        # aspect="equal",
        cmap=plt.cm.coolwarm,
    )

    # set x-axis properties
    plt.xticks(tick_pos, hist_names, fontsize=12, rotation=90)
    bin_labels = [
        f"{v:.2f}".rstrip("0").rstrip(".") if isinstance(v, (float, int)) else v
        for v in bars_pos.keys()
    ]
    plt.yticks(list(range(n_vals)), bin_labels, fontsize=12)

    # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel("Values", fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()

    cbar = fig.colorbar(im, ax=axs)
    cbar.set_label("Bin probability")

    # Aspect ratio scaling (approximate)
    w = 0.24 * n_dates + 3.0
    h = 0.24 * n_vals + 3.5
    fig.set_size_inches(w, h)
    fig.tight_layout()

    return plt_to_str()
