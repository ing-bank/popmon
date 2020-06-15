# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import logging
import math
from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pybase64

NUM_NS_DAY = 24 * 3600 * int(1e9)

logger = logging.getLogger()


def plt_to_base64():
    """Outputting plot as a base64 encoded string.

    :return: base64 encoded plot image
    :rtype:   str
    """
    tmpfile = BytesIO()

    plt.savefig(tmpfile, format="png")
    plt.close()

    return pybase64.b64encode(tmpfile.getvalue()).decode("utf-8")


def plot_bars_b64(data, labels=None, bounds=None, ylim=False, skip_empty=True):
    """ Plotting histogram data.

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
    ax.bar(index, data, width=width, align="center", alpha=0.5)

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
    return plt_to_base64()


def plot_traffic_lights_b64(data, labels=None, skip_empty=True):
    """ Plotting histogram data.

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

    COLORS = ["green", "yellow", "red"]
    ones = np.ones(n)

    index = np.arange(n)

    for i, color in enumerate(COLORS):
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

    return plt_to_base64()


def grouped_bar_chart_b64(data, labels, legend):
    """ Plotting grouped histogram data.

    :param numpy.ndarray data: bin values of histograms
    :param list labels: common bin labels for all histograms
    :param list legend: corresponing names of histograms we want to represent
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

    return plt_to_base64()


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

    plt.figure(figsize=(9, 7))

    alpha = 1.0 / len(hists)
    for i, hist in enumerate(hists):
        try:
            hist_values = hist[0]
            hist_bins = hist[1]
        except BaseException:
            raise ValueError("Cannot extract binning and values from input histogram")

        assert hist_values is not None and len(
            hist_values
        ), "Histogram bin values have not been set."
        assert hist_bins is not None and len(
            hist_bins
        ), "Histogram binning has not been set."

        # basic attribute check: time stamps treated as numeric.
        if is_ts:
            is_num = True

        # plot numeric and time stamps
        if is_num:
            bin_edges = hist_bins
            bin_values = hist_values
            assert (
                len(bin_edges) == len(bin_values) + 1
            ), "bin edges (+ upper edge) and bin values have inconsistent lengths: {:d} vs {:d}. {}".format(
                len(bin_edges), len(bin_values), x_label
            )

            if is_ts:
                # difference in seconds
                be_tsv = [pd.Timestamp(ts).value for ts in bin_edges]
                width = np.diff(be_tsv)
                # pd.Timestamp(ts).value is in ns
                # maplotlib dates have base of 1 day
                width = width / NUM_NS_DAY
            elif width_in:
                width = width_in
            else:
                width = np.diff(bin_edges)

            # plot histogram
            plt.bar(
                bin_edges[:-1],
                bin_values,
                width=width,
                alpha=alpha,
                label=hist_names[i],
            )

            # set x-axis properties
            if xlim:
                plt.xlim(xlim)
            else:
                plt.xlim(min(bin_edges), max(bin_edges))
            plt.xticks(fontsize=12, rotation=90 if is_ts else 0)

        # plot categories
        else:
            labels = hist_bins
            values = hist_values
            assert len(labels) == len(
                values
            ), "labels and values have different array lengths: {:d} vs {:d}. {}".format(
                len(labels), len(values), x_label
            )

            # plot histogram
            tick_pos = np.arange(len(labels)) + 0.5
            plt.bar(tick_pos, values, width=0.8, alpha=alpha, label=hist_names[i])

            # set x-axis properties
            def xtick(lab):
                """Get x-tick."""
                lab = str(lab)
                if len(lab) > top:
                    lab = lab[:17] + "..."
                return lab

            plt.xlim((0.0, float(len(labels))))
            plt.xticks(
                tick_pos, [xtick(lab) for lab in labels], fontsize=12, rotation=90
            )

    # set common histogram properties
    plt.xlabel(x_label, fontsize=14)
    plt.ylabel(str(y_label) if y_label is not None else "Bin count", fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid()
    plt.legend()

    return plt_to_base64()
