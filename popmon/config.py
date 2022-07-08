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
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from histogrammar.dfinterface.make_histograms import get_time_axes
from pydantic import BaseModel, BaseSettings
from typing_extensions import Literal

# Global configuration for the joblib parallelization. Could be used to change the number of jobs, and/or change
# the backend from default (loki) to 'multiprocessing' or 'threading'.
# (see https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html for details)
parallel_args = {"n_jobs": 1}


class SectionModel(BaseModel):
    name: str
    """Name of the section in the report"""

    description: str
    """Description of the section in the report"""


class ProfilesSection(SectionModel):
    name = "Profiles"
    """Name of the profiles section in the report"""

    description = """Basic statistics of the data (profiles) calculated for each time period (a period
                       is represented by one bin). The yellow and red lines represent the corresponding
                       traffic light bounds (default: 4 and 7 standard deviations with respect to the reference data)."""
    """Description of the profiles section in the report"""


class AlertSection(SectionModel):
    name = "Alerts"
    """Name of the alerts section in the report"""

    description = "Alerts aggregated by all traffic lights for each feature."
    """Description of the alerts section in the report"""

    descriptions: Dict[Literal["n_green", "n_yellow", "n_red"], str] = {
        "n_green": "Total number of green traffic lights (observed for all statistics)",
        "n_yellow": "Total number of  yellow traffic lights (observed for all statistics)",
        "n_red": "Total number of red traffic lights (observed for all statistics)",
    }
    """Descriptions of the individual alerts"""


class HistogramSectionModel(SectionModel):
    name = "Histograms"
    """Name of the histograms section in the report"""

    description = "Histograms of the last few time slots (default: 2)."
    """Description of the histograms section in the report"""

    hist_names: List[
        Literal["heatmap", "heatmap_column_normalized", "heatmap_row_normalized"]
    ] = [
        "heatmap",
        "heatmap_column_normalized",
        "heatmap_row_normalized",
    ]
    """Heatmaps of histograms to display in the report"""

    hist_names_formatted: Dict[
        Literal["heatmap", "heatmap_column_normalized", "heatmap_row_normalized"], str
    ] = {
        "heatmap": "Heatmap",
        "heatmap_column_normalized": "Column-Normalized Heatmap",
        "heatmap_row_normalized": "Row-Normalized Heatmap",
    }
    """Pretty-print names for the heatmaps"""

    descriptions: Dict[
        Literal["heatmap", "heatmap_column_normalized", "heatmap_row_normalized"], str
    ] = {
        "heatmap": "The heatmap shows the frequency of each value over time. If a variable has a high number of distinct values"
        "(i.e. has a high cardinality), then the most frequent values are displayed and the remaining are grouped as 'Others'. "
        "The maximum number of values to should is configurable (default: 20).",
        "heatmap_column_normalized": "The column-normalized heatmap allows for comparing of time bins when the counts in each bin vary.",
        "heatmap_row_normalized": "The row-normalized heatmaps allows for monitoring one value over time.",
    }
    """Descriptions of the heatmaps in the report"""

    plot_hist_n: int = 2
    """plot histograms for last 'n' periods. default is 2 (optional)"""

    top_n: int = 20
    """plot heatmap for top 'n' categories. default is 20 (optional)"""

    cmap: str = "ylorrd"
    """colormap for histogram heatmaps"""


class TrafficLightsSection(SectionModel):
    name = "Traffic Lights"
    """Name of the traffic lights section in the report"""

    description = "Traffic light calculation for different statistics (based on the calculated normalized residual, a.k.a. pull). Statistics for which all traffic lights are green are hidden from view by default."
    """Description of the traffic lights section in the report"""


class ComparisonsSection(SectionModel):
    name = "Comparisons"
    """Name of the comparisons section in the report"""

    description = (
        "Statistical comparisons of each time period (one bin) to the reference data."
    )
    """Description of the comparisons section in the report"""


class OverviewSection(SectionModel):
    name = "Overview"
    """Name of the overview section in the report"""

    description = "Alerts aggregated per feature"
    """Description of the overview section in the report"""


class Section(BaseModel):
    """Configuration for the individual sections"""

    profiles: ProfilesSection = ProfilesSection()
    alerts: AlertSection = AlertSection()
    histograms: HistogramSectionModel = HistogramSectionModel()
    overview: OverviewSection = OverviewSection()
    comparisons: ComparisonsSection = ComparisonsSection()
    traffic_lights: TrafficLightsSection = TrafficLightsSection()


class Report(BaseModel):
    """Report-specific configuration"""

    title = "POPMON Report"
    """Report title in browser and navbar. May contain HTML."""

    skip_empty_plots: bool = True
    """if false, also show empty plots in report with only nans or zeroes (optional)"""

    last_n: int = 0
    """plot statistic data for last 'n' periods (optional)"""

    skip_first_n: int = 0
    """in plot skip first 'n' periods. last_n takes precedence (optional)"""

    skip_last_n: int = 0
    """in plot skip last 'n' periods. last_n takes precedence (optional)"""

    report_filepath: Optional[Union[str, Path]] = None
    """the file path where to output the report (optional)"""

    extended_report: bool = True
    """if True, show all the generated statistics in the report (optional)
    if set to False, then smaller show_stats (see below)"""

    online_report: bool = True
    """Use a CDN to host resources, or embed them into the report."""

    show_stats: List[str] = [
        "distinct*",
        "filled*",
        "nan*",
        "mean*",
        "std*",
        "p05*",
        "p50*",
        "p95*",
        "max*",
        "min*",
        "fraction_true*",
        "phik*",
        "*unknown_labels*",
        "*chi2_norm*",
        "*zscore*",
        "n_*",
        "*jsd*",
        "*psi*",
        "*max_prob_diff*",
    ]
    """list of statistic name patterns to show in the report. If None, show all (optional)"""

    primary_color = "#000080"
    """Primary color used throughout the report"""

    tl_colors: Dict[str, str] = {
        "green": "#008000",
        "yellow": "#FFC800",
        "red": "#FF0000",
    }
    """"Configure line colors in barplots of Comparisons and Profiles section. Need to be hex format (full length)"""

    section: Section = Section()
    """Configuration for the individual sections"""


class Comparison(BaseModel):
    """Parameters related to comparisons"""

    window: int = 10
    """size of rolling window and/or trend detection. default is 10."""

    shift: int = 1
    """shift of time-bins in rolling/expanding window. default is 1."""


class Monitoring(BaseModel):
    """Parameters related to monitoring"""

    monitoring_rules: Dict[str, List[Union[float, int]]] = {
        "*_pull": [7, 4, -4, -7],
        "*_zscore": [7, 4, -4, -7],
        "[!p]*_unknown_labels": [0.5, 0.5, 0, 0],
    }
    """
    monitoring rules to generate traffic light alerts.
    The default setting is:

    .. code-block:: python

        monitoring_rules = {
            "*_pull": [7, 4, -4, -7],
            "*_zscore": [7, 4, -4, -7],
            "[!p]*_unknown_labels": [0.5, 0.5, 0, 0],
        }

    Note that the (filename based) wildcards such as * apply to all statistic names matching that pattern.
    For example, ``"*_pull"`` applies for all features to all statistics ending on "_pull".
    You can also specify rules for specific features and/or statistics by leaving out wildcard and putting the
    feature name in front. E.g.

    .. code-block:: python

        monitoring_rules = {
            "featureA:*_pull": [5, 3, -3, -5],
            "featureA:nan": [4, 1, 0, 0],
            "*_pull": [7, 4, -4, -7],
            "nan": [8, 1, 0, 0],
        }

    In case of multiple rules could apply for a feature's statistic, the most specific one applies.
    So in case of the statistic "nan": "featureA:nan" is used for "featureA", and the other "nan" rule
    for all other features.
    """

    pull_rules: Dict[str, List[Union[float, int]]] = {"*_pull": [7, 4, -4, -7]}
    """
    red and yellow (possibly dynamic) boundaries shown in plots in the report.
    Default is:

    .. code-block:: python

        pull_rules = {"*_pull": [7, 4, -4, -7]}

    This means that the shown yellow boundaries are at -4, +4 standard deviations around the (reference) mean,
    and the shown red boundaries are at -7, +7 standard deviations around the (reference) mean.
    Note that the (filename based) wildcards such as * apply to all statistic names matching that pattern.
    (The same string logic applies as for monitoring_rules.)
    """


class Settings(BaseSettings):
    report: Report = Report()
    """Settings regarding the report"""

    comparison: Comparison = Comparison()
    """Settings related to the comparisons"""

    monitoring: Monitoring = Monitoring()
    """Settings related to monitoring"""

    time_axis: str = ""
    """
    name of datetime feature, used as time axis, e.g. 'date'. (column should be timestamp, date(time) or numeric batch id)
    if empty string, will be auto-guessed.
    """

    reference_type: Literal[
        "self", "external", "rolling", "expanding", "self_split"
    ] = "self"
    """
    type of reference used for comparisons
    """

    features: Optional[List[str]] = None
    """
    columns to pick up from input data. (default is all features).
    For multi-dimensional histograms, separate the column names with a ':'. Example features list is:

        .. code-block:: python

            features = ["x", "date", "date:x", "date:y", "date:x:y"]

    If time_axis is set or found, and if no features provided, features becomes: ['date:x', 'date:y', 'date:z'] etc.
    """

    binning: Literal["auto", "unit"] = "auto"
    """
    default binning to revert to in case bin_specs not supplied. When using "auto", semi-clever binning
    is automatically done.
    """

    bin_specs: Dict[str, Any] = {}
    """
    dictionaries used for rebinning numeric or timestamp features.
    An example bin_specs dictionary is:

    .. code-block:: python

        bin_specs = {
            "x": {"bin_width": 1, "bin_offset": 0},
            "y": {"num": 10, "low": 0.0, "high": 2.0},
            "x:y": [{}, {"num": 5, "low": 0.0, "high": 1.0}],
        }

    In the bin specs for x:y, x is not provided (here) and reverts to the 1-dim setting.
    The 'bin_width', 'bin_offset' notation makes an open-ended histogram (for that feature) with given bin width
    and offset. The notation 'num', 'low', 'high' gives a fixed range histogram from 'low' to 'high' with 'num'
    number of bins.
    """

    # Config utilities
    def ensure_features_time_axis(self):
        self.features = [
            c if c.startswith(self.time_axis) else f"{self.time_axis}:{c}"
            for c in self.features
        ]

    def set_time_axis_dataframe(self, df):
        time_axes = get_time_axes(df)
        num = len(time_axes)
        if num == 1:
            self.time_axis = time_axes[0]
        elif num == 0:
            raise ValueError(
                "No obvious time-axes found. Cannot generate stability report."
            )
        else:
            raise ValueError(
                f"Found {num} time-axes: {time_axes}. Set *one* time_axis manually!"
            )

    def set_time_axis_hists(self, hists):
        # auto guess the time_axis: find the most frequent first column name in the histograms list
        first_cols = [k.split(":")[0] for k in list(hists.keys())]
        self.time_axis = max(set(first_cols), key=first_cols.count)

    def set_bin_specs_by_time_width_and_offset(
        self, time_width: Union[str, int, float], time_offset: Union[str, int, float]
    ):
        if self.time_axis in self.bin_specs:
            raise ValueError(
                f'time-axis "{self.time_axis}" already found in binning specifications.'
            )
        # convert time width and offset to nanoseconds
        self.bin_specs[self.time_axis] = {
            "bin_width": float(pd.Timedelta(time_width).value),
            "bin_offset": float(pd.Timestamp(time_offset).value),
        }

    class Config:
        validate_all = True
        validate_assignment = True
