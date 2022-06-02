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
from typing import Literal, Optional, Union

from pydantic import BaseModel, BaseSettings
from pydantic.fields import Field

# Global configuration for the joblib parallelization. Could be used to change the number of jobs, and/or change
# the backend from default (loki) to 'multiprocessing' or 'threading'.
# (see https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html for details)
parallel_args = {"n_jobs": 1}

# Usage the `ing_matplotlib_theme`
themed = True


class ProfilesSection(BaseModel):
    name = "Profiles"
    description = """Basic statistics of the data (profiles) calculated for each time period (a period
                       is represented by one bin). The yellow and red lines represent the corresponding
                       traffic light bounds (default: 4 and 7 standard deviations with respect to the reference data)."""


class AlertSection(BaseModel):
    name = "Alerts"
    description = "Alerts aggregated by all traffic lights for each feature."

    descriptions = {
        "n_green": "Total number of green traffic lights (observed for all statistics)",
        "n_yellow": "Total number of  yellow traffic lights (observed for all statistics)",
        "n_red": "Total number of red traffic lights (observed for all statistics)",
    }


class HistogramSectionModel(BaseModel):
    name = "Histograms"
    description = "Histograms of the last few time slots (default: 2)."

    hist_names: list[
        Literal["heatmap", "heatmap_column_normalized", "heatmap_row_normalized"]
    ] = [
        "heatmap",
        "heatmap_column_normalized",
        "heatmap_row_normalized",
    ]
    hist_names_formatted = {
        "heatmap": "Heatmap",
        "heatmap_column_normalized": "Column-Normalized Heatmap",
        "heatmap_row_normalized": "Row-Normalized Heatmap",
    }
    descriptions = {
        "heatmap": "The heatmap shows the frequency of each value over time. If a variable has a high number of distinct values"
        "(i.e. has a high cardinality), then the most frequent values are displayed and the remaining are grouped as 'Others'. "
        "The maximum number of values to should is configurable (default: 20).",
        "heatmap_column_normalized": "The column-normalized heatmap allows for comparing of time bins when the counts in each bin vary.",
        "heatmap_row_normalized": "The row-normalized heatmaps allows for monitoring one value over time.",
    }
    plot_hist_n: int = 2
    cmap: str = "autumn_r"


class TrafficLightsSection(BaseModel):
    name = "Traffic Lights"
    description = "Traffic light calculation for different statistics (based on the calculated normalized residual, a.k.a. pull). Statistics for which all traffic lights are green are hidden from view by default."


class ComparisonsSection(BaseModel):
    name = "Comparisons"
    description = (
        "Statistical comparisons of each time period (one bin) to the reference data."
    )


class OverviewSection(BaseModel):
    name = "Overview"
    description = "Alerts aggregated per feature"


class Section(BaseModel):
    profiles: ProfilesSection = ProfilesSection()
    alerts: AlertSection = AlertSection()
    histograms: HistogramSectionModel = HistogramSectionModel()
    overview: OverviewSection = OverviewSection()
    comparisons: ComparisonsSection = ComparisonsSection()
    traffic_lights: TrafficLightsSection = TrafficLightsSection()


def get_stats():
    from popmon.analysis.comparison.comparisons import Comparisons

    comparisons = Comparisons.get_descriptions()

    stats = [
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
    ]

    for key in comparisons.keys():
        stats.append(f"*{key}*")

    return stats


class Report(BaseModel):
    """Report-specific configuration"""

    skip_empty_plots: bool = True
    last_n: int = 0
    skip_first_n: int = 0
    skip_last_n: int = 0
    report_filepath: Optional[Union[str, Path]] = None
    # if set to false, then smaller show_stats
    # if limited report is selected, check if stats list is provided, if not, get a default minimal list
    # show_stats = show_stats if not extended_report else None
    extended_report: bool = True
    show_stats: list[str] = Field(default_factory=get_stats)
    section: Section = Section()
    top_n: int = 20


class Comparison(BaseModel):
    window = 10
    shift = 1


class Monitoring(BaseModel):
    monitoring_rules: dict[str, list[float]] = {
        "*_pull": [7, 4, -4, -7],
        "*_zscore": [7, 4, -4, -7],
        "[!p]*_unknown_labels": [0.5, 0.5, 0, 0],
    }
    pull_rules: dict[str, list[float]] = {"*_pull": [7, 4, -4, -7]}


class Settings(BaseSettings):
    report: Report = Report()
    comparison: Comparison = Comparison()
    monitoring: Monitoring = Monitoring()

    @classmethod
    def get_keys(cls):
        aliases = {}
        ambiguous = []
        for key, value in cls.schema()["properties"].items():
            if key in aliases:
                ambiguous.append(key)
                del aliases[key]
            elif key in ambiguous:
                continue

            if "allOf" in value:
                for skey, svalue in value["default"].items():
                    if skey in aliases:
                        ambiguous.append(key)
                        del aliases[key]
                    else:
                        aliases[skey] = (key, skey)
            else:
                aliases[key] = key
        return aliases
