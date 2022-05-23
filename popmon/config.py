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

from popmon.analysis.comparison.comparisons import Comparisons
from popmon.analysis.profiling.profiles import Profiles

profiles = Profiles.get_descriptions()


comparisons = {
    "ks": "Kolmogorov-Smirnov test statistic comparing each time slot to {ref}",
    "ks_zscore": "Z-score of the Kolmogorov-Smirnov test, comparing each time slot with {ref}",
    "ks_pvalue": "p-value of the Kolmogorov-Smirnov test, comparing each time slot with {ref}",
    "pearson": "Pearson correlation between each time slot and {ref}",
    "chi2": "Chi-squared test statistic, comparing each time slot with {ref}",
    "chi2_norm": "Normalized chi-squared statistic, comparing each time slot with {ref}",
    "chi2_pvalue": "p-value of the chi-squared statistic, comparing each time slot with {ref}",
    "chi2_zscore": "Z-score of the chi-squared statistic, comparing each time slot with {ref}",
    "chi2_max_residual": "The largest absolute normalized residual (|chi|) observed in all bin pairs "
    + "(one histogram in a time slot and one in {ref})",
    "chi2_spike_count": "The number of normalized residuals of all bin pairs (one histogram in a time"
    + " slot and one in {ref}) with absolute value bigger than a given threshold (default: 7).",
    "unknown_labels": "Are categories observed in a given time slot that are not present in {ref}?",
}
comparisons.update(Comparisons.get_descriptions())

references = {
    "ref": "the reference data",
    "roll": "a rolling window",
    "prev1": "the preceding time slot",
    "expanding": "all preceding time slots",
}

alerts = {
    "n_green": "Total number of green traffic lights (observed for all statistics)",
    "n_yellow": "Total number of  yellow traffic lights (observed for all statistics)",
    "n_red": "Total number of red traffic lights (observed for all statistics)",
    "worst": "Worst traffic light (observed for all statistics)",
}

section_descriptions = {
    "profiles": """Basic statistics of the data (profiles) calculated for each time period (a period
                   is represented by one bin). The yellow and red lines represent the corresponding
                   traffic light bounds (default: 4 and 7 standard deviations with respect to the reference data).""",
    "comparisons": "Statistical comparisons of each time period (one bin) to the reference data.",
    "traffic_lights": "Traffic light calculation for different statistics (based on the calculated normalized residual, a.k.a. pull). Statistics for which all traffic lights are green are hidden from view by default.",
    "alerts": "Alerts aggregated by all traffic lights for each feature.",
    "histograms": "Histograms of the last few time slots (default: 2).",
    "overview": "Alerts aggregated per feature",
}

histograms = {
    "heatmap": "The heatmap shows the frequency of each value over time. If a variable has a high number of distinct values"
    "(i.e. has a high cardinality), then the most frequent values are displayed and the remaining are grouped as 'Others'. "
    "The maximum number of values to should is configurable (default: 20).",
    "heatmap_column_normalized": "The column-normalized heatmap allows for comparing of time bins when the counts in each bin vary.",
    "heatmap_row_normalized": "The row-normalized heatmaps allows for monitoring one value over time.",
}

config = {
    "section_descriptions": section_descriptions,
    "limited_stats": [
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
        "*ks*",
        "*zscore*",
        "n_*",
        "worst",
    ],
}
for key in Comparisons.get_comparisons().keys():
    config["limited_stats"].append(f"*{key}*")


def get_stat_description(name: str):
    """Gets the description of a statistic.

    :param str name: the name of the statistic.

    :returns str: the description of the statistic. If not found, returns an empty string
    """
    if not isinstance(name, str):
        raise TypeError("Statistic's name should be a string.")

    if name in histograms:
        return histograms[name]
    if name in profiles:
        return profiles[name]
    if name in alerts:
        return alerts[name]

    head, *tail = name.split("_")
    tail = "_".join(tail)

    if tail in comparisons and head in references:
        return comparisons[tail].format(ref=references[head])

    return ""


# Global configuration for the joblib parallelization. Could be used to change the number of jobs, and/or change
# the backend from default (loki) to 'multiprocessing' or 'threading'.
# (see https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html for details)
parallel_args = {"n_jobs": 1}

# Usage the `ing_matplotlib_theme`
themed = True
