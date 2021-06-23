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


from fnmatch import fnmatch

profiles = {
    "count": "Number of entries (non-NaN and NaN)",
    "distinct": "Number of distinct entries",
    "filled": "Number of non-missing entries (non-NaN)",
    "nan": "Number of missing entries (NaN)",
    "overflow": "Number of values larger than the maximum bin-edge of the histogram.",
    "underflow": "Number of values smaller than the minimum bin-edge of the histogram.",
    "min": "Minimum value",
    "max": "Maximum value",
    "mean": "Mean value",
    "most_probable_value": "Most probable value",
    "std": "Standard deviation",
    "phik": "phi-k correlation between the two variables of the histogram",
    "phik_pvalue": "p-value of the contingency test of the 2d histogram",
    "phik_zscore": "Z-score of the contingency test of the 2d histogram",
}

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
    "max_prob_diff": "The largest absolute difference between all bin pairs of two normalized "
    + "histograms (one histogram in a time slot and one in {ref})",
    "unknown_labels": "Are categories observed in a given time slot that are not present in {ref}?",
}

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
        "*max_prob_diff*",
        "*zscore*",
        "n_*",
        "worst",
    ],
}


def get_stat_description(name: str):
    """Gets the description of a statistic.

    :param str name: the name of the statistic.

    :returns str: the description of the statistic. If not found, returns an empty string
    """
    if not isinstance(name, str):
        raise TypeError("Statistic's name should be a string.")

    if name in profiles:
        return profiles[name]
    if name in alerts:
        return alerts[name]

    head, *tail = name.split("_")
    tail = "_".join(tail)

    if tail in comparisons and head in references:
        return comparisons[tail].format(ref=references[head])

    if fnmatch(name, "p[0-9][0-9]"):
        return f"{int(name[1:])}% percentile"

    return ""
