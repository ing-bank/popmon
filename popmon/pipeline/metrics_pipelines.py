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


from ..alerting import (
    AlertsSummary,
    DynamicBounds,
    StaticBounds,
    TrafficLightAlerts,
    traffic_light_summary,
)
from ..analysis.apply_func import ApplyFunc
from ..analysis.comparison.hist_comparer import (
    ExpandingHistComparer,
    PreviousHistComparer,
    ReferenceHistComparer,
    RollingHistComparer,
)
from ..analysis.functions import rolling_lr_zscore
from ..analysis.profiling import HistProfiler
from ..analysis.profiling.pull_calculator import (
    ExpandingPullCalculator,
    ReferencePullCalculator,
    RefMedianMadPullCalculator,
    RollingPullCalculator,
)
from ..base import Pipeline
from ..hist.hist_splitter import HistSplitter


def metrics_self_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    monitoring_rules={},
    pull_rules={},
    features=None,
    **kwargs,
):
    """Example metrics pipeline for comparing test data with itself (full test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date'
    :param int window: window size for trend detection. default is 10
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param kwargs: residual keyword arguments
    :return: assembled self reference pipeline
    """
    modules = [
        # --- 1. splitting of test histograms
        HistSplitter(
            read_key=hists_key,
            store_key="split_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        # --- 2. for each histogram with datetime i, comparison of histogram i with histogram i-1, results in
        #        chi2 comparison of histograms
        PreviousHistComparer(read_key="split_hists", store_key="comparisons"),
        # --- 3. Comparison of with profiled test histograms, results in chi2 comparison of histograms
        ReferenceHistComparer(
            reference_key="split_hists",
            assign_to_key="split_hists",
            store_key="comparisons",
        ),
        RefMedianMadPullCalculator(
            reference_key="comparisons",
            assign_to_key="comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
            metrics=["ref_max_prob_diff"],
        ),
        # --- 4. profiling of histograms, then pull calculation compared with reference mean and std,
        #        to obtain normalized residuals of profiles
        HistProfiler(read_key="split_hists", store_key="profiles"),
        RefMedianMadPullCalculator(
            reference_key="profiles",
            assign_to_key="profiles",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
        ),
        # --- 5. looking for significant rolling linear trends in selected features/metrics
        ApplyFunc(
            apply_to_key="profiles",
            assign_to_key="comparisons",
            apply_funcs=[
                {
                    "func": rolling_lr_zscore,
                    "suffix": f"_trend{window}_zscore",
                    "entire": True,
                    "window": window,
                    "metrics": ["mean", "phik", "fraction_true"],
                }
            ],
            msg="Computing significance of (rolling) trend in means of features",
        ),
        # --- 6. generate dynamic traffic light boundaries, based on traffic lights for normalized residuals,
        #        used for plotting in popmon_profiles report.
        StaticBounds(
            read_key="profiles",
            rules=pull_rules,
            store_key="dynamic_bounds",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        StaticBounds(
            read_key="comparisons",
            rules=pull_rules,
            store_key="dynamic_bounds_comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        # --- 7. expand all (wildcard) static traffic light bounds and apply them.
        #        Applied to both profiles and comparisons datasets
        TrafficLightAlerts(
            read_key="profiles",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds",
        ),
        TrafficLightAlerts(
            read_key="comparisons",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds_comparisons",
        ),
        ApplyFunc(
            apply_to_key="traffic_lights",
            apply_funcs=[{"func": traffic_light_summary, "axis": 1, "suffix": ""}],
            assign_to_key="alerts",
            msg="Generating traffic light alerts summary.",
        ),
        AlertsSummary(read_key="alerts"),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def metrics_external_reference(
    hists_key="test_hists",
    ref_hists_key="ref_hists",
    time_axis="date",
    window=10,
    monitoring_rules={},
    pull_rules={},
    features=None,
    **kwargs,
):
    """Example metrics pipeline for comparing test data with other (full) external reference set

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str ref_hists_key: key to reference histograms in datastore. default is 'ref_hists'
    :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
    :param int window: window size for trend detection. default is 10
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param kwargs: residual keyword arguments
    :return: assembled external reference pipeline
    """
    modules = [
        # --- 1. splitting of test histograms
        HistSplitter(
            read_key=hists_key,
            store_key="split_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        # --- 2. for each histogram with datetime i, comparison of histogram i with histogram i-1, results in
        #        chi2 comparison of histograms
        PreviousHistComparer(read_key="split_hists", store_key="comparisons"),
        # --- 3. Profiling of split reference histograms, then chi2 comparison with test histograms
        HistSplitter(
            read_key=ref_hists_key,
            store_key="split_ref_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        ReferenceHistComparer(
            reference_key="split_ref_hists",
            assign_to_key="split_hists",
            store_key="comparisons",
        ),
        RefMedianMadPullCalculator(
            reference_key="comparisons",
            assign_to_key="comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
            metrics=["ref_max_prob_diff"],
        ),
        # --- 4. pull calculation compared with reference mean and std, to obtain normalized residuals of profiles
        HistProfiler(read_key="split_hists", store_key="profiles"),
        HistProfiler(read_key="split_ref_hists", store_key="ref_profiles"),
        ReferencePullCalculator(
            reference_key="ref_profiles",
            assign_to_key="profiles",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
        ),
        # --- 5. looking for significant rolling linear trends in selected features/metrics
        ApplyFunc(
            apply_to_key="profiles",
            assign_to_key="comparisons",
            apply_funcs=[
                {
                    "func": rolling_lr_zscore,
                    "suffix": f"_trend{window}_zscore",
                    "entire": True,
                    "window": window,
                    "metrics": ["mean", "phik", "fraction_true"],
                }
            ],
            msg="Computing significance of (rolling) trend in means of features",
        ),
        # --- 6. generate dynamic traffic light boundaries, based on traffic lights for normalized residuals,
        #        used for plotting in popmon_profiles report.
        StaticBounds(
            read_key="profiles",
            rules=pull_rules,
            store_key="dynamic_bounds",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        StaticBounds(
            read_key="comparisons",
            rules=pull_rules,
            store_key="dynamic_bounds_comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        # --- 7. expand all (wildcard) static traffic light bounds and apply them.
        #        Applied to both profiles and comparisons datasets
        TrafficLightAlerts(
            read_key="profiles",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds",
        ),
        TrafficLightAlerts(
            read_key="comparisons",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds_comparisons",
        ),
        ApplyFunc(
            apply_to_key="traffic_lights",
            apply_funcs=[{"func": traffic_light_summary, "axis": 1, "suffix": ""}],
            assign_to_key="alerts",
            msg="Generating traffic light alerts summary.",
        ),
        AlertsSummary(read_key="alerts"),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def metrics_rolling_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    shift=1,
    monitoring_rules={},
    pull_rules={},
    features=None,
    **kwargs,
):
    """Example metrics pipeline for comparing test data with itself (rolling test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date'
    :param int window: size of rolling window and for trend detection. default is 10
    :param int shift: shift in rolling window. default is 1
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param kwargs: residual keyword arguments
    :return: assembled rolling reference pipeline
    """
    modules = [
        # --- 1. splitting of test histograms
        HistSplitter(
            read_key=hists_key,
            store_key="split_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        # --- 2. for each histogram with datetime i, comparison of histogram i with histogram i-1, results in
        #        chi2 comparison of histograms
        PreviousHistComparer(read_key="split_hists", store_key="comparisons"),
        # --- 3. profiling of reference histograms, then comparison of with profiled test histograms
        #        results in chi2 comparison of histograms
        RollingHistComparer(
            read_key="split_hists", window=window, shift=shift, store_key="comparisons"
        ),
        RefMedianMadPullCalculator(
            reference_key="comparisons",
            assign_to_key="comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
            metrics=["roll_max_prob_diff"],
        ),
        # --- 4. profiling of histograms, then pull calculation compared with reference mean and std,
        #        to obtain normalized residuals of profiles
        HistProfiler(read_key="split_hists", store_key="profiles"),
        RollingPullCalculator(
            read_key="profiles",
            window=window,
            shift=shift,
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
        ),
        # --- 5. looking for significant rolling linear trends in selected features/metrics
        ApplyFunc(
            apply_to_key="profiles",
            assign_to_key="comparisons",
            apply_funcs=[
                {
                    "func": rolling_lr_zscore,
                    "suffix": f"_trend{window}_zscore",
                    "entire": True,
                    "window": window,
                    "metrics": ["mean", "phik", "fraction_true"],
                }
            ],
            msg="Computing significance of (rolling) trend in means of features",
        ),
        # --- 6. generate dynamic traffic light boundaries, based on traffic lights for normalized residuals,
        #        used for plotting in popmon_profiles report.
        DynamicBounds(
            read_key="profiles",
            rules=pull_rules,
            store_key="dynamic_bounds",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        DynamicBounds(
            read_key="comparisons",
            rules=pull_rules,
            store_key="dynamic_bounds_comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        # --- 7. expand all (wildcard) static traffic light bounds and apply them.
        #        Applied to both profiles and comparisons datasets
        TrafficLightAlerts(
            read_key="profiles",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds",
        ),
        TrafficLightAlerts(
            read_key="comparisons",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds_comparisons",
        ),
        ApplyFunc(
            apply_to_key="traffic_lights",
            apply_funcs=[{"func": traffic_light_summary, "axis": 1, "suffix": ""}],
            assign_to_key="alerts",
            msg="Generating traffic light alerts summary.",
        ),
        AlertsSummary(read_key="alerts"),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def metrics_expanding_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    shift=1,
    monitoring_rules={},
    pull_rules={},
    features=None,
    **kwargs,
):
    """Example metrics pipeline for comparing test data with itself (expanding test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date'
    :param int window: window size for trend detection. default is 10
    :param int shift: shift in expanding window. default is 1
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param kwargs: residual keyword arguments
    :return: assembled expanding reference pipeline
    """
    modules = [
        # --- 1. splitting of test histograms
        HistSplitter(
            read_key=hists_key,
            store_key="split_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        # --- 2. for each histogram with datetime i, comparison of histogram i with histogram i-1, results in
        #        chi2 comparison of histograms
        PreviousHistComparer(read_key="split_hists", store_key="comparisons"),
        # --- 3. profiling of reference histograms, then comparison of with profiled test histograms
        #        results in chi2 comparison of histograms
        ExpandingHistComparer(
            read_key="split_hists", shift=shift, store_key="comparisons"
        ),
        # --- 4. profiling of histograms, then pull calculation compared with reference mean and std,
        #        to obtain normalized residuals of profiles
        RefMedianMadPullCalculator(
            reference_key="comparisons",
            assign_to_key="comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
            metrics=["expanding_max_prob_diff"],
        ),
        HistProfiler(read_key="split_hists", store_key="profiles"),
        ExpandingPullCalculator(
            read_key="profiles",
            shift=shift,
            suffix_mean="_mean",
            suffix_std="_std",
            suffix_pull="_pull",
        ),
        # --- 5. looking for significant rolling linear trends in selected features/metrics
        ApplyFunc(
            apply_to_key="profiles",
            assign_to_key="comparisons",
            apply_funcs=[
                {
                    "func": rolling_lr_zscore,
                    "suffix": f"_trend{window}_zscore",
                    "entire": True,
                    "window": window,
                    "metrics": ["mean", "phik", "fraction_true"],
                }
            ],
            msg="Computing significance of (rolling) trend in means of features",
        ),
        # --- 6. generate dynamic traffic light boundaries, based on traffic lights for normalized residuals,
        #        used for plotting in popmon_profiles report.
        DynamicBounds(
            read_key="profiles",
            rules=pull_rules,
            store_key="dynamic_bounds",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        DynamicBounds(
            read_key="comparisons",
            rules=pull_rules,
            store_key="dynamic_bounds_comparisons",
            suffix_mean="_mean",
            suffix_std="_std",
        ),
        # --- 7. expand all (wildcard) static traffic light bounds and apply them.
        #        Applied to both profiles and comparisons datasets
        TrafficLightAlerts(
            read_key="profiles",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds",
        ),
        TrafficLightAlerts(
            read_key="comparisons",
            rules=monitoring_rules,
            store_key="traffic_lights",
            expanded_rules_key="static_bounds_comparisons",
        ),
        ApplyFunc(
            apply_to_key="traffic_lights",
            apply_funcs=[{"func": traffic_light_summary, "axis": 1, "suffix": ""}],
            assign_to_key="alerts",
            msg="Generating traffic light alerts summary.",
        ),
        AlertsSummary(read_key="alerts"),
    ]

    pipeline = Pipeline(modules)
    return pipeline
