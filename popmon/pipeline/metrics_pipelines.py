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
from typing import List, Union

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
from ..base import Module, Pipeline
from ..config import Settings
from ..hist.hist_splitter import HistSplitter


def get_metrics_pipeline_class(reference_type, reference):
    _metrics_pipeline_register = {
        "self": SelfReferenceMetricsPipeline,
        "external": ExternalReferenceMetricsPipeline,
        "self_split": ExternalReferenceMetricsPipeline,
        "rolling": RollingReferenceMetricsPipeline,
        "expanding": ExpandingReferenceMetricsPipeline,
    }

    if reference_type not in _metrics_pipeline_register:
        raise ValueError(
            f"reference_type should be in {str(_metrics_pipeline_register.keys())}'."
        )
    if (
        reference_type in ["external", "self_split"]
        and not isinstance(reference, dict)
        and reference is not None
    ):
        raise TypeError("reference should be a dict of histogrammar histograms.")

    return _metrics_pipeline_register[reference_type]


def create_metrics_pipeline(
    settings: Settings,
    reference_type="self",
    reference=None,
    hists_key="hists",
    **kwargs,
):
    # configuration and datastore for report pipeline
    cfg = {
        "hists_key": hists_key,
        "settings": settings,
        **kwargs,
    }

    # execute reporting pipeline
    cls = get_metrics_pipeline_class(reference_type, reference)
    pipeline = cls(**cfg)
    return pipeline


def get_splitting_modules(
    hists_key, features, time_axis
) -> List[Union[Module, Pipeline]]:
    """
    Splitting of test histograms. For each histogram with datetime i, comparison of histogram i with histogram i-1,
    results in chi2 comparison of histograms
    """
    modules: List[Union[Module, Pipeline]] = [
        HistSplitter(
            read_key=hists_key,
            store_key="split_hists",
            features=features,
            feature_begins_with=f"{time_axis}:",
        ),
        PreviousHistComparer(read_key="split_hists", store_key="comparisons"),
        HistProfiler(read_key="split_hists", store_key="profiles"),
    ]
    return modules


def get_traffic_light_modules(monitoring_rules) -> List[Union[Module, Pipeline]]:
    """
    Expand all (wildcard) static traffic light bounds and apply them.
    Applied to both profiles and comparisons datasets
    """
    modules: List[Union[Module, Pipeline]] = [
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
    return modules


def get_static_bound_modules(pull_rules) -> List[Union[Module, Pipeline]]:
    """
    generate dynamic traffic light boundaries, based on traffic lights for normalized residuals, used for
    plotting in popmon_profiles report.
    """
    modules: List[Union[Module, Pipeline]] = [
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
    ]
    return modules


def get_dynamic_bound_modules(pull_rules) -> List[Union[Module, Pipeline]]:
    """
    Generate dynamic traffic light boundaries, based on traffic lights for normalized residuals, used for
    plotting in popmon_profiles report.
    """
    modules: List[Union[Module, Pipeline]] = [
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
    ]
    return modules


def get_trend_modules(window) -> List[Union[Module, Pipeline]]:
    """Looking for significant rolling linear trends in selected features/metrics"""
    modules: List[Union[Module, Pipeline]] = [
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
    ]
    return modules


class SelfReferenceMetricsPipeline(Pipeline):
    def __init__(
        self,
        settings: Settings,
        hists_key,
    ):
        """Example metrics pipeline for comparing test data with itself (full test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :return: assembled self reference pipeline
        """
        from popmon.analysis.comparison import Comparisons

        reference_prefix = "ref"
        reference_modules: List[Union[Module, Pipeline]] = [
            # 3. Comparison of with profiled test histograms, results in chi2 comparison of histograms
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
                metrics=[
                    f"{reference_prefix}_{key}"
                    for key in Comparisons.get_keys()
                    if key in ["max_prob_diff", "psi", "jsd"]
                ],
            ),
            # 4. profiling of histograms, then pull calculation compared with reference mean and std,
            #        to obtain normalized residuals of profiles
            RefMedianMadPullCalculator(
                reference_key="profiles",
                assign_to_key="profiles",
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
            ),
        ]

        modules = (
            get_splitting_modules(hists_key, settings.features, settings.time_axis)
            + reference_modules
            + get_trend_modules(settings.comparison.window)
            + get_static_bound_modules(settings.monitoring.pull_rules)
            + get_traffic_light_modules(settings.monitoring.monitoring_rules)
        )
        super().__init__(modules)


class ExternalReferenceMetricsPipeline(Pipeline):
    def __init__(
        self,
        settings: Settings,
        hists_key="test_hists",
        ref_hists_key="ref_hists",
    ):
        """Example metrics pipeline for comparing test data with other (full) external reference set

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :param str ref_hists_key: key to reference histograms in datastore. default is 'ref_hists'
        :return: assembled external reference pipeline
        """
        from popmon.analysis.comparison import Comparisons

        reference_prefix = "ref"
        reference_modules: List[Union[Module, Pipeline]] = [
            # 3. Profiling of split reference histograms, then chi2 comparison with test histograms
            HistSplitter(
                read_key=ref_hists_key,
                store_key="split_ref_hists",
                features=settings.features,
                feature_begins_with=f"{settings.time_axis}:",
            ),
            ReferenceHistComparer(
                reference_key="split_ref_hists",
                assign_to_key="split_hists",
                store_key="comparisons",
            ),
            HistProfiler(read_key="split_ref_hists", store_key="ref_profiles"),
            RefMedianMadPullCalculator(
                reference_key="comparisons",
                assign_to_key="comparisons",
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
                metrics=[
                    f"{reference_prefix}_{key}"
                    for key in Comparisons.get_keys()
                    if key in ["max_prob_diff", "psi", "jsd"]
                ],
            ),
            # 4. pull calculation compared with reference mean and std, to obtain normalized residuals of profiles
            ReferencePullCalculator(
                reference_key="ref_profiles",
                assign_to_key="profiles",
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
            ),
        ]
        modules = (
            get_splitting_modules(hists_key, settings.features, settings.time_axis)
            + reference_modules
            + get_trend_modules(settings.comparison.window)
            + get_static_bound_modules(settings.monitoring.pull_rules)
            + get_traffic_light_modules(settings.monitoring.monitoring_rules)
        )
        super().__init__(modules)


class RollingReferenceMetricsPipeline(Pipeline):
    def __init__(
        self,
        settings: Settings,
        hists_key="test_hists",
    ):
        """Example metrics pipeline for comparing test data with itself (rolling test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :return: assembled rolling reference pipeline
        """
        from popmon.analysis.comparison import Comparisons

        reference_prefix = "roll"
        reference_modules: List[Union[Module, Pipeline]] = [
            # 3. profiling of reference histograms, then comparison of with profiled test histograms
            #        results in chi2 comparison of histograms
            RollingHistComparer(
                read_key="split_hists",
                window=settings.comparison.window,
                shift=settings.comparison.shift,
                store_key="comparisons",
            ),
            RefMedianMadPullCalculator(
                reference_key="comparisons",
                assign_to_key="comparisons",
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
                metrics=[
                    f"{reference_prefix}_{key}"
                    for key in Comparisons.get_keys()
                    if key in ["max_prob_diff", "psi", "jsd"]
                ],
            ),
            # 4. profiling of histograms, then pull calculation compared with reference mean and std,
            #        to obtain normalized residuals of profiles
            RollingPullCalculator(
                read_key="profiles",
                window=settings.comparison.window,
                shift=settings.comparison.shift,
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
            ),
        ]

        modules = (
            get_splitting_modules(hists_key, settings.features, settings.time_axis)
            + reference_modules
            + get_trend_modules(settings.comparison.window)
            + get_dynamic_bound_modules(settings.monitoring.pull_rules)
            + get_traffic_light_modules(settings.monitoring.monitoring_rules)
        )
        super().__init__(modules)


class ExpandingReferenceMetricsPipeline(Pipeline):
    def __init__(
        self,
        settings: Settings,
        hists_key="test_hists",
    ):
        """Example metrics pipeline for comparing test data with itself (expanding test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :return: assembled expanding reference pipeline
        """
        from popmon.analysis.comparison import Comparisons

        reference_prefix = "expanding"
        reference_modules: List[Union[Module, Pipeline]] = [
            # 3. profiling of reference histograms, then comparison of with profiled test histograms
            #    results in chi2 comparison of histograms
            ExpandingHistComparer(
                read_key="split_hists",
                shift=settings.comparison.shift,
                store_key="comparisons",
            ),
            # 4. profiling of histograms, then pull calculation compared with reference mean and std,
            #        to obtain normalized residuals of profiles
            RefMedianMadPullCalculator(
                reference_key="comparisons",
                assign_to_key="comparisons",
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
                metrics=[
                    f"{reference_prefix}_{key}"
                    for key in Comparisons.get_keys()
                    if key in ["max_prob_diff", "psi", "jsd"]
                ],
            ),
            ExpandingPullCalculator(
                read_key="profiles",
                shift=settings.comparison.shift,
                suffix_mean="_mean",
                suffix_std="_std",
                suffix_pull="_pull",
            ),
        ]

        modules = (
            get_splitting_modules(hists_key, settings.features, settings.time_axis)
            + reference_modules
            + get_trend_modules(settings.comparison.window)
            + get_dynamic_bound_modules(settings.monitoring.pull_rules)
            + get_traffic_light_modules(settings.monitoring.monitoring_rules)
        )
        super().__init__(modules)
