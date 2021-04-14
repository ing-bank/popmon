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


from pathlib import Path

from ..base import Pipeline
from ..config import config
from ..io import FileWriter
from ..pipeline.metrics_pipelines import (
    metrics_expanding_reference,
    metrics_external_reference,
    metrics_rolling_reference,
    metrics_self_reference,
)
from ..visualization import (
    AlertSectionGenerator,
    HistogramSection,
    ReportGenerator,
    SectionGenerator,
    TrafficLightSectionGenerator,
)


def self_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    monitoring_rules={},
    pull_rules={},
    features=None,
    skip_empty_plots=True,
    last_n=0,
    plot_hist_n=6,
    report_filepath=None,
    show_stats=None,
    **kwargs,
):
    """Example pipeline for comparing test data with itself (full test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
    :param int window: window size for trend detection. default is 10
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param bool skip_empty_plots: if false, also show empty plots in report with only nans or zeroes (optional)
    :param int last_n: plot statistic data for last 'n' periods (optional)
    :param int plot_hist_n: plot histograms for last 'n' periods. default is 1 (optional)
    :param str report_filepath: the file path where to output the report (optional)
    :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
    :param kwargs: residual keyword arguments
    :return: assembled self reference pipeline
    """
    modules = [
        metrics_self_reference(
            hists_key,
            time_axis,
            window,
            monitoring_rules,
            pull_rules,
            features,
            **kwargs,
        ),
        ReportPipe(
            sections_key="report_sections",
            store_key="html_report",
            skip_empty_plots=skip_empty_plots,
            last_n=last_n,
            plot_hist_n=plot_hist_n,
            report_filepath=report_filepath,
            show_stats=show_stats,
        ),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def external_reference(
    hists_key="test_hists",
    ref_hists_key="ref_hists",
    time_axis="date",
    window=10,
    monitoring_rules={},
    pull_rules={},
    features=None,
    skip_empty_plots=True,
    last_n=0,
    plot_hist_n=2,
    report_filepath=None,
    show_stats=None,
    **kwargs,
):
    """Example pipeline for comparing test data with other (full) external reference set

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str ref_hists_key: key to reference histograms in datastore. default is 'ref_hists'
    :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
    :param int window: window size for trend detection. default is 10
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param bool skip_empty_plots: if false, show empty plots in report with only nans or zeroes (optional)
    :param int last_n: plot statistic data for last 'n' periods (optional)
    :param int plot_hist_n: plot histograms for last 'n' periods. default is 1 (optional)
    :param str report_filepath: the file path where to output the report (optional)
    :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
    :param kwargs: residual keyword arguments
    :return: assembled external reference pipeline
    """
    modules = [
        metrics_external_reference(
            hists_key,
            ref_hists_key,
            time_axis,
            window,
            monitoring_rules,
            pull_rules,
            features,
            **kwargs,
        ),
        ReportPipe(
            sections_key="report_sections",
            store_key="html_report",
            skip_empty_plots=skip_empty_plots,
            last_n=last_n,
            plot_hist_n=plot_hist_n,
            report_filepath=report_filepath,
            show_stats=show_stats,
        ),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def rolling_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    shift=1,
    monitoring_rules={},
    pull_rules={},
    features=None,
    skip_empty_plots=True,
    last_n=0,
    plot_hist_n=6,
    report_filepath=None,
    show_stats=None,
    **kwargs,
):
    """Example pipeline for comparing test data with itself (rolling test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
    :param int window: size of rolling window and for trend detection. default is 10
    :param int shift: shift in rolling window. default is 1
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param bool skip_empty_plots: if false, show empty plots in report with only nans or zeroes (optional)
    :param int last_n: plot statistic data for last 'n' periods (optional)
    :param int plot_hist_n: plot histograms for last 'n' periods. default is 1 (optional)
    :param str report_filepath: the file path where to output the report (optional)
    :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
    :param kwargs: residual keyword arguments
    :return: assembled rolling reference pipeline
    """
    modules = [
        metrics_rolling_reference(
            hists_key,
            time_axis,
            window,
            shift,
            monitoring_rules,
            pull_rules,
            features,
            **kwargs,
        ),
        ReportPipe(
            sections_key="report_sections",
            store_key="html_report",
            skip_empty_plots=skip_empty_plots,
            last_n=last_n,
            plot_hist_n=plot_hist_n,
            report_filepath=report_filepath,
            show_stats=show_stats,
        ),
    ]

    pipeline = Pipeline(modules)
    return pipeline


def expanding_reference(
    hists_key="test_hists",
    time_axis="date",
    window=10,
    shift=1,
    monitoring_rules={},
    pull_rules={},
    features=None,
    skip_empty_plots=True,
    last_n=0,
    plot_hist_n=6,
    report_filepath=None,
    show_stats=None,
    **kwargs,
):
    """Example pipeline for comparing test data with itself (expanding test set)

    :param str hists_key: key to test histograms in datastore. default is 'test_hists'
    :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
    :param int window: window size for trend detection. default is 10
    :param int shift: shift in expanding window. default is 1
    :param dict monitoring_rules: traffic light rules
    :param dict pull_rules: pull rules to determine dynamic boundaries
    :param list features: features of histograms to pick up from input data (optional)
    :param bool skip_empty_plots: if false, show empty plots in report with only nans or zeroes (optional)
    :param int last_n: plot statistic data for last 'n' periods (optional)
    :param int plot_hist_n: plot histograms for last 'n' periods. default is 1 (optional)
    :param str report_filepath: the file path where to output the report (optional)
    :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
    :param kwargs: residual keyword arguments
    :return: assembled expanding reference pipeline
    """
    modules = [
        metrics_expanding_reference(
            hists_key,
            time_axis,
            window,
            shift,
            monitoring_rules,
            pull_rules,
            features,
            **kwargs,
        ),
        ReportPipe(
            sections_key="report_sections",
            store_key="html_report",
            skip_empty_plots=skip_empty_plots,
            last_n=last_n,
            plot_hist_n=plot_hist_n,
            report_filepath=report_filepath,
            show_stats=show_stats,
        ),
    ]

    pipeline = Pipeline(modules)
    return pipeline


class ReportPipe(Pipeline):
    """Pipeline of modules for generating sections and a final report."""

    def __init__(
        self,
        sections_key="report_sections",
        store_key="html_report",
        profiles_section="Profiles",
        comparisons_section="Comparisons",
        traffic_lights_section="Traffic Lights",
        alerts_section="Alerts",
        histograms_section="Histograms",
        report_filepath=None,
        show_stats=None,
        skip_empty_plots=True,
        last_n=0,
        skip_first_n=0,
        skip_last_n=0,
        plot_hist_n=6,
    ):
        """Initialize an instance of Report.

        :param str sections_key: key to store sections data in the datastore
        :param str store_key: key to store the HTML report data in the datastore
        :param str profiles_section: name for the profile data section. default is 'Profiles'
        :param str comparisons_section: name for the comparison data section. default is 'Comparisons'
        :param str traffic_lights_section: name for the traffic light section. default is 'Traffic Lights'
        :param str alerts_section: name for the alerts section. default is 'Alerts'
        :param str histograms_section: name for the histograms section. default is 'Histograms'
        :param str report_filepath: the file path where to output the report (optional)
        :param bool skip_empty_plots: if false, also show empty plots in report with only nans or zeroes (optional)
        :param int last_n: plot statistic data for last 'n' periods (optional)
        :param int skip_first_n: when plotting data skip first 'n' periods. last_n takes precedence (optional)
        :param int skip_last_n: when plotting data skip last 'n' periods. last_n takes precedence (optional)
        :param int plot_hist_n: plot histograms for last 'n' periods. default is 1 (optional)
        :param list show_stats: list of statistic name patterns to show in the report. If None, show all (optional)
        """
        super().__init__(modules=[])
        self.store_key = store_key

        # dictionary of section descriptions
        descs = config["section_descriptions"]

        # default keyword arguments for each section
        def sg_kws(read_key):
            return {
                "read_key": read_key,
                "store_key": sections_key,
                "skip_empty_plots": skip_empty_plots,
                "last_n": last_n,
                "skip_first_n": skip_first_n,
                "skip_last_n": skip_last_n,
                "show_stats": show_stats,
                "description": descs.get(read_key, ""),
            }

        self.modules = [
            # --- o generate sections
            #       - a section of profiled statistics with dynamic or static traffic light bounds
            #       - a section of histogram and pull comparison statistics
            #       - a section showing all traffic light alerts of monitored statistics
            #       - a section with a summary of traffic light alerts
            # --- o generate report
            HistogramSection(
                read_key="split_hists",
                store_key=sections_key,
                section_name=histograms_section,
                hist_name_starts_with="histogram",
                last_n=plot_hist_n,
                description=descs.get("histograms", ""),
            ),
            TrafficLightSectionGenerator(
                section_name=traffic_lights_section, **sg_kws("traffic_lights")
            ),
            AlertSectionGenerator(section_name=alerts_section, **sg_kws("alerts")),
            SectionGenerator(
                dynamic_bounds="dynamic_bounds_comparisons",
                static_bounds="static_bounds_comparisons",
                section_name=comparisons_section,
                ignore_stat_endswith=["_mean", "_std", "_pull"],
                **sg_kws("comparisons"),
            ),
            SectionGenerator(
                dynamic_bounds="dynamic_bounds",
                section_name=profiles_section,
                static_bounds="static_bounds",
                ignore_stat_endswith=["_mean", "_std", "_pull"],
                **sg_kws("profiles"),
            ),
            ReportGenerator(read_key=sections_key, store_key=store_key),
        ]
        if isinstance(report_filepath, (str, Path)) and len(report_filepath) > 0:
            self.modules.append(FileWriter(store_key, file_path=report_filepath))

    def transform(self, datastore):
        self.logger.info(f'Generating report "{self.store_key}".')
        return super().transform(datastore)
