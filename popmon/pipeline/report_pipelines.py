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

from ..base import Pipeline
from ..config import Report
from ..io import FileWriter
from ..pipeline.metrics_pipelines import (
    ExpandingReferenceMetricsPipeline,
    ExternalReferenceMetricsPipeline,
    RollingReferenceMetricsPipeline,
    SelfReferenceMetricsPipeline,
)
from ..visualization import (
    AlertSectionGenerator,
    HistogramSection,
    ReportGenerator,
    SectionGenerator,
    TrafficLightSectionGenerator,
)
from ..visualization.overview_section import OverviewSectionGenerator


def get_report_pipeline_class(reference_type, reference):
    _report_pipeline = {
        "self": SelfReference,
        "external": ExternalReference,
        "rolling": RollingReference,
        "expanding": ExpandingReference,
    }
    reference_types = list(_report_pipeline.keys())
    if reference_type not in reference_types:
        raise ValueError(f"reference_type should be one of {str(reference_types)}.")
    if reference_type == "external" and not isinstance(reference, dict):
        raise TypeError("reference should be a dict of histogrammar histograms.")

    return _report_pipeline[reference_type]


class SelfReference(Pipeline):
    def __init__(
        self,
        hists_key="test_hists",
        time_axis="date",
        features=None,
        settings=None,
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
            SelfReferenceMetricsPipeline(
                hists_key,
                time_axis,
                features,
                settings,
            ),
            ReportPipe(
                sections_key="report_sections",
                store_key="html_report",
                settings=settings.report,
            ),
        ]

        super().__init__(modules)


class ExternalReference(Pipeline):
    def __init__(
        self,
        hists_key="test_hists",
        ref_hists_key="ref_hists",
        time_axis="date",
        features=None,
        settings=None,
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
            ExternalReferenceMetricsPipeline(
                hists_key,
                ref_hists_key,
                time_axis,
                features,
                settings,
            ),
            ReportPipe(
                sections_key="report_sections",
                store_key="html_report",
                settings=settings.report,
            ),
        ]

        super().__init__(modules)


class RollingReference(Pipeline):
    def __init__(
        self,
        hists_key="test_hists",
        time_axis="date",
        features=None,
        settings=None,
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
            RollingReferenceMetricsPipeline(
                hists_key,
                time_axis,
                features,
                settings,
            ),
            ReportPipe(
                sections_key="report_sections",
                store_key="html_report",
                settings=settings.report,
            ),
        ]

        super().__init__(modules)


class ExpandingReference(Pipeline):
    def __init__(
        self,
        hists_key="test_hists",
        time_axis="date",
        features=None,
        settings=None,
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
            ExpandingReferenceMetricsPipeline(
                hists_key,
                time_axis,
                features,
                settings,
            ),
            ReportPipe(
                sections_key="report_sections",
                store_key="html_report",
                settings=settings.report,
            ),
        ]

        super().__init__(modules)


class ReportPipe(Pipeline):
    """Pipeline of modules for generating sections and a final report."""

    def __init__(
        self,
        sections_key="report_sections",
        store_key="html_report",
        settings: Report = None,
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
        self.store_key = store_key

        modules = [
            OverviewSectionGenerator(
                read_key="traffic_lights",
                store_key=sections_key,
                settings=settings,
            ),
            # generate section with histogram
            HistogramSection(
                read_key="split_hists",
                store_key=sections_key,
                hist_name_starts_with="histogram",
                settings=settings.section.histograms,
                top_n=settings.top_n,
            ),
            # section showing all traffic light alerts of monitored statistics
            TrafficLightSectionGenerator(
                read_key="traffic_lights",
                store_key=sections_key,
                settings=settings,
            ),
            # section with a summary of traffic light alerts
            AlertSectionGenerator(
                read_key="alerts",
                store_key=sections_key,
                settings=settings,
            ),
            # section of histogram and pull comparison statistics
            SectionGenerator(
                dynamic_bounds="dynamic_bounds_comparisons",
                static_bounds="static_bounds_comparisons",
                section_name=settings.section.comparisons.name,
                ignore_stat_endswith=["_mean", "_std", "_pull"],
                read_key="comparisons",
                description=settings.section.comparisons.description,
                store_key=sections_key,
                settings=settings,
            ),
            # section of profiled statistics with dynamic or static traffic light bounds
            SectionGenerator(
                dynamic_bounds="dynamic_bounds",
                section_name=settings.section.profiles.name,
                static_bounds="static_bounds",
                ignore_stat_endswith=["_mean", "_std", "_pull"],
                read_key="profiles",
                description=settings.section.profiles.description,
                store_key=sections_key,
                settings=settings,
            ),
            # generate report
            ReportGenerator(read_key=sections_key, store_key=store_key),
        ]
        if (
            isinstance(settings.report_filepath, (str, Path))
            and len(settings.report_filepath) > 0
        ):
            modules.append(FileWriter(store_key, file_path=settings.report_filepath))

        super().__init__(modules=modules)

    def transform(self, datastore):
        self.logger.info(f'Generating report "{self.store_key}".')
        return super().transform(datastore)
