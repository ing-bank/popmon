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
from ..config import Report, Settings
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
        settings: Settings,
        features: list,
        hists_key: str = "test_hists",
        time_axis: str = "date",
    ):
        """Example pipeline for comparing test data with itself (full test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
        :param list features: features of histograms to pick up from input data (optional)
        :return: assembled self reference pipeline
        """
        modules = [
            SelfReferenceMetricsPipeline(
                hists_key=hists_key,
                time_axis=time_axis,
                features=features,
                settings=settings,
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
        settings: Settings,
        hists_key: str = "test_hists",
        ref_hists_key: str = "ref_hists",
        time_axis: str = "date",
        features=None,
    ):
        """Example pipeline for comparing test data with other (full) external reference set

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :param str ref_hists_key: key to reference histograms in datastore. default is 'ref_hists'
        :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
        :param list features: features of histograms to pick up from input data (optional)
        :param kwargs: residual keyword arguments
        :return: assembled external reference pipeline
        """
        modules = [
            ExternalReferenceMetricsPipeline(
                hists_key=hists_key,
                ref_hists_key=ref_hists_key,
                time_axis=time_axis,
                features=features,
                settings=settings,
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
        settings: Settings,
        hists_key: str = "test_hists",
        time_axis: str = "date",
        features=None,
    ):
        """Example pipeline for comparing test data with itself (rolling test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
        :param list features: features of histograms to pick up from input data (optional)
        :return: assembled rolling reference pipeline
        """
        modules = [
            RollingReferenceMetricsPipeline(
                settings=settings,
                hists_key=hists_key,
                time_axis=time_axis,
                features=features,
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
        settings: Settings,
        hists_key: str = "test_hists",
        time_axis: str = "date",
        features=None,
    ):
        """Example pipeline for comparing test data with itself (expanding test set)

        :param str hists_key: key to test histograms in datastore. default is 'test_hists'
        :param str time_axis: name of datetime feature. default is 'date' (column should be timestamp, date(time) or numeric batch id)
        :param list features: features of histograms to pick up from input data (optional)
        :return: assembled expanding reference pipeline
        """
        modules = [
            ExpandingReferenceMetricsPipeline(
                hists_key=hists_key,
                time_axis=time_axis,
                features=features,
                settings=settings,
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
        settings: Report,
        sections_key: str = "report_sections",
        store_key: str = "html_report",
    ):
        """Initialize an instance of Report.

        :param str sections_key: key to store sections data in the datastore
        :param str store_key: key to store the HTML report data in the datastore
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
