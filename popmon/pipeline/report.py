# Copyright (c) 2023 ING Analytics Wholesale Banking
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

from __future__ import annotations

import logging

from histogrammar.dfinterface.make_histograms import get_bin_specs, make_histograms

from popmon.config import Settings
from popmon.pipeline.dataset_splitter import split_dataset
from popmon.pipeline.report_pipelines import ReportPipe, get_report_pipeline_class
from popmon.resources import templates_env

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s]: %(message)s"
)
logger = logging.getLogger()


def stability_report(
    hists,
    settings: Settings | None = None,
    reference=None,
    **kwargs,
):
    """Create a data stability monitoring html report for given dict of input histograms.

    :param dict hists: input histograms to be profiled and monitored over time.
    :param popmon.config.Settings settings: popmon configuration object
    :param reference: histograms used as reference. default is None
    :param kwargs: when settings=None, parameters such as `features` and `time_axis` can be passed
    :return: dict with results of reporting pipeline
    """

    if settings is None:
        settings = Settings(**kwargs)

    # perform basic input checks
    if not isinstance(hists, dict):
        raise TypeError("hists should be a dict of histogrammar histograms.")

    if isinstance(settings.time_axis, str) and len(settings.time_axis) == 0:
        settings._set_time_axis_hists(hists)

    # configuration and datastore for report pipeline
    cfg = {
        "hists_key": "hists",
        "settings": settings,
    }

    datastore = {"hists": hists}
    if settings.reference_type in ["external", "self_split"]:
        cfg["ref_hists_key"] = "ref_hists"
        datastore["ref_hists"] = reference

    # execute reporting pipeline
    pipeline = get_report_pipeline_class(settings.reference_type, reference)(**cfg)
    result = pipeline.transform(datastore)

    stability_report_result = StabilityReport(datastore=result)
    return stability_report_result


def df_stability_report(
    df,
    settings: Settings | None | None = None,
    time_width=None,
    time_offset: int = 0,
    var_dtype=None,
    reference=None,
    split=None,
    **kwargs,
):
    """Create a data stability monitoring html report for given pandas or spark dataframe.

    :param df: input pandas/spark dataframe to be profiled and monitored over time.
    :param popmon.config.Settings settings: popmon configuration object
    :param time_width: bin width of time axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1w', 3600e9 (number of ns),
                      anything understood by pd.Timedelta(time_width).value

    :param time_offset: bin offset of time axis. str or number (ns). note: bin_specs takes precedence. (optional)

        .. code-block:: text

            Examples: '1-1-2020', 0 (number of ns since 1-1-1970),
                      anything parsed by pd.Timestamp(time_offset).value

    :param dict var_dtype: dictionary with specified datatype per feature. auto-guessed when not provided.
    :param reference: reference dataframe or histograms. default is None
    :return: dict with results of reporting pipeline
    """

    if settings is None:
        settings = Settings(**kwargs)

    if len(settings.time_axis) == 0:
        settings._set_time_axis_dataframe(df)
        logger.info(f'Time-axis automatically set to "{settings.time_axis}"')

    if settings.time_axis not in df.columns:
        raise ValueError(
            f'time_axis  "{settings.time_axis}" not found in columns of dataframe.'
        )

    if (
        reference is not None
        and not isinstance(reference, dict)
        and settings.time_axis not in reference.columns
    ):
        raise ValueError(
            f'time_axis "{settings.time_axis}" not found in columns of reference dataframe.'
        )

    if settings.features is not None:
        # by now time_axis is defined. ensure that all histograms start with it.
        settings._ensure_features_time_axis()

    # interpret time_width and time_offset
    if time_width is not None:
        if not isinstance(time_width, (str, int, float)):
            raise TypeError
        if not isinstance(time_offset, (str, int, float)):
            raise TypeError

        settings._set_bin_specs_by_time_width_and_offset(time_width, time_offset)

    reference_hists = None
    if settings.reference_type == "self" and split is not None and reference is None:
        settings.reference_type = "self_split"
        reference, df = split_dataset(df, split, settings.time_axis)

    if reference is not None:
        if settings.reference_type != "self_split":
            settings.reference_type = "external"

        if isinstance(reference, dict):
            # 1. reference is dict of histograms
            # extract features and bin_specs from reference histograms
            reference_hists = reference
            if settings.features is not None or settings.bin_specs != {}:
                raise ValueError(
                    "When providing a reference, the `features` and `bin_specs` settings should be default (as they are overriden)"
                )

            settings.features = list(reference_hists.keys())
            settings.bin_specs = get_bin_specs(reference_hists)
        else:
            # 2. reference is pandas or spark dataframe
            # generate histograms and return updated features, bin_specs, time_axis, etc.
            (
                reference_hists,
                settings.features,
                settings.bin_specs,
                settings.time_axis,
                var_dtype,
            ) = make_histograms(
                reference,
                settings.features,
                settings.binning,
                settings.bin_specs,
                settings.time_axis,
                var_dtype,
                ret_specs=True,
            )

    # use the same features, bin_specs, time_axis, etc as for reference hists
    hists = make_histograms(
        df,
        features=settings.features,
        binning=settings.binning,
        bin_specs=settings.bin_specs,
        time_axis=settings.time_axis,
        var_dtype=var_dtype,
    )

    # generate data stability report
    return stability_report(
        hists=hists,
        settings=settings,
        reference=reference_hists,
    )


class StabilityReport:
    """Representation layer of the report.

    Stability report module wraps the representation functionality of the report
    after running the pipeline and generating the report. Report can be represented
    as a HTML string, HTML file or Jupyter notebook's cell output.
    """

    def __init__(self, datastore, read_key: str = "html_report") -> None:
        """Initialize an instance of StabilityReport.

        :param str read_key: key of HTML report data to read from data store. default is html_report.
        """
        self.read_key = read_key
        self.datastore = datastore
        self.logger = logging.getLogger()

    @property
    def html_report(self):
        return self.datastore[self.read_key]

    def _repr_html_(self):
        """HTML representation of the class (report) embedded in an iframe.

        :return HTML: HTML report in an iframe
        """
        from IPython.core.display import display

        return display(self.to_notebook_iframe())

    def __repr__(self) -> str:
        """Override so that Jupyter Notebook does not print the object."""
        return ""

    def to_html(self, escape: bool = False):
        """HTML code representation of the report (represented as a string).

        :param bool escape: escape characters which could conflict with other HTML code. default: False
        :return str: HTML code of the report
        """

        if escape:
            import html

            return html.escape(self.html_report)
        return self.html_report

    def to_file(self, filename) -> None:
        """Store HTML report in the local file system.

        :param str filename: filename for the HTML report
        """
        with open(filename, "w+") as file:
            file.write(self.to_html())

    def to_notebook_iframe(self, width: str = "100%", height: str = "100%"):
        """HTML representation of the class (report) embedded in an iframe.

        :param str width: width of the frame to be shown
        :param str height: height of the frame to be shown
        :return HTML: HTML report in an iframe
        """
        from IPython.core.display import HTML

        # get iframe's snippet code, insert report's HTML code and display it as HTML
        return HTML(
            templates_env(
                filename="notebook_iframe.html",
                src=self.to_html(escape=True),
                width=width,
                height=height,
            )
        )

    def regenerate(
        self,
        store_key: str = "html_report",
        sections_key: str = "report_sections",
        settings: Settings | None = None,
    ):
        """Regenerate HTML report with different plot settings
        :param str sections_key: key to store sections data in the datastore. default is 'report_sections'.
        :param str store_key: key to store the HTML report data in the datastore. default is 'html_report'
        :param Settings settings: configuration to regenerate the report
        :return HTML: HTML report in an iframe
        """
        # basic checks
        if not self.datastore:
            self.logger.warning("Empty datastore, could not regenerate report.")
            return None

        # start from clean slate
        if sections_key in self.datastore:
            del self.datastore[sections_key]
        if store_key in self.datastore:
            del self.datastore[store_key]
        if settings is None:
            settings = Settings()

        pipeline = ReportPipe(
            sections_key=sections_key,
            settings=settings,
        )
        result = pipeline.transform(self.datastore)

        stability_report = StabilityReport(datastore=result)
        return stability_report
