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
from popmon.pipeline.metrics_pipelines import create_metrics_pipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s]: %(message)s"
)
logger = logging.getLogger()


def stability_metrics(
    hists,
    settings: Settings,
    reference=None,
):
    """Create a data stability monitoring datastore for given dict of input histograms.

    :param dict hists: input histograms to be profiled and monitored over time.
    :param popmon.config.Settings settings: popmon configuration object
    :param reference: histograms used as reference. default is None
    :return: dict with results of metrics pipeline
    """

    if not isinstance(hists, dict):
        raise TypeError("hists should be a dict of histogrammar histograms.")

    if isinstance(settings.time_axis, str) and len(settings.time_axis) == 0:
        settings._set_time_axis_hists(hists)

    kwargs = {}
    if (
        settings.reference_type in ["external", "self_split"]
        and "ref_hists_key" not in kwargs
    ):
        kwargs["ref_hists_key"] = "ref_hists"

    pipeline = create_metrics_pipeline(
        settings=settings,
        reference=reference,
        hists_key="hists",
        **kwargs,
    )

    datastore = {"hists": hists}
    if settings.reference_type in ["external", "self_split"]:
        datastore["ref_hists"] = reference

    return pipeline.transform(datastore)


def df_stability_metrics(
    df,
    settings: Settings | None = None,
    time_width=None,
    time_offset: int = 0,
    var_dtype=None,
    reference=None,
    **kwargs,
):
    """Create a data stability monitoring html datastore for given pandas or spark dataframe.

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
    :param kwargs: residual keyword arguments, passed on to stability_report()
    :return: dict with results of metrics pipeline
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
            f'time_axis  "{settings.time_axis}" not found in columns of reference dataframe.'
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

    if reference is not None:
        if settings.reference_type not in ["external", "self_split"]:
            raise TypeError(
                "When providing a `reference`, the `reference_type` should be equal to `external` or `self_split`"
            )

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
        kwargs["reference_hists"] = reference_hists

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
    return stability_metrics(
        hists,
        settings=settings,
        reference=reference,
    )
