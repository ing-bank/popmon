# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

from popmon.hist.filling import make_histograms
from popmon.pipeline.metrics import df_stability_metrics
from popmon.pipeline.report import df_stability_report

try:
    from pyspark.sql import DataFrame

    # add function to create histogrammar histograms
    DataFrame.pm_make_histograms = make_histograms
    # add function to create stability report
    DataFrame.pm_stability_report = df_stability_report
    # add function to create metrics over time without stability report
    DataFrame.pm_stability_metrics = df_stability_metrics
except (ModuleNotFoundError, AttributeError):
    pass
