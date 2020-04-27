from popmon.hist.filling import make_histograms
from popmon.pipeline.report import df_stability_report
from popmon.pipeline.metrics import df_stability_metrics

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
