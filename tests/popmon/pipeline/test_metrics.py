import pandas as pd
import pytest

from popmon import resources
from popmon.base import Pipeline
from popmon.config import Settings
from popmon.io import JsonReader
from popmon.pipeline.metrics import df_stability_metrics, stability_metrics


def test_hists_stability_metrics():
    settings = Settings(reference_type="rolling")
    settings.comparison.window = 5
    settings.features = [
        "date:bankrupt",
        "date:country",
        "date:bankrupt",
        "date:A_score",
        "date:A_score:num_employees",
    ]

    # get histograms
    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"), store_key="hists"
            )
        ]
    )
    datastore = pipeline.transform(datastore={})
    hists = datastore["hists"]

    # generate metrics
    ds = stability_metrics(hists, settings=settings)

    cols = ["profiles", "comparisons", "traffic_lights", "alerts"]
    for c in cols:
        assert c in list(ds.keys())


def test_df_stability_metrics():
    # generate metrics directly from dataframe
    bin_specs = {
        "date": {
            "bin_width": pd.Timedelta("1y").value,
            "bin_offset": pd.Timestamp("2000-1-1").value,
        },
        "latitude": {"bin_width": 5.0, "bin_offset": 0.0},
    }

    settings = Settings(
        time_axis="date",
        binning="unit",
        features=["date:isActive", "date:eyeColor", "date:latitude"],
        bin_specs=bin_specs,
    )

    ds = df_stability_metrics(
        df=pytest.test_df,
        settings=settings,
    )

    cols = ["profiles", "comparisons", "traffic_lights", "alerts"]
    for c in cols:
        assert c in list(ds.keys())
