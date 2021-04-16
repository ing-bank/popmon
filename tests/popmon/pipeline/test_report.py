import pandas as pd
import pytest

from popmon import resources
from popmon.base import Pipeline
from popmon.hist.filling import get_bin_specs
from popmon.io import JsonReader
from popmon.pipeline.report import df_stability_report, stability_report


def test_hists_stability_report():
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

    # generate report
    hist_list = [
        "date:bankrupt",
        "date:country",
        "date:bankrupt",
        "date:A_score",
        "date:A_score:num_employees",
    ]
    stability_report(hists, reference_type="rolling", window=5, features=hist_list)


def test_df_stability_report():
    # generate report directly from dataframe
    features = ["date:isActive", "date:eyeColor", "date:latitude"]
    bin_specs = {
        "date": {
            "bin_width": pd.Timedelta("1y").value,
            "bin_offset": pd.Timestamp("2000-1-1").value,
        },
        "latitude": {"bin_width": 5.0, "bin_offset": 0.0},
    }
    rep = df_stability_report(
        pytest.test_df,
        time_axis="date",
        features=features,
        binning="unit",
        bin_specs=bin_specs,
    )

    # regenerate report, changing the plot window settings
    rep.regenerate(last_n=4)
    rep.regenerate(skip_first_n=1, skip_last_n=1)


def test_df_stability_report_self():
    time_width = "1y"
    time_offset = "2020-1-1"

    # generate report directly from dataframe
    features = ["date:eyeColor", "date:latitude"]
    rep = df_stability_report(
        pytest.test_df,
        time_axis="date",
        reference_type="self",
        features=features,
        time_width=time_width,
        time_offset=time_offset,
    )

    # test that time_width and time_offset got picked up correctly.
    datastore = rep.datastore
    hists = datastore["hists"]
    bin_specs = get_bin_specs(hists)

    assert pd.Timedelta(time_width).value == bin_specs["date:eyeColor"][0]["binWidth"]
    assert pd.Timestamp(time_offset).value == bin_specs["date:eyeColor"][0]["origin"]
    assert pd.Timedelta(time_width).value == bin_specs["date:latitude"][0]["binWidth"]
    assert pd.Timestamp(time_offset).value == bin_specs["date:latitude"][0]["origin"]


def test_df_stability_report_external():
    # generate report directly from dataframe
    features = ["date:eyeColor", "date:latitude"]
    df_stability_report(
        pytest.test_df,
        time_axis="date",
        reference_type="external",
        reference=pytest.test_df,
        features=features,
    )


def test_df_stability_report_rolling():
    # generate report directly from dataframe
    features = ["date:isActive", "date:latitude"]
    df_stability_report(
        pytest.test_df, time_axis="date", reference_type="rolling", features=features
    )


def test_df_stability_report_expanding():
    # generate report directly from dataframe
    features = ["date:isActive", "date:eyeColor"]
    df_stability_report(
        pytest.test_df, time_axis="date", reference_type="expanding", features=features
    )
