import numpy as np
import pandas as pd
import pytest

from popmon import resources
from popmon.analysis.apply_func import ApplyFunc
from popmon.analysis.comparison.hist_comparer import (
    ExpandingHistComparer,
    ReferenceHistComparer,
    RollingHistComparer,
    hist_compare,
)
from popmon.analysis.functions import expanding_hist
from popmon.base import Pipeline
from popmon.hist.hist_splitter import HistSplitter
from popmon.io import JsonReader


def test_hist_compare():
    hist_list = [
        "date:country",
        "date:bankrupt",
        "date:num_employees",
        "date:A_score",
        "date:A_score:num_employees",
    ]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist", store_key="output_hist", features=hist_list
            ),
            ApplyFunc(
                apply_to_key="output_hist",
                apply_funcs=[
                    {
                        "func": expanding_hist,
                        "shift": 1,
                        "suffix": "sum",
                        "entire": True,
                        "hist_name": "histogram",
                    }
                ],
            ),
            ApplyFunc(
                apply_to_key="output_hist",
                assign_to_key="comparison",
                apply_funcs=[
                    {
                        "func": hist_compare,
                        "hist_name1": "histogram",
                        "hist_name2": "histogram_sum",
                        "suffix": "",
                        "axis": 1,
                    }
                ],
            ),
        ]
    )
    datastore = pipeline.transform(datastore={})

    df = datastore["comparison"]["num_employees"]
    np.testing.assert_array_equal(df["chi2"].values[-1], 0.7017543859649122)


def test_reference_hist_comparer():
    hist_list = ["date:country", "date:bankrupt", "date:num_employees", "date:A_score"]
    features = ["country", "bankrupt", "num_employees", "A_score"]

    cols = [
        "ref_pearson",
        "ref_chi2",
        "ref_chi2_zscore",
        "ref_chi2_norm",
        "ref_chi2_pvalue",
        "ref_chi2_max_residual",
        "ref_chi2_spike_count",
        "ref_ks",
        "ref_ks_zscore",
        "ref_ks_pvalue",
        "ref_max_prob_diff",
        "ref_jsd",
        "ref_psi",
        "ref_unknown_labels",
    ]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist", store_key="output_hist", features=hist_list
            ),
            ReferenceHistComparer(
                reference_key="output_hist",
                assign_to_key="output_hist",
                store_key="comparison",
            ),
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "comparison" in datastore and isinstance(datastore["comparison"], dict)
    assert len(datastore["comparison"].keys()) == len(features)
    for f in features:
        assert f in datastore["comparison"]
    for f in features:
        assert isinstance(datastore["comparison"][f], pd.DataFrame)

    df = datastore["comparison"]["A_score"]
    assert len(df) == 16
    assert set(df.columns) == set(cols)
    np.testing.assert_almost_equal(df["ref_chi2"].mean(), 2.623206018518519)

    df = datastore["comparison"]["country"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["ref_chi2"].mean(), 0.9804481792717087)

    df = datastore["comparison"]["bankrupt"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["ref_chi2"].mean(), 0.6262951496388027)

    df = datastore["comparison"]["num_employees"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["ref_chi2"].mean(), 4.213429217840983)


def test_expanding_hist_comparer():
    hist_list = ["date:country", "date:bankrupt", "date:num_employees", "date:A_score"]
    features = ["country", "bankrupt", "num_employees", "A_score"]

    cols = [
        "expanding_pearson",
        "expanding_chi2",
        "expanding_chi2_zscore",
        "expanding_chi2_norm",
        "expanding_chi2_pvalue",
        "expanding_chi2_max_residual",
        "expanding_chi2_spike_count",
        "expanding_ks",
        "expanding_ks_zscore",
        "expanding_ks_pvalue",
        "expanding_max_prob_diff",
        "expanding_jsd",
        "expanding_psi",
        "expanding_unknown_labels",
    ]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist", store_key="output_hist", features=hist_list
            ),
            ExpandingHistComparer(read_key="output_hist", store_key="comparison"),
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "comparison" in datastore and isinstance(datastore["comparison"], dict)
    assert len(datastore["comparison"].keys()) == len(features)
    for f in features:
        assert f in datastore["comparison"]
    for f in features:
        assert isinstance(datastore["comparison"][f], pd.DataFrame)

    df = datastore["comparison"]["A_score"]
    assert len(df) == 16
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["expanding_chi2"].mean(), 2.8366236044275257)

    df = datastore["comparison"]["country"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["expanding_chi2"].mean(), 1.1224348056645368)

    df = datastore["comparison"]["bankrupt"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["expanding_chi2"].mean(), 0.6901425387043608)

    df = datastore["comparison"]["num_employees"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["expanding_chi2"].mean(), 4.243731870738727)


@pytest.mark.filterwarnings("ignore:An input array is constant")
@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
def test_rolling_hist_comparer():
    hist_list = ["date:country", "date:bankrupt", "date:num_employees", "date:A_score"]
    features = ["country", "bankrupt", "num_employees", "A_score"]

    cols = [
        "roll_pearson",
        "roll_chi2",
        "roll_chi2_zscore",
        "roll_chi2_norm",
        "roll_chi2_pvalue",
        "roll_chi2_max_residual",
        "roll_chi2_spike_count",
        "roll_ks",
        "roll_ks_zscore",
        "roll_ks_pvalue",
        "roll_max_prob_diff",
        "roll_psi",
        "roll_jsd",
        "roll_unknown_labels",
    ]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist", store_key="output_hist", features=hist_list
            ),
            RollingHistComparer(
                read_key="output_hist", store_key="comparison", window=5
            ),
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "comparison" in datastore and isinstance(datastore["comparison"], dict)
    assert len(datastore["comparison"].keys()) == len(features)
    for f in features:
        assert f in datastore["comparison"]
    for f in features:
        assert isinstance(datastore["comparison"][f], pd.DataFrame)

    df = datastore["comparison"]["A_score"]
    assert len(df) == 16
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["roll_chi2"].mean(), 2.927272727272727)

    df = datastore["comparison"]["country"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["roll_chi2"].mean(), 1.3022619047619046)

    df = datastore["comparison"]["bankrupt"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["roll_chi2"].mean(), 0.7251681783824641)

    df = datastore["comparison"]["num_employees"]
    assert len(df) == 17
    np.testing.assert_array_equal(sorted(df.columns), sorted(cols))
    np.testing.assert_almost_equal(df["roll_chi2"].mean(), 4.0995701058201055)
