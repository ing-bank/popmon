import pandas as pd
import pytest

from popmon import resources
from popmon.analysis.hist_numpy import assert_similar_hists, check_similar_hists
from popmon.base import Pipeline
from popmon.hist.hist_splitter import HistSplitter
from popmon.io import JsonReader


def test_hist_splitter():

    hist_list = [
        "date:country",
        "date:bankrupt",
        "date:num_employees",
        "date:A_score",
        "date:A_score:num_employees",
    ]
    features = [
        "country",
        "bankrupt",
        "num_employees",
        "A_score",
        "A_score:num_employees",
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
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "output_hist" in datastore and isinstance(datastore["output_hist"], dict)
    assert len(datastore["output_hist"].keys()) == len(features)
    for f in features:
        assert f in datastore["output_hist"]
    for f in features:
        assert isinstance(datastore["output_hist"][f], pd.DataFrame)

    for f in features:
        df = datastore["output_hist"][f]
        split_list = df.reset_index().to_dict("records")
        hlist = [s["histogram"] for s in split_list]
        assert_similar_hists(hlist)


@pytest.mark.filterwarnings("ignore:Input histograms have inconsistent dimensions.")
def test_hist_splitter_filter():
    """Test of hist_splitter option filter_empty_split_hists

    One of the split histograms of type date:A_score:num_employees is empty and only contains a NaN.
    In this test, those empty split-histograms are *not* removed, leading to split-histograms of
    inconsistent types.
    """

    hist_list = ["date:A_score:num_employees"]
    features = ["A_score:num_employees"]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist",
                store_key="output_hist",
                features=hist_list,
                filter_empty_split_hists=False,
            ),
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "output_hist" in datastore and isinstance(datastore["output_hist"], dict)
    assert len(datastore["output_hist"].keys()) == len(features)
    for f in features:
        assert f in datastore["output_hist"]
    for f in features:
        assert isinstance(datastore["output_hist"][f], pd.DataFrame)

    for f in features:
        df = datastore["output_hist"][f]
        split_list = df.reset_index().to_dict("records")
        hlist = [s["histogram"] for s in split_list]
        check = check_similar_hists(hlist)
        assert check is False
