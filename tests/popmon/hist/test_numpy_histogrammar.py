#!/usr/bin/env python3

import pytest

from popmon.base import Pipeline
from popmon.hist.filling import NumpyHistogrammar


def test_assert_dataframe():
    pandas_filler = NumpyHistogrammar(
        features=["age", "fruit", "latitude", ["longitude", "active"]]
    )
    with pytest.raises(TypeError):
        pandas_filler.assert_dataframe("coconut")


def test_get_histograms():

    np_array = pytest.test_df.to_records(index=False)

    np_filler = NumpyHistogrammar(
        features=[
            "date",
            "isActive",
            "age",
            "eyeColor",
            "gender",
            "company",
            "latitude",
            "longitude",
            ["isActive", "age"],
            ["latitude", "longitude"],
        ],
        bin_specs={
            "longitude": {"bin_width": 5, "bin_offset": 0},
            "latitude": {"bin_width": 5, "bin_offset": 0},
        },
    )
    current_hists = np_filler.get_histograms(np_array)

    assert current_hists["age"].toJson() == pytest.age
    assert current_hists["company"].toJson() == pytest.company
    assert current_hists["date"].toJson() == pytest.date
    assert current_hists["eyeColor"].toJson() == pytest.eyesColor
    assert current_hists["gender"].toJson() == pytest.gender
    assert current_hists["isActive"].toJson() == pytest.isActive
    assert current_hists["isActive:age"].toJson() == pytest.isActive_age
    assert current_hists["latitude"].toJson() == pytest.latitude
    assert current_hists["longitude"].toJson() == pytest.longitude
    assert current_hists["latitude:longitude"].toJson() == pytest.latitude_longitude


def test_get_histograms_module():

    np_filler = NumpyHistogrammar(
        features=[
            "date",
            "isActive",
            "age",
            "eyeColor",
            "gender",
            "company",
            "latitude",
            "longitude",
            ["isActive", "age"],
            ["latitude", "longitude"],
        ],
        bin_specs={
            "longitude": {"bin_width": 5, "bin_offset": 0},
            "latitude": {"bin_width": 5, "bin_offset": 0},
        },
        read_key="input",
        store_key="output",
    )

    pipeline = Pipeline(modules=[np_filler])
    datastore = pipeline.transform(
        datastore={"input": pytest.test_df.to_records(index=False)}
    )

    assert "output" in datastore
    current_hists = datastore["output"]

    assert current_hists["age"].toJson() == pytest.age
    assert current_hists["company"].toJson() == pytest.company
    assert current_hists["date"].toJson() == pytest.date
    assert current_hists["eyeColor"].toJson() == pytest.eyesColor
    assert current_hists["gender"].toJson() == pytest.gender
    assert current_hists["isActive"].toJson() == pytest.isActive
    assert current_hists["isActive:age"].toJson() == pytest.isActive_age
    assert current_hists["latitude"].toJson() == pytest.latitude
    assert current_hists["longitude"].toJson() == pytest.longitude
    assert current_hists["latitude:longitude"].toJson() == pytest.latitude_longitude
