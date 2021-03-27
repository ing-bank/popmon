from os.path import abspath, dirname, join

import pandas as pd
import pytest

from popmon.hist.filling import make_histograms
from popmon.pipeline.metrics import df_stability_metrics

try:
    from pyspark.sql import SparkSession

    spark_found = True
except (ModuleNotFoundError, AttributeError):
    spark_found = False


@pytest.fixture
def spark_context():
    if not spark_found:
        return None

    current_path = dirname(abspath(__file__))

    hist_spark_jar = join(current_path, "jars/histogrammar-sparksql_2.11-1.0.11.jar")
    hist_jar = join(current_path, "jars/histogrammar_2.11-1.0.11.jar")

    spark = (
        SparkSession.builder.master("local")
        .appName("popmon-pytest")
        .config("spark.jars", f"{hist_spark_jar},{hist_jar}")
        .config("spark.sql.execution.arrow.enabled", "false")
        .config("spark.sql.session.timeZone", "GMT")
        .getOrCreate()
    )
    return spark


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
@pytest.mark.filterwarnings(
    "ignore:createDataFrame attempted Arrow optimization because"
)
def test_spark_stability_metrics(spark_context):
    spark_df = spark_context.createDataFrame(pytest.test_df)

    # generate metrics directly from spark dataframe
    features = ["date:isActive", "date:eyeColor", "date:latitude"]
    bin_specs = {
        "date": {
            "bin_width": pd.Timedelta("1y").value,
            "bin_offset": pd.Timestamp("2000-1-1").value,
        },
        "latitude": {"bin_width": 5.0, "bin_offset": 0.0},
    }
    ds = df_stability_metrics(
        spark_df,
        time_axis="date",
        features=features,
        binning="unit",
        bin_specs=bin_specs,
    )

    cols = ["profiles", "comparisons", "traffic_lights", "alerts"]
    for c in cols:
        assert c in list(ds.keys())


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
@pytest.mark.filterwarnings(
    "ignore:createDataFrame attempted Arrow optimization because"
)
def test_spark_make_histograms(spark_context):
    pytest.age["data"]["name"] = "b'age'"
    pytest.company["data"]["name"] = "b'company'"
    pytest.eyesColor["data"]["name"] = "b'eyeColor'"
    pytest.gender["data"]["name"] = "b'gender'"
    pytest.isActive["data"]["name"] = "b'isActive'"
    pytest.latitude["data"]["name"] = "b'latitude'"
    pytest.longitude["data"]["name"] = "b'longitude'"
    pytest.transaction["data"]["name"] = "b'transaction'"

    pytest.latitude_longitude["data"]["name"] = "b'latitude:longitude'"
    pytest.latitude_longitude["data"]["bins:name"] = "unit_func"

    spark_df = spark_context.createDataFrame(pytest.test_df)

    # test make_histograms() function call with spark df
    current_hists = make_histograms(
        spark_df,
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
            "transaction",
        ],
        bin_specs={
            "transaction": {"num": 100, "low": -2000, "high": 2000},
            "longitude": {"bin_width": 5.0, "bin_offset": 0.0},
            "latitude": {"bin_width": 5.0, "bin_offset": 0.0},
        },
        binning="unit",
    )

    assert current_hists["age"].toJson() == pytest.age
    assert current_hists["company"].toJson() == pytest.company
    assert current_hists["eyeColor"].toJson() == pytest.eyesColor
    assert current_hists["gender"].toJson() == pytest.gender
    assert current_hists["latitude"].toJson() == pytest.latitude
    assert current_hists["longitude"].toJson() == pytest.longitude
    assert current_hists["transaction"].toJson() == pytest.transaction
