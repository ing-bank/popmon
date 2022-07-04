from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest

from popmon.hist.filling import make_histograms
from popmon.pipeline.metrics import df_stability_metrics

try:
    from pyspark import __version__ as pyspark_version
    from pyspark.sql import SparkSession

    spark_found = True
except (ModuleNotFoundError, AttributeError):
    spark_found = False


@pytest.fixture
def spark_context():
    if not spark_found:
        return None

    current_path = Path(__file__).parent

    scala = "2.12" if int(pyspark_version[0]) >= 3 else "2.11"
    hist_spark_jar = current_path / f"jars/histogrammar-sparksql_{scala}-1.0.11.jar"
    hist_jar = current_path / f"jars/histogrammar_{scala}-1.0.11.jar"

    spark = (
        SparkSession.builder.master("local")
        .appName("popmon-pytest")
        .config("spark.jars", f"{hist_spark_jar},{hist_jar}")
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
            "bin_width": pd.Timedelta(365, "days").value,
            "bin_offset": pd.Timestamp(year=2000, month=1, day=1).value,
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
    names = [
        "age",
        "company",
        "eyeColor",
        "gender",
        "latitude",
        "longitude",
        "transaction",
    ]

    pytest.latitude_longitude["data"]["name"] = "'latitude:longitude'"
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

    # backwards compatibility
    for name in names:
        v1 = deepcopy(getattr(pytest, name))
        v1["data"]["name"] = f"'{name}'"

        v2 = deepcopy(getattr(pytest, name))
        v2["data"]["name"] = f"b'{name}'"

        output = current_hists[name].toJson()
        assert output == v1 or output == v2
