from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from popmon.pipeline.dataset_splitter import split_dataset

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


@pytest.fixture
def test_dataframe_spark(spark_context):
    n_samples = 1000
    start = datetime.today()
    df = pd.DataFrame(
        {
            "date": [start + timedelta(days=delta) for delta in range(n_samples)],
            "f1": [1] * n_samples,
            "f2": [0] * n_samples,
        }
    )
    spark_df = spark_context.createDataFrame(df)
    return spark_df


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_int(test_dataframe_spark):
    reference, df = split_dataset(test_dataframe_spark, split=3, time_axis="date")

    assert reference.count() == 3
    assert df.count() == 997
    assert reference.columns == ["date", "f1", "f2"]
    assert df.columns == ["date", "f1", "f2"]


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_int_underflow(test_dataframe_spark):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=0, time_axis="date")

    assert e.value.args[0] == "Number of instances should be greater than 0"


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_int_overflow(test_dataframe_spark):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=1001, time_axis="date")

    assert (
        e.value.args[0]
        == "Returned dataframe is empty. Please adjust the `split` argument"
    )


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_float(test_dataframe_spark):
    reference, df = split_dataset(test_dataframe_spark, split=0.45, time_axis="date")

    assert reference.count() == 450
    assert df.count() == 550
    assert reference.columns == ["date", "f1", "f2"]
    assert df.columns == ["date", "f1", "f2"]


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_float_round(test_dataframe_spark):
    reference, df = split_dataset(test_dataframe_spark, split=0.8888, time_axis="date")

    assert reference.count() == 888
    assert df.count() == 112
    assert reference.columns == ["date", "f1", "f2"]
    assert df.columns == ["date", "f1", "f2"]


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_float_underflow(test_dataframe_spark):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=0.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"

    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=-1.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_float_overflow(test_dataframe_spark):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=1.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"

    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_spark, split=10.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_condition(test_dataframe_spark):
    reference, df = split_dataset(
        test_dataframe_spark,
        split=f"date < '{(datetime.today() + timedelta(days=50, hours=5)).strftime('%Y-%m-%d %H:%M:%S')}'",
        time_axis="date",
    )

    assert reference.count() == 51
    assert df.count() == 949
    assert reference.columns == ["date", "f1", "f2"]
    assert df.columns == ["date", "f1", "f2"]


@pytest.mark.spark
@pytest.mark.skipif(not spark_found, reason="spark not found")
def test_split_dataset_spark_condition_false(test_dataframe_spark):
    with pytest.raises(ValueError) as e:
        split_dataset(
            test_dataframe_spark,
            split=f"date < '{(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}'",
            time_axis="date",
        )

    assert e.value.args[0] == "Reference is empty. Please adjust the `split` argument"
