from datetime import datetime, timedelta

import pandas as pd
import pytest

from popmon.pipeline.dataset_splitter import split_dataset


@pytest.fixture
def test_dataframe_pandas():
    n_samples = 1000
    start = datetime.today()
    return pd.DataFrame(
        {
            "date": [start + timedelta(days=delta) for delta in range(n_samples)],
            "f1": [1] * n_samples,
            "f2": [0] * n_samples,
        }
    )


def test_split_dataset_pandas_int(test_dataframe_pandas):
    reference, df = split_dataset(test_dataframe_pandas, split=3, time_axis="date")

    assert reference.shape[0] == 3
    assert df.shape[0] == 997
    assert reference.columns.values.tolist() == ["date", "f1", "f2"]
    assert df.columns.values.tolist() == ["date", "f1", "f2"]


def test_split_dataset_pandas_int_underflow(test_dataframe_pandas):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=0, time_axis="date")

    assert e.value.args[0] == "Number of instances should be greater than 0"


def test_split_dataset_pandas_int_overflow(test_dataframe_pandas):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=1001, time_axis="date")

    assert (
        e.value.args[0]
        == "Returned dataframe is empty. Please adjust the `split` argument"
    )


def test_split_dataset_pandas_float(test_dataframe_pandas):
    reference, df = split_dataset(test_dataframe_pandas, split=0.45, time_axis="date")

    assert reference.shape[0] == 450
    assert df.shape[0] == 550
    assert reference.columns.values.tolist() == ["date", "f1", "f2"]
    assert df.columns.values.tolist() == ["date", "f1", "f2"]


def test_split_dataset_pandas_float_round(test_dataframe_pandas):
    reference, df = split_dataset(test_dataframe_pandas, split=0.8888, time_axis="date")

    assert reference.shape[0] == 888
    assert df.shape[0] == 112
    assert reference.columns.values.tolist() == ["date", "f1", "f2"]
    assert df.columns.values.tolist() == ["date", "f1", "f2"]


def test_split_dataset_pandas_float_underflow(test_dataframe_pandas):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=0.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"

    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=-1.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"


def test_split_dataset_pandas_float_overflow(test_dataframe_pandas):
    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=1.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"

    with pytest.raises(ValueError) as e:
        _ = split_dataset(test_dataframe_pandas, split=10.0, time_axis="date")

    assert e.value.args[0] == "Fraction should be 0 > fraction > 1"


def test_split_dataset_pandas_condition(test_dataframe_pandas):
    reference, df = split_dataset(
        test_dataframe_pandas,
        split=test_dataframe_pandas.date
        < datetime.today() + timedelta(days=50, hours=5),
        time_axis="date",
    )

    assert reference.shape[0] == 51
    assert df.shape[0] == 949
    assert reference.columns.values.tolist() == ["date", "f1", "f2"]
    assert df.columns.values.tolist() == ["date", "f1", "f2"]


def test_split_dataset_pandas_condition_false(test_dataframe_pandas):
    with pytest.raises(ValueError) as e:
        split_dataset(
            test_dataframe_pandas,
            split=test_dataframe_pandas.date < datetime.today() - timedelta(days=1),
            time_axis="date",
        )

    assert e.value.args[0] == "Reference is empty. Please adjust the `split` argument"
