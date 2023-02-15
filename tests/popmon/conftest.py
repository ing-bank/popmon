from json import load
from os.path import dirname

import numpy as np
import pandas as pd
import pytest

from popmon import resources


def get_comparer_data():
    test_comparer_df = {}
    df = pd.DataFrame(
        data={
            "mae": [0.1, 0.11, 0.12, 0.2, 0.09],
            "mse": [0.1, 0.1, 0.1, 0.1, 0.1],
            "date": [2000, 2001, 2002, 2003, 2004],
        }
    )
    df = df.set_index("date")
    test_comparer_df["the_feature"] = df

    df = pd.DataFrame(
        data={
            "mae": [0.1, 0.11, 0.12, 0.2, 0.09],
            "date": [2000, 2001, 2002, 2003, 2004],
        }
    )
    df = df.set_index("date")
    test_comparer_df["dummy_feature"] = df

    return test_comparer_df


def get_ref_comparer_data():
    ref_data = pd.DataFrame()
    # we do not add "mse_std" on purpose to have some noise in the data
    ref_data["metric"] = ["mae_mean", "mae_std", "mae_pull", "mse_mean"]
    ref_data["value"] = [0.124, 0.0376, 0.0376, 0.09]
    ref_data["feature"] = "the_feature"
    ref_data["date"] = np.arange(ref_data.shape[0]) + 2010

    return ref_data


def pytest_configure():
    # attach common test data
    pytest.test_comparer_df = get_comparer_data()
    pytest.test_ref_comparer_df = get_ref_comparer_data()

    parent_path = dirname(__file__)
    TEMPLATE_PATH = f"{parent_path}/hist/resource"
    CSV_FILE = "test.csv.gz"

    with open("{}/{}".format(TEMPLATE_PATH, "age.json")) as f:
        pytest.age = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "company.json")) as f:
        pytest.company = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "date.json")) as f:
        pytest.date = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "eyesColor.json")) as f:
        pytest.eyeColor = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "gender.json")) as f:
        pytest.gender = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "isActive.json")) as f:
        pytest.isActive = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "isActive_age.json")) as f:
        pytest.isActive_age = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "latitude.json")) as f:
        pytest.latitude = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "longitude.json")) as f:
        pytest.longitude = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "latitude_longitude.json")) as f:
        pytest.latitude_longitude = load(f)

    with open("{}/{}".format(TEMPLATE_PATH, "transaction.json")) as f:
        pytest.transaction = load(f)

    df = pd.read_csv(resources.data(CSV_FILE))
    df["date"] = pd.to_datetime(df["date"])
    pytest.test_df = df
