import numpy as np
import pandas as pd
import pytest

from popmon.alerting import (
    ComputeTLBounds,
    DynamicBounds,
    StaticBounds,
    TrafficLightAlerts,
    pull_bounds,
    traffic_light,
)
from popmon.analysis.apply_func import ApplyFunc
from popmon.analysis.functions import (
    expanding_mean,
    expanding_std,
    pull,
    rolling_lr,
    rolling_mean,
)
from popmon.base import Pipeline
from popmon.config import Settings
from popmon.visualization.section_generator import SectionGenerator


def get_test_data():
    df = pd.DataFrame()
    df["a"] = np.arange(100)
    df["b"] = 1 + (df["a"] % 2)
    return df


def test_traffic_light():
    assert traffic_light(2.2, red_high=2.0, yellow_high=0.8) == 2
    assert traffic_light(1.7, red_high=2.0, yellow_high=0.8) == 1
    assert traffic_light(0.5, red_high=2.0, yellow_high=0.8) == 0

    with pytest.raises(Exception):
        traffic_light(0.5, red_high=1.0, yellow_high=2.0)


@pytest.mark.filterwarnings("ignore:Using or importing the ABCs from")
def test_apply_monitoring_business_rules():
    datastore = {"test_data": pytest.test_comparer_df}

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 0.15],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [1, 0, 0, 0],
            "*_pull": [7, 4, -4, -7],
        }
    }

    pipeline = TrafficLightAlerts(
        read_key="test_data", rules=conf["monitoring_rules"], store_key="tl"
    )

    datastore = pipeline.transform(datastore)

    assert "tl" in datastore
    test_data = datastore["tl"]

    assert "the_feature" in test_data
    output = test_data["the_feature"]
    assert output["mae"].tolist() == [2, 2, 2, 1, 2]
    assert output["mse"].tolist() == [0] * 5

    assert "dummy_feature" in test_data
    output = test_data["dummy_feature"]
    assert output["mae"].tolist() == [1] * 5


def test_apply_dynamic_traffic_light_bounds():
    datastore = {"to_profile": {"asc_numbers": get_test_data()}}

    conf = {"monitoring_rules": {"*_pull": [7, 4, -4, -7]}}

    m1 = ApplyFunc(
        apply_to_key="to_profile", features=["asc_numbers"], metrics=["a", "b"]
    )
    m1.add_apply_func(np.std, suffix="_std")
    m1.add_apply_func(np.mean, suffix="_mean")

    m2 = ApplyFunc(apply_to_key="to_profile", features=["asc_numbers"])
    m2.add_apply_func(
        pull, suffix="_pull", axis=1, suffix_mean="_mean", suffix_std="_std"
    )

    m5 = DynamicBounds(
        read_key="to_profile",
        store_key="tl",
        rules=conf["monitoring_rules"],
        suffix_mean="_mean",
        suffix_std="_std",
    )

    pipeline = Pipeline(modules=[m1, m2, m5])
    datastore = pipeline.transform(datastore)

    assert "tl" in datastore
    test_data = datastore["tl"]
    assert "asc_numbers" in test_data
    p = test_data["asc_numbers"]

    tlcs = [
        "traffic_light_a_red_high",
        "traffic_light_a_yellow_high",
        "traffic_light_a_yellow_low",
        "traffic_light_a_red_low",
        "traffic_light_b_red_high",
        "traffic_light_b_yellow_high",
        "traffic_light_b_yellow_low",
        "traffic_light_b_red_low",
    ]
    for c in tlcs:
        assert c in p.columns

    np.testing.assert_almost_equal(p["traffic_light_a_red_high"].values[0], 251.5624903)
    np.testing.assert_almost_equal(
        p["traffic_light_a_yellow_high"].values[0], 164.96428019
    )
    np.testing.assert_almost_equal(
        p["traffic_light_a_yellow_low"].values[0], -65.96428019
    )
    np.testing.assert_almost_equal(
        p["traffic_light_a_red_low"].values[0], -152.56249033
    )
    np.testing.assert_almost_equal(p["traffic_light_b_red_high"].values[0], 5.0)
    np.testing.assert_almost_equal(p["traffic_light_b_yellow_high"].values[0], 3.5)
    np.testing.assert_almost_equal(p["traffic_light_b_yellow_low"].values[0], -0.5)
    np.testing.assert_almost_equal(p["traffic_light_b_red_low"].values[0], -2.0)


def test_apply_static_traffic_light_bounds():
    datastore = {"to_profile": {"asc_numbers": get_test_data()}}

    conf = {"monitoring_rules": {"*_pull": [7, 4, -4, -7]}}

    m1 = ApplyFunc(
        apply_to_key="to_profile", features=["asc_numbers"], metrics=["a", "b"]
    )
    m1.add_apply_func(np.std, suffix="_std")
    m1.add_apply_func(np.mean, suffix="_mean")

    m2 = ApplyFunc(apply_to_key="to_profile", features=["asc_numbers"])
    m2.add_apply_func(
        pull, suffix="_pull", axis=1, suffix_mean="_mean", suffix_std="_std"
    )

    m5 = StaticBounds(
        read_key="to_profile",
        store_key="tl",
        rules=conf["monitoring_rules"],
        suffix_mean="_mean",
        suffix_std="_std",
    )

    pipeline = Pipeline(modules=[m1, m2, m5])
    datastore = pipeline.transform(datastore)

    assert "tl" in datastore
    test_data = datastore["tl"]
    assert "asc_numbers" in test_data
    p = test_data["asc_numbers"]

    tlcs = [
        "traffic_light_a_red_high",
        "traffic_light_a_yellow_high",
        "traffic_light_a_yellow_low",
        "traffic_light_a_red_low",
        "traffic_light_b_red_high",
        "traffic_light_b_yellow_high",
        "traffic_light_b_yellow_low",
        "traffic_light_b_red_low",
    ]
    for c in tlcs:
        assert c in p.columns

    np.testing.assert_almost_equal(p["traffic_light_a_red_high"].values[1], 251.5624903)
    np.testing.assert_almost_equal(
        p["traffic_light_a_yellow_high"].values[1], 164.96428019
    )
    np.testing.assert_almost_equal(
        p["traffic_light_a_yellow_low"].values[1], -65.96428019
    )
    np.testing.assert_almost_equal(
        p["traffic_light_a_red_low"].values[1], -152.56249033
    )
    np.testing.assert_almost_equal(p["traffic_light_b_red_high"].values[1], 5.0)
    np.testing.assert_almost_equal(p["traffic_light_b_yellow_high"].values[1], 3.5)
    np.testing.assert_almost_equal(p["traffic_light_b_yellow_low"].values[1], -0.5)
    np.testing.assert_almost_equal(p["traffic_light_b_red_low"].values[1], -2.0)


def test_rolling_window_funcs():
    datastore = {"to_profile": {"asc_numbers": get_test_data()}}

    m = ApplyFunc(
        apply_to_key="to_profile", features=["asc_numbers"], metrics=["a", "b"]
    )
    m.add_apply_func(
        rolling_mean, suffix="_rolling_3_mean", entire=True, window=3, shift=0
    )
    m.add_apply_func(
        rolling_lr, suffix="_rolling_10_slope", entire=True, window=10, index=0
    )
    m.add_apply_func(
        rolling_lr, suffix="_rolling_10_intercept", entire=True, window=10, index=1
    )

    datastore = Pipeline(modules=[m]).transform(datastore)
    feature_df = datastore["to_profile"]["asc_numbers"]

    np.testing.assert_array_almost_equal(
        feature_df["a_rolling_3_mean"].tolist(), [np.nan] * 2 + list(range(1, 99))
    )
    np.testing.assert_array_almost_equal(
        feature_df["a_rolling_10_slope"].tolist(), [np.nan] * 9 + [1.0] * 91
    )
    np.testing.assert_array_almost_equal(
        feature_df["a_rolling_10_intercept"].tolist(),
        [np.nan] * 9 + [float(i) for i in range(0, 91)],
    )


def test_report_traffic_light_bounds():
    datastore = {"to_profile": {"asc_numbers": get_test_data()}}
    settings = Settings()
    settings.monitoring.monitoring_rules = {
        "the_feature:mae": [8, 4, 2, 0.15],
        "mse": [0.2, 0.11, 0.09, 0],
        "mae": [1, 0, 0, 0],
        "*_pull": [7, 4, -4, -7],
    }
    settings.monitoring.pull_rules = {"*_pull": [7, 4, -4, -7]}

    m1 = ApplyFunc(
        apply_to_key="to_profile", features=["asc_numbers"], metrics=["a", "b"]
    )
    m1.add_apply_func(expanding_mean, suffix="_std", entire=True)
    m1.add_apply_func(expanding_std, suffix="_mean", entire=True)

    m2 = ApplyFunc(apply_to_key="to_profile", features=["asc_numbers"])
    m2.add_apply_func(
        pull, suffix="_pull", axis=1, suffix_mean="_mean", suffix_std="_std"
    )

    ctlb = ComputeTLBounds(
        read_key="to_profile",
        store_key="static_tlb",
        monitoring_rules=settings.monitoring.monitoring_rules,
    )

    m3 = ComputeTLBounds(
        read_key="to_profile",
        monitoring_rules=settings.monitoring.pull_rules,
        apply_funcs_key="dynamic_tlb",
        func=pull_bounds,
        metrics_wide=True,
        axis=1,
    )

    m4 = ApplyFunc(
        apply_to_key=m3.read_key, assign_to_key="dtlb", apply_funcs_key="dynamic_tlb"
    )

    rg = SectionGenerator(
        read_key="to_profile",
        store_key="section",
        section_name="Profiles",
        dynamic_bounds="dtlb",
        static_bounds="static_tlb",
        settings=settings.report,
    )

    pipeline = Pipeline(modules=[m1, m2, ctlb, m3, m4, rg])
    datastore = pipeline.transform(datastore)
