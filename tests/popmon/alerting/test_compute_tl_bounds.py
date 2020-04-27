import pytest
from popmon.alerting import ComputeTLBounds, collect_traffic_light_bounds


def test_collect_traffic_light_bounds():
    test_dict = {"a": 2, "b:c": 5, "b:d": 6, "x:y:z": 17}

    pkeys, nkeys = collect_traffic_light_bounds(test_dict)

    assert nkeys == ["a"]
    assert len(pkeys) == 2
    assert pkeys["x:y"] == ["z"]
    assert set(pkeys["b"]) == {"c", "d"}


def test_compute_traffic_light_bounds():

    datastore = {
        "test_data": pytest.test_comparer_df,
    }

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 2],
            "dummy_feature:*": [0, 0, 0, 0],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [0, 0, 0, 0],
            "*": [0, 0, 0, 0],
        }
    }

    module = ComputeTLBounds(
        read_key="test_data",
        store_key="output_data",
        ignore_features=["dummy_feature"],
        monitoring_rules=conf["monitoring_rules"],
    )

    output = module.transform(datastore)["output_data"]
    assert "dummy_feature:mae" not in output.keys()
    assert output["the_feature:mae"] == [8, 4, 2, 2]
    assert output["the_feature:mse"] == [0.2, 0.11, 0.09, 0]


def test_compute_traffic_light_funcs():

    datastore = {
        "test_data": pytest.test_comparer_df,
    }

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 2],
            "dummy_feature:*": [0, 0, 0, 0],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [0, 0, 0, 0],
            "*": [0, 0, 0, 0],
        }
    }

    module = ComputeTLBounds(
        read_key="test_data",
        apply_funcs_key="output_data",
        monitoring_rules=conf["monitoring_rules"],
    )

    output = module.transform(datastore)["output_data"]
    assert len(output) == 3

    assert output[0]['features'] == ['dummy_feature']
    assert output[0]['metrics'] == ['mae']
    assert output[0]['args'] == (0, 0, 0, 0)

    assert output[1]['features'] == ['the_feature']
    assert output[1]['metrics'] == ['mae']
    assert output[1]['args'] == (8, 4, 2, 2)

    assert output[2]['features'] == ['the_feature']
    assert output[2]['metrics'] == ['mse']
    assert output[2]['args'] == (0.2, 0.11, 0.09, 0)
