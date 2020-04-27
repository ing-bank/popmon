import pandas as pd
from pytest import test_comparer_df
from popmon.base import Pipeline
from popmon.alerting import ComputeTLBounds, traffic_light_summary, AlertsSummary
from popmon.analysis.apply_func import ApplyFunc


def test_integration_alerting():
    datastore = {
        "test_data": test_comparer_df,
    }

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 0.15],
            "dummy_feature:*": [0, 0, 0, 0],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [0, 0, 0, 0],
            "*": [0, 0, 0, 0],
        }
    }

    ctlb = ComputeTLBounds(
        read_key="test_data",
        store_key="traffic_light_bounds",
        apply_funcs_key="traffic_light_funcs",
        ignore_features=["dummy_feature"],
        monitoring_rules=conf["monitoring_rules"]
    )

    atlb = ApplyFunc(
        apply_to_key=ctlb.read_key,
        assign_to_key='output_data',
        apply_funcs_key="traffic_light_funcs",
    )

    pipeline = Pipeline(modules=[ctlb, atlb])
    datastore = pipeline.transform(datastore)

    output = datastore[atlb.store_key]["the_feature"]

    alerts_per_color_per_date = pd.DataFrame()
    for i, color in enumerate(["green", "yellow", "red"]):
        alerts_per_color_per_date[f"n_{color}"] = (output.values == i).sum(axis=1)

    alerts_total_per_color = alerts_per_color_per_date.sum(axis=0)

    assert alerts_total_per_color["n_green"] == 5
    assert alerts_total_per_color["n_yellow"] == 1
    assert alerts_total_per_color["n_red"] == 4


def test_traffic_light_summary():
    datastore = {
        "test_data": test_comparer_df,
    }

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 0.15],
            "dummy_feature:*": [0, 0, 0, 0],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [0, 0, 0, 0],
            "*": [0, 0, 0, 0],
        }
    }

    ctlb = ComputeTLBounds(
        read_key="test_data",
        store_key="traffic_light_bounds",
        apply_funcs_key="traffic_light_funcs",
        ignore_features=["dummy_feature"],
        monitoring_rules=conf["monitoring_rules"],
        prefix='tl_'
    )

    atlb = ApplyFunc(
        apply_to_key=ctlb.read_key,
        assign_to_key='output_data',
        apply_funcs_key="traffic_light_funcs",
    )

    tls = ApplyFunc(apply_to_key='output_data', apply_funcs=[dict(func=traffic_light_summary, axis=1, suffix='')],
                    assign_to_key='alerts')

    pipeline = Pipeline(modules=[ctlb, atlb, tls])
    datastore = pipeline.transform(datastore)

    output = datastore['alerts']["the_feature"]

    assert output["worst"].values[-1] == 2
    assert output["n_green"].values[-1] == 1
    assert output["n_yellow"].values[-1] == 0
    assert output["n_red"].values[-1] == 1


def test_traffic_light_summary_combination():
    datastore = {
        "test_data": test_comparer_df,
    }

    conf = {
        "monitoring_rules": {
            "the_feature:mae": [8, 4, 2, 0.15],
            "dummy_feature:*": [0, 0, 0, 0],
            "mse": [0.2, 0.11, 0.09, 0],
            "mae": [0, 0, 0, 0],
            "*": [0, 0, 0, 0],
        }
    }

    ctlb = ComputeTLBounds(
        read_key="test_data",
        store_key="traffic_light_bounds",
        apply_funcs_key="traffic_light_funcs",
        ignore_features=["dummy_feature"],
        monitoring_rules=conf["monitoring_rules"],
        prefix='tl_'
    )

    atlb = ApplyFunc(
        apply_to_key=ctlb.read_key,
        assign_to_key='output_data',
        apply_funcs_key="traffic_light_funcs",
    )

    tls = ApplyFunc(apply_to_key='output_data', apply_funcs=[dict(func=traffic_light_summary, axis=1, suffix='')],
                    assign_to_key='alerts')

    asum = AlertsSummary(read_key='alerts')

    pipeline = Pipeline(modules=[ctlb, atlb, tls, asum])
    datastore = pipeline.transform(datastore)

    alerts = datastore['alerts']
    assert '_AGGREGATE_' in alerts
    output = datastore['alerts']["_AGGREGATE_"]

    assert output["worst"].values[-1] == 2
    assert output["n_green"].values[-1] == 1
    assert output["n_yellow"].values[-1] == 0
    assert output["n_red"].values[-1] == 1
