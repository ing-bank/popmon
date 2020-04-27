import numpy as np
import pandas as pd
from popmon.base import Pipeline
from popmon.analysis.apply_func import ApplyFunc, apply_func_array, apply_func
from popmon.analysis.profiling.pull_calculator import RollingPullCalculator, \
    ReferencePullCalculator, ExpandingPullCalculator, RefMedianMadPullCalculator
from popmon.analysis.functions import pull
from pytest import test_comparer_df


def get_test_data():
    df = pd.DataFrame()
    df["a"] = np.arange(100)
    df["b"] = 1 + (df["a"] % 2)
    return df


def test_pull():
    datastore = dict()
    datastore["to_profile"] = {"asc_numbers": get_test_data()}

    module1 = ApplyFunc(apply_to_key="to_profile")
    module1.add_apply_func(np.std, suffix='_std', entire=True)
    module1.add_apply_func(np.mean, suffix='_mean', entire=True)

    module2 = ApplyFunc(apply_to_key="to_profile", features=["asc_numbers"])
    module2.add_apply_func(pull, suffix='_pull', axis=1, suffix_mean='_mean', suffix_std='_std', cols=['a', 'b'])

    pipeline = Pipeline(modules=[module1, module2])
    datastore = pipeline.transform(datastore)

    p = datastore['to_profile']["asc_numbers"]

    np.testing.assert_almost_equal(p["a_pull"].values[0], -1.714816)
    np.testing.assert_almost_equal(p["b_pull"].values[0], -1.0)


def test_apply_func_module():
    datastore = dict()
    datastore["to_profile"] = {"asc_numbers": get_test_data()}

    def func(x):
        return x + 1

    module = ApplyFunc(apply_to_key="to_profile", store_key="profiled", features=["asc_numbers"])

    module.add_apply_func(np.std, entire=True)
    module.add_apply_func(np.mean, entire=True)
    module.add_apply_func(func)

    datastore = module.transform(datastore)

    p = datastore['profiled']["asc_numbers"]

    np.testing.assert_equal(p["a_mean"].values[0], 49.5)
    np.testing.assert_equal(p["b_mean"].values[0], 1.5)
    np.testing.assert_almost_equal(p["a_std"].values[0], 28.86607)
    np.testing.assert_almost_equal(p["b_std"].values[0], 0.5)


def test_variance_comparer():
    datastore = dict()
    datastore["to_profile"] = test_comparer_df

    module1 = ApplyFunc(apply_to_key="to_profile", features=["the_feature", "dummy_feature"])
    module1.add_apply_func(np.std, suffix='_std', entire=True)
    module1.add_apply_func(np.mean, suffix='_mean', entire=True)

    module2 = ApplyFunc(apply_to_key="to_profile", features=["the_feature", "dummy_feature"])
    module2.add_apply_func(pull, suffix='_pull', axis=1, suffix_mean='_mean', suffix_std='_std')

    pipeline = Pipeline(modules=[module1, module2])
    datastore = pipeline.transform(datastore)

    p = datastore['to_profile']["the_feature"]
    np.testing.assert_almost_equal(p["mae_pull"].values[2], -0.1017973, 5)
    np.testing.assert_almost_equal(p["mae_pull"].values[3], 1.934149074, 6)

    p = datastore['to_profile']["dummy_feature"]
    np.testing.assert_almost_equal(p["mae_pull"].values[0], -0.6107839182)


def test_reference_pull_comparer():
    datastore = dict()
    datastore["to_profile"] = test_comparer_df

    mod = ReferencePullCalculator(reference_key='to_profile', assign_to_key='to_profile',
                                  features=["the_feature", "dummy_feature"])
    datastore = mod.transform(datastore)

    p = datastore['to_profile']["the_feature"]
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[2], -0.1017973, 5)
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[3], 1.934149074, 6)

    p = datastore['to_profile']["dummy_feature"]
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[0], -0.6107839182)


def test_median_mad_pull_comparer():
    datastore = dict()
    datastore["to_profile"] = test_comparer_df

    mod = RefMedianMadPullCalculator(reference_key='to_profile', assign_to_key='to_profile',
                                     features=["the_feature", "dummy_feature"])
    datastore = mod.transform(datastore)

    p = datastore['to_profile']["the_feature"]
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[2], 0.6745)
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[3], 6.070500000000004)

    p = datastore['to_profile']["dummy_feature"]
    np.testing.assert_almost_equal(p["mae_ref_pull"].values[0], -0.6745)


def test_rolling_pull_comparer():
    datastore = dict()
    datastore["to_profile"] = test_comparer_df

    mod = RollingPullCalculator(read_key='to_profile', features=["the_feature", "dummy_feature"], window=3)
    datastore = mod.transform(datastore)

    p = datastore['to_profile']["the_feature"]
    np.testing.assert_almost_equal(p["mae_roll_pull"].values[-1], -1.0811798054391777, 5)

    p = datastore['to_profile']["dummy_feature"]
    np.testing.assert_almost_equal(p["mae_roll_pull"].values[-1], -1.0811798054391777)


def test_expanding_pull_comparer():
    datastore = dict()
    datastore["to_profile"] = test_comparer_df

    mod = ExpandingPullCalculator(read_key='to_profile', features=["the_feature", "dummy_feature"])
    datastore = mod.transform(datastore)

    p = datastore['to_profile']["the_feature"]
    np.testing.assert_almost_equal(p["mae_exp_pull"].values[-1], -0.9292716592757299, 6)

    p = datastore['to_profile']["dummy_feature"]
    np.testing.assert_almost_equal(p["mae_exp_pull"].values[-1], -0.9292716592757299)


def test_apply_func():
    feature = "asc_numbers"
    df = get_test_data()

    apply_funcs = [{'func': np.std, 'features': [feature], 'metrics': ['a', 'b'], 'entire': True},
                   {'func': np.mean, 'features': [feature], 'metrics': ['a', 'b'], 'entire': True}]

    d = apply_func(feature=feature, selected_metrics=['a', 'b'], df=df, arr=apply_funcs[0])
    np.testing.assert_array_equal(list(d.keys()), ['a_std', 'b_std'])
    np.testing.assert_almost_equal(d["a_std"], 28.86607)
    np.testing.assert_almost_equal(d["b_std"], 0.5)

    d = apply_func(feature=feature, selected_metrics=['a', 'b'], df=df, arr=apply_funcs[1])
    np.testing.assert_array_equal(list(d.keys()), ['a_mean', 'b_mean'])
    np.testing.assert_equal(d["a_mean"], 49.5)
    np.testing.assert_equal(d["b_mean"], 1.5)


def test_apply_func_array():
    feature = "asc_numbers"
    df = get_test_data()

    apply_funcs = [{'func': np.std, 'features': [feature], 'metrics': ['a', 'b'], 'entire': True},
                   {'func': np.mean, 'features': [feature], 'metrics': ['a', 'b'], 'entire': True}]

    f, p = apply_func_array(feature=feature, metrics=['a', 'b'], apply_to_df=df, assign_to_df=None,
                            apply_funcs=apply_funcs, same_key=True)

    cols = ['a', 'b', 'a_std', 'b_std', 'a_mean', 'b_mean']
    np.testing.assert_array_equal(p.columns, cols)
    np.testing.assert_equal(p["a_mean"].values[0], 49.5)
    np.testing.assert_equal(p["b_mean"].values[0], 1.5)
    np.testing.assert_almost_equal(p["a_std"].values[0], 28.86607)
    np.testing.assert_almost_equal(p["b_std"].values[0], 0.5)
