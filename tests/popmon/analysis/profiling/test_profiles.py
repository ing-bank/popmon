import numpy as np

from popmon.analysis.profiling.profiles import profile_fraction_of_true


def test_fraction_of_true():
    res = profile_fraction_of_true([], [])
    assert np.isnan(res)
    res = profile_fraction_of_true(["a"], [10])
    assert np.isnan(res)
    res = profile_fraction_of_true(["a", "b", "c"], [10, 10, 10])
    assert np.isnan(res)

    res = profile_fraction_of_true(np.array(["True", "False"]), np.array([0, 0]))
    assert np.isnan(res)
    res = profile_fraction_of_true(np.array(["True", "False"]), np.array([10, 10]))
    assert res == 0.5
    res = profile_fraction_of_true(np.array([True, False]), [10, 10])
    assert res == 0.5

    res = profile_fraction_of_true(np.array(["True"]), np.array([10]))
    assert res == 1.0
    res = profile_fraction_of_true(np.array([True]), np.array([10]))
    assert res == 1.0
    res = profile_fraction_of_true(np.array(["False"]), np.array([10]))
    assert res == 0.0
    res = profile_fraction_of_true(np.array([False]), np.array([10]))
    assert res == 0.0
