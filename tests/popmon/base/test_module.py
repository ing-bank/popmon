import numpy as np
import pytest

from popmon.base import Module


class Scaler(Module):
    _input_keys = ("input_key",)
    _output_keys = ("output_key",)

    def __init__(self, input_key, output_key, mean, std):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.mean = mean
        self.std = std

    def transform(self, input_array: np.ndarray):
        res = input_array - np.mean(input_array)
        res = res / np.std(res)
        res = res * self.std
        res = res + self.mean
        return res


@pytest.fixture()
def test_module():
    return Scaler(input_key="x", output_key="scaled_x", mean=2.0, std=0.3)


def test_popmon_module(test_module):
    datastore = {"x": np.arange(10)}
    datastore = test_module.transform(datastore)

    assert "x" in datastore  # check if key 'x' is still in the datastore
    np.testing.assert_almost_equal(np.mean(datastore["scaled_x"]), 2.0, decimal=5)
    np.testing.assert_almost_equal(np.std(datastore["scaled_x"]), 0.3, decimal=5)


def test_popmon_module_repr(test_module):
    assert str(test_module) == "Scaler(input_key='x', output_key='scaled_x')"
