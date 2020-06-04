import numpy as np

from popmon.base import Module


def test_popmon_module():
    class Scaler(Module):
        def __init__(self, input_key, output_key, mean, std):
            super().__init__()
            self.input_key = input_key
            self.output_key = output_key
            self.mean = mean
            self.std = std

        def transform(self, datastore):
            input_array = self.get_datastore_object(
                datastore, self.input_key, dtype=np.ndarray
            )
            res = input_array - np.mean(input_array)
            res = res / np.std(res)
            res = res * self.std
            res = res + self.mean
            datastore[self.output_key] = res
            return datastore

    test_module = Scaler(input_key="x", output_key="scaled_x", mean=2.0, std=0.3)

    datastore = {"x": np.arange(10)}
    datastore = test_module.transform(datastore)

    assert "x" in datastore  # check if key 'x' is still in the datastore
    np.testing.assert_almost_equal(np.mean(datastore["scaled_x"]), 2.0, decimal=5)
    np.testing.assert_almost_equal(np.std(datastore["scaled_x"]), 0.3, decimal=5)
