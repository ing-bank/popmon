import logging

import numpy as np

from popmon.base import Module, Pipeline


class LogTransformer(Module):
    _input_keys = ("input_key",)
    _output_keys = ("output_key",)

    def __init__(self, input_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key

    def transform(self, input_array: np.ndarray):
        output = np.log(input_array)
        self.logger.info(f"{self.__class__.__name__} is calculated.")
        return output


class PowerTransformer(Module):
    _input_keys = ("input_key",)
    _output_keys = ("output_key",)

    def __init__(self, input_key, output_key, power):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.power = power

    def transform(self, input_array: np.ndarray):
        result = np.power(input_array, self.power)
        return result


class SumNormalizer(Module):
    _input_keys = ("input_key",)
    _output_keys = ("output_key",)

    def __init__(self, input_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key

    def transform(self, input_array: np.ndarray):
        result = input_array / input_array.sum()
        return result


class WeightedSum(Module):
    _input_keys = ("input_key", "weight_key")
    _output_keys = ("output_key",)

    def __init__(self, input_key, weight_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.weight_key = weight_key
        self.output_key = output_key

    def transform(self, input_array: np.ndarray, weights: np.ndarray):
        result = np.sum(input_array * weights)
        self.logger.info(f"{self.__class__.__name__} is calculated.")
        return result


def test_popmon_pipeline():
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    datastore = {"x": np.array([7, 2, 7, 9, 6]), "weights": np.array([1, 1, 2, 1, 2])}
    expected_result = np.sum(
        np.power(np.log(datastore["x"]), 2) * datastore["weights"]
    ) / np.sum(datastore["weights"])

    log_pow_pipeline = Pipeline(
        modules=[
            LogTransformer(input_key="x", output_key="log_x"),
            PowerTransformer(input_key="log_x", output_key="log_pow_x", power=2),
        ]
    )

    pipeline = Pipeline(
        modules=[
            log_pow_pipeline,
            SumNormalizer(input_key="weights", output_key="norm_weights"),
            WeightedSum(
                input_key="log_pow_x", weight_key="norm_weights", output_key="res"
            ),
        ],
        logger=logger,
    )

    assert pipeline.transform(datastore)["res"] == expected_result
