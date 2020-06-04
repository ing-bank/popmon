import logging

import numpy as np

from popmon.base import Module, Pipeline


class LogTransformer(Module):
    def __init__(self, input_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key

    def transform(self, datastore):
        input_array = self.get_datastore_object(
            datastore, self.input_key, dtype=np.ndarray
        )
        datastore[self.output_key] = np.log(input_array)
        self.logger.info(
            "{module_name} is calculated.".format(module_name=self.__class__.__name__)
        )
        return datastore


class PowerTransformer(Module):
    def __init__(self, input_key, output_key, power):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.power = power

    def transform(self, datastore):
        input_array = self.get_datastore_object(
            datastore, self.input_key, dtype=np.ndarray
        )
        datastore[self.output_key] = np.power(input_array, self.power)
        return datastore


class SumNormalizer(Module):
    def __init__(self, input_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key

    def transform(self, datastore):
        input_array = self.get_datastore_object(
            datastore, self.input_key, dtype=np.ndarray
        )
        datastore[self.output_key] = input_array / input_array.sum()
        return datastore


class WeightedSum(Module):
    def __init__(self, input_key, weight_key, output_key):
        super().__init__()
        self.input_key = input_key
        self.weight_key = weight_key
        self.output_key = output_key

    def transform(self, datastore):
        input_array = self.get_datastore_object(
            datastore, self.input_key, dtype=np.ndarray
        )
        weights = self.get_datastore_object(
            datastore, self.weight_key, dtype=np.ndarray
        )
        datastore[self.output_key] = np.sum(input_array * weights)
        self.logger.info(
            "{module_name} is calculated.".format(module_name=self.__class__.__name__)
        )
        return datastore


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
