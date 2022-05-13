# Copyright (c) 2022 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import logging
from abc import ABCMeta
from functools import wraps


def datastore_helper(func):
    """Decorator for passing and storing only the relevant keys in the datastore to
    the transform() method."""

    @wraps(func)
    def _transform(self, datastore):
        """Transformation helper function"""
        inputs = {}
        self.logger.debug(f"load from: {type(self)}")
        for key in self._input_keys:
            key_value = self.__dict__[key]
            if key_value and len(key_value) > 0:
                if isinstance(key_value, list):
                    inputs[key] = [datastore.get(k) for k in key_value]
                else:
                    inputs[key] = datastore.get(key_value)
            else:
                inputs[key] = None

            self.logger.debug(
                "load(key=%s, key_value=%s, value=%.100s})", key, key_value, inputs[key]
            )

        # transformation
        outputs = func(self, *list(inputs.values()))

        # transform returns None if no update needs to be made
        if outputs is not None:
            if len(self._output_keys) == 1:
                outputs = (outputs,)

            for k, v in zip(self._output_keys, outputs):
                key_value = self.__dict__[k]
                self.logger.debug(
                    "store(key=%s, key_value=%s, value=%.100s)", k, key_value, v
                )
                if key_value and len(key_value) > 0:
                    datastore[key_value] = v

        return datastore

    return _transform


class ModuleMetaClass(type):
    """Metaclass that wraps all transform() methods using the datastore_helper
    This obviates the need to decorate all methods in subclasses"""

    def __new__(cls, name, bases, local):
        if "transform" in local:
            value = local["transform"]
            if callable(value):
                local["transform"] = datastore_helper(value)
        return type.__new__(cls, name, bases, local)


def combine_classes(*args):
    """Combine multiple metaclasses"""
    name = "".join(a.__name__ for a in args)
    return type(name, args, {})


class Module(metaclass=combine_classes(ABCMeta, ModuleMetaClass)):
    """Abstract base class used for modules in a pipeline."""

    _input_keys = None
    _output_keys = None

    def __init__(self):
        """Module initialization"""
        self.logger = logging.getLogger()
        self.features = []
        self.feature_begins_with = []
        self.ignore_features = []

    def _get_values(self, keys):
        """Get the class attribute values for certain keys."""
        values = {}
        for x in keys:
            value = self.__dict__[x]
            if value != "" and value is not None and value not in values:
                values[x] = value
        return values

    def get_inputs(self):
        return self._get_values(self._input_keys)

    def get_outputs(self):
        return self._get_values(self._output_keys)

    def get_description(self):
        return ""

    def set_logger(self, logger):
        """Set logger of module

        :param logger: input logger
        """
        self.logger = logger

    @staticmethod
    def get_datastore_object(datastore, feature, dtype, default=None):
        """Get object from datastore.

        Bit more advanced than dict.get()

        :param dict datastore: input datastore
        :param str feature: key of object to retrieve
        :param obj dtype: required datatype of object. Could be specific data type or tuple of dtypes
        :param obj default: object to default to in case key not found.
        :return: retrieved object
        """
        if default is not None:
            obj = datastore.get(feature, default)
        else:
            try:
                obj = datastore[feature]
            except KeyError as e:
                raise ValueError(f"`{feature}` not found in the datastore!") from e

        if not isinstance(obj, dtype):
            raise TypeError(f"obj `{feature}` is not an instance of `{dtype}`!")
        return obj

    def get_features(self, all_features: list) -> list:
        """Get all features that meet feature_begins_with and ignore_features requirements

        :param list all_features: input features list
        :return: pruned features list
        :rtype: list
        """
        all_features = sorted(all_features)
        features = self.features or all_features

        if self.feature_begins_with:
            features = [k for k in features if k.startswith(self.feature_begins_with)]
        if self.ignore_features:
            features = [k for k in features if k not in self.ignore_features]

        features_not_in_input = [
            feature for feature in features if feature not in all_features
        ]
        for feature in features_not_in_input:
            self.logger.warning(f'Feature "{feature}" not in input data; skipping.')

        features = [feature for feature in features if feature in all_features]
        return features

    def transform(self, *args):
        """Central function of the module.

        Typically transform() takes something from the datastore, does something to it, and puts the results
        back into the datastore again, to be passed on to the next module in the pipeline.

        :param dict datastore: input datastore
        :return: updated output datastore
        :rtype: dict
        """
        raise NotImplementedError
