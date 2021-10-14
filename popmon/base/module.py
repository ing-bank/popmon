# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
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
from abc import ABC, abstractmethod


class Module(ABC):
    """Base class used for modules in a pipeline."""

    _input_keys = None
    _output_keys = None

    def __init__(self):
        """Module initialization"""
        self.logger = logging.getLogger()
        self.features = []
        self.feature_begins_with = []
        self.ignore_features = []

    def get_inputs(self):
        in_keys = {}
        for x in self._input_keys:
            in_key = self.__dict__[x]
            if in_key != "" and in_key is not None and in_key not in in_keys:
                in_keys[x] = in_key
        return in_keys

    def get_outputs(self):
        out_keys = {}
        for x in self._output_keys:
            out_key = self.__dict__[x]
            if out_key != "" and out_key is not None and out_key not in out_keys:
                out_keys[x] = out_key
        return out_keys

    # @abstractmethod
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
            except KeyError:
                raise ValueError(f"`{feature}` not found in the datastore!")

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
                f"load(key={key}, key_value={key_value}, value={str(inputs[key]):.100s})"
            )

        # cache datastore
        self._datastore = datastore

        # transformation
        outputs = self.transform(*list(inputs.values()))

        # transform returns None if no update needs to be made
        if outputs is not None:
            if len(self._output_keys) == 1:
                outputs = (outputs,)

            for k, v in zip(self._output_keys, outputs):
                key_value = self.__dict__[k]
                self.logger.debug(
                    f"store(key={k}, key_value={key_value}, value={str(v):.100s})"
                )
                if key_value and len(key_value) > 0:  # and v is not None:
                    datastore[key_value] = v

        return datastore

    def transform(self, *args):
        """Central function of the module.

        Typically transform() takes something from the datastore, does something to it, and puts the results
        back into the datastore again, to be passed on to the next module in the pipeline.

        :param dict datastore: input datastore
        :return: updated output datastore
        :rtype: dict
        """
        raise NotImplementedError
