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


class Module:
    """Base class used for modules in a pipeline."""

    def __init__(self):
        """Module initialization"""
        self.logger = logging.getLogger()
        self.features = []
        self.feature_begins_with = []
        self.ignore_features = []

    def set_logger(self, logger):
        """Set logger of module

        :param logger: input logger
        """
        self.logger = logger

    def get_datastore_object(self, datastore, feature, dtype, default=None):
        """Get object from datastore.

        Bit more advanced than dict.get()

        :param dict datastore: input datastore
        :param str feature: key of object to retrieve
        :param obj dtype: required datatype of object. Could be specific data type or tuple of dtypes
        :param obj default: object to default to in case key not found.
        :return: retrieved object
        """
        obj = datastore.get(feature)
        if obj is None:
            if default is not None:
                obj = default
            else:
                raise ValueError(f"`{feature}` not found in the datastore!")
        if not isinstance(obj, dtype):
            raise ValueError(f"obj `{feature}` is not an instance of `{dtype}`!")
        return obj

    def get_features(self, all_features):
        """Get all features that meet feature_begins_with and ignore_features requirements

        :param list all_features: input features list
        :return: pruned features list
        :rtype: list
        """
        all_features = sorted(all_features)
        features = self.features
        if not self.features:
            features = all_features
        if self.feature_begins_with:
            features = [k for k in features if k.startswith(self.feature_begins_with)]
        if self.ignore_features:
            features = [k for k in features if k not in self.ignore_features]

        features_not_in_input = [
            feature for feature in features if feature not in all_features
        ]
        features = [feature for feature in features if feature in all_features]

        for feature in features_not_in_input:
            self.logger.warning(f'Feature "{feature}" not in input data; skipping.')

        return features

    def transform(self, datastore):
        """Central function of the module.

        Typically transform() takes something from the datastore, does something to it, and puts the results
        back into the datastore again, to be passed on to the next module in the pipeline.

        :param dict datastore: input datastore
        :return: updated output datastore
        :rtype: dict
        """
        return datastore
