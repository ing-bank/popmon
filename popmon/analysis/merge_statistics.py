# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import pandas as pd

from ..base import Module


class MergeStatistics(Module):
    """ Merging dictionaries of features containing dataframes with statistics as its values.
    """

    def __init__(self, read_keys, store_key):
        """Initialize an instance of MergeStatistics.

        :param str read_keys: list of keys of input data to read from the datastore
        :param str store_key: key of output data to store in the datastore
        """
        super().__init__()
        self.read_keys = read_keys
        self.store_key = store_key

    def transform(self, datastore):
        dicts = [
            self.get_datastore_object(datastore, read_key, dtype=dict)
            for read_key in self.read_keys
        ]
        merged_stats = dict()
        for dict_ in dicts:
            for feature in dict_.keys():
                # we add statistics dataframe to the final output for specific feature however
                # if the feature already exists - we concatenate its dataframe with the existing one
                if isinstance(dict_[feature], pd.DataFrame):
                    if feature in merged_stats:
                        merged_stats[feature] = merged_stats[feature].combine_first(
                            dict_[feature]
                        )
                    else:
                        merged_stats[feature] = dict_[feature]
        datastore[self.store_key] = merged_stats
        return datastore
