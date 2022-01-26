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


from typing import List

import pandas as pd

from ..base import Module


class MergeStatistics(Module):
    """Merging dictionaries of features containing dataframes with statistics as its values."""

    _input_keys = ("read_keys",)
    _output_keys = ("store_key",)

    def __init__(self, read_keys: List[str], store_key: str):
        """Initialize an instance of MergeStatistics.

        :param list read_keys: list of keys of input data to read from the datastore
        :param str store_key: key of output data to store in the datastore
        """
        super().__init__()
        self.read_keys = read_keys
        self.store_key = store_key

    def transform(self, dicts: list):
        merged_stats = {}
        for dict_ in dicts:
            for feature, values in dict_.items():
                if isinstance(values, pd.DataFrame):
                    # we add statistics dataframe to the final output for specific feature however
                    # if the feature already exists - we concatenate its dataframe with the existing one
                    if feature in merged_stats:
                        merged_stats[feature] = merged_stats[feature].combine_first(
                            values
                        )
                    else:
                        merged_stats[feature] = values
        return merged_stats
