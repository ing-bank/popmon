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


import pandas as pd

from ..base import Module
from ..hist.hist_utils import (
    get_histogram,
    is_timestamp,
    split_hist_along_first_dimension,
)


class HistSplitter(Module):

    """Module divides a histogram along first axis encountered, eg. time.

    For example, split histogram time:x:y along time axis.
    This will produce a data-frame summarizing the split information,
    where time is the index and each row is a x:y histogram.
    """

    def __init__(
        self,
        read_key,
        store_key,
        features=None,
        ignore_features=None,
        feature_begins_with="",
        project_on_axes=True,
        flatten_output=False,
        short_keys=True,
        var_timestamp=None,
        index_col="date",
        hist_col="histogram",
        filter_empty_split_hists=True,
    ):
        """Initialize an instance.

        :param str read_key: key of input histogram-dict to read from data store
        :param str store_key: key of output data to store in data store
        :param list features: features of histograms to pick up from input data (optional)
        :param list ignore_features: ignore list of features to compare with reference, if present (optional)
        :param str feature_begins_with: require feature to begin with a given string (optional)
        :param bool project_on_axes: histogram time:x:y will also be divided along x and y. default is true.
        :param bool flatten_output: if true, flatten_output instead of add histogram-dict.
        :param bool short_keys: if true, use short descriptive dict keys in storage dict.
        :param list var_timestamp: list of variables that are converted timestamps (in ns since 1970).
        :param str index_col: key for index in split dictionary. default is 'date'
        :param str hist_col: key in output dict that contains the histogram. default is 'histogram'
        :param bool filter_empty_split_hists: filter out empty sub-histograms after splitting. default is True.
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        self.features = features or []
        self.ignore_features = ignore_features or []
        self.feature_begins_with = feature_begins_with
        self.project_on_axes = project_on_axes
        self.flatten_output = flatten_output
        self.short_keys = short_keys
        self.var_timestamp = var_timestamp or []
        self.index_col = index_col
        self.hist_col = hist_col
        self.filter_empty_split_hists = filter_empty_split_hists

        if self.flatten_output and self.short_keys:
            raise ValueError(
                "flatten_output requires short_keys attribute to be False."
            )

    def update_divided(self, divided, split, yname):
        if self.flatten_output:
            divided.update(split)
        else:
            divided[yname] = [
                {self.index_col: k, self.hist_col: h} for k, h in split.items()
            ]
        return divided

    def transform(self, datastore):
        divided = {}

        self.logger.info(
            f'Splitting histograms "{self.read_key}" as "{self.store_key}"'
        )
        data = self.get_datastore_object(datastore, self.read_key, dtype=dict)

        # determine all possible features, used for comparison below
        features = self.get_features(data.keys())

        # if so requested split selected histograms along first axis, and then divide
        for feature in features[:]:
            self.logger.debug(f'Now splitting histogram "{feature}"')
            hist = get_histogram(data[feature])
            if hist.n_dim <= 1:
                self.logger.debug(
                    f'Histogram "{feature}" does not have two or more dimensions, nothing to split; skipping.'
                )
                continue

            cols = feature.split(":")
            if len(cols) != hist.n_dim:
                self.logger.error(
                    f'Dimension of histogram "{feature}" not consistent: {hist.n_dim} vs {len(cols)}; skipping.'
                )
                continue

            xname, yname = cols[0], ":".join(cols[1:])  # 'time:x:y' -> 'time', 'x:y'
            if yname in divided:
                self.logger.debug(f'Histogram "{yname}" already divided; skipping.')
                continue

            # if requested split selected histograms along first axis. e.g. time:x:y is split along time
            # then check if sub-hists of x:y can be further projected. eg. x:y is projected on x and y as well.
            # datatype properties
            is_ts = is_timestamp(hist) or xname in self.var_timestamp
            split = split_hist_along_first_dimension(
                hist=hist,
                short_keys=self.short_keys,
                convert_time_index=is_ts,
                xname=xname,
                yname=yname,
                filter_empty_split_hists=self.filter_empty_split_hists,
            )
            if not split:
                self.logger.warning(f'Split histogram "{yname}" is empty; skipping.')
                continue

            self.update_divided(divided=divided, split=split, yname=yname)

        # turn divided dicts into dataframes with index
        keys = list(divided.keys())
        for k in keys:
            divided[k] = pd.DataFrame(divided.pop(k)).set_index(self.index_col)

        datastore[self.store_key] = divided
        return datastore
