# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
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


import numpy as np
import pandas as pd

from ...hist.filling.pandas_histogrammar import PandasHistogrammar


class NumpyHistogrammar(PandasHistogrammar):
    """Fill histogrammar histograms.

    Algorithm to fill histogrammar style bin, sparse-bin and category histograms.

    Timestamp features are converted to nanoseconds before
    the binning is applied. Final histograms are stored in the datastore.
    """

    def __init__(
        self,
        features=None,
        binning="unit",
        bin_specs=None,
        time_axis="",
        var_dtype=None,
        read_key=None,
        store_key=None,
        nbins_1d=40,
        nbins_2d=20,
        nbins_3d=10,
        max_nunique=500,
    ):
        """Initialize module instance.

        Store and do basic check on the attributes HistogramFillerBase.

        :param list features: colums to pick up from input data. (default is all features)
            For multi-dimensional histograms, separate the column names with a :

            Example features list is:

            .. code-block:: python

                features = ['x', 'date', 'date:x', 'date:y', 'date:x:y']

        :param str binning: default binning to revert to in case bin_specs not supplied. options are:
            "unit" or "auto", default is "unit". When using "auto", semi-clever binning is automatically done.
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp features

            Example bin_specs dictionary is:

            .. code-block:: python

                bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                             'y': {'num': 10, 'low': 0.0, 'high': 2.0},
                             'x:y': [{}, {'num': 5, 'low': 0.0, 'high': 1.0}]}

            In the bin specs for x:y, x reverts to the 1-dim setting.

        :param str time_axis: name of datetime feature, used as time axis, eg 'date'. if True, will be guessed.
            If time_axis is set, if no features given, features becomes: ['date:x', 'date:y', 'date:z'] etc.
        :param dict var_dtype: dictionary with specified datatype per feature (optional)
        :param str read_key: key of input histogram-dict to read from data store .
            (only required when calling transform(datastore) as module)
        :param str store_key: key of output data to store in data store
            (only required when calling transform(datastore) as module)
        :param int nbins_1d: auto-binning number of bins for 1d histograms. default is 40.
        :param int nbins_2d: auto-binning number of bins for 2d histograms. default is 20.
        :param int nbins_3d: auto-binning number of bins for 3d histograms. default is 10.
        :param int max_nunique: auto-binning threshold for unique categorical values. default is 500.
        """
        PandasHistogrammar.__init__(
            self,
            features,
            binning,
            bin_specs,
            time_axis,
            var_dtype,
            read_key,
            store_key,
            nbins_1d,
            nbins_2d,
            nbins_3d,
            max_nunique,
        )

    def _execute(self, df):
        if not isinstance(df, np.ndarray):
            raise TypeError("retrieved object not of type np.ndarray")
        return super()._execute(pd.DataFrame(df))
