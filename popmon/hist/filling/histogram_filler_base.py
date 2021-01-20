"""
Copyright Eskapade:
License Apache-2: https://github.com/KaveIO/Eskapade-Core/blob/master/LICENSE
Reference link:
https://github.com/KaveIO/Eskapade/blob/master/python/eskapade/analysis/histogram_filling.py
All modifications copyright ING WBAA.
"""

import copy
import logging
from collections import defaultdict

import histogrammar as hg
import numpy as np
import pandas as pd

from ...base import Module
from ...hist.filling.utils import check_column, check_dtype


class HistogramFillerBase(Module):
    """Base class link to fill histograms.

    Timestamp features are
    converted to nanoseconds before the binning is applied.
    Semi-clever auto-binning is applied in case no bin specifications are provided.
    Final histograms are stored in the datastore.
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
            For multi-dimensional histograms, separate the column names with a ":"
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
        super().__init__()

        features = features or []
        self.features = [check_column(c) for c in features]
        if not any([binning == opt for opt in ["auto", "unit"]]):
            raise TypeError('binning should be "auto" or "unit".')
        self.binning = binning
        self.bin_specs = bin_specs or {}
        self.time_axis = time_axis
        var_dtype = var_dtype or {}
        self.var_dtype = {k: check_dtype(v) for k, v in var_dtype.items()}
        self.read_key = read_key
        self.store_key = store_key

        # several default unit values
        self._unit_bin_specs = {"bin_width": 1.0, "bin_offset": 0.0}
        self._unit_timestamp_specs = {
            "bin_width": pd.Timedelta(days=30).value,
            "bin_offset": pd.Timestamp("2010-01-04").value,
        }
        self._auto_n_bins_1d = nbins_1d
        self._auto_n_bins_2d = nbins_2d
        self._auto_n_bins_3d = nbins_3d
        self._nunique_threshold = max_nunique

        # these get filled during execution
        self._hists = {}

    def assert_dataframe(self, df):
        """assert dataframe datatype"""
        raise NotImplementedError("assert_dataframe not implemented!")

    def get_features(self, df):
        raise NotImplementedError("get_features not implemented!")

    def get_quantiles(self, df, quantiles, columns):
        """return dict with quantiles for given columns"""
        raise NotImplementedError("get_quantiles not implemented!")

    def get_nunique(self, df, columns):
        """return dict with number of unique entries for given columns"""
        raise NotImplementedError("get_nunique not implemented!")

    def process_features(self, df, cols_by_type):
        raise NotImplementedError("process_features not implemented!")

    def fill_histograms(self, idf):
        raise NotImplementedError("fill_histograms not implemented!")

    def construct_empty_hist(self, features):
        raise NotImplementedError("construct_empty_hist not implemented!")

    def _auto_n_bins(self, c):
        """Return number of bins for this histogram

        :param list c: list of columns for this histogram
        :return: number of bins to use for this histogram
        """
        if isinstance(c, str):
            c = [c]
        if len(self.time_axis) > 0 and c[0] == self.time_axis:
            # in case of time-axis, use fine-grained binning
            # do this by removing first element, decreasing size of c.
            # note that affects original input c, so copy first
            c = copy.copy(c)
            del c[0]
        if len(c) <= 1:
            return self._auto_n_bins_1d
        elif len(c) == 2:
            return self._auto_n_bins_2d
        elif len(c) == 3:
            return self._auto_n_bins_3d
        else:
            return self._auto_n_bins_3d

    def _execute(self, df):
        """
        _execute() does five things:

        * check presence and data type of requested features
        * timestamp variables are converted to nanosec (integers)
        * clever auto-binning is done in case no bin-specs have been provided
        * do the actual value counting based on categories and created indices
        * then convert to histograms
        """
        df = self.assert_dataframe(df)

        # 1. check presence and data type of requested features
        # sort features into numerical, timestamp and category based
        cols_by_type = self.categorize_features(df)

        # 2. assign features to make histograms of (if not already provided)
        #    and figure out time-axis if provided
        #    check if all features are present in dataframe
        self.assign_and_check_features(df, cols_by_type)

        # 3. timestamp variables are converted to ns here
        idf = self.process_features(df, cols_by_type)

        # 4. complete bin-specs that have not been provided in case of 'auto' binning option
        if self.binning == "auto":
            self.auto_complete_bin_specs(idf, cols_by_type)

        # 5. do the actual histogram/counter filling
        self.logger.info(
            f"Filling {len(self.features)} specified histograms. {self.binning}-binning."
        )
        self.fill_histograms(idf)

        return self._hists

    def assign_and_check_features(self, df, cols_by_type):
        """auto assign feature to make histograms of and do basic checks on them

        :param df: input dateframe
        :param cols_by_type: dict of columns classified by type
        """
        # user leaves feature selection up to us
        no_initial_features = len(self.features) == 0

        all_cols = (
            list(cols_by_type["num"])
            + list(cols_by_type["dt"])
            + list(cols_by_type["str"])
        )

        # 1. assign / figure out a time axis
        if isinstance(self.time_axis, str) and len(self.time_axis) > 0:
            # a) specified time axis
            if self.time_axis not in all_cols:
                raise RuntimeError(
                    f'Specified time-axis "{self.time_axis}" not found in dataframe.'
                )
        elif isinstance(self.time_axis, bool) and self.time_axis:
            # b) try to figure out time axis
            self.time_axis = ""
            num = len(cols_by_type["dt"])
            if num == 1:
                # the obvious choice
                self.time_axis = list(cols_by_type["dt"])[0]
                self.logger.info(f'Time-axis automatically set to "{self.time_axis}"')
            elif num == 0:
                self.logger.warning(
                    "No obvious time-axes found to choose from. So not used."
                )
            else:
                self.logger.warning(
                    f'Found {num} time-axes: {cols_by_type["dt"]}. Set *one* time_axis manually! Now NOT used.'
                )
        else:
            # c) no time axis
            self.time_axis = ""

        # 2. assign all features to make histograms of, in case not provided by user
        if no_initial_features:
            if len(self.time_axis) > 0:
                # time-axis is selected: make histograms of all columns in dataframe vs time-axis
                self.features = [
                    [self.time_axis, c]
                    for c in sorted(self.get_features(df))
                    if c != self.time_axis
                ]
            else:
                # make histograms of all columns in dataframe
                self.features = [[c] for c in sorted(self.get_features(df))]

        # 3. check presence of all features (in case provided by user)
        all_selected_cols = np.unique([j for i in self.features for j in i])
        for c in all_selected_cols:
            if c not in self.get_features(df):
                raise RuntimeError(f"Requested feature {c} not in dataframe.")

        # 4. check number of unique entries for categorical features
        #    this can be an expensive call, so avoid if possible. do run however when debugging.
        if no_initial_features or self.logger.level == logging.DEBUG:
            str_cols = [c for c in all_selected_cols if c in cols_by_type["str"]]
            nuniq = self.get_nunique(df, str_cols)
            huge_cats = []
            for c in str_cols:
                if nuniq[c] < self._nunique_threshold:
                    continue
                if no_initial_features:
                    # we're the boss. we're not going to histogram this ...
                    huge_cats.append(c)
                else:  # debug mode
                    self.logger.warning(
                        f"Column {c} has {nuniq[c]} unique entries (large). Really histogram it?"
                    )
            # scrub self.features of huge categories.
            self.features = [
                cols
                for cols in self.features
                if not any([c in huge_cats for c in cols])
            ]

    def auto_complete_bin_specs(self, df, cols_by_type):
        """auto complete the bin-specs that have not been provided

        :param df: input dataframe
        :param cols_by_type: dict of columns classified by type
        """
        # auto-determine binning of numerical and time features for which no bin_specs exist
        # do this based on range of 5-95% quantiles, so extreme outliers are binned separately
        # otherwise, the idea is to always reuse 1-dim binning for high n-dim, if those exist.
        bs_keys = list(self.bin_specs.keys())  # create initial unchanging list of keys
        all_selected_cols = np.unique([j for i in self.features for j in i])
        cols = list(cols_by_type["num"]) + list(cols_by_type["dt"])
        num_cols = [c for c in all_selected_cols if c in cols and c not in bs_keys]

        # quantiles for bin specs
        int_cols = [c for c in num_cols if c in cols_by_type["int"]]
        quantiles_i = self.get_quantiles(df, quantiles=[0.0, 1.0], columns=int_cols)
        float_cols = [c for c in num_cols if c not in cols_by_type["int"]]
        quantiles_f = self.get_quantiles(df, quantiles=[0.05, 0.95], columns=float_cols)

        for cols in self.features:
            n = ":".join(cols)
            if len(cols) == 1 and n not in num_cols:
                continue
            if n in bs_keys:
                # already provided; will pick that one up
                continue
            # get default number of bins for n-dim histogram
            n_bins = self._auto_n_bins(cols)
            specs = []
            for idx, c in enumerate(cols):
                if c not in num_cols or c in bs_keys:
                    # skip categorical; revert to what is already provided by user at 1dim-level
                    specs.append({})
                    continue

                if c in float_cols:
                    q = quantiles_f[c]
                    # by default, n_bins covers range 5-95% quantiles + we add 10%
                    # basically this gives a nice plot when plotted
                    # specs for Bin and Sparselybin histograms
                    if q[1] == q[0]:
                        # in case of highly imbalanced data it can happen that q05=q95. If so use min and max instead.
                        q = (self.get_quantiles(df, quantiles=[0.0, 1.0], columns=[c]))[
                            c
                        ]
                    qdiff = (q[1] - q[0]) * (1.0 / 0.9) if q[1] > q[0] else 1.0
                    bin_width = qdiff / float(n_bins)
                    bin_offset = q[0] - qdiff * 0.05
                    low = q[0] - qdiff * 0.05
                    high = q[1] + qdiff * 0.05
                elif c in int_cols:
                    # for ints use bins around integer values
                    low = quantiles_i[c][0]
                    high = quantiles_i[c][1]
                    bin_width = np.max((np.round((high - low) / float(n_bins)), 1.0))
                    bin_offset = low = np.floor(low - 0.5) + 0.5
                    n_bins = int((high - low) // bin_width) + int(
                        (high - low) % bin_width > 0.0
                    )
                    high = low + n_bins * bin_width

                if c == self.time_axis and idx == 0:
                    # time axis is always sparselybin (unbound)
                    specs.append({"bin_width": bin_width, "bin_offset": bin_offset})
                elif len(cols) >= 3:
                    # always binned histogram for high n-dim histograms, avoid potentially exploding histograms
                    specs.append({"num": n_bins, "low": low, "high": high})
                else:
                    # sparse allowed for low dimensional histograms (1 and 2 dim)
                    specs.append({"bin_width": bin_width, "bin_offset": bin_offset})
            if len(cols) == 1:
                specs = specs[0]
            self.bin_specs[n] = specs

    def get_data_type(self, df, col):
        """Get data type of dataframe column.

        :param df: input data frame
        :param str col: column
        """
        if col not in self.get_features(df):
            raise KeyError(f'column "{col:s}" not in input dataframe')
        return df[col].dtype

    def categorize_features(self, df):
        """Categorize features of dataframe by data type.

        :param df: input (pandas) data frame
        """
        # check presence and data type of requested features
        # sort features into numerical, timestamp and category based
        cols_by_type = defaultdict(set)

        features = (
            self.features if self.features else [[c] for c in self.get_features(df)]
        )

        for col_list in features:
            for col in col_list:

                dt = check_dtype(self.get_data_type(df, col))

                if col not in self.var_dtype:
                    self.var_dtype[col] = dt

                if np.issubdtype(dt, np.integer):
                    colset = cols_by_type["int"]
                    if col not in colset:
                        colset.add(col)
                if np.issubdtype(dt, np.number):
                    colset = cols_by_type["num"]
                    if col not in colset:
                        colset.add(col)
                elif np.issubdtype(dt, np.datetime64):
                    colset = cols_by_type["dt"]
                    if col not in colset:
                        colset.add(col)
                else:
                    colset = cols_by_type["str"]
                    if col not in colset:
                        colset.add(col)

                self.logger.debug(
                    'Data type of column "{col}" is "{type}".'.format(
                        col=col, type=self.var_dtype[col]
                    )
                )
        return cols_by_type

    def var_bin_specs(self, c, idx=0):
        """Determine bin_specs to use for variable c.

        :param list c: list of variables, or string variable
        :param int idx: index of the variable in c, for which to return the bin specs. default is 0.
        :return: selected bin_specs of variable
        """
        if isinstance(c, str):
            c = [c]
        n = ":".join(c)

        # determine default bin specs
        dt = np.dtype(self.var_dtype[c[idx]])
        is_timestamp = isinstance(dt.type(), np.datetime64)
        default = (
            self._unit_bin_specs if not is_timestamp else self._unit_timestamp_specs
        )

        # get bin specs
        if n in self.bin_specs and len(c) > 1 and len(c) == len(self.bin_specs[n]):
            result = self.bin_specs[n][idx]
            if not result:
                result = self.bin_specs.get(c[idx], default)
        else:
            result = self.bin_specs.get(c[idx], default)
        return result

    def get_histograms(self, input_df):
        """Handy function to directly get dict of histograms corresponding to input dataframe.

        :param input_df: spark/pandas input dataframe
        :return: dict of histograms
        """
        return self._execute(input_df)

    def get_features_specs(self):
        """Return bin specifications used to generate histograms

        Can then be passed on to other histogram filler to get identical histograms.
        """
        features = [":".join(c) for c in self.features]  # rejoin substrings
        return features, self.bin_specs, self.var_dtype, self.time_axis

    def transform(self, datastore):
        """Transform function called when used as module in a pipeline

        :param dict datastore: input datastore
        :return: datastore
        """
        if not isinstance(self.read_key, str) and len(self.read_key) > 0:
            raise ValueError("read_key has not been properly set.")
        if not isinstance(self.store_key, str) and len(self.store_key) > 0:
            raise ValueError("store_key has not been properly set.")
        if self.read_key not in datastore:
            raise KeyError("read_key not found in datastore")

        df = datastore[self.read_key]
        hists = self.get_histograms(df)
        datastore[self.store_key] = hists
        return datastore

    def get_hist_bin(self, hist, features, quant, col, dt):
        is_number = np.issubdtype(dt, np.number)
        is_timestamp = np.issubdtype(dt, np.datetime64)

        if is_number or is_timestamp:
            # numbers and timestamps are put in a sparse binned histogram
            specs = self.var_bin_specs(features, features.index(col))
            if "bin_width" in specs:
                hist = hg.SparselyBin(
                    binWidth=specs["bin_width"],
                    origin=specs.get("bin_offset", 0),
                    quantity=quant,
                    value=hist,
                )
            elif "num" in specs and "low" in specs and "high" in specs:
                hist = hg.Bin(
                    num=specs["num"],
                    low=specs["low"],
                    high=specs["high"],
                    quantity=quant,
                    value=hist,
                )
            else:
                raise RuntimeError("Do not know how to interpret bin specifications.")
        else:
            # string and booleans are treated as categories
            hist = hg.Categorize(quantity=quant, value=hist)

        return hist
