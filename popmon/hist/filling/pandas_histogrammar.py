"""
Copyright Eskapade:
License Apache-2: https://github.com/KaveIO/Eskapade-Core/blob/master/LICENSE
Reference link:
https://github.com/KaveIO/Eskapade/blob/master/python/eskapade/analysis/links/hist_filler.py
All modifications copyright ING WBAA.
"""

import contextlib
import multiprocessing

import histogrammar as hg
import joblib
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

from ...hist.filling import utils
from ...hist.filling.histogram_filler_base import HistogramFillerBase


class PandasHistogrammar(HistogramFillerBase):
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

        :param list features: columns to pick up from input data. (default is all features)
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
        HistogramFillerBase.__init__(
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

    def assert_dataframe(self, df):
        """Check that input data is a filled pandas data frame.

        :param df: input (pandas) data frame
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"retrieved object not of type {pd.DataFrame}")
        if df.shape[0] == 0:
            raise RuntimeError("data is empty")
        return df

    def get_features(self, df):
        """Get columns of (pandas) dataframe

        :param df: input pandas dataframe
        """
        return df.columns.tolist()

    def get_quantiles(self, df, quantiles=[0.05, 0.95], columns=[]):
        """return dict with quantiles for given columns

        :param df: input pandas data frame
        :param quantiles: list of quantiles. default is [0.05, 0.95]
        :param columns: columns to select. default is all.
        """
        if len(columns) == 0:
            return {}
        qdf = df[columns].quantile(quantiles)
        qd = {c: qdf[c].values.tolist() for c in columns}
        return qd

    def get_nunique(self, df, columns=[]):
        """return dict with number of unique entries for given columns

        :param df: input pandas data frame
        :param columns: columns to select (optional)
        """
        if not columns:
            columns = df.columns
        return df[columns].nunique().to_dict()

    def process_features(self, df, cols_by_type):
        """Process features before histogram filling.

        Specifically, convert timestamp features to integers

        :param df: input (pandas) data frame
        :param cols_by_type: dictionary of column sets for each type
        :returns: output (pandas) data frame with converted timestamp features
        :rtype: pandas DataFrame
        """
        # timestamp variables are converted to ns here
        # make temp df for value counting (used below)
        idf = df[list(cols_by_type["num"]) + list(cols_by_type["str"])].copy()
        for col in cols_by_type["dt"]:
            self.logger.debug(
                'Converting column "{col}" of type "{type}" to nanosec.'.format(
                    col=col, type=self.var_dtype[col]
                )
            )
            idf[col] = df[col].apply(utils.to_ns)
        return idf

    def fill_histograms(self, idf):
        """Fill the histograms

        :param idf: converted input dataframe
        """
        # construct empty histograms if needed
        for cols in self.features:
            name = ":".join(cols)
            if name not in self._hists:
                # create an (empty) histogram of right type
                self._hists[name] = self.construct_empty_hist(cols)

        # parallel histogram filling with working progress bar
        num_cores = multiprocessing.cpu_count()
        with tqdm_joblib(
            tqdm(total=len(self.features), ncols=100)
        ) as progress_bar:  # noqa: F841
            res = Parallel(n_jobs=num_cores)(
                delayed(_fill_histogram)(
                    idf=idf[c], hist=self._hists[":".join(c)], features=c
                )
                for c in self.features
            )
            # update dictionary
            for name, hist in res:
                self._hists[name] = hist

    def construct_empty_hist(self, features):
        """Create an (empty) histogram of right type.

        Create a multi-dim histogram by iterating through the features in
        reverse order and passing a single-dim hist as input to the next
        column.

        :param list features: histogram features
        :return: created histogram
        :rtype: histogrammar.Count
        """
        hist = hg.Count()

        # create a multi-dim histogram by iterating through the features
        # in reverse order and passing a single-dim hist as input
        # to the next column
        revcols = list(reversed(features))
        for idx, col in enumerate(revcols):
            # histogram type depends on the data type
            dt = self.var_dtype[col]

            # processing function, e.g. only accept boolians during filling
            f = utils.QUANTITY[dt]
            if len(features) == 1:
                # df[col] is a pd.series
                quant = lambda x, fnc=f: fnc(x)  # noqa
            else:
                # df[features] is a pd.Dataframe
                # fix column to col
                quant = lambda x, fnc=f, clm=col: fnc(x[clm])  # noqa

            hist = self.get_hist_bin(hist, features, quant, col, dt)

        return hist


def _fill_histogram(idf, hist, features):
    """Fill input histogram with column(s) of input dataframe.

    Separate function call for parallellization.

    :param idf: input data frame used for filling histogram
    :param hist: empty histogrammar histogram about to be filled
    :param list features: histogram column(s)
    """
    name = ":".join(features)
    clm = features[0] if len(features) == 1 else features
    # do the actual filling
    hist.fill.numpy(idf[clm])
    return name, hist


# tqdm working with joblib
@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument

    From: https://stackoverflow.com/questions/24983493/tracking-progress-of-joblib-parallel-execution?rq=1
    """

    class TqdmBatchCompletionCallback:
        def __init__(self, time, index, parallel):
            self.index = index
            self.parallel = parallel

        def __call__(self, index):
            tqdm_object.update()
            if self.parallel._original_iterator is not None:
                self.parallel.dispatch_next()

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()
