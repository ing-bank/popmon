# Copyright (c) 2023 ING Analytics Wholesale Banking
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

from popmon.base import Module
from popmon.hist.hist_utils import get_bin_centers, is_numeric, is_timestamp


class HistProfiler(Module):
    """Generate profiles of histograms using default statistical functions.

    Profiles are:

    - 1 dim histograms, all: 'count', 'filled', 'distinct', 'nan', 'most_probable_value', 'overflow', 'underflow'.
    - 1 dim histograms, numeric: mean, std, min, max, p01, p05, p16, p50, p84, p95, p99.
    - 1 dim histograms, boolean: fraction of true entries.
    - 2 dim histograms: count, phi_k correlation constant, p-value and Z-score of contingency test.
    - n dim histograms: count (n >= 3)

    :param str read_key: key of the input test data to read from the datastore
    :param str store_key: key of the output data to store in the datastore
    :param list features: features of data-frames to pick up from input data (optional)
    :param list ignore_features: features to ignore (optional)
    :param list var_timestamp: list of timestamp variables (optional)
    :param str hist_col: key for histogram in split dictionary
    :param str index_col: key for index in split dictionary
    :param dict stats_functions: function_name, function(bin_labels, bin_counts) dictionary
    """

    _input_keys = ("read_key",)
    _output_keys = ("store_key",)

    def __init__(
        self,
        read_key,
        store_key,
        features=None,
        ignore_features=None,
        var_timestamp=None,
        hist_col: str = "histogram",
        index_col: str = "date",
        stats_functions=None,
    ) -> None:
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.var_timestamp = var_timestamp or []
        self.hist_col = hist_col
        self.index_col = index_col

        if stats_functions is not None:
            raise NotImplementedError

    def _profile_1d_histogram(self, name, hist):
        from popmon.analysis import Profiles

        # preprocessing value counts and TS
        is_num = is_numeric(hist)
        is_ts = is_timestamp(hist) or name in self.var_timestamp

        bin_labels, values = get_bin_centers(hist)
        bin_counts = np.array([v.entries for v in values])

        if len(bin_counts) == 0:
            self.logger.warning(f'Histogram "{name}" is empty; skipping.')
            return {}

        if is_ts:
            to_timestamp = np.vectorize(lambda x: pd.to_datetime(x).value)
            bin_labels = to_timestamp(bin_labels)

        otype = "num" if is_num else "cat"

        # calc 1d-histogram statistics
        profile = {}
        if otype == "cat":
            args = [bin_labels, bin_counts]
        else:
            bin_width = hist.bin_width()
            args = [bin_labels, bin_counts, bin_width]

        profile.update(Profiles.run(args, dim=1, htype=otype))

        args = [bin_labels, bin_counts]
        profile.update(Profiles.run(args, dim=1, htype="all"))

        # difference between htype=None and htype="all" are arguments (bin labels vs hist)
        profile.update(Profiles.run([hist], dim=1, htype=None))
        profile.update(Profiles.run([hist], dim=-1, htype=None))

        # postprocessing TS
        if is_ts:
            profile = {
                k: pd.Timestamp(v) if k != "std" else pd.Timedelta(v)
                for k, v in profile.items()
            }

        return profile

    def _profile_nd_histogram(self, name, hist, dim):
        from popmon.analysis import Profiles

        if hist.n_dim < dim:
            self.logger.warning(
                f"Histogram {name} has {hist.n_dim} dimensions (<{dim}); cannot profile. Returning empty."
            )
            return {}

        # calc nd-histogram statistics
        profile = Profiles.run([hist], dim=dim, htype=None)
        profile.update(Profiles.run([hist], dim=dim, htype="all"))
        profile.update(Profiles.run([hist], dim=dim, htype="num"))
        profile.update(Profiles.run([hist], dim=dim, htype="cat"))

        profile.update(Profiles.run([hist], dim=-1, htype=None))
        return profile

    def _profile_hist(self, split, hist_name):
        from popmon.analysis.profiling import Profiles

        if len(split) == 0:
            self.logger.error(f'Split histograms dict "{hist_name}" is empty. Return.')
            return []

        hist0 = split[0][self.hist_col]
        dimension = hist0.n_dim
        is_num = is_numeric(hist0)
        htype = "num" if is_num else "cat"

        # these are the profiled quantities we will monitor
        expected_fields = (
            Profiles.get_keys_by_dim_and_htype(dim=dimension, htype=htype)
            + Profiles.get_keys_by_dim_and_htype(dim=dimension, htype="all")
            + Profiles.get_keys_by_dim_and_htype(dim=dimension, htype=None)
        )

        # profiles regardless of dim and htype (e.g. count)
        expected_fields += Profiles.get_keys_by_dim_and_htype(dim=None, htype=None)

        # profiles regardless of dim
        expected_fields += Profiles.get_keys_by_dim_and_htype(dim=-1, htype=htype)
        expected_fields += Profiles.get_keys_by_dim_and_htype(dim=-1, htype="all")
        expected_fields += Profiles.get_keys_by_dim_and_htype(dim=-1, htype=None)

        expected_fields += [self.index_col, self.hist_col]

        # now loop over split-axis, e.g. time index, and profile each sub-hist x:y
        profile_list = []
        for hist_dict in split:
            index, hist = hist_dict[self.index_col], hist_dict[self.hist_col]

            profile = {self.index_col: index, self.hist_col: hist}

            if dimension == 1:
                profile.update(self._profile_1d_histogram(hist_name, hist))
            else:
                profile.update(
                    self._profile_nd_histogram(hist_name, hist, dim=dimension)
                )

            if sorted(profile.keys()) != sorted(expected_fields):
                self.logger.error(
                    f'Could not extract full profile for sub-hist "{hist_name} {index}".'
                    f"Differences: {set(profile.keys()).symmetric_difference(set(expected_fields))}. Skipping."
                )
            else:
                profile_list.append(profile)

        return profile_list

    def transform(self, data: dict) -> dict:
        self.logger.info(
            f'Profiling histograms "{self.read_key}" as "{self.store_key}"'
        )
        features = self.get_features(list(data.keys()))

        profiled = {}
        for feature in features[:]:
            df = self.get_datastore_object(data, feature, dtype=pd.DataFrame)
            hist_split_list = df.reset_index().to_dict("records")

            self.logger.debug(f'Profiling histogram "{feature}".')
            profile_list = self._profile_hist(split=hist_split_list, hist_name=feature)
            if len(profile_list) > 0:
                profiled[feature] = pd.DataFrame(profile_list).set_index(
                    [self.index_col]
                )

        return profiled
