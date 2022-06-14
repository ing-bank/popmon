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


import numpy as np
import pandas as pd

from popmon.analysis.profiling.profiles import Profiles
from popmon.stats import numpy as pm_np

from ...analysis.hist_numpy import get_2dgrid
from ...base import Module
from ...hist.hist_utils import get_bin_centers, is_numeric, is_timestamp, sum_entries


@Profiles.register(
    key=["min", "max", "p01", "p05", "p16", "p50", "p84", "p95", "p99"],
    description=[
        "Minimum value",
        "Maximum value",
        "1% percentile",
        "5% percentile",
        "16% percentile",
        "50% percentile (median)",
        "84% percentile",
        "95% percentile",
        "99% percentile",
    ],
    dim=1,
    htype="num",
)
def profile_quantiles(x, w):
    return pm_np.quantile(
        x, q=[0.0, 1.0, 0.01, 0.05, 0.16, 0.50, 0.84, 0.95, 0.99], weights=w
    )


@Profiles.register(key="mean", description="Mean value", dim=1, htype="num")
def profile_mean(x, w):
    return pm_np.mean(x, w)


@Profiles.register(key="std", description="Standard deviation", dim=1, htype="num")
def profile_std(x, w):
    return pm_np.std(x, w)


@Profiles.register(key="nan", description="Number of missing entries (NaN)", dim=1)
def profile_nan(hist):
    if hasattr(hist, "nanflow"):
        return hist.nanflow.entries
    elif hasattr(hist, "bins") and "NaN" in hist.bins:
        return hist.bins["NaN"].entries
    return 0


@Profiles.register(
    key="overflow",
    description="Number of values larger than the maximum bin-edge of the histogram.",
    dim=1,
)
def profile_overflow(hist):
    if hasattr(hist, "overflow"):
        return hist.overflow.entries
    return 0


@Profiles.register(
    key="underflow",
    description="Number of values smaller than the minimum bin-edge of the histogram.",
    dim=1,
)
def profile_underflow(hist):
    if hasattr(hist, "underflow"):
        return hist.underflow.entries
    return 0


@Profiles.register(
    key="phik",
    description="phi-k correlation between the two variables of the histogram",
    dim=2,
)
def profile_phik(hist):
    from phik import phik

    # calculate phik correlation
    try:
        grid = get_2dgrid(hist)
    except Exception:
        raise

    try:
        phi_k = phik.phik_from_hist2d(observed=grid)
    except ValueError:
        # self.logger.debug(
        #     f"Not enough values in the 2d `{name}` time-split histogram to apply the phik test."
        # )
        phi_k = np.nan
    return phi_k


@Profiles.register(
    key="count", description="Number of entries (non-NaN and NaN)", dim=None
)
def profile_count(hist):
    return int(sum_entries(hist))


@Profiles.register(
    key="filled",
    description="Number of non-missing entries (non-NaN)",
    dim=1,
    htype="all",
)
def profile_filled(_, bin_counts):
    return bin_counts.sum()


@Profiles.register(
    key="distinct", description="Number of distinct entries", dim=1, htype="all"
)
def profile_distinct(bin_labels, bin_counts):
    return len(np.unique(bin_labels[bin_counts > 0]))


@Profiles.register(
    key="fraction_of_true", description="", dim=1, htype="cat"
)  # or type="bool"
def profile_fraction_of_true(bin_labels, bin_counts):
    return pm_np.fraction_of_true(bin_labels, bin_counts)


@Profiles.register(
    key="most_probable_value", description="Most probable value", dim=1, htype="all"
)
def profile_most_probable_value(bin_labels, bin_counts):
    return bin_labels[np.argmax(bin_counts)]


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
        hist_col="histogram",
        index_col="date",
        stats_functions=None,
    ):
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key

        self.features = features or []
        self.ignore_features = ignore_features or []
        self.var_timestamp = var_timestamp or []
        self.hist_col = hist_col
        self.index_col = index_col

        if stats_functions is not None:
            raise NotImplementedError()

    def _profile_1d_histogram(self, name, hist):
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
        for (key, htype), func in Profiles.get_profiles(dim=1).items():
            if htype is not None and htype != otype and htype != "all":
                # skipping; type not applicable
                continue

            if htype is None:
                args = [hist]
            else:
                args = [bin_labels, bin_counts]

            results = func(*args)

            if isinstance(key, (list, tuple)):
                for k, v in zip(key, results):
                    profile[k] = v
            else:
                profile[key] = results

        # postprocessing TS
        if is_ts:
            profile = {
                k: pd.Timestamp(v) if k != "std" else pd.Timedelta(v)
                for k, v in profile.items()
            }

        # postprocessing sum
        profile["count"] = profile["filled"] + profile["nan"]

        return profile

    def _profile_nd_histogram(self, name, hist, dim):
        if hist.n_dim < dim:
            self.logger.warning(
                f"Histogram {name} has {hist.n_dim} dimensions (<{dim}); cannot profile. Returning empty."
            )
            return {}

        # calc nd-histogram statistics
        profile = {}
        for (key, htype), func in Profiles.get_profiles(dim).items():
            if htype is None:
                result = func(hist)
            else:
                raise NotImplementedError("histogram types for nD not implemented")

            if isinstance(key, (list, tuple)):
                for k, v in zip(key, result):
                    profile[k] = v
            else:
                profile[key] = result

        return profile

    def _profile_hist(self, split, hist_name):
        if len(split) == 0:
            self.logger.error(f'Split histograms dict "{hist_name}" is empty. Return.')
            return []

        hist0 = split[0][self.hist_col]
        dimension = hist0.n_dim
        is_num = is_numeric(hist0)
        htype = "num" if is_num else "cat"

        # these are the profiled quantities we will monitor
        if dimension == 1:
            expected_fields = Profiles.get_profile_keys(dim=1, htype=htype)
        else:
            expected_fields = Profiles.get_profile_keys(dim=dimension)
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
                    f'Could not extract full profile for sub-hist "{hist_name} {index}". Skipping.'
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
