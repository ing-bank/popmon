# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import numpy as np
import pandas as pd
from phik import phik

import popmon.stats.numpy as pm_np

from ...analysis.hist_numpy import get_2dgrid
from ...base import Module
from ...hist.histogram import sum_entries

DEFAULT_STATS = {
    "mean": pm_np.mean,
    "std": pm_np.std,
    "min,max,p01,p05,p16,p50,p84,p95,p99": lambda x, w: pm_np.quantile(
        x, q=[0.0, 1.0, 0.01, 0.05, 0.16, 0.50, 0.84, 0.95, 0.99], weights=w
    ),
}
NUM_NS_DAY = 24 * 3600 * int(1e9)


class HistProfiler(Module):
    """Generate profiles of histograms using default statistical functions.

    Profiles are:

    - 1 dim histograms, all: 'count', 'filled', 'distinct', 'nan', 'most_probable_value', 'overflow', 'underflow'.
    - 1 dim histograms, numeric: mean, std, min, max, p01, p05, p16, p50, p84, p95, p99.
    - 1 dim histograms, boolean: fraction of true entries.
    - 2 dim histograms: count, phi_k correlation constant, p-value and Z-score of contingency test.

    :param str read_key: key of the input test data to read from the datastore
    :param str store_key: key of the output data to store in the datastore
    :param list features: features of data-frames to pick up from input data (optional)
    :param list ignore_features: features to ignore (optional)
    :param list var_timestamp: list of timestamp variables (optional)
    :param str hist_col: key for histogram in split dictionary
    :param str index_col: key for index in split dictionary
    :param dict stats_functions: function_name, function(bin_labels, bin_counts) dictionary
    """

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

        self.general_stats_1d = [
            "count",
            "filled",
            "distinct",
            "nan",
            "most_probable_value",
            "overflow",
            "underflow",
        ]
        self.general_stats_2d = ["count", "phik"]
        self.category_stats_1d = ["fraction_true"]

        self.stats_functions = stats_functions
        if self.stats_functions is None:
            self.stats_functions = DEFAULT_STATS
            self.logger.debug(
                f"No stats function dict is provided. {self.stats_functions.keys()} is set as default"
            )

    def _profile_1d_histogram(self, name, hc):
        is_num = hc.is_num
        is_ts = hc.is_ts or name in self.var_timestamp

        bin_labels = np.array(hc.get_bin_centers()[0])
        bin_counts = np.array([v.entries for v in hc.get_bin_centers()[1]])

        if len(bin_counts) == 0:
            self.logger.warning(f'Histogram "{name}" is empty; skipping.')
            return dict()

        if is_ts:
            to_timestamp = np.vectorize(lambda x: pd.to_datetime(x).value)
            bin_labels = to_timestamp(bin_labels)

        profile = dict()
        profile["filled"] = bin_counts.sum()
        profile["nan"] = hc.hist.nanflow.entries if hasattr(hc.hist, "nanflow") else 0
        profile["overflow"] = (
            hc.hist.overflow.entries if hasattr(hc.hist, "overflow") else 0
        )
        profile["underflow"] = (
            hc.hist.underflow.entries if hasattr(hc.hist, "underflow") else 0
        )
        profile["count"] = profile["filled"] + profile["nan"]
        profile["distinct"] = len(np.unique(bin_labels))
        mpv = bin_labels[np.argmax(bin_counts)]  # most probable value
        profile["most_probable_value"] = mpv if not is_ts else pd.Timestamp(mpv)

        if is_num and profile["filled"] > 0:
            for f_names, func in self.stats_functions.items():
                names = f_names.split(",")
                results = func(bin_labels, bin_counts)
                if len(names) == 1:
                    results = [results]

                if is_ts:
                    results = [
                        pd.Timedelta(result)
                        if f_name == "std"
                        else pd.Timestamp(result)
                        for f_name, result in zip(name, results)
                    ]

                profile.update({k: v for k, v in zip(names, results)})
        elif not is_num:
            profile["fraction_true"] = pm_np.fraction_of_true(bin_labels, bin_counts)

        return profile

    def _profile_2d_histogram(self, name, hc):
        if hc.n_dim < 2:
            self.logger.warning(
                f"Histogram {name} has {hc.n_dim} dimensions (<2); cannot profile. Returning empty."
            )
            return []
        try:
            grid = get_2dgrid(hc.hist)
        except Exception as e:
            raise e

        # calc some basic 2d-histogram statistics
        sume = int(sum_entries(hc.hist))

        # calculate phik correlation
        try:
            phi_k = phik.phik_from_hist2d(observed=grid)
            # p, Z = significance.significance_from_hist2d(values=grid, significance_method='asymptotic')
        except ValueError:
            self.logger.debug(
                f"Not enough values in the 2d `{name}` time-split histogram to apply the phik test."
            )
            phi_k = np.nan

        return {"count": sume, "phik": phi_k}

    def _profile_hist(self, split, hist_name):
        if len(split) == 0:
            self.logger.error(f'Split histograms dict "{hist_name}" is empty. Return.')
            return []

        hist0 = split[0][self.hist_col]
        dimension = hist0.n_dim
        is_num = hist0.is_num

        # these are the profiled quantities we will monitor
        fields = []
        if dimension == 1:
            fields = list(self.general_stats_1d)
            fields += (
                [v for key in self.stats_functions.keys() for v in key.split(",")]
                if is_num
                else list(self.category_stats_1d)
            )
        elif dimension == 2:
            fields = list(self.general_stats_2d)

        # now loop over split-axis, e.g. time index, and profile each sub-hist x:y
        profile_list = []
        for hist_dict in split:
            index, hc = hist_dict[self.index_col], hist_dict[self.hist_col]

            profile = {self.index_col: index, self.hist_col: hc}

            if dimension == 1:
                profile.update(self._profile_1d_histogram(hist_name, hc))
            elif dimension == 2:
                profile.update(self._profile_2d_histogram(hist_name, hc))

            if sorted(profile.keys()) != sorted(
                fields + [self.index_col, self.hist_col]
            ):
                self.logger.error(
                    f'Could not extract full profile for sub-hist "{hist_name} {index}". Skipping.'
                )
            else:
                profile_list.append(profile)

        return profile_list

    def transform(self, datastore):
        self.logger.info(
            f'Profiling histograms "{self.read_key}" as "{self.store_key}"'
        )
        data = self.get_datastore_object(datastore, self.read_key, dtype=dict)
        profiled = dict()

        features = self.get_features(data.keys())

        for feature in features[:]:
            df = self.get_datastore_object(data, feature, dtype=pd.DataFrame)
            hc_split_list = df.reset_index().to_dict("records")

            self.logger.debug(f'Profiling histogram "{feature}".')
            profile_list = self._profile_hist(split=hc_split_list, hist_name=feature)
            if len(profile_list) > 0:
                profiled[feature] = pd.DataFrame(profile_list).set_index(
                    [self.index_col]
                )

        datastore[self.store_key] = profiled
        return datastore
