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


import histogrammar as hg
import numpy as np

from ..analysis.hist_numpy import assert_similar_hists
from ..base import Module


class HistStitcher(Module):
    """Module stitches histograms by date"""

    def __init__(
        self,
        mode="add",
        time_axis=None,
        time_bin_idx=None,
        read_key=None,
        delta_key=None,
        store_key=None,
    ):
        """Stitching histograms by first axis.

        :param str mode: options for histogram stitching: "add" or "replace". default is "add".
        :param str time_axis: name of the first axis, to stitch on.
        :param str time_bin_idx: value of delta dataset used for stitching,
            in case delta or first dataset is a batch without time_axis. Should be an ordered string or integer.
        :param str read_key: key of input histogram-dict to read from data store.
            (only required when calling transform(datastore) as module)
        :param str delta_key: key of delta histogram-dict to read from data store.
            (only required when calling transform(datastore) as module)
        :param str store_key: key of output data to store in data store
            (only required when calling transform(datastore) as module)
        """
        super().__init__()
        self.mode = mode
        self.time_axis = time_axis
        self.time_bin_idx = time_bin_idx
        self.read_key = read_key
        self.delta_key = delta_key
        self.store_key = store_key
        self.allowed_modes = ["add", "replace"]
        assert self.mode in self.allowed_modes

    def transform(self, datastore):
        # --- get input dict lists
        self.logger.info(
            f'Stitching histograms "{self.read_key}" and "{self.delta_key}" as "{self.store_key}"'
        )

        hists_basis = self.get_datastore_object(datastore, self.read_key, dtype=dict)
        hists_delta = self.get_datastore_object(datastore, self.delta_key, dtype=dict)

        stitched = self.stitch_histograms(self.mode, hists_basis, hists_delta)

        datastore[self.store_key] = stitched
        return datastore

    def stitch_histograms(
        self,
        mode=None,
        hists_basis=None,
        hists_delta=None,
        hists_list=None,
        time_axis="",
        time_bin_idx=None,
    ):
        """Stitching histograms by first axis.

        Histograms in hists_delta are added to those in hists_basis. Bins are summed or replaced, set this with 'mode'.
        'time_axis' specifies the name of the first axis. If the time_axis is not found, it is created, and
        histograms get inserted the time_bin_idx values.

        :param str mode: options for histogram stitching: "add" or "replace" bins. default is "add".
        :param dict hists_basis: input dict of basis histograms.
        :param dict hists_delta: delta dict of histograms to add to hists_basis.
        :param list hists_list: alternative for [hists_basis, hists_delta, etc]. can have multiple deltas.
            first item in list is taken as hists_basis (optional)
        :param str time_axis: time-axis used for stitching (optional).
        :param time_bin_idx: (list of) time-value(s) at which to insert hist-deltas into hist-basis.
        :return: dict with stitched histograms. If stitching is not possible, returns hists_basis.
        """
        # set stitching mode
        mode = (
            mode if isinstance(mode, str) and mode in self.allowed_modes else self.mode
        )
        time_axis = (
            time_axis
            if isinstance(time_axis, str) and len(time_axis) > 0
            else self.time_axis
        )
        time_bin_idx = (
            time_bin_idx
            if isinstance(time_bin_idx, (str, int, list, tuple))
            else self.time_bin_idx
        )
        hists_list = hists_list or []

        if time_bin_idx is not None:
            if isinstance(time_bin_idx, (str, int)):
                time_bin_idx = [time_bin_idx]
            if not isinstance(time_bin_idx, (list, tuple)):
                raise TypeError(
                    "time_bin_idx should be a (list of) ordered integer(s) or string(s)"
                )
            dts = [type(tv) for tv in time_bin_idx]
            if not dts.count(dts[0]) == len(dts):
                raise TypeError(f"time_bin_idxs have inconsistent datatypes: {dts}")

        # basic checks and conversions
        if isinstance(hists_basis, dict) and len(hists_basis) > 0:
            hists_list.insert(0, hists_basis)
        if isinstance(hists_delta, dict) and len(hists_delta) > 0:
            hists_list.append(hists_delta)
        elif isinstance(hists_delta, list):
            for hd in hists_delta:
                if isinstance(hd, dict) and len(hd) > 0:
                    hists_list.append(hd)
        if not isinstance(hists_list, list) or len(hists_list) == 0:
            raise TypeError(
                "hists_list should be a filled list of histogram dictionaries."
            )
        for hd in hists_list:
            if not isinstance(hd, dict) or len(hd) == 0:
                raise TypeError(
                    "hists_list should be a list of filled histogram dictionaries."
                )
        hists_basis = hists_list[0]
        hists_delta = hists_list[1:]

        # determine possible features, used for comparisons below
        if isinstance(time_axis, str) and len(time_axis) > 0:
            self.feature_begins_with = f"{time_axis}:"

        # Three possibilities
        # 1. if there are no basis hists starting with "time_axis:", assume that this the very first batch.
        # 2. if delta(s) do not start with "time_axis:", assume that this is a set of batches without time_axis
        # 3. deltas already have a time-axis. start stitching of histograms by using bins.update()

        features_basis = self.get_features(
            list(hists_basis.keys())
        )  # basis keys that start with time_axis
        if len(features_basis) > 0:
            # pruning of keys not starting with time_axis
            hists_list[0] = hists_basis = {
                k: h for k, h in hists_basis.items() if k in features_basis
            }

        # 1. if there are no basis hists starting with "time_axis:", assume that this the very first batch.
        if (
            len(features_basis) == 0
            and time_axis
            and len(hists_basis) > 0
            and time_axis
        ):
            if time_bin_idx is None:
                self.logger.info(
                    f'Inserting basis histograms in axis "{time_axis}" at bin index 0.'
                )
                time_bin_idx = [0]
            hists_basis_new = {}
            for k, hist in hists_basis.items():
                feature = f"{time_axis}:{k}"
                self.logger.debug(f'Now creating histogram "{feature}"')
                hists_basis_new[feature] = self._create_hist_with_time_axis(
                    hist, time_bin_idx[0]
                )
            # reset hists_basis
            features_basis = self.get_features(list(hists_basis_new.keys()))
            hists_list[0] = hists_basis = hists_basis_new
            time_bin_idx = time_bin_idx[1:]

        # -- is there a need to continue? There need to be overlapping hists.
        delta_keys = set()
        if len(hists_delta) > 0:
            delta_keys = set(hists_delta[0].keys())
            for hd in hists_delta[1:]:
                delta_keys &= set(hd.keys())
        if len(hists_delta) == 0 or len(delta_keys) == 0 or len(hists_basis) == 0:
            self.logger.debug(
                "No overlapping delta features. returning pruned hists_basis."
            )
            return hists_basis

        stitched = {}

        # 2. if delta(s) do not start with "time_axis:", assume that this is a set of batches without time_axis
        features_delta = self.get_features(
            list(delta_keys)
        )  # delta keys that start with time_axis
        if (
            len(features_basis) > 0
            and len(features_delta) == 0
            and len(delta_keys) > 0
            and time_axis
        ):
            if time_bin_idx is None or len(time_bin_idx) == 0:
                time_bin_idx = self._generate_time_bin_idx(
                    hists_basis, features_basis, time_axis, len(hists_delta)
                )
                if time_bin_idx is None:
                    raise ValueError(
                        "Request to insert delta hists but time_bin_idx not set. Please do."
                    )
                self.logger.info(
                    f'Inserting delta histograms in axis "{time_axis}" at bin indices {time_bin_idx}.'
                )
            if len(hists_delta) != len(time_bin_idx):
                raise ValueError(
                    "Not enough time_bin_idxs set to insert delta histograms."
                )
            for key in list(delta_keys):
                feature = f"{time_axis}:{key}"
                if feature not in features_basis:
                    continue
                self.logger.debug(f'Now inserting into histogram "{feature}"')
                hist_list = [hd[key] for hd in hists_delta]
                stitched[feature] = self._insert_hists(
                    hists_basis[feature], hist_list, time_bin_idx, mode
                )
            # add basis hists without any overlap
            for feature in features_basis:
                if feature not in stitched:
                    stitched[feature] = hists_basis[feature]
            return stitched

        # 3. deltas already have a time-axis. start stitching of histograms by using bins.update()
        overlapping_keys = set(hists_list[0].keys())
        for hd in hists_list[1:]:
            overlapping_keys &= set(hd.keys())
        features_overlap = self.get_features(
            list(overlapping_keys)
        )  # all overlapping keys that start with time_axis
        if len(features_overlap) == 0:
            # no overlap, then return basis histograms
            self.logger.warning(
                "No overlapping basis-delta features. returning pruned hists_basis."
            )
            return hists_basis
        for feature in features_overlap:
            self.logger.debug(f'Now stitching histograms "{feature}"')
            hist_list = [hd[feature] for hd in hists_list]
            stitched[feature] = self._stitch_by_update(mode, hist_list)
        # add basis hists without any overlap
        for feature in features_basis:
            if feature not in stitched:
                stitched[feature] = hists_basis[feature]
        return stitched

    def _find_max_time_bin_index(self, hists_basis, features_basis, time_axis):
        """Find the maximum time-bin index in dict of basis histograms

        :param hists_basis: dict of basis histograms
        :param features_basis: list of features to look at
        :param time_axis: name of time-axis
        :return: maximum time-bin index found or None
        """
        # basic checks
        assert isinstance(time_axis, str) and len(time_axis) > 0
        assert len(features_basis) > 0
        assert all([f.startswith(time_axis) for f in features_basis])

        hist_list = list(hists_basis.values())

        all_sparse = all([isinstance(h, hg.SparselyBin) for h in hist_list])
        all_cat = (
            all([isinstance(h, hg.Categorize) for h in hist_list])
            if not all_sparse
            else False
        )

        max_time_bin_idx = None
        if all_sparse or all_cat:
            max_time_bin_idx = max(
                max(h.bins.keys()) for h in hist_list if len(h.bins) > 0
            )
        return max_time_bin_idx

    def _generate_time_bin_idx(self, hists_basis, features_basis, time_axis, n):
        """Generate n consecutive time-bin indices beyond max existing time-bin index

        :param hists_basis: dict of basis histograms
        :param features_basis: list of features to look at
        :param time_axis: name of time-axis
        :param n: number of time-bin indices to generate
        :return: maximum time-bin index found or None
        """
        assert n > 0
        max_time_bin_idx = self._find_max_time_bin_index(
            hists_basis, features_basis, time_axis
        )
        if max_time_bin_idx is not None:
            self.logger.info(f"Maximum time bin index found: {max_time_bin_idx}")
        time_bin_idx = None
        if isinstance(max_time_bin_idx, (int, np.int64)):
            start = max_time_bin_idx + 1
            stop = start + n
            time_bin_idx = np.arange(start, stop)
        return time_bin_idx

    def _insert_hists(self, hbasis, hdelta_list, time_bin_idx, mode):
        """Add delta hist to hist_basis at time-value

        :param hbasis: basis histogram with time-axis
        :param hdelta_list: delta histogram without time-axis
        :param time_bin_idx: list of time-value at which to insert hdeltas into hbasis
        :param str mode: options for histogram stitching: "add" or "replace". default is "add".
        :return: stitched hbasis histogram
        """
        # basic checks on time-values
        if len(hdelta_list) != len(time_bin_idx) or len(hdelta_list) == 0:
            raise ValueError(
                "hdelta_list and time_bin_idx should be filled and same size."
            )
        dts = [type(tv) for tv in time_bin_idx]
        if not dts.count(dts[0]) == len(dts):
            raise TypeError(f"time_bin_idxs have inconsistent datatypes: {dts}")
        if not isinstance(time_bin_idx[0], (str, int, np.integer)):
            raise TypeError("time_bin_idxs should be an (ordered) string or integer.")

        # consistency checks on histogram definitions
        if not hasattr(hbasis, "bins"):
            raise ValueError(
                "basis histogram does not have bins attribute. cannot insert."
            )
        if len(hbasis.bins) > 0:
            hbk0 = list(hbasis.bins.values())[0]
            assert_similar_hists([hbk0] + hdelta_list)
        else:
            assert_similar_hists(hdelta_list)

        # check consistency of hbasis and time-value type
        if isinstance(time_bin_idx[0], str):
            if not isinstance(hbasis, hg.Categorize):
                raise TypeError("hbasis does not accept string time-values.")
        elif isinstance(time_bin_idx[0], (int, np.int64)):
            if not isinstance(hbasis, hg.SparselyBin):
                raise TypeError("hbasis does not accept integer time-values.")

        # stitch all the hdeltas into hbasis
        hsum = hbasis.copy()

        for tv, hdelta in zip(time_bin_idx, hdelta_list):
            if tv in hsum.bins:
                # replace or sum?
                hbt = hsum.bins[tv]
                hbt_entries = hbt.entries
                if mode == "replace":
                    hsum.bins[tv] = hdelta
                    hsum.entries += hdelta.entries - hbt_entries
                else:
                    hsum.bins[tv] += hdelta
                    hsum.entries += hdelta.entries
            else:
                # insert at time_bin_idx
                hsum.bins[tv] = hdelta
                hsum.entries += hdelta.entries

        return hsum

    def _create_hist_with_time_axis(self, hist, time_bin_idx):
        """Create histogram with time-axis and place hist into it at time-value

        :param hist: input histogram to insert into histogram with time-axis
        :param str time_bin_idx: time-value at which to insert histogram
        :return: histogram with time-axis
        """
        # basic checks
        if time_bin_idx is None or not isinstance(time_bin_idx, (str, int)):
            raise TypeError(
                "time_bin_idx not set. should be an (ordered) string or integer."
            )

        ht = (
            hg.SparselyBin(binWidth=1.0, origin=0.0, quantity=lambda x: x)
            if isinstance(time_bin_idx, int)
            else hg.Categorize(quantity=lambda x: x)
        )  # noqa
        ht.bins[time_bin_idx] = hist
        ht.entries = hist.entries
        return ht

    def _stitch_by_update(self, mode, hist_list):
        """Get sum of histograms using h1.bins.update(h2.bins), from first to last hist

        Get sum of list of histograms by using bins.update(), where the update is applied
        by iterating over the histogram list, from the first to the last histogram.
        Meaning the next bins can overwrite the previous bins.
        This is used for merging (bins of) histograms that can be topped up over time.

        :param str mode: options for histogram stitching: "add" or "replace". default is "add".
        :param list hist_list: list of input histogrammar histograms
        :return: list of consistent 1d numpy arrays with bin_entries for list of input histograms
        """
        # --- basic checks
        if len(hist_list) == 0:
            raise ValueError("Input histogram list has zero length.")
        assert_similar_hists(hist_list)

        # --- loop over all histograms and update zeroed-original consecutively
        hsum = hist_list[0].zero()

        if mode == "replace":
            # check consistency of histogram bins attributes
            is_bins = all([hasattr(hist, "bins") for hist in hist_list])
            if not is_bins:
                raise TypeError("Not all input histograms have bins attribute.")
            # update bins consecutively for each time-delta.
            for hist in hist_list:
                hsum.bins.update(hist.bins)
            hsum.entries = sum(b.entries for b in hsum.bins.values())
        else:
            for hist in hist_list:
                hsum += hist
        return hsum


def stitch_histograms(
    mode=None,
    hists_basis=None,
    hists_delta=None,
    hists_list=None,
    time_axis=None,
    time_bin_idx=None,
):
    """Stitching histograms by first axis.

    Histograms in hists_delta are added to those in hists_basis. Bins are summed or replaced, set this with 'mode'.
    'time_axis' specifies the name of the first axis. If the time_axis is not found, it is created, and
    histograms get inserted the time_bin_idx values.

    :param str mode: options for histogram stitching: "add" or "replace" bins. default is "add".
    :param dict hists_basis: input dict of basis histograms.
    :param dict hists_delta: delta dict of histograms to add to hists_basis.
    :param list hists_list: alternative for [hists_basis, hists_delta, etc]. can have multiple deltas.
        first item in list is taken as hists_basis (optional)
    :param str time_axis: time-axis used for stitching (optional).
    :param time_bin_idx: (list of) time-value(s) at which to insert hist-deltas into hist-basis.
    :return: dict with stitched histograms. If stitching is not possible, returns hists_basis.
    """
    stitcher = HistStitcher()
    return stitcher.stitch_histograms(
        mode=mode,
        hists_basis=hists_basis,
        hists_delta=hists_delta,
        hists_list=hists_list,
        time_axis=time_axis,
        time_bin_idx=time_bin_idx,
    )
