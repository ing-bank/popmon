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

from popmon.analysis.hist_numpy import get_2dgrid
from popmon.base.registry import Registry
from popmon.hist.hist_utils import sum_entries
from popmon.stats import numpy as pm_np

Profiles = Registry()


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
def profile_quantiles(x, w, _):
    return tuple(
        pm_np.quantile(
            x, q=[0.0, 1.0, 0.01, 0.05, 0.16, 0.50, 0.84, 0.95, 0.99], weights=w
        )
    )


@Profiles.register(key="mean", description="Mean value", dim=1, htype="num")
def profile_mean(x, w, _):
    return pm_np.mean(x, w)


@Profiles.register(key="std", description="Standard deviation", dim=1, htype="num")
def profile_std(x, w, _):
    return pm_np.std(x, w)


@Profiles.register(
    key="nan", description="Number of missing entries (NaN)", dim=1, htype=None
)
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
    htype=None,
)
def profile_overflow(hist):
    if hasattr(hist, "overflow"):
        return hist.overflow.entries
    return 0


@Profiles.register(
    key="underflow",
    description="Number of values smaller than the minimum bin-edge of the histogram.",
    dim=1,
    htype=None,
)
def profile_underflow(hist):
    if hasattr(hist, "underflow"):
        return hist.underflow.entries
    return 0


@Profiles.register(
    key="phik",
    description="phi-k correlation between the two variables of the histogram",
    dim=2,
    htype=None,
)
def profile_phik(hist):
    from phik import phik

    # calculate phik correlation
    grid = get_2dgrid(hist)

    try:
        phi_k = phik.phik_from_hist2d(observed=grid)
    except ValueError:
        # self.logger.debug(
        #     f"Not enough values in the 2d `{name}` time-split histogram to apply the phik test."
        # )
        phi_k = np.nan
    return phi_k


@Profiles.register(
    key="count", description="Number of entries (non-NaN and NaN)", dim=-1, htype=None
)
def profile_count(hist):
    return int(sum_entries(hist))


@Profiles.register(
    key="entropy",
    description="Entropy in nats",
    dim=-1,
    htype=None,
)
def profile_entropy(hist):
    h = hist.bin_entries()
    h = h / h.sum()
    return -(h * np.ma.log(h)).sum()


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
    key="fraction_of_true",
    description="Compute fraction of 'true' (as in boolean) labels",
    dim=1,
    htype="cat",
)
def profile_fraction_of_true(bin_labels, bin_counts):
    """Compute fraction of 'true' labels

    :param bin_labels: Array containing numbers whose mean is desired. If `a` is not an
        array, a conversion is attempted.
    :param bin_entries: Array containing weights for the elements of `a`. If `weights` is not an
        array, a conversion is attempted.
    :return: fraction of 'true' labels
    """
    bin_labels = np.array(bin_labels)
    bin_entries = np.array(bin_counts)
    assert len(bin_labels) == len(bin_entries)

    def replace(bl):
        if bl in {"True", "true"}:
            return True
        elif bl in {"False", "false"}:
            return False
        return np.nan

    # basic checks: dealing with boolean labels
    # also accept strings of 'True' and 'False'
    if len(bin_labels) == 0 or len(bin_labels) > 4 or np.sum(bin_entries) == 0:
        return np.nan
    if not np.all([isinstance(bl, (bool, np.bool_)) for bl in bin_labels]):
        if not np.all([isinstance(bl, (str, np.str_, np.bytes_)) for bl in bin_labels]):
            return np.nan
        # all strings from hereon
        n_true = (bin_labels == "True").sum() + (bin_labels == "true").sum()
        n_false = (bin_labels == "False").sum() + (bin_labels == "false").sum()
        n_nan = (
            (bin_labels == "NaN").sum()
            + (bin_labels == "nan").sum()
            + (bin_labels == "None").sum()
            + (bin_labels == "none").sum()
            + (bin_labels == "Null").sum()
            + (bin_labels == "null").sum()
        )
        if n_true + n_false + n_nan != len(bin_labels):
            return np.nan
        # convert string to boolean
        bin_labels = np.array([replace(bl) for bl in bin_labels])

    sum_true = np.sum([be for bl, be in zip(bin_labels, bin_entries) if bl])
    sum_false = np.sum([be for bl, be in zip(bin_labels, bin_entries) if not bl])
    sum_entries = sum_true + sum_false
    if sum_entries == 0:
        # all nans scenario
        return np.nan
    # exclude nans from fraction
    return (1.0 * sum_true) / sum_entries


@Profiles.register(
    key="most_probable_value", description="Most probable value", dim=1, htype="all"
)
def profile_most_probable_value(bin_labels, bin_counts):
    return bin_labels[np.argmax(bin_counts)]
