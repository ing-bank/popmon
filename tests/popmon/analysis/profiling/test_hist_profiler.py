import histogrammar as hg
import numpy as np
import pandas as pd

from popmon.analysis.profiling.hist_profiler import HistProfiler
from popmon.hist.hist_utils import get_bin_centers


def test_profile_hist1d():
    num_bins = 1000
    num_entries = 10000
    hist_name = "histogram"
    split_len = 10
    split = []

    np.random.seed(0)
    for _ in range(split_len):
        h = hg.Bin(num_bins, 0, 1, lambda x: x)
        h.fill.numpy(np.random.uniform(0, 1, num_entries))
        split.append({"date": pd.Timestamp("2019 - 1 - 1"), hist_name: h})

    hp = HistProfiler(
        read_key="dummy_input",
        store_key="dummy_output",
        hist_col=hist_name,
        index_col="date",
    )

    profiles = hp._profile_hist(split, hist_name="feature")
    assert len(profiles) == split_len
    assert "p95" in profiles[0]
    assert profiles[1]["max"] == np.max(get_bin_centers(split[1][hist_name])[0])
    assert len(profiles[0][hist_name].bin_entries()) == num_bins
