import pandas as pd
import numpy as np
import histogrammar as hg
from popmon.hist.histogram import HistogramContainer
from popmon.analysis.profiling.hist_profiler import HistProfiler


def test_profile_hist1d():
    num_bins = 1000
    num_entries = 10000
    hist_name = "histogram"
    split_len = 10
    split = []

    np.random.seed(0)
    for i in range(split_len):
        h = hg.Bin(num_bins, 0, 1, lambda x: x)
        h.fill.numpy(np.random.uniform(0, 1, num_entries))
        split.append({"date": pd.Timestamp("2019 - 1 - 1"), hist_name: HistogramContainer(h)})

    hp = HistProfiler(read_key="dummy_input", store_key="dummy_output", hist_col=hist_name, index_col="date")

    profiles = hp._profile_hist(split, hist_name="feature")

    assert len(profiles) == split_len
    assert "p95" in profiles[0]
    assert profiles[1]["max"] == np.max(split[1][hist_name].get_bin_centers()[0])
    assert len(profiles[0][hist_name].hist.bin_entries()) == num_bins
