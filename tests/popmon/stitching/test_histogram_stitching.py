#!/usr/bin/env python3

import numpy as np
import pytest

from popmon.hist.filling import get_bin_specs, make_histograms
from popmon.stitching import stitch_histograms


def test_histogram_stitching():
    features1 = sorted(["date:isActive", "date:eyeColor", "date:latitude"])
    features2 = sorted(["isActive", "eyeColor", "latitude", "age"])

    hists1 = make_histograms(pytest.test_df, features=features1)
    bs = get_bin_specs(hists1, skip_first_axis=True)
    hists2 = make_histograms(pytest.test_df, features=features2, bin_specs=bs)

    # add 'date' axis to hists2 (ts=50) and stitch with hists1
    hists3 = stitch_histograms(
        hists_basis=hists1, hists_delta=hists2, time_axis="date", time_bin_idx=[50]
    )
    np.testing.assert_array_equal(sorted(hists3.keys()), features1)
    assert hists3["date:isActive"].entries == 800
    assert hists3["date:isActive"].bins[50].entries == 400

    # add 'date' axis to hists2 (ts=50) and hists2 (ts=51) and stitch
    hists3 = stitch_histograms(
        hists_basis=hists2, hists_delta=hists2, time_axis="date", time_bin_idx=[50, 51]
    )
    np.testing.assert_array_equal(
        sorted(hists3.keys()), sorted(features1 + ["date:age"])
    )
    assert hists3["date:age"].entries == 800
    assert hists3["date:age"].bins[50].entries == 400
    assert hists3["date:age"].bins[51].entries == 400

    # add 'date' axis to hists2 and hists2 and stitch at auto-bins 0, 1
    hists3 = stitch_histograms(hists_basis=hists2, hists_delta=hists2, time_axis="date")
    np.testing.assert_array_equal(
        sorted(hists3.keys()), sorted(features1 + ["date:age"])
    )
    assert hists3["date:age"].entries == 800
    assert 0 in hists3["date:age"].bins
    assert 1 in hists3["date:age"].bins

    # add 'date' axis to hists2 and hists2 and stitch at bin 50 and auto-bins 51
    hists3 = stitch_histograms(
        hists_basis=hists2, hists_delta=hists2, time_axis="date", time_bin_idx=50
    )
    np.testing.assert_array_equal(
        sorted(hists3.keys()), sorted(features1 + ["date:age"])
    )
    assert hists3["date:age"].entries == 800
    assert 51 in hists3["date:age"].bins

    # no stitching b/c no overlap, returns hists1
    hists3 = stitch_histograms(hists_basis=hists1, hists_delta=hists2)
    np.testing.assert_array_equal(sorted(hists3.keys()), features1)
    assert hists3["date:latitude"].entries == 400
    assert 50 not in hists3["date:latitude"].bins

    # add hists1 to hists1
    hists3 = stitch_histograms(hists_basis=hists1, hists_delta=hists1)
    np.testing.assert_array_equal(sorted(hists3.keys()), features1)
    assert hists3["date:latitude"].entries == 800
    assert 50 not in hists3["date:latitude"].bins

    # add hists2 to hists2
    hists3 = stitch_histograms(hists_basis=hists2, hists_delta=hists2)
    np.testing.assert_array_equal(sorted(hists3.keys()), features2)
    assert hists3["age"].entries == 800

    # add hists1 to hists1
    hists3 = stitch_histograms(hists_basis=hists1, hists_delta=hists1, mode="replace")
    np.testing.assert_array_equal(sorted(hists3.keys()), features1)
    assert hists3["date:latitude"].entries == 400

    # add 'date' axis to hists2 (ts=50) and stitch with hists1
    hists3 = stitch_histograms(
        hists_basis=hists1,
        hists_delta=hists2,
        time_axis="date",
        time_bin_idx=[1],
        mode="replace",
    )
    np.testing.assert_array_equal(sorted(hists3.keys()), features1)
    assert hists3["date:isActive"].bins[1].entries == 400
    assert hists3["date:isActive"].entries == 777
