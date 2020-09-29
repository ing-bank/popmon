import numpy as np
import pandas as pd
import pytest

from popmon.analysis.hist_numpy import (
    assert_similar_hists,
    check_similar_hists,
    get_2dgrid,
    get_consistent_numpy_1dhists,
    get_consistent_numpy_2dgrids,
    get_consistent_numpy_entries,
    get_contentType,
    prepare_2dgrid,
    set_2dgrid,
)
from popmon.hist.histogram import HistogramContainer
from popmon.hist.patched_histogrammer import histogrammar as hg


def to_ns(x):
    """convert timestamp to nanosec since 1970-1-1"""
    return pd.to_datetime(x).value


def unit(x):
    """unit return function"""
    return x


def get_test_histograms1():
    """Get set 1 of test histograms"""
    # dummy dataset with mixed types
    # convert timestamp (col D) to nanosec since 1970-1-1
    df = pd.util.testing.makeMixedDataFrame()
    df["date"] = df["D"].apply(to_ns)
    df["boolT"] = True
    df["boolF"] = False

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist1 = hg.Categorize(unit("C"))
    hist2 = hg.Bin(5, 0, 5, unit("A"), value=hist1)
    hist3 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist2,
    )
    # fill them
    hist1.fill.numpy(df)
    hist2.fill.numpy(df)
    hist3.fill.numpy(df)

    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)
    hc3 = HistogramContainer(hist3)

    return df, hc1, hc2, hc3


def get_test_histograms2():
    """Get set 2 of test histograms"""
    # dummy dataset with mixed types
    # convert timestamp (col D) to nanosec since 1970-1-1
    df = pd.util.testing.makeMixedDataFrame()

    # building 1d-, 2d-histogram (iteratively)
    hist1 = hg.Categorize(unit("C"))
    hist2 = hg.Bin(5, 0, 5, unit("A"), value=hist1)
    hist3 = hg.Bin(5, 0, 5, unit("A"))
    hist4 = hg.Categorize(unit("C"), value=hist3)

    # fill them
    hist1.fill.numpy(df)
    hist2.fill.numpy(df)
    hist3.fill.numpy(df)
    hist4.fill.numpy(df)

    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)
    hc3 = HistogramContainer(hist3)
    hc4 = HistogramContainer(hist4)

    return df, hc1, hc2, hc3, hc4


def test_histogram():
    """Test the dummy histogram we're working with below"""
    df, hc1, hc2, hc3 = get_test_histograms1()
    hist1 = hc1.hist
    hist2 = hc2.hist
    hist3 = hc3.hist

    assert hist1.entries == 5
    assert hist1.n_dim == 1
    assert hist1.size == 5

    assert hist2.entries == 5
    assert hist2.n_dim == 2
    assert hist2.num == 5

    assert hist3.entries == 5
    assert hist3.n_dim == 3
    assert hist3.num == 7


def test_get_contentType():
    """Test getting type of a histogram"""
    df, hc1, hc2, hc3 = get_test_histograms1()
    hist1 = hc1.hist
    hist2 = hc2.hist
    hist3 = hc3.hist

    assert get_contentType(hist1) == "Categorize"
    assert get_contentType(hist2) == "Bin"
    assert get_contentType(hist3) == "SparselyBin"


@pytest.mark.filterwarnings("ignore:Input histogram only has")
def test_prepare_2dgrid():
    """Test preparation of grid for extraction of number of entries for 2d hists"""
    df, hc1, hc2, hc3 = get_test_histograms1()

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist1 = hg.Categorize(unit("C"))
    hist2 = hg.Bin(5, 0, 5, unit("A"), value=hist1)
    hist3 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist2,
    )
    # fill them
    hist1.fill.numpy(df)
    hist2.fill.numpy(df)
    hist3.fill.numpy(df)

    xkeys1, ykeys1 = prepare_2dgrid(hist1)
    xkeys2, ykeys2 = prepare_2dgrid(hist2)
    xkeys3, ykeys3 = prepare_2dgrid(hist3)

    np.testing.assert_array_equal(xkeys1, [])
    np.testing.assert_array_equal(ykeys1, [])
    np.testing.assert_array_equal(xkeys2, [0, 1, 2, 3, 4])
    np.testing.assert_array_equal(ykeys2, ["foo1", "foo2", "foo3", "foo4", "foo5"])
    np.testing.assert_array_equal(xkeys3, [0, 1, 4, 5, 6])
    np.testing.assert_array_equal(ykeys3, [0, 1, 2, 3, 4])


@pytest.mark.filterwarnings("ignore:Input histogram only has")
def test_set_2dgrid():
    """Test setting the grid for extraction of number of entries for 2d hists"""
    df, hc1, hc2, hc3 = get_test_histograms1()
    hist1 = hc1.hist
    hist2 = hc2.hist
    hist3 = hc3.hist

    xkeys1, ykeys1 = prepare_2dgrid(hist1)
    xkeys2, ykeys2 = prepare_2dgrid(hist2)
    xkeys3, ykeys3 = prepare_2dgrid(hist3)

    grid1 = set_2dgrid(hist1, xkeys1, ykeys1)
    grid2 = set_2dgrid(hist2, xkeys2, ykeys2)
    grid3 = set_2dgrid(hist3, xkeys3, ykeys3)

    grid_comp = np.asarray(
        [
            [1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0],
        ]
    )

    assert (grid1 == np.zeros((0, 0))).all()
    assert (grid2 == grid_comp).all()
    assert (grid3 == grid_comp).all()


@pytest.mark.filterwarnings("ignore:Input histogram only has")
def test_get_2dgrid():
    """Test extraction of number of entries for 2d hists"""
    df, hc1, hc2, hc3 = get_test_histograms1()
    hist1 = hc1.hist
    hist2 = hc2.hist
    hist3 = hc3.hist

    grid1 = get_2dgrid(hist1)
    grid2 = get_2dgrid(hist2)
    grid3 = get_2dgrid(hist3)

    grid_comp = np.asarray(
        [
            [1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0],
        ]
    )

    assert (grid1 == np.zeros((0, 0))).all()
    assert (grid2 == grid_comp).all()
    assert (grid3 == grid_comp).all()


def test_get_consistent_numpy_2dgrids():
    """Test extraction of number of entries for 2d hists

    When first making bin_edges of input histograms consistent to each other.
    """
    df1 = pd.DataFrame(
        {
            "A": [0, 1, 2, 3, 4, 3, 2, 1, 1, 1],
            "C": ["f1", "f3", "f4", "f3", "f4", "f2", "f2", "f1", "f3", "f4"],
        }
    )
    df2 = pd.DataFrame(
        {
            "A": [2, 3, 4, 5, 7, 4, 6, 5, 7, 8],
            "C": ["f7", "f3", "f5", "f8", "f9", "f2", "f3", "f6", "f7", "f7"],
        }
    )

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist0 = hg.Categorize(unit("C"))
    hist1 = hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"), value=hist0)
    hist2 = hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"), value=hist0)

    # fill them
    hist0.fill.numpy(df1)
    hist1.fill.numpy(df1)
    hist2.fill.numpy(df2)

    hc0 = HistogramContainer(hist0)
    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)

    args = [""]
    try:
        get_consistent_numpy_2dgrids([hc0, hc0])
    except ValueError as e:
        args = e.args

    grid2d_list = get_consistent_numpy_2dgrids([hc1, hc2])

    g1 = np.asarray(
        [
            [1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ]
    )
    g2 = np.asarray(
        [
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
        ]
    )
    grid2d_comp = [g1, g2]

    # MB 20190828: not sure if this is the right way to test for exceptions.
    assert (
        args[0] == "Input histogram only has 1 dimensions (<2). Cannot compute 2d-grid."
    )

    for i in range(2):
        assert (grid2d_list[i] == grid2d_comp[i]).all()


def test_get_consistent_numpy_1dhists():
    """Test extraction of number of entries and bin-edges/labels

    When first making bin_edges/bin-labels of input histograms consistent to each other.
    """
    df1 = pd.DataFrame({"A": [0, 1, 2, 3, 4, 3, 2, 1, 1, 1]})
    df2 = pd.DataFrame({"A": [2, 3, 4, 5, 7, 4, 6, 5, 7, 8]})

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist1 = hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"))
    hist2 = hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"))

    # fill them
    hist1.fill.numpy(df1)
    hist2.fill.numpy(df2)

    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)

    nphist1, nphist2 = get_consistent_numpy_1dhists([hc1, hc2], get_bin_labels=False)
    nphist_list, centers = get_consistent_numpy_1dhists([hc1, hc2], get_bin_labels=True)

    entries1 = [1.0, 4.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0, 0.0]
    entries2 = [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0]
    bin_edges = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    bin_centers = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]

    np.testing.assert_array_equal(nphist1[0], entries1)
    np.testing.assert_array_equal(nphist1[1], bin_edges)
    np.testing.assert_array_equal(nphist2[0], entries2)
    np.testing.assert_array_equal(nphist2[1], bin_edges)

    np.testing.assert_array_equal(nphist_list[0][0], entries1)
    np.testing.assert_array_equal(nphist_list[0][1], bin_edges)
    np.testing.assert_array_equal(nphist_list[1][0], entries2)
    np.testing.assert_array_equal(nphist_list[1][1], bin_edges)
    np.testing.assert_array_equal(centers, bin_centers)


def test_get_consistent_numpy_entries():
    """Test extraction of number of entries

    When first making bin_edges of input histograms consistent to each other.
    """
    df1 = pd.DataFrame(
        {
            "A": [0, 1, 2, 3, 4, 3, 2, 1, 1, 1],
            "C": ["f1", "f3", "f4", "f3", "f4", "f2", "f2", "f1", "f3", "f4"],
        }
    )
    df2 = pd.DataFrame(
        {
            "A": [2, 3, 4, 5, 7, 4, 6, 5, 7, 8],
            "C": ["f7", "f3", "f5", "f8", "f9", "f2", "f3", "f6", "f7", "f7"],
        }
    )

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist0 = HistogramContainer(hg.Categorize(unit("C")))
    hist1 = HistogramContainer(hg.Categorize(unit("C")))
    hist2 = HistogramContainer(
        hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"))
    )
    hist3 = HistogramContainer(
        hg.SparselyBin(origin=0.0, binWidth=1.0, quantity=unit("A"))
    )

    # fill them
    for hist, df in zip([hist0, hist1, hist2, hist3], [df1, df2, df1, df2]):
        hist.hist.fill.numpy(df)

    e0, e1 = get_consistent_numpy_entries([hist0, hist1], get_bin_labels=False)
    _, labels01 = get_consistent_numpy_entries([hist0, hist1], get_bin_labels=True)

    e2, e3 = get_consistent_numpy_entries([hist2, hist3], get_bin_labels=False)
    _, centers23 = get_consistent_numpy_entries([hist2, hist3], get_bin_labels=True)

    entries0 = [2.0, 2.0, 3.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    entries1 = [0.0, 1.0, 2.0, 0.0, 1.0, 1.0, 3.0, 1.0, 1.0]
    labels = ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"]

    entries2 = [1.0, 4.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0, 0.0]
    entries3 = [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0]
    centers = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]

    np.testing.assert_array_equal(e0, entries0)
    np.testing.assert_array_equal(e1, entries1)
    np.testing.assert_array_equal(labels01, labels)

    np.testing.assert_array_equal(e2, entries2)
    np.testing.assert_array_equal(e3, entries3)
    np.testing.assert_array_equal(centers23, centers)


@pytest.mark.filterwarnings("ignore:Input histograms have inconsistent")
def test_check_similar_hists():
    """Test similarity of list of histograms

    Check similarity of: type, n-dim, sub-hists, specific type attributes
    """
    # dummy dataset with mixed types
    # convert timestamp (col D) to nanosec since 1970-1-1
    df = pd.util.testing.makeMixedDataFrame()
    df["date"] = df["D"].apply(to_ns)

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist0 = hg.Bin(5, 0, 5, unit("A"))
    hist1 = hg.Categorize(unit("C"))
    hist2 = hg.Bin(5, 0, 5, unit("A"), value=hist1)
    hist3 = hg.Categorize(unit("C"), value=hist0)
    hist4 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist2,
    )
    hist5 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist3,
    )
    # fill them
    for hist in [hist0, hist1, hist2, hist3, hist4, hist5]:
        hist.fill.numpy(df)

    hc0 = HistogramContainer(hist0)
    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)
    hc3 = HistogramContainer(hist3)
    hc4 = HistogramContainer(hist4)
    hc5 = HistogramContainer(hist5)

    for hc in [hc0, hc1, hc2, hc3, hc4, hc5]:
        assert check_similar_hists([hc, hc])

    assert not check_similar_hists([hc0, hc1])
    assert not check_similar_hists([hc2, hc3])
    assert not check_similar_hists([hc4, hc5])


@pytest.mark.filterwarnings("ignore:Input histograms have inconsistent")
def test_assert_similar_hists():
    """Test assert on similarity of list of histograms

    Check similarity of: type, n-dim, sub-hists, specific type attributes
    """
    # dummy dataset with mixed types
    # convert timestamp (col D) to nanosec since 1970-1-1
    df = pd.util.testing.makeMixedDataFrame()
    df["date"] = df["D"].apply(to_ns)

    # building 1d-, 2d-, and 3d-histogram (iteratively)
    hist0 = hg.Bin(5, 0, 5, unit("A"))
    hist1 = hg.Categorize(unit("C"))
    hist2 = hg.Bin(5, 0, 5, unit("A"), value=hist1)
    hist3 = hg.Categorize(unit("C"), value=hist0)

    hist4 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist2,
    )
    hist5 = hg.SparselyBin(
        origin=pd.Timestamp("2009-01-01").value,
        binWidth=pd.Timedelta(days=1).value,
        quantity=unit("date"),
        value=hist3,
    )
    # fill them
    for hist in [hist0, hist1, hist2, hist3, hist4, hist5]:
        hist.fill.numpy(df)

    hc0 = HistogramContainer(hist0)
    hc1 = HistogramContainer(hist1)
    hc2 = HistogramContainer(hist2)
    hc3 = HistogramContainer(hist3)
    hc4 = HistogramContainer(hist4)
    hc5 = HistogramContainer(hist5)

    for hc in [hc0, hc1, hc2, hc3, hc4, hc5]:
        assert check_similar_hists([hc, hc])

    args01 = [""]
    args23 = [""]
    args45 = [""]

    try:
        assert_similar_hists([hc0, hc1])
    except ValueError as e:
        args01 = e.args

    try:
        assert_similar_hists([hc2, hc3])
    except ValueError as e:
        args23 = e.args

    try:
        assert_similar_hists([hc4, hc5])
    except ValueError as e:
        args45 = e.args

    assert args01[0] == "Input histograms are not all similar."
    assert args23[0] == "Input histograms are not all similar."
    assert args45[0] == "Input histograms are not all similar."


def test_datatype():
    """Test datatypes assigned to histograms"""
    df, hc1, hc2, hc3 = get_test_histograms1()
    hist1 = hc1.hist
    hist2 = hc2.hist
    hist3 = hc3.hist

    assert hist1.datatype == str
    np.testing.assert_array_equal(hist2.datatype, [np.float64, str])
    np.testing.assert_array_equal(hist3.datatype, [np.datetime64, np.float64, str])
