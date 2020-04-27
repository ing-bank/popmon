import numpy as np
import pandas as pd
from popmon.hist.patched_histogrammer import histogrammar as hg
from popmon.hist.histogram import HistogramContainer, sum_entries, sum_over_x, project_on_x, \
    project_split2dhist_on_axis


def get_test_data():
    df = pd.util.testing.makeMixedDataFrame()
    df['date'] = df['D'].apply(lambda x: pd.to_datetime(x).value)
    return df


def unit(x):
    return x


def get_histograms():
    df = get_test_data()

    hist1 = hg.Categorize(unit('C'))
    hist2 = hg.Bin(5, 0, 5, unit('A'), value=hist1)
    hist3 = hg.SparselyBin(origin=pd.Timestamp('2009-01-01').value, binWidth=pd.Timedelta(days=1).value,
                           quantity=unit('date'), value=hist2)

    for hist in [hist1, hist2, hist3]:
        hist.fill.numpy(df)

    return hist1, hist2, hist3


def test_histogrammar():
    hist1, hist2, hist3 = get_histograms()

    assert hist1.entries == 5
    assert hist1.n_dim == 1
    assert hist1.size == 5

    assert hist2.entries == 5
    assert hist2.n_dim == 2
    assert hist2.num == 5

    assert hist3.entries == 5
    assert hist3.n_dim == 3
    assert hist3.num == 7


def test_histogram_attributes():
    hist1, hist2, hist3 = get_histograms()

    hist_obj1 = HistogramContainer(hist1)
    hist_obj2 = HistogramContainer(hist2)
    hist_obj3 = HistogramContainer(hist3)

    assert hist_obj1.is_num is False
    assert hist_obj1.is_ts is False
    assert hist_obj2.is_num is True
    assert hist_obj2.is_ts is False
    assert hist_obj3.is_num is True
    assert hist_obj3.is_ts is True


def test_sparse_bin_centers_x():
    hist1, hist2, hist3 = get_histograms()

    hist_obj3 = HistogramContainer(hist3)
    centers3, values3 = hist_obj3.sparse_bin_centers_x()

    np.testing.assert_array_equal(centers3, [1.2308112e+18, 1.2308976e+18, 1.2311568e+18, 1.2312432e+18, 1.2313296e+18])


def test_split_hist_along_first_dimension():
    hist1, hist2, hist3 = get_histograms()
    hist_obj1 = HistogramContainer(hist1)
    hist_obj2 = HistogramContainer(hist2)
    hist_obj3 = HistogramContainer(hist3)

    split3a = hist_obj3.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=True)
    split3b = hist_obj3.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=False)
    split3c = hist_obj3.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=False, convert_time_index=True)

    keys3a = list(split3a.keys())
    keys3b = list(split3b.keys())
    keys3c = list(split3c.keys())

    check3a = [pd.Timestamp('2009-01-01 12:00:00'),
               pd.Timestamp('2009-01-02 12:00:00'), pd.Timestamp('2009-01-05 12:00:00'),
               pd.Timestamp('2009-01-06 12:00:00'), pd.Timestamp('2009-01-07 12:00:00')]
    check3b = [1.2308112e+18, 1.2308976e+18, 1.2311568e+18, 1.2312432e+18, 1.2313296e+18]
    check3c = ['y[x=2009-01-01 12:00:00]', 'y[x=2009-01-02 12:00:00]', 'y[x=2009-01-05 12:00:00]',
               'y[x=2009-01-06 12:00:00]', 'y[x=2009-01-07 12:00:00]']

    np.testing.assert_array_equal(keys3a, check3a)
    np.testing.assert_array_equal(keys3b, check3b)
    np.testing.assert_array_equal(keys3c, check3c)

    split2a = hist_obj2.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=True)
    split2b = hist_obj2.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=False)
    split2c = hist_obj2.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=False, convert_time_index=False)

    keys2a = list(split2a.keys())
    keys2b = list(split2b.keys())
    keys2c = list(split2c.keys())

    check2a = [0.5, 1.5, 2.5, 3.5, 4.5]
    check2b = [0.5, 1.5, 2.5, 3.5, 4.5]
    check2c = ['y[x=0.5]', 'y[x=1.5]', 'y[x=2.5]', 'y[x=3.5]', 'y[x=4.5]']

    np.testing.assert_array_equal(keys2a, check2a)
    np.testing.assert_array_equal(keys2b, check2b)
    np.testing.assert_array_equal(keys2c, check2c)

    split1a = hist_obj1.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=True)
    split1b = hist_obj1.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=True, convert_time_index=False)
    split1c = hist_obj1.split_hist_along_first_dimension(xname='x', yname='y',
                                                         short_keys=False, convert_time_index=False)

    keys1a = list(split1a.keys())
    keys1b = list(split1b.keys())
    keys1c = list(split1c.keys())

    check1a = ['foo1', 'foo2', 'foo3', 'foo4', 'foo5']
    check1b = ['foo1', 'foo2', 'foo3', 'foo4', 'foo5']
    check1c = ['x=foo1', 'x=foo2', 'x=foo3', 'x=foo4', 'x=foo5']

    np.testing.assert_array_equal(keys1a, check1a)
    np.testing.assert_array_equal(keys1b, check1b)
    np.testing.assert_array_equal(keys1c, check1c)

    # look at the split hists
    hs3 = split3a[keys3a[0]]
    hs2 = split2a[keys2a[0]]
    hs1 = split1a[keys1a[0]]

    assert hs3.n_dim == 2
    assert hs2.n_dim == 1
    assert isinstance(hs3, hg.Bin)
    assert isinstance(hs2, hg.Categorize)
    assert isinstance(hs1, hg.Count)
    assert hs3.contentType == 'Categorize'
    assert hs2.contentType == 'Count'


def test_sum_entries():
    hist1, hist2, hist3 = get_histograms()

    assert sum_entries(hist1) == 5
    assert sum_entries(hist2) == 5
    assert sum_entries(hist3) == 5


def test_sum_over_x():
    df = get_test_data()

    hist1 = hg.Categorize(unit('C'))
    hist2 = hg.Bin(5, 0, 5, unit('A'), value=hist1)
    hist3 = hg.Bin(5, 0, 5, unit('A'))
    hist4 = hg.Categorize(unit('C'), value=hist3)

    for hist in [hist1, hist2, hist3, hist4]:
        hist.fill.numpy(df)

    histC = sum_over_x(hist2)
    histA = sum_over_x(hist4)

    bin_edgesA = histA.bin_edges()
    bin_entriesA = histA.bin_entries()
    bin_edges3 = hist3.bin_edges()
    bin_entries3 = hist3.bin_entries()

    bin_labelsC = histC.bin_labels()
    bin_entriesC = histC.bin_entries()
    bin_labels1 = hist1.bin_labels()
    bin_entries1 = hist1.bin_entries(bin_labelsC)  # match order of labels

    np.testing.assert_array_equal(bin_edgesA, bin_edges3)
    np.testing.assert_array_equal(bin_entriesA, bin_entries3)
    np.testing.assert_array_equal(sorted(bin_labelsC), sorted(bin_labels1))
    np.testing.assert_array_equal(bin_entriesC, bin_entries1)


def test_project_on_x():
    df = get_test_data()
    hist1 = hg.Categorize(unit('C'))
    hist2 = hg.Bin(5, 0, 5, unit('A'), value=hist1)
    hist3 = hg.Bin(5, 0, 5, unit('A'))
    hist4 = hg.Categorize(unit('C'), value=hist3)

    for hist in [hist1, hist2, hist3, hist4]:
        hist.fill.numpy(df)

    histA = project_on_x(hist2)
    histC = project_on_x(hist4)

    bin_edgesA = histA.bin_edges()
    bin_entriesA = histA.bin_entries()
    bin_edges3 = hist3.bin_edges()
    bin_entries3 = hist3.bin_entries()

    bin_labelsC = histC.bin_labels()
    bin_entriesC = histC.bin_entries()
    bin_labels1 = hist1.bin_labels()
    bin_entries1 = hist1.bin_entries(bin_labelsC)  # match order of labels

    np.testing.assert_array_equal(bin_edgesA, bin_edges3)
    np.testing.assert_array_equal(bin_entriesA, bin_entries3)
    np.testing.assert_array_equal(sorted(bin_labelsC), sorted(bin_labels1))
    np.testing.assert_array_equal(bin_entriesC, bin_entries1)


def test_project_split2dhist_on_axis():
    df = get_test_data()

    histA = hg.Bin(5, 0, 5, unit('A'))
    histC = hg.Categorize(unit('C'))
    hist1 = hg.Categorize(unit('C'), value=histA)
    hist2 = hg.Bin(5, 0, 5, unit('A'), value=histC)

    histDCA = hg.SparselyBin(origin=pd.Timestamp('2009-01-01').value, binWidth=pd.Timedelta(days=1).value,
                             quantity=unit('date'), value=hist1)
    histDAC = hg.SparselyBin(origin=pd.Timestamp('2009-01-01').value, binWidth=pd.Timedelta(days=1).value,
                             quantity=unit('date'), value=hist2)

    histDA = hg.SparselyBin(origin=pd.Timestamp('2009-01-01').value, binWidth=pd.Timedelta(days=1).value,
                            quantity=unit('date'), value=histA)
    histDC = hg.SparselyBin(origin=pd.Timestamp('2009-01-01').value, binWidth=pd.Timedelta(days=1).value,
                            quantity=unit('date'), value=histC)

    for hist in [histDA, histDC, histDCA, histDAC]:
        hist.fill.numpy(df)

    # split along date axis
    splitAC = HistogramContainer(histDAC).split_hist_along_first_dimension(xname='x', yname='y',
                                                                           short_keys=True, convert_time_index=True)
    splitCA = HistogramContainer(histDCA).split_hist_along_first_dimension(xname='x', yname='y',
                                                                           short_keys=True, convert_time_index=True)
    splitA0 = HistogramContainer(histDA).split_hist_along_first_dimension(xname='x', yname='y',
                                                                          short_keys=True, convert_time_index=True)
    splitC0 = HistogramContainer(histDC).split_hist_along_first_dimension(xname='x', yname='y',
                                                                          short_keys=True, convert_time_index=True)

    splitA1 = project_split2dhist_on_axis(splitAC, 'x')
    splitA2 = project_split2dhist_on_axis(splitCA, 'y')
    splitC1 = project_split2dhist_on_axis(splitAC, 'y')
    splitC2 = project_split2dhist_on_axis(splitCA, 'x')

    assert len(splitA0) == len(splitA1)
    assert len(splitA0) == len(splitA2)

    for key, h0 in splitA0.items():
        assert key in splitA1
        assert key in splitA2
        h1 = splitA1[key]
        h2 = splitA2[key]
        bin_edges0 = h0.bin_edges()
        bin_edges1 = h1.bin_edges()
        bin_edges2 = h2.bin_edges()
        bin_entries0 = h0.bin_entries()
        bin_entries1 = h1.bin_entries()
        bin_entries2 = h2.bin_entries()
        np.testing.assert_array_equal(bin_edges0, bin_edges1)
        np.testing.assert_array_equal(bin_edges0, bin_edges2)
        np.testing.assert_array_equal(bin_entries0, bin_entries1)
        np.testing.assert_array_equal(bin_entries0, bin_entries2)

    assert len(splitC0) == len(splitC1)
    assert len(splitC0) == len(splitC2)

    for key, h0 in splitC0.items():
        assert key in splitC1
        assert key in splitC2
        h1 = splitC1[key]
        h2 = splitC2[key]
        bin_labels0 = h0.bin_labels()
        bin_labels1 = h1.bin_labels()
        bin_labels2 = h2.bin_labels()
        bin_entries0 = h0.bin_entries()
        bin_entries1 = h1.bin_entries(bin_labels0)
        bin_entries2 = h2.bin_entries(bin_labels0)
        np.testing.assert_array_equal(sorted(bin_labels0), sorted(bin_labels1))
        np.testing.assert_array_equal(sorted(bin_labels0), sorted(bin_labels2))
        np.testing.assert_array_equal(bin_entries0, bin_entries1)
        np.testing.assert_array_equal(bin_entries0, bin_entries2)


def test_datatype():
    """ Test datatypes assigned to histograms
    """
    hist1, hist2, hist3 = get_histograms()
    hist0 = hg.Count()

    assert isinstance(None, hist0.datatype)
    assert hist1.datatype == str
    np.testing.assert_array_equal(hist2.datatype, [np.float64, str])
    np.testing.assert_array_equal(hist3.datatype, [np.datetime64, np.float64, str])
