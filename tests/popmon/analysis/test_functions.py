import numpy as np
import pandas as pd
import pytest
from popmon.base import Pipeline
from popmon.io import JsonReader
from popmon.hist.hist_splitter import HistSplitter
from popmon.analysis.apply_func import ApplyFunc
from popmon.analysis.functions import roll, expand, roll_norm_hist_mean_cov, expand_norm_hist_mean_cov
from popmon.analysis.functions import expanding_hist, rolling_hist, relative_chi_squared, normalized_hist_mean_cov
from popmon.analysis.comparison import ReferenceNormHistComparer, RollingNormHistComparer, ExpandingNormHistComparer
from popmon import resources


def test_roll():
    df = pd.DataFrame({'a': np.arange(100)})
    rdf = roll(df, window=5, shift=1)
    arr = rdf['a'].values[-1]

    check = np.array([94, 95, 96, 97, 98])
    np.testing.assert_array_equal(arr, check)


def test_expand():
    df = pd.DataFrame({'a': np.arange(10)})
    edf = expand(df, shift=1)
    arr = edf['a'].values[-1]

    check = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8])
    np.testing.assert_array_equal(arr, check)


def test_expanding_hist():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist',
                  apply_funcs=[dict(func=expanding_hist, shift=1, suffix='sum', entire=True, hist_name='histogram')]),
    ])
    datastore = pipeline.transform(datastore={})

    df = datastore['output_hist']['num_employees']
    h = df['histogram_sum'].values[-1]
    bin_entries = h.hist.bin_entries()

    check = np.array([11., 1., 1., 0., 0., 0., 0., 0., 0., 0., 1., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 1., 0., 0., 0., 0., 0.,
                      0., 0., 1., 0., 0., 0., 0., 0., 0., 0., 0., 1., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 1.])

    np.testing.assert_array_almost_equal(bin_entries, check)


def test_rolling_hist():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist',
                  apply_funcs=[dict(func=rolling_hist, window=5, shift=1, suffix='sum', entire=True,
                                    hist_name='histogram')]),
    ])
    datastore = pipeline.transform(datastore={})

    df = datastore['output_hist']['num_employees']
    h = df['histogram_sum'].values[-2]
    bin_entries = h.hist.bin_entries()

    check = np.array([4., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 1.])

    np.testing.assert_array_almost_equal(bin_entries, check)


def test_normalized_hist_mean_cov():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist', assign_to_key='output_hist',
                  apply_funcs=[dict(func=normalized_hist_mean_cov, suffix='')])
    ])
    datastore = pipeline.transform(datastore={})

    assert 'output_hist' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['output_hist']

    df = datastore['output_hist']['A_score']

    check = np.array([[0.22916667, -0.01041667, -0.0625, -0.13541667, -0.02083333],
                      [-0.01041667, 0.015625, 0.01041667, -0.01354167, -0.00208333],
                      [-0.0625, 0.01041667, 0.12916667, -0.06458333, -0.0125],
                      [-0.13541667, -0.01354167, -0.06458333, 0.240625, -0.02708333],
                      [-0.02083333, -0.00208333, -0.0125, -0.02708333, 0.0625]])

    for hm, hc, hb in zip(df['histogram_mean'].values, df['histogram_cov'].values, df['histogram_binning'].values):
        np.testing.assert_array_almost_equal(hm, [0.3125, 0.03125, 0.1875, 0.40625, 0.0625])
        np.testing.assert_array_almost_equal(hb, [1.5, 2.5, 3.5, 4.5, 5.5])
        np.testing.assert_array_almost_equal(hc, check)


def test_roll_norm_hist_mean_cov():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[
            dict(func=roll_norm_hist_mean_cov, hist_name='histogram', window=5, shift=1, suffix='', entire=True)])
    ])
    datastore = pipeline.transform(datastore={})

    assert 'output_hist' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['output_hist']

    df = datastore['output_hist']['num_employees']
    mean = df['histogram_mean'].values[-2]

    check = np.array([0.8, 0.1, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.1])

    np.testing.assert_array_almost_equal(mean, check)


def test_expand_norm_hist_mean_cov():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[
            dict(func=expand_norm_hist_mean_cov, hist_name='histogram', shift=1, suffix='', entire=True)])
    ])
    datastore = pipeline.transform(datastore={})

    assert 'output_hist' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['output_hist']

    df = datastore['output_hist']['num_employees']
    mean = df['histogram_mean'].values[-2]

    check = np.array([0.56666667, 0.03333333, 0.03333333, 0., 0.,
                      0., 0., 0., 0., 0.,
                      0.06666667, 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0., 0.,
                      0., 0.06666667, 0., 0., 0.,
                      0., 0., 0., 0., 0.06666667,
                      0., 0., 0., 0., 0.,
                      0., 0., 0., 0.03333333, 0.06666667, 0.06666667])

    np.testing.assert_array_almost_equal(mean, check)


@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
def test_chi_squared1():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[
            dict(func=roll_norm_hist_mean_cov, hist_name='histogram', window=5, shift=1, suffix='', entire=True)]),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[dict(func=relative_chi_squared, suffix='', axis=1)])
    ])
    datastore = pipeline.transform(datastore={})

    assert 'output_hist' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['output_hist']

    df = datastore['output_hist']['A_score']
    np.testing.assert_almost_equal(df['chi2'][6], 3.275000000000001)
    df = datastore['output_hist']['A_score:num_employees']
    np.testing.assert_almost_equal(df['chi2'][-2], 2.1333333333333315)
    df = datastore['output_hist']['bankrupt']
    np.testing.assert_almost_equal(df['chi2'][6], 0.19687500000000002)
    df = datastore['output_hist']['country']
    np.testing.assert_almost_equal(df['chi2'][5], 0.8999999999999994)
    df = datastore['output_hist']['num_employees']
    np.testing.assert_almost_equal(df['chi2'][5], 0.849999999999999)


@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
def test_chi_squared2():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[
            dict(func=expand_norm_hist_mean_cov, hist_name='histogram', shift=1, suffix='', entire=True)]),
        ApplyFunc(apply_to_key='output_hist', apply_funcs=[dict(func=relative_chi_squared, suffix='', axis=1)])
    ])
    datastore = pipeline.transform(datastore={})

    assert 'output_hist' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['output_hist']

    df = datastore['output_hist']['A_score']
    np.testing.assert_almost_equal(df['chi2'][-1], 4.066666666666674)
    df = datastore['output_hist']['A_score:num_employees']
    np.testing.assert_almost_equal(df['chi2'][-2], 3.217532467532462)
    df = datastore['output_hist']['bankrupt']
    np.testing.assert_almost_equal(df['chi2'][-1], 0.11718750000000011)
    df = datastore['output_hist']['country']
    np.testing.assert_almost_equal(df['chi2'][-1], 0.6093749999999999)
    df = datastore['output_hist']['num_employees']
    np.testing.assert_almost_equal(df['chi2'][-1], 1.1858766233766194)


def test_chi_ReferenceNormHistComparer():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ReferenceNormHistComparer(reference_key='output_hist', assign_to_key='output_hist', store_key='comparisons')
    ])
    datastore = pipeline.transform(datastore={})

    assert 'comparisons' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['comparisons']

    df = datastore['comparisons']['A_score']
    np.testing.assert_almost_equal(df['chi2'][0], 1.2734375)


def test_chi_ExpandingNormHistComparer():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ExpandingNormHistComparer(read_key='output_hist', store_key='comparisons')
    ])
    datastore = pipeline.transform(datastore={})

    assert 'comparisons' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['comparisons']

    df = datastore['comparisons']['A_score']
    np.testing.assert_almost_equal(df['chi2'][-1], 4.06666667)


def test_chi_RollingNormHistComparer():
    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        RollingNormHistComparer(read_key='output_hist', store_key='comparisons', window=10)
    ])
    datastore = pipeline.transform(datastore={})

    assert 'comparisons' in datastore
    for f in ['A_score', 'A_score:num_employees', 'bankrupt', 'country', 'num_employees']:
        assert f in datastore['comparisons']

    df = datastore['comparisons']['A_score']
    np.testing.assert_almost_equal(df['chi2'][-1], 45.200000)
