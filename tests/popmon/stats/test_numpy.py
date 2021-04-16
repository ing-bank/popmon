import itertools

import numpy as np
import pytest
from scipy import linalg, stats

from popmon.stats.numpy import (
    fraction_of_true,
    ks_prob,
    ks_test,
    mean,
    probability_distribution_mean_covariance,
    quantile,
    std,
    uu_chi2,
)


def get_data():
    np.random.seed(5)
    a = np.random.randint(0, 10, size=(3, 4, 5, 6))
    w = np.random.randint(0, 10, size=(3, 4, 5, 6))
    return a, w


def test_fraction_of_true():
    res = fraction_of_true([], [])
    assert np.isnan(res)
    res = fraction_of_true(["a"], [10])
    assert np.isnan(res)
    res = fraction_of_true(["a", "b", "c"], [10, 10, 10])
    assert np.isnan(res)

    res = fraction_of_true(np.array(["True", "False"]), np.array([0, 0]))
    assert np.isnan(res)
    res = fraction_of_true(np.array(["True", "False"]), np.array([10, 10]))
    assert res == 0.5
    res = fraction_of_true(np.array([True, False]), [10, 10])
    assert res == 0.5

    res = fraction_of_true(np.array(["True"]), np.array([10]))
    assert res == 1.0
    res = fraction_of_true(np.array([True]), np.array([10]))
    assert res == 1.0
    res = fraction_of_true(np.array(["False"]), np.array([10]))
    assert res == 0.0
    res = fraction_of_true(np.array([False]), np.array([10]))
    assert res == 0.0


def test_mean_shapes():
    a, w = get_data()
    out = mean(a)
    assert out.ndim == 0
    out = mean(a, w)
    assert out.ndim == 0
    out = mean(a, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel()[0], mean(a), rtol=1e-07)
    out = mean(a, w, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel()[0], mean(a, w), rtol=1e-07)
    out = mean(a, axis=1)
    assert out.shape == (3, 5, 6)
    out = mean(a, w, axis=1)
    assert out.shape == (3, 5, 6)
    out = mean(a, axis=(1, 2))
    assert out.shape == (3, 6)
    np.testing.assert_allclose(out, mean(a, np.ones_like(a), axis=(1, 2)), rtol=1e-07)
    out = mean(a, w, axis=(1, 2))
    assert out.shape == (3, 6)
    out = mean(a, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(out.ravel(), mean(a, axis=(1, 2)).ravel(), rtol=1e-07)
    out = mean(a, w, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(out.ravel(), mean(a, w, axis=(1, 2)).ravel(), rtol=1e-07)


def test_std_shapes():
    a, w = get_data()
    out = std(a)
    assert out.ndim == 0
    out = std(a, w)
    assert out.ndim == 0
    out = std(a, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel()[0], std(a), rtol=1e-07)
    out = std(a, w, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel()[0], std(a, w), rtol=1e-07)
    out = std(a, axis=1)
    assert out.shape == (3, 5, 6)
    out = std(a, w, axis=1)
    assert out.shape == (3, 5, 6)
    out = std(a, axis=(1, 2))
    assert out.shape == (3, 6)
    np.testing.assert_allclose(out, std(a, np.ones_like(a), axis=(1, 2)), rtol=1e-07)
    out = std(a, w, axis=(1, 2))
    assert out.shape == (3, 6)
    out = std(a, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(out.ravel(), std(a, axis=(1, 2)).ravel(), rtol=1e-07)
    out = std(a, w, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(out.ravel(), std(a, w, axis=(1, 2)).ravel(), rtol=1e-07)


def test_quantiles_shapes():
    a, w = get_data()
    out = quantile(a, 0.5)
    assert out.ndim == 0
    out = quantile(a, 0.5, w)
    assert out.ndim == 0
    out = quantile(a, (0.4, 0.6))
    assert out.ndim == 1
    out = quantile(a, (0.4, 0.6), w)
    assert out.ndim == 1
    out = quantile(a, 0.5, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel(), quantile(a, 0.5), rtol=1e-07)
    out = quantile(a, 0.5, w, keepdims=True)
    assert out.shape == (1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel(), quantile(a, 0.5, w), rtol=1e-07)
    out = quantile(a, (0.4, 0.6), keepdims=True)
    assert out.shape == (2, 1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel(), quantile(a, (0.4, 0.6)), rtol=1e-07)
    out = quantile(a, (0.4, 0.6), w, keepdims=True)
    assert out.shape == (2, 1, 1, 1, 1)
    np.testing.assert_allclose(out.ravel(), quantile(a, (0.4, 0.6), w), rtol=1e-07)
    out = quantile(a, 0.5, axis=1)
    assert out.shape == (3, 5, 6)
    out = quantile(a, 0.5, w, axis=1)
    assert out.shape == (3, 5, 6)
    out = quantile(a, (0.4, 0.6), axis=1)
    assert out.shape == (2, 3, 5, 6)
    out = quantile(a, (0.4, 0.6), w, axis=1)
    assert out.shape == (2, 3, 5, 6)
    out = quantile(a, 0.5, axis=(1, 2))
    assert out.shape == (3, 6)
    out = quantile(a, 0.5, w, axis=(1, 2))
    assert out.shape == (3, 6)
    out = quantile(a, (0.4, 0.6), axis=(1, 2))
    assert out.shape == (2, 3, 6)
    out = quantile(a, (0.4, 0.6), w, axis=(1, 2))
    assert out.shape == (2, 3, 6)
    out = quantile(a, 0.5, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(
        out.ravel(), quantile(a, 0.5, axis=(1, 2)).ravel(), rtol=1e-07
    )
    out = quantile(a, 0.5, w, axis=(1, 2), keepdims=True)
    assert out.shape == (3, 1, 1, 6)
    np.testing.assert_allclose(
        out.ravel(), quantile(a, 0.5, w, axis=(1, 2)).ravel(), rtol=1e-07
    )
    out = quantile(a, (0.4, 0.6), axis=(1, 2), keepdims=True)
    assert out.shape == (2, 3, 1, 1, 6)
    np.testing.assert_allclose(
        out.ravel(), quantile(a, (0.4, 0.6), axis=(1, 2)).ravel(), rtol=1e-07
    )
    out = quantile(a, (0.4, 0.6), w, axis=(1, 2), keepdims=True)
    assert out.shape == (2, 3, 1, 1, 6)
    np.testing.assert_allclose(
        out.ravel(), quantile(a, (0.4, 0.6), w, axis=(1, 2)).ravel(), rtol=1e-07
    )


def test_statistics_1():
    a, w = get_data()
    _means = np.zeros((3, 1, 1, 6))
    _weights = np.zeros((3, 1, 1, 6))
    for i, j, k, l in itertools.product(
        range(a.shape[0]), range(a.shape[1]), range(a.shape[2]), range(a.shape[3])
    ):
        _means[i, 0, 0, l] += w[i, j, k, l] * a[i, j, k, l]
        _weights[i, 0, 0, l] += w[i, j, k, l]
    _means /= _weights
    np.testing.assert_allclose(
        _means, mean(a, w, axis=(1, 2), keepdims=True), rtol=1e-07
    )
    _stds = np.zeros((3, 1, 1, 6))
    for i, j, k, l in itertools.product(
        range(a.shape[0]), range(a.shape[1]), range(a.shape[2]), range(a.shape[3])
    ):
        _stds[i, 0, 0, l] += w[i, j, k, l] * (a[i, j, k, l] - _means[i, 0, 0, l]) ** 2
    _stds = np.sqrt(_stds / _weights)
    np.testing.assert_allclose(_stds, std(a, w, axis=(1, 2), keepdims=True), rtol=1e-07)
    _values = np.zeros((3, 6, 4 * 5))
    _weights = np.zeros((3, 6, 4 * 5))
    for i, j, k, l in itertools.product(
        range(a.shape[0]), range(a.shape[1]), range(a.shape[2]), range(a.shape[3])
    ):
        _values[i, l, j * a.shape[2] + k] = a[i, j, k, l]
        _weights[i, l, j * a.shape[2] + k] = w[i, j, k, l]

    def get_quantiles(q):
        _quantiles = np.zeros((3, 6))
        for i in range(a.shape[0]):
            for ll in range(a.shape[3]):
                isort = np.argsort(_values[i, ll])
                v = _values[i, ll][isort]
                u = _weights[i, ll][isort]
                U = u.cumsum()
                r = (U - 0.5 * u) / U[-1]
                for m in range(1, len(u)):
                    if r[m - 1] <= q and r[m] > q:
                        _quantiles[i, ll] = v[m - 1] + (q - r[m - 1]) / (
                            r[m] - r[m - 1]
                        ) * (v[m] - v[m - 1])
                        break
        return _quantiles

    np.testing.assert_allclose(
        get_quantiles(0.1), quantile(a, 0.1, w, axis=(1, 2), keepdims=False), rtol=1e-07
    )
    np.testing.assert_allclose(
        get_quantiles(0.5), quantile(a, 0.5, w, axis=(1, 2), keepdims=False), rtol=1e-07
    )
    np.testing.assert_allclose(
        get_quantiles(0.9), quantile(a, 0.9, w, axis=(1, 2), keepdims=False), rtol=1e-07
    )


@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
def test_uu_chi2():
    arr1 = np.array([1, 2, 0, 5])
    arr2 = np.array([3, 4, 0, 2])

    chi2_value, chi2_norm, z_score, p_value, res = uu_chi2(arr1, arr2)

    np.testing.assert_equal(len(res) + 1, arr1.shape[0])
    np.testing.assert_almost_equal(chi2_value, 2.9036, 4)
    np.testing.assert_almost_equal(chi2_norm, 1.45180, 4)
    np.testing.assert_almost_equal(z_score, 0.7252, 4)
    np.testing.assert_almost_equal(p_value, 0.2341, 4)


def test_ks_test():
    np.testing.assert_almost_equal(ks_test([2, 4], [1, 3]), 0.1291, 4)
    np.testing.assert_equal(ks_test([1, 1], [5, 7]), ks_test([5, 7], [1, 1]))


def test_ks_prob():
    np.testing.assert_equal(ks_prob(0.1), 1)
    np.testing.assert_equal(ks_prob(10.0), np.nan)
    np.testing.assert_equal(ks_prob(0.4), ks_prob(-0.4))
    np.testing.assert_almost_equal(ks_prob(0.8), 0.5441, 4)
    np.testing.assert_almost_equal(ks_prob(3.0), 0.0, 4)


def test_probability_distribution_mean_covariance():
    np.random.seed(42)
    n_bins = 10
    n_histos = 5000
    max_hist_entries = 10000
    rel_error = 0.1
    # basic = np.random.uniform(0, 1, size=n_bins)
    bin_entries = []
    for k in range(n_histos):
        bin_probs = np.random.normal(1.0, rel_error, size=n_bins)  # + basic
        bin_probs /= np.sum(bin_probs)
        bin_entries.append(np.random.multinomial(max_hist_entries, bin_probs))
    bin_entries = np.array(bin_entries)

    chi2s = []
    pvalues = []

    for i in range(n_histos):
        others = np.delete(bin_entries, [i], axis=0)
        norm_mean, norm_cov = probability_distribution_mean_covariance(others)
        single_norm, single_cov = probability_distribution_mean_covariance(
            [bin_entries[i]]
        )

        # total covariance
        cov = norm_cov
        variance = np.diagonal(cov)

        if np.linalg.cond(cov) < 0.1 / np.finfo(cov.dtype).eps:
            # We try to use the precision matrix (inverse covariance matrix) for the chi-squared calculation
            pm = linalg.inv(cov)
            chi_squared = np.dot(
                (norm_mean - single_norm), np.dot(pm, (norm_mean - single_norm))
            )
        else:
            # If a covariance matrix is singular we fall back on using variances
            chi_squared = np.sum(
                (norm_mean - single_norm) ** 2 / (variance + np.finfo(np.float).eps)
            )

        n_bins = len(bin_entries[i])
        p_value = stats.chi2.sf(chi_squared, n_bins - 1)
        chi2s.append(chi_squared)
        pvalues.append(p_value)

    h, _ = np.histogram(pvalues, bins=10, range=(0, 1), density=True)
    np.testing.assert_allclose(h, 1, rtol=1e-01, atol=0)
