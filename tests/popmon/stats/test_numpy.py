import itertools

import numpy as np
from scipy import linalg, stats

from popmon.stats.numpy import (
    mean,
    probability_distribution_mean_covariance,
    quantile,
    std,
)


def get_data():
    rng = np.random.default_rng(5)
    a = rng.integers(0, 10, size=(3, 4, 5, 6))
    w = rng.integers(0, 10, size=(3, 4, 5, 6))
    return a, w


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


def test_probability_distribution_mean_covariance():
    rng = np.random.default_rng(42)
    n_bins = 10
    n_histos = 5000
    max_hist_entries = 10000
    rel_error = 0.1
    bin_entries = []
    for _ in range(n_histos):
        bin_probs = rng.normal(1.0, rel_error, size=n_bins)  # + basic
        bin_probs /= np.sum(bin_probs)
        bin_entries.append(rng.multinomial(max_hist_entries, bin_probs))
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
                (norm_mean - single_norm) ** 2 / (variance + np.finfo(float).eps)
            )

        n_bins = len(bin_entries[i])
        p_value = stats.chi2.sf(chi_squared, n_bins - 1)
        chi2s.append(chi_squared)
        pvalues.append(p_value)

    h, _ = np.histogram(pvalues, bins=10, range=(0, 1), density=True)
    np.testing.assert_allclose(h, 1, rtol=1e-01, atol=0)
