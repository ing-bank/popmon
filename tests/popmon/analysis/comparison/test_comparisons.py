import numpy as np
import pytest

from popmon.analysis.comparison.comparisons import (
    jensen_shannon_divergence,
    ks_prob,
    ks_test,
    kullback_leibler_divergence,
    population_stability_index,
    uu_chi2,
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


def test_kl():
    np.testing.assert_almost_equal(
        kullback_leibler_divergence(
            np.array([0.25, 0.25, 0.25, 0.25]), np.array([0.85, 0.05, 0.05, 0.05])
        ),
        0.90105,
        4,
    )
    np.testing.assert_almost_equal(
        kullback_leibler_divergence(
            np.array([0.85, 0.05, 0.05, 0.05]), np.array([0.25, 0.25, 0.25, 0.25])
        ),
        0.79875,
        4,
    )
    np.testing.assert_equal(
        kullback_leibler_divergence(
            np.array([0.25, 0.25, 0.25, 0.25]), np.array([0.25, 0.25, 0.25, 0.25])
        ),
        0,
    )
    np.testing.assert_equal(
        kullback_leibler_divergence(
            np.array([0.85, 0.05, 0.05, 0.05]), np.array([0.85, 0.05, 0.05, 0.05])
        ),
        0,
    )
    np.testing.assert_almost_equal(
        kullback_leibler_divergence(
            np.array([0.0, 0.0, 0.0, 0.0]), np.array([0.0, 0.0, 0.0, 0.05])
        ),
        0,
        4,
    )


def test_psi():
    p = np.array([0.85, 0.05, 0.05, 0.05])
    q = np.array([0.25, 0.25, 0.25, 0.25])

    np.testing.assert_almost_equal(
        population_stability_index(p, q), 1.699815077214137, 4
    )
    np.testing.assert_almost_equal(
        population_stability_index(p, q), population_stability_index(q, p), 4
    )
    np.testing.assert_almost_equal(population_stability_index(q, q), 0.0, 4)


def test_jsd():
    p = np.array([0.85, 0.05, 0.05, 0.05])
    q = np.array([0.25, 0.25, 0.25, 0.25])

    # JSD is symmetric
    np.testing.assert_almost_equal(
        jensen_shannon_divergence(p, q), jensen_shannon_divergence(q, p), 4
    )

    # JSD = 0 iff P=Q
    np.testing.assert_almost_equal(jensen_shannon_divergence(q, q), 0, 4)
