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
from scipy import stats

from popmon.base.registry import Registry

Comparisons = Registry()


@Comparisons.register(
    key="max_prob_diff",
    description="The largest absolute difference between all bin pairs of two normalized histograms",
    htype="all",
)
def googl_test(bins_1, bins_2):
    """Google-paper test

    Reference link: https://mlsys.org/Conferences/2019/doc/2019/167.pdf

    :param bins_1: first array of bin entries
    :param bins_2: second array of entries

    :return: maximum difference between the two entry distributions
    :rtype: float
    """

    def dist(bins):
        sum_ = np.sum(bins)
        return bins / sum_ if sum_ else bins

    return np.max(np.abs(dist(bins_1) - dist(bins_2)))


@Comparisons.register(key="psi", description="Population Stability Index", htype="all")
def population_stability_index(po, qo):
    epsilon = 10e-6
    p = po.copy()
    q = qo.copy()
    p += epsilon
    q += epsilon
    return np.sum((p - q) * np.log(p / q))


def kullback_leibler_divergence(po, qo):
    epsilon = 10e-6
    p = po.copy()
    q = qo.copy()
    p += epsilon
    q += epsilon
    return np.sum(p * np.log(p / q))


@Comparisons.register(key="jsd", description="Jensen-Shannon Divergence", htype="all")
def jensen_shannon_divergence(p, q):
    m = 0.5 * (p + q)
    return 0.5 * (kullback_leibler_divergence(p, m) + kullback_leibler_divergence(q, m))


def ks_test(hist_1, hist_2):
    """KS-test for two histograms with different number of entries

    Copyright ROOT:
    Formulas translated from c++ to python, but formulas otherwise not modified.
    Reference: link: https://root.cern.ch/doc/master/classTH1.html#TH1:KolmogorovTest
    GNU license: https://root.cern.ch/license
    All modifications copyright INGA WB.

    :param hist_1: 1D array with bin counts of the histogram_1
    :param hist_2: 1D array with bin counts of the histogram_2

    :return: ks_score: Kolmogorov-Smirnov Test score
    :rtype: float
    """
    if len(hist_1) == 0 or len(hist_2) == 0:
        raise ValueError("Input histogram(s) has zero size.")
    if len(hist_1) != len(hist_2):
        raise ValueError("Input histograms have unequal size.")

    sum_1 = np.sum(hist_1)
    sum_2 = np.sum(hist_2)
    if sum_1 == 0 or sum_2 == 0:
        return np.nan

    normalized_cumsum_1 = np.cumsum(hist_1) / sum_1
    normalized_cumsum_2 = np.cumsum(hist_2) / sum_2

    d = np.abs(normalized_cumsum_1 - normalized_cumsum_2)

    return np.max(d) * np.sqrt(sum_1 * sum_2 / (sum_1 + sum_2))


def ks_prob(testscore):
    """KS-probability corresponding ti KS test score

    Copyright ROOT:
    Formulas translated from c++ to python, but formulas otherwise not modified.
    Reference: https://root.cern.ch/doc/master/classTH1.html#TH1:KolmogorovTest
    GNU license: https://root.cern.ch/license
    All modifications copyright INGA WB.

    :param float testscore: Kolmogorov-Smirnov test score

    :return: approximate pvalue for the Kolmogorov-Smirnov test score
    :rtype: float
    """
    fj = np.array([-2, -8, -18, -32])
    r = np.zeros(4)

    w = 2.50662827
    c = np.array([-1.2337005501361697, -11.103304951225528, -30.842513753404244])

    u = abs(testscore)
    pvalue = np.nan
    if u < 0.2:
        pvalue = 1
    elif u < 0.755:
        v = np.power(u, -2)
        pvalue = 1 - w * np.exp(c * v).sum() / u
    elif u < 6.8116:
        v = np.power(u, 2)
        max_j = int(max(1, round(3.0 / u)))
        r[:max_j] = np.exp(fj[:max_j] * v)
        pvalue = 2 * (r[0] - r[1] + r[2] - r[3])

    return pvalue


@Comparisons.register(
    key=["ks", "ks_pvalue", "ks_zscore"],
    description=[
        "Kolmogorov-Smirnov test statistic",
        "p-value of the Kolmogorov-Smirnov test",
        "Z-score of the Kolmogorov-Smirnov test",
    ],
    dim=1,
    htype="num",
)
def ks(p, q, *_):
    # KS-test only properly defined for (ordered) 1D interval variables
    ks_testscore = ks_test(p, q)
    ks_pvalue = ks_prob(ks_testscore)
    ks_zscore = -stats.norm.ppf(ks_pvalue)
    return ks_testscore, ks_pvalue, ks_zscore


@Comparisons.register(
    key="unknown_labels",
    description="Are categories observed in a given time slot that are not present in the reference?",
    dim=1,
    htype="cat",
)
def unknown_labels(hist1, hist2) -> bool:
    # check consistency of bin_labels
    labels1 = hist1.keySet
    labels2 = hist2.keySet
    subset = labels1 <= labels2
    return not subset


@Comparisons.register(
    key="pearson",
    description="Pearson correlation coefficient",
    dim=(2,),
    htype="all",
)
def pearson(p, q, *_):
    # calculate pearson coefficient
    pearson_coeff = np.nan
    if len(p) >= 2:
        same0 = all(p == p[0])
        same1 = all(q == q[0])
        if not same0 and not same1:
            # this avoids std==0, and thereby avoid runtime warnings
            pearson_coeff, _ = stats.pearsonr(p, q)
    return pearson_coeff


def uu_chi2(n, m):
    """Normalized Chi^2 formula for two histograms with different number of entries

    Copyright ROOT:
    Formulas translated from c++ to python, but formulas otherwise not modified.
    Reference: https://root.cern.ch/doc/master/classTH1.html#a6c281eebc0c0a848e7a0d620425090a5
    GNU License: https://root.cern.ch/license
    All modifications copyright INGA WB.

    :param n: 1d array with bin counts of the reference set
    :param m: 1d array with bin counts of the test set
    :return: tuple of floats (chi2_value, chi2_norm, z_score, p_value, res)
    """

    def _not_finite_to_zero(x):
        res = x.copy()
        res[~np.isfinite(res)] = 0
        return res

    if len(n) == 0 or len(m) == 0:
        raise ValueError("Input histogram(s) has zero size.")
    if len(n) != len(m):
        raise ValueError("Input histograms have unequal size.")

    N = np.sum(n)  # noqa: N806
    M = np.sum(m)  # noqa: N806

    if N == 0 or M == 0:
        return np.nan, np.nan, np.nan, np.nan, [0] * len(n)

    # remove all zero entries in the sum, to present division by zero for individual bins
    z = n + m
    n = n[z != 0]
    m = m[z != 0]

    dof = ((n != 0) | (m != 0)).sum() - 1
    chi2_value = _not_finite_to_zero(((M * n - N * m) ** 2) / (n + m)).sum() / M / N

    chi2_norm = chi2_value / dof if dof > 0 else np.nan
    p_value = stats.chi2.sf(chi2_value, dof)
    z_score = -stats.norm.ppf(p_value)

    p = (n + m) / (N + M)

    if (p == 1).any():
        # unusual case of (only) one bin with p==1, avoids division with zero below
        res = np.array([np.nan] * len(p))
    else:
        res = _not_finite_to_zero(
            (n - N * p) / np.sqrt(N * p) / np.sqrt((1 - N / (N + M)) * (1 - p))
        )

    return chi2_value, chi2_norm, z_score, p_value, res


@Comparisons.register(
    key=[
        "chi2",
        "chi2_norm",
        "chi2_zscore",
        "chi2_pvalue",
        "chi2_max_residual",
        "chi2_spike_count",
    ],
    description=[
        "Chi-squared test statistic",
        "Normalized chi-squared statistic",
        "Z-score of the chi-squared statistic",
        "p-value of the chi-squared statistic",
        "The largest absolute normalized residual (|chi|) observed in all bin pairs",
        "The number of normalized residuals of all bin pairs with absolute value bigger than a given threshold (default: 7).",
    ],
    htype="all",
)
def chi2(*args, max_res_bound=7.0):
    chi2r, chi2_norm, zscore, pvalue, res = uu_chi2(*args)
    abs_residual = np.abs(res)
    chi2_max_residual = np.max(abs_residual)
    chi2_spike_count = np.sum(abs_residual[abs_residual > max_res_bound])

    return chi2r, chi2_norm, zscore, pvalue, chi2_max_residual, chi2_spike_count
