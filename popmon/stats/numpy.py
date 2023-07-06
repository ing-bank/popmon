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
import pandas as pd


def mean(a, weights=None, axis=None, dtype=None, keepdims: bool = False, ddof: int = 0):
    """
    Compute the weighted mean along the specified axis.

    :param a: Array containing numbers whose mean is desired. If `a` is not an array, a conversion is attempted.
    :param weights: Array containing weights for the elements of `a`. If `weights` is not an
        array, a conversion is attempted.
    :param axis: Axis or axes along which the means are computed. The default is to
        compute the mean of the flattened array. Type is None or int or tuple of ints, optional.
    :param dtype: data type to use in computing the mean.
    :param bool keepdims: If this is set to True, the axes which are reduced are left
        in the result as dimensions with size one.
    :param int ddof: delta degrees of freedom
    :return: np.ndarray
    """
    if weights is None:
        return np.mean(a, axis=axis, dtype=dtype, keepdims=keepdims)
    else:
        w = np.array(weights)

        return np.sum(w * np.array(a), axis=axis, dtype=dtype, keepdims=keepdims) / (
            np.sum(w, axis=axis, dtype=dtype, keepdims=keepdims) - ddof
        )


def std(a, weights=None, axis=None, dtype=None, ddof: int = 0, keepdims: bool = False):
    """
    Compute the weighted standard deviation along the specified axis.

    :param a: Array containing numbers whose standard deviation is desired. If `a` is not an
        array, a conversion is attempted.
    :param weights: Array containing weights for the elements of `a`. If `weights` is not an
        array, a conversion is attempted.
    :param axis: Axis or axes along which the means are computed. The default is to
        compute the mean of the flattened array. Type is None or int or tuple of ints, optional.
    :param dtype: data type to use in computing the mean.
    :param int ddof: Delta Degrees of Freedom.  The divisor used in calculations
        is ``W - ddof``, where ``W`` is the sum of weights (or number of elements
        if `weights` is None). By default `ddof` is zero
    :param bool keepdims: If this is set to True, the axes which are reduced are left
        in the result as dimensions with size one.
    :return: np.ndarray
    """
    if weights is None:
        return np.std(a, axis=axis, dtype=dtype, ddof=ddof, keepdims=keepdims)
    else:
        m = mean(a, weights=weights, axis=axis, keepdims=True)
        v = mean((a - m) ** 2, weights=weights, axis=axis, keepdims=keepdims, ddof=ddof)
        return np.sqrt(v)


def median(a, weights=None, axis=None, keepdims: bool = False):
    """
    Compute the weighted median along the specified axis.

    After https://en.wikipedia.org/wiki/Percentile#Weighted_percentile

    :param a: Array containing numbers whose median is desired. If `a` is not an
        array, a conversion is attempted.
    :param weights: Array containing weights for the elements of `a`. If `weights` is not an
        array, a conversion is attempted.
    :param axis: Axis or axes along which the means are computed. The default is to
        compute the mean of the flattened array. Type is None or int or tuple of ints, optional.
    :param bool keepdims: If this is set to True, the axes which are reduced are left
        in the result as dimensions with size one.
    :return: number or array
    """
    return quantile(a, q=0.5, weights=weights, axis=axis, keepdims=keepdims)


def quantile(a, q, weights=None, axis=None, keepdims: bool = False):
    """
    Compute the weighted quantiles along the specified axis

    After https://en.wikipedia.org/wiki/Percentile#Weighted_percentile

    If `q` is a single quantile and `axis=None`, then the result
    is a scalar. If multiple quantiles are given, first axis of
    the result corresponds to the quantiles. The other axes are
    the axes that remain after the reduction of `a`.

    :param a: Array containing numbers whose median is desired. If `a` is not an
        array, a conversion is attempted
    :param q: Quantile or sequence of quantiles to compute, which must be between 0 and 1 inclusive
    :param weights: Array containing weights for the elements of `a`. If `weights` is not an
        array, a conversion is attempted.
    :param axis: Axis or axes along which the quantiles are computed. The
        default is to compute the quantile(s) along a flattened. Type is int, tuple of int, None, optional.
        version of the array
    :param bool keepdims: If this is set to True, the axes which are reduced are left
        in the result as dimensions with size one.
    :return: scalar or ndarray
    """
    q = q if not hasattr(q, "__iter__") else q[0] if len(q) == 1 else tuple(q)
    if weights is None:
        return np.quantile(a, q, axis=axis, keepdims=keepdims)
    elif axis is None:
        raveled_data = np.ravel(a)
        idx = np.argsort(raveled_data)
        sorted_data = raveled_data[idx]
        sorted_weights = np.ravel(weights)[idx]
        Sn = np.cumsum(sorted_weights)  # noqa: N806
        Pn = (Sn - 0.5 * sorted_weights) / Sn[-1]  # noqa: N806
        y = np.interp(q, Pn, sorted_data)
        if keepdims:
            y = y.reshape((*y.shape, *(1,) * np.ndim(a)))

        return y
    else:
        # Move the dimensions which are reduced to the back
        axis = [axis] if not hasattr(axis, "__iter__") else axis
        destination = list(range(-len(axis), 0, 1))
        a_moved = np.moveaxis(a, source=axis, destination=destination)

        # Reshape into a 2D-array, with the first axis the dimensions
        # that are not reduced, and the second the dimensions that are reduced
        shape = (-1, np.prod(a_moved.shape[-len(axis) :]))
        a_shaped = a_moved.reshape(shape)

        w = np.moveaxis(weights, source=axis, destination=destination).reshape(shape)

        # Determine the quantiles and reshape backwards
        y = np.array([quantile(x, q, u) for x, u in zip(a_shaped, w)]).T
        if keepdims:
            shape = (
                *y.shape[:-1],
                *(1 if i in axis else x for i, x in enumerate(a.shape)),
            )
        else:
            shape = *y.shape[:-1], *a_moved.shape[: -len(destination)]

        y = y.reshape(shape)
        return y


def probability_distribution_mean_covariance(entries_list):
    """Mean normalized histogram and covariance of list of input histograms

    :param entries_list: numpy 2D array shape (n_histos, n_bins,) with bin counts of histograms
    :return: mean normalized histogram, covariance probability matrix
    """
    if len(entries_list) == 0:
        raise ValueError("List of input histogram entries is empty.")

    entries_list = np.atleast_2d(entries_list)
    n_histos = entries_list.shape[0]

    if n_histos == 1:
        # catch potential empty histogram
        if np.sum(entries_list[0]) == 0:
            return entries_list[0], None
        norm_hist_mean = entries_list[0] / np.sum(entries_list[0])
        return norm_hist_mean, None

    # At least two histograms from here on ...
    # Normalize the histograms along the bin axis, so that histograms with different number of entries
    # are still comparable
    normed_list = entries_list / (
        np.sum(entries_list, axis=1, dtype=float)[:, np.newaxis] + np.finfo(float).eps
    )

    # Determine the mean histogram (unbiased)
    norm_hist_mean = np.sum(normed_list, axis=0) / n_histos

    # For each histogram determine the second moment (i.e. mean of the product of two bins entries)
    # of all the other histograms
    cross_entries = normed_list[:, :, np.newaxis] * normed_list[:, np.newaxis, :]
    sum2_cross_entries = np.sum(cross_entries, axis=0) / n_histos

    # Determine the unbiased covariance matrices between bins for all the histograms.
    # note: use one degree of freedom less because of we're using the evaluated mean as input
    norm_hist_cov = (
        sum2_cross_entries
        - norm_hist_mean[:, np.newaxis] * norm_hist_mean[np.newaxis, :]
    ) * (n_histos / (n_histos - 1))

    return norm_hist_mean, norm_hist_cov


def covariance_multinomial_probability_distribution(entries):
    """Calculate covariance matrix of a single multinomial probability distribution

    :param entries: entries of input histogram
    :return: numpy 2D array with covariance matrix of multinomial probability distribution
    """
    n_bins = len(entries)
    n_entries = np.sum(entries)
    prob = entries / n_entries

    covariance_matrix = np.zeros((n_bins, n_bins))

    for i in range(n_bins):
        for j in range(i, n_bins):
            if i == j:
                covariance_matrix[i][j] = (prob[i] * (1 - prob[i])) / n_entries
            else:
                covariance_matrix[i][j] = -(prob[i] * prob[j]) / n_entries
                covariance_matrix[j][i] = covariance_matrix[i][j]

    return covariance_matrix


def mad(a, c=0.6745, axis: int = 0):
    """Median Absolute Deviation along given axis of an array

    mad = median(abs(a - median(a)))/c

    Copyright statsmodels:
    Kindly taken from statsmodels package and then modified to work with dataframes as well.
    Reference: https://www.statsmodels.org/dev/_modules/statsmodels/robust/scale.html#mad
    License: https://github.com/statsmodels/statsmodels/blob/master/LICENSE.txt
    All modifications copyright INGA WB.

    :param a: array_like Input array.
    :param float c: optional. The normalization constant. Defined as scipy.stats.norm.ppf(3/4.),
        which is approximately .6745.
    :param int axis: optional. The default is 0. Can also be None.
    :param center: callable or float. If a callable is provided, such as the default `np.median` then it
        is expected to be called center(a). The axis argument will be applied
        via np.apply_over_axes. Otherwise, provide a float.
    :return: mad
    :rtype: float
    """
    if isinstance(a, pd.DataFrame):
        a = a.select_dtypes([np.number]).dropna(axis=1, how="all")

    center = a.median(axis=axis)
    rel_abs_diff = (a - center).abs() / c
    mad = rel_abs_diff.median(axis=axis)
    return mad
