import numpy as np

from popmon.visualization.histogram_section import get_top_categories


def test_get_top_categories():
    entries_list = np.array([[1, 2], [4, 3], [1, 3], [3, 3]])
    bins = ["cat1", "cat2", "cat3", "cat4"]
    top_lim = 2

    e0, b0 = get_top_categories(entries_list, bins, top_lim)

    e1 = np.array([[3, 3], [4, 3], [2, 5]])
    b1 = ["cat4", "cat2", "Others"]

    np.testing.assert_array_equal(e0, e1)
    np.testing.assert_array_equal(b0, b1)
