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


"""Hartigan & Hartigan's dip test for unimodality

How to enable this extension:
    - Install te diptest package: `pip install diptest` or `pip install popmon[diptest]`
    - To show the diptest values in your report:
        settings.report.show_stats.append("diptest*")
        OR
        settings.report.extended_report = True

"""
import numpy as np

from popmon.analysis import Profiles
from popmon.extensions.extension import Extension


def extension() -> None:
    from diptest import diptest

    @Profiles.register(
        key=["diptest_value", "diptest_pvalue"],
        description=[
            "diptest value for Hartigan & Hartigan's test for unimodality",
            "p-value for the diptest",
        ],
        dim=1,
        htype="num",
    )
    def diptest_profile(bin_centers, bin_values, bin_width, rng=None):
        if rng is None:
            rng = np.random.default_rng()

        counts = bin_values.astype(int)
        n = counts.sum()
        hbw = bin_width / 2

        # unpack histogram into ordered samples
        sample = np.repeat(bin_centers, counts)

        # uniform noise
        sample_noise = sample + rng.uniform(-hbw, hbw, n)

        # compute diptest
        dip, pval = diptest(sample_noise)
        return dip, pval


class Diptest(Extension):
    name = "diptest"
    extension = extension
