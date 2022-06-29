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


def extension():
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
    def diptest_profile(bin_centers, bin_values):
        # unpack histogram into ordered samples
        sample = np.repeat(bin_centers, bin_values.astype(int))

        # compute diptest
        dip, pval = diptest(sample, sort_x=False)
        return dip, pval


class Diptest(Extension):
    name = "diptest"
    requirements = ["diptest"]
    extension = extension
