# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
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


import logging

from popmon import resources

from ..base import Pipeline
from ..config import config
from ..io import JsonReader
from ..pipeline.report_pipelines import self_reference


def run():
    """Example that run self-reference pipeline and produces monitoring report"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s]: %(message)s"
    )

    cfg = {
        **config,
        "histograms_path": resources.data("synthetic_histograms.json"),
        "hists_key": "hists",
        "ref_hists_key": "hists",
        "datetime_name": "date",
        "window": 20,
        "shift": 1,
        "monitoring_rules": {
            "*_pull": [7, 4, -4, -7],
            # "*_pvalue": [1, 0.999, 0.001, 0.0001],
            "*_zscore": [7, 4, -4, -7],
        },
        "pull_rules": {"*_pull": [7, 4, -4, -7]},
        "show_stats": config["limited_stats"],
    }

    pipeline = Pipeline(
        modules=[
            JsonReader(file_path=cfg["histograms_path"], store_key=cfg["hists_key"]),
            self_reference(**cfg),
            # fixed_reference(**config),
            # rolling_reference(**config),
            # expanding_reference(**config),
        ]
    )
    pipeline.transform(datastore={})


if __name__ == "__main__":
    run()
