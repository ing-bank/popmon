# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import logging

from popmon import resources

from ..base import Pipeline
from ..config import config
from ..io import JsonReader
from ..pipeline.report_pipelines import self_reference


def run():
    """ Example that run self-reference pipeline and produces monitoring report
    """
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
