from popmon import resources
from popmon.base import Pipeline
from popmon.io import JsonReader
from popmon.pipeline.report_pipelines import (
    ExpandingReference,
    ExternalReference,
    RollingReference,
    SelfReference,
)


def test_self_reference():
    hist_list = ["date:A_score", "date:A_score:num_employees"]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"), store_key="hists"
            ),
            SelfReference(hists_key="hists", features=hist_list),
        ]
    )
    pipeline.transform(datastore={})


def test_external_reference():
    hist_list = ["date:country", "date:bankrupt"]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"), store_key="hists"
            ),
            ExternalReference(
                hists_key="hists",
                ref_hists_key="hists",
                features=hist_list,
            ),
        ]
    )
    pipeline.transform(datastore={})


def test_rolling_reference():
    hist_list = ["date:country", "date:A_score:num_employees"]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"), store_key="hists"
            ),
            RollingReference(
                hists_key="hists",
                window=5,
                features=hist_list,
            ),
        ]
    )
    pipeline.transform(datastore={})


def test_expanding_reference():
    hist_list = ["date:bankrupt", "date:num_employees"]

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"), store_key="hists"
            ),
            ExpandingReference(hists_key="hists", features=hist_list),
        ]
    )
    pipeline.transform(datastore={})
