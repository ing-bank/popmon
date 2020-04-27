from popmon.base import Pipeline
from popmon.io import JsonReader
from popmon.pipeline.report_pipelines import self_reference
from popmon.pipeline.report_pipelines import external_reference, rolling_reference, expanding_reference
from popmon import resources


def test_self_reference():
    hist_list = ['date:A_score', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="hists"),
        self_reference(hists_key='hists', features=hist_list),
    ])
    pipeline.transform(datastore={})


def test_external_reference():
    hist_list = ['date:country', 'date:bankrupt']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="hists"),
        external_reference(hists_key='hists', ref_hists_key='hists', features=hist_list),
    ])
    pipeline.transform(datastore={})


def test_rolling_reference():
    hist_list = ['date:country', 'date:A_score:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="hists"),
        rolling_reference(hists_key='hists', window=5, features=hist_list),
    ])
    pipeline.transform(datastore={})


def test_expanding_reference():
    hist_list = ['date:bankrupt', 'date:num_employees']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="hists"),
        expanding_reference(hists_key='hists', features=hist_list),
    ])
    pipeline.transform(datastore={})
