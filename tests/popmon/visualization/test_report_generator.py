import pandas as pd
import pytest
from popmon.base import Pipeline
from popmon.io import JsonReader
from popmon.hist.hist_splitter import HistSplitter
from popmon.analysis.comparison.hist_comparer import ReferenceHistComparer
from popmon.visualization import SectionGenerator, ReportGenerator
from popmon import resources


@pytest.mark.filterwarnings("ignore:An input array is constant")
@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
@pytest.mark.filterwarnings("ignore:All-NaN slice encountered")
def test_report_generator():

    hist_list = ['date:country', 'date:bankrupt', 'date:num_employees', 'date:A_score']
    features = ['country', 'bankrupt', 'num_employees', 'A_score']

    pipeline = Pipeline(modules=[
        JsonReader(file_path=resources.data("example_histogram.json"), store_key="example_hist"),
        HistSplitter(read_key='example_hist', store_key='output_hist', features=hist_list),
        ReferenceHistComparer(reference_key='output_hist', assign_to_key='output_hist', store_key='comparison'),
        SectionGenerator(read_key="comparison", store_key="all_sections", section_name="Comparisons", last_n=2),
        ReportGenerator(read_key="all_sections", store_key="final_report")
    ])
    datastore = pipeline.transform(datastore={})

    assert 'comparison' in datastore and isinstance(datastore['comparison'], dict)
    assert len(datastore['comparison'].keys()) == len(features)
    for f in features:
        assert f in datastore['comparison']
    for f in features:
        assert isinstance(datastore['comparison'][f], pd.DataFrame)

    assert pipeline.modules[-2].last_n == 2
    assert 'final_report' in datastore
    assert isinstance(datastore['final_report'], str) and len(datastore['final_report']) > 0
