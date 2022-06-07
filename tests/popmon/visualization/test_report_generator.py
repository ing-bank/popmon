import pandas as pd
import pytest

from popmon import resources
from popmon.analysis.comparison.hist_comparer import ReferenceHistComparer
from popmon.base import Pipeline
from popmon.config import Settings
from popmon.hist.hist_splitter import HistSplitter
from popmon.io import JsonReader
from popmon.visualization import ReportGenerator, SectionGenerator


@pytest.mark.filterwarnings("ignore:An input array is constant")
@pytest.mark.filterwarnings("ignore:invalid value encountered in true_divide")
@pytest.mark.filterwarnings("ignore:All-NaN slice encountered")
def test_report_generator():

    hist_list = ["date:country", "date:bankrupt", "date:num_employees", "date:A_score"]
    features = ["country", "bankrupt", "num_employees", "A_score"]

    settings = Settings()

    pipeline = Pipeline(
        modules=[
            JsonReader(
                file_path=resources.data("example_histogram.json"),
                store_key="example_hist",
            ),
            HistSplitter(
                read_key="example_hist", store_key="output_hist", features=hist_list
            ),
            ReferenceHistComparer(
                reference_key="output_hist",
                assign_to_key="output_hist",
                store_key="comparison",
            ),
            SectionGenerator(
                read_key="comparison",
                store_key="all_sections",
                section_name="Comparisons",
                settings=settings.report,
            ),
            ReportGenerator(
                read_key="all_sections",
                store_key="final_report",
                settings=settings.report,
            ),
        ]
    )
    datastore = pipeline.transform(datastore={})

    assert "comparison" in datastore and isinstance(datastore["comparison"], dict)
    assert len(datastore["comparison"].keys()) == len(features)
    for f in features:
        assert f in datastore["comparison"]
    for f in features:
        assert isinstance(datastore["comparison"][f], pd.DataFrame)

    assert isinstance(pipeline.modules[-2], SectionGenerator)
    assert pipeline.modules[-2].last_n == 0
    assert "final_report" in datastore
    assert (
        isinstance(datastore["final_report"], str)
        and len(datastore["final_report"]) > 0
    )
