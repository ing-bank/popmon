import pandas as pd
from scipy.io.arff import loadarff

import popmon
from popmon import Settings


def load_arff(name) -> pd.DataFrame:
    """Load Arff file and decode string values"""
    raw_data = loadarff(name)
    df = pd.DataFrame(raw_data[0])
    object_idx = df.select_dtypes([object]).columns
    df[object_idx] = df[object_idx].stack().str.decode("utf-8").unstack()
    return df


def dataset_summary(df):
    print(f"shape={df.shape}, columns={df.columns.values}")
    print("Sample of the data")
    print(df.head(10))


def synthetic_data_stream_report(
    data, features, report_file, time_width=1000, **kwargs
):
    data["index"] = data.index.values
    # generate stability report using automatic binning of all encountered features
    # (importing popmon automatically adds this functionality to a dataframe)
    settings = Settings(features=features, time_axis="index")
    settings.monitoring.pull_rules = {"*_pull": [7, 4, -4, -7]}
    settings.monitoring.monitoring_rules = {
        "*_pull": [7, 4, -4, -7],
        "*_zscore": [7, 4, -4, -7],
        "[!p]*_unknown_labels": [0.5, 0.5, 0, 0],
    }

    report = popmon.df_stability_report(
        data,
        time_width=time_width,
        time_offset=0,
        settings=settings,
        **kwargs,
    )

    # or save the report to file
    report.to_file(report_file)
