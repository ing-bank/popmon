"""
Example configuration for the circles dataset
"""
from synthetic_data_streams import (
    dataset_summary,
    load_arff,
    synthetic_data_stream_report,
)

dataset_name = "circles_w_500_n_0.1"

# Stream (101-200)
v = "101"

# Monitor the each feature w.r.t. the label
features = ["index:x:class", "index:y:class", "index:x:y:class"]

dataset_file = f"data/{dataset_name}/{dataset_name}_{v}.arff"
report_file = f"reports/{dataset_name}_{v}.html"

df = load_arff(dataset_file)

dataset_summary(df)

# Reduce the time_width for this smaller dataset
synthetic_data_stream_report(df, features, report_file, time_width=1000)
