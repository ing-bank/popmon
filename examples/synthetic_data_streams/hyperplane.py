"""
Example configuration for the hyperplane dataset
"""
from synthetic_data_streams import (
    dataset_summary,
    load_artff,
    synthetic_data_stream_report,
)

dataset_name = "hyperplane"
v = "1"

# Monitor the each feature w.r.t. the label
features = [f"index:attr{i}:output" for i in range(10)]

dataset_file = f"data/{dataset_name}{v}.arff"
report_file = f"reports/{dataset_name}_{v}.html"

df = load_artff(dataset_file)

dataset_summary(df)

# Reduce the time_width for this smaller dataset
synthetic_data_stream_report(df, features, report_file, time_width=500)
