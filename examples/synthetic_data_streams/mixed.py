"""
Example configuration for the mixed dataset
"""
from synthetic_data_streams import (
    dataset_summary,
    load_artff,
    synthetic_data_stream_report,
)

dataset_name = "mixed_w_50_n_0.1"

# Stream (101-200)
v = "101"

# Monitor the feature distribution (equivalent to features=[])
# features = ["index:v", "index:w", "index:x", "index:y", "index:class"]

# Monitor the each feature w.r.t. the label
features = ["index:v:class", "index:w:class", "index:x:class", "index:y:class"]

dataset_file = f"data/{dataset_name}/{dataset_name}_{v}.arff"
report_file = f"reports/{dataset_name}_{v}.html"

df = load_artff(dataset_file)

dataset_summary(df)

# Reduce the time_width for this smaller dataset
synthetic_data_stream_report(df, features, report_file, time_width=1000)
