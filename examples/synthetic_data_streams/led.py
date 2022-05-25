"""
Example configuration for the LED dataset
"""
from synthetic_data_streams import (
    dataset_summary,
    load_arff,
    synthetic_data_stream_report,
)

dataset_name = "led_w_500_n_0.1"

# Stream (101-200)
v = "101"

# Obtained by running once with:
# features = []

# Most alerts are found in: a7,a6,a4,a22,a15,a12,a0
features = [
    # Monitor the each feature w.r.t. the label
    "index:a0:class",
    "index:a4:class",
    "index:a6:class",
    "index:a7:class",
    "index:a12:class",
    "index:a15:class",
    "index:a22:class",
    # the relevant interactions correspond to 2^7 (128) * number of classes entries (10) per time slice
    # "index:a0:a4:a6:a7:a12:a15:a22:class",
]


dataset_file = f"data/{dataset_name}/{dataset_name}_{v}.arff"
report_file = f"reports/{dataset_name}_{v}.html"

df = load_arff(dataset_file)

dataset_summary(df)

# Reduce the time_width for this smaller dataset
synthetic_data_stream_report(df, features, report_file, time_width=1000)
