"""
Example configuration for the SEA dataset
"""
from synthetic_data_streams import (
    dataset_summary,
    load_artff,
    synthetic_data_stream_report,
)

dataset_name = "sea"

# Generate report with each feature (equivalent to [f"index:{feature}" for feature in df.columns])
# features = []

# Monitor interactions of each feature with the class variable
# features = ["index:at1:cl", "index:at2:cl", "index:at3:cl"]

# From the interactions, we see that only the first two features are relevant. We can monitor their interaction
#  with the class variable in higher-dimension
features = ["index:at1:at2:cl"]

dataset_file = f"data/{dataset_name}.arff"
report_file = f"reports/{dataset_name}.html"

df = load_artff(dataset_file)

dataset_summary(df)

# 50.000 samples, then 2500 time-width results in 20 batches
synthetic_data_stream_report(df, features, report_file, time_width=2500)
