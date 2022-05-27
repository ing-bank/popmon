"""
Example configuration for the hyperplane dataset
"""
from sklearn.linear_model import LogisticRegression
from synthetic_data_streams import (
    dataset_summary,
    load_arff,
    synthetic_data_stream_report,
)

dataset_name = "hyperplane"
v = "1"

# Monitor the each feature w.r.t. the label
features = [f"index:attr{i}:output" for i in range(10)]

# Also monitor predictions w.r.t. the label (see below)
features += ["index:prediction:output"]

dataset_file = f"data/{dataset_name}{v}.arff"
report_file = f"reports/{dataset_name}_{v}.html"

df = load_arff(dataset_file)

# Fit a logistic regression on the first 10% of the data.
model = LogisticRegression(C=1e5)
model.fit(df.loc[:1000, df.columns != "output"], df.loc[:1000, "output"])

# Use the model to predict over the full dataset
df["prediction"] = model.predict_proba(df.loc[:, df.columns != "output"])[:, 1]

dataset_summary(df)

# The training set for the model will be used as reference.
# The reduced time_width is because this is a smaller dataset compared to the rest
synthetic_data_stream_report(
    df, features, report_file, time_width=500, reference="start", split=1000
)
