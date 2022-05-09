import pickle
from pathlib import Path

import pandas as pd
from elastic_connector import ElasticConnector

import popmon  # noqa
from popmon import resources

if __name__ == "__main__":
    # Start the Elasticsearch connector
    es_obj = ElasticConnector()

    # Create an index to store the data
    es_obj.create_index("popmon-integration")

    # Path to cache the popmon report
    cache_file = Path("example-metrics.pkl")

    # Compute if not cached
    if not cache_file.exists():
        # open synthetic data
        df = pd.read_csv(
            resources.data("flight_delays.csv.gz"), index_col=0, parse_dates=["DATE"]
        )

        # generate stability metrics using automatic binning of all encountered features
        # (importing popmon automatically adds this functionality to a dataframe)
        metrics = df.pm_stability_metrics(
            time_axis="DATE",
            time_width="1w",
            time_offset="2015-07-02",
            pull_rules={"*_pull": [10, 7, -7, -10]},
        )

        with cache_file.open("wb") as f:
            pickle.dump(metrics, f)

    # Load report from cache
    with cache_file.open("rb") as f:
        metrics = pickle.load(f)

    # Push individual histograms to ES per feature
    for feature, data in metrics["split_hists"].items():
        messages = []
        row = data.iloc[0]
        for histogram_idx, (idx, row) in enumerate(data.iterrows()):
            # Labels for Categorical/Numerical features
            if type(row["histogram"]).__name__ == "CategorizeHistogramMethods":
                labels = row["histogram"].bin_labels()
            else:
                labels = row["histogram"].bin_centers()

            for entry_id, (key, value) in enumerate(
                zip(labels, row["histogram"].bin_entries())
            ):
                message = {
                    "_id": f"{feature}-{histogram_idx}-{entry_id}",
                    "doc_type": "histogram",
                    "message": value,
                    "timestamp": idx.strftime("%Y-%m-%d %H:%I:%S"),
                    "type": key,
                    "feature": feature,
                }
                messages.append(message)
        es_obj.push_bulk_to_index("popmon-integration", messages)

    # Save profiles, comparisons, traffic lights and alerts to the index
    # (bounds and beyond are up for contributions)

    # Please note that the data types of the values are not yet considered.
    # This causes the exceptions 'Exception is :: X document(s) failed to index.' Cases where this generates issues
    # is for mixtures between float/text (e.g. NaN) and fields such as 'most probable value' that contains float/text
    # depending on the feature.
    # (see also https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html)
    for doc_type in ["profiles", "comparisons", "traffic_lights", "alerts"]:
        for feature, data in metrics[doc_type].items():
            columns = data.columns
            messages = [
                {
                    "_id": f"{feature}-{column}-{entry_id}",
                    "doc_type": doc_type,
                    "message": str(row[column]),
                    "timestamp": idx.strftime("%Y-%m-%d %H:%I:%S"),
                    "type": column,
                    "feature": feature,
                }
                for entry_id, (idx, row) in enumerate(data.iterrows())
                for column in columns
            ]
            es_obj.push_bulk_to_index("popmon-integration", messages)
