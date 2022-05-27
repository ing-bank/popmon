import pandas as pd
from scipy.io.arff import loadarff

import popmon


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
    data, features, report_file, time_width=1000, reference="full", **kwargs
):
    data["index"] = data.index.values

    if reference == "full":
        ref_df = data
        df = data
    elif reference == "start":
        # split at this point, by default time_width
        split = kwargs.get("split", time_width)
        ref_df = data[:split]
        df = data[split:]
    else:
        raise ValueError("reference type should be 'full' or 'start'.")

    hists_ref = popmon.make_histograms(
        ref_df,
        time_axis="index",
        time_width=time_width,
        features=features,
        time_offset=0,
    )
    ref_bin_specs = popmon.get_bin_specs(hists_ref)
    features = list(ref_bin_specs.keys())

    df["batch"] = df.index // time_width

    hists_list = [
        popmon.make_histograms(df_chunk, features=features, bin_specs=ref_bin_specs)
        for _, df_chunk in df.groupby("batch")
        if not df_chunk.empty
    ]

    hists = popmon.stitch_histograms(
        hists_list=hists_list,
        time_axis="index",
        time_bin_idx=sorted(set(df["batch"].values.tolist())),
    )

    # generate stability report using automatic binning of all encountered features
    # (importing popmon automatically adds this functionality to a dataframe)
    pull_rules = {"*_pull": [7, 4, -4, -7]}
    monitoring_rules = {
        "*_pull": [7, 4, -4, -7],
        "*_zscore": [7, 4, -4, -7],
        "[!p]*_unknown_labels": [0.5, 0.5, 0, 0],
    }
    report = popmon.stability_report(
        hists,
        pull_rules=pull_rules,
        monitoring_rules=monitoring_rules,
        reference_type="external",
        reference=hists_ref,
    )

    # or save the report to file
    report.to_file(report_file)
