import pandas as pd
from popmon.analysis.merge_statistics import MergeStatistics


def test_merge_statistics():
    df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                        'B': ['B0', 'B1', 'B2', 'B3'],
                        'C': ['C0', 'C1', 'C2', 'C3'],
                        'D': ['D0', 'D1', 'D2', 'D3']},
                       index=[0, 1, 2, 3])

    df2 = pd.DataFrame({'B': ['B2_', 'B3_', 'B6_', 'B7_'],
                        'D': ['D2_', 'D3_', 'D6_', 'D7_'],
                        'F': ['F2', 'F3', 'F6', 'F7']},
                       index=[2, 3, 6, 7])

    out = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3', None, None],
                        'B': ['B0', 'B1', 'B2', 'B3', 'B6_', 'B7_'],
                        'C': ['C0', 'C1', 'C2', 'C3', None, None],
                        'D': ['D0', 'D1', 'D2', 'D3', 'D6_', 'D7_'],
                        'F': [None, None, 'F2', 'F3', 'F6', 'F7']},
                       index=[0, 1, 2, 3, 6, 7])

    datastore = {
        "first_df": {
            "feature_1": df1,
            "feature_2": df2,
            "feature_3": df1
        },
        "second_df": {
            "feature_1": df2,
            "feature_3": None
        }
    }
    datastore = MergeStatistics(read_keys=["first_df", "second_df"], store_key="output_df").transform(datastore)

    pd.testing.assert_frame_equal(df1.combine_first(df2), out)
    pd.testing.assert_frame_equal(datastore["output_df"]["feature_1"], out)
    pd.testing.assert_frame_equal(datastore["output_df"]["feature_2"], df2)
    pd.testing.assert_frame_equal(datastore["output_df"]["feature_3"], df1)
