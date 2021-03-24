import pandas as pd

import popmon
from popmon import resources

# open synthetic data
df = pd.read_csv(
    resources.data("flight_delays.csv.gz"), index_col=0, parse_dates=["DATE"]
)

# generate stability report using automatic binning of all encountered features
# (importing popmon automatically adds this functionality to a dataframe)
report = df.pm_stability_report(
    time_axis="DATE",
    time_width="1w",
    time_offset="2015-07-02",
    extended_report=False,
    pull_rules={"*_pull": [10, 7, -7, -10]},
)

# or save the report to file
report.to_file("flight_delays_report.html")
