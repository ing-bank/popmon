import pandas as pd

import popmon
from popmon import Settings, resources

# open synthetic data
df = pd.read_csv(
    resources.data("flight_delays.csv.gz"), index_col=0, parse_dates=["DATE"]
)


# Configuration of the monitoring rules and report
settings = Settings()
settings.report.extended_report = False
settings.monitoring.pull_rules = {"*_pull": [10, 7, -7, -10]}

# generate stability report using automatic binning of all encountered features
# (importing popmon automatically adds this functionality to a dataframe)
report = popmon.df_stability_report(
    df,
    reference_type="self",
    time_axis="DATE",
    time_width="1w",
    time_offset="2015-07-02",
    settings=settings,
)

# or save the report to file
report.to_file("flight_delays_report.html")
