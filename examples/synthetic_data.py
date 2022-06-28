import pandas as pd

import popmon  # noqa
from popmon import resources

# open synthetic data
df = pd.read_csv(resources.data("test.csv.gz"), parse_dates=["date"])

# generate stability report using automatic binning of all encountered features
# (importing popmon automatically adds this functionality to a dataframe)
report = df.pm_stability_report(
    time_axis="date",
    time_width="2w",
    features=["date:age", "date:gender", "date:isActive", "date:eyeColor"],
)

# or save the report to file
report.to_file("test_data_report.html")
