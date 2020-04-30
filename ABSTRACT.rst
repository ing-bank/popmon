Population Shift Monitoring
===========================

Tracking model performance is crucial to guarantee that a model behaves as designed initially.
Predictions may be ahead in time, so true performance can only be verified later, for example
one year ahead. Typical model performance questions are: is the model performing as expected,
and are predictions made on current incoming data still valid?

Model performance depends directly on the data used for training and the data to make predictions.
Changes in the latter can affect the performance and make predictions unreliable. Given that input
data often change over time, it is important to track changes in both input distributions and
delivered predictions, and to act on them when significantly different - for example to diagnose
and retrain an incorrect model in production.

To make monitoring both more consistent and semi-automatic, at ING we have developed a generic
Python package called `popmon` to monitor the stability of data populations over time, using
techniques from statistical process control.

`popmon` works with both pandas and spark datasets. `popmon` creates histograms of features binned
in time-slices, and compares the stability of the profiles and distributions of those histograms
using statistical tests, both over time and with respect to a reference. It works with numerical,
ordinal, categorical features, and the histograms can be higher-dimensional, e.g. it can also track
correlations between any two features. `popmon` can automatically flag and alert on changes observed
over time, such as trends, shifts, peaks, outliers, anomalies, changing correlations, etc, using
monitoring business rules.
