===============
Report settings
===============

Some more details on stability report settings, in particular how to set:
the reference dataset, binning specifications, monitoring rules, and where to plot boundaries.


Reference types
---------------

When generating a report from a DataFrame, the reference type can be set with the option ``reference_type``,
in four different ways:

1. Using the DataFrame on which the stability report is built as a self-reference. (This is the default setting.)

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="self")

2. Using an external reference DataFrame or set of histograms:

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="external", reference=reference)

3. Using a rolling window as reference, by default the 10 preceding time slots:

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="rolling", window=10, shift=1)

4. Using an expanding window of all preceding time slots:

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="expanding", shift=1)


Binning specifications
----------------------

Without any specific binning specification provided, by default automatic binning is applied to numeric and timestamp
features. Binning specificaton is a dictionary used for specific rebinning of numeric or timestamp features.

To specify the time-axis binning alone, do:

.. code-block:: python

  report = df.pm_stability_report(time_axis='date', time_width='1w', time_offset='2020-1-6')

All other features (except for 'date') are auto-binned in this example.

To specify your own binning specifications for individual features or combinations of features, do:

.. code-block:: python

  # generate stability report with specific binning specifications
  report = df.pm_stability_report(bin_specs=your_bin_specs)

An example bin_specs dictionary is:

.. code-block:: python

    bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                 'y': {'num': 10, 'low': 0.0, 'high': 2.0},
                 'x:y': [{}, {'num': 5, 'low': 0.0, 'high': 1.0}],
                 'date': {'bin_width': pd.Timedelta('4w').value,
                          'bin_offset': pd.Timestamp('2015-1-1').value}}

In the bin specs for 'x:y', 'x' is not provided (here) and reverts to the 1-dim setting.
Any time-axis, when specified here ('date'), needs to be specified in nanoseconds. This takes precedence over
the input arguments ``time_width`` and ``time_offset``.

The 'bin_width', 'bin_offset' notation makes an open-ended histogram (for that feature) with given bin width
and offset. 'bin_offset' is the lower edge of the bin with internal index 0.

The notation 'num', 'low', 'high' gives a fixed range histogram from 'low' to 'high' with 'num'
number of bins.



Monitoring rules
----------------

The monitoring rules are used to generate traffic light alerts.

As indicated we use traffic lights to indicate where large deviations from the reference occur.
By default we determine the traffic lights as set as follows:

* Green traffic light: the value of interest is less than four standard deviations away from the reference.
* Yellow traffic light: the value of interest is between four and seven standard deviations away from the reference.
* Red traffic light: the value of interest is more than seven standard deviations away from the reference.

When generating a report, they can be provided as a dictionary:

.. code-block:: python

  # generate stability report with specific monitoring rules
  report = df.pm_stability_report(monitoring_rules=your_monitoring_rules)

When not provided, the default setting is:

.. code-block:: python

    monitoring_rules = {"*_pull": [7, 4, -4, -7],
                        "*_zscore": [7, 4, -4, -7],
                        "[!p]*_unknown_labels": [0.5, 0.5, 0, 0]}

Note that the (filename based) wildcards such as * apply to all statistic names matching that pattern.
For example, ``"*_pull"`` applies for all features to all statistics ending on "_pull". Same for ``"*_zscore"``.
For ``"[!p]*_unknown_labels"``, the rule is not applied to any statistic starting with the letter p.

Each monitoring rule always has 4 numbers, e.g. by default for each pull: [7, 4, -4, -7].

* The inner two numbers of the list correspond to the high and low boundaries of the yellow traffic light,
  so +4 and -4 in this example.
* The outer two numbers of the list correspond to the high and low boundaries of the red traffic light,
  so +7 and -7 in this example.

You can also specify rules for specific features and/or statistics by leaving out wildcards and putting the
feature name in front. This also works for a combinations of two features. E.g.

.. code-block:: python

    monitoring_rules = {"featureA:*_pull": [5, 3, -3, -5],
                        "featureA:featureB:*_pull": [6, 3, -3, -6],
                        "featureA:nan": [4, 1, 0, 0],
                        "*_pull": [7, 4, -4, -7],
                        "nan": [8, 1, 0, 0]}

In the case where multiple rules could apply for a feature's statistic, the most specific one gets applied.
So in case of the statistic "nan": "featureA:nan" is used for "featureA", and the other "nan" rule
for all other features.


Plotting of traffic light boundaries
------------------------------------

Where the red and yellow boundaries are shown in a plot of a feature's statistic can be set with the
``pull_rules`` option. Usually the same numbers are used here as for the monitoring rules, but this is
not necessary.

Note that, depending on the chosen reference data set, the reference mean and standard deviation can change
over time. The red and yellow boundaries used to assign traffic lights can therefore change over
time as well.

When generating a report, the ``pull_rules`` can be provided as a dictionary:

.. code-block:: python

  # generate stability report with specific monitoring rules
  report = df.pm_stability_report(pull_rules=your_pull_rules)

The default for `pull_rules` is:

.. code-block:: python

    pull_rules = {"*_pull": [7, 4, -4, -7]}

This means that the shown yellow boundaries are at -4, +4 standard deviations around the (reference) mean,
and the shown red boundaries are at -7, +7 standard deviations around the (reference) mean.

Note that the (filename based) wildcards such as * apply to all statistic names matching that pattern.
The same wild card logic applies as for the monitoring rules.


Just metrics, no report
-----------------------

When you're only interested in generating the metrics for the report, but not actually generate the report,
you can do the following:

.. code-block:: python

  # generate stability metrics but no report
  datastore = df.pm_stability_metrics()

This function has the exact same options as discussed in the sections above.

The datastore is a dictionary that contains all evaluated metrics displayed in the report.
For example, you will see the keys ``profiles``, ``comparisons``, ``traffic_lights`` and ``alerts``.

Each of these objects is in itself a dictionary that has as keys the features in the corresponding report-section,
and every key points to a pandas dataframe with the metrics of that feature over time.
