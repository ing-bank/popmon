===========================
Population Shift Monitoring
===========================

|build| |docs| |release| |release_date| |downloads|

|logo|

`popmon` is a package that allows one to check the stability of a dataset.
`popmon` works with both **pandas** and **spark datasets**.

`popmon` creates histograms of features binned in time-slices,
and compares the stability of the `profiles <https://popmon.readthedocs.io/en/latest/profiles.html>`_ and distributions of
those histograms using `statistical tests <https://popmon.readthedocs.io/en/latest/comparisons.html>`_, both over time and with respect to a reference.
It works with numerical, ordinal, categorical features, and the histograms can be higher-dimensional, e.g. it can also track correlations between any two features.
`popmon` can **automatically flag** and alert on **changes observed over time**, such
as trends, shifts, peaks, outliers, anomalies, changing correlations, etc,
using monitoring business rules.

|example|

Announcements
=============

Spark 3.0
---------

With Spark 3.0, based on Scala 2.12, make sure to pick up the correct `histogrammar` jar files:

.. code-block:: python

  spark = SparkSession.builder.config(
      "spark.jars.packages",
      "io.github.histogrammar:histogrammar_2.12:1.0.20,io.github.histogrammar:histogrammar-sparksql_2.12:1.0.20",
  ).getOrCreate()

For Spark 2.X compiled against scala 2.11, in the string above simply replace 2.12 with 2.11.

Examples
========

- `Flight Delays and Cancellations Kaggle data <https://crclz.com/popmon/reports/flight_delays_report.html>`_
- `Synthetic data (code example below) <https://crclz.com/popmon/reports/test_data_report.html>`_

Documentation
=============

The entire `popmon` documentation including tutorials can be found at `read-the-docs <https://popmon.readthedocs.io>`_.


Notebooks
=========

.. list-table::
   :widths: 80 20
   :header-rows: 1

   * - Tutorial
     - Colab link
   * - `Basic tutorial <https://nbviewer.jupyter.org/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_basic.ipynb>`_
     - |notebook_basic_colab|
   * - `Detailed example (featuring configuration, Apache Spark and more) <https://nbviewer.jupyter.org/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_advanced.ipynb>`_
     - |notebook_advanced_colab|
   * - `Incremental datasets (online analysis) <https://nbviewer.jupyter.org/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_incremental_data.ipynb>`_
     - |notebook_incremental_data_colab|
   * - `Report interpretation (step-by-step guide) <https://nbviewer.jupyter.org/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_reports.ipynb>`_
     - |notebook_reports_colab|

Check it out
============

The `popmon` library requires Python 3.6+ and is pip friendly. To get started, simply do:

.. code-block:: bash

  $ pip install popmon

or check out the code from our GitHub repository:

.. code-block:: bash

  $ git clone https://github.com/ing-bank/popmon.git
  $ pip install -e popmon

where in this example the code is installed in edit mode (option -e).

You can now use the package in Python with:

.. code-block:: python

  import popmon

**Congratulations, you are now ready to use the popmon library!**

Quick run
=========

As a quick example, you can do:

.. code-block:: python

  import pandas as pd
  import popmon
  from popmon import resources

  # open synthetic data
  df = pd.read_csv(resources.data("test.csv.gz"), parse_dates=["date"])
  df.head()

  # generate stability report using automatic binning of all encountered features
  # (importing popmon automatically adds this functionality to a dataframe)
  report = df.pm_stability_report(time_axis="date", features=["date:age", "date:gender"])

  # to show the output of the report in a Jupyter notebook you can simply run:
  report

  # or save the report to file
  report.to_file("monitoring_report.html")

To specify your own binning specifications and features you want to report on, you do:

.. code-block:: python

  # time-axis specifications alone; all other features are auto-binned.
  report = df.pm_stability_report(
      time_axis="date", time_width="1w", time_offset="2020-1-6"
  )

  # histogram selections. Here 'date' is the first axis of each histogram.
  features = [
      "date:isActive",
      "date:age",
      "date:eyeColor",
      "date:gender",
      "date:latitude",
      "date:longitude",
      "date:isActive:age",
  ]

  # Specify your own binning specifications for individual features or combinations thereof.
  # This bin specification uses open-ended ("sparse") histograms; unspecified features get
  # auto-binned. The time-axis binning, when specified here, needs to be in nanoseconds.
  bin_specs = {
      "longitude": {"bin_width": 5.0, "bin_offset": 0.0},
      "latitude": {"bin_width": 5.0, "bin_offset": 0.0},
      "age": {"bin_width": 10.0, "bin_offset": 0.0},
      "date": {
          "bin_width": pd.Timedelta("4w").value,
          "bin_offset": pd.Timestamp("2015-1-1").value,
      },
  }

  # generate stability report
  report = df.pm_stability_report(features=features, bin_specs=bin_specs, time_axis=True)

These examples also work with spark dataframes.
You can see the output of such example notebook code `here <https://crclz.com/popmon/reports/test_data_report.html>`_.
For all available examples, please see the `tutorials <https://popmon.readthedocs.io/en/latest/tutorials.html>`_ at read-the-docs.

Pipelines for monitoring dataset shift
======================================
Advanced users can leverage popmon's modular data pipeline to customize their workflow.
Visualization of the pipeline can be useful when debugging, or for didactic purposes.
There is a `script <https://github.com/ing-bank/popmon/tree/master/tools/>`_ included with the package that you can use.
The plotting is configurable, and depending on the options you will obtain a result that can be used for understanding the data flow, the high-level components and the (re)use of datasets.

|pipeline|

*Example pipeline visualization (click to enlarge)*

Reports and integrations
========================
The data shift computations that popmon performs, are by default displayed in a self-contained HTML report.
This format is favourable in many real-world environments, where access may be restricted.
Moreover, reports can be easily shared with others.

Access to the datastore means that its possible to integrate popmon in almost any workflow.
To give an example, one could store the histogram data in a PostgreSQL database and load that from Grafana and benefit from their visualisation and alert handling features (e.g. send an email or slack message upon alert).
This may be interesting to teams that are already invested in particular choice of dashboarding tool.

Possible integrations are:

+----------------+---------------+
| |grafana_logo| | |kibana_logo| |
+----------------+---------------+
| Grafana        | Kibana        |
+----------------+---------------+

Resources on how to integrate popmon are available in the `examples directory <https://github.com/ing-bank/popmon/tree/master/examples/integrations>`_.
Contributions of additional or improved integrations are welcome!

.. |grafana_logo| image:: https://upload.wikimedia.org/wikipedia/en/a/a1/Grafana_logo.svg
    :alt: Grafana logo
    :height: 120
    :target: https://github.com/grafana/grafana

.. |kibana_logo| image:: https://miro.medium.com/max/1400/1*HW_x9ZvIbUkyaqHstsB1ig.png
    :alt: Kibana logo
    :height: 120
    :target: https://github.com/elastic/kibana



Resources
=========

Presentations
-------------

+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| Title                                                                                          | Host                                                                                             | Date              | Speaker                 |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| Popmon - population monitoring made easy                                                       | `Big Data Technology Warsaw Summit 2021 <https://bigdatatechwarsaw.eu/>`_                        | February 25, 2021 | Simon Brugman           |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| Popmon - population monitoring made easy                                                       | `Data Lunch @ Eneco <https://www.eneco.nl/>`_                                                    | October 29, 2020  | Max Baak, Simon Brugman |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| Popmon - population monitoring made easy                                                       | `Data Science Summit 2020 <https://dssconf.pl/en/>`_                                             | October 16, 2020  | Max Baak                |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| `Population Shift Monitoring Made Easy: the popmon package <https://youtu.be/PgaQpxzT_0g>`_    | `Online Data Science Meetup @ ING WBAA <https://www.meetup.com/nl-NL/Tech-Meetups-ING/events/>`_ | July 8 2020       | Tomas Sostak            |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| `Popmon: Population Shift Monitoring Made Easy <https://www.youtube.com/watch?v=HE-3YeVYqPY>`_ | `PyData Fest Amsterdam 2020 <https://amsterdam.pydata.org/>`_                                    | June 16, 2020     | Tomas Sostak            |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+
| Popmon: Population Shift Monitoring Made Easy                                                  | `Amundsen Community Meetup <https://github.com/amundsen-io/amundsen>`_                           | June 4, 2020      | Max Baak                |
+------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------+-------------------+-------------------------+


Articles
--------

+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
| Title                                                                                                                                                                                             | Date             | Author                                      |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
|`Monitoring Model Drift with Python <https://medium.com/broadhorizon-cmotions/monitoring-model-drift-with-python-b9e15ca16b18>`_                                                                   | April 16, 2022   | Jeanine Schoonemann                         |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
|`The Statistics Underlying the Popmon Hood <https://www.theanalyticslab.nl/the-statistics-underlying-the-popmon-hood/>`_                                                                           | April 15, 2022   | Jurriaan Nagelkerke and Jeanine Schoonemann |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
|`popmon: code breakfast session <https://simonbrugman.nl/2021/11/09/popmon-code-breakfast.html>`_                                                                                                  | November 9, 2022 | Simon Brugman                               |       
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
| `Population Shift Analysis: Monitoring Data Quality with Popmon <https://www.codemotion.com/magazine/dev-hub/big-data-analyst/popmon-data-quality-monitoring/>`_                                  | May 21, 2021     | Vito Gentile                                |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+
| `Popmon Open Source Package — Population Shift Monitoring Made Easy <https://medium.com/wbaa/population-monitoring-open-source-1ce3139d8c3a>`_                                                    | May 20, 2020     | Nicole Mpozika                              |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+---------------------------------------------+

Software
--------

- `Kedro-popmon <https://github.com/stephanecollot/kedro-popmon>`_ is a plugin to integrate popmon reporting with kedro. This plugin allows you to automate the process of popmon feature and output stability monitoring. Package created by `Marian Dabrowski <https://www.linkedin.com/in/marian-dabrowski/>`_ and `Stephane Collot <https://github.com/stephanecollot/>`_.

Project contributors
====================

This package was authored by ING Wholesale Banking Advanced Analytics.
Special thanks to the following people who have contributed to the development of this package: `Ahmet Erdem <https://github.com/aerdem4>`_, `Fabian Jansen <https://github.com/faab5>`_, `Nanne Aben <https://github.com/nanne-aben>`_, Mathieu Grimal.

Contact and support
===================

* Issues & Ideas & Support: https://github.com/ing-bank/popmon/issues

Please note that ING WBAA provides support only on a best-effort basis.

License
=======
Copyright ING WBAA. `popmon` is completely free, open-source and licensed under the `MIT license <https://en.wikipedia.org/wiki/MIT_License>`_.

.. |logo| image:: https://raw.githubusercontent.com/ing-bank/popmon/master/docs/source/assets/popmon-logo.png
    :alt: POPMON logo
    :target: https://github.com/ing-bank/popmon
.. |example| image:: https://raw.githubusercontent.com/ing-bank/popmon/master/docs/source/assets/report_overview.png
    :alt: Traffic Light Overview
.. |pipeline| image:: https://raw.githubusercontent.com/ing-bank/popmon/master/docs/source/assets/pipeline.png
    :alt: Pipeline Visualization
    :target: https://github.com/ing-bank/popmon/files/7417124/pipeline_amazingpipeline_subgraphs_unversioned.pdf
.. |build| image:: https://github.com/ing-bank/popmon/workflows/build/badge.svg
    :alt: Build status
.. |docs| image:: https://readthedocs.org/projects/popmon/badge/?version=latest
    :alt: Package docs status
    :target: https://popmon.readthedocs.io
.. |release| image:: https://img.shields.io/github/v/release/ing-bank/popmon
    :alt: Latest GitHub release
    :target: https://github.com/ing-bank/popmon/releases
.. |release_date| image:: https://img.shields.io/github/release-date/ing-bank/popmon
    :alt: GitHub Release Date
    :target: https://github.com/ing-bank/popmon/releases

.. |notebook_basic_colab| image:: https://colab.research.google.com/assets/colab-badge.svg
    :alt: Open in Colab
    :target: https://colab.research.google.com/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_basic.ipynb
.. |notebook_advanced_colab| image:: https://colab.research.google.com/assets/colab-badge.svg
    :alt: Open in Colab
    :target: https://colab.research.google.com/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_advanced.ipynb
.. |notebook_incremental_data_colab| image:: https://colab.research.google.com/assets/colab-badge.svg
    :alt: Open in Colab
    :target: https://colab.research.google.com/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_incremental_data.ipynb
.. |notebook_reports_colab| image:: https://colab.research.google.com/assets/colab-badge.svg
    :alt: Open in Colab
    :target: https://colab.research.google.com/github/ing-bank/popmon/blob/master/popmon/notebooks/popmon_tutorial_reports.ipynb
.. |downloads| image:: https://pepy.tech/badge/popmon
    :alt: PyPi downloads
    :target: https://pepy.tech/project/popmon
