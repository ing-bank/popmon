=========
Tutorials
=========

The available tutorials are:

* A basic tutorial: this covers generating a stability report from a DataFrame, as well as various simple configurations.
* A detailed example tutorial: this shows how to generating a stability report from a DataFrame (either pandas or Spark), as well as various configurations, create your own custom popmon pipeline, allowing you much more extensive control over which comparisons are made and how they end up in the report.
* Incremental data: a tutorial showing how to stitch the histograms of incremental (batch-wise) datasets and to generate reports from those.
* Reports: a tutorial on the interpretation of individual plots in the report.

You can see these notebooks here:

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
