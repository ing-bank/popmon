======================
Package tree structure
======================


.. code-block:: text

    .
    ├── alerting/
    │   ├── alerts_summary.py
    │   └── compute_tl_bounds.py
    ├── analysis/
    │   ├── apply_func.py
    │   ├── comparison/
    │   │   └── hist_comparer.py
    │   ├── functions.py
    │   ├── hist_numpy.py
    │   ├── merge_statistics.py
    │   └── profiling/
    │       ├── hist_profiler.py
    │       └── pull_calculator.py
    ├── base/
    │   ├── module.py
    │   └── pipeline.py
    ├── config.py
    ├── decorators/
    │   ├── pandas.py
    │   └── spark.py
    ├── hist/
    │   ├── filling/
    │   │   ├── histogram_filler_base.py
    │   │   ├── make_histograms.py
    │   │   ├── numpy_histogrammar.py
    │   │   ├── pandas_histogrammar.py
    │   │   ├── spark_histogrammar.py
    │   │   └── utils.py
    │   ├── hist_splitter.py
    │   ├── histogram.py
    │   └── patched_histogrammer.py
    ├── io/
    │   ├── file_reader.py
    │   ├── file_writer.py
    │   └── json_reader.py
    ├── notebooks/
    │   ├── flight_delays.csv.gz
    │   ├── flight_delays_reference.csv.gz
    │   ├── popmon_tutorial_advanced.ipynb
    │   ├── popmon_tutorial_basic.ipynb
    │   └── popmon_tutorial_incremental_data.ipynb
    ├── pipeline/
    │   ├── amazing_pipeline.py
    │   ├── metrics.py
    │   ├── metrics_pipelines.py
    │   ├── report.py
    │   └── report_pipelines.py
    ├── resources.py
    ├── stats/
    │   └── numpy.py
    ├── stitching/
    │   └── hist_stitcher.py
    ├── test_data/
    │   ├── data_generator_hists.json.gz
    │   ├── example.json
    │   ├── example_histogram.json
    │   ├── synthetic_histograms.json
    │   └── test.csv.gz
    ├── version.py
    └── visualization/
        ├── backend.py
        ├── histogram_section.py
        ├── report_generator.py
        ├── section_generator.py
        ├── templates/
        │   ├── assets/
        │   │   ├── css/
        │   │   │   ├── bootstrap.min.css
        │   │   │   └── custom-style.css
        │   │   └── js/
        │   │       ├── bootstrap.bundle.min.js
        │   │       ├── custom-script.js
        │   │       ├── jquery.easing.min.js
        │   │       ├── jquery.min.js
        │   │       └── scrolling-nav.js
        │   ├── card.html
        │   ├── core.html
        │   ├── footer.html
        │   ├── header.html
        │   ├── modal-popup.html
        │   ├── notebook_iframe.html
        │   └── section.html
        └── utils.py
