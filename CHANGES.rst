=============
Release notes
=============

Version 0.4.0, (16-04-2021)
---------------------------
Documentation:

* **docs**: include BDTWS presentation
* **docs**: clarify that ``time_axis`` should be date or numeric
* **docs**: initialize spark with both histogrammar jar files

Build system

* **build**: Migrate to version 1.0.25 of ``histogrammar``.
* **build**: update ``pyupgrade`` to v2.12.0
* **build**: update ``isort`` to 5.8.0
* **build**: update ``flake8`` to 3.9.0

Version 0.3.14, Feb 2021
------------------------
* Pin ``histogrammar`` version for backwards compatibility

Version 0.3.13, Feb 2021
------------------------
* ``Spark 3.0`` support (``histogrammar`` update) (#87)
* Improved documentation
* Few minor package improvements

Version 0.3.12, Jan 2021
------------------------
* Add proper check of matrix invertibility of covariance matrix in stats/numpy.py
* Add support for the Spark ``date`` type
* Install Spark on Github Actions to be able to include spark tests in our CI/CD pipeline
* Upgrade linting to use ``pre-commit`` (including ``pyupgrade`` for ``python3.6`` syntax upgrades)
* Add documentation on how to run popmon using ``spark`` on ``Google Colab`` (minimal example from scratch)

Version 0.3.11, Dec 2020
------------------------
Features:

* Traffic light overview (#62)

Documentation:

* Downloads badge readme
* List talks and articles in readme (#66)
* Add image to ``README.rst`` (#64)

Other improvements:

* Change notebook testing to pytest-notebook (previously these tests were skipped in CI). Add try-except ImportError for pyspark code. (#67)
* Fix a few typo's
* Suppress ``matplotlib backend`` verbose warning
* Click on "popmon report" also scrolls to top
* Update HTML reports using ``Github Actions`` (#63)
* Bugfix in ``hist.py`` that broke the advanced tutorial.

Notebooks:

* Add ``%%capture`` to pip install inside of notebooks.
* Make package install in notebooks work with paths with spaces.
* ``Pickle`` doesn't work with tests (not really a popmon-specific feature anyway). Changed the notebook to fix the issue, left the code for reference.

Version 0.3.10, Oct 2020
------------------------
* Traffic light overview
* Add image to ``README.rst``
* Add building of examples to Github Actions CI
* Format notebooks (``nbqa``)
* Remove ``matplotlib backend`` warning
* Fix navigation in title of report

Version 0.3.9, Sep 2020
------------------------
* Fix: refactorize Bin creation and fix scipy version for pytestDevelop
* Fix: dataset links in tutorial
* Lint: isort 5, latest black version
* Internal: simplification of weighted mean/std computation

Version 0.3.8, July 2020
------------------------
* Fixing automated ``PyPi`` deployment.
* Removing enabling of unnecessary notebook extensions.

Version 0.3.7, July 2020
------------------------
* Using ``ING``'s matplotlib style for the report plots (orange plots).
* Add ``popmon`` installation command at the beginning of example notebooks (seamless running).

Version 0.3.6, July 2020
------------------------
* Extending make.bat and Makefile to support ``make install`` (on all platforms).
* Add a snippet on how to use ``popmon`` with Spark dataframes to the docs.
* Update tutorial badges in the documentation.
* Migrate to standard MIT license header.

Version 0.3.5, June 2020
------------------------
* Extended the ``make`` commands (added ``make install`` and ``make lint check=1`` for check only).
* Add license headers to source files.

Version 0.3.4, June 2020
------------------------

* Several improvements aimed at new users, such as updated landing page, documentation and notebooks
* Performance improvement for weighted quantiles
* Consistent code formatting using black and isort
* Automatic release to PyPi on Github Release
* Platform agnostic file handling
* More informative exception messages

Version 0.3.3, April 2020
-------------------------

* Released the first open-source version of popmon.
* Please see documentation for full details: https://popmon.readthedocs.io
