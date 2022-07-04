Reference types
===============

When generating a report from a DataFrame, the reference type can be set with the option ``reference_type``,
in different ways:

+-----------------+
| Reference Type  |
+=================+
| Self            |
+-----------------+
| External        |
+-----------------+
| Rolling         |
+-----------------+
| Expanding       |
+-----------------+

Note that, by default, ``popmon`` also performs a rolling comparison of the histograms in each time period with those in the
previous time period. The results of these comparisons contain the term "prev1", and are found in the comparisons section
of a report.

Self reference
--------------

Using the DataFrame on which the stability report is built as a self-reference. This reference method is static: each time slot is compared to all the slots in the DataFrame (all included in one distribution). This is the default reference setting.

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="self")


The self-reference compares against the full dataset by default.
It is also supported to use a subset of the beginning of the data are used as reference point, e.g. the training data for a model.
The size of this subset is taken based on the ``split`` parameter.
``split`` accepts (1) a number of samples (integer), (2) a fraction of the dataset (float) or (3) a condition (string).

    .. code-block:: python

      # use the first 1000 rows as reference
      report = df.pm_stability_report(reference_type="self", split=1000)


External reference
------------------

Using an external reference DataFrame or set of histograms. This is also a static method: each time slot is compared to all the time slots in the reference data.

    .. code-block:: python

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(reference_type="external", reference=reference)


Rolling reference
-----------------

Using a rolling window within the same DataFrame as reference. This method is dynamic: we can set the size of the window and the shift from the current time slot. By default the 10 preceding time slots are used as reference (shift=1, window_size=10).

    .. code-block:: python

      # reference_type should be passed to the settings when provided
      settings = Settings(reference_type="rolling")
      settings.comparison.window = 10
      settings.comparison.shift = 1

      # alternatively you could do
      settings.reference_type = "rolling"

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(settings=settings)


Expanding reference
-------------------

Using an expanding window on all preceding time slots within the same DataFrame. This is also a dynamic method, with variable window size. All the available previous time slots are used. For example, if we have 2 time slots available and shift=1, window size will be 1 (so the previous slot is the reference), while if we have 10 time slots and shift=1, window size will be 9 (and all previous time slots are reference).

    .. code-block:: python

      settings = Settings(reference_type="expanding")
      settings.comparison.shift = 1

      # generate stability report with specific monitoring rules
      report = df.pm_stability_report(settings=settings)
