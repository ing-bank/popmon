========
Profiles
========

Profiles: tracking a metric over time

Available profiles
------------------
The following metrics are implemented:

Any dimension

- count

1D histogram, all types:

- filled
- underflow, overflow
- nan

1D histogram, numeric:

- mean
- 1%, 5%, 16%, 50% (median), 84%, 95%, 99% percentiles
- std
- min, max

1D histogram, categorical

- fraction of true

2D histogram:

- phik


Custom profiles
---------------

Tracking custom metrics over time is easy.
The following code snippet registers a new metric to ``popmon``.

.. code-block:: python

    import numpy as np

    from popmon.analysis.profiling.profiles import Profiles


    @Profiles.register(key="name_of_profile", description="<description_for_report>", dim=2)
    def your_profile_function_name(hist) -> float:
        """Write your function to profile the histogram."""
        return np.sum(p)

Variations:

- A profile function may return multiple values for efficiency (e.g. quantiles do not need to be computed)

.. code-block:: python

    @Profiles.register(
        key=["key1", "key2"], description=["Statistic 1", "Statistic 2"], dim=None
    )
    def your_profile_function_name(hist) -> float:
        result1, result2 = your_logic(hist)
        return result1, result2

- A profile may work on the histogram, or on the value counts/labels (also for efficiency).
This occurs when the ``htype`` parameter is passed (1D only)

.. code-block:: python

    @Profiles.register(
        key="name_of_profile", description="<description_for_report>", dim=1, htype="all"
    )
    def your_profile_function_name(bin_labels, bin_counts) -> float:
        return bin_counts.sum()

- Profiles may depend on variable type (possible values for ``htype``: ``num``, ``cat``, ``all``).

.. code-block:: python

    @Profiles.register(
        key="name_of_profile", description="<description_for_report>", dim=1, htype="num"
    )
    def your_profile_function_name(bin_labels, bin_counts) -> float:
        return bin_counts.sum()

If you developed a custom profiles that could be generically used, then please considering contributing it to the package.
