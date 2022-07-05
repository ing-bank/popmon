===========
Comparisons
===========

Comparisons: statistically comparing each time slot to the reference data.

Available comparisons
---------------------
A rich set of the most frequently used comparisons is available out-of-the-box:

+------------+-----------------+---------------------------------+
| Dimension  | Histogram Type  | Comparison                      |
+============+=================+=================================+
| 1D         | Numeric         | Kolmogorov-Smirnov test         |
+------------+-----------------+---------------------------------+
| 1D         | Categorical     | Unknown labels present          |
+------------+-----------------+---------------------------------+
| 2D+        | Any             | Pearson correlation             |
+------------+-----------------+---------------------------------+
| Any        | Any             | Chi-squared test                |
+------------+-----------------+---------------------------------+
| Any        | Any             | Jensen-Shannon Divergence       |
+------------+-----------------+---------------------------------+
| Any        | Any             | Population Stability Index      |
+------------+-----------------+---------------------------------+
| Any        | Any             | Maximum probability difference  |
+------------+-----------------+---------------------------------+

The comparisons registry can be consulted for available comparisons:

.. code-block:: python

    from popmon.analysis import Comparisons

    print(Comparisons.get_keys())


Comparison extensions
---------------------

There are currently no comparison extensions available, and contributions are welcome.
Have a look at the :doc:`Profiles <profiles>` page for the available profile extensions.

Custom comparisons
------------------

In some scenario's, you might have reason to prefer a not yet implemented comparison function.
``popmon`` allows to extend the present set of comparisons with only a few lines of code.
The code below demonstrates how this could be achieved:

.. code-block:: python

    import numpy as np

    from popmon.analysis.comparison.comparisons import Comparisons


    @Comparisons.register(key="name_of_comparison", description="<description_for_report>")
    def your_comparison_function_name(p, q) -> float:
        """Write your function that takes each time slot and reference data and returns a real number."""
        return np.sum(np.abs(p - q))

If you developed a custom comparison that could be generically used, then please considering contributing it to the package.

Comparison settings
-------------------

Whenever a comparison has parameters, it is possible to alter them globally:

.. code-block:: python

    from functools import partial

    from popmon.analysis.comparison.comparison_registry import Comparisons

    # Set the max_res_bound to 5 (default 7) for the chi2 comparison function
    Comparisons.update_func(
        "chi2", partial(Comparisons.get_func("chi2"), max_res_bound=5.0)
    )
