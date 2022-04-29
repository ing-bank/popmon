# Copyright (c) 2022 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# MB 20210323: histogrammming code hade been moved to histogrammar v1.0.20+
#              these imports are kept for backwards compatibility.

from histogrammar.dfinterface.make_histograms import (
    get_bin_specs,
    get_one_time_axis,
    get_time_axes,
    has_one_time_axis,
    make_histograms,
)
from histogrammar.dfinterface.pandas_histogrammar import PandasHistogrammar
from histogrammar.dfinterface.spark_histogrammar import SparkHistogrammar

__all__ = [
    "PandasHistogrammar",
    "SparkHistogrammar",
    "make_histograms",
    "get_time_axes",
    "get_one_time_axis",
    "has_one_time_axis",
    "get_bin_specs",
]
