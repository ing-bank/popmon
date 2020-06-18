# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
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


import histogrammar
import numpy as np

# large numbers (time in ns since 1970) used to determine if float corresponds to a timestamp
DATE_LOW = 5e16  # 1971-08-02 16:53:20 in nanosec
DATE_HIGH = 9.9e18  # 2260-1-1 in nanosec

COMMON_HIST_TYPES = (
    histogrammar.Categorize,
    histogrammar.Bin,
    histogrammar.SparselyBin,
    histogrammar.specialized.CategorizeHistogramMethods,
    histogrammar.specialized.HistogramMethods,
    histogrammar.specialized.SparselyHistogramMethods,
    histogrammar.specialized.CategorizeHistogramMethods,
    histogrammar.specialized.TwoDimensionallyHistogramMethods,
    histogrammar.specialized.SparselyTwoDimensionallyHistogramMethods,
)


def get_datatype(cls):
    """Get histogrammar histogram datatype(s) of its axes

    Return data type of the variable represented by the histogram.  If not
    already set, will determine datatype automatically.

    :returns: list with datatypes of all dimenensions of the histogram
    :rtype: list
    """
    datatype = []
    if isinstance(cls, histogrammar.Count):
        return datatype
    if isinstance(cls, histogrammar.Categorize):
        if len(cls.bins) > 0:
            dt = type(list(cls.bins.keys())[0])
            dt = np.dtype(dt).type
            if (dt is np.str_) or (dt is np.string_) or (dt is np.object_):
                dt = str
            datatype = [dt]
    elif isinstance(cls, (histogrammar.Bin, histogrammar.SparselyBin)):
        datatype = [np.number]
        bin_centers = cls.bin_centers()
        if len(bin_centers) > 0:
            dt = type(bin_centers[-1])
            dt = np.dtype(dt).type
            datatype = [dt]
            # HACK: making an educated guess for timestamp
            # timestamp is in ns since 1970, so a huge number.
            is_ts = DATE_LOW < bin_centers[-1] < DATE_HIGH
            if is_ts:
                datatype = [np.datetime64]
    # histogram may have a subhistogram. Extract it and recurse
    if hasattr(cls, "bins"):
        hist = list(cls.bins.values())[0] if cls.bins else histogrammar.Count()
    elif hasattr(cls, "values"):
        hist = cls.values[0] if cls.values else histogrammar.Count()
    else:
        hist = histogrammar.Count()
    return datatype + get_datatype(hist)


@property
def datatype(self):  # noqa
    """Data type of histogram variable.

    Return data type of the variable represented by the histogram.  If not
    already set, will determine datatype automatically.

    :returns: data type
    :rtype: type or list(type)
    """
    # making an educated guess to determine data-type categories
    if not hasattr(self, "_datatype"):
        datatype = get_datatype(self)
        if isinstance(datatype, list):
            if len(datatype) == 1:
                return datatype[0]
            elif len(datatype) == 0:
                return type(None)
        return datatype

    if isinstance(self._datatype, list):
        if len(self._datatype) == 1:
            return self._datatype[0]
        elif len(self._datatype) == 0:
            return type(None)
    return self._datatype


@datatype.setter
def datatype(self, dt):
    """Set data type of histogram variable.

    Set data type of the variable represented by the histogram.

    :param type dt: type of the variable represented by the histogram
    :raises RunTimeError: if datatype has already been set, it will not overwritten
    """
    if hasattr(self, "_datatype"):
        raise RuntimeError("datatype already set")
    self._datatype = dt


# --- we decorate here
histogrammar.Bin.datatype = datatype
histogrammar.SparselyBin.datatype = datatype
histogrammar.Categorize.datatype = datatype
histogrammar.Count.datatype = datatype
