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


import numpy as np
import pandas as pd

NUM_NS_DAY = 24 * 3600 * int(1e9)


def check_column(col, sep=":"):
    """Convert input column string to list of columns

    :param col: input string
    :param sep: default ":"
    :return: list of columns
    """
    if isinstance(col, str):
        col = col.split(sep)
    elif not isinstance(col, list):
        raise TypeError(f'Columns "{col}" needs to be a string or list of strings')
    return col


def check_dtype(dtype):
    """Convert datatype to consistent numpy datatype

    :param dtype: input datatype
    :rtype: numpy.dtype.type
    """
    try:
        if hasattr(dtype, "type"):
            # this converts pandas types, such as pd.Int64, into numpy types
            dtype = type(dtype.type())
        dtype = np.dtype(dtype).type
        if dtype in {np.str_, np.string_, np.object_}:
            dtype = np.dtype(str).type
    except BaseException:
        raise RuntimeError(f'unknown assigned datatype "{dtype}"')
    return dtype


def to_ns(x):
    """Convert input timestamps to nanoseconds (integers).

    :param x: value to be converted
    :returns: converted value
    :rtype: int
    """
    if pd.isnull(x):
        return 0
    try:
        return pd.to_datetime(x).value
    except Exception:
        if hasattr(x, "__str__"):
            return pd.to_datetime(str(x)).value
    return 0


def to_str(val):
    """Convert input to (array of) string(s).

    :param val: value to be converted
    :returns: converted value
    :rtype: str or np.ndarray
    """
    if isinstance(val, str):
        return val
    elif hasattr(val, "__iter__"):
        return np.asarray(
            list(
                map(
                    lambda s: s
                    if isinstance(s, str)
                    else str(s)
                    if hasattr(s, "__str__")
                    else "",
                    val,
                )
            )
        )

    elif hasattr(val, "__str__"):
        return str(val)

    return ""


def only_str(val):
    """Pass input value or array only if it is a string.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: str or np.ndarray
    """
    if isinstance(val, str):
        return val
    elif hasattr(val, "__iter__"):
        return np.asarray([s if isinstance(s, str) else "None" for s in val])
    return "None"


def only_bool(val):
    """Pass input value or array only if it is a bool.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.bool or np.ndarray
    """
    if isinstance(val, (np.bool_, bool)):
        return val
    elif hasattr(val, "__iter__") and not isinstance(val, str):
        return np.asarray(
            [s if isinstance(s, (np.bool_, bool)) else np.nan for s in val]
        )
    return np.nan


def only_int(val):
    """Pass input val value or array only if it is an integer.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64 or np.ndarray
    """
    if isinstance(val, (np.int64, int)):
        return val
    elif hasattr(val, "__iter__") and not isinstance(val, str):
        return np.asarray(
            [s if isinstance(s, (np.int64, int)) else np.nan for s in val]
        )
    return np.nan


def only_float(val):
    """Pass input val value or array only if it is a float.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.float64 or np.ndarray
    """
    if isinstance(val, (np.float64, float)):
        return val
    elif hasattr(val, "__iter__") and not isinstance(val, str):
        return np.asarray(
            [s if isinstance(s, (np.float64, float)) else np.nan for s in val]
        )
    return np.nan


QUANTITY = {
    str: only_str,
    np.str_: only_str,
    int: only_int,
    np.int64: only_int,
    np.int32: only_int,
    bool: only_bool,
    np.bool_: only_bool,
    float: only_float,
    np.float64: only_float,
    np.datetime64: only_int,
}


def value_to_bin_index(val, **kwargs):
    """Convert value to bin index.

    Convert a numeric or timestamp column to an integer bin index.

    :param bin_width: bin_width value needed to convert column
        to an integer bin index
    :param bin_offset: bin_offset value needed to convert column
        to an integer bin index
    """
    try:
        # NOTE this notation also works for timestamps
        bin_width = kwargs.get("bin_width", 1)
        bin_offset = kwargs.get("bin_offset", 0)
        bin_index = int(np.floor((val - bin_offset) / bin_width))
        return bin_index
    except BaseException:
        pass
    return val


def value_to_bin_center(val, **kwargs):
    """Convert value to bin center.

    Convert a numeric or timestamp column to a common bin center value.

    :param bin_width: bin_width value needed to convert column
        to a common bin center value
    :param bin_offset: bin_offset value needed to convert column
        to a common bin center value
    """
    try:
        # NOTE this notation also works for timestamps, and does not change the
        # unit
        bin_width = kwargs.get("bin_width", 1)
        bin_offset = kwargs.get("bin_offset", 0)
        bin_index = int(np.floor((val - bin_offset) / bin_width))
        obj_type = type(bin_width)
        return bin_offset + obj_type((bin_index + 0.5) * bin_width)
    except BaseException:
        pass
    return val
