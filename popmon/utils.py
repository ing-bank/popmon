# Copyright (c) 2023 ING Analytics Wholesale Banking
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

from __future__ import annotations

import fnmatch
from textwrap import shorten
from typing import Callable

import pandas as pd
from joblib import Parallel, delayed


def short_date(date):
    """
    Shorten date string to length of 22
    """
    if isinstance(date, pd.Timestamp):
        # Drop the time of day when midnight or noon
        d = (
            str(date).split(" ")[0]
            if date.hour in [0, 12] and date.minute == 0 and date.second == 0
            else str(date)
        )
    else:
        d = str(date)

    return shorten(d, width=22, placeholder="")


def filter_metrics(
    metrics, ignore_stat_endswith: list[str], show_stats: list[str] | None = None
):
    """
    Filter metrics by excluding based on suffix and/or including when matching a pattern
    """
    metrics = [
        m for m in metrics if not any(m.endswith(s) for s in ignore_stat_endswith)
    ]
    if show_stats is not None:
        metrics = [
            m
            for m in metrics
            if any(fnmatch.fnmatch(m, pattern) for pattern in show_stats)
        ]
    return metrics


def parallel(func: Callable, args_list, mode: str = "args"):
    """
    Routine for parallel processing
    """
    from popmon.config import parallel_args

    if parallel_args["n_jobs"] == 1:
        results = [
            func(*args) if mode == "args" else func(**args) for args in args_list
        ]
    else:
        results = Parallel(**parallel_args)(
            delayed(func)(*args) if mode == "args" else delayed(func)(**args)
            for args in args_list
        )
    return results
