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
from copy import copy
from typing import Callable, List, Optional, Tuple, Union


class Profiles:
    _profile_descriptions = {}
    _profile_funcs = {-1: {}}

    @classmethod
    def register(
        cls,
        key: Union[str, List[str], Tuple[str]],
        description: Union[str, List[str], Tuple[str]],
        dim: Optional[int] = None,
        htype: Optional[str] = None,
    ):
        if dim is None:
            dim = -1
        if isinstance(key, list):
            key = tuple(key)

        if isinstance(description, list):
            description = tuple(description)

        def f(func: Callable):
            if isinstance(key, tuple):
                for k, d in zip(key, description):
                    cls._profile_descriptions[k] = d
            else:
                cls._profile_descriptions[key] = description

            if dim not in cls._profile_funcs:
                cls._profile_funcs[dim] = {}
            cls._profile_funcs[dim][(key, htype)] = func
            return func

        return f

    @classmethod
    def get_profiles(
        cls,
        dim: Optional[int] = None,
        htype: Optional[str] = None,
    ):
        def merge(d1, d2):
            x = copy(d1)
            x.update(d2)
            return x

        if dim is None:
            v = cls._profile_funcs[-1]
        else:
            v = merge(cls._profile_funcs.get(dim, {}), cls._profile_funcs[-1])

        return v

    @classmethod
    def get_profile_keys(
        cls,
        dim: Optional[int] = None,
        htype: Optional[str] = None,
    ):
        def flatten(input_list):
            vals = []
            for v in input_list:
                if isinstance(v, (list, tuple)):
                    for v2 in v:
                        vals.append(v2)
                else:
                    vals.append(v)
            return vals

        return flatten(
            [
                k
                for (k, dtype), v in cls.get_profiles(dim).items()
                if dtype is None or htype is None or dtype == "all" or htype == dtype
            ]
        )

    @classmethod
    def get_descriptions(cls):
        return cls._profile_descriptions
