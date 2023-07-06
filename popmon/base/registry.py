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

from collections import defaultdict
from typing import Any, Callable


class Registry:
    _properties = ("dim", "htype")

    def __init__(self) -> None:
        self._keys: list[str] = []
        self._descriptions: dict[str, str] = {}
        self._properties_to_func: defaultdict[
            str, defaultdict[str, dict[Any, Any]]
        ] = defaultdict(lambda: defaultdict(dict))
        self._func_name_to_properties: dict[Any, Any] = {}

    def register(
        self,
        key: str | list[str] | tuple[str],
        description: str | list[str] | tuple[str],
        dim: int = -1,
        htype: str | None = None,
    ):
        # rename for function use, without changing api
        keys = key
        del key

        descriptions = description
        del description

        # ensure that keys are a tuple
        if isinstance(keys, list):
            keys = tuple(keys)
        elif not isinstance(keys, tuple):
            keys = (keys,)

        # ensure that description is a tuple
        if isinstance(descriptions, list):
            descriptions = tuple(descriptions)
        elif not isinstance(descriptions, tuple):
            descriptions = (descriptions,)

        def f(func: Callable):
            # function names should be unique
            if func.__name__ in self._func_name_to_properties:
                raise ValueError(
                    f"A function with the name '{func.__name__}' has already been registered."
                )

            # keys should unique correspond to a function
            for key in keys:
                if key in self._keys:
                    raise ValueError(f"Key '{key}' has already been registered.")

            # register properties
            self._keys += list(keys)
            self._func_name_to_properties[func.__name__] = (dim, htype, keys)
            self._properties_to_func[dim][htype][keys] = func
            self._descriptions.update(dict(zip(keys, descriptions)))

            return func

        return f

    # Methods
    def _get_func_properties_by_name(
        self, function_name: str
    ) -> tuple[int, str, tuple[str]]:
        return self._func_name_to_properties[function_name]

    def get_func_by_name(self, function_name: str) -> Callable:
        """
         Get a function by the function name

        :param str function_name: name of the original function
        """
        dim, htype, key = self._get_func_properties_by_name(function_name)
        return self._properties_to_func[dim][htype][key]

    def get_func_by_dim_and_htype(self, dim, htype) -> dict[tuple[str], Callable]:
        return self._properties_to_func[dim][htype]

    def get_keys(self) -> list[str]:
        """List of keys associated with registered functions"""
        return self._keys

    def get_keys_by_dim_and_htype(self, dim, htype) -> list[str]:
        """Flat list of keys for a provided dimension and histogram type"""
        return [v for values in self._properties_to_func[dim][htype] for v in values]

    def get_descriptions(self) -> dict[str, str]:
        """Dictionary of key->description associated with registered functions"""
        return self._descriptions

    def update_func(self, name, func) -> None:
        dim, htype, key = self._func_name_to_properties[name]
        self._properties_to_func[dim][htype][key] = func

    def run(self, args, dim, htype):
        output = {}
        for key, func in self.get_func_by_dim_and_htype(dim=dim, htype=htype).items():
            results = func(*args)
            if not isinstance(results, tuple):
                results = (results,)
            output.update(dict(zip(key, results)))
        return output
