# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
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


import collections.abc
from pathlib import Path
from typing import Callable, Optional, Union

from ..base import Module


class FileReader(Module):
    """Module to read contents from a file, transform the contents with a function and write them to the datastore."""

    def __init__(
        self,
        store_key: str,
        file_path: Union[str, Path],
        apply_func: Optional[Callable] = None,
        **kwargs,
    ):
        """Initialize an instance.

        :param str store_key: key of input data to be stored in the datastore
        :param str file_path: the file path to read the data from
        :param callable apply_func: function to be used for the transformation of data (optional)
        :param dict kwargs: additional keyword arguments which would be passed to `apply_func`
        """
        super().__init__()
        if not isinstance(file_path, (str, Path)):
            raise TypeError("file_path should be of type `str` or `pathlib.Path`")
        if apply_func is not None and not isinstance(
            apply_func, collections.abc.Callable
        ):
            raise TypeError("transformation function must be a callable object")

        self.store_key = store_key
        self.file_path = file_path
        self.apply_func = apply_func
        self.kwargs = kwargs

    def transform(self, datastore):
        with open(self.file_path) as file:
            data = file.read()

        # if a transformation function is provided, transform the data
        if self.apply_func is not None:
            data = self.apply_func(data, **self.kwargs)

        self.logger.info(
            f'Object "{self.store_key}" read from file "{self.file_path}".'
        )

        # store the transformed/original contents
        datastore[self.store_key] = data
        return datastore
