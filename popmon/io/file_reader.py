# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import collections.abc
from pathlib import Path
from typing import Callable, Optional, Union

from ..base import Module


class FileReader(Module):
    """Module to read contents from a file, transform the contents with a function and write them to the datastore.
    """

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
