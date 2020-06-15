# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import collections.abc
import copy
from pathlib import Path
from typing import Callable, Optional, Union

from ..base import Module


class FileWriter(Module):
    """Module transforms specific datastore content and writes it to a file.
    """

    def __init__(
        self,
        read_key: str,
        store_key: Optional[str] = None,
        file_path: Optional[Union[str, Path]] = None,
        apply_func: Optional[Callable] = None,
        **kwargs,
    ):
        """Initialize an instance.

        :param str read_key: key of input histogram-dict to read from data store
        :param str store_key: key of output data to store in data store (optional)
        :param str file_path: the file path where to output the report (optional)
        :param callable apply_func: function to be used for the transformation of data (optional)
        :param dict kwargs: additional keyword arguments which would be passed to `apply_func`
        """
        super().__init__()
        if file_path is not None and not isinstance(file_path, (str, Path)):
            raise TypeError("file_path should be of type `str` or `pathlib.Path`")
        if apply_func is not None and not isinstance(
            apply_func, collections.abc.Callable
        ):
            raise TypeError("transformation function must be a callable object")
        self.read_key = read_key
        self.store_key = store_key
        self.file_path = file_path
        self.apply_func = apply_func
        self.kwargs = kwargs

    def transform(self, datastore):
        data = copy.deepcopy(datastore[self.read_key])

        # if a transformation function is provided, transform the data
        if self.apply_func is not None:
            data = self.apply_func(data, **self.kwargs)

        # if file path is provided, write data to a file. Otherwise, write data into the datastore
        if self.file_path is None:
            datastore[
                self.read_key if self.store_key is None else self.store_key
            ] = data
        else:
            with open(self.file_path, "w+") as file:
                file.write(data)
            self.logger.info(
                f'Object "{self.read_key}" written to file "{self.file_path}".'
            )

        return datastore
