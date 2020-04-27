import copy
from pathlib import PosixPath
from collections.abc import Callable
from ..base import Module


class FileWriter(Module):
    """Module transforms specific datastore content and writes it to a file.
    """

    def __init__(self, read_key, store_key=None, file_path=None, apply_func=None, **kwargs):
        """Initialize an instance.

        :param str read_key: key of input histogram-dict to read from data store
        :param str store_key: key of output data to store in data store (optional)
        :param str file_path: the file path where to output the report (optional)
        :param callable apply_func: function to be used for the transformation of data (optional)
        :param dict kwargs: additional keyword arguments which would be passed to `apply_func`
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key
        if not isinstance(file_path, (type(None), str, PosixPath)):
            raise AssertionError('file path\'s format is not supported (must be a string or Posix).')
        self.file_path = file_path
        if not isinstance(apply_func, (type(None), Callable)):
            raise AssertionError('transformation function must be a callable object')
        self.apply_func = apply_func
        self.kwargs = kwargs

    def transform(self, datastore):
        data = copy.deepcopy(datastore[self.read_key])

        # if a transformation function is provided, transform the data
        if self.apply_func is not None:
            data = self.apply_func(data, **self.kwargs)

        # if file path is provided, write data to a file. Otherwise, write data into the datastore
        if self.file_path is None:
            datastore[self.read_key if self.store_key is None else self.store_key] = data
        else:
            with open(self.file_path, 'w+') as file:
                file.write(data)
            self.logger.info(f'Object \"{self.read_key}\" written to file \"{self.file_path}\".')

        return datastore
