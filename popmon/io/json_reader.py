import json
from pathlib import Path
from typing import Union

from ..io import FileReader


class JsonReader(FileReader):
    """Read json file's contents into the datastore.
    """

    def __init__(self, file_path: Union[str, Path], store_key: str):
        """Initialize an instance.

        :param str store_key: key of input data to be stored in the datastore
        :param str file_path: the file path to read the data from
        """
        super().__init__(store_key, file_path, apply_func=json.loads)

    def transform(self, datastore):
        return super().transform(datastore)
