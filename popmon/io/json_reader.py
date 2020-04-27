import json
from ..io import FileReader


class JsonReader(FileReader):
    """Read json file's contents into the datastore.
    """

    def __init__(self, file_path, store_key):
        """Initialize an instance.

        :param str store_key: key of input data to be stored in the datastore
        :param str file_path: the file path to read the data from
        """
        super().__init__(store_key, file_path, apply_func=json.loads)

    def transform(self, datastore):
        return super().transform(datastore)
