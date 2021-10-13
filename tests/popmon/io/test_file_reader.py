import json

from popmon import resources
from popmon.io import FileReader


def test_file_reader_json():
    fr = FileReader(
        file_path=resources.data("example.json"),
        store_key="example",
        apply_func=json.loads,
    )
    datastore = fr._transform(datastore={})

    assert datastore["example"]["boolean"]
    assert len(datastore["example"]["array"]) == 3
    assert datastore["example"]["category"]["a"] == 0
