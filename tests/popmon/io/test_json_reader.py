from popmon.io import JsonReader
from popmon import resources


def test_json_reader():
    jr = JsonReader(file_path=resources.data("example.json"), store_key="example")
    datastore = jr.transform(datastore={})

    assert datastore["example"]["boolean"]
    assert len(datastore["example"]["array"]) == 3
    assert datastore["example"]["category"]["a"] == 0
