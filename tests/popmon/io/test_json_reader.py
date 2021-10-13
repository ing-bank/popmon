from popmon import resources
from popmon.io import JsonReader


def test_json_reader():
    jr = JsonReader(file_path=resources.data("example.json"), store_key="example")
    datastore = jr._transform(datastore={})

    assert datastore["example"]["boolean"]
    assert len(datastore["example"]["array"]) == 3
    assert datastore["example"]["category"]["a"] == 0
