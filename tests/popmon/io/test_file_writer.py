import copy
import json

import pandas as pd
import pytest

from popmon.io import FileWriter

DATA = {"name": ["Name"], "surname": ["Surname"]}


def get_ready_ds():
    return copy.deepcopy({"my_data": DATA})


def to_json(data, **kwargs):
    return json.dumps(data, **kwargs)


def to_pandas(data):
    return pd.DataFrame.from_dict(data)


def test_file_writer_json():
    datastore = get_ready_ds()
    FileWriter("my_data", apply_func=to_json)._transform(datastore)
    assert datastore["my_data"] == to_json(DATA)


def test_file_writer_json_with_kwargument():
    datastore = get_ready_ds()
    FileWriter("my_data", apply_func=to_json, indent=4)._transform(datastore)
    assert datastore["my_data"] == to_json(DATA, indent=4)


def test_file_writer_not_a_func():
    datastore = get_ready_ds()
    with pytest.raises(TypeError):
        FileWriter("my_data", apply_func={})._transform(datastore)


def test_file_writer_df():
    datastore = get_ready_ds()
    FileWriter(
        "my_data", store_key="transformed_data", apply_func=to_pandas
    )._transform(datastore)
    assert datastore["my_data"] == DATA
    assert datastore["transformed_data"].to_dict() == to_pandas(DATA).to_dict()
