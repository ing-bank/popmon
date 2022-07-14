import pytest
from pydantic.error_wrappers import ValidationError

from popmon import Settings


def test_settings_docs_example():
    settings = Settings()
    settings.time_axis = "date"
    assert settings.time_axis == "date"

    settings = Settings(time_axis="date")
    assert settings.time_axis == "date"


def test_setting_validation():
    settings = Settings()
    with pytest.raises(ValidationError):
        settings.time_axis = ["test"]
