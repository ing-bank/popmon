import json
from pathlib import Path

from popmon.extensions import extensions


def get_extras():
    """Obtain extras from extensions"""
    extras = {}
    for extension in extensions:
        extras.update(extension.extras)

    return extras


def write_extras():
    """Write extras to extras.json for setup.py"""
    extras = get_extras()
    file_path = Path(__file__).parent.parent.parent / "extras.json"

    with file_path.open("w") as f:
        json.dump(extras, f)


if __name__ == "__main__":
    write_extras()
