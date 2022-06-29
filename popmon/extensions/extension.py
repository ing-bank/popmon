import importlib.util
from typing import Callable, List


def is_installed(package):
    is_present = importlib.util.find_spec(package)
    return is_present is not None


class Extension:
    name: str
    requirements: List[str]
    extension: Callable

    @property
    def extras(self):
        return {self.name: self.requirements}

    def check(self):
        if all(is_installed(package) for package in self.requirements):
            func = self.extension
            func = func.__func__
            func()
