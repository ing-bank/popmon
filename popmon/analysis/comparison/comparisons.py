from typing import Callable


class Comparisons:
    _comparison_descriptions = {}
    _comparison_funcs = {}

    @classmethod
    def register(cls, key: str, description: str):
        def f(func: Callable):
            cls._comparison_descriptions[key] = description
            cls._comparison_funcs[key] = func

        return f

    @classmethod
    def get_comparisons(cls):
        return cls._comparison_funcs

    @classmethod
    def get_descriptions(cls):
        return cls._comparison_descriptions
