"""Application services for non-transport business flows."""

from importlib import import_module


def __getattr__(name: str):
    try:
        return import_module(f"{__name__}.{name}")
    except ModuleNotFoundError as exc:
        raise AttributeError(name) from exc
