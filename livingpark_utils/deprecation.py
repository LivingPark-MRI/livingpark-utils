"""Facilitate the deprecation of functions."""
import functools
import warnings
from collections.abc import Callable
from typing import Any


def deprecated(extra: str = None) -> Any:
    """Mark a function as deprecated and issue a warning.

    Parameters
    ----------
    func : Callable
        Function to deprecate.

    extra : str, default None
        Extra information about the deprecated function.

    Returns
    -------
    Any
        Result of the initial function.
    """

    def _decorator(func: Callable):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)  # turn off filter
            warnings.warn(
                (
                    f"Call to deprecated function {func.__name__}."
                    "\nThis function will be remove in future version."
                    + (f"\n{extra}" if extra else "")
                ),
                category=DeprecationWarning,
                stacklevel=2,
            )
            warnings.simplefilter("default", DeprecationWarning)  # reset filter
            return func(*args, **kwargs)

        return _wrapper

    return _decorator
