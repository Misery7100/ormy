from typing import Awaitable, Callable, ParamSpec, TypeVar

# ----------------------- #

P = ParamSpec("P")
R = TypeVar("R")

AsyncCallable = Callable[P, Awaitable[R]]

# ----------------------- #

__all__ = ["AsyncCallable"]
