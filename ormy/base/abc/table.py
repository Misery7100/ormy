from typing import TypeVar

from .abstract import AbstractSingleABC
from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound="ConfigABC")
Ts = TypeVar("Ts", bound="TableSingleABC")

# ----------------------- #


class TableSingleABC(AbstractSingleABC):
    """
    Abstract Base Class for Table-Oriented Object-Relational Mapping
    """

    pass
