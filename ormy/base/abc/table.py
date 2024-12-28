from typing import TypeVar

from ormy.utils.logging import LogLevel, console_logger

from .abstract import AbstractSingleABC
from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound="ConfigABC")
Ts = TypeVar("Ts", bound="TableSingleABC")

logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


class TableSingleABC(AbstractSingleABC):
    """
    Abstract Base Class for Table-Oriented Object-Relational Mapping
    """

    pass
