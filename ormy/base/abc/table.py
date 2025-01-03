from typing import TypeVar

from ormy.utils.logging import LogManager

from .abstract import AbstractSingleABC
from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound="ConfigABC")
Ts = TypeVar("Ts", bound="TableSingleABC")

logger = LogManager.get_logger(__name__)

# ----------------------- #


class TableSingleABC(AbstractSingleABC):
    """
    Abstract Base Class for Table-Oriented Object-Relational Mapping
    """

    pass
