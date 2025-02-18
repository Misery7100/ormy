from abc import abstractmethod
from typing import Any, Self

from ormy._abc import AbstractABC

# ----------------------- #


class TableABC(AbstractABC):
    """Abstract Base Class for Table-Oriented Object-Relational Mapping"""

    @classmethod
    @abstractmethod
    def create_table(cls, exist_ok: bool = True) -> None: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def insert(
        cls,
        data: list[Self] | Self | list[dict[str, Any]] | dict[str, Any],
    ) -> None: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def query(cls, query: str, params: dict[str, Any]) -> list[Self]: ...
