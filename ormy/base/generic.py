from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Literal, Optional, Sequence, Set, TypeVar

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

# ----------------------- #

Bm = TypeVar("Bm", bound="BaseModel")
Tb = TypeVar("Tb", bound="TabularData")

# ----------------------- #


class ExtendedEnum(Enum):
    """A base class for extended enumerations."""

    @classmethod
    def list(cls):
        """Return a list of values from the enumeration."""

        return list(map(lambda c: c.value, cls))


# ----------------------- #


# TODO: add paginate method
class TabularData(list):
    _valid_keys: Set[str] = set()

    # ....................... #

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls, handler(Sequence[Dict[str, Any]])
        )

    # ....................... #

    def __init__(self: Tb, items: Sequence[Dict[str, Any] | Bm] | Tb = []):
        items = self._validate_data(items)
        super().__init__(items)

    # ....................... #

    def _validate_item(self: Tb, item: Dict[str, Any]):
        assert isinstance(item, dict), "Item must be a dictionary"

        if set(item.keys()) != self._valid_keys:
            raise ValueError("Item must have the same keys as the other items")

        return True

    # ....................... #

    def _validate_data(self, data: Sequence[Dict[str, Any] | Bm] | Tb = []):
        if not data:
            return []

        _data = [x.model_dump() if not isinstance(x, dict) else x for x in data]
        self._valid_keys = set(_data[0].keys())

        return [item for item in _data if self._validate_item(item)]

    # ....................... #

    def slice(
        self,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
    ):
        if include:
            return self.__class__(
                [{k: v for k, v in x.items() if k in include} for x in self]
            )

        elif exclude:
            return self.__class__(
                [{k: v for k, v in x.items() if k not in exclude} for x in self]
            )

        return self.__class__(self)

    # ....................... #

    def paginate(self, page: int = 1, size: int = 20):
        start = (page - 1) * size
        end = page * size

        return self.__class__(self[start:end])

    # ....................... #

    def append(self, x: Dict[str, Any]):
        self._validate_item(x)
        super().append(x)

    # ....................... #

    def unique(self, key: str) -> Set[str]:
        return set(x[key] for x in self if key in x)

    # ....................... #

    def join(
        self: Tb,
        other: Tb,
        *,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
        prefix: Optional[str] = None,
    ) -> Tb:
        """
        Merge two tabular data objects
        """

        if kind not in ["inner", "left"]:
            raise ValueError("Kind must be either 'inner' or 'left'")

        if not self:
            return self

        if not other:
            if kind == "left":
                return self

            return self.__class__()

        if on is not None:
            left_on = on
            right_on = on

        assert left_on in self._valid_keys, f"Key {left_on} is not in the valid keys"
        assert right_on in other._valid_keys, f"Key {right_on} is not in the valid keys"

        intersection = self.unique(left_on).intersection(other.unique(right_on))

        if len(intersection) == 0:
            if kind == "left":
                return self

            return self.__class__()

        res = []

        for x in self:
            if x[left_on] in intersection:
                item = deepcopy(next(y for y in other if y[right_on] == x[left_on]))

                if prefix:
                    item = {f"{prefix}_{k}": v for k, v in item.items()}

                else:
                    item.pop(right_on)

                res.append({**x, **item})

            elif kind == "left":
                item = {k: fill_none for k in other._valid_keys}

                if prefix:
                    item = {f"{prefix}_{k}": v for k, v in item.items()}

                else:
                    item.pop(right_on)

                res.append({**x, **item})

        return self.__class__(res)
