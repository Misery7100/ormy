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

    # ....................... #

    def _validate_data(self, data: Sequence[Dict[str, Any] | Bm] | Tb = []):
        if not data:
            return []

        _data = [x.model_dump() if not isinstance(x, dict) else x for x in data]
        self._valid_keys = set(_data[0].keys())
        map(self._validate_item, _data)

        return _data

    # ....................... #

    def slice(
        self,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
    ):
        if include:
            new_items = [{k: v for k, v in x.items() if k in include} for x in self]
            return self.__class__(new_items)

        elif exclude:
            new_items = [{k: v for k, v in x.items() if k not in exclude} for x in self]
            return self.__class__(new_items)

        else:
            return self.__class__(self)

    # ....................... #

    def append(self, x: Dict[str, Any]):
        self._validate_item(x)
        super().append(x)

    # ....................... #

    def unique(self, key: str) -> Set[str]:
        assert key in self._valid_keys, f"Key {key} is not in the valid keys"

        return set(map(lambda x: x[key], self))

    # ....................... #

    def join(
        self: Tb,
        other: Tb,
        *,
        key: Optional[str] = None,
        left_key: Optional[str] = None,
        right_key: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
    ) -> Tb:
        """
        Merge two tabular data objects
        """

        assert kind in ["inner", "left"], "Kind must be either 'inner' or 'left'"

        if key is not None:
            left_key = key
            right_key = key

        assert left_key in self._valid_keys, f"Key {left_key} is not in the valid keys"
        assert (
            right_key in other._valid_keys
        ), f"Key {right_key} is not in the valid keys"

        intersection = self.unique(left_key).intersection(other.unique(right_key))

        if len(intersection) == 0:
            return self.__class__()

        res = []

        for x in self:
            if x[left_key] in intersection:
                item = deepcopy(next(y for y in other if y[right_key] == x[left_key]))
                item.pop(right_key)
                res.append({**x, **item})

            elif kind == "left":
                res.append(
                    {**x, **{k: fill_none for k in other._valid_keys if k != right_key}}
                )

        return self.__class__(res)
