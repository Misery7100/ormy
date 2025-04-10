from copy import deepcopy
from enum import Enum
from functools import reduce
from typing import (
    Any,
    Iterable,
    Literal,
    Optional,
    Self,
    Sequence,
    SupportsIndex,
    TypeVar,
    overload,
)

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from ormy.exceptions import BadRequest

# ----------------------- #

B = TypeVar("B", bound=BaseModel)

# ----------------------- #


class ExtendedEnum(Enum):
    """A base class for extended enumerations."""

    @classmethod
    def list(cls):
        """
        Return a list of values from the enumeration

        Returns:
            res (list): A list of values from the enumeration
        """

        return list(map(lambda c: c.value, cls))


# ----------------------- #


class Row(dict):
    pass


# ....................... #

# TODO:
# - add `apply` method


class TabularData(list):
    _valid_keys: set[str] = set()

    # ....................... #

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        """
        Get the Pydantic core schema for the tabular data

        Args:
            source_type (Any): The source type
            handler (GetCoreSchemaHandler): The handler for the core schema

        Returns:
            res (CoreSchema): The core schema for the tabular data
        """

        return core_schema.no_info_after_validator_function(
            cls, handler(list[dict[str, Any]])
        )

    # ....................... #

    def __init__(
        self: Self,
        items: list[dict[str, Any]] | list[B] | Self = [],
    ):
        """
        Initialize the tabular data

        Args:
            items (list[dict[str, Any] | BaseModel] | TabularData): The items to initialize the tabular data with
        """

        items = self._validate_data(items)
        super().__init__(items)

    # ....................... #

    @overload
    def __getitem__(self: Self, index: SupportsIndex) -> Any: ...

    # ....................... #

    @overload
    def __getitem__(self: Self, index: slice) -> list[Any]: ...

    # ....................... #

    @overload
    def __getitem__(self: Self, index: str) -> list[Any]: ...

    # ....................... #

    def __getitem__(self: Self, index: str | SupportsIndex | slice | list[str]):
        """
        Get an item from the tabular data

        Args:
            index (str | SupportsIndex | slice): The index to get the item from

        Returns:
            res (list): The item from the tabular data
        """

        if isinstance(index, str):
            if index not in self._valid_keys:
                raise BadRequest(f"Column '{index}' not found")

            return self.__class__([{index: row[index]} for row in self])

        elif isinstance(index, list):
            for k in index:
                if k not in self._valid_keys:
                    raise BadRequest(f"Column '{k}' not found")

            records: list[dict[str, Any]] = [
                {k: v for k, v in x.items() if k in index} for x in self
            ]

            return self.__class__(records)

        elif isinstance(index, slice):  # ???
            return self.__class__(super().__getitem__(index))

        else:
            return super().__getitem__(index)

    # ....................... #

    @overload
    def __setitem__(self: Self, index: SupportsIndex, value: Any) -> None: ...

    # ....................... #

    @overload
    def __setitem__(self: Self, index: slice, value: Iterable[Any]) -> None: ...

    # ....................... #

    @overload
    def __setitem__(self: Self, index: str, value: Any): ...

    # ....................... #

    def __setitem__(
        self: Self,
        index: str | SupportsIndex | slice,
        value: Any | Iterable[Any],
    ):
        """
        Set an item in the tabular data

        Args:
            index (str | SupportsIndex | slice): The index to set the item in
            value (Any | Iterable[Any]): The value to set the item to
        """

        if isinstance(index, str):
            if not isinstance(value, (list, tuple)):
                for row in self:
                    row[index] = value

            else:
                if len(value) != len(self):
                    raise BadRequest("Length of values must match the number of rows")

                for i, row in enumerate(self):
                    row[index] = value[i]

            self._valid_keys.add(index)

        else:
            super().__setitem__(index, value)

    # ....................... #

    def _validate_item(self: Self, item: dict[str, Any]):
        """
        Validate an item

        Args:
            item (dict[str, Any]): The item to validate

        Returns:
            res (bool): Whether the item is valid
        """

        assert isinstance(item, dict), "Item must be a dictionary"

        missing_keys = self._valid_keys - set(item.keys())

        for x in missing_keys:
            item[x] = None

        return True

    # ....................... #

    def _validate_data(
        self: Self,
        data: list[dict[str, Any]] | list[B] | Self = [],
    ):
        """
        Validate the data

        Args:
            data (Sequence[dict[str, Any] | Bm] | TabularData): The data to validate

        Returns:
            res (list): The validated data
        """

        if not data:
            return []

        _data = [x.model_dump() if not isinstance(x, dict) else x for x in data]
        self._valid_keys = reduce(
            lambda x, y: x | y, map(lambda x: set(x.keys()), _data)
        )

        return [item for item in _data if self._validate_item(item)]

    # ....................... #

    def slice(
        self: Self,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
    ):
        """
        Slice the tabular data

        Args:
            include (Optional[Sequence[str]]): The columns to include
            exclude (Optional[Sequence[str]]): The columns to exclude

        Returns:
            res (TabularData): The sliced tabular data
        """

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

    def paginate(self: Self, page: int = 1, size: int = 20):
        start = (page - 1) * size
        end = page * size

        return self.__class__(self[start:end])

    # ....................... #

    def append(self: Self, x: dict[str, Any]):
        self._validate_item(x)
        super().append(x)

    # ....................... #

    def unique(self: Self, key: str):
        return set(x[key] for x in self if key in x)

    # ....................... #

    def join(
        self: Self,
        other: Self,
        *,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
        prefix: Optional[str] = None,
    ):
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
