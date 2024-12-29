from copy import deepcopy
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    SupportsIndex,
    TypeVar,
    overload,
)

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from .error import BadInput

# ----------------------- #

Bm = TypeVar("Bm", bound="BaseModel")
Tb = TypeVar("Tb", bound="TabularData")

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

# TODO:
# - add `apply` method


class TabularData(list):
    _valid_keys: Set[str] = set()

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
            cls, handler(Sequence[Dict[str, Any]])
        )

    # ....................... #

    def __init__(self: Tb, items: Sequence[Dict[str, Any] | Bm] | Tb = []):
        """
        Initialize the tabular data

        Args:
            items (Sequence[Dict[str, Any] | BaseModel] | TabularData): The items to initialize the tabular data with
        """

        items = self._validate_data(items)
        super().__init__(items)

    # ....................... #

    @overload
    def __getitem__(self, index: SupportsIndex) -> Any: ...

    # ....................... #

    @overload
    def __getitem__(self, index: slice) -> list[Any]: ...

    # ....................... #

    @overload
    def __getitem__(self, index: str) -> list[Any]: ...

    # ....................... #

    def __getitem__(self, index: str | SupportsIndex | slice | List[str]):
        """
        Get an item from the tabular data

        Args:
            index (str | SupportsIndex | slice): The index to get the item from

        Returns:
            res (list): The item from the tabular data
        """

        if isinstance(index, str):
            if index not in self._valid_keys:
                raise BadInput(f"Column '{index}' not found")

            return self.__class__([{index: row[index]} for row in self])

        elif isinstance(index, list):
            for k in index:
                if k not in self._valid_keys:
                    raise BadInput(f"Column '{k}' not found")

            records = [{k: v for k, v in x.items() if k in index} for x in self]
            return self.__class__(records)

        elif isinstance(index, slice):  # ???
            return self.__class__(super().__getitem__(index))

        else:
            return super().__getitem__(index)

    # ....................... #

    @overload
    def __setitem__(self, index: SupportsIndex, value: Any) -> None: ...

    # ....................... #

    @overload
    def __setitem__(self, index: slice, value: Iterable[Any]) -> None: ...

    # ....................... #

    @overload
    def __setitem__(self, index: str, value: Any): ...

    # ....................... #

    def __setitem__(
        self,
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
                    raise BadInput("Length of values must match the number of rows")

                for i, row in enumerate(self):
                    row[index] = value[i]

            self._valid_keys.add(index)

        else:
            super().__setitem__(index, value)

    # ....................... #

    def _validate_item(self, item: Dict[str, Any]):
        """
        Validate an item

        Args:
            item (Dict[str, Any]): The item to validate

        Returns:
            res (bool): Whether the item is valid
        """

        assert isinstance(item, dict), "Item must be a dictionary"

        if set(item.keys()) != self._valid_keys:
            raise BadInput("Item must have the same keys as the other items")

        return True

    # ....................... #

    def _validate_data(self: Tb, data: Sequence[Dict[str, Any] | Bm] | Tb = []):
        """
        Validate the data

        Args:
            data (Sequence[Dict[str, Any] | Bm] | TabularData): The data to validate

        Returns:
            res (list): The validated data
        """

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
