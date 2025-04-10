from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Annotated, Any, Literal, Optional

from pydantic import BaseModel, Field
from typing_extensions import NotRequired, TypedDict

from ormy.base.decorator import json_schema_modifier, remove_description
from ormy.base.generic import TabularData
from ormy.base.pydantic import TableResponse

# ----------------------- #


@json_schema_modifier(remove_description)
class SortOrder(StrEnum):
    """
    Order of the sort

    Attributes:
        asc (str): Ascending Order
        desc (str): Descending Order
    """

    asc = "asc"
    desc = "desc"


# ....................... #


@json_schema_modifier(remove_description)
class SortField(BaseModel):
    """
    Sort field model

    Attributes:
        key (str): Key of the field
        default (bool): Whether the Field is the default sort field
    """

    key: str
    default: bool = False


# ----------------------- #
# TODO: add filter operators (mb use a separate uniform interface)


class FilterABC(ABC, BaseModel):
    """
    Abstract Base Class for Search Filters

    Attributes:
        key (str): Key of the filter
        value (Any, optional): The filter value
        type (str): The filter type
    """

    key: str
    value: Optional[Any] = None
    type: str = "abc"

    # ....................... #

    @abstractmethod
    def build(self) -> Optional[str]: ...


# ....................... #


@json_schema_modifier(remove_description)
class BooleanFilter(FilterABC):
    """
    Boolean filter

    Attributes:
        key (str): Key of the filter
        value (bool): The filter value
    """

    value: Optional[bool] = None
    type: Literal["boolean"] = "boolean"

    # ....................... #

    def build(self):
        if self.value is not None:
            return f"{self.key} = {str(self.value).lower()}"

        return None


# ....................... #


class BooleanFilterDict(TypedDict):
    key: str
    value: NotRequired[bool]
    type: Literal["boolean"]


# ....................... #


@json_schema_modifier(remove_description)
class NumberFilter(FilterABC):
    """
    Numeric filter

    Attributes:
        key (str): Key of the filter
        value (Tuple[float | None, float | None]): The filter value
    """

    value: tuple[Optional[float], Optional[float]] = (None, None)
    type: Literal["number"] = "number"

    # ....................... #

    def build(self):
        low, high = self.value

        if low is None and high is not None:
            return f"{self.key} <= {high}"

        if low is not None and high is None:
            return f"{self.key} >= {low}"

        if low is not None and high is not None:
            return f"{self.key} {low} TO {high}"

        return None


# ....................... #


class NumberFilterDict(TypedDict):
    key: str
    value: NotRequired[tuple[Optional[float], Optional[float]]]
    type: Literal["number"]


# ....................... #


@json_schema_modifier(remove_description)
class DatetimeFilter(FilterABC):
    """
    Datetime filter

    Attributes:
        key (str): Key of the filter
        value (Tuple[int | None, int | None]): The filter value
    """

    value: tuple[Optional[int], Optional[int]] = (None, None)
    type: Literal["datetime"] = "datetime"

    # ....................... #

    def build(self):
        low, high = self.value

        if low is None and high is not None:
            return f"{self.key} <= {high}"

        if low is not None and high is None:
            return f"{self.key} >= {low}"

        if low is not None and high is not None:
            return f"{self.key} {low} TO {high}"

        return None


# ....................... #


class DatetimeFilterDict(TypedDict):
    key: str
    value: NotRequired[tuple[Optional[int], Optional[int]]]
    type: Literal["datetime"]


# ....................... #


@json_schema_modifier(remove_description)
class ArrayFilter(FilterABC):
    """
    Array filter

    Attributes:
        key (str): Key of the filter
        value (list[Any]): The filter value
    """

    value: list[Any] = []
    type: Literal["array"] = "array"

    # ....................... #

    def build(self):
        if self.value:
            return f"{self.key} IN {self.value}"

        return None


# ....................... #


class ArrayFilterDict(TypedDict):
    key: str
    value: NotRequired[list[Any]]
    type: Literal["array"]


# ....................... #

AnyFilter = Annotated[
    BooleanFilter | NumberFilter | DatetimeFilter | ArrayFilter,
    Field(discriminator="type"),
]

# ....................... #

AnyFilterDict = (
    BooleanFilterDict | NumberFilterDict | DatetimeFilterDict | ArrayFilterDict
)

# ----------------------- #


class SearchRequest(BaseModel):
    query: str = ""
    sort: Optional[str] = None
    order: SortOrder = SortOrder.desc
    filters: list[AnyFilter] = []


# ....................... #


class SearchRequestDict(TypedDict):
    query: str
    sort: NotRequired[str]
    order: NotRequired[SortOrder]
    filters: NotRequired[list[AnyFilterDict]]


# ----------------------- #


class SearchResponse(TableResponse):
    @classmethod
    def from_search_results(cls, res: Any):
        """Create a SearchResponse from a search results"""

        # TODO: Replace with ormy errors

        assert res.hits is not None, "Hits must be provided"
        assert res.hits_per_page is not None, "Hits per page must be provided"
        assert res.page is not None, "Page must be provided"
        assert res.total_hits is not None, "Total hits must be provided"

        return cls(
            hits=TabularData(res.hits),
            size=res.hits_per_page,
            page=res.page,
            count=res.total_hits,
        )


# ....................... #


@json_schema_modifier(remove_description)
class MeilisearchReference(BaseModel):
    """
    Meilisearch reference model

    Attributes:
        sort (list[ormy.extension.meilisearch.schema.SortField]): The sort fields
        filters (list[ormy.extension.meilisearch.schema.AnyFilter]): The filters
    """

    sort: list[SortField] = []
    filters: list[AnyFilter] = []
