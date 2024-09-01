from abc import ABC, abstractmethod
from enum import StrEnum
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from meilisearch_python_sdk.models.search import SearchResults
from pydantic import BaseModel, Field, model_validator

from ormwtf.base.typing import FieldName

# ----------------------- #


class SortOrder(StrEnum):
    asc = "asc"
    desc = "desc"


# ....................... #


class SortField(BaseModel):
    key: FieldName
    title: str
    default: bool = False


# ----------------------- #

F = TypeVar("F", bound="FilterABC")


class FilterABC(ABC, BaseModel):
    key: FieldName
    title: str
    value: Optional[Any] = None
    type: str = "abc"

    # ....................... #

    @abstractmethod
    def build(self) -> Optional[str]: ...


# ....................... #


class BooleanFilter(FilterABC):
    value: Optional[bool] = None
    type: Literal["boolean"] = "boolean"

    # ....................... #

    def build(self):
        if self.value is not None:
            return f"{self.key} = {str(self.value).lower()}"

        return None


# ....................... #


class NumericFilter(FilterABC):
    value: Tuple[Optional[float], Optional[float]] = (None, None)
    type: Literal["numeric"] = "numeric"

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


class DatetimeFilter(FilterABC):
    value: Tuple[Optional[int], Optional[int]] = (None, None)
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


class ArrayFilter(FilterABC):
    value: List[Any] = []
    type: Literal["array"] = "array"

    # ....................... #

    def build(self):
        if self.value:
            return f"{self.key} IN {self.value}"

        return None


# ....................... #

SomeFilter = Union[BooleanFilter, NumericFilter, DatetimeFilter, ArrayFilter]

# ----------------------- #


class SearchRequest(BaseModel):
    query: str = Field(
        default="",
        title="Query",
    )
    sort: Optional[str] = Field(
        default=None,
        title="Sort Field",
    )
    order: SortOrder = Field(
        default=SortOrder.desc,
        title="Sort Order",
    )
    filters: List[SomeFilter] = Field(
        default_factory=list,
        title="Filters",
        discriminator="type",
    )


# ----------------------- #

S = TypeVar("S", bound="SearchResponse")
T = TypeVar("T")


class SearchResponse(BaseModel, Generic[T]):
    hits: List[T] = Field(
        default_factory=list,
        title="Hits",
    )
    size: int = Field(
        ...,
        title="Hits per Page",
    )
    page: int = Field(
        ...,
        title="Current Page",
    )
    count: int = Field(
        ...,
        title="Total number of Hits",
    )

    # ....................... #

    @classmethod
    def from_search_results(cls: Type[S], res: SearchResults) -> S:
        return cls(
            hits=res.hits,
            size=res.hits_per_page,  # type: ignore
            page=res.page,  # type: ignore
            count=res.total_hits,  # type: ignore
        )


# ....................... #


class MeilisearchReference(BaseModel):
    table_schema: List[Dict[str, Any]] = Field(
        default_factory=list,
        title="Table Schema",
    )
    sort: List[SortField] = Field(
        default_factory=list,
        title="Sort Fields",
    )
    filters: List[SomeFilter] = Field(
        default_factory=list,
        title="Filters",
        discriminator="type",
    )

    # ....................... #

    @model_validator(mode="after")
    def filter_schema_fields(self):
        self.table_schema = [
            {k: v for k, v in field.items() if k in ["key", "title", "type"]}
            for field in self.table_schema
        ]

        return self
