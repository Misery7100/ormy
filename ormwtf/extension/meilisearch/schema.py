from enum import StrEnum
from typing import Generic, List, Optional, Type, TypeVar

from meilisearch_python_sdk.models.search import SearchResults
from meilisearch_python_sdk.models.settings import MeilisearchSettings  # noqa: F401
from meilisearch_python_sdk.types import Filter
from pydantic import BaseModel, Field

# ----------------------- #


# ----------------------- #


class SortOrder(StrEnum):
    asc = "asc"
    desc = "desc"


# ....................... #


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
    filters: Optional[Filter] = Field(
        default=None,
        title="Filters",
    )


# ----------------------- #

S = TypeVar("S", bound="SearchResponse")
T = TypeVar("T")

# ....................... #


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
