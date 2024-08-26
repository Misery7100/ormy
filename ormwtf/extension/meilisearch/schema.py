from enum import StrEnum
from typing import Any, Dict, List, Optional

from meilisearch_python_sdk.types import Filter
from pydantic import BaseModel, ConfigDict, Field

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


# ....................... #


class SearchResponse(BaseModel):

    model_config = ConfigDict(populate_by_name=True)

    # ....................... #

    hits: List[Dict[str, Any]] = Field(
        default_factory=list,
        title="Hits",
    )
    size: int = Field(
        ...,
        validation_alias="hits_per_page",
        title="Hits per Page",
    )
    page: int = Field(
        ...,
        title="Current Page",
    )
    count: int = Field(
        ...,
        validation_alias="total_hits",
        title="Total number of Hits",
    )
