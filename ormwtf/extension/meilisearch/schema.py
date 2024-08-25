from enum import StrEnum
from typing import Any, Dict, List, Optional

from meilisearch_python_sdk.types import Filter
from pydantic import BaseModel, Field  # noqa: F401

# ----------------------- #


class SortOrder(StrEnum):
    asc = "asc"
    desc = "desc"


# ....................... #


class SearchRequest(BaseModel):
    query: str
    sort: Optional[str] = None
    order: SortOrder = SortOrder.desc
    filters: Optional[Filter] = None


# ....................... #


class SearchResponse(BaseModel):
    hits: List[Dict[str, Any]] = []
    size: int = Field(validation_alias="hits_per_page")
    page: int
    count: int = Field(validation_alias="total_hits")
