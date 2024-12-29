from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    TypeAlias,
    TypeVar,
)

from pydantic import BaseModel

# ----------------------- #

# Annotations
FieldName = Annotated[str, "The name of the data model field"]
FieldTitle = Annotated[str, "The title of the data model field"]
FieldDataType = Annotated[str, "The data type of the data model field"]

FieldSchema = Annotated[
    Dict[str, FieldName | FieldTitle | FieldDataType], "The data model field"
]

# Optional annotations
AbstractData = Annotated[BaseModel | Dict[str, Any], "Abstract data"]
DocumentID = Annotated[str, "Document ID"]

# Aliases
Wildcard: TypeAlias = Literal["*", "all"]

T = TypeVar("T")
R = TypeVar("R")

AsyncCallable = Callable[[T], Awaitable[R]]
