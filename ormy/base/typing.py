from typing import (
    Annotated,
    Awaitable,
    Callable,
    Dict,
    Literal,
    TypeAlias,
    TypeVar,
    ParamSpec,
)

from .abc.typing import AbstractData, DocumentID

# ----------------------- #

# Annotations
FieldName = Annotated[str, "The name of the data model field"]
FieldTitle = Annotated[str, "The title of the data model field"]
FieldDataType = Annotated[str, "The data type of the data model field"]

FieldSchema = Annotated[
    Dict[str, FieldName | FieldTitle | FieldDataType], "The data model field"
]

# Aliases
Wildcard: TypeAlias = Literal["*", "all"]

P = ParamSpec("P")
R = TypeVar("R")

AsyncCallable = Callable[P, Awaitable[R]]

# ----------------------- #

__all__ = [
    "AbstractData",
    "AsyncCallable",
    "DocumentID",
    "FieldDataType",
    "FieldSchema",
    "FieldName",
    "FieldTitle",
    "Wildcard",
]
