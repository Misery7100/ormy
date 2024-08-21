from typing import Annotated, Any, Dict, Literal, TypeAlias

# ----------------------- #

# Annotations
FieldName = Annotated[str, "The name of the data model field"]
FieldTitle = Annotated[str, "The title of the data model field"]
FieldDataType = Annotated[str, "The data type of the data model field"]

Field = Annotated[
    Dict[str, FieldName | FieldTitle | FieldDataType], "The data model field"
]

Settings = Annotated[Dict[str, Any], "settings"]
DocumentID = Annotated[str, "Document ID"]

# Aliases
Wildcard: TypeAlias = Literal["*", "all"]
