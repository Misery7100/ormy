from typing import Annotated, Any, Dict, Literal, TypeAlias

# ----------------------- #

# Annotations
FieldName = Annotated[str, "Data model field name"]
FieldDataType = Annotated[str, "Data model field type"]
Settings = Annotated[Dict[str, Any], "settings"]
DocumentID = Annotated[str, "Document ID"]

# Aliases
Wildcard: TypeAlias = Literal["*"]
