from typing import Annotated, Literal, TypeAlias

# ----------------------- #

# Annotations
FieldName = Annotated[str, "Data model field name"]
FieldDataType = Annotated[str, "Data model field type"]

# Aliases
Wildcard: TypeAlias = Literal["*"]
