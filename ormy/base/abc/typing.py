from typing import Annotated, Any, Dict

from pydantic import BaseModel

# ----------------------- #

AbstractData = Annotated[BaseModel | Dict[str, Any], "Abstract data"]
DocumentID = Annotated[str | int, "Document ID"]
