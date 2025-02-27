from typing import Any

from pydantic import BaseModel

# ----------------------- #

AbstractData = BaseModel | dict[str, Any]
