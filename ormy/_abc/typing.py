from typing import Any, Dict

from pydantic import BaseModel

# ----------------------- #

AbstractData = BaseModel | Dict[str, Any]
