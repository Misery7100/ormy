from typing import Optional  # noqa: F401

# ----------------------- #


class OrmyError(Exception):
    """Base class for all exceptions raised by the package"""

    def __init__(self, detail: str):
        self._detail = detail

    # ....................... #

    def __str__(self):
        return str(self._detail)
