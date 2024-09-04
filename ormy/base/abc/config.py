from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import ConfigDict

from ormy.base.pydantic import Base

# ----------------------- #

C = TypeVar("C", bound="ConfigABC")


class ConfigABC(Base, ABC):
    """
    Abstract Base Class for ORM Configuration
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ....................... #

    def _default_helper(self: C, *fields: str) -> bool:
        """
        Helper function to check if a field has default value
        """

        for field in fields:
            if field not in self.model_fields.keys():
                raise ValueError(f"Field {field} not found in model")

            required = getattr(
                self.model_fields[field],
                "required",
                False,
            )
            default = getattr(
                self.model_fields[field],
                "default",
                None,
            )

            if required or (getattr(self, field) != default):
                return False

        return True

    # ....................... #

    def merge(self: C, other: C) -> C:
        """
        Merge two configurations
        """

        vals = {}

        for field in self.model_fields.keys():
            if not self._default_helper(field):
                vals[field] = getattr(self, field)

            else:
                vals[field] = getattr(other, field)

        return self.model_validate(vals)

    # ....................... #

    @abstractmethod
    def is_default(self: C) -> bool: ...
