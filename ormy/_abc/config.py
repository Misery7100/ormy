from abc import ABC, abstractmethod
from typing import ClassVar, Self

from pydantic import ConfigDict

from ormy.base.logging import LogLevel
from ormy.base.pydantic import Base

# ----------------------- #


class Mergeable(Base):
    def _default_helper(self: Self, *fields: str, union: bool = True) -> bool:
        """
        Helper method to check if a field has default value

        Args:
            fields (str): The fields to check (args)
            union (bool): Whether to check if all fields are default

        Returns:
            bool: True if the field has default value, False otherwise
        """

        _checks = []

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
                _checks.append(True)  # Not default value

            else:
                _checks.append(False)  # Default value

        if union:
            not_default = all(_checks)

        else:
            not_default = any(_checks)

        return not not_default

    # ....................... #

    def merge(self: Self, other: Self):
        """
        Merge two mergeable objects

        Args:
            other: The other mergeable object to merge with

        Returns:
            The merged mergeable object
        """

        vals = {}

        for field in self.model_fields.keys():
            val_self = getattr(self, field)
            val_other = getattr(other, field)

            if isinstance(val_self, Mergeable) and isinstance(val_other, Mergeable):
                merged = (
                    val_self.merge(val_other)
                    if hasattr(val_self, "merge")
                    else val_self
                )
                vals[field] = merged

            elif not self._default_helper(field):
                vals[field] = val_self

            else:
                vals[field] = getattr(other, field)

        return self.model_validate(vals)


# ....................... #


class ConfigABC(Mergeable, ABC):
    """
    Abstract Base Class for ORM Configuration
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )
    log_level: ClassVar[LogLevel] = LogLevel.INFO

    # ....................... #

    @abstractmethod
    def is_default(self: Self) -> bool: ...
