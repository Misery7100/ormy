from enum import Enum

# ----------------------- #


class ExtendedEnum(Enum):
    """A base class for extended enumerations."""

    @classmethod
    def list(cls):
        """Return a list of values from the enumeration."""

        return list(map(lambda c: c.value, cls))
