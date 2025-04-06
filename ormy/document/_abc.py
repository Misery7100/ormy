from abc import abstractmethod
from typing import Any, ClassVar, Mapping, Self, TypeVar, cast

from pydantic import Field

from ormy._abc import (
    AbstractABC,
    AbstractData,
    AbstractExtensionABC,
    ConfigABC,
    SemiFrozenField,
)
from ormy.base.func import hex_uuid4
from ormy.base.pydantic import IGNORE
from ormy.exceptions import Conflict

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)

# TODO: DocumentConfigABC

# ....................... #


class DocumentABC(AbstractABC):
    """Abstract Base Class for Document-Oriented Object-Relational Mapping"""

    id: str = Field(default_factory=hex_uuid4)

    semi_frozen_fields: ClassVar[Mapping[str, SemiFrozenField | dict[str, Any]]] = {}

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)
        cls.__parse_semi_frozen_fields()

    # ....................... #

    @classmethod
    def __parse_semi_frozen_fields(cls):
        """Parse semi-frozen fields"""

        new = {}

        for field, value in cls.semi_frozen_fields.items():
            if isinstance(value, dict):
                new[field] = SemiFrozenField(**value)

            else:
                new[field] = value

        cls.semi_frozen_fields = new

    # ....................... #

    @classmethod
    @abstractmethod
    def create(cls, data: Self) -> Self: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def acreate(cls, data: Self) -> Self: ...

    # ....................... #

    @abstractmethod
    def save(self: Self) -> Self: ...

    # ....................... #

    @abstractmethod
    async def asave(self: Self) -> Self: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def find(cls, id_: str) -> Self: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(cls, id_: str) -> Self: ...

    # ....................... #

    @abstractmethod
    def kill(self: Self) -> None: ...

    # ....................... #

    @abstractmethod
    async def akill(self: Self) -> None: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def kill_many(cls, *args: Any, **kwargs: Any) -> None: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def akill_many(cls, *args: Any, **kwargs: Any) -> None: ...

    # ....................... #

    def update(
        self: Self,
        data: AbstractData,
        autosave: bool = True,
        soft_frozen: bool = True,
    ):
        """
        Update the document with the given data

        Args:
            data (AbstractData): Data to update the document with
            autosave (bool): Save the document after updating

        Returns:
            self (Ds): Updated document
        """

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, IGNORE)

            if val != IGNORE and k in self.model_fields:
                if k in self.semi_frozen_fields.keys():
                    _semi = self.semi_frozen_fields[k]
                    semi = cast(SemiFrozenField, _semi)

                    if semi.evaluate(self):
                        if not soft_frozen:
                            raise Conflict(
                                f"Field {k} is semi-frozen within context {semi.context}"
                            )

                        else:
                            continue

                elif self.model_fields[k].frozen:
                    if not soft_frozen:
                        raise Conflict(f"Field {k} is frozen")

                    else:
                        continue

                setattr(self, k, val)

        if autosave:
            return self.save()

        return self

    # ....................... #

    async def aupdate(
        self: Self,
        data: AbstractData,
        autosave: bool = True,
        soft_frozen: bool = True,
    ):
        """
        Update the document with the given data

        Args:
            data (AbstractData): Data to update the document with
            autosave (bool): Save the document after updating

        Returns:
            self (Ds): Updated document
        """

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, IGNORE)

            if val != IGNORE and k in self.model_fields:
                if k in self.semi_frozen_fields.keys():
                    _semi = self.semi_frozen_fields[k]
                    semi = cast(SemiFrozenField, _semi)

                    if semi.evaluate(self):
                        if not soft_frozen:
                            raise Conflict(
                                f"Field {k} is semi-frozen within context {semi.context}"
                            )

                        else:
                            continue

                elif self.model_fields[k].frozen:
                    if not soft_frozen:
                        raise Conflict(f"Field {k} is frozen")

                    else:
                        continue

                setattr(self, k, val)

        if autosave:
            return await self.asave()

        return self


# ....................... #


class DocumentExtensionABC(AbstractExtensionABC):
    """Document Extension ABC Base Class"""
