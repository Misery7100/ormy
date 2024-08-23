from abc import ABC, abstractmethod
from typing import Optional, Type, TypeVar

from pydantic import Field

from .func import hex_uuid4
from .pydantic import Base
from .typing import AbstractData, DocumentID

# ----------------------- #

T = TypeVar("T", bound="DocumentOrmABC")

# ....................... #


class DocumentOrmABC(Base, ABC):
    """
    Abstract Base Class for Document-Oriented Object-Relational Mapping
    """

    # ....................... #

    id: DocumentID = Field(title="Document ID", default_factory=hex_uuid4)

    # ....................... #

    @classmethod
    @abstractmethod
    def create(cls: Type[T], data: T) -> T: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def acreate(cls: Type[T], data: T) -> T: ...

    # ....................... #

    @abstractmethod
    def save(self: T) -> T: ...

    # ....................... #

    @abstractmethod
    async def asave(self: T) -> T: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def find(
        cls: Type[T],
        id_: DocumentID,
        bypass: bool = False,
    ) -> Optional[T]: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(
        cls: Type[T],
        id_: DocumentID,
        bypass: bool = False,
    ) -> Optional[T]: ...

    # ....................... #

    @classmethod
    async def update(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        ignore_none: bool = True,
        autosave: bool = True,
    ) -> T:
        """
        ...
        """

        instance = cls.find(id_)

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(instance, k):
                setattr(instance, k, val)

        if autosave:
            return instance.save()

        return instance

    # ....................... #

    @classmethod
    async def aupdate(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        ignore_none: bool = True,
        autosave: bool = True,
    ) -> T:
        """
        ...
        """

        instance = await cls.afind(id_)

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(instance, k):
                setattr(instance, k, val)

        if autosave:
            return await instance.asave()

        return instance
