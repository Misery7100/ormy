from abc import abstractmethod
from typing import Optional, Type, TypeVar

from pydantic import Field

from ormy.base.func import hex_uuid4
from ormy.base.typing import AbstractData, DocumentID
from ormy.utils.logging import LogLevel, console_logger

from .abstract import AbstractABC
from .config import ConfigABC

# ----------------------- #

D = TypeVar("D", bound="DocumentABC")
C = TypeVar("C", bound="ConfigABC")

logger = console_logger(__name__, level=LogLevel.INFO)


class DocumentABC(AbstractABC):
    """
    Abstract Base Class for Document-Oriented Object-Relational Mapping
    """

    id: DocumentID = Field(title="Document ID", default_factory=hex_uuid4)

    # ....................... #

    @classmethod
    @abstractmethod
    def create(cls: Type[D], data: D) -> D: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def acreate(cls: Type[D], data: D) -> D: ...

    # ....................... #

    @abstractmethod
    def save(self: D) -> D: ...

    # ....................... #

    @abstractmethod
    async def asave(self: D) -> D: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def find(
        cls: Type[D],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[D]: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(
        cls: Type[D],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[D]: ...

    # ....................... #

    def update(
        self: D,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> D:
        """
        ...
        """

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(self, k):
                setattr(self, k, val)

        if autosave:
            return self.save()

        return self

    # ....................... #

    async def aupdate(
        self: D,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> D:
        """
        ...
        """

        if isinstance(data, dict):
            keys = data.keys()

        else:
            keys = data.model_fields.keys()
            data = data.model_dump()

        for k in keys:
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(self, k):
                setattr(self, k, val)

        if autosave:
            return await self.asave()

        return self

    # ....................... #

    @classmethod
    def update_by_id(
        cls: Type[D],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[D]:

        instance = cls.find(id_)

        if instance:
            return instance.update(
                data,
                *args,
                ignore_none=ignore_none,
                autosave=autosave,
                **kwargs,
            )

        return None

    # ....................... #

    @classmethod
    async def aupdate_by_id(
        cls: Type[D],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[D]:

        instance = await cls.afind(id_)

        if instance:
            return await instance.aupdate(
                data,
                *args,
                ignore_none=ignore_none,
                autosave=autosave,
                **kwargs,
            )

        return None
