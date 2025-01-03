from abc import abstractmethod
from typing import Any, Optional, Type, TypeVar

from pydantic import Field

from ormy.base.func import hex_uuid4
from ormy.utils.logging import LogManager

from .abstract import AbstractABC, AbstractSingleABC
from .config import ConfigABC
from .typing import AbstractData, DocumentID

# ----------------------- #

D = TypeVar("D", bound="DocumentABC")
C = TypeVar("C", bound="ConfigABC")
Ds = TypeVar("Ds", bound="DocumentSingleABC")

logger = LogManager.get_logger(__name__)

# ----------------------- #


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
    ) -> Optional[D | Any]: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(
        cls: Type[D],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[D | Any]: ...

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


# ----------------------- #


class DocumentSingleABC(AbstractSingleABC):
    """
    Abstract Base Class for Document-Oriented Object-Relational Mapping
    """

    id: DocumentID = Field(title="Document ID", default_factory=hex_uuid4)

    # ....................... #

    @classmethod
    @abstractmethod
    def create(cls: Type[Ds], data: Ds) -> Ds: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def acreate(cls: Type[Ds], data: Ds) -> Ds: ...

    # ....................... #

    @abstractmethod
    def save(self: Ds) -> Ds: ...

    # ....................... #

    @abstractmethod
    async def asave(self: Ds) -> Ds: ...

    # ....................... #

    @classmethod
    @abstractmethod
    def find(
        cls: Type[Ds],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[Ds | Any]: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(
        cls: Type[Ds],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[Ds | Any]: ...

    # ....................... #

    def update(
        self: Ds,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Ds:
        """
        Update the document with the given data

        Args:
            data (AbstractData): Data to update the document with
            ignore_none (bool): Ignore None values
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
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(self, k):
                setattr(self, k, val)

        if autosave:
            return self.save()

        return self

    # ....................... #

    async def aupdate(
        self: Ds,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Ds:
        """
        Update the document with the given data

        Args:
            data (AbstractData): Data to update the document with
            ignore_none (bool): Ignore None values
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
            val = data.get(k, None)

            if not (val is None and ignore_none) and hasattr(self, k):
                setattr(self, k, val)

        if autosave:
            return await self.asave()

        return self

    # ....................... #

    @classmethod
    def update_by_id(
        cls: Type[Ds],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[Ds]:
        """
        Update the document with the given data

        Args:
            id_ (DocumentID): ID of the document to update
            data (AbstractData): Data to update the document with
            ignore_none (bool): Ignore None values
            autosave (bool): Save the document after updating

        Returns:
            res (Optional[Ds]): Updated document or None if not found
        """

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
        cls: Type[Ds],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[Ds]:
        """
        Update the document with the given data by ID

        Args:
            id_ (DocumentID): ID of the document to update
            data (AbstractData): Data to update the document with
            ignore_none (bool): Ignore None values
            autosave (bool): Save the document after updating

        Returns:
            res (Optional[Ds]): Updated document or None if not found
        """

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
