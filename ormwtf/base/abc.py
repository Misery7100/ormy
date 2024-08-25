import inspect
from abc import ABC, abstractmethod
from typing import ClassVar, Optional, Type, TypeVar

from pydantic import ConfigDict, Field

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

    config: ClassVar[Base] = Base()

    # ....................... #

    id: DocumentID = Field(title="Document ID", default_factory=hex_uuid4)

    # ....................... #

    def __init_subclass__(cls: Type[T], **kwargs):
        """Initialize subclass with config inheritance"""

        super().__init_subclass__(**kwargs)

        # TODO: move to base utils ?
        parents = inspect.getmro(cls)[1:]
        nearest = None

        for p in parents:
            cfg = getattr(p, "config", None)
            mcfg: ConfigDict = getattr(p, "model_config", {})  # type: ignore
            ignored_types = mcfg.get("ignored_types", tuple())

            if type(cfg) in ignored_types:
                nearest = p
                break

        if (nearest is not None) and (
            (nearest_config := getattr(nearest, "config", None)) is not None
        ):
            values = {**nearest_config.model_dump(), **cls.config.model_dump()}
            cls.config = type(cls.config)(**values)

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
        *args,
        **kwargs,
    ) -> Optional[T]: ...

    # ....................... #

    @classmethod
    @abstractmethod
    async def afind(
        cls: Type[T],
        id_: DocumentID,
        *args,
        **kwargs,
    ) -> Optional[T]: ...

    # ....................... #

    @classmethod
    def update(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[T]:
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

        if autosave and instance is not None:
            return instance.save()

        return instance

    # ....................... #

    @classmethod
    async def aupdate(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        *args,
        ignore_none: bool = True,
        autosave: bool = True,
        **kwargs,
    ) -> Optional[T]:
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

        if autosave and instance is not None:
            return await instance.asave()

        return instance
