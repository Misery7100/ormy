import inspect
import json
from contextlib import asynccontextmanager, contextmanager
from typing import ClassVar, Type, TypeVar

from pydantic import Field
from redis import Redis
from redis import asyncio as aioredis

from ormwtf.base.func import hex_uuid4
from ormwtf.base.pydantic import Base
from ormwtf.base.typing import AbstractData, DocumentID

from .config import RedisConfig

# ----------------------- #

T = TypeVar("T", bound="RedisBase")

# ....................... #


class RedisBase(Base):

    config: ClassVar[RedisConfig] = RedisConfig.with_defaults()

    # ....................... #

    id: DocumentID = Field(title="Document ID", default_factory=hex_uuid4)

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass with config inheritance"""

        super().__init_subclass__(**kwargs)
        superclass = inspect.getmro(cls)[1]
        values = {**superclass.config, **cls.config}
        cls.config = RedisConfig.with_defaults(**values)

    # ....................... #

    @classmethod
    @contextmanager
    def _client(cls: Type[T]):
        """Get syncronous Redis client"""

        url = cls.config.url()
        r = Redis.from_url(url)

        try:
            yield r

        finally:
            r.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _aclient(cls: Type[T]):
        """Get asyncronous Redis client"""

        url = cls.config.url()
        r = aioredis.from_url(url)

        try:
            yield r

        finally:
            await r.close()

    # ....................... #

    @classmethod
    def _build_key(cls: Type[T], key: str) -> str:
        """Build key for Redis storage"""

        collection = cls.config["collection"]
        return f"{collection}:{key}"

    # ....................... #

    @classmethod
    def create(cls: Type[T], data: T) -> T:
        """
        ...
        """

        document = data.model_dump()
        _id = document["id"]
        key = cls._build_key(_id)

        if cls.find(_id, bypass=True) is not None:
            raise ValueError(f"Document with ID {_id} already exists")

        with cls._client() as client:
            client.set(key, json.dumps(document))

        return data

    # ....................... #

    @classmethod
    async def acreate(cls: Type[T], data: T) -> T:
        """
        ...
        """

        document = data.model_dump()
        _id = document["id"]
        key = cls._build_key(_id)

        if await cls.afind(_id, bypass=True) is not None:
            raise ValueError(f"Document with ID {_id} already exists")

        async with cls._aclient() as client:
            await client.set(key, json.dumps(document))

        return data

    # ....................... #

    def save(self: T) -> T:
        """
        ...
        """

        document = self.model_dump()
        _id = document["id"]
        key = self._build_key(_id)

        with self._client() as client:
            client.set(key, json.dumps(document))

        return self

    # ....................... #

    async def asave(self: T) -> T:
        """
        ...
        """

        document = self.model_dump()
        _id = document["id"]
        key = self._build_key(_id)

        async with self._aclient() as client:
            await client.set(key, json.dumps(document))

        return self

    # ....................... #

    @classmethod
    def update(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        ignore_none: bool = True,
        autosave: bool = True,
    ) -> T:
        pass

    # ....................... #

    @classmethod
    async def aupdate(
        cls: Type[T],
        id_: DocumentID,
        data: AbstractData,
        ignore_none: bool = True,
        autosave: bool = True,
    ) -> T:
        pass

    # ....................... #

    @classmethod
    def find(cls: Type[T], id_: DocumentID, bypass: bool = False) -> T:
        key = cls._build_key(id_)

        with cls._client() as client:
            res = client.get(key)

            if not res:
                if bypass:
                    return

                raise ValueError(f"Document with ID {id_} not found")

            return cls.model_validate_json(res)

    # ....................... #

    @classmethod
    async def afind(cls: Type[T], id_: DocumentID, bypass: bool = False) -> T:
        key = cls._build_key(id_)

        async with cls._aclient() as client:
            res = await client.get(key)

            if not res:
                if bypass:
                    return

                raise ValueError(f"Document with ID {id_} not found")

            return cls.model_validate_json(res)

    # ....................... #
