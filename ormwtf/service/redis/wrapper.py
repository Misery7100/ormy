import json
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar

from pydantic import ConfigDict
from redis import Redis
from redis import asyncio as aioredis

from ormwtf.base.abc import DocumentOrmABC
from ormwtf.base.typing import DocumentID

from .config import RedisConfig

# ----------------------- #

T = TypeVar("T", bound="RedisBase")

# ....................... #


class RedisBase(DocumentOrmABC):  # TODO: add docstrings
    """Base ORM document model class for Redis"""

    config: ClassVar[RedisConfig] = RedisConfig()
    model_config = ConfigDict(ignored_types=(RedisConfig,))

    _registry: ClassVar[Dict[int, Dict[str, Any]]] = {}

    # ....................... #

    def __init_subclass__(cls: Type[T], **kwargs):
        """Initialize subclass with config"""

        super().__init_subclass__(**kwargs)
        cls._register_subclass()

    # ....................... #

    @classmethod
    def _register_subclass(cls: Type[T]):
        """Register subclass in the registry"""

        db = cls.config.database
        col = cls.config.collection

        cls._registry[db] = cls._registry.get(db, {})
        cls._registry[db][col] = cls

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

        collection = cls.config.collection
        return f"{collection}:{key}"

    # ....................... #

    @classmethod
    def create(cls: Type[T], data: T) -> T:
        """
        Create a new document in the collection (meta)

        Args:
            data (RedisBase): Data model to be created

        Returns:
            res (RedisBase): Created data model
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
        Create a new document in the collection (meta) in async mode

        Args:
            data (RedisBase): Data model to be created

        Returns:
            res (RedisBase): Created data model
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
    def find(cls: Type[T], id_: DocumentID, bypass: bool = False) -> Optional[T]:
        key = cls._build_key(id_)

        with cls._client() as client:
            res = client.get(key)

        if res:
            return cls.model_validate_json(res)

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return res

    # ....................... #

    @classmethod
    async def afind(cls: Type[T], id_: DocumentID, bypass: bool = False) -> Optional[T]:
        key = cls._build_key(id_)

        async with cls._aclient() as client:
            res = await client.get(key)

        if res:
            return cls.model_validate_json(res)

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return res

    # ....................... #
