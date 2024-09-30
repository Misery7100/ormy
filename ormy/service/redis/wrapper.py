import json
from contextlib import asynccontextmanager, contextmanager
from typing import Optional, Type, TypeVar

from aioredlock import Aioredlock  # type: ignore
from redis import Redis
from redis import asyncio as aioredis

from ormy.base.abc import DocumentABC
from ormy.base.typing import DocumentID
from ormy.utils.logging import LogLevel, console_logger

from .config import RedisConfig

# ----------------------- #

T = TypeVar("T", bound="RedisBase")

logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class RedisBase(DocumentABC):  # TODO: add docstrings
    """Base ORM document model class for Redis"""

    configs = [RedisConfig()]
    _registry = {RedisConfig: {}}

    # ....................... #

    def __init_subclass__(cls: Type[T], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._redis_register_subclass()
        cls._merge_registry()

        RedisBase._registry = cls._merge_registry_helper(
            RedisBase._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _redis_register_subclass(cls: Type[T]):
        """Register subclass in the registry"""

        cfg = cls.get_config(type_=RedisConfig)
        db = cfg.database
        col = cfg.collection

        # TODO: use exact default value from class
        if cfg.include_to_registry and not cfg.is_default():
            logger.debug(f"Registering {cls.__name__} in {db}.{col}")
            logger.debug(f"Registry before: {cls._registry}")

            cls._registry[RedisConfig] = cls._registry.get(RedisConfig, {})
            cls._registry[RedisConfig][db] = cls._registry[RedisConfig].get(db, {})
            cls._registry[RedisConfig][db][col] = cls

            logger.debug(f"Registry after: {cls._registry}")

    # ....................... #

    @classmethod
    @contextmanager
    def _client(cls: Type[T]):
        """Get syncronous Redis client"""

        cfg = cls.get_config(type_=RedisConfig)
        url = cfg.url()
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

        cfg = cls.get_config(type_=RedisConfig)
        url = cfg.url()
        r = aioredis.from_url(url)

        try:
            yield r

        finally:
            await r.close()

    # ....................... #

    @classmethod
    def _alock_manager(cls: Type[T]):
        """
        ...
        """

        cfg = cls.get_config(type_=RedisConfig)
        url = cfg.url()

        return Aioredlock([url])

    # ....................... #

    @classmethod
    def _build_key(cls: Type[T], key: str) -> str:
        """Build key for Redis storage"""

        cfg = cls.get_config(type_=RedisConfig)

        return f"{cfg.collection}:{key}"

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
        key = self._build_key(self.id)

        with self._client() as client:
            client.set(key, json.dumps(document))

        return self

    # ....................... #

    async def asave(self: T) -> T:
        """
        ...
        """

        document = self.model_dump()
        key = self._build_key(self.id)

        async with self._aclient() as client:
            await client.set(key, json.dumps(document))

        return self

    # ....................... #

    async def alock(self: T, **kwargs) -> None:
        """
        ...
        """

        key = self._build_key(self.id)
        return await self._alock_manager().lock(key, **kwargs)

    # ....................... #

    async def aextend(self: T, **kwargs) -> None:
        """
        ...
        """

        key = self._build_key(self.id)
        return await self._alock_manager().extend(key, **kwargs)

    # ....................... #

    async def ais_locked(self: T) -> bool:
        """
        ...
        """

        key = self._build_key(self.id)
        return await self._alock_manager().is_locked(key)

    # ....................... #

    async def aunlock(self: T) -> None:
        """
        ...
        """

        key = self._build_key(self.id)
        return await self._alock_manager().unlock(key)

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
