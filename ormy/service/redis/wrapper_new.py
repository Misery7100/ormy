import json  # noqa: F401
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, ClassVar, Optional, Self, Type, TypeVar  # noqa: F401

from redis import Redis
from redis import asyncio as aioredis
from redis.asyncio.client import Pipeline as Apipeline  # noqa: F401
from redis.client import Pipeline  # noqa: F401

from ormy.base.abc import DocumentSingleABC
from ormy.base.error import Conflict, NotFound
from ormy.base.typing import AsyncCallable, DocumentID

from .config import RedisConfig

# ----------------------- #

R = TypeVar("R", bound="RedisSingleBase")
T = TypeVar("T")

# ----------------------- #


class RedisSingleBase(DocumentSingleABC):
    """MongoDB base class"""

    config: ClassVar[RedisConfig] = RedisConfig()

    __static: ClassVar[Optional[Redis]] = None
    __astatic: ClassVar[Optional[aioredis.Redis]] = None

    # ....................... #

    def __init_subclass__(cls: Type[R], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_subclass_helper(discriminator=["database", "collection"])

    # ....................... #

    @classmethod
    def __is_static_redis(cls: Type[R]):
        """Check if static Redis client is used"""

        return not cls.config.context_client

    # ....................... #

    @classmethod
    def __static_client(cls):
        """
        Get static Redis client

        Returns:
            client (redis.Redis): Static Redis client
        """

        if cls.__static is None:
            url = cls.config.url()
            cls.__static = Redis.from_url(
                url,
                decode_responses=True,
            )

        return cls.__static

    # ....................... #

    @classmethod
    async def __astatic_client(cls):
        """
        Get static async Redis client

        Returns:
            client (redis.asyncio.Redis): Static async Redis client
        """

        if cls.__astatic is None:
            url = cls.config.url()
            cls.__astatic = aioredis.from_url(
                url,
                decode_responses=True,
            )

        return cls.__astatic

    # ....................... #

    @classmethod
    @contextmanager
    def __client(cls):
        """Get syncronous Redis client"""

        url = cls.config.url()
        r = Redis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            r.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def __aclient(cls):
        """Get asyncronous Redis client"""

        url = cls.config.url()
        r = aioredis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            await r.close()

    # ....................... #

    @classmethod
    def __execute_task(cls, task: Callable[[Redis], T]) -> T:
        """Execute task"""

        if cls.__is_static_redis():
            c = cls.__static_client()
            return task(c)

        else:
            with cls.__client() as c:
                return task(c)

    # ....................... #

    @classmethod
    async def __aexecute_task(cls, task: AsyncCallable[[aioredis.Redis], T]) -> T:
        """Execute async task"""

        if cls.__is_static_redis():
            c = await cls.__astatic_client()
            return await task(c)

        else:
            async with cls.__aclient() as c:
                return await task(c)

    # ....................... #

    @classmethod
    def _build_key(cls, key: DocumentID) -> str:
        """Build key for Redis storage"""

        return f"{cls.config.collection}:{key}"

    # ....................... #

    @classmethod
    def create(cls: Type[R], data: R) -> R:
        """
        Create a new document in Redis

        Args:
            data (RedisSingleBase): Data model to be created

        Returns:
            res (RedisSingleBase): Created data model

        Raises:
            _ (ormy.base.error.Conflict): Document already exists
        """

        document = data.model_dump(mode="json")

        _id = document["id"]
        key = cls._build_key(_id)

        try:
            cls.find(_id)
            raise Conflict("Document already exists")

        except NotFound:
            pass

        def _task(c: Redis):
            c.set(key, json.dumps(document))

        cls.__execute_task(_task)

        return data

    # ....................... #

    @classmethod
    async def acreate(cls: Type[R], data: R) -> R:
        """
        Create a new document in Redis

        Args:
            data (RedisSingleBase): Data model to be created

        Returns:
            res (RedisSingleBase): Created data model

        Raises:
            _ (ormy.base.error.Conflict): Document already exists
        """

        document = data.model_dump(mode="json")

        _id = document["id"]
        key = cls._build_key(_id)

        try:
            await cls.afind(_id)
            raise Conflict("Document already exists")

        except NotFound:
            pass

        async def _atask(c: aioredis.Redis):
            await c.set(key, json.dumps(document))

        await cls.__aexecute_task(_atask)

        return data

    # ....................... #

    @classmethod
    @contextmanager
    def pipe(cls, **kwargs):
        """Get syncronous Redis pipeline"""

        def _task(c: Redis):
            p = c.pipeline(**kwargs)
            return p

        try:
            yield cls.__execute_task(_task)

        finally:
            pass

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def apipe(cls, **kwargs):
        """Get asyncronous Redis pipeline"""

        async def _atask(c: aioredis.Redis):
            p = c.pipeline(**kwargs)
            return p

        try:
            yield await cls.__aexecute_task(_atask)

        finally:
            pass

    # ....................... #

    def watch(self: Self, pipe: Pipeline):
        """
        Watch for changes in the Redis key

        Args:
            pipe (redis.Pipeline): Redis pipeline
        """

        key = self._build_key(self.id)
        pipe.watch(key)

    # ....................... #

    async def awatch(self: Self, pipe: Apipeline):
        """
        Watch for changes in the Redis key

        Args:
            pipe (redis.asyncio.Pipeline): Redis pipeline
        """

        key = self._build_key(self.id)
        await pipe.watch(key)

    # ....................... #

    def save(self: Self, pipe: Optional[Pipeline] = None):
        """
        Save document to Redis
        """

        document = self.model_dump()
        key = self._build_key(self.id)

        def _task(c: Redis):
            c.set(key, json.dumps(document))

        if pipe is None:
            self.__execute_task(_task)

        else:
            pipe.multi()
            pipe.set(key, json.dumps(document))
            pipe.execute()

        return self

    # ....................... #

    async def asave(self: Self, pipe: Optional[Apipeline] = None):
        """
        Save document to Redis
        """

        document = self.model_dump()
        key = self._build_key(self.id)

        async def _atask(c: aioredis.Redis):
            await c.set(key, json.dumps(document))

        if pipe is None:
            await self.__aexecute_task(_atask)

        else:
            pipe.multi()
            pipe.set(key, json.dumps(document))
            await pipe.execute()

        return self

    # ....................... #

    @classmethod
    def find(cls: Type[R], id_: DocumentID) -> R:
        """
        Find document in Redis

        Args:
            id_ (DocumentID): Document ID

        Returns:
            res (RedisSingleBase): Found document

        Raises:
            _ (ormy.base.error.NotFound): Document not found
        """

        key = cls._build_key(id_)

        def _task(c: Redis):
            res = c.get(key)

            if res:
                return cls.model_validate_json(res)

            raise NotFound("Document not found")

        return cls.__execute_task(_task)

    # ....................... #

    @classmethod
    async def afind(cls: Type[R], id_: DocumentID) -> R:
        """
        Find document in Redis

        Args:
            id_ (DocumentID): Document ID

        Returns:
            res (RedisSingleBase): Found document

        Raises:
            _ (ormy.base.error.NotFound): Document not found
        """

        key = cls._build_key(id_)

        async def _atask(c: aioredis.Redis):
            res = await c.get(key)

            if res:
                return cls.model_validate_json(res)

            raise NotFound("Document not found")

        return await cls.__aexecute_task(_atask)
