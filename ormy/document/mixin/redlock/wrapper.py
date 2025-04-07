import asyncio
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, ClassVar, Optional, TypeVar

from ormy.base.typing import AsyncCallable
from ormy.document._abc import DocumentMixinABC
from ormy.exceptions import Conflict, InternalError, ModuleNotFound

try:
    from redis import Redis
    from redis import asyncio as aioredis
except ImportError as e:
    raise ModuleNotFound(extra="redlock", packages=["redis"]) from e

from .config import RedlockConfig

# ----------------------- #

T = TypeVar("T")

# ----------------------- #


class RedlockMixin(DocumentMixinABC):
    """Redlock mixin"""

    mixin_configs: ClassVar[list[Any]] = [RedlockConfig()]

    __redlock_static: ClassVar[Optional[Redis]] = None
    __aredlock_static: ClassVar[Optional[aioredis.Redis]] = None

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls.defer_mixin_registration(
            config=RedlockConfig,
            discriminator=["database", "collection"],
        )

    # ....................... #

    @classmethod
    def _get_redlock_collection(cls):
        """Get collection"""

        cfg = cls.get_mixin_config(type_=RedlockConfig)
        col = cfg.collection

        return col

    # ....................... #

    @classmethod
    def __is_static_redlock(cls):
        """Check if static Redis client is used"""

        cfg = cls.get_mixin_config(type_=RedlockConfig)
        use_static = not cfg.context_client

        return use_static

    # ....................... #

    @classmethod
    def _redlock_static_client(cls):
        """
        Get static Redis client for lock purposes

        Returns:
            client (redis.Redis): Static Redis client
        """

        if cls.__redlock_static is None:
            cfg = cls.get_mixin_config(type_=RedlockConfig)
            url = cfg.url()
            cls.__redlock_static = Redis.from_url(
                url,
                decode_responses=True,
            )

        return cls.__redlock_static

    # ....................... #

    @classmethod
    async def _aredlock_static_client(cls):
        """
        Get static async Redis client for lock purposes

        Returns:
            client (redis.asyncio.Redis): Static async Redis client
        """

        if cls.__aredlock_static is None:
            cfg = cls.get_mixin_config(type_=RedlockConfig)
            url = cfg.url()
            cls.__aredlock_static = aioredis.from_url(
                url,
                decode_responses=True,
            )

        return cls.__aredlock_static

    # ....................... #

    @classmethod
    @contextmanager
    def _redlock_client(cls):
        """Get syncronous Redis client for lock purposes"""

        cfg = cls.get_mixin_config(type_=RedlockConfig)
        url = cfg.url()
        r = Redis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            r.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _aredlock_client(cls):
        """Get asyncronous Redis client for lock purposes"""

        cfg = cls.get_mixin_config(type_=RedlockConfig)
        url = cfg.url()
        r = aioredis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            await r.close()

    # ....................... #

    @classmethod
    def __redlock_execute_task(cls, task: Callable[[Any], T]) -> T:
        """Execute task"""

        if cls.__is_static_redlock():
            c = cls._redlock_static_client()
            return task(c)

        else:
            with cls._redlock_client() as c:
                return task(c)

    # ....................... #

    @classmethod
    async def __aredlock_execute_task(cls, task: AsyncCallable[[Any], T]) -> T:
        """Execute async task"""

        if cls.__is_static_redlock():
            c = await cls._aredlock_static_client()
            return await task(c)

        else:
            async with cls._aredlock_client() as c:
                return await task(c)

    # ....................... #

    @classmethod
    def _acquire_lock(
        cls,
        key: str,
        unique_id: Optional[str] = None,
        timeout: int = 10,
    ) -> tuple[Optional[bool], Optional[str]]:
        """
        Acquire a lock with a unique identifier.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str, optional): A unique identifier for this lock holder.
            timeout (int): The timeout for the lock in seconds.

        Returns:
            result (bool): True if the lock was acquired, False otherwise.
            unique_id (str): The unique identifier for this lock holder.
        """

        unique_id = unique_id or str(uuid.uuid4())

        def _task(c: Redis):
            result = c.set(
                key,
                unique_id,
                nx=True,
                ex=timeout,
            )

            return result, unique_id if result else None

        return cls.__redlock_execute_task(_task)

    # ....................... #

    @classmethod
    async def _aacquire_lock(
        cls,
        key: str,
        unique_id: Optional[str] = None,
        timeout: int = 10,
    ) -> tuple[Optional[bool], Optional[str]]:
        """
        Acquire a lock with a unique identifier.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str, optional): A unique identifier for this lock holder.
            timeout (int): The timeout for the lock in seconds.

        Returns:
            result (bool): True if the lock was acquired, False otherwise.
            unique_id (str): The unique identifier for this lock holder.
        """

        unique_id = unique_id or str(uuid.uuid4())

        async def _task(c: aioredis.Redis):
            result = await c.set(
                key,
                unique_id,
                nx=True,
                ex=timeout,
            )

            return result, unique_id if result else None

        return await cls.__aredlock_execute_task(_task)

    # ....................... #

    @classmethod
    def _release_lock(cls, key: str, unique_id: str) -> bool:
        """
        Release the lock if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.

        Returns:
            result (bool): True if the lock was released, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """

        def _task(c: Redis):
            result = c.eval(
                script,
                1,
                key,
                unique_id,
            )

            return result

        return cls.__redlock_execute_task(_task)

    # ....................... #

    @classmethod
    async def _arelease_lock(cls, key: str, unique_id: str) -> bool:
        """
        Release the lock if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.

        Returns:
            result (bool): True if the lock was released, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """

        async def _task(c: aioredis.Redis):
            result = await c.eval(
                script,
                1,
                key,
                unique_id,
            )

            return result

        return await cls.__aredlock_execute_task(_task)

    # ....................... #

    @classmethod
    def _extend_lock(
        cls,
        key: str,
        unique_id: str,
        additional_time: int,
    ) -> bool:
        """
        Extend the lock expiration if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.
            additional_time (int): The additional time to extend the lock in seconds.

        Returns:
            result (bool): True if the lock was extended, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        def _task(c: Redis):
            result = c.eval(
                script,
                1,
                key,
                unique_id,
                additional_time,
            )

            return result == 1

        return cls.__redlock_execute_task(_task)

    # ....................... #

    @classmethod
    async def _aextend_lock(
        cls,
        key: str,
        unique_id: str,
        additional_time: int,
    ) -> bool:
        """
        Extend the lock expiration if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.
            additional_time (int): The additional time to extend the lock in seconds.

        Returns:
            result (bool): True if the lock was extended, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        async def _task(c: aioredis.Redis):
            result = await c.eval(
                script,
                1,
                key,
                unique_id,
                additional_time,
            )

            return result == 1

        return await cls.__aredlock_execute_task(_task)

    # ....................... #

    @classmethod
    @contextmanager
    def redlock_cls(  # TODO: exponential backoff, retry logic
        cls,
        id_: str,
        timeout: int = 10,
        extend_interval: int = 5,
        auto_extend: bool = True,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            id_ (str): The unique identifier of the entity.
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.
            auto_extend (bool): Whether to automatically extend the lock.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            InternalError: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout or the lock aquisition or extension fails.
        """

        if timeout <= 0:
            raise InternalError("timeout must be greater than 0")

        if extend_interval <= 0:
            raise InternalError("extend_interval must be greater than 0")

        if extend_interval >= timeout:
            raise InternalError("extend_interval must be less than timeout")

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"
        result, unique_id = cls._acquire_lock(
            key=resource,
            timeout=timeout,
        )

        if not result:
            raise Conflict(
                f"{resource} already locked",
            )

        extend_task = None
        stop_extend = threading.Event()

        def extend_lock_periodically(resource: str, unique_id: str):
            try:
                while not stop_extend.is_set():
                    time.sleep(extend_interval)
                    success = cls._extend_lock(
                        key=resource,
                        unique_id=unique_id,
                        additional_time=timeout,
                    )
                    if not success:
                        raise InternalError(f"Failed to extend lock for {resource}")
            except Exception as e:
                raise InternalError(f"Error in lock extension: {e}")

        try:
            if auto_extend:
                extend_task = threading.Thread(
                    target=extend_lock_periodically,
                    kwargs={
                        "resource": resource,
                        "unique_id": unique_id,
                    },
                    daemon=True,
                )
                extend_task.start()

            yield result

        finally:
            if auto_extend:
                stop_extend.set()

                if extend_task:
                    extend_task.join()

            if result and unique_id:
                cls._release_lock(
                    key=resource,
                    unique_id=unique_id,
                )

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def aredlock_cls(  # TODO: exponential backoff, retry logic
        cls,
        id_: str,
        timeout: int = 10,
        extend_interval: int = 5,
        auto_extend: bool = True,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            id_ (str): The unique identifier of the entity.
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.
            auto_extend (bool): Whether to automatically extend the lock.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            InternalError: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout or the lock aquisition or extension fails.
        """

        if timeout <= 0:
            raise InternalError("timeout must be greater than 0")

        if extend_interval <= 0:
            raise InternalError("extend_interval must be greater than 0")

        if extend_interval >= timeout:
            raise InternalError("extend_interval must be less than timeout")

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"
        result, unique_id = await cls._aacquire_lock(
            key=resource,
            timeout=timeout,
        )

        if not result:
            raise Conflict(
                f"{resource} already locked",
            )

        if not unique_id:
            raise InternalError(f"Failed to acquire lock for {resource}")

        extend_task = None
        stop_extend = asyncio.Event()

        async def extend_lock_periodically(resource: str, unique_id: str):
            try:
                while not stop_extend.is_set():
                    await asyncio.sleep(extend_interval)
                    success = await cls._aextend_lock(
                        key=resource,
                        unique_id=unique_id,
                        additional_time=timeout,
                    )
                    if not success:
                        raise InternalError(f"Failed to extend lock for {resource}")

            except asyncio.CancelledError:
                pass

        try:
            if auto_extend:
                extend_task = asyncio.create_task(
                    extend_lock_periodically(
                        resource=resource,
                        unique_id=unique_id,
                    )
                )

            yield result

        finally:
            if auto_extend:
                stop_extend.set()

                if extend_task:
                    extend_task.cancel()
                    try:
                        await extend_task
                    except asyncio.CancelledError:
                        pass

            if result and unique_id:
                await cls._arelease_lock(
                    key=resource,
                    unique_id=unique_id,
                )
